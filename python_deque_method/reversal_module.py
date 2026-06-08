"""
Mongo-baseline quote reversal module for the live HFT app.

This module does not place orders and does not use static tick-reversal
thresholds. It reads the latest quote-reversal baseline from
SymbolState.latest_reversal_baseline(), builds the close/reverse thresholds
from that Mongo-loaded baseline, then inspects the recent quotes already held
inside SymbolState.

Position mapping:
  existing short  / entry_action SELL -> short_to_long baseline -> upward quote reversal -> BUY
  existing long   / entry_action BUY  -> long_to_short baseline -> downward quote reversal -> SELL
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


CLOSE_THRESHOLD_GROUP = "close_thresholds"
REVERSE_THRESHOLD_GROUP = "reverse_thresholds"


@dataclass(slots=True)
class QuoteReversalConfig:
    enabled: bool = True

    # All threshold values below should be populated from
    # SymbolState.latest_reversal_baseline().settings/thresholds.
    lookback_ms: int = 0
    minimum_ticks: int = 0
    minimum_persist_ms: int = 0
    minimum_mid_move: float = 0.0
    minimum_mid_move_percent: float | None = None
    maximum_spread_percent: float | None = None
    require_bid_ask_mid_same_direction: bool = True
    allow_reverse: bool = False

    supported_entry_actions: set[str] = field(default_factory=lambda: {"BUY", "SELL"})

    # Mongo baseline context, used for diagnostics and output.
    reversal_side: str | None = None
    threshold_group: str | None = None
    baseline_document_id: str | None = None
    baseline_run_id: str | None = None
    baseline_generated_at_utc: str | None = None
    baseline_candidate_count: int | None = None
    baseline_quality: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class QuotePoint:
    event_ms: int
    bid: float
    ask: float
    mid: float
    spread: float
    spread_percent: float
    raw: dict[str, Any]


@dataclass(slots=True)
class QuoteReversalDecision:
    symbol: str
    enabled: bool
    should_exit: bool
    should_reverse: bool
    close_reason: str | None
    reverse_direction: str | None
    order_action: str | None
    failed_reasons: list[str]
    position_side: str
    entry_action: str
    position_quantity: int
    lookback_ms: int
    valid_quote_count: int
    first_event_ms: int | None
    last_event_ms: int | None
    persist_ms: int | None
    first_bid: float | None
    first_ask: float | None
    first_mid: float | None
    last_bid: float | None
    last_ask: float | None
    last_mid: float | None
    latest_spread: float | None
    latest_spread_percent: float | None
    bid_delta: float | None
    ask_delta: float | None
    mid_delta: float | None
    directional_bid_delta: float | None
    directional_ask_delta: float | None
    directional_mid_delta: float | None
    mid_move_percent: float | None
    bid_ask_mid_same_direction: bool
    thresholds: dict[str, Any]
    action_mode: str | None = None
    queued_action: str | None = None
    reversal_side: str | None = None
    threshold_group: str | None = None
    baseline_document_id: str | None = None
    baseline_run_id: str | None = None
    baseline_generated_at_utc: str | None = None
    baseline_candidate_count: int | None = None
    baseline_quality: dict[str, Any] = field(default_factory=dict)


# =========================
# Raw short-key helpers
# =========================
def raw_float(value: Any) -> float | None:
    if value is None or value == "":
        return None

    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8", errors="replace")
        except UnicodeDecodeError:
            return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def raw_int(value: Any) -> int | None:
    float_value = raw_float(value)
    if float_value is None:
        return None

    try:
        return int(float_value)
    except (TypeError, ValueError, OverflowError):
        return None


def normalize_timestamp_to_ms(timestamp: int) -> int:
    if timestamp <= 0:
        return 0

    # nanoseconds
    if timestamp >= 10_000_000_000_000_000:
        return timestamp // 1_000_000

    # microseconds
    if timestamp >= 10_000_000_000_000:
        return timestamp // 1_000

    # milliseconds
    if timestamp >= 10_000_000_000:
        return timestamp

    # seconds
    return timestamp * 1000


def quote_event_ms(quote: dict[str, Any]) -> int | None:
    for key in ("t", "event_ms", "timestamp_ms", "ts_ms"):
        raw_value = raw_int(quote.get(key))
        if raw_value is not None and raw_value > 0:
            return normalize_timestamp_to_ms(raw_value)
    return None


def quote_point_from_raw(quote: dict[str, Any]) -> QuotePoint | None:
    bid = raw_float(quote.get("bp"))
    ask = raw_float(quote.get("ap"))
    event_ms = quote_event_ms(quote)

    if bid is None or ask is None or event_ms is None:
        return None

    if bid <= 0 or ask <= 0 or ask <= bid:
        return None

    mid = (bid + ask) / 2.0
    if mid <= 0:
        return None

    spread = ask - bid
    spread_percent = (spread / mid) * 100.0

    return QuotePoint(
        event_ms=event_ms,
        bid=bid,
        ask=ask,
        mid=mid,
        spread=spread,
        spread_percent=spread_percent,
        raw=quote,
    )


# =========================
# Generic object/dict helpers
# =========================
def read_attr(source: Any, key: str, default: Any = None) -> Any:
    if isinstance(source, dict):
        return source.get(key, default)
    return getattr(source, key, default)


def to_bool(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)

    raw = str(value).strip().lower()
    if raw in {"1", "true", "t", "yes", "y"}:
        return True
    if raw in {"0", "false", "f", "no", "n"}:
        return False
    return None


def required_positive_int(source: dict[str, Any], key: str, *, label: str) -> int:
    value = source.get(key)
    if value is None or value == "":
        raise ValueError(f"Missing Mongo reversal baseline value: {label}")

    try:
        parsed = int(round(float(value)))
    except (TypeError, ValueError):
        raise ValueError(f"Invalid Mongo reversal baseline integer: {label}={value!r}") from None

    if parsed <= 0:
        raise ValueError(f"Mongo reversal baseline integer must be positive: {label}={value!r}")

    return parsed


def required_non_negative_int(source: dict[str, Any], key: str, *, label: str) -> int:
    value = source.get(key)
    if value is None or value == "":
        raise ValueError(f"Missing Mongo reversal baseline value: {label}")

    try:
        parsed = int(round(float(value)))
    except (TypeError, ValueError):
        raise ValueError(f"Invalid Mongo reversal baseline integer: {label}={value!r}") from None

    if parsed < 0:
        raise ValueError(f"Mongo reversal baseline integer must be non-negative: {label}={value!r}")

    return parsed


def required_non_negative_float(source: dict[str, Any], key: str, *, label: str) -> float:
    value = source.get(key)
    if value is None or value == "":
        raise ValueError(f"Missing Mongo reversal baseline value: {label}")

    try:
        parsed = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid Mongo reversal baseline float: {label}={value!r}") from None

    if parsed < 0:
        raise ValueError(f"Mongo reversal baseline float must be non-negative: {label}={value!r}")

    return parsed


def required_bool(source: dict[str, Any], key: str, *, label: str) -> bool:
    parsed = to_bool(source.get(key))
    if parsed is None:
        raise ValueError(f"Missing or invalid Mongo reversal baseline boolean: {label}")
    return parsed


# =========================
# Position / side mapping
# =========================
def position_side_from_entry_action(entry_action: str) -> str:
    action = str(entry_action or "").upper()
    if action == "BUY":
        return "long"
    if action == "SELL":
        return "short"
    return "unknown"


def reversal_direction_for_entry_action(entry_action: str) -> tuple[str | None, int, str | None]:
    action = str(entry_action or "").upper()

    if action == "SELL":
        # Existing short is invalidated by upward quote repricing.
        return "up", 1, "BUY"

    if action == "BUY":
        # Existing long is invalidated by downward quote repricing.
        return "down", -1, "SELL"

    return None, 0, None


def mongo_reversal_side_for_position(position: Any) -> str | None:
    position_side = position_side_from_entry_action(str(getattr(position, "entry_action", "") or ""))

    if position_side == "long":
        return "long_to_short"

    if position_side == "short":
        return "short_to_long"

    return None


# =========================
# SymbolState / baseline readers
# =========================
def latest_reversal_baseline_from_state(state: Any) -> Any | None:
    latest_reversal_baseline = getattr(state, "latest_reversal_baseline", None)
    if latest_reversal_baseline is None:
        return None

    try:
        return latest_reversal_baseline()
    except Exception:
        logger.exception(
            "Failed reading latest reversal baseline from state for %s",
            getattr(state, "symbol", "unknown"),
        )
        return None


def read_recent_quotes_from_state(
    state: Any,
    *,
    lookback_ms: int,
    now_ms: int | None = None,
) -> list[dict[str, Any]]:
    recent_quotes_method = getattr(state, "recent_quotes", None)
    if recent_quotes_method is None:
        return []

    try:
        if now_ms is not None:
            quotes = recent_quotes_method(lookback_ms, now_ms=now_ms)
        else:
            quotes = recent_quotes_method(lookback_ms)
    except TypeError:
        quotes = recent_quotes_method(lookback_ms)
    except Exception:
        logger.exception(
            "Failed reading recent quotes from state for %s",
            getattr(state, "symbol", "unknown"),
        )
        return []

    return [quote for quote in quotes if isinstance(quote, dict)]


def build_quote_reversal_config_from_baseline(
    *,
    baseline: Any,
    reversal_side: str,
    threshold_group: str,
    allow_reverse: bool,
    enabled: bool = True,
) -> QuoteReversalConfig:
    symbol = str(read_attr(baseline, "symbol", "") or "").upper()
    settings = read_attr(baseline, "settings", {}) or {}
    baseline_thresholds = read_attr(baseline, "thresholds", {}) or {}
    side_thresholds = baseline_thresholds.get(reversal_side) or {}
    thresholds = side_thresholds.get(threshold_group) or {}

    if not isinstance(settings, dict):
        raise ValueError(f"Invalid Mongo reversal baseline settings for {symbol}")

    if not isinstance(thresholds, dict) or not thresholds:
        raise ValueError(
            f"Missing Mongo reversal baseline thresholds: {reversal_side}.{threshold_group}"
        )

    return QuoteReversalConfig(
        enabled=enabled,
        lookback_ms=required_positive_int(
            settings,
            "lookback_ms",
            label=f"settings.lookback_ms for {symbol}",
        ),
        minimum_ticks=required_positive_int(
            thresholds,
            "minimum_ticks",
            label=f"{reversal_side}.{threshold_group}.minimum_ticks",
        ),
        minimum_persist_ms=required_non_negative_int(
            thresholds,
            "minimum_persist_ms",
            label=f"{reversal_side}.{threshold_group}.minimum_persist_ms",
        ),
        minimum_mid_move=required_non_negative_float(
            thresholds,
            "minimum_mid_move",
            label=f"{reversal_side}.{threshold_group}.minimum_mid_move",
        ),
        minimum_mid_move_percent=required_non_negative_float(
            thresholds,
            "minimum_mid_move_percent",
            label=f"{reversal_side}.{threshold_group}.minimum_mid_move_percent",
        ),
        maximum_spread_percent=required_non_negative_float(
            thresholds,
            "maximum_spread_percent",
            label=f"{reversal_side}.{threshold_group}.maximum_spread_percent",
        ),
        require_bid_ask_mid_same_direction=required_bool(
            thresholds,
            "require_bid_ask_mid_same_direction",
            label=f"{reversal_side}.{threshold_group}.require_bid_ask_mid_same_direction",
        ),
        allow_reverse=allow_reverse,
        reversal_side=reversal_side,
        threshold_group=threshold_group,
        baseline_document_id=str(read_attr(baseline, "document_id", "") or "") or None,
        baseline_run_id=read_attr(baseline, "run_id", None),
        baseline_generated_at_utc=read_attr(baseline, "generated_at_utc", None),
        baseline_candidate_count=read_attr(baseline, "candidate_count", None),
        baseline_quality=dict(read_attr(baseline, "quality", {}) or {}),
    )


def quote_reversal_config_to_dict(config: QuoteReversalConfig | None) -> dict[str, Any] | None:
    if config is None:
        return None

    return {
        "enabled": config.enabled,
        "lookback_ms": config.lookback_ms,
        "minimum_ticks": config.minimum_ticks,
        "minimum_persist_ms": config.minimum_persist_ms,
        "minimum_mid_move": config.minimum_mid_move,
        "minimum_mid_move_percent": config.minimum_mid_move_percent,
        "maximum_spread_percent": config.maximum_spread_percent,
        "require_bid_ask_mid_same_direction": config.require_bid_ask_mid_same_direction,
        "allow_reverse": config.allow_reverse,
        "reversal_side": config.reversal_side,
        "threshold_group": config.threshold_group,
        "baseline_document_id": config.baseline_document_id,
        "baseline_run_id": config.baseline_run_id,
        "baseline_generated_at_utc": config.baseline_generated_at_utc,
        "baseline_candidate_count": config.baseline_candidate_count,
        "baseline_quality": config.baseline_quality,
    }


def empty_quote_reversal_decision(
    *,
    position: Any,
    state: Any,
    config: QuoteReversalConfig | None = None,
    failed_reasons: list[str] | None = None,
    action_mode: str | None = None,
) -> QuoteReversalDecision:
    symbol = str(getattr(position, "symbol", None) or getattr(state, "symbol", "") or "").upper()
    entry_action = str(getattr(position, "entry_action", "") or "").upper()
    position_quantity = int(getattr(position, "quantity", 0) or 0)
    position_side = position_side_from_entry_action(entry_action)
    reverse_direction, _, order_action = reversal_direction_for_entry_action(entry_action)
    config_dict = quote_reversal_config_to_dict(config) or {}

    return QuoteReversalDecision(
        symbol=symbol,
        enabled=config.enabled if config is not None else False,
        should_exit=False,
        should_reverse=False,
        close_reason=None,
        reverse_direction=reverse_direction,
        order_action=order_action,
        failed_reasons=failed_reasons or [],
        position_side=position_side,
        entry_action=entry_action,
        position_quantity=position_quantity,
        lookback_ms=int(config.lookback_ms if config is not None else 0),
        valid_quote_count=0,
        first_event_ms=None,
        last_event_ms=None,
        persist_ms=None,
        first_bid=None,
        first_ask=None,
        first_mid=None,
        last_bid=None,
        last_ask=None,
        last_mid=None,
        latest_spread=None,
        latest_spread_percent=None,
        bid_delta=None,
        ask_delta=None,
        mid_delta=None,
        directional_bid_delta=None,
        directional_ask_delta=None,
        directional_mid_delta=None,
        mid_move_percent=None,
        bid_ask_mid_same_direction=False,
        thresholds=config_dict,
        action_mode=action_mode,
        queued_action=None,
        reversal_side=config.reversal_side if config is not None else None,
        threshold_group=config.threshold_group if config is not None else None,
        baseline_document_id=config.baseline_document_id if config is not None else None,
        baseline_run_id=config.baseline_run_id if config is not None else None,
        baseline_generated_at_utc=config.baseline_generated_at_utc if config is not None else None,
        baseline_candidate_count=config.baseline_candidate_count if config is not None else None,
        baseline_quality=config.baseline_quality if config is not None else {},
    )


# =========================
# Decision builders
# =========================
def build_quote_reversal_decision(
    *,
    position: Any,
    state: Any,
    config: QuoteReversalConfig | None = None,
    now_ms: int | None = None,
) -> QuoteReversalDecision:
    """
    Low-level quote reversal evaluator.

    If config is supplied, it evaluates that config directly. If config is not
    supplied, it reads SymbolState.latest_reversal_baseline() and evaluates the
    close_thresholds for the open position side. The high-level helper
    build_mongo_quote_reversal_decision() should be preferred when you want
    REVERSE-first then CLOSE fallback behavior.
    """
    if config is None:
        baseline = latest_reversal_baseline_from_state(state)
        if baseline is None:
            return empty_quote_reversal_decision(
                position=position,
                state=state,
                failed_reasons=["missing state.latest_reversal_baseline()"],
                action_mode="CLOSE",
            )

        reversal_side = mongo_reversal_side_for_position(position)
        if reversal_side is None:
            return empty_quote_reversal_decision(
                position=position,
                state=state,
                failed_reasons=["unsupported position side for Mongo reversal baseline"],
                action_mode="CLOSE",
            )

        try:
            config = build_quote_reversal_config_from_baseline(
                baseline=baseline,
                reversal_side=reversal_side,
                threshold_group=CLOSE_THRESHOLD_GROUP,
                allow_reverse=False,
                enabled=True,
            )
        except ValueError as exc:
            return empty_quote_reversal_decision(
                position=position,
                state=state,
                failed_reasons=[str(exc)],
                action_mode="CLOSE",
            )

    symbol = str(getattr(position, "symbol", None) or getattr(state, "symbol", "") or "").upper()
    entry_action = str(getattr(position, "entry_action", "") or "").upper()
    position_quantity = int(getattr(position, "quantity", 0) or 0)
    position_side = position_side_from_entry_action(entry_action)
    reverse_direction, direction_sign, order_action = reversal_direction_for_entry_action(entry_action)
    failed_reasons: list[str] = []
    config_dict = quote_reversal_config_to_dict(config) or {}

    if not config.enabled:
        failed_reasons.append("quote reversal disabled")
        return empty_quote_reversal_decision(
            position=position,
            state=state,
            config=config,
            failed_reasons=failed_reasons,
        )

    if not symbol:
        failed_reasons.append("missing symbol")
        return empty_quote_reversal_decision(
            position=position,
            state=state,
            config=config,
            failed_reasons=failed_reasons,
        )

    if entry_action not in config.supported_entry_actions or direction_sign == 0 or order_action is None:
        failed_reasons.append(f"unsupported entry_action {entry_action!r}")
        return empty_quote_reversal_decision(
            position=position,
            state=state,
            config=config,
            failed_reasons=failed_reasons,
        )

    if position_quantity <= 0:
        failed_reasons.append("position quantity <= 0")
        return empty_quote_reversal_decision(
            position=position,
            state=state,
            config=config,
            failed_reasons=failed_reasons,
        )

    raw_quotes = read_recent_quotes_from_state(
        state,
        lookback_ms=max(1, int(config.lookback_ms)),
        now_ms=now_ms,
    )
    quote_points = [
        point
        for point in (quote_point_from_raw(quote) for quote in raw_quotes)
        if point is not None
    ]
    quote_points.sort(key=lambda point: point.event_ms)

    required_ticks = max(1, int(config.minimum_ticks))
    if len(quote_points) < required_ticks:
        failed_reasons.append(
            f"valid quote ticks {len(quote_points)} < minimum {config.minimum_ticks}"
        )
        decision = empty_quote_reversal_decision(
            position=position,
            state=state,
            config=config,
            failed_reasons=failed_reasons,
        )
        decision.valid_quote_count = len(quote_points)
        return decision

    # Use the latest N valid ticks so an old quote inside the lookback does not
    # dominate the latest reversal signal.
    selected_points = quote_points[-required_ticks:]
    first = selected_points[0]
    last = selected_points[-1]

    persist_ms = max(0, last.event_ms - first.event_ms)
    bid_delta = last.bid - first.bid
    ask_delta = last.ask - first.ask
    mid_delta = last.mid - first.mid

    directional_bid_delta = bid_delta * direction_sign
    directional_ask_delta = ask_delta * direction_sign
    directional_mid_delta = mid_delta * direction_sign

    mid_move_percent = (
        (directional_mid_delta / first.mid) * 100.0
        if first.mid > 0
        else None
    )

    bid_ask_mid_same_direction = (
        directional_bid_delta > 0
        and directional_ask_delta > 0
        and directional_mid_delta > 0
    )

    if config.minimum_persist_ms > 0 and persist_ms < config.minimum_persist_ms:
        failed_reasons.append(
            f"persist_ms {persist_ms} < minimum {config.minimum_persist_ms}"
        )

    if config.require_bid_ask_mid_same_direction and not bid_ask_mid_same_direction:
        failed_reasons.append("bid/ask/mid are not repricing together against position")

    if directional_mid_delta < config.minimum_mid_move:
        failed_reasons.append(
            f"directional_mid_delta {round(directional_mid_delta, 6)} < minimum {config.minimum_mid_move}"
        )

    if config.minimum_mid_move_percent is not None and config.minimum_mid_move_percent > 0:
        if mid_move_percent is None or mid_move_percent < config.minimum_mid_move_percent:
            failed_reasons.append(
                f"mid_move_percent {None if mid_move_percent is None else round(mid_move_percent, 6)} < minimum {config.minimum_mid_move_percent}"
            )

    if config.maximum_spread_percent is not None and config.maximum_spread_percent > 0:
        if last.spread_percent > config.maximum_spread_percent:
            failed_reasons.append(
                f"latest_spread_percent {round(last.spread_percent, 6)} > maximum {config.maximum_spread_percent}"
            )

    should_exit = len(failed_reasons) == 0
    close_reason = None

    if should_exit:
        close_reason = f"mongo_quote_reversal_{config.reversal_side}_{config.threshold_group}"

    return QuoteReversalDecision(
        symbol=symbol,
        enabled=config.enabled,
        should_exit=should_exit,
        should_reverse=bool(should_exit and config.allow_reverse),
        close_reason=close_reason,
        reverse_direction=reverse_direction,
        order_action=order_action,
        failed_reasons=failed_reasons,
        position_side=position_side,
        entry_action=entry_action,
        position_quantity=position_quantity,
        lookback_ms=config.lookback_ms,
        valid_quote_count=len(quote_points),
        first_event_ms=first.event_ms,
        last_event_ms=last.event_ms,
        persist_ms=persist_ms,
        first_bid=first.bid,
        first_ask=first.ask,
        first_mid=round(first.mid, 6),
        last_bid=last.bid,
        last_ask=last.ask,
        last_mid=round(last.mid, 6),
        latest_spread=round(last.spread, 6),
        latest_spread_percent=round(last.spread_percent, 6),
        bid_delta=round(bid_delta, 6),
        ask_delta=round(ask_delta, 6),
        mid_delta=round(mid_delta, 6),
        directional_bid_delta=round(directional_bid_delta, 6),
        directional_ask_delta=round(directional_ask_delta, 6),
        directional_mid_delta=round(directional_mid_delta, 6),
        mid_move_percent=None if mid_move_percent is None else round(mid_move_percent, 6),
        bid_ask_mid_same_direction=bid_ask_mid_same_direction,
        thresholds=config_dict,
        action_mode=None,
        queued_action="reverse" if should_exit and config.allow_reverse else "close" if should_exit else None,
        reversal_side=config.reversal_side,
        threshold_group=config.threshold_group,
        baseline_document_id=config.baseline_document_id,
        baseline_run_id=config.baseline_run_id,
        baseline_generated_at_utc=config.baseline_generated_at_utc,
        baseline_candidate_count=config.baseline_candidate_count,
        baseline_quality=config.baseline_quality,
    )


def build_mongo_quote_reversal_decision(
    *,
    position: Any,
    state: Any,
    enabled: bool = True,
    action_mode: str = "REVERSE",
    close_threshold_group: str = CLOSE_THRESHOLD_GROUP,
    reverse_threshold_group: str = REVERSE_THRESHOLD_GROUP,
    now_ms: int | None = None,
) -> QuoteReversalDecision:
    """
    High-level Mongo reversal decision.

    This is the main entry point for the live app. It reads
    state.latest_reversal_baseline(), builds thresholds from the matching
    side, tries reverse_thresholds first when action_mode == "REVERSE", then
    falls back to close_thresholds.
    """
    if not enabled:
        return empty_quote_reversal_decision(
            position=position,
            state=state,
            config=QuoteReversalConfig(enabled=False),
            failed_reasons=["Mongo quote reversal disabled"],
            action_mode=action_mode,
        )

    baseline = latest_reversal_baseline_from_state(state)
    if baseline is None:
        return empty_quote_reversal_decision(
            position=position,
            state=state,
            failed_reasons=["missing state.latest_reversal_baseline()"],
            action_mode=action_mode,
        )

    reversal_side = mongo_reversal_side_for_position(position)
    if reversal_side is None:
        return empty_quote_reversal_decision(
            position=position,
            state=state,
            failed_reasons=["unsupported position side for Mongo reversal baseline"],
            action_mode=action_mode,
        )

    failed_reasons: list[str] = []
    normalized_action_mode = str(action_mode or "CLOSE").upper()

    if normalized_action_mode == "REVERSE":
        try:
            reverse_config = build_quote_reversal_config_from_baseline(
                baseline=baseline,
                reversal_side=reversal_side,
                threshold_group=reverse_threshold_group,
                allow_reverse=True,
                enabled=enabled,
            )
        except ValueError as exc:
            failed_reasons.append(str(exc))
        else:
            reverse_decision = build_quote_reversal_decision(
                position=position,
                state=state,
                config=reverse_config,
                now_ms=now_ms,
            )
            reverse_decision.action_mode = normalized_action_mode
            reverse_decision.queued_action = "reverse" if reverse_decision.should_reverse else None

            if reverse_decision.should_exit and reverse_decision.should_reverse:
                return reverse_decision

            failed_reasons.extend(
                f"reverse_thresholds: {reason}" for reason in reverse_decision.failed_reasons
            )

    try:
        close_config = build_quote_reversal_config_from_baseline(
            baseline=baseline,
            reversal_side=reversal_side,
            threshold_group=close_threshold_group,
            allow_reverse=False,
            enabled=enabled,
        )
    except ValueError as exc:
        failed_reasons.append(str(exc))
        return empty_quote_reversal_decision(
            position=position,
            state=state,
            failed_reasons=failed_reasons,
            action_mode=normalized_action_mode,
        )

    close_decision = build_quote_reversal_decision(
        position=position,
        state=state,
        config=close_config,
        now_ms=now_ms,
    )
    close_decision.action_mode = normalized_action_mode
    close_decision.queued_action = "close" if close_decision.should_exit else None

    if not close_decision.should_exit and failed_reasons:
        close_decision.failed_reasons = failed_reasons + [
            f"close_thresholds: {reason}" for reason in close_decision.failed_reasons
        ]

    return close_decision


def quote_reversal_decision_to_dict(decision: QuoteReversalDecision) -> dict[str, Any]:
    return {
        "symbol": decision.symbol,
        "enabled": decision.enabled,
        "should_exit": decision.should_exit,
        "should_reverse": decision.should_reverse,
        "close_reason": decision.close_reason,
        "reverse_direction": decision.reverse_direction,
        "order_action": decision.order_action,
        "failed_reasons": decision.failed_reasons,
        "position_side": decision.position_side,
        "entry_action": decision.entry_action,
        "position_quantity": decision.position_quantity,
        "action_mode": decision.action_mode,
        "queued_action": decision.queued_action,
        "reversal_side": decision.reversal_side,
        "threshold_group": decision.threshold_group,
        "baseline_document_id": decision.baseline_document_id,
        "baseline_run_id": decision.baseline_run_id,
        "baseline_generated_at_utc": decision.baseline_generated_at_utc,
        "baseline_candidate_count": decision.baseline_candidate_count,
        "baseline_quality": decision.baseline_quality,
        "lookback_ms": decision.lookback_ms,
        "valid_quote_count": decision.valid_quote_count,
        "first_event_ms": decision.first_event_ms,
        "last_event_ms": decision.last_event_ms,
        "persist_ms": decision.persist_ms,
        "first_bid": decision.first_bid,
        "first_ask": decision.first_ask,
        "first_mid": decision.first_mid,
        "last_bid": decision.last_bid,
        "last_ask": decision.last_ask,
        "last_mid": decision.last_mid,
        "latest_spread": decision.latest_spread,
        "latest_spread_percent": decision.latest_spread_percent,
        "deltas": {
            "bid_delta": decision.bid_delta,
            "ask_delta": decision.ask_delta,
            "mid_delta": decision.mid_delta,
            "directional_bid_delta": decision.directional_bid_delta,
            "directional_ask_delta": decision.directional_ask_delta,
            "directional_mid_delta": decision.directional_mid_delta,
            "mid_move_percent": decision.mid_move_percent,
        },
        "bid_ask_mid_same_direction": decision.bid_ask_mid_same_direction,
        "thresholds": decision.thresholds,
    }
