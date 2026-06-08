"""
Hard trigger module for the live HFT app.

This module consumes BusyTicketRank objects from busy_ticket_module.py.
It does not recalculate live ranking metrics.
It only reads recent quotes from symbol state for:
  - latest spread percent filter
  - previous completed one-second trend confirmation

Trigger flow:
  Live rank generated
    -> apply hard trigger minimums
    -> reject if latest spread percent is above max
    -> confirm total trend over previous completed seconds
    -> return trigger candidate decision
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from pprint import pformat
from typing import Any

logger = logging.getLogger(__name__)


# =========================
# Trigger tuning variables
# =========================
MAX_SPREAD_PERCENT = 0.8
PREVIOUS_TREND_SECONDS = 3



from busy_ticket_module import (
    BusyTicketConfig,
    BusyTicketRank,
    build_ordered_busy_ticket_ranks,
)


@dataclass(slots=True)
class TriggerConfig:
    enabled: bool = True
    print_to_console: bool = True
    write_to_file: bool = False
    interval_seconds: float = 1.0
    limit: int = 5600

    # Hard filter: only allow shares whose latest quote spread is <= this % of mid.
    # Set to None or <= 0 to disable.
    maximum_spread_percent: float | None = MAX_SPREAD_PERCENT

    # Separate trigger-side confirmation window.
    # This does NOT change BusyTicketConfig.window_ms.
    # It checks the previous completed N one-second quote buckets.
    previous_trend_seconds: int = PREVIOUS_TREND_SECONDS
    require_previous_trend_confirmation: bool = True

    # By default, only print when something actually passes the hard trigger.
    print_empty_cycles: bool = False

    minimum_score: float = 150.0
    required_quote_rate_regime: set[str] = field(default_factory=lambda: {"large", "busy", "extreme"})
    allowed_trade_rate_regimes: set[str] = field(default_factory=lambda: {"large", "busy", "extreme"})
    required_volume_rate_regime: set[str] = field(default_factory=lambda: {"large", "busy", "extreme"})
    required_quote_move_regime: set[str] = field(default_factory=lambda: {"large", "busy", "extreme"})


    require_bid_ask_mid_same_direction: bool = True
    allowed_directions: set[str] = field(default_factory=lambda: {"up", "down"})


@dataclass(slots=True)
class TriggerDecision:
    symbol: str
    is_trigger_candidate: bool
    direction: str
    quality: str | None
    failed_reasons: list[str]
    score: float
    quote_rate_regime: str
    trade_rate_regime: str
    volume_rate_regime: str
    quote_move_regime: str
    quote_rate_ratio: float | None
    trade_rate_ratio: float | None
    volume_rate_ratio: float | None
    quote_move_ratio: float | None
    last_spread_percent: float | None
    previous_trend_confirmation: dict[str, Any] | None
    bid_ask_mid_same_direction: bool
    source_is_busy_ticket: bool
    rank: BusyTicketRank


def raw_float(value: Any) -> float | None:
    if value is None or value == "":
        return None

    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
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


def direction_from_delta(delta: float | None) -> str:
    if delta is None:
        return "unknown"
    if delta > 0:
        return "up"
    if delta < 0:
        return "down"
    return "flat"


def quote_event_ms(quote: dict[str, Any]) -> int | None:
    for key in ("t", "event_ms", "timestamp_ms", "ts_ms"):
        event_ms = raw_int(quote.get(key))
        if event_ms is not None and event_ms > 0:
            return event_ms

    return None


def quote_bid_ask_mid(quote: dict[str, Any]) -> tuple[float, float, float] | None:
    bid = raw_float(quote.get("bp"))
    ask = raw_float(quote.get("ap"))

    if bid is None or ask is None or bid <= 0 or ask <= 0:
        return None

    mid = (bid + ask) / 2.0
    if mid <= 0:
        return None

    return bid, ask, mid


def quote_spread_percent(quote: dict[str, Any]) -> float | None:
    values = quote_bid_ask_mid(quote)
    if values is None:
        return None

    bid, ask, mid = values
    spread = ask - bid
    if spread <= 0:
        return None

    return round((spread / mid) * 100.0, 4)


def rank_spread_percent(rank: BusyTicketRank) -> float | None:
    for attr_name in (
        "last_spread_percent",
        "spread_percent",
        "last_spread_pct",
        "spread_pct",
    ):
        value = raw_float(getattr(rank, attr_name, None))
        if value is not None:
            return value

    last_spread = raw_float(getattr(rank, "last_spread", None))
    if last_spread is None or last_spread <= 0:
        return None

    for mid_attr_name in (
        "last_mid",
        "quote_last_mid",
        "last_quote_mid",
        "latest_mid",
        "mid",
    ):
        mid = raw_float(getattr(rank, mid_attr_name, None))
        if mid is not None and mid > 0:
            return round((last_spread / mid) * 100.0, 4)

    return None


def state_reference_now_ms(state: Any) -> int | None:
    latest_trade_event_ms = raw_int(getattr(state, "latest_trade_event_ms", None)) or 0
    latest_quote_event_ms = raw_int(getattr(state, "latest_quote_event_ms", None)) or 0
    reference_now_ms = max(latest_trade_event_ms, latest_quote_event_ms)

    if reference_now_ms <= 0:
        return None

    return reference_now_ms


def read_recent_quotes_from_state(
    state: Any,
    *,
    window_ms: int,
    now_ms: int | None = None,
) -> list[dict[str, Any]]:
    recent_quotes_method = getattr(state, "recent_quotes", None)
    if recent_quotes_method is None:
        return []

    reference_now_ms = now_ms if now_ms is not None else state_reference_now_ms(state)
    if reference_now_ms is None:
        return []

    try:
        recent_quotes = recent_quotes_method(
            window_ms,
            now_ms=reference_now_ms,
        )
    except TypeError:
        recent_quotes = recent_quotes_method(window_ms)
    except Exception:
        logger.exception(
            "Failed to read recent quotes for %s",
            getattr(state, "symbol", "unknown"),
        )
        return []

    return [quote for quote in recent_quotes if isinstance(quote, dict)]


def latest_quote_spread_percent_from_state(
    state: Any,
    *,
    window_ms: int,
    now_ms: int | None = None,
) -> float | None:
    recent_quotes = read_recent_quotes_from_state(
        state,
        window_ms=window_ms,
        now_ms=now_ms,
    )

    recent_quotes.sort(key=lambda quote: quote_event_ms(quote) or 0)

    for quote in reversed(recent_quotes):
        spread_percent = quote_spread_percent(quote)
        if spread_percent is not None:
            return spread_percent

    return None


def latest_spread_percent_by_symbol(
    store: Any,
    *,
    window_ms: int,
    symbols: set[str] | None = None,
) -> dict[str, float | None]:
    result: dict[str, float | None] = {}
    requested_symbols = {symbol.upper() for symbol in symbols} if symbols else None

    for state in getattr(store, "symbols", {}).values():
        symbol = str(getattr(state, "symbol", "")).upper()
        if not symbol:
            continue

        if requested_symbols is not None and symbol not in requested_symbols:
            continue

        result[symbol] = latest_quote_spread_percent_from_state(
            state,
            window_ms=window_ms,
        )

    return result


def build_previous_trend_confirmation_from_state(
    state: Any,
    *,
    expected_direction: str,
    previous_seconds: int,
) -> dict[str, Any]:
    symbol = str(getattr(state, "symbol", "")).upper()
    previous_seconds = int(previous_seconds)

    result: dict[str, Any] = {
        "symbol": symbol,
        "enabled": previous_seconds > 0,
        "expected_direction": expected_direction,
        "previous_seconds": previous_seconds,
        "confirmed": False,
        "reason": None,
        "total": None,
        "seconds": [],
    }

    if previous_seconds <= 0:
        result["confirmed"] = True
        result["reason"] = "disabled"
        return result

    if expected_direction not in {"up", "down"}:
        result["reason"] = f"expected_direction {expected_direction!r} is not tradable"
        return result

    reference_now_ms = state_reference_now_ms(state)
    if reference_now_ms is None:
        result["reason"] = "no latest event timestamp available"
        return result

    current_second_start_ms = (reference_now_ms // 1000) * 1000
    first_previous_second_start_ms = current_second_start_ms - (previous_seconds * 1000)

    # Read one extra second of quotes so the previous completed buckets are available
    # even when the current live second has just started.
    lookback_ms = (previous_seconds + 1) * 1000
    recent_quotes = read_recent_quotes_from_state(
        state,
        window_ms=lookback_ms,
        now_ms=reference_now_ms,
    )

    buckets: dict[int, list[dict[str, Any]]] = {
        first_previous_second_start_ms + (idx * 1000): []
        for idx in range(previous_seconds)
    }

    for quote in recent_quotes:
        event_ms = quote_event_ms(quote)
        if event_ms is None:
            continue

        if event_ms < first_previous_second_start_ms or event_ms >= current_second_start_ms:
            continue

        second_start_ms = (event_ms // 1000) * 1000
        if second_start_ms in buckets:
            buckets[second_start_ms].append(quote)

    complete_seconds: list[dict[str, Any]] = []
    missing_seconds: list[int] = []

    for second_start_ms in sorted(buckets):
        quotes = buckets[second_start_ms]
        quotes.sort(key=lambda quote: quote_event_ms(quote) or 0)

        if not quotes:
            missing_seconds.append(second_start_ms)
            result["seconds"].append(
                {
                    "second_start_ms": second_start_ms,
                    "quote_count": 0,
                    "direction": "missing",
                }
            )
            continue

        first_quote = None
        last_quote = None
        first_values = None
        last_values = None

        for quote in quotes:
            values = quote_bid_ask_mid(quote)
            if values is None:
                continue

            if first_quote is None:
                first_quote = quote
                first_values = values

            last_quote = quote
            last_values = values

        if first_quote is None or last_quote is None or first_values is None or last_values is None:
            missing_seconds.append(second_start_ms)
            result["seconds"].append(
                {
                    "second_start_ms": second_start_ms,
                    "quote_count": len(quotes),
                    "direction": "missing_bid_ask",
                }
            )
            continue

        first_bid, first_ask, first_mid = first_values
        last_bid, last_ask, last_mid = last_values
        bid_delta = round(last_bid - first_bid, 6)
        ask_delta = round(last_ask - first_ask, 6)
        mid_delta = round(last_mid - first_mid, 6)
        second_direction = direction_from_delta(mid_delta)

        second_payload = {
            "second_start_ms": second_start_ms,
            "quote_count": len(quotes),
            "first_event_ms": quote_event_ms(first_quote),
            "last_event_ms": quote_event_ms(last_quote),
            "first_bid": first_bid,
            "first_ask": first_ask,
            "first_mid": round(first_mid, 6),
            "last_bid": last_bid,
            "last_ask": last_ask,
            "last_mid": round(last_mid, 6),
            "bid_delta": bid_delta,
            "ask_delta": ask_delta,
            "mid_delta": mid_delta,
            "direction": second_direction,
            "matches_expected_direction": second_direction == expected_direction,
        }

        complete_seconds.append(second_payload)
        result["seconds"].append(second_payload)

    if missing_seconds:
        result["reason"] = (
            f"only {len(complete_seconds)}/{previous_seconds} previous seconds have valid quotes"
        )
        result["missing_second_start_ms"] = missing_seconds
        return result

    if len(complete_seconds) < previous_seconds:
        result["reason"] = (
            f"only {len(complete_seconds)}/{previous_seconds} previous seconds available"
        )
        return result

    first_second = complete_seconds[0]
    last_second = complete_seconds[-1]

    total_bid_delta = round(last_second["last_bid"] - first_second["first_bid"], 6)
    total_ask_delta = round(last_second["last_ask"] - first_second["first_ask"], 6)
    total_mid_delta = round(last_second["last_mid"] - first_second["first_mid"], 6)
    total_direction = direction_from_delta(total_mid_delta)

    result["total"] = {
        "first_second_start_ms": first_second["second_start_ms"],
        "last_second_start_ms": last_second["second_start_ms"],
        "first_bid": first_second["first_bid"],
        "first_ask": first_second["first_ask"],
        "first_mid": first_second["first_mid"],
        "last_bid": last_second["last_bid"],
        "last_ask": last_second["last_ask"],
        "last_mid": last_second["last_mid"],
        "bid_delta": total_bid_delta,
        "ask_delta": total_ask_delta,
        "mid_delta": total_mid_delta,
        "direction": total_direction,
        "matches_expected_direction": total_direction == expected_direction,
    }

    if total_direction != expected_direction:
        result["reason"] = (
            f"previous {previous_seconds}s total direction {total_direction!r} "
            f"does not match trigger direction {expected_direction!r}"
        )
        return result

    result["confirmed"] = True
    result["reason"] = "confirmed"
    return result


def previous_trend_confirmation_by_symbol(
    store: Any,
    *,
    previous_seconds: int,
    direction_by_symbol: dict[str, str],
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}

    for state in getattr(store, "symbols", {}).values():
        symbol = str(getattr(state, "symbol", "")).upper()
        if not symbol or symbol not in direction_by_symbol:
            continue

        result[symbol] = build_previous_trend_confirmation_from_state(
            state,
            expected_direction=direction_by_symbol[symbol],
            previous_seconds=previous_seconds,
        )

    return result



def build_trigger_decision(
    rank: BusyTicketRank,
    *,
    config: TriggerConfig | None = None,
    last_spread_percent: float | None = None,
    previous_trend_confirmation: dict[str, Any] | None = None,
) -> TriggerDecision:
    config = config or TriggerConfig()
    failed_reasons: list[str] = []

    effective_spread_percent = (
        last_spread_percent
        if last_spread_percent is not None
        else rank_spread_percent(rank)
    )

    if rank.busy_ticket_score < config.minimum_score:
        failed_reasons.append(
            f"score {rank.busy_ticket_score} < {config.minimum_score}"
        )

    if rank.quote_rate_regime not in config.required_quote_rate_regime:
        failed_reasons.append(
            f"quote_rate_regime {rank.quote_rate_regime!r} not in {sorted(config.required_quote_rate_regime)!r}"
        )

    if rank.trade_rate_regime not in config.allowed_trade_rate_regimes:
        failed_reasons.append(
            f"trade_rate_regime {rank.trade_rate_regime!r} not in {sorted(config.allowed_trade_rate_regimes)!r}"
        )

    if rank.volume_rate_regime not in config.required_volume_rate_regime:
        failed_reasons.append(
            f"volume_rate_regime {rank.volume_rate_regime!r} not in {sorted(config.required_volume_rate_regime)!r}"
        )

    if rank.quote_move_regime not in config.required_quote_move_regime:
        failed_reasons.append(
            f"quote_move_regime {rank.quote_move_regime!r} not in {sorted(config.required_quote_move_regime)!r}"
        )


    if config.maximum_spread_percent is not None and config.maximum_spread_percent > 0:
        if effective_spread_percent is None:
            failed_reasons.append(
                f"spread_percent unavailable for max {config.maximum_spread_percent}%"
            )
        elif effective_spread_percent > config.maximum_spread_percent:
            failed_reasons.append(
                f"spread_percent {effective_spread_percent}% > {config.maximum_spread_percent}%"
            )

    if config.require_previous_trend_confirmation and config.previous_trend_seconds > 0:
        if previous_trend_confirmation is None:
            failed_reasons.append(
                f"previous_{config.previous_trend_seconds}s_trend unavailable"
            )
        elif not previous_trend_confirmation.get("confirmed", False):
            reason = previous_trend_confirmation.get("reason") or "not confirmed"
            failed_reasons.append(
                f"previous_{config.previous_trend_seconds}s_trend not confirmed: {reason}"
            )

    if config.require_bid_ask_mid_same_direction and not rank.bid_ask_mid_same_direction:
        failed_reasons.append("bid/ask/mid not same direction")

    if rank.direction not in config.allowed_directions:
        failed_reasons.append(
            f"direction {rank.direction!r} not in {sorted(config.allowed_directions)!r}"
        )

    is_trigger_candidate = len(failed_reasons) == 0

    return TriggerDecision(
        symbol=rank.symbol,
        is_trigger_candidate=is_trigger_candidate,
        direction=rank.direction,
        quality=rank.baseline_confidence,
        failed_reasons=failed_reasons,
        score=rank.busy_ticket_score,
        quote_rate_regime=rank.quote_rate_regime,
        trade_rate_regime=rank.trade_rate_regime,
        volume_rate_regime=rank.volume_rate_regime,
        quote_move_regime=rank.quote_move_regime,
        quote_rate_ratio=rank.quote_rate_ratio,
        trade_rate_ratio=rank.trade_rate_ratio,
        volume_rate_ratio=rank.volume_rate_ratio,
        quote_move_ratio=rank.quote_move_ratio,
        last_spread_percent=effective_spread_percent,
        previous_trend_confirmation=previous_trend_confirmation,
        bid_ask_mid_same_direction=rank.bid_ask_mid_same_direction,
        source_is_busy_ticket=rank.is_busy_ticket,
        rank=rank,
    )


def trigger_decision_to_dict(decision: TriggerDecision) -> dict[str, Any]:
    rank = decision.rank

    return {
        "symbol": decision.symbol,
        "is_trigger_candidate": decision.is_trigger_candidate,
        "direction": decision.direction,
        "quality": decision.quality,
        "failed_reasons": decision.failed_reasons,
        "score": decision.score,
        "source_is_busy_ticket": decision.source_is_busy_ticket,
        "regimes": {
            "quote_rate": decision.quote_rate_regime,
            "trade_rate": decision.trade_rate_regime,
            "volume_rate": decision.volume_rate_regime,
            "quote_move_speed": decision.quote_move_regime,
            "spread": rank.spread_regime,
        },
        "ratios_vs_normal": {
            "quote_rate": decision.quote_rate_ratio,
            "trade_rate": decision.trade_rate_ratio,
            "volume_rate": decision.volume_rate_ratio,
            "quote_move_speed": decision.quote_move_ratio,
            "spread": rank.spread_ratio,
        },
        "rates": {
            "quotes_per_second": rank.quotes_per_second,
            "trades_per_second": rank.trades_per_second,
            "volume_per_second": rank.volume_per_second,
            "movement_condition_trades_per_second": rank.movement_condition_trades_per_second,
        },
        "quote_repricing": {
            "mid_delta": rank.quote_mid_delta,
            "abs_mid_delta": rank.quote_abs_mid_delta,
            "mid_move_speed_per_second": rank.quote_mid_move_speed_per_second,
            "bid_delta": rank.quote_bid_delta,
            "ask_delta": rank.quote_ask_delta,
            "bid_ask_mid_same_direction": decision.bid_ask_mid_same_direction,
        },
        "aggregate_move": {
            "direction": rank.aggregate_direction,
            "confirms_quote_direction": rank.aggregate_confirms_quote_direction,
            "close_delta": rank.aggregate_close_delta,
            "abs_close_delta": rank.aggregate_abs_close_delta,
        },
        "spread": {
            "first_spread": rank.first_spread,
            "last_spread": rank.last_spread,
            "last_spread_percent": decision.last_spread_percent,
            "spread_delta": rank.spread_delta,
            "spread_widened": rank.spread_widened,
        },
        "previous_trend_confirmation": decision.previous_trend_confirmation,
        "conditions": {
            "movement_condition_trade_count": rank.movement_condition_trade_count,
            "condition_counts": rank.condition_counts,
        },
        "baseline_confidence": rank.baseline_confidence,
    }


def build_trigger_decisions(
    ranks: list[BusyTicketRank],
    *,
    config: TriggerConfig | None = None,
    spread_percent_by_symbol: dict[str, float | None] | None = None,
    previous_trend_by_symbol: dict[str, dict[str, Any]] | None = None,
) -> list[TriggerDecision]:
    config = config or TriggerConfig()
    spread_percent_by_symbol = spread_percent_by_symbol or {}
    previous_trend_by_symbol = previous_trend_by_symbol or {}

    return [
        build_trigger_decision(
            rank,
            config=config,
            last_spread_percent=spread_percent_by_symbol.get(rank.symbol.upper()),
            previous_trend_confirmation=previous_trend_by_symbol.get(rank.symbol.upper()),
        )
        for rank in ranks
    ]


def build_trigger_candidates(
    ranks: list[BusyTicketRank],
    *,
    config: TriggerConfig | None = None,
    spread_percent_by_symbol: dict[str, float | None] | None = None,
    previous_trend_by_symbol: dict[str, dict[str, Any]] | None = None,
) -> list[TriggerDecision]:
    decisions = build_trigger_decisions(
        ranks,
        config=config,
        spread_percent_by_symbol=spread_percent_by_symbol,
        previous_trend_by_symbol=previous_trend_by_symbol,
    )
    return [decision for decision in decisions if decision.is_trigger_candidate]


def trigger_minimums_to_dict(config: TriggerConfig) -> dict[str, Any]:
    return {
        "minimum_score": config.minimum_score,
        "required_quote_rate_regime": sorted(config.required_quote_rate_regime),
        "allowed_trade_rate_regimes": sorted(config.allowed_trade_rate_regimes),
        "required_volume_rate_regime": sorted(config.required_volume_rate_regime),
        "required_quote_move_regime": sorted(config.required_quote_move_regime),
        "maximum_spread_percent": config.maximum_spread_percent,
        "previous_trend_seconds": config.previous_trend_seconds,
        "require_previous_trend_confirmation": config.require_previous_trend_confirmation,
        "require_bid_ask_mid_same_direction": config.require_bid_ask_mid_same_direction,
        "allowed_directions": sorted(config.allowed_directions),
    }


async def print_trigger_candidate_loop(
    store: Any,
    *,
    append_output_to_file: Callable[[str], Any] | None = None,
    busy_ticket_config: BusyTicketConfig | None = None,
    trigger_config: TriggerConfig | None = None,
    on_candidates: Callable[[list[TriggerDecision], dict[str, Any]], Any] | None = None,
) -> None:
    busy_ticket_config = busy_ticket_config or BusyTicketConfig()
    trigger_config = trigger_config or TriggerConfig()

    while True:
        await asyncio.sleep(trigger_config.interval_seconds)

        if not trigger_config.enabled:
            continue

        ranks = build_ordered_busy_ticket_ranks(
            store,
            config=busy_ticket_config,
        )

        if trigger_config.limit > 0:
            ranks = ranks[: trigger_config.limit]

        rank_symbols = {rank.symbol.upper() for rank in ranks}
        direction_by_symbol = {
            rank.symbol.upper(): rank.direction
            for rank in ranks
        }

        spread_lookback_ms = max(
            int((trigger_config.previous_trend_seconds + 1) * 1000),
            1000,
        )

        spread_percent_by_symbol = latest_spread_percent_by_symbol(
            store,
            window_ms=spread_lookback_ms,
            symbols=rank_symbols,
        )

        previous_trend_by_symbol = previous_trend_confirmation_by_symbol(
            store,
            previous_seconds=trigger_config.previous_trend_seconds,
            direction_by_symbol=direction_by_symbol,
        )

        candidates = build_trigger_candidates(
            ranks,
            config=trigger_config,
            spread_percent_by_symbol=spread_percent_by_symbol,
            previous_trend_by_symbol=previous_trend_by_symbol,
        )

        if not candidates and not trigger_config.print_empty_cycles:
            continue

        payload = {
            "trigger_candidates": {
                "minimums": trigger_minimums_to_dict(trigger_config),
                "rank_window_ms": busy_ticket_config.window_ms,
                "rank_limit": busy_ticket_config.limit,
                "candidate_count": len(candidates),
                "candidates": [
                    trigger_decision_to_dict(candidate)
                    for candidate in candidates
                ],
            }
        }

        output = (
            "\n========== TRIGGER CANDIDATES ==========\n"
            f"{pformat(payload, sort_dicts=False)}\n"
            "========================================\n"
        )

        if trigger_config.print_to_console:
            print(output, flush=True)

        if trigger_config.write_to_file and append_output_to_file is not None:
            append_output_to_file(output)

        if on_candidates is not None and candidates:
            try:
                result = on_candidates(candidates, payload)
                if asyncio.iscoroutine(result):
                    await result
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Trigger candidate callback failed")
