"""
Busy-ticket ranking module for the live HFT app.

Uses raw Massive short keys only. No long-name fallbacks are used.

Expected live event fields:
  T: ev, sym, p, s, c, t, q, ...
  Q: ev, sym, bp, ap, bs, as, c, i, t, q, ...
  A: ev, sym, c, h, l, s, e, ...

Expected state object:
  - symbol
  - latest_trade_event_ms
  - latest_quote_event_ms
  - recent_trades(window_ms, now_ms=...)
  - recent_quotes(window_ms, now_ms=...)
  - recent_aggregates(count)
  - latest_baseline()

Expected store object:
  - symbols: dict[str, SymbolState-like]
"""

from __future__ import annotations

import asyncio
import ast
from collections.abc import Callable
from dataclasses import dataclass, field
from pprint import pformat
from typing import Any


BUSY_TICKET_REGIME_ORDER = {
    "none": 0,
    "normal": 1,
    "large": 2,
    "busy": 3,
    "extreme": 4,
}


@dataclass(slots=True)
class BusyTicketConfig:
    enabled: bool = True
    print_to_console: bool = False
    write_to_file: bool = False
    interval_seconds: float = 1
    limit: int = 5600
    window_ms: int = 1_000
    movement_condition_codes: set[int] = field(default_factory=lambda: {10, 14, 41})
    minimum_quotes_for_direction: int = 2

    # Keep this low so you still see the ranked tape while tuning.
    # is_busy_ticket is shown in the output; the list is still ranked by pressure.
    minimum_busy_ticket_score: float = 25.0

    same_direction_reprice_bonus: float = 3.0
    spread_widened_bonus: float = 1.0
    aggregate_confirmation_bonus: float = 1.0
    condition_trade_bonus: float = 0.5


@dataclass(slots=True)
class QuoteMove:
    quote_count: int
    valid_quote_count: int
    first_quote_ms: int | None
    last_quote_ms: int | None
    elapsed_seconds: float | None
    first_bid: float | None
    first_ask: float | None
    first_mid: float | None
    first_spread: float | None
    last_bid: float | None
    last_ask: float | None
    last_mid: float | None
    last_spread: float | None
    bid_delta: float | None
    ask_delta: float | None
    mid_delta: float | None
    abs_mid_delta: float | None
    spread_delta: float | None
    spread_widened: bool
    direction: str
    bid_ask_mid_same_direction: bool
    mid_move_speed_per_second: float | None


@dataclass(slots=True)
class AggregateMove:
    close_delta: float | None
    abs_close_delta: float | None
    direction: str


@dataclass(slots=True)
class BusyTicketRank:
    symbol: str
    busy_ticket_score: float
    is_busy_ticket: bool
    direction: str
    aggregate_direction: str
    aggregate_confirms_quote_direction: bool
    quotes_per_second: float
    trades_per_second: float
    volume_per_second: float
    quote_rate_regime: str
    trade_rate_regime: str
    volume_rate_regime: str
    quote_move_regime: str
    spread_regime: str
    quote_rate_ratio: float | None
    trade_rate_ratio: float | None
    volume_rate_ratio: float | None
    quote_move_ratio: float | None
    spread_ratio: float | None
    quote_mid_delta: float | None
    quote_abs_mid_delta: float | None
    quote_mid_move_speed_per_second: float | None
    quote_bid_delta: float | None
    quote_ask_delta: float | None
    bid_ask_mid_same_direction: bool
    first_spread: float | None
    last_spread: float | None
    spread_delta: float | None
    spread_widened: bool
    condition_counts: dict[str, int]
    movement_condition_trade_count: int
    movement_condition_trades_per_second: float
    aggregate_close_delta: float | None
    aggregate_abs_close_delta: float | None
    baseline_confidence: str | None
    sort_key: tuple[Any, ...]


def raw_int(value: Any) -> int | None:
    if value is None or value == "":
        return None

    if isinstance(value, bool):
        return int(value)

    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def raw_float(value: Any) -> float | None:
    if value is None or value == "":
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_int_listish(value: Any) -> list[int]:
    if value is None or value == "":
        return []

    if isinstance(value, (list, tuple, set)):
        result: list[int] = []
        for item in value:
            try:
                result.append(int(item))
            except (TypeError, ValueError):
                continue
        return result

    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")

    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []

        try:
            parsed = ast.literal_eval(raw)
        except (ValueError, SyntaxError):
            parsed = None

        if isinstance(parsed, (list, tuple, set)):
            result: list[int] = []
            for item in parsed:
                try:
                    result.append(int(item))
                except (TypeError, ValueError):
                    continue
            return result

        if parsed is not None:
            try:
                return [int(parsed)]
            except (TypeError, ValueError):
                pass

        if "," in raw:
            result: list[int] = []
            for part in raw.split(","):
                part = part.strip()
                if not part:
                    continue
                try:
                    result.append(int(part))
                except (TypeError, ValueError):
                    continue
            return result

        try:
            return [int(raw)]
        except (TypeError, ValueError):
            return []

    try:
        return [int(value)]
    except (TypeError, ValueError):
        return []


def event_time_ms(item: dict[str, Any]) -> int | None:
    timestamp = raw_int(item.get("t"))
    if timestamp is not None:
        # Main app already stores milliseconds, but this keeps ns/us payloads sane.
        if timestamp > 10_000_000_000_000_000:
            return timestamp // 1_000_000
        if timestamp > 10_000_000_000_000:
            return timestamp // 1_000
        return timestamp

    end_timestamp = raw_int(item.get("e"))
    if end_timestamp is not None:
        if end_timestamp > 10_000_000_000_000_000:
            return end_timestamp // 1_000_000
        if end_timestamp > 10_000_000_000_000:
            return end_timestamp // 1_000
        return end_timestamp

    start_timestamp = raw_int(item.get("s"))
    if start_timestamp is not None:
        if start_timestamp > 10_000_000_000_000_000:
            return start_timestamp // 1_000_000
        if start_timestamp > 10_000_000_000_000:
            return start_timestamp // 1_000
        return start_timestamp

    return None


def safe_ratio(numerator: Any, denominator: Any) -> float | None:
    numerator_float = raw_float(numerator)
    denominator_float = raw_float(denominator)

    if numerator_float is None or denominator_float is None or denominator_float <= 0:
        return None

    return round(numerator_float / denominator_float, 4)


def regime_metric_value(
    baseline: Any,
    regime_name: str,
    metric_name: str,
) -> float | None:
    regimes = getattr(baseline, "regimes", {}) or {}
    regime = regimes.get(regime_name) or {}
    return raw_float(regime.get(metric_name))


def normal_metric_value(baseline: Any, metric_name: str) -> float | None:
    return regime_metric_value(baseline, "normal", metric_name)


def classify_against_baseline(
    *,
    current_value: float | None,
    baseline: Any,
    metric_name: str,
) -> str:
    if current_value is None:
        return "none"

    extreme_value = regime_metric_value(baseline, "extreme", metric_name)
    busy_value = regime_metric_value(baseline, "busy", metric_name)
    large_value = regime_metric_value(baseline, "large", metric_name)
    normal_value = regime_metric_value(baseline, "normal", metric_name)

    if extreme_value is not None and current_value >= extreme_value:
        return "extreme"

    if busy_value is not None and current_value >= busy_value:
        return "busy"

    if large_value is not None and current_value >= large_value:
        return "large"

    if normal_value is not None and current_value >= normal_value:
        return "normal"

    return "none"


def quote_prices(quote: dict[str, Any]) -> tuple[float, float, float, float] | None:
    bid = raw_float(quote.get("bp"))
    ask = raw_float(quote.get("ap"))

    if bid is None or ask is None:
        return None

    spread = ask - bid
    if spread <= 0:
        return None

    mid = (bid + ask) / 2.0
    return bid, ask, mid, spread


def quote_direction_from_delta(delta: float | None) -> str:
    if delta is None:
        return "unknown"
    if delta > 0:
        return "up"
    if delta < 0:
        return "down"
    return "flat"


def build_quote_move(
    quotes: list[dict[str, Any]],
    *,
    minimum_quotes_for_direction: int,
) -> QuoteMove:
    valid: list[tuple[dict[str, Any], int | None, float, float, float, float]] = []

    for quote in quotes:
        prices = quote_prices(quote)
        if prices is None:
            continue

        bid, ask, mid, spread = prices
        valid.append((quote, event_time_ms(quote), bid, ask, mid, spread))

    if len(valid) < minimum_quotes_for_direction:
        return QuoteMove(
            quote_count=len(quotes),
            valid_quote_count=len(valid),
            first_quote_ms=None,
            last_quote_ms=None,
            elapsed_seconds=None,
            first_bid=None,
            first_ask=None,
            first_mid=None,
            first_spread=None,
            last_bid=None,
            last_ask=None,
            last_mid=None,
            last_spread=None,
            bid_delta=None,
            ask_delta=None,
            mid_delta=None,
            abs_mid_delta=None,
            spread_delta=None,
            spread_widened=False,
            direction="unknown",
            bid_ask_mid_same_direction=False,
            mid_move_speed_per_second=None,
        )

    _, first_ms, first_bid, first_ask, first_mid, first_spread = valid[0]
    _, last_ms, last_bid, last_ask, last_mid, last_spread = valid[-1]

    bid_delta = last_bid - first_bid
    ask_delta = last_ask - first_ask
    mid_delta = last_mid - first_mid
    abs_mid_delta = abs(mid_delta)
    spread_delta = last_spread - first_spread
    spread_widened = spread_delta > 0
    direction = quote_direction_from_delta(mid_delta)

    same_direction_up = bid_delta > 0 and ask_delta > 0 and mid_delta > 0
    same_direction_down = bid_delta < 0 and ask_delta < 0 and mid_delta < 0
    bid_ask_mid_same_direction = same_direction_up or same_direction_down

    elapsed_seconds: float | None = None
    mid_move_speed_per_second: float | None = None

    if first_ms is not None and last_ms is not None:
        elapsed_seconds = max((last_ms - first_ms) / 1000.0, 0.001)
        mid_move_speed_per_second = abs_mid_delta / elapsed_seconds

    return QuoteMove(
        quote_count=len(quotes),
        valid_quote_count=len(valid),
        first_quote_ms=first_ms,
        last_quote_ms=last_ms,
        elapsed_seconds=elapsed_seconds,
        first_bid=round(first_bid, 6),
        first_ask=round(first_ask, 6),
        first_mid=round(first_mid, 6),
        first_spread=round(first_spread, 6),
        last_bid=round(last_bid, 6),
        last_ask=round(last_ask, 6),
        last_mid=round(last_mid, 6),
        last_spread=round(last_spread, 6),
        bid_delta=round(bid_delta, 6),
        ask_delta=round(ask_delta, 6),
        mid_delta=round(mid_delta, 6),
        abs_mid_delta=round(abs_mid_delta, 6),
        spread_delta=round(spread_delta, 6),
        spread_widened=spread_widened,
        direction=direction,
        bid_ask_mid_same_direction=bid_ask_mid_same_direction,
        mid_move_speed_per_second=(
            round(mid_move_speed_per_second, 6)
            if mid_move_speed_per_second is not None
            else None
        ),
    )


def build_aggregate_move(aggregates: list[dict[str, Any]]) -> AggregateMove:
    if len(aggregates) < 2:
        return AggregateMove(
            close_delta=None,
            abs_close_delta=None,
            direction="unknown",
        )

    previous_close = raw_float(aggregates[-2].get("c"))
    latest_close = raw_float(aggregates[-1].get("c"))

    if previous_close is None or latest_close is None:
        return AggregateMove(
            close_delta=None,
            abs_close_delta=None,
            direction="unknown",
        )

    close_delta = latest_close - previous_close
    return AggregateMove(
        close_delta=round(close_delta, 6),
        abs_close_delta=round(abs(close_delta), 6),
        direction=quote_direction_from_delta(close_delta),
    )


def count_movement_conditions(
    trades: list[dict[str, Any]],
    movement_condition_codes: set[int],
) -> tuple[dict[str, int], int]:
    counts = {str(code): 0 for code in sorted(movement_condition_codes)}
    movement_trade_count = 0

    for trade in trades:
        codes = set(parse_int_listish(trade.get("c")))
        matched = codes.intersection(movement_condition_codes)
        if not matched:
            continue

        movement_trade_count += 1
        for code in matched:
            counts[str(code)] += 1

    return counts, movement_trade_count


def rounded_ratio(current_value: float | None, baseline_value: float | None) -> float | None:
    return safe_ratio(current_value, baseline_value)


def build_busy_ticket_rank(
    state: Any,
    *,
    now_ms: int | None = None,
    config: BusyTicketConfig | None = None,
) -> BusyTicketRank | None:
    config = config or BusyTicketConfig()
    baseline = state.latest_baseline()
    if baseline is None:
        return None

    reference_now_ms = now_ms if now_ms is not None else max(
        getattr(state, "latest_trade_event_ms", 0) or 0,
        getattr(state, "latest_quote_event_ms", 0) or 0,
    )

    if reference_now_ms <= 0:
        return None

    recent_trades = state.recent_trades(config.window_ms, now_ms=reference_now_ms)
    recent_quotes = state.recent_quotes(config.window_ms, now_ms=reference_now_ms)
    recent_aggregates = state.recent_aggregates(2)

    seconds = max(config.window_ms / 1000.0, 0.001)

    quotes_per_second = len(recent_quotes) / seconds
    trades_per_second = len(recent_trades) / seconds
    volume_per_second = (
        sum(raw_float(trade.get("s")) or 0.0 for trade in recent_trades)
        / seconds
    )

    quote_move = build_quote_move(
        recent_quotes,
        minimum_quotes_for_direction=config.minimum_quotes_for_direction,
    )
    aggregate_move = build_aggregate_move(recent_aggregates)
    condition_counts, movement_condition_trade_count = count_movement_conditions(
        recent_trades,
        config.movement_condition_codes,
    )
    movement_condition_trades_per_second = movement_condition_trade_count / seconds

    quote_rate_regime = classify_against_baseline(
        current_value=quotes_per_second,
        baseline=baseline,
        metric_name="median_quote_update_count",
    )
    trade_rate_regime = classify_against_baseline(
        current_value=trades_per_second,
        baseline=baseline,
        metric_name="median_trade_count",
    )
    volume_rate_regime = classify_against_baseline(
        current_value=volume_per_second,
        baseline=baseline,
        metric_name="median_share_volume",
    )
    quote_move_regime = classify_against_baseline(
        current_value=quote_move.mid_move_speed_per_second,
        baseline=baseline,
        metric_name="median_abs_last_mid_delta",
    )
    spread_regime = classify_against_baseline(
        current_value=quote_move.last_spread,
        baseline=baseline,
        metric_name="median_last_spread",
    )

    quote_rate_ratio = rounded_ratio(
        quotes_per_second,
        normal_metric_value(baseline, "median_quote_update_count"),
    )
    trade_rate_ratio = rounded_ratio(
        trades_per_second,
        normal_metric_value(baseline, "median_trade_count"),
    )
    volume_rate_ratio = rounded_ratio(
        volume_per_second,
        normal_metric_value(baseline, "median_share_volume"),
    )
    quote_move_ratio = rounded_ratio(
        quote_move.mid_move_speed_per_second,
        normal_metric_value(baseline, "median_abs_last_mid_delta"),
    )
    spread_ratio = rounded_ratio(
        quote_move.last_spread,
        normal_metric_value(baseline, "median_last_spread"),
    )

    aggregate_confirms_quote_direction = (
        quote_move.direction in {"up", "down"}
        and aggregate_move.direction == quote_move.direction
    )

    score_parts = [
        quote_rate_ratio or 0.0,
        trade_rate_ratio or 0.0,
        volume_rate_ratio or 0.0,
        quote_move_ratio or 0.0,
        spread_ratio or 0.0,
    ]

    busy_ticket_score = sum(score_parts)

    if quote_move.bid_ask_mid_same_direction:
        busy_ticket_score += config.same_direction_reprice_bonus

    if quote_move.spread_widened:
        busy_ticket_score += config.spread_widened_bonus

    if aggregate_confirms_quote_direction:
        busy_ticket_score += config.aggregate_confirmation_bonus

    busy_ticket_score += (
        movement_condition_trade_count * config.condition_trade_bonus
    )

    direction_is_usable = quote_move.direction in {"up", "down"}
    is_busy_ticket = (
        busy_ticket_score >= config.minimum_busy_ticket_score
        and direction_is_usable
        and quote_move.bid_ask_mid_same_direction
    )

    baseline_quality = getattr(baseline, "quality", {}) or {}
    baseline_confidence = baseline_quality.get("confidence")

    sort_key = (
        is_busy_ticket,
        BUSY_TICKET_REGIME_ORDER.get(quote_move_regime, 0),
        BUSY_TICKET_REGIME_ORDER.get(quote_rate_regime, 0),
        BUSY_TICKET_REGIME_ORDER.get(trade_rate_regime, 0),
        BUSY_TICKET_REGIME_ORDER.get(volume_rate_regime, 0),
        quote_move.bid_ask_mid_same_direction,
        quote_move.spread_widened,
        aggregate_confirms_quote_direction,
        movement_condition_trade_count,
        busy_ticket_score,
        quote_move_ratio or 0.0,
        quote_rate_ratio or 0.0,
        trade_rate_ratio or 0.0,
        volume_rate_ratio or 0.0,
        spread_ratio or 0.0,
    )

    return BusyTicketRank(
        symbol=str(getattr(state, "symbol", "")).upper(),
        busy_ticket_score=round(busy_ticket_score, 4),
        is_busy_ticket=is_busy_ticket,
        direction=quote_move.direction,
        aggregate_direction=aggregate_move.direction,
        aggregate_confirms_quote_direction=aggregate_confirms_quote_direction,
        quotes_per_second=round(quotes_per_second, 4),
        trades_per_second=round(trades_per_second, 4),
        volume_per_second=round(volume_per_second, 4),
        quote_rate_regime=quote_rate_regime,
        trade_rate_regime=trade_rate_regime,
        volume_rate_regime=volume_rate_regime,
        quote_move_regime=quote_move_regime,
        spread_regime=spread_regime,
        quote_rate_ratio=quote_rate_ratio,
        trade_rate_ratio=trade_rate_ratio,
        volume_rate_ratio=volume_rate_ratio,
        quote_move_ratio=quote_move_ratio,
        spread_ratio=spread_ratio,
        quote_mid_delta=quote_move.mid_delta,
        quote_abs_mid_delta=quote_move.abs_mid_delta,
        quote_mid_move_speed_per_second=quote_move.mid_move_speed_per_second,
        quote_bid_delta=quote_move.bid_delta,
        quote_ask_delta=quote_move.ask_delta,
        bid_ask_mid_same_direction=quote_move.bid_ask_mid_same_direction,
        first_spread=quote_move.first_spread,
        last_spread=quote_move.last_spread,
        spread_delta=quote_move.spread_delta,
        spread_widened=quote_move.spread_widened,
        condition_counts=condition_counts,
        movement_condition_trade_count=movement_condition_trade_count,
        movement_condition_trades_per_second=round(
            movement_condition_trades_per_second,
            4,
        ),
        aggregate_close_delta=aggregate_move.close_delta,
        aggregate_abs_close_delta=aggregate_move.abs_close_delta,
        baseline_confidence=baseline_confidence,
        sort_key=sort_key,
    )


def busy_ticket_rank_to_dict(rank: BusyTicketRank) -> dict[str, Any]:
    return {
        "symbol": rank.symbol,
        "busy_ticket_score": rank.busy_ticket_score,
        "is_busy_ticket": rank.is_busy_ticket,
        "direction": rank.direction,
        "aggregate_direction": rank.aggregate_direction,
        "aggregate_confirms_quote_direction": rank.aggregate_confirms_quote_direction,
        "rates": {
            "quotes_per_second": rank.quotes_per_second,
            "trades_per_second": rank.trades_per_second,
            "volume_per_second": rank.volume_per_second,
            "movement_condition_trades_per_second": rank.movement_condition_trades_per_second,
        },
        "regimes": {
            "quote_rate": rank.quote_rate_regime,
            "trade_rate": rank.trade_rate_regime,
            "volume_rate": rank.volume_rate_regime,
            "quote_move_speed": rank.quote_move_regime,
            "spread": rank.spread_regime,
        },
        "ratios_vs_normal": {
            "quote_rate": rank.quote_rate_ratio,
            "trade_rate": rank.trade_rate_ratio,
            "volume_rate": rank.volume_rate_ratio,
            "quote_move_speed": rank.quote_move_ratio,
            "spread": rank.spread_ratio,
        },
        "quote_repricing": {
            "mid_delta": rank.quote_mid_delta,
            "abs_mid_delta": rank.quote_abs_mid_delta,
            "mid_move_speed_per_second": rank.quote_mid_move_speed_per_second,
            "bid_delta": rank.quote_bid_delta,
            "ask_delta": rank.quote_ask_delta,
            "bid_ask_mid_same_direction": rank.bid_ask_mid_same_direction,
        },
        "spread": {
            "first_spread": rank.first_spread,
            "last_spread": rank.last_spread,
            "spread_delta": rank.spread_delta,
            "spread_widened": rank.spread_widened,
        },
        "conditions": {
            "movement_condition_trade_count": rank.movement_condition_trade_count,
            "condition_counts": rank.condition_counts,
        },
        "aggregate_move": {
            "close_delta": rank.aggregate_close_delta,
            "abs_close_delta": rank.aggregate_abs_close_delta,
        },
        "baseline_confidence": rank.baseline_confidence,
    }


def build_ordered_busy_ticket_ranks(
    store: Any,
    *,
    now_ms: int | None = None,
    config: BusyTicketConfig | None = None,
) -> list[BusyTicketRank]:
    config = config or BusyTicketConfig()
    ranks: list[BusyTicketRank] = []

    for state in getattr(store, "symbols", {}).values():
        rank = build_busy_ticket_rank(
            state,
            now_ms=now_ms,
            config=config,
        )

        if rank is not None:
            ranks.append(rank)

    ranks.sort(key=lambda item: item.sort_key, reverse=True)

    if config.limit > 0:
        return ranks[: config.limit]

    return ranks


async def print_busy_ticket_loop(
    store: Any,
    *,
    append_output_to_file: Callable[[str], Any] | None = None,
    config: BusyTicketConfig | None = None,
) -> None:
    config = config or BusyTicketConfig()

    while True:
        await asyncio.sleep(config.interval_seconds)

        if not config.enabled:
            continue

        ranks = build_ordered_busy_ticket_ranks(
            store,
            config=config,
        )

        payload = {
            "busy_tickets": {
                "ordering": [
                    "is_busy_ticket",
                    "quote_move_regime",
                    "quote_rate_regime",
                    "trade_rate_regime",
                    "volume_rate_regime",
                    "bid_ask_mid_same_direction",
                    "spread_widened",
                    "aggregate_confirms_quote_direction",
                    "movement_condition_trade_count",
                    "busy_ticket_score",
                    "quote_move_ratio",
                    "quote_rate_ratio",
                    "trade_rate_ratio",
                    "volume_rate_ratio",
                    "spread_ratio",
                ],
                "window_ms": config.window_ms,
                "limit": config.limit,
                "movement_condition_codes": sorted(config.movement_condition_codes),
                "ranked_symbols": [
                    busy_ticket_rank_to_dict(rank)
                    for rank in ranks
                ],
            }
        }

        output = (
            "\n========== BUSY TICKETS ==========\n"
            f"{pformat(payload, sort_dicts=False)}\n"
            "==================================\n"
        )

        if config.print_to_console:
            print(output, flush=True)

        if config.write_to_file and append_output_to_file is not None:
            append_output_to_file(output)