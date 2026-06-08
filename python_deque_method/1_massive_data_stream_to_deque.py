import asyncio
import ast
import os
from collections import deque
from dataclasses import dataclass, field
from pprint import pformat
from typing import Any


from massive_direct_stream import DirectMassiveStreamConfig, MassiveDirectDequeStreamer, RawMarketEvent

import massive_conditions as massive_conditions_module
from logging_utils import (
    append_output_to_file,
    configure_logging,
)
from busy_ticket_module import (
    BusyTicketConfig,
    print_busy_ticket_loop,
)
from trigger_module import (
    TriggerConfig,
    print_trigger_candidate_loop,
)

from time_utils import (
    format_market_ms,
    normalize_timestamp_to_ms,
    utc_now,
    utc_now_ms,
)
from mongo_baseline_module import (
    BaselineSnapshot,
    MAX_BASELINES_PER_SYMBOL,
    MAX_REVERSAL_BASELINES_PER_SYMBOL,
    ReversalBaselineSnapshot,
    load_mongo_baselines_into_market_state,
)


# =========================
# Direct Massive stream variables
# =========================
# Redis has been removed from live intake.
# Configure the imported MassiveDirectDequeStreamer with:
MASSIVE_API_KEY = os.getenv("MASSIVE_API_KEY", "replace_me")
#   SUBSCRIPTION_MODE=mongo|wildcard|explicit
#   MASSIVE_RAW_QUEUE_MAXSIZE
#   MASSIVE_EVENT_QUEUE_MAXSIZE
#   ENABLE_MASSIVE_QUEUE_METRICS

# =========================
MAX_AGGREGATES_PER_SYMBOL = 150
TRADE_RETENTION_MS = 150_000
QUOTE_RETENTION_MS = 150_000


# =========================
# Debug / diagnostics
# =========================
ENABLE_SYMBOL_STATE_DEBUG_PRINTING = True

# 0 = print every Massive event after it has been added to SymbolState.
# Use 10 later if you want one print per symbol every 10 seconds.
SYMBOL_STATE_DEBUG_PRINT_INTERVAL_SECONDS = 1

ENABLE_SYMBOL_STATE_DEBUG_PRINT_TO_CONSOLE = True
ENABLE_SYMBOL_STATE_DEBUG_WRITE_TO_FILE = True

# Empty set = print all symbols. Example: {"AAPL", "TSLA"} to filter.   ( To filter to single "set[str] = {"AAPL"}" #  )
SYMBOL_STATE_DEBUG_SYMBOLS:  set[str] = set() #everything


# =========================
# Small conversion helpers
# =========================
def b2s(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, dict):
        return {b2s(k): b2s(v) for k, v in value.items()}
    if isinstance(value, list):
        return [b2s(v) for v in value]
    if isinstance(value, tuple):
        return tuple(b2s(v) for v in value)
    return value


def to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


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


def parse_stream_family(stream_name: str) -> tuple[str, str | None]:
    parts = stream_name.split(":")
    if len(parts) == 4 and parts[0] == "massive" and parts[3] == "stream":
        return parts[1], parts[2]
    return "unknown", None


def parse_int_listish(value: Any) -> list[int]:
    """
    Accepts raw Massive short-code values such as c or i:
    - list / tuple / set of ints/strings
    - scalar int/string
    - stringified arrays like "[1, 2, 3]"
    - comma-separated strings like "1,2,3"
    """
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


# =========================
# Position snapshots / exits
# =========================


def raw_event_type(item: RawMarketEvent) -> str:
    return str(item.get("ev") or "").upper()


def raw_event_symbol(item: RawMarketEvent) -> str:
    symbol = item.get("sym")
    if symbol not in (None, ""):
        return str(symbol).upper()

    return ""


def raw_source_sequence(item: RawMarketEvent) -> str:
    return str(item.get("_source_seq") or "")


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


def raw_market_time_ms(item: RawMarketEvent) -> int:
    event_type = raw_event_type(item)

    if event_type in {"T", "Q"}:
        timestamp = raw_int(item.get("t"))
        if timestamp is not None:
            return normalize_timestamp_to_ms(timestamp)

    if event_type == "A":
        end_timestamp = raw_int(item.get("e"))
        if end_timestamp is not None:
            return normalize_timestamp_to_ms(end_timestamp)

        start_timestamp = raw_int(item.get("s"))
        if start_timestamp is not None:
            return normalize_timestamp_to_ms(start_timestamp)

    timestamp = raw_int(item.get("t"))
    if timestamp is not None:
        return normalize_timestamp_to_ms(timestamp)

    return utc_now_ms()


def trade_market_time_ms(trade: RawMarketEvent) -> int:
    return raw_market_time_ms(trade)


def quote_market_time_ms(quote: RawMarketEvent) -> int:
    return raw_market_time_ms(quote)


def aggregate_market_time_ms(aggregate: RawMarketEvent) -> int:
    return raw_market_time_ms(aggregate)


def trade_sort_key(trade: RawMarketEvent) -> tuple[int, int, int, str]:
    return (
        trade_market_time_ms(trade),
        raw_int(trade.get("q")) if raw_int(trade.get("q")) is not None else -1,
        raw_int(trade.get("i")) if raw_int(trade.get("i")) is not None else -1,
        raw_source_sequence(trade),
    )


def quote_sort_key(quote: RawMarketEvent) -> tuple[int, int, str]:
    return (
        quote_market_time_ms(quote),
        raw_int(quote.get("q")) if raw_int(quote.get("q")) is not None else -1,
        raw_source_sequence(quote),
    )


def aggregate_sort_key(aggregate: RawMarketEvent) -> tuple[int, int, str]:
    return (
        aggregate_market_time_ms(aggregate),
        raw_int(aggregate.get("s")) if raw_int(aggregate.get("s")) is not None else -1,
        raw_source_sequence(aggregate),
    )


def trade_indicator_profile_to_dict(
    profile: massive_conditions_module.TradeConditionProfile,
) -> dict[str, Any]:
    return {
        "codes": list(profile.codes),
        "names": list(profile.names),
        "updates_high_low": profile.updates_high_low,
        "updates_last": profile.updates_last,
        "updates_volume": profile.updates_volume,
    }


def quote_condition_profile_to_dict(
    profile: massive_conditions_module.QuoteConditionProfile,
) -> dict[str, Any]:
    return {
        "codes": list(profile.codes),
        "names": list(profile.names),
        "is_valid": profile.is_valid,
        "is_firm": profile.is_firm,
        "allows_spread": profile.allows_spread,
    }


def raw_trade_indicator_profile(
    trade: RawMarketEvent,
) -> massive_conditions_module.TradeConditionProfile:
    return massive_conditions_module.build_trade_condition_profile_from_codes(
        parse_int_listish(trade.get("c"))
    )


def raw_quote_condition_profile(
    quote: RawMarketEvent,
) -> massive_conditions_module.QuoteConditionProfile:
    conditions = parse_int_listish(quote.get("c"))
    indicators = parse_int_listish(quote.get("i"))
    profile_codes = conditions if conditions else indicators
    return massive_conditions_module.build_quote_condition_profile_from_codes(profile_codes)


# =========================
# =========================
# Per-symbol state
# =========================
@dataclass(slots=True)
class SymbolState:
    symbol: str
    aggregates: deque[RawMarketEvent] = field(
        default_factory=lambda: deque(maxlen=MAX_AGGREGATES_PER_SYMBOL)
    )
    trades: deque[RawMarketEvent] = field(default_factory=deque)
    quotes: deque[RawMarketEvent] = field(default_factory=deque)
    baselines: deque[BaselineSnapshot] = field(
        default_factory=lambda: deque(maxlen=MAX_BASELINES_PER_SYMBOL)
    )
    reversal_baselines: deque[ReversalBaselineSnapshot] = field(
        default_factory=lambda: deque(maxlen=MAX_REVERSAL_BASELINES_PER_SYMBOL)
    )
    latest_trade_event_ms: int = 0
    latest_quote_event_ms: int = 0

    def prune_old_trades(self, reference_now_ms: int | None = None) -> None:
        now_ms = max(reference_now_ms or 0, self.latest_trade_event_ms)
        cutoff = now_ms - TRADE_RETENTION_MS
        while self.trades and trade_market_time_ms(self.trades[0]) < cutoff:
            self.trades.popleft()

    def prune_old_quotes(self, reference_now_ms: int | None = None) -> None:
        now_ms = max(reference_now_ms or 0, self.latest_quote_event_ms)
        cutoff = now_ms - QUOTE_RETENTION_MS
        while self.quotes and quote_market_time_ms(self.quotes[0]) < cutoff:
            self.quotes.popleft()

    def _append_trade_in_order(self, trade: RawMarketEvent) -> None:
        if not self.trades:
            self.trades.append(trade)
            return

        incoming_key = trade_sort_key(trade)
        if incoming_key >= trade_sort_key(self.trades[-1]):
            self.trades.append(trade)
            return

        for index in range(len(self.trades) - 1, -1, -1):
            if trade_sort_key(self.trades[index]) <= incoming_key:
                self.trades.insert(index + 1, trade)
                return

        self.trades.appendleft(trade)

    def _append_quote_in_order(self, quote: RawMarketEvent) -> None:
        if not self.quotes:
            self.quotes.append(quote)
            return

        incoming_key = quote_sort_key(quote)
        if incoming_key >= quote_sort_key(self.quotes[-1]):
            self.quotes.append(quote)
            return

        for index in range(len(self.quotes) - 1, -1, -1):
            if quote_sort_key(self.quotes[index]) <= incoming_key:
                self.quotes.insert(index + 1, quote)
                return

        self.quotes.appendleft(quote)

    def _append_aggregate_in_order(self, aggregate: RawMarketEvent) -> None:
        if not self.aggregates:
            self.aggregates.append(aggregate)
            return

        incoming_key = aggregate_sort_key(aggregate)
        if incoming_key >= aggregate_sort_key(self.aggregates[-1]):
            self.aggregates.append(aggregate)
            return

        ordered = list(self.aggregates)
        for index in range(len(ordered) - 1, -1, -1):
            if aggregate_sort_key(ordered[index]) <= incoming_key:
                ordered.insert(index + 1, aggregate)
                break
        else:
            ordered.insert(0, aggregate)

        self.aggregates = deque(
            ordered[-MAX_AGGREGATES_PER_SYMBOL:],
            maxlen=MAX_AGGREGATES_PER_SYMBOL,
        )

    def add(self, item: RawMarketEvent) -> None:
        event_type = raw_event_type(item)

        if event_type == "Q":
            now_ms = quote_market_time_ms(item)
            self.latest_quote_event_ms = max(self.latest_quote_event_ms, now_ms)
            self._append_quote_in_order(item)
            self.prune_old_quotes(self.latest_quote_event_ms)
            return

        if event_type == "T":
            now_ms = trade_market_time_ms(item)
            self.latest_trade_event_ms = max(self.latest_trade_event_ms, now_ms)
            self._append_trade_in_order(item)
            self.prune_old_trades(self.latest_trade_event_ms)
            return

        if event_type == "A":
            self._append_aggregate_in_order(item)

    def latest_price(self) -> float | None:
        if self.trades:
            return raw_float(self.trades[-1].get("p"))
        if self.aggregates:
            return raw_float(self.aggregates[-1].get("c"))
        return None

    def latest_share_spread(self) -> float | None:
        if not self.quotes:
            return None

        latest_quote = self.quotes[-1]
        bid_price = raw_float(latest_quote.get("bp"))
        ask_price = raw_float(latest_quote.get("ap"))

        if bid_price is None or ask_price is None:
            return None

        spread = ask_price - bid_price
        return spread if spread > 0 else None

    def recent_trades(
        self,
        window_ms: int,
        *,
        now_ms: int | None = None,
    ) -> list[RawMarketEvent]:
        if not self.trades:
            return []

        newest_ms = now_ms if now_ms is not None else trade_market_time_ms(self.trades[-1])
        cutoff = newest_ms - window_ms

        recent: list[RawMarketEvent] = []
        for trade in reversed(self.trades):
            if trade_market_time_ms(trade) < cutoff:
                break
            recent.append(trade)

        recent.reverse()
        return recent

    def recent_quotes(
        self,
        window_ms: int,
        *,
        now_ms: int | None = None,
    ) -> list[RawMarketEvent]:
        if not self.quotes:
            return []

        newest_ms = now_ms if now_ms is not None else quote_market_time_ms(self.quotes[-1])
        cutoff = newest_ms - window_ms

        recent: list[RawMarketEvent] = []
        for quote in reversed(self.quotes):
            if quote_market_time_ms(quote) < cutoff:
                break
            recent.append(quote)

        recent.reverse()
        return recent

    def latest_trade_indicator_profile(
        self,
    ) -> massive_conditions_module.TradeConditionProfile | None:
        if not self.trades:
            return None
        return raw_trade_indicator_profile(self.trades[-1])

    def latest_quote_condition_profile(
        self,
    ) -> massive_conditions_module.QuoteConditionProfile | None:
        if not self.quotes:
            return None
        return raw_quote_condition_profile(self.quotes[-1])

    def recent_aggregates(self, count: int | None = None) -> list[RawMarketEvent]:
        if count is None or count <= 0:
            return list(self.aggregates)
        return list(self.aggregates)[-count:]

    def add_baseline(self, baseline: BaselineSnapshot) -> None:
        self.baselines.append(baseline)

    def latest_baseline(self) -> BaselineSnapshot | None:
        if not self.baselines:
            return None
        return self.baselines[-1]

    def add_reversal_baseline(self, baseline: ReversalBaselineSnapshot) -> None:
        self.reversal_baselines.append(baseline)

    def latest_reversal_baseline(self) -> ReversalBaselineSnapshot | None:
        if not self.reversal_baselines:
            return None
        return self.reversal_baselines[-1]


class MarketStateStore:
    def __init__(self) -> None:
        self.symbols: dict[str, SymbolState] = {}

    def get(self, symbol: str) -> SymbolState:
        symbol = symbol.upper()
        if symbol not in self.symbols:
            self.symbols[symbol] = SymbolState(symbol=symbol)
        return self.symbols[symbol]

    def update(self, item: RawMarketEvent) -> SymbolState | None:
        if not isinstance(item, dict):
            return None

        event_type = raw_event_type(item)
        if event_type not in {"A", "T", "Q"}:
            return None

        symbol = raw_event_symbol(item)
        if not symbol:
            return None

        state = self.get(symbol)
        state.add(item)
        return state

    def add_baseline(self, baseline: BaselineSnapshot) -> SymbolState:
        state = self.get(baseline.symbol)
        state.add_baseline(baseline)
        return state

    def add_reversal_baseline(self, baseline: ReversalBaselineSnapshot) -> SymbolState:
        state = self.get(baseline.symbol)
        state.add_reversal_baseline(baseline)
        return state

    def latest_baseline(self, symbol: str) -> BaselineSnapshot | None:
        return self.get(symbol).latest_baseline()

    def latest_reversal_baseline(self, symbol: str) -> ReversalBaselineSnapshot | None:
        return self.get(symbol).latest_reversal_baseline()

    def baseline_count(self) -> int:
        return sum(len(state.baselines) for state in self.symbols.values())

    def reversal_baseline_count(self) -> int:
        return sum(len(state.reversal_baselines) for state in self.symbols.values())


market_state = MarketStateStore()


async def process_event(item: RawMarketEvent) -> SymbolState | None:
    return market_state.update(item)


def event_market_time_ms(item: RawMarketEvent) -> int:
    return raw_market_time_ms(item)


def raw_event_for_debug(item: RawMarketEvent) -> dict[str, Any]:
    snapshot = dict(item)
    ts_ms = raw_market_time_ms(item)
    snapshot["_timestamp_ms"] = ts_ms
    snapshot["_timestamp_local"] = format_market_ms(ts_ms)
    return snapshot


def latest_trade_details(state: SymbolState) -> dict[str, Any] | None:
    if not state.trades:
        return None

    trade = state.trades[-1]
    details = raw_event_for_debug(trade)
    details["_condition_profile"] = trade_indicator_profile_to_dict(
        raw_trade_indicator_profile(trade)
    )
    return details


def latest_quote_details(state: SymbolState) -> dict[str, Any] | None:
    if not state.quotes:
        return None

    quote = state.quotes[-1]
    details = raw_event_for_debug(quote)

    bid_price = raw_float(quote.get("bp"))
    ask_price = raw_float(quote.get("ap"))
    if bid_price is not None and ask_price is not None:
        details["_spread"] = ask_price - bid_price

    details["_condition_profile"] = quote_condition_profile_to_dict(
        raw_quote_condition_profile(quote)
    )
    return details


def latest_aggregate_details(state: SymbolState) -> dict[str, Any] | None:
    if not state.aggregates:
        return None

    aggregate = state.aggregates[-1]
    details = raw_event_for_debug(aggregate)

    end_timestamp = raw_int(aggregate.get("e"))
    if end_timestamp is not None:
        end_timestamp_ms = normalize_timestamp_to_ms(end_timestamp)
        details["_end_timestamp_ms"] = end_timestamp_ms
        details["_end_timestamp_local"] = format_market_ms(end_timestamp_ms)

    return details


def print_symbol_state_details(
    *,
    state: SymbolState,
    triggering_item: RawMarketEvent,
    event_time_ms: int,
) -> None:
    payload = {
        "symbol": state.symbol,
        "received_from_massive": raw_event_for_debug(triggering_item),
        "triggering_event_type": raw_event_type(triggering_item),
        "event_time_ms": event_time_ms,
        "event_time_local": format_market_ms(event_time_ms),
        "counts": {
            "aggregates": len(state.aggregates),
            "trades": len(state.trades),
            "quotes": len(state.quotes),
            "baselines": len(state.baselines),
            "reversal_baselines": len(state.reversal_baselines),
        },
        "latest_price": state.latest_price(),
        "latest_share_spread": state.latest_share_spread(),
        "latest_trade_event_ms": state.latest_trade_event_ms,
        "latest_quote_event_ms": state.latest_quote_event_ms,
        "latest_trade": latest_trade_details(state),
        "latest_quote": latest_quote_details(state),
        "latest_aggregate": latest_aggregate_details(state),
    }

    output = (
        "\n========== SYMBOL STATE FROM MASSIVE ==========\n"
        f"{pformat(payload, sort_dicts=False)}\n"
        "=============================================\n"
    )

    if ENABLE_SYMBOL_STATE_DEBUG_PRINT_TO_CONSOLE:
        print(output, flush=True)

    if ENABLE_SYMBOL_STATE_DEBUG_WRITE_TO_FILE:
        append_output_to_file(output)


# Direct Massive websocket intake lives in massive_direct_stream.py.
# This file consumes raw short-key Massive events directly.


async def main() -> None:
    await load_mongo_baselines_into_market_state(market_state)

    last_symbol_state_print_wall_ms: dict[str, int] = {}

    def maybe_print_symbol_state(
        *,
        state: SymbolState,
        item: RawMarketEvent,
        event_time_ms: int,
    ) -> None:
        if not ENABLE_SYMBOL_STATE_DEBUG_PRINTING:
            return

        symbol = state.symbol.upper()

        if SYMBOL_STATE_DEBUG_SYMBOLS and symbol not in SYMBOL_STATE_DEBUG_SYMBOLS:
            return

        wall_now_ms = utc_now_ms()
        last_print_ms = last_symbol_state_print_wall_ms.get(symbol, 0)

        if wall_now_ms - last_print_ms < SYMBOL_STATE_DEBUG_PRINT_INTERVAL_SECONDS * 1000:
            return

        last_symbol_state_print_wall_ms[symbol] = wall_now_ms

        print_symbol_state_details(
            state=state,
            triggering_item=item,
            event_time_ms=event_time_ms,
        )

    async def on_update(item: RawMarketEvent) -> None:
        state = await process_event(item)

        if state is None:
            return

        item_event_time_ms = event_market_time_ms(item)

        maybe_print_symbol_state(
            state=state,
            item=item,
            event_time_ms=item_event_time_ms,
        )

    busy_ticket_config = BusyTicketConfig()
    trigger_config = TriggerConfig()
    busy_ticket_task = asyncio.create_task(
        print_busy_ticket_loop(
            market_state,
            append_output_to_file=append_output_to_file,
            config=busy_ticket_config,
        )
    )
    trigger_candidate_task = asyncio.create_task(
        print_trigger_candidate_loop(
            market_state,
            append_output_to_file=append_output_to_file,
            busy_ticket_config=busy_ticket_config,
            trigger_config=trigger_config,
        )
    )

    try:
        async with MassiveDirectDequeStreamer(
            config=DirectMassiveStreamConfig(),
            on_update=on_update,
        ) as streamer:
            await streamer.run()
    finally:
        busy_ticket_task.cancel()
        trigger_candidate_task.cancel()

        await asyncio.gather(
            busy_ticket_task,
            trigger_candidate_task,
            return_exceptions=True,
        )


async def app() -> None:
    await main()

if __name__ == "__main__":
    configure_logging()

    append_output_to_file(
        f"\n========== STARTED {utc_now().isoformat(timespec='seconds')} =========="
    )

    asyncio.run(app())