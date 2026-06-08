import asyncio
import json
import logging
import os
import queue
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable

import motor.motor_asyncio
from massive import WebSocketClient
from massive.websocket.models import Feed, Market


logger = logging.getLogger(__name__)

RawFrame = str | bytes | bytearray | memoryview
RawMarketEvent = dict[str, Any]


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return int(raw.replace("_", ""))


@dataclass(slots=True)
class DirectMassiveStreamConfig:
    """
    Redis-free Massive websocket intake.

    The emitted event is the raw Massive short-key dict:
      T: ev, sym, i, x, p, s, c, t, pt, q, z, trfi, trft, ds
      Q: ev, sym, c, i, bx, ax, bp, ap, bs, as, t, pt, q, z
      A: ev, sym, v, av, op, vw, o, c, h, l, a, z, s, e, otc, fv

    No Redis stream key, Redis id, or Redis field wrapper is added.
    """
    api_key: str | None = os.getenv("MASSIVE_API_KEY", "REPLACE_ME")

    # "mongo" loads symbols from Mongo and subscribes to A./T./Q. per symbol.
    # "wildcard" subscribes to A.*, T.*, Q.*.
    # "explicit" uses explicit_symbols.
    subscription_mode: str = os.getenv("SUBSCRIPTION_MODE", "mongo").strip().lower()
    explicit_symbols: tuple[str, ...] = ()

    mongo_uri: str = os.getenv("MONGO_URI", "mongodb://192.168.1.126:27017")
    mongo_database: str = os.getenv("MONGO_DATABASE", "trading_data")
    mongo_ticker_collection: str = os.getenv("MONGO_TICKER_COLLECTION", "ibkr_nasdaq_tickers")

    subscribe_aggs: bool = _env_bool("SUBSCRIBE_AGGS", True)
    subscribe_trades: bool = _env_bool("SUBSCRIBE_TRADES", True)
    subscribe_quotes: bool = _env_bool("SUBSCRIBE_QUOTES", True)

    wildcard_aggs_pattern: str = os.getenv("WILDCARD_AGGS_PATTERN", "A.*")
    wildcard_trades_pattern: str = os.getenv("WILDCARD_TRADES_PATTERN", "T.*")
    wildcard_quotes_pattern: str = os.getenv("WILDCARD_QUOTES_PATTERN", "Q.*")

    raw_queue_maxsize: int = _env_int("MASSIVE_RAW_QUEUE_MAXSIZE", 5_000_000)
    event_queue_maxsize: int = _env_int("MASSIVE_EVENT_QUEUE_MAXSIZE", 100_000)

    # Keep the websocket callback non-blocking.
    drop_on_raw_queue_full: bool = _env_bool("MASSIVE_DROP_ON_RAW_QUEUE_FULL", True)
    raw_drain_batch_size: int = _env_int("MASSIVE_RAW_DRAIN_BATCH_SIZE", 10_000)

    ws_ping_interval_sec: float = float(os.getenv("WS_PING_INTERVAL_SEC", "20"))
    ws_ping_timeout_sec: float = float(os.getenv("WS_PING_TIMEOUT_SEC", "60"))
    ws_close_timeout_sec: float = float(os.getenv("WS_CLOSE_TIMEOUT_SEC", "10"))

    log_every_n_raw_drops: int = _env_int("LOG_EVERY_N_RAW_DROPS", 1000)
    log_every_n_json_errors: int = _env_int("LOG_EVERY_N_JSON_ERRORS", 100)

    enable_queue_metrics: bool = _env_bool("ENABLE_MASSIVE_QUEUE_METRICS", True)
    queue_metrics_interval_seconds: float = float(
        os.getenv("MASSIVE_QUEUE_METRICS_INTERVAL_SECONDS", "1")
    )


def compact_json_loads(frame: RawFrame) -> Any:
    if isinstance(frame, (bytes, bytearray, memoryview)):
        text = bytes(frame).decode("utf-8", errors="replace")
    else:
        text = frame

    return json.loads(text)


def raw_frame_to_events(frame: RawFrame) -> list[RawMarketEvent]:
    """
    Convert one raw Massive websocket frame into raw short-key event dicts.

    This intentionally does not rename fields and does not wrap the payload.
    """
    obj = compact_json_loads(frame)

    if isinstance(obj, dict):
        events = [obj]
    elif isinstance(obj, list):
        events = obj
    else:
        return []

    output: list[RawMarketEvent] = []

    for event in events:
        if not isinstance(event, dict):
            continue

        # Ignore Massive status/auth/subscription messages.
        if event.get("ev") == "status":
            continue

        output.append(event)

    return output


def raw_event_type(item: RawMarketEvent) -> str:
    return str(item.get("ev") or "").upper()


def raw_float(value: Any) -> float | None:
    if value is None or value == "":
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def valid_direct_market_event(item: RawMarketEvent) -> bool:
    event_type = raw_event_type(item)

    if event_type not in {"A", "T", "Q"}:
        return False

    symbol = item.get("sym")
    if symbol in (None, ""):
        return False

    # Keep the existing app-side trade filter that rejected empty/zero-size trades.
    if event_type == "T":
        trade_size = raw_float(item.get("s"))
        if trade_size is None or trade_size <= 0:
            return False

    return True


async def fetch_nasdaq_tickers(
    *,
    mongo_uri: str,
    database: str,
    collection: str,
) -> list[str]:
    client = motor.motor_asyncio.AsyncIOMotorClient(mongo_uri)
    try:
        db = client[database]
        ticker_collection = db[collection]

        symbols: list[str] = []
        async for doc in ticker_collection.find({}, {"_id": 0, "symbol": 1}):
            sym = doc.get("symbol")
            if sym:
                symbols.append(str(sym).strip().upper())

        return symbols
    finally:
        client.close()


def build_subscription_patterns(symbols: Iterable[str]) -> tuple[str, str, str]:
    normalized = [str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()]
    aggs = ",".join(f"A.{sym}" for sym in normalized)
    trades = ",".join(f"T.{sym}" for sym in normalized)
    quotes = ",".join(f"Q.{sym}" for sym in normalized)
    return aggs, trades, quotes


class MassiveDirectDequeStreamer:
    """
    Starts Massive's blocking websocket client in a background thread, drains raw
    websocket frames into an asyncio.Queue, and emits raw Massive dicts to on_update.

    on_update is where your live ranking app calls process_event(), which appends
    the event into the per-symbol deques.
    """

    def __init__(
        self,
        config: DirectMassiveStreamConfig | None = None,
        *,
        on_update: Callable[[RawMarketEvent], Any],
    ) -> None:
        self.config = config or DirectMassiveStreamConfig()
        self.on_update = on_update

        self.raw_frame_queue: queue.Queue[RawFrame] = queue.Queue(
            maxsize=self.config.raw_queue_maxsize
        )
        self.event_queue: asyncio.Queue[RawMarketEvent] = asyncio.Queue(
            maxsize=self.config.event_queue_maxsize
        )

        self.raw_received_count = 0
        self.raw_drop_count = 0
        self.json_error_count = 0
        self.received_count = 0
        self.processed_count = 0
        self.failed_count = 0

        self._last_metrics_wall = time.monotonic()
        self._last_raw_received_count = 0
        self._last_received_count = 0
        self._last_processed_count = 0
        self._last_failed_count = 0

        self._client: WebSocketClient | None = None
        self._websocket_thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._tasks: list[asyncio.Task[Any]] = []

    async def __aenter__(self) -> "MassiveDirectDequeStreamer":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def close(self) -> None:
        self._stop_event.set()

        client = self._client
        if client is not None:
            close = getattr(client, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    logger.debug("Massive websocket close failed", exc_info=True)

        for task in self._tasks:
            task.cancel()

        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

        self._tasks.clear()

    def validate_config(self) -> None:
        if not self.config.api_key or self.config.api_key == "REPLACE_ME":
            raise RuntimeError("Set MASSIVE_API_KEY in your environment.")

        if self.config.subscription_mode not in {"mongo", "wildcard", "explicit"}:
            raise RuntimeError(
                f"Invalid SUBSCRIPTION_MODE={self.config.subscription_mode!r}. "
                "Use 'mongo', 'wildcard', or 'explicit'."
            )

        if self.config.subscription_mode == "explicit" and not self.config.explicit_symbols:
            raise RuntimeError("SUBSCRIPTION_MODE='explicit' requires explicit_symbols.")

    async def build_patterns(self) -> tuple[str, str, str]:
        mode = self.config.subscription_mode

        if mode == "wildcard":
            return (
                self.config.wildcard_aggs_pattern,
                self.config.wildcard_trades_pattern,
                self.config.wildcard_quotes_pattern,
            )

        if mode == "explicit":
            return build_subscription_patterns(self.config.explicit_symbols)

        symbols = await fetch_nasdaq_tickers(
            mongo_uri=self.config.mongo_uri,
            database=self.config.mongo_database,
            collection=self.config.mongo_ticker_collection,
        )
        return build_subscription_patterns(symbols)

    def subscribe_all(
        self,
        client: WebSocketClient,
        *,
        aggs_pattern: str,
        trades_pattern: str,
        quotes_pattern: str,
    ) -> None:
        if self.config.subscribe_aggs and aggs_pattern:
            client.subscribe(aggs_pattern)

        if self.config.subscribe_trades and trades_pattern:
            client.subscribe(trades_pattern)

        if self.config.subscribe_quotes and quotes_pattern:
            client.subscribe(quotes_pattern)

    def handle_msg(self, msg: RawFrame) -> None:
        self.raw_received_count += 1

        try:
            if self.config.drop_on_raw_queue_full:
                self.raw_frame_queue.put_nowait(msg)
            else:
                self.raw_frame_queue.put(msg)
        except queue.Full:
            self.raw_drop_count += 1
            if self.raw_drop_count % self.config.log_every_n_raw_drops == 0:
                logger.warning(
                    "Massive raw queue full; dropped=%s raw_q=%s",
                    self.raw_drop_count,
                    self.raw_frame_queue.qsize(),
                )

    def _run_websocket_client(self) -> None:
        assert self._client is not None

        try:
            self._client.run(
                self.handle_msg,
                close_timeout=self.config.ws_close_timeout_sec,
                ping_interval=self.config.ws_ping_interval_sec,
                ping_timeout=self.config.ws_ping_timeout_sec,
            )
        except Exception:
            if not self._stop_event.is_set():
                logger.exception("Massive websocket client stopped unexpectedly")
            raise

    async def start_websocket_thread(self) -> None:
        self.validate_config()

        aggs_pattern, trades_pattern, quotes_pattern = await self.build_patterns()

        self._client = WebSocketClient(
            api_key=self.config.api_key,
            feed=Feed.RealTime,
            market=Market.Stocks,
            raw=True,
        )

        self.subscribe_all(
            self._client,
            aggs_pattern=aggs_pattern,
            trades_pattern=trades_pattern,
            quotes_pattern=quotes_pattern,
        )

        logger.info(
            "Starting direct Massive websocket: subscriptions=%r websocket=%r queue=%r",
            {
                "aggs": aggs_pattern if self.config.subscribe_aggs else None,
                "trades": trades_pattern if self.config.subscribe_trades else None,
                "quotes": quotes_pattern if self.config.subscribe_quotes else None,
            },
            {
                "ping_interval_sec": self.config.ws_ping_interval_sec,
                "ping_timeout_sec": self.config.ws_ping_timeout_sec,
                "close_timeout_sec": self.config.ws_close_timeout_sec,
                "raw": True,
            },
            {
                "raw_queue_maxsize": self.config.raw_queue_maxsize,
                "event_queue_maxsize": self.config.event_queue_maxsize,
                "drop_on_raw_queue_full": self.config.drop_on_raw_queue_full,
            },
        )

        self._websocket_thread = threading.Thread(
            target=self._run_websocket_client,
            daemon=True,
            name="massive_direct_websocket",
        )
        self._websocket_thread.start()

    async def enqueue(self, item: RawMarketEvent) -> None:
        self.received_count += 1
        await self.event_queue.put(item)

    async def emit(self, item: RawMarketEvent) -> None:
        result = self.on_update(item)
        if asyncio.iscoroutine(result):
            await result

    async def raw_frame_parser_loop(self) -> None:
        while True:
            frame = await asyncio.to_thread(self.raw_frame_queue.get)
            batch = [frame]

            for _ in range(max(0, self.config.raw_drain_batch_size - 1)):
                try:
                    batch.append(self.raw_frame_queue.get_nowait())
                except queue.Empty:
                    break

            for raw_frame in batch:
                try:
                    for item in raw_frame_to_events(raw_frame):
                        if valid_direct_market_event(item):
                            await self.enqueue(item)
                except json.JSONDecodeError:
                    self.json_error_count += 1
                    if self.json_error_count % self.config.log_every_n_json_errors == 0:
                        logger.warning(
                            "Raw Massive frame JSON parse failed; errors=%s",
                            self.json_error_count,
                        )
                except Exception:
                    self.failed_count += 1
                    logger.exception("Failed parsing raw Massive frame")
                finally:
                    self.raw_frame_queue.task_done()

    async def process_event_queue(self) -> None:
        while True:
            item = await self.event_queue.get()

            try:
                await self.emit(item)
                self.processed_count += 1
            except asyncio.CancelledError:
                raise
            except Exception:
                self.failed_count += 1
                logger.exception("on_update failed for direct Massive item=%r", item)
            finally:
                self.event_queue.task_done()

    async def print_queue_metrics_loop(self) -> None:
        while True:
            await asyncio.sleep(self.config.queue_metrics_interval_seconds)

            if not self.config.enable_queue_metrics:
                continue

            now = time.monotonic()
            elapsed_seconds = max(now - self._last_metrics_wall, 0.001)

            raw_received_delta = self.raw_received_count - self._last_raw_received_count
            received_delta = self.received_count - self._last_received_count
            processed_delta = self.processed_count - self._last_processed_count
            failed_delta = self.failed_count - self._last_failed_count

            queue_size = self.event_queue.qsize()
            raw_queue_size = self.raw_frame_queue.qsize()
            queue_fill_pct = (
                queue_size / self.config.event_queue_maxsize * 100.0
                if self.config.event_queue_maxsize > 0
                else 0.0
            )
            raw_queue_fill_pct = (
                raw_queue_size / self.config.raw_queue_maxsize * 100.0
                if self.config.raw_queue_maxsize > 0
                else 0.0
            )

            logger.info(
                "MASSIVE DIRECT QUEUE METRICS: %r",
                {
                    "raw_queue_size": raw_queue_size,
                    "raw_queue_maxsize": self.config.raw_queue_maxsize,
                    "raw_queue_fill_pct": round(raw_queue_fill_pct, 2),
                    "event_queue_size": queue_size,
                    "event_queue_maxsize": self.config.event_queue_maxsize,
                    "event_queue_fill_pct": round(queue_fill_pct, 2),
                    "raw_received_total": self.raw_received_count,
                    "events_received_total": self.received_count,
                    "events_processed_total": self.processed_count,
                    "raw_drops_total": self.raw_drop_count,
                    "json_errors_total": self.json_error_count,
                    "failed_total": self.failed_count,
                    "raw_received_per_second": round(raw_received_delta / elapsed_seconds, 2),
                    "events_received_per_second": round(received_delta / elapsed_seconds, 2),
                    "events_processed_per_second": round(processed_delta / elapsed_seconds, 2),
                    "failed_per_second": round(failed_delta / elapsed_seconds, 2),
                },
            )

            self._last_metrics_wall = now
            self._last_raw_received_count = self.raw_received_count
            self._last_received_count = self.received_count
            self._last_processed_count = self.processed_count
            self._last_failed_count = self.failed_count

    async def start(self) -> None:
        await self.start_websocket_thread()

        self._tasks = [
            asyncio.create_task(self.raw_frame_parser_loop(), name="massive_raw_frame_parser"),
            asyncio.create_task(self.process_event_queue(), name="massive_event_processor"),
            asyncio.create_task(self.print_queue_metrics_loop(), name="massive_queue_metrics"),
        ]

    async def run(self) -> None:
        await self.start()

        try:
            await asyncio.Future()
        finally:
            await self.close()
