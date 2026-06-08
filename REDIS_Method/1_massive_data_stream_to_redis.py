"""
This application has been designed with raw speed and instant processing in mind. I use raw data feeds from Massive.com in the form of JSON
which then gets passed using a writer to REDIS. The throughput has been tested and successful at around 1,200,000 messages per second
at the start of the trading day and will continue to write around 20/50,000 operations a second for the rest of the day. This was running on
Ubuntu 24.04, coupled with DDR5 memory. I tested this succesfully on DDR4 memory too.

Warning, the entire day will consume around 500GB of memory, the records have a TTL of 2 hours, so they purge each day if not written to.

there is also public\redis_purge_old_events.py which will purge events to 15 minutes, I run this as a CRON job on my linux box to trim 
the database

This is fast enough to take the queues with little to no lag between the trades and quotes arriving, and your application being ready to use 
that data. Trade times are written around 60ms later to the database. I used a unix socket since TCP/IP is incapable of taking this many 
messages, it's not the amount of bandwidth that's the issue, it's the number of messages as queue times will skyrocket on your raw_q side, 
the unix socket is twice as fast.

I consume this data into a python deque for algorithmic processing.


MONGO CONFIGURATION

In order to use mongo, you need a collection of symbols, I used EODHD.com to pull the symbol list, I did validate those with my broker 
first though to ensure I can use them. 1_nasdaq_tickers_to_mongoDB will import from EOD HD and create the list, there is code in 
create_nasdaq_tickers_collection to create it all if you need.

"""

import os
import time
import json
import queue
import asyncio
import threading
from typing import List, Dict, Any, Optional, Tuple, Union

import redis
import motor.motor_asyncio
from redis.backoff import ExponentialBackoff
from redis.retry import Retry
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from massive import WebSocketClient
from massive.websocket.models import Feed, Market


# ============================================================
# TUNING / CONFIG
# ============================================================

# -------------------------
# Redis connection settings
# -------------------------
REDIS_SOCKET_PATH = os.getenv("REDIS_SOCKET_PATH", "/run/redis/redis-server.sock")
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

REDIS_USERNAME = os.getenv("REDIS_USERNAME", "default")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

REDIS_SOCKET_TIMEOUT = float(os.getenv("REDIS_SOCKET_TIMEOUT", "5"))
REDIS_SOCKET_CONNECT_TIMEOUT = float(os.getenv("REDIS_SOCKET_CONNECT_TIMEOUT", "5"))
REDIS_HEALTH_CHECK_INTERVAL = int(os.getenv("REDIS_HEALTH_CHECK_INTERVAL", "30"))
REDIS_DECODE_RESPONSES = False

REDIS_RETRY_ATTEMPTS = int(os.getenv("REDIS_RETRY_ATTEMPTS", "5"))
REDIS_RETRY_BACKOFF_BASE_SEC = float(os.getenv("REDIS_RETRY_BACKOFF_BASE_SEC", "0.1"))
REDIS_RETRY_BACKOFF_CAP_SEC = float(os.getenv("REDIS_RETRY_BACKOFF_CAP_SEC", "2.0"))
REDIS_RETRY_ON_TIMEOUT = True

# -------------------------
# Massive / market settings
# -------------------------
MASSIVE_API_KEY = os.getenv("MASSIVE_API_KEY", "REPLACE_ME")
MASSIVE_FEED = Feed.RealTime
MASSIVE_MARKET = Market.Stocks
# -------------------------
# Subscriptions can be * or mongo, use mongo if you want to select specific price ranges to import, for example you might only 
# be able to afford $10 stock so there is no point wasting CPU cycles on $500 stock imports. 
# -------------------------


SUBSCRIPTION_MODE = os.getenv("SUBSCRIPTION_MODE", "mongo").strip().lower()

SUBSCRIBE_AGGS = True
SUBSCRIBE_TRADES = True
SUBSCRIBE_QUOTES = True

SUBSCRIBE_AGGS_PATTERN = ""
SUBSCRIBE_TRADES_PATTERN = ""
SUBSCRIBE_QUOTES_PATTERN = ""

WILDCARD_AGGS_PATTERN = "A.*"
WILDCARD_TRADES_PATTERN = "T.*"
WILDCARD_QUOTES_PATTERN = "Q.*"

# -------------------------
# Websocket keepalive tuning
# -------------------------
WS_PING_INTERVAL_SEC = float(os.getenv("WS_PING_INTERVAL_SEC", "20"))
WS_PING_TIMEOUT_SEC = float(os.getenv("WS_PING_TIMEOUT_SEC", "60"))
WS_CLOSE_TIMEOUT_SEC = float(os.getenv("WS_CLOSE_TIMEOUT_SEC", "10"))

# -------------------------
# Queue / worker tuning
# -------------------------
# Single queue. The websocket callback drops raw Massive websocket frames into
# this queue, and the Redis writer parses/serializes them while preserving short
# Massive field names such as ev, sym, p, s, t, q, bp, ap, etc.
RAW_QUEUE_MAXSIZE = int(os.getenv("RAW_QUEUE_MAXSIZE", "5_000_000"))

# KEEP THIS TRUE if you do not want the websocket callback to block.
DROP_ON_RAW_QUEUE_FULL = True

# -------------------------
# Redis write batching
# -------------------------
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "45000"))
FLUSH_INTERVAL_SEC = float(os.getenv("FLUSH_INTERVAL_SEC", "0.05"))

# If a Redis pipeline write fails after some commands already reached Redis,
# retrying blindly can duplicate XADD entries. Default is to drop failed batch.
DROP_BATCH_ON_WRITE_ERROR = True
WRITE_ERROR_SLEEP_SEC = float(os.getenv("WRITE_ERROR_SLEEP_SEC", "0.5"))

# -------------------------
# Redis key/data retention
# -------------------------
STREAM_MAXLEN_PER_SYMBOL_DAY = int(os.getenv("STREAM_MAXLEN_PER_SYMBOL_DAY", "15000000"))
STREAM_TTL_SEC = int(os.getenv("STREAM_TTL_SEC", str(180 * 60)))

# -------------------------
# Logging / diagnostics
# -------------------------
LOG_EVERY_N_RAW_DROPS = int(os.getenv("LOG_EVERY_N_RAW_DROPS", "1000"))
LOG_EVERY_N_WRITE_ERRORS = int(os.getenv("LOG_EVERY_N_WRITE_ERRORS", "1"))
LOG_EVERY_N_JSON_ERRORS = int(os.getenv("LOG_EVERY_N_JSON_ERRORS", "100"))
LOG_QUEUE_SIZES = os.getenv("LOG_QUEUE_SIZES", "true").lower() == "true"
LOG_QUEUE_SIZES_INTERVAL_SEC = float(os.getenv("LOG_QUEUE_SIZES_INTERVAL_SEC", "5"))


# ============================================================
# Globals
# ============================================================

RawFrame = Union[str, bytes, bytearray, memoryview]

raw_q: "queue.Queue[RawFrame]" = queue.Queue(maxsize=RAW_QUEUE_MAXSIZE)

counter_lock = threading.Lock()
raw_drop_count = 0
write_error_count = 0
json_error_count = 0
records_written_count = 0


# ============================================================
# Logging / counters
# ============================================================

def log(msg: str) -> None:
    print(msg, flush=True)


def inc_counter(name: str, amount: int = 1) -> int:
    global raw_drop_count, write_error_count, json_error_count, records_written_count

    with counter_lock:
        if name == "raw_drop":
            raw_drop_count += amount
            return raw_drop_count
        if name == "write_error":
            write_error_count += amount
            return write_error_count
        if name == "json_error":
            json_error_count += amount
            return json_error_count
        if name == "records_written":
            records_written_count += amount
            return records_written_count

    return 0


def get_counter_snapshot() -> Dict[str, int]:
    with counter_lock:
        return {
            "raw_drops": raw_drop_count,
            "write_errors": write_error_count,
            "json_errors": json_error_count,
            "records_written": records_written_count,
        }


# ============================================================
# Redis client
# ============================================================

r = redis.Redis(
    unix_socket_path=REDIS_SOCKET_PATH,
    db=REDIS_DB,
    username=REDIS_USERNAME or None,
    password=REDIS_PASSWORD or None,
    decode_responses=REDIS_DECODE_RESPONSES,
    socket_timeout=REDIS_SOCKET_TIMEOUT,
    socket_connect_timeout=REDIS_SOCKET_CONNECT_TIMEOUT,
    health_check_interval=REDIS_HEALTH_CHECK_INTERVAL,
    retry=Retry(
        ExponentialBackoff(
            base=REDIS_RETRY_BACKOFF_BASE_SEC,
            cap=REDIS_RETRY_BACKOFF_CAP_SEC,
        ),
        REDIS_RETRY_ATTEMPTS,
    ),
    retry_on_error=[
        RedisConnectionError,
        RedisTimeoutError,
        ConnectionResetError,
        OSError,
    ],
    retry_on_timeout=REDIS_RETRY_ON_TIMEOUT,
)


# ============================================================
# Subscription helpers
# ============================================================

async def fetch_nasdaq_tickers():
    client = motor.motor_asyncio.AsyncIOMotorClient("mongodb://127.0.0.1:27017")
    db = client["trading_data"]
    collection = db["nasdaq_tickers"]

    symbols = []
    async for doc in collection.find({}, {"_id": 0, "symbol": 1}):
        sym = doc.get("symbol")
        if sym:
            symbols.append(sym.strip().upper())

    client.close()
    return symbols


def build_subscription_patterns(symbols: List[str]) -> Tuple[str, str, str]:
    aggs = ",".join(f"A.{sym}" for sym in symbols)
    trades = ",".join(f"T.{sym}" for sym in symbols)
    quotes = ",".join(f"Q.{sym}" for sym in symbols)
    return aggs, trades, quotes


# ============================================================
# Raw JSON helpers
# ============================================================

def _get_value(obj: Any, *names: str) -> Any:
    if obj is None:
        return None

    for name in names:
        if isinstance(obj, dict):
            if name in obj and obj[name] is not None:
                return obj[name]
        else:
            v = getattr(obj, name, None)
            if v is not None:
                return v

    return None


def raw_frame_to_events(frame: RawFrame) -> List[Tuple[str, Dict[str, Any]]]:
    """
    Convert one raw websocket frame into per-event compact JSON strings.

    Because WebSocketClient(raw=True) is used, Massive's model layer does not
    rename short field names into long model attributes. Redis stores short-key
    JSON such as ev, sym, p, s, t, q, bp, ap, etc. WARNING!!!! IF YOU PUT A DICT IN
    YOU WILL RUIN THE QUEUES, YOU CANNOT KEEP UP WITH THE RESTRUCTURE AND IT USES
    TOO MUCH MEMORY. DO NOT CHANGE THE FORMAT!

    """
    if isinstance(frame, (bytes, bytearray, memoryview)):
        text = bytes(frame).decode("utf-8", errors="replace")
    else:
        text = frame

    try:
        obj = json.loads(text)
    except Exception:
        n = inc_counter("json_error")
        if n % LOG_EVERY_N_JSON_ERRORS == 0:
            log(f"[json] raw frame parse failed, errors={n}")
        return []

    if isinstance(obj, dict):
        events = [obj]
    elif isinstance(obj, list):
        events = obj
    else:
        return []

    out: List[Tuple[str, Dict[str, Any]]] = []

    for event in events:
        if not isinstance(event, dict):
            continue

        # Ignore Massive status/auth/subscription messages.
        if event.get("ev") == "status":
            continue

        raw_json = json.dumps(
            event,
            ensure_ascii=False,
            separators=(",", ":"),
            default=str,
        )

        out.append((raw_json, event))

    return out


def detect_payload_type(payload: Any, meta: Optional[Dict[str, Any]] = None) -> Optional[str]:
    ev = _get_value(meta, "ev", "event_type") if meta else None
    if ev is None:
        ev = _get_value(payload, "ev", "event_type")

    if ev is None:
        return None

    ev_str = str(ev).upper()

    if ev_str.startswith("T"):
        return "trade"

    if ev_str.startswith("Q"):
        return "quote"

    if ev_str.startswith("A"):
        return "agg"

    return None


def get_symbol(payload: Any, meta: Optional[Dict[str, Any]] = None) -> Optional[str]:
    sym = _get_value(meta, "sym", "symbol") if meta else None
    if sym is None:
        sym = _get_value(payload, "sym", "symbol")
    return str(sym) if sym else None


# ============================================================
# Redis key helpers
# ============================================================

def stream_key_for_symbol(sym: str, msg_type: str) -> str:
    if msg_type == "trade":
        return f"massive:trades:{sym}:stream"

    if msg_type == "quote":
        return f"massive:quotes:{sym}:stream"

    return f"massive:1secagg:{sym}:stream"


# ============================================================
# Redis write path
# ============================================================

def flush_batch(batch: List[RawFrame]) -> int:
    records_written = 0

    with r.pipeline(transaction=False) as pipe:
        stream_expire_set = set()

        for frame in batch:
            for raw_json, payload in raw_frame_to_events(frame):
                msg_type = detect_payload_type(payload, payload)
                if not msg_type:
                    continue

                sym = get_symbol(payload, payload)
                if not sym:
                    continue

                stream_key = stream_key_for_symbol(sym, msg_type)

                pipe.xadd(
                    stream_key,
                    fields={"json": raw_json},
                    maxlen=STREAM_MAXLEN_PER_SYMBOL_DAY,
                    approximate=True,
                )

                if STREAM_TTL_SEC > 0 and stream_key not in stream_expire_set:
                    pipe.expire(stream_key, STREAM_TTL_SEC)
                    stream_expire_set.add(stream_key)

                records_written += 1

        if records_written > 0:
            pipe.execute()
            inc_counter("records_written", records_written)

    return records_written


def redis_writer():
    batch: List[RawFrame] = []
    last_flush = time.monotonic()

    while True:
        timeout = max(0.0, FLUSH_INTERVAL_SEC - (time.monotonic() - last_flush))

        try:
            msg = raw_q.get(timeout=timeout)
            batch.append(msg)
            raw_q.task_done()
        except queue.Empty:
            pass

        now = time.monotonic()
        should_flush = batch and (
            len(batch) >= BATCH_SIZE or (now - last_flush) >= FLUSH_INTERVAL_SEC
        )

        if not should_flush:
            continue

        try:
            flush_batch(batch)
            batch.clear()
            last_flush = now

        except (RedisConnectionError, RedisTimeoutError, ConnectionResetError, OSError) as e:
            n = inc_counter("write_error")
            if n % LOG_EVERY_N_WRITE_ERRORS == 0:
                log(
                    f"[redis_writer] write failed "
                    f"(errors={n}, batch_size={len(batch)}, raw_q={raw_q.qsize()}): {e!r}"
                )

            if DROP_BATCH_ON_WRITE_ERROR:
                batch.clear()

            try:
                r.ping()
            except Exception:
                pass

            time.sleep(WRITE_ERROR_SLEEP_SEC)
            last_flush = time.monotonic()

        except Exception as e:
            n = inc_counter("write_error")
            log(
                f"[redis_writer] unexpected error "
                f"(errors={n}, batch_size={len(batch)}, raw_q={raw_q.qsize()}): {e!r}"
            )

            if DROP_BATCH_ON_WRITE_ERROR:
                batch.clear()

            time.sleep(WRITE_ERROR_SLEEP_SEC)
            last_flush = time.monotonic()


def queue_size_logger():
    while True:
        try:
            counters = get_counter_snapshot()
            log(
                f"[queues] raw_q={raw_q.qsize()} "
                f"records_written={counters['records_written']} "
                f"raw_drops={counters['raw_drops']} "
                f"write_errors={counters['write_errors']} "
                f"json_errors={counters['json_errors']}"
            )
        except Exception as e:
            log(f"[queue_size_logger] error: {e!r}")

        time.sleep(LOG_QUEUE_SIZES_INTERVAL_SEC)


# ============================================================
# WebSocket callback
# KEEP THIS NON-BLOCKING
# ============================================================

def handle_msg(msg: RawFrame):
    try:
        if DROP_ON_RAW_QUEUE_FULL:
            raw_q.put_nowait(msg)
        else:
            raw_q.put(msg)
    except queue.Full:
        n = inc_counter("raw_drop")
        if n % LOG_EVERY_N_RAW_DROPS == 0:
            log(
                f"[handle_msg] raw queue full, dropped={n}, "
                f"raw_q={raw_q.qsize()}"
            )


# ============================================================
# Startup helpers
# ============================================================

def subscribe_all(client: WebSocketClient) -> None:
    if SUBSCRIBE_AGGS:
        client.subscribe(SUBSCRIBE_AGGS_PATTERN)

    if SUBSCRIBE_TRADES:
        client.subscribe(SUBSCRIBE_TRADES_PATTERN)

    if SUBSCRIBE_QUOTES:
        client.subscribe(SUBSCRIBE_QUOTES_PATTERN)


def validate_config() -> None:
    if not MASSIVE_API_KEY or MASSIVE_API_KEY == "REPLACE_ME":
        raise RuntimeError("Set MASSIVE_API_KEY in your environment.")

    if not REDIS_SOCKET_PATH:
        raise RuntimeError("Set REDIS_SOCKET_PATH in your environment.")

    if not os.path.exists(REDIS_SOCKET_PATH):
        raise RuntimeError(f"Redis Unix socket does not exist: {REDIS_SOCKET_PATH}")

    if not REDIS_PASSWORD or REDIS_PASSWORD == "REPLACE_ME":
        raise RuntimeError("Set REDIS_PASSWORD in your environment.")


def start_background_threads() -> None:
    t = threading.Thread(
        target=redis_writer,
        daemon=True,
        name="redis_writer",
    )
    t.start()

    if LOG_QUEUE_SIZES:
        t = threading.Thread(
            target=queue_size_logger,
            daemon=True,
            name="queue_size_logger",
        )
        t.start()


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":
    validate_config()

    try:
        r.ping()
        log("[startup] Redis ping OK")
    except Exception as e:
        raise RuntimeError(f"Redis connection failed at startup: {e!r}") from e

    start_background_threads()

    if SUBSCRIPTION_MODE == "wildcard":
        SUBSCRIBE_AGGS_PATTERN = WILDCARD_AGGS_PATTERN
        SUBSCRIBE_TRADES_PATTERN = WILDCARD_TRADES_PATTERN
        SUBSCRIBE_QUOTES_PATTERN = WILDCARD_QUOTES_PATTERN

    elif SUBSCRIPTION_MODE == "mongo":
        symbols = asyncio.run(fetch_nasdaq_tickers())
        SUBSCRIBE_AGGS_PATTERN, SUBSCRIBE_TRADES_PATTERN, SUBSCRIBE_QUOTES_PATTERN = build_subscription_patterns(symbols)

    else:
        raise RuntimeError(
            f"Invalid SUBSCRIPTION_MODE={SUBSCRIPTION_MODE!r}. "
            "Use 'mongo' or 'wildcard'."
        )

    client = WebSocketClient(
        api_key=MASSIVE_API_KEY,
        feed=MASSIVE_FEED,
        market=MASSIVE_MARKET,
        raw=True,
    )

    subscribe_all(client)

    log(
        f"[startup] subscriptions={{"
        f"'aggs': {SUBSCRIBE_AGGS_PATTERN if SUBSCRIBE_AGGS else None}, "
        f"'trades': {SUBSCRIBE_TRADES_PATTERN if SUBSCRIBE_TRADES else None}, "
        f"'quotes': {SUBSCRIBE_QUOTES_PATTERN if SUBSCRIBE_QUOTES else None}"
        f"}}"
    )

    log(
        f"[startup] websocket={{"
        f"'ping_interval_sec': {WS_PING_INTERVAL_SEC}, "
        f"'ping_timeout_sec': {WS_PING_TIMEOUT_SEC}, "
        f"'close_timeout_sec': {WS_CLOSE_TIMEOUT_SEC}, "
        f"'raw': True"
        f"}}"
    )

    log(
        f"[startup] queue={{"
        f"'RAW_QUEUE_MAXSIZE': {RAW_QUEUE_MAXSIZE}, "
        f"'DROP_ON_RAW_QUEUE_FULL': {DROP_ON_RAW_QUEUE_FULL}, "
        f"'LOG_QUEUE_SIZES': {LOG_QUEUE_SIZES}, "
        f"'LOG_QUEUE_SIZES_INTERVAL_SEC': {LOG_QUEUE_SIZES_INTERVAL_SEC}"
        f"}}"
    )

    log(
        f"[startup] redis_write={{"
        f"'BATCH_SIZE': {BATCH_SIZE}, "
        f"'FLUSH_INTERVAL_SEC': {FLUSH_INTERVAL_SEC}, "
        f"'STREAM_MAXLEN_PER_SYMBOL_DAY': {STREAM_MAXLEN_PER_SYMBOL_DAY}, "
        f"'STREAM_TTL_SEC': {STREAM_TTL_SEC}, "
        f"'DROP_BATCH_ON_WRITE_ERROR': {DROP_BATCH_ON_WRITE_ERROR}, "
        f"'field': 'json'"
        f"}}"
    )

    client.run(
        handle_msg,
        close_timeout=WS_CLOSE_TIMEOUT_SEC,
        ping_interval=WS_PING_INTERVAL_SEC,
        ping_timeout=WS_PING_TIMEOUT_SEC,
    )
