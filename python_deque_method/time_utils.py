from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo


LOCAL_TIMEZONE = ZoneInfo("Europe/London")
EXCHANGE_TIMEZONE = ZoneInfo("America/New_York")
TRADING_START_EXCHANGE = time(9, 30)
TRADING_END_EXCHANGE = time(16, 0)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_ms() -> int:
    return int(utc_now().timestamp() * 1000)


def normalize_timestamp_to_ms(value: int) -> int:
    """
    Best-effort normalization:
    - seconds      -> ms
    - milliseconds -> ms
    - microseconds -> ms
    - nanoseconds  -> ms
    """
    if value >= 10**18:  # nanoseconds
        return value // 1_000_000
    if value >= 10**15:  # microseconds
        return value // 1_000
    if value >= 10**12:  # milliseconds
        return value
    if value >= 10**9:  # seconds
        return value * 1000
    return value


def market_time_ms_to_local_dt(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).astimezone(LOCAL_TIMEZONE)


def market_time_ms_to_exchange_dt(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).astimezone(EXCHANGE_TIMEZONE)


def trading_enabled(ts_ms: int) -> bool:
    exchange_dt = market_time_ms_to_exchange_dt(ts_ms)

    if exchange_dt.weekday() >= 5:
        return False

    return TRADING_START_EXCHANGE <= exchange_dt.time() < TRADING_END_EXCHANGE


def format_market_ms(ts_ms: int | None) -> str | None:
    if ts_ms is None or ts_ms <= 0:
        return None

    return market_time_ms_to_local_dt(ts_ms).isoformat(timespec="milliseconds")