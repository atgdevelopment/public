"""This exports the entire REDIS database to CSV files for further analysis, I don't use REDIS because I've only got 64GB of memory and 
frequently run out when using 16000 equity products.


"""
#!/usr/bin/env python3

import os
import re
import csv
from typing import Dict, List, Tuple, Any, Set

import redis


# ============================================================
# Redis connection settings
# ============================================================

REDIS_HOST = os.getenv("REDIS_HOST", "192.168.1.126")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

REDIS_USERNAME = os.getenv("REDIS_USERNAME", "default")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "REPLACE_ME")
REDIS_USE_SSL = os.getenv("REDIS_USE_SSL", "false")

REDIS_SOCKET_TIMEOUT = float(os.getenv("REDIS_SOCKET_TIMEOUT", "10"))
REDIS_SOCKET_CONNECT_TIMEOUT = float(os.getenv("REDIS_SOCKET_CONNECT_TIMEOUT", "10"))


# ============================================================
# Export config
# ============================================================

OUT_DIR = r"D:\redis_reports\040626"

XRANGE_PAGE_SIZE = int(os.getenv("XRANGE_PAGE_SIZE", "10000"))

STREAM_MATCHES = [
    "massive:trades:*:stream",
    "massive:quotes:*:stream",
    "massive:*agg:*:stream",
]


# ============================================================
# Redis
# ============================================================

def build_redis_client() -> redis.Redis:
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        username=REDIS_USERNAME,
        password=REDIS_PASSWORD,
        ssl=REDIS_USE_SSL,
        decode_responses=True,
        socket_timeout=REDIS_SOCKET_TIMEOUT,
        socket_connect_timeout=REDIS_SOCKET_CONNECT_TIMEOUT,
        health_check_interval=30,
    )


# ============================================================
# Helpers
# ============================================================

def log(msg: str) -> None:
    print(msg, flush=True)


def safe_filename(value: str) -> str:
    safe = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", value)
    safe = safe.strip().strip(".")

    if not safe:
        safe = "UNKNOWN"

    return safe


def parse_massive_stream_key(key: str) -> Tuple[str, str]:
    """
    Examples:
      massive:trades:AAPL:stream      -> trades, AAPL
      massive:quotes:AAPL:stream      -> quotes, AAPL
      massive:1secagg:AAPL:stream     -> 1secagg, AAPL
      massive:5minagg:AAPL:stream     -> 5minagg, AAPL
    """

    parts = key.split(":")

    if len(parts) >= 4 and parts[0] == "massive" and parts[-1] == "stream":
        stream_type = parts[1]
        symbol = ":".join(parts[2:-1])
        return stream_type, symbol

    return "unknown", key


def iter_stream(r: redis.Redis, key: str, page_size: int = XRANGE_PAGE_SIZE):
    last_id = None

    while True:
        min_id = f"({last_id}" if last_id else "-"
        rows = r.xrange(key, min=min_id, max="+", count=page_size)

        if not rows:
            break

        for entry_id, fields in rows:
            yield entry_id, fields

        last_id = rows[-1][0]


def find_stream_keys(r: redis.Redis) -> List[str]:
    keys: Set[str] = set()

    for pattern in STREAM_MATCHES:
        for key in r.scan_iter(match=pattern, count=1000):
            keys.add(key)

    return sorted(keys)


def discover_csv_fields(
    r: redis.Redis,
    key: str,
) -> Tuple[List[str], int]:
    fieldnames = set()
    row_count = 0

    for _, fields in iter_stream(r, key):
        fieldnames.update(fields.keys())
        row_count += 1

    ordered_fields = [
        "redis_stream_id",
        "redis_stream_key",
        "stream_type",
        "symbol",
        *sorted(fieldnames),
    ]

    return ordered_fields, row_count


def make_output_path(
    out_dir: str,
    key: str,
    used_paths: Set[str],
    index: int,
) -> str:
    stream_type, symbol = parse_massive_stream_key(key)

    filename = f"{safe_filename(stream_type)}_{safe_filename(symbol)}.csv"
    out_path = os.path.join(out_dir, filename)

    if out_path.lower() in used_paths:
        filename = f"{safe_filename(stream_type)}_{safe_filename(symbol)}_{index}.csv"
        out_path = os.path.join(out_dir, filename)

    used_paths.add(out_path.lower())

    return out_path


def export_stream_to_csv(
    r: redis.Redis,
    key: str,
    out_path: str,
) -> int:
    stream_type, symbol = parse_massive_stream_key(key)

    fieldnames, row_count = discover_csv_fields(r, key)

    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames,
            extrasaction="ignore",
        )
        writer.writeheader()

        for entry_id, fields in iter_stream(r, key):
            row: Dict[str, Any] = {
                "redis_stream_id": entry_id,
                "redis_stream_key": key,
                "stream_type": stream_type,
                "symbol": symbol,
            }

            row.update(fields)
            writer.writerow(row)

    return row_count


# ============================================================
# Main
# ============================================================

def main() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)

    r = build_redis_client()
    r.ping()

    stream_keys = find_stream_keys(r)

    if not stream_keys:
        log("No matching streams found.")
        log(f"Patterns searched: {', '.join(STREAM_MATCHES)}")
        return

    log(f"Found {len(stream_keys)} streams")
    log(f"Output directory: {OUT_DIR}")
    log("")

    used_output_paths: Set[str] = set()

    total_rows_written = 0
    files_written = 0

    for i, key in enumerate(stream_keys, start=1):
        stream_type, symbol = parse_massive_stream_key(key)
        out_path = make_output_path(
            out_dir=OUT_DIR,
            key=key,
            used_paths=used_output_paths,
            index=i,
        )

        log(f"[{i}/{len(stream_keys)}] Exporting {key}")
        log(f"    type={stream_type}; symbol={symbol}")

        rows_written = export_stream_to_csv(
            r=r,
            key=key,
            out_path=out_path,
        )

        files_written += 1
        total_rows_written += rows_written

        log(f"    rows_written={rows_written:,}")
        log(f"    wrote {out_path}")
        log("")

    log("Done.")
    log(f"Output directory: {OUT_DIR}")
    log(f"CSV files written: {files_written:,}")
    log(f"Total rows written: {total_rows_written:,}")


if __name__ == "__main__":
    main()