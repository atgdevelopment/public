"""
This will purge to 15 minutes of REDIS data, use 

& "C:\Program Files\Python314\python.exe" .\32_new_short_version.py --input-dir "D:\redis_reports\020526" --workers 6 --no-print-json

or wherever you want to store your CSV files and it'll export your entire redis directory to CSV for baseline creation

I use this as a CRON job on Ubuntu to purge after 20 minutes, so 5 minutes of data, every 20 minutes. This is much more efficient, 
effective and doesn't take loads of memory cycles which can slow down the import and increase queue time.

For example, I purged 20GB and my raw queue increased to 12,000, that doesn't happen during normal operation.


"""

#!/usr/bin/env python3

import os
import time
import logging
import redis


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


REDIS_SOCKET_PATH = os.getenv("REDIS_SOCKET_PATH", "/run/redis/redis-server.sock")
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

REDIS_USERNAME = os.getenv("REDIS_USERNAME", "default")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "REPLACE_ME")


STREAM_KEY = os.getenv("STREAM_KEY", "events")

# Retain the last 1 hour 45 minutes by default
RETENTION_HOURS = int(os.getenv("RETENTION_HOURS", "0"))
RETENTION_MINUTES = int(os.getenv("RETENTION_MINUTES", "15"))


def main() -> None:
    if not REDIS_PASSWORD:
        raise RuntimeError("REDIS_PASSWORD environment variable is not set")

    r = redis.Redis(
        unix_socket_path=REDIS_SOCKET_PATH,
        db=REDIS_DB,
        username=REDIS_USERNAME,
        password=REDIS_PASSWORD,
        decode_responses=True,
        socket_timeout=10,
    )

    r.ping()

    retention_ms = ((RETENTION_HOURS * 60) + RETENTION_MINUTES) * 60 * 1000
    cutoff_ms = int(time.time() * 1000) - retention_ms
    cutoff_id = f"{cutoff_ms}-0"

    deleted = r.xtrim(
        STREAM_KEY,
        minid=cutoff_id,
        approximate=True,
    )

    logging.info(
        "Trimmed Redis stream '%s' on DB%s via socket %s. Deleted %s entries older than %s "
        "(retention: %sh %sm)",
        STREAM_KEY,
        REDIS_DB,
        REDIS_SOCKET_PATH,
        deleted,
        cutoff_id,
        RETENTION_HOURS,
        RETENTION_MINUTES,
    )


if __name__ == "__main__":
    main()