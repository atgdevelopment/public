#!/usr/bin/env python3
"""
Trade/quote/1-second aggregate price movement analysis pipeline.

Inputs expected:
  trades_<SYMBOL>.csv
  quotes_<SYMBOL>.csv
  1secagg_<SYMBOL>.csv

Supports both:
  1. Flat CSV columns, e.g. t, p, s, c, bp, ap, bs, as
  2. Redis-exported CSV columns where the Massive payload is inside a `json` column:
       redis_stream_id, redis_stream_key, stream_type, symbol, json

Primary target:
  abs_close_delta = abs(current_1sec_close - previous_1sec_close)

Direction is secondary:
  direction = up/down/flat

MongoDB output:
  - Writes into ONE existing collection only:
      database:   trading_data
      collection: baselines

Per symbol this can now write two latest documents:
  - px_move_symbol
      _id: symbol:<SYMBOL>:latest
  - quote_rev_symbol
      _id: quote_reversal_symbol:<SYMBOL>:latest

      

If you want to change the default folder, it's here, otherwise use switches and direct the app to your folder
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("F:/temp/redis_report/130526"),
        help="Folder containing trades_<SYMBOL>.csv, quotes_<SYMBOL>.csv, and 1secagg_<SYMBOL>.csv. Defaults to F:/temp/redis_report/130526.",
    )


This script refuses to create a new MongoDB database or collection.
No Mongo indexes are created.
No CSV outputs are written.
"""

from __future__ import annotations

import argparse
import asyncio
from concurrent.futures import ProcessPoolExecutor, as_completed
import gc
import json
import math
import os
import re
import traceback
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd
from motor.motor_asyncio import AsyncIOMotorClient


# Massive / Polygon stock trade condition labels.
# Unknown future/new codes are still preserved as "Unmapped condition" instead of failing.
CONDITION_LABELS = {
    -1: "No condition array",
    0: "Regular Sale",
    1: "Acquisition",
    2: "Average Price Trade",
    3: "Bunched Trade",
    4: "Bunched Sold Trade",
    5: "Distribution",
    6: "Placeholder",
    7: "Cash Sale",
    8: "Closing Prints",
    9: "Cross Trade",
    10: "Derivatively Priced",
    11: "Reopening Prints",
    12: "Form T",
    13: "Extended Trading Hours / Sold Out of Sequence",
    14: "Intermarket Sweep",
    15: "Market Center Official Close",
    16: "Market Center Official Open",
    17: "Market Center Opening Trade",
    18: "Market Center Reopening Trade",
    19: "Market Center Closing Trade",
    20: "Next Day",
    21: "Price Variation Trade",
    22: "Prior Reference Price",
    23: "Rule 127 / Rule 155 Trade",
    24: "Seller",
    25: "Sold Last",
    26: "Sold Out of Sequence",
    27: "Stopped Stock Regular Trade",
    28: "Stopped Stock Sold Last",
    29: "Stopped Stock Sold Out of Sequence",
    30: "Contingent Trade",
    31: "Qualified Contingent Trade",
    32: "Sold Out",
    33: "Sold Out of Sequence",
    34: "Split Trade",
    35: "Stock Option Trade",
    36: "Yellow Flag Regular Trade",
    37: "Odd Lot Trade",
    38: "Corrected Consolidated Close",
    39: "Unknown / Reserved",
    40: "Held",
    41: "Trade Thru Exempt",
    46: "Contingent Trade",
    52: "Contingent Trade",
    53: "Qualified Contingent Trade",
    59: "Placeholder for 611 Exempt",
}


# =========================
# JSON / cleaning helpers
# =========================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def parse_json_cell(value: Any) -> dict:
    """
    Parse one Redis-exported JSON cell.

    Empty/null values become {}, but malformed non-empty JSON is allowed to raise
    so the bad file is visible instead of silently producing wrong analysis.
    """
    if isinstance(value, dict):
        return value

    if value is None:
        return {}

    try:
        if pd.isna(value):
            return {}
    except (TypeError, ValueError):
        pass

    text = str(value).strip()

    if text == "" or text.lower() in {"nan", "none", "null"}:
        return {}

    return json.loads(text)


def load_stream_csv(path: Path) -> pd.DataFrame:
    """
    Load either a flat CSV or a Redis-exported CSV with a JSON payload column.

    For Redis exports, this expands the `json` column into normal dataframe
    columns such as:
      trades:  t, p, s, c
      quotes:  t, bp, ap, bs, as
      1secagg: s, c, h, l

    Redis metadata columns are kept.
    If a JSON key overlaps with a metadata column, the JSON key is renamed to
    json_<key> so the metadata column is not overwritten.
    """
    df = pd.read_csv(path)

    if "json" not in df.columns:
        return df

    meta = df.drop(columns=["json"])
    parsed = pd.json_normalize(df["json"].map(parse_json_cell))

    overlapping_cols = set(meta.columns) & set(parsed.columns)
    if overlapping_cols:
        parsed = parsed.rename(
            columns={col: f"json_{col}" for col in overlapping_cols}
        )

    return pd.concat(
        [meta.reset_index(drop=True), parsed.reset_index(drop=True)],
        axis=1,
    )


def parse_condition_array(value: Any) -> tuple[int, ...]:
    """
    Normalize Massive trade condition arrays into a sorted tuple of ints.

    Handles both raw lists from parsed JSON, e.g. [12, 37], and old string
    formats, e.g. "[12, 37]".
    """
    if isinstance(value, (list, tuple, np.ndarray)):
        out = []

        for item in value:
            try:
                if pd.isna(item):
                    continue
            except (TypeError, ValueError):
                pass

            out.append(int(item))

        return tuple(sorted(out))

    if value is None:
        return tuple()

    try:
        if pd.isna(value):
            return tuple()
    except (TypeError, ValueError):
        pass

    text = str(value).strip()

    if text == "" or text.lower() in {"nan", "none", "null", "[]"}:
        return tuple()

    return tuple(sorted(int(x) for x in re.findall(r"-?\d+", text)))


def as_jsonable(value: Any) -> Any:
    """
    Convert pandas/numpy values into safe JSON/Mongo values.

    Important: this removes NaN/Inf values because they are awkward in JSON
    and can make downstream Mongo consumers inconsistent.
    """
    if isinstance(value, (np.integer,)):
        return int(value)

    if isinstance(value, (np.floating,)):
        value = float(value)
        return value if math.isfinite(value) else None

    if isinstance(value, float):
        return value if math.isfinite(value) else None

    if isinstance(value, (np.bool_,)):
        return bool(value)

    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None

        return value.isoformat()

    if isinstance(value, np.ndarray):
        return [as_jsonable(v) for v in value.tolist()]

    if isinstance(value, dict):
        return {str(k): as_jsonable(v) for k, v in value.items()}

    if isinstance(value, list):
        return [as_jsonable(v) for v in value]

    if isinstance(value, tuple):
        return [as_jsonable(v) for v in value]

    return value


def records(df: pd.DataFrame, limit: int | None = None) -> list[dict]:
    if limit is not None:
        df = df.head(limit)

    return as_jsonable(df.to_dict(orient="records"))


def validate_required_columns(
    df: pd.DataFrame,
    required_columns: list[str],
    file_label: str,
    path: Path,
) -> None:
    missing = [col for col in required_columns if col not in df.columns]

    if missing:
        raise KeyError(
            f"{file_label} file {path.name} is missing required column(s) after JSON flatten: {missing}. "
            f"Available columns: {list(df.columns)}. "
            "If this is a Redis export, make sure it has a valid `json` column."
        )


def coerce_numeric_columns(
    df: pd.DataFrame,
    columns: list[str],
    file_label: str,
    path: Path,
) -> None:
    for col in columns:
        before_non_null = df[col].notna().sum()
        df[col] = pd.to_numeric(df[col], errors="coerce")
        after_non_null = df[col].notna().sum()

        if before_non_null > 0 and after_non_null == 0:
            raise ValueError(
                f"{file_label} file {path.name} column {col!r} could not be converted to numeric."
            )


def sort_by_existing(df: pd.DataFrame, preferred_columns: list[str]) -> pd.DataFrame:
    sort_columns = [col for col in preferred_columns if col in df.columns]

    if not sort_columns:
        return df.reset_index(drop=True)

    return df.sort_values(sort_columns).reset_index(drop=True)


# =========================
# File discovery
# =========================

def extract_symbol_from_filename(path: Path, prefix: str) -> str | None:
    name = path.name

    if not name.startswith(prefix):
        return None

    if not name.lower().endswith(".csv"):
        return None

    symbol = name[len(prefix):-4]

    if not symbol:
        return None

    return symbol


def discover_symbols(input_dir: Path) -> tuple[list[str], list[dict]]:
    trade_symbols = {}
    quote_symbols = {}
    agg_symbols = {}

    for path in input_dir.glob("trades_*.csv"):
        symbol = extract_symbol_from_filename(path, "trades_")
        if symbol:
            trade_symbols[symbol.upper()] = symbol

    for path in input_dir.glob("quotes_*.csv"):
        symbol = extract_symbol_from_filename(path, "quotes_")
        if symbol:
            quote_symbols[symbol.upper()] = symbol

    for path in input_dir.glob("1secagg_*.csv"):
        symbol = extract_symbol_from_filename(path, "1secagg_")
        if symbol:
            agg_symbols[symbol.upper()] = symbol

    all_keys = sorted(set(trade_symbols) | set(quote_symbols) | set(agg_symbols))

    complete_symbols = []
    incomplete_symbols = []

    for key in all_keys:
        has_trades = key in trade_symbols
        has_quotes = key in quote_symbols
        has_agg = key in agg_symbols

        if has_trades and has_quotes and has_agg:
            complete_symbols.append(trade_symbols[key])
        else:
            incomplete_symbols.append(
                {
                    "symbol": key,
                    "has_trades_file": has_trades,
                    "has_quotes_file": has_quotes,
                    "has_1secagg_file": has_agg,
                    "expected_trades_file": f"trades_{key}.csv",
                    "expected_quotes_file": f"quotes_{key}.csv",
                    "expected_1secagg_file": f"1secagg_{key}.csv",
                }
            )

    return complete_symbols, incomplete_symbols


def resolve_symbol_case(input_dir: Path, requested_symbol: str) -> str:
    discovered_symbols, _ = discover_symbols(input_dir)
    symbol_map = {symbol.upper(): symbol for symbol in discovered_symbols}
    return symbol_map.get(requested_symbol.upper(), requested_symbol)



# =========================
# Tick-level quote reversal analysis
# =========================

def safe_float(value: Any) -> float | None:
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    try:
        result = float(value)
    except (TypeError, ValueError):
        return None

    return result if math.isfinite(result) else None


def safe_int(value: Any) -> int | None:
    result = safe_float(value)
    if result is None:
        return None

    try:
        return int(result)
    except (TypeError, ValueError, OverflowError):
        return None


def normalize_timestamp_to_ms_for_reversal(value: Any) -> int | None:
    """
    Normalizes common epoch units into milliseconds.

    Existing exports are usually already in milliseconds, but this also handles
    microsecond/nanosecond Massive timestamps if they appear in a different dump.
    """
    raw = safe_int(value)
    if raw is None or raw <= 0:
        return None

    if raw > 10_000_000_000_000_000:  # ns epoch
        return raw // 1_000_000

    if raw > 10_000_000_000_000:  # us epoch
        return raw // 1_000

    if raw > 10_000_000_000:  # ms epoch
        return raw

    if raw > 1_000_000_000:  # s epoch
        return raw * 1_000

    return raw


def parse_int_csv_arg(value: str) -> list[int]:
    return [int(part.strip()) for part in str(value).split(",") if part.strip()]


def stream_id_sort_tuple(value: Any) -> tuple[int, int]:
    match = re.match(r"^(\d+)-(\d+)$", str(value or ""))
    if not match:
        return (0, 0)
    return (int(match.group(1)), int(match.group(2)))


def direction_from_delta(delta: float) -> str:
    if delta > 0:
        return "up"
    if delta < 0:
        return "down"
    return "flat"


def reversal_side_from_direction(direction: str) -> str:
    if direction == "up":
        return "short_to_long"
    if direction == "down":
        return "long_to_short"
    return "unknown"


def prepare_reversal_quotes(quotes: pd.DataFrame) -> pd.DataFrame:
    validate_required_columns(quotes, ["t", "bp", "ap"], "quotes", Path("quotes_<symbol>.csv"))

    q = quotes.copy()
    q["event_ms"] = q["t"].map(normalize_timestamp_to_ms_for_reversal)
    q["bid"] = pd.to_numeric(q["bp"], errors="coerce")
    q["ask"] = pd.to_numeric(q["ap"], errors="coerce")

    q = q.dropna(subset=["event_ms", "bid", "ask"]).copy()
    q = q[(q["bid"] > 0) & (q["ask"] > 0) & (q["ask"] > q["bid"])].copy()

    if q.empty:
        return q

    q["event_ms"] = q["event_ms"].astype("int64")
    q["mid"] = (q["bid"] + q["ask"]) / 2.0
    q["spread"] = q["ask"] - q["bid"]
    q["spread_percent"] = (q["spread"] / q["mid"]) * 100.0

    if "redis_stream_id" in q.columns:
        stream_pairs = q["redis_stream_id"].map(stream_id_sort_tuple)
        q["_stream_ms"] = [pair[0] for pair in stream_pairs]
        q["_stream_seq"] = [pair[1] for pair in stream_pairs]
    else:
        q["_stream_ms"] = 0
        q["_stream_seq"] = np.arange(len(q))

    if "q" not in q.columns:
        q["q"] = np.nan

    q = q.sort_values(
        ["event_ms", "q", "_stream_ms", "_stream_seq"],
        na_position="last",
    ).reset_index(drop=True)

    return q


def prepare_reversal_aggs(agg: pd.DataFrame) -> pd.DataFrame:
    validate_required_columns(agg, ["s", "c", "h", "l"], "1secagg", Path("1secagg_<symbol>.csv"))

    a = agg.copy()
    a["sec_ms"] = a["s"].map(normalize_timestamp_to_ms_for_reversal)
    a["sec_ms"] = (pd.to_numeric(a["sec_ms"], errors="coerce") // 1000) * 1000

    for col in ["c", "h", "l"]:
        a[col] = pd.to_numeric(a[col], errors="coerce")

    if "o" in a.columns:
        a["o"] = pd.to_numeric(a["o"], errors="coerce")
    else:
        a["o"] = np.nan

    if "v" in a.columns:
        a["v"] = pd.to_numeric(a["v"], errors="coerce")
    else:
        a["v"] = np.nan

    a = a.dropna(subset=["sec_ms", "c", "h", "l"]).copy()

    if a.empty:
        return a

    a["sec_ms"] = a["sec_ms"].astype("int64")

    if "e" in a.columns:
        a["end_ms"] = a["e"].map(normalize_timestamp_to_ms_for_reversal)
    else:
        a["end_ms"] = a["sec_ms"] + 999

    if "redis_stream_id" in a.columns:
        stream_pairs = a["redis_stream_id"].map(stream_id_sort_tuple)
        a["_stream_ms"] = [pair[0] for pair in stream_pairs]
        a["_stream_seq"] = [pair[1] for pair in stream_pairs]
    else:
        a["_stream_ms"] = 0
        a["_stream_seq"] = np.arange(len(a))

    a = a.sort_values(
        ["sec_ms", "end_ms", "_stream_ms", "_stream_seq"],
        na_position="last",
    )

    # Live aggregate streams can update the same second multiple times.
    # Use the last update for each second when validating follow-through.
    a = a.groupby("sec_ms", as_index=False).tail(1)
    a = a.sort_values("sec_ms").reset_index(drop=True)

    return a


def validate_reversal_followthrough(
    *,
    aggs: pd.DataFrame,
    signal_ms: int,
    signal_mid: float,
    direction: str,
    ft_seconds: list[int],
    success_min_move: float,
    success_min_move_pct: float,
) -> dict[str, Any]:
    result: dict[str, Any] = {}

    for sec in ft_seconds:
        result[f"future_{sec}s_close"] = None
        result[f"future_{sec}s_move"] = None
        result[f"future_{sec}s_move_percent"] = None
        result[f"future_{sec}s_success"] = False

    if aggs.empty or direction not in {"up", "down"} or signal_mid <= 0:
        return result

    sign = 1 if direction == "up" else -1
    signal_sec_ms = (signal_ms // 1000) * 1000
    future = aggs[aggs["sec_ms"] > signal_sec_ms].copy()

    for sec in ft_seconds:
        target_sec_ms = signal_sec_ms + (sec * 1000)
        within = future[future["sec_ms"] <= target_sec_ms]

        if within.empty:
            continue

        future_close = safe_float(within.iloc[-1]["c"])
        if future_close is None:
            continue

        move = (future_close - signal_mid) * sign
        move_percent = (move / signal_mid) * 100.0
        required_move = max(
            success_min_move,
            (success_min_move_pct / 100.0) * signal_mid,
        )

        result[f"future_{sec}s_close"] = future_close
        result[f"future_{sec}s_move"] = round(move, 6)
        result[f"future_{sec}s_move_percent"] = round(move_percent, 6)
        result[f"future_{sec}s_success"] = bool(move >= required_move)

    if ft_seconds:
        max_sec = max(ft_seconds)
        horizon = future[future["sec_ms"] <= signal_sec_ms + max_sec * 1000]

        if not horizon.empty:
            highs = pd.to_numeric(horizon["h"], errors="coerce").dropna()
            lows = pd.to_numeric(horizon["l"], errors="coerce").dropna()

            if direction == "up":
                mfe = (float(highs.max()) - signal_mid) if not highs.empty else None
                mae = (signal_mid - float(lows.min())) if not lows.empty else None
            else:
                mfe = (signal_mid - float(lows.min())) if not lows.empty else None
                mae = (float(highs.max()) - signal_mid) if not highs.empty else None

            result[f"future_{max_sec}s_mfe"] = round(mfe, 6) if mfe is not None else None
            result[f"future_{max_sec}s_mae"] = round(mae, 6) if mae is not None else None

    return result


def find_quote_reversal_candidates_from_frames(
    *,
    symbol: str,
    quotes: pd.DataFrame,
    aggs: pd.DataFrame,
    tick_counts: list[int],
    lookback_ms: int,
    min_persist_ms: int,
    max_persist_ms: int,
    min_mid_move: float,
    min_mid_move_pct: float,
    max_spread_percent: float,
    ft_seconds: list[int],
    success_min_move: float,
    success_min_move_pct: float,
    dedup_gap_ms: int,
) -> pd.DataFrame:
    q = prepare_reversal_quotes(quotes)
    a = prepare_reversal_aggs(aggs)

    if q.empty or a.empty:
        return pd.DataFrame()

    tick_counts = sorted(set(int(x) for x in tick_counts if int(x) > 1))
    if not tick_counts:
        return pd.DataFrame()

    event_ms = q["event_ms"].to_numpy(dtype=np.int64)
    bid = q["bid"].to_numpy(dtype=float)
    ask = q["ask"].to_numpy(dtype=float)
    mid = q["mid"].to_numpy(dtype=float)
    spread = q["spread"].to_numpy(dtype=float)
    spread_percent = q["spread_percent"].to_numpy(dtype=float)

    candidates: list[dict[str, Any]] = []

    for end_idx in range(len(q)):
        for tick_count in tick_counts:
            start_idx = end_idx - tick_count + 1
            if start_idx < 0:
                continue

            first_ms = int(event_ms[start_idx])
            last_ms = int(event_ms[end_idx])
            persist_ms = last_ms - first_ms

            if persist_ms < min_persist_ms:
                continue
            if max_persist_ms > 0 and persist_ms > max_persist_ms:
                continue
            if lookback_ms > 0 and persist_ms > lookback_ms:
                continue

            first_bid = float(bid[start_idx])
            first_ask = float(ask[start_idx])
            first_mid = float(mid[start_idx])
            last_bid = float(bid[end_idx])
            last_ask = float(ask[end_idx])
            last_mid = float(mid[end_idx])

            bid_delta = last_bid - first_bid
            ask_delta = last_ask - first_ask
            mid_delta = last_mid - first_mid
            direction = direction_from_delta(mid_delta)

            if direction not in {"up", "down"}:
                continue

            sign = 1 if direction == "up" else -1

            if bid_delta * sign <= 0:
                continue
            if ask_delta * sign <= 0:
                continue
            if mid_delta * sign <= 0:
                continue

            abs_mid_move = abs(mid_delta)
            mid_move_percent = (abs_mid_move / first_mid) * 100.0 if first_mid > 0 else 0.0
            required_mid_move = max(
                min_mid_move,
                (min_mid_move_pct / 100.0) * first_mid,
            )

            if abs_mid_move < required_mid_move:
                continue

            last_spread_percent = float(spread_percent[end_idx])
            if max_spread_percent > 0 and last_spread_percent > max_spread_percent:
                continue

            validation = validate_reversal_followthrough(
                aggs=a,
                signal_ms=last_ms,
                signal_mid=last_mid,
                direction=direction,
                ft_seconds=ft_seconds,
                success_min_move=success_min_move,
                success_min_move_pct=success_min_move_pct,
            )

            best_success = any(
                bool(validation.get(f"future_{sec}s_success"))
                for sec in ft_seconds
            )

            candidates.append(
                {
                    "symbol": symbol,
                    "reversal_side": reversal_side_from_direction(direction),
                    "reversal_direction": direction,
                    "signal_ms": last_ms,
                    "signal_utc": pd.to_datetime(last_ms, unit="ms", utc=True).isoformat(),
                    "first_ms": first_ms,
                    "first_utc": pd.to_datetime(first_ms, unit="ms", utc=True).isoformat(),
                    "persist_ms": persist_ms,
                    "tick_count": tick_count,
                    "first_bid": round(first_bid, 6),
                    "first_ask": round(first_ask, 6),
                    "first_mid": round(first_mid, 6),
                    "last_bid": round(last_bid, 6),
                    "last_ask": round(last_ask, 6),
                    "last_mid": round(last_mid, 6),
                    "bid_delta": round(bid_delta, 6),
                    "ask_delta": round(ask_delta, 6),
                    "mid_delta": round(mid_delta, 6),
                    "abs_mid_move": round(abs_mid_move, 6),
                    "mid_move_percent": round(mid_move_percent, 6),
                    "last_spread": round(float(spread[end_idx]), 6),
                    "last_spread_percent": round(last_spread_percent, 6),
                    "same_dir_all": True,
                    "best_ft_success": bool(best_success),
                    **validation,
                }
            )

    if not candidates:
        return pd.DataFrame()

    df = pd.DataFrame(candidates)
    df = df.sort_values(
        ["reversal_side", "signal_ms", "abs_mid_move", "tick_count"],
        ascending=[True, True, False, True],
    ).reset_index(drop=True)

    # De-duplicate overlapping candidates. Multiple tick counts can identify
    # the same signal; keep the strongest one inside each side/gap.
    kept_rows = []
    last_kept_ms_by_side: dict[str, int] = {}

    for _, row in df.iterrows():
        side = str(row["reversal_side"])
        signal_ms = int(row["signal_ms"])
        last_kept_ms = last_kept_ms_by_side.get(side)

        if last_kept_ms is not None and signal_ms - last_kept_ms < dedup_gap_ms:
            continue

        kept_rows.append(row)
        last_kept_ms_by_side[side] = signal_ms

    if not kept_rows:
        return pd.DataFrame()

    return pd.DataFrame(kept_rows).reset_index(drop=True)


def summarize_reversal_side(candidates: pd.DataFrame, ft_seconds: list[int]) -> dict[str, Any]:
    if candidates.empty:
        return {
            "candidate_count": 0,
            "success_count": 0,
            "success_rate": None,
            "basis": "none",
        }

    success = candidates[candidates["best_ft_success"] == True].copy()
    basis = success if not success.empty else candidates

    summary: dict[str, Any] = {
        "candidate_count": int(len(candidates)),
        "success_count": int(len(success)),
        "success_rate": round(float(len(success) / len(candidates)), 6) if len(candidates) else None,
        "basis": "successes" if not success.empty else "all_candidates",
    }

    for col in [
        "tick_count",
        "persist_ms",
        "abs_mid_move",
        "mid_move_percent",
        "last_spread_percent",
        "bid_delta",
        "ask_delta",
        "mid_delta",
    ]:
        values = pd.to_numeric(basis[col], errors="coerce").dropna()
        if values.empty:
            continue

        summary[f"median_{col}"] = round(float(values.quantile(0.50)), 6)
        summary[f"p75_{col}"] = round(float(values.quantile(0.75)), 6)
        summary[f"p90_{col}"] = round(float(values.quantile(0.90)), 6)

    for sec in ft_seconds:
        col = f"future_{sec}s_success"
        if col in candidates.columns:
            summary[f"followthrough_{sec}s_rate"] = round(float(candidates[col].mean()), 6)

    return as_jsonable(summary)


def reversal_confidence(short_to_long: dict[str, Any], long_to_short: dict[str, Any]) -> str:
    success_count = int(short_to_long.get("success_count") or 0) + int(long_to_short.get("success_count") or 0)

    if success_count >= 100:
        return "high"
    if success_count >= 25:
        return "medium"
    if success_count >= 5:
        return "low"
    return "very_low"


def build_reversal_thresholds(side_summary: dict[str, Any], args) -> dict[str, Any]:
    if not side_summary or not side_summary.get("candidate_count"):
        return {}

    def int_value(key: str, default: int) -> int:
        value = safe_float(side_summary.get(key))
        if value is None:
            return default
        return max(1, int(round(value)))

    def float_value(key: str, default: float) -> float:
        value = safe_float(side_summary.get(key))
        if value is None:
            return default
        return round(value, 6)

    return {
        "close_thresholds": {
            "minimum_ticks": int_value("median_tick_count", 3),
            "minimum_persist_ms": int_value("median_persist_ms", args.reversal_min_persist_ms),
            "minimum_mid_move": float_value("median_abs_mid_move", args.reversal_min_mid_move),
            "min_mid_move_pct": float_value("median_mid_move_percent", args.reversal_min_mid_move_pct),
            "max_spread_pct": float_value("p90_last_spread_percent", args.reversal_max_spread_percent),
            "require_same_dir": True,
        },
        "reverse_thresholds": {
            "minimum_ticks": int_value("p75_tick_count", 4),
            "minimum_persist_ms": int_value("p75_persist_ms", args.reversal_min_persist_ms),
            "minimum_mid_move": float_value("p75_abs_mid_move", args.reversal_min_mid_move),
            "min_mid_move_pct": float_value("p75_mid_move_percent", args.reversal_min_mid_move_pct),
            "max_spread_pct": float_value("p75_last_spread_percent", args.reversal_max_spread_percent),
            "require_same_dir": True,
        },
    }


def run_symbol_reversal_analysis(
    symbol: str,
    input_dir: Path,
    run_id: str,
    args,
) -> dict:
    quotes_path = input_dir / f"quotes_{symbol}.csv"
    agg_path = input_dir / f"1secagg_{symbol}.csv"

    if not quotes_path.exists() or not agg_path.exists():
        raise FileNotFoundError(
            f"Missing quote/aggregate file(s) for reversal analysis: {quotes_path}, {agg_path}"
        )

    quotes = load_stream_csv(quotes_path)
    agg = load_stream_csv(agg_path)

    tick_counts = parse_int_csv_arg(args.reversal_tick_counts)
    ft_seconds = parse_int_csv_arg(args.reversal_ft_seconds)

    candidates = find_quote_reversal_candidates_from_frames(
        symbol=symbol,
        quotes=quotes,
        aggs=agg,
        tick_counts=tick_counts,
        lookback_ms=args.reversal_lookback_ms,
        min_persist_ms=args.reversal_min_persist_ms,
        max_persist_ms=args.reversal_max_persist_ms,
        min_mid_move=args.reversal_min_mid_move,
        min_mid_move_pct=args.reversal_min_mid_move_pct,
        max_spread_percent=args.reversal_max_spread_percent,
        ft_seconds=ft_seconds,
        success_min_move=args.reversal_success_min_move,
        success_min_move_pct=args.reversal_success_min_move_pct,
        dedup_gap_ms=args.reversal_dedup_gap_ms,
    )

    if candidates.empty:
        short_to_long = summarize_reversal_side(pd.DataFrame(), ft_seconds)
        long_to_short = summarize_reversal_side(pd.DataFrame(), ft_seconds)
        candidate_examples = []
    else:
        short_to_long = summarize_reversal_side(
            candidates[candidates["reversal_side"] == "short_to_long"],
            ft_seconds,
        )
        long_to_short = summarize_reversal_side(
            candidates[candidates["reversal_side"] == "long_to_short"],
            ft_seconds,
        )
        candidate_examples = records(
            candidates.sort_values(
                ["best_ft_success", "abs_mid_move"],
                ascending=[False, False],
            ),
            args.reversal_candidate_example_limit,
        )

    summary = {
        "document_type": "quote_rev_symbol",
        "run_id": run_id,
        "generated_at_utc": utc_now_iso(),
        "symbol": symbol,
        "input_dir": str(input_dir),
        "input_files": {
            "quotes": str(quotes_path),
            "1secagg": str(agg_path),
        },
        "settings": {
            "lookback_ms": args.reversal_lookback_ms,
            "tick_counts": tick_counts,
            "min_persist_ms": args.reversal_min_persist_ms,
            "max_persist_ms": args.reversal_max_persist_ms,
            "min_mid_move": args.reversal_min_mid_move,
            "min_mid_move_pct": args.reversal_min_mid_move_pct,
            "max_spread_percent": args.reversal_max_spread_percent,
            "ft_seconds": ft_seconds,
            "success_min_move": args.reversal_success_min_move,
            "success_min_move_pct": args.reversal_success_min_move_pct,
            "dedup_gap_ms": args.reversal_dedup_gap_ms,
        },
        "input_row_counts": {
            "quotes": len(quotes),
            "1secagg": len(agg),
        },
        "candidate_count": int(len(candidates)),
        "short_to_long": short_to_long,
        "long_to_short": long_to_short,
        "thresholds": {
            "short_to_long": build_reversal_thresholds(short_to_long, args),
            "long_to_short": build_reversal_thresholds(long_to_short, args),
        },
        "quality": {
            "confidence": reversal_confidence(short_to_long, long_to_short),
            "s2l_success": short_to_long.get("success_count", 0),
            "l2s_success": long_to_short.get("success_count", 0),
        },
        "candidate_examples": candidate_examples,
    }

    return as_jsonable(summary)


def make_reversal_batch_fields(reversal_summary: dict | None, error: Exception | None = None) -> dict:
    if error is not None:
        return {
            "qr_status": "error",
            "qr_candidates": None,
            "qr_conf": None,
            "qr_s2l_success": None,
            "qr_l2s_success": None,
            "qr_error": str(error),
        }

    if not reversal_summary:
        return {
            "qr_status": "disabled",
            "qr_candidates": None,
            "qr_conf": None,
            "qr_s2l_success": None,
            "qr_l2s_success": None,
            "qr_error": "",
        }

    return {
        "qr_status": "ok",
        "qr_candidates": reversal_summary.get("candidate_count"),
        "qr_conf": (reversal_summary.get("quality") or {}).get("confidence"),
        "qr_s2l_success": (reversal_summary.get("short_to_long") or {}).get("success_count"),
        "qr_l2s_success": (reversal_summary.get("long_to_short") or {}).get("success_count"),
        "qr_error": "",
    }

# =========================
# Analysis
# =========================

def run_symbol_analysis(
    symbol: str,
    input_dir: Path,
    run_id: str,
    max_prev_gap_ms: int = 2000,
    top_n_conditions: int = 25,
    top_n_condition_combos: int = 25,
) -> dict:
    trades_path = input_dir / f"trades_{symbol}.csv"
    quotes_path = input_dir / f"quotes_{symbol}.csv"
    agg_path = input_dir / f"1secagg_{symbol}.csv"

    missing_paths = [
        str(path)
        for path in [trades_path, quotes_path, agg_path]
        if not path.exists()
    ]

    if missing_paths:
        raise FileNotFoundError(
            f"Missing required input file(s) for {symbol}: {missing_paths}"
        )

    trades = load_stream_csv(trades_path)
    quotes = load_stream_csv(quotes_path)
    agg = load_stream_csv(agg_path)

    validate_required_columns(trades, ["t", "p", "s", "c"], "trades", trades_path)
    validate_required_columns(quotes, ["t", "bp", "ap", "bs", "as"], "quotes", quotes_path)
    validate_required_columns(agg, ["s", "c", "h", "l"], "1secagg", agg_path)

    coerce_numeric_columns(trades, ["t", "p", "s"], "trades", trades_path)
    coerce_numeric_columns(quotes, ["t", "bp", "ap", "bs", "as"], "quotes", quotes_path)
    coerce_numeric_columns(agg, ["s", "c", "h", "l"], "1secagg", agg_path)

    trades = trades.dropna(subset=["t", "p", "s"]).copy()
    quotes = quotes.dropna(subset=["t", "bp", "ap", "bs", "as"]).copy()
    agg = agg.dropna(subset=["s", "c", "h", "l"]).copy()

    if trades.empty:
        raise ValueError(f"No usable trade rows for {symbol} after loading {trades_path.name}.")

    if quotes.empty:
        raise ValueError(f"No usable quote rows for {symbol} after loading {quotes_path.name}.")

    if agg.empty:
        raise ValueError(f"No usable 1secagg rows for {symbol} after loading {agg_path.name}.")

    trades["sec_ms"] = (trades["t"] // 1000) * 1000
    quotes["sec_ms"] = (quotes["t"] // 1000) * 1000
    agg["sec_ms"] = agg["s"]

    for frame in (trades, quotes, agg):
        frame["sec_utc"] = pd.to_datetime(frame["sec_ms"], unit="ms", utc=True)

    trades["cond_tuple"] = trades["c"].apply(parse_condition_array)
    trades["cond_combo"] = trades["cond_tuple"].apply(lambda t: str(list(t)) if len(t) else "[]")
    trades["has_no_condition"] = trades["cond_tuple"].apply(lambda t: len(t) == 0)

    trades = sort_by_existing(trades, ["t", "redis_stream_id"])
    trades["prev_trade_price"] = trades["p"].shift(1)
    trades["tick_delta"] = trades["p"] - trades["prev_trade_price"]

    trades["tick_direction"] = np.select(
        [trades["tick_delta"] > 0, trades["tick_delta"] < 0],
        ["up", "down"],
        default="flat",
    )

    trades["notional"] = trades["p"] * trades["s"]

    trade_sec = (
        trades.groupby("sec_ms", sort=False)
        .agg(
            trade_count=("p", "size"),
            share_volume=("s", "sum"),
            trade_notional=("notional", "sum"),
            first_trade_price=("p", "first"),
            last_trade_price=("p", "last"),
            min_trade_price=("p", "min"),
            max_trade_price=("p", "max"),
            mean_trade_price=("p", "mean"),
            tick_up_count=("tick_direction", lambda x: (x == "up").sum()),
            tick_down_count=("tick_direction", lambda x: (x == "down").sum()),
            tick_flat_count=("tick_direction", lambda x: (x == "flat").sum()),
            no_condition_trade_count=("has_no_condition", "sum"),
        )
        .reset_index()
    )

    trade_sec["trade_vwap"] = (
        trade_sec["trade_notional"] / trade_sec["share_volume"].replace(0, np.nan)
    )

    exploded = trades[["sec_ms", "cond_tuple"]].explode("cond_tuple")
    exploded_conditions = exploded[exploded["cond_tuple"].notna()].rename(
        columns={"cond_tuple": "condition"}
    )

    if not exploded_conditions.empty:
        cond_sec_counts = (
            exploded_conditions.groupby(["sec_ms", "condition"])
            .size()
            .unstack(fill_value=0)
        )
        cond_sec_counts.columns = [
            f"cond_{int(c)}_trade_count" for c in cond_sec_counts.columns
        ]
        cond_sec_counts = cond_sec_counts.reset_index()
        trade_sec = trade_sec.merge(cond_sec_counts, on="sec_ms", how="left")

        for col in trade_sec.columns:
            if col.startswith("cond_") and col.endswith("_trade_count"):
                trade_sec[col] = trade_sec[col].fillna(0).astype(int)

    quotes = sort_by_existing(quotes, ["t", "redis_stream_id"])
    quotes["spread"] = quotes["ap"] - quotes["bp"]
    quotes["mid"] = (quotes["ap"] + quotes["bp"]) / 2
    quotes["quote_prev_mid"] = quotes["mid"].shift(1)
    quotes["quote_mid_delta_tick"] = quotes["mid"] - quotes["quote_prev_mid"]

    quotes["quote_tick_dir"] = np.select(
        [quotes["quote_mid_delta_tick"] > 0, quotes["quote_mid_delta_tick"] < 0],
        ["up", "down"],
        default="flat",
    )

    quote_sec = (
        quotes.groupby("sec_ms", sort=False)
        .agg(
            quote_update_count=("ap", "size"),
            first_bid=("bp", "first"),
            last_bid=("bp", "last"),
            mean_bid=("bp", "mean"),
            first_ask=("ap", "first"),
            last_ask=("ap", "last"),
            mean_ask=("ap", "mean"),
            first_mid=("mid", "first"),
            last_mid=("mid", "last"),
            mean_mid=("mid", "mean"),
            first_spread=("spread", "first"),
            last_spread=("spread", "last"),
            mean_spread=("spread", "mean"),
            median_spread=("spread", "median"),
            max_spread=("spread", "max"),
            min_spread=("spread", "min"),
            last_bid_size=("bs", "last"),
            last_ask_size=("as", "last"),
            mean_bid_size=("bs", "mean"),
            mean_ask_size=("as", "mean"),
            quote_mid_up_count=("quote_tick_dir", lambda x: (x == "up").sum()),
            quote_mid_down_count=("quote_tick_dir", lambda x: (x == "down").sum()),
            quote_mid_flat_count=("quote_tick_dir", lambda x: (x == "flat").sum()),
        )
        .reset_index()
    )

    agg_raw = sort_by_existing(agg, ["sec_ms", "redis_stream_id"]).copy()
    agg_raw["prev_agg_sec_ms"] = agg_raw["sec_ms"].shift(1)
    agg_raw["agg_gap_ms"] = agg_raw["sec_ms"] - agg_raw["prev_agg_sec_ms"]
    agg_raw["prev_close"] = agg_raw["c"].shift(1)
    agg_raw["close_delta"] = agg_raw["c"] - agg_raw["prev_close"]
    agg_raw["abs_close_delta"] = agg_raw["close_delta"].abs()

    agg_raw["direction"] = np.select(
        [agg_raw["close_delta"] > 0, agg_raw["close_delta"] < 0],
        ["up", "down"],
        default="flat",
    )

    joined = (
        agg_raw.merge(trade_sec, on="sec_ms", how="inner")
        .merge(quote_sec, on="sec_ms", how="inner")
    )
    joined = joined[joined["agg_gap_ms"].le(max_prev_gap_ms)].copy()
    joined = sort_by_existing(joined, ["sec_ms", "redis_stream_id"])

    if joined.empty:
        raise ValueError(
            f"No joined rows for {symbol}. "
            f"Check whether trades, quotes, and 1secagg overlap by second."
        )

    for col in ["last_bid", "last_ask", "last_mid", "last_spread", "mean_spread"]:
        joined[f"{col}_delta"] = joined[col] - joined[col].shift(1)

    joined["agg_range"] = joined["h"] - joined["l"]
    joined["spread_widened"] = joined["last_spread_delta"] > 0

    joined["mid_reprice_dir"] = np.select(
        [joined["last_mid_delta"] > 0, joined["last_mid_delta"] < 0],
        ["up", "down"],
        default="flat",
    )

    joined["bid_reprice_dir"] = np.select(
        [joined["last_bid_delta"] > 0, joined["last_bid_delta"] < 0],
        ["up", "down"],
        default="flat",
    )

    joined["ask_reprice_dir"] = np.select(
        [joined["last_ask_delta"] > 0, joined["last_ask_delta"] < 0],
        ["up", "down"],
        default="flat",
    )

    p50, p90, p95, p99 = joined["abs_close_delta"].quantile([0.5, 0.9, 0.95, 0.99]).values

    joined["is_normal"] = joined["abs_close_delta"] <= p50
    joined["is_large"] = joined["abs_close_delta"] >= p90
    joined["is_busy"] = joined["abs_close_delta"] >= p95
    joined["is_extreme"] = joined["abs_close_delta"] >= p99

    joined["regime"] = np.select(
        [
            joined["is_extreme"],
            joined["is_busy"],
            joined["is_large"],
            joined["is_normal"],
        ],
        ["extreme", "busy", "large", "normal"],
        default="middle",
    )

    joined["mid_same_dir"] = (
        joined["mid_reprice_dir"] == joined["direction"]
    ) & (joined["direction"] != "flat")

    joined["bid_same_dir"] = (
        joined["bid_reprice_dir"] == joined["direction"]
    ) & (joined["direction"] != "flat")

    joined["ask_same_dir"] = (
        joined["ask_reprice_dir"] == joined["direction"]
    ) & (joined["direction"] != "flat")

    def summarize_regime(label: str, mask: pd.Series) -> dict:
        d = joined.loc[mask]
        nonflat = d[d["direction"] != "flat"]

        return {
            "regime": label,
            "rows": len(d),
            "row_share": len(d) / len(joined),
            "median_abs_close_delta": d["abs_close_delta"].median(),
            "mean_abs_close_delta": d["abs_close_delta"].mean(),
            "median_trade_count": d["trade_count"].median(),
            "median_share_volume": d["share_volume"].median(),
            "med_quote_updates": d["quote_update_count"].median(),
            "median_last_spread": d["last_spread"].median(),
            "med_abs_mid_delta": d["last_mid_delta"].abs().median(),
            "spread_widen_rate": d["spread_widened"].mean(),
            "up_rate": (d["direction"] == "up").mean(),
            "down_rate": (d["direction"] == "down").mean(),
            "flat_rate": (d["direction"] == "flat").mean(),
            "mid_same_dir_rate": (
                nonflat["mid_same_dir"].mean() if len(nonflat) else np.nan
            ),
            "bid_same_dir_rate": (
                nonflat["bid_same_dir"].mean() if len(nonflat) else np.nan
            ),
            "ask_same_dir_rate": (
                nonflat["ask_same_dir"].mean() if len(nonflat) else np.nan
            ),
        }

    regime_summary = pd.DataFrame(
        [
            summarize_regime("normal", joined["is_normal"]),
            summarize_regime("large", joined["is_large"]),
            summarize_regime("busy", joined["is_busy"]),
            summarize_regime("extreme", joined["is_extreme"]),
        ]
    )

    def direction_rows(df: pd.DataFrame, scope: str) -> list[dict]:
        rows = []

        if len(df) == 0:
            return rows

        for direction, d in df.groupby("direction"):
            rows.append(
                {
                    "scope": scope,
                    "direction": direction,
                    "rows": len(d),
                    "scope_share": len(d) / len(df),
                    "median_close_delta": d["close_delta"].median(),
                    "median_abs_close_delta": d["abs_close_delta"].median(),
                    "median_trade_count": d["trade_count"].median(),
                    "median_share_volume": d["share_volume"].median(),
                    "med_quote_updates": d["quote_update_count"].median(),
                    "median_last_spread": d["last_spread"].median(),
                    "median_last_bid_delta": d["last_bid_delta"].median(),
                    "median_last_ask_delta": d["last_ask_delta"].median(),
                    "median_last_mid_delta": d["last_mid_delta"].median(),
                    "bid_same_rate": (
                        d["bid_same_dir"].mean()
                        if direction != "flat"
                        else np.nan
                    ),
                    "ask_same_rate": (
                        d["ask_same_dir"].mean()
                        if direction != "flat"
                        else np.nan
                    ),
                    "mid_same_rate": (
                        d["mid_same_dir"].mean()
                        if direction != "flat"
                        else np.nan
                    ),
                    "spread_widen_rate": d["spread_widened"].mean(),
                }
            )

        return rows

    direction_summary = pd.DataFrame(
        direction_rows(joined, "all_joined")
        + direction_rows(joined[joined["is_large"]], "large")
        + direction_rows(joined[joined["is_busy"]], "busy")
        + direction_rows(joined[joined["is_extreme"]], "extreme")
    )

    joined_unique = (
        sort_by_existing(joined, ["sec_ms", "redis_stream_id"])
        .drop_duplicates("sec_ms", keep="last")
        .copy()
    )

    map_cols = [
        "sec_ms",
        "c",
        "prev_close",
        "close_delta",
        "abs_close_delta",
        "direction",
        "is_normal",
        "is_large",
        "is_busy",
        "is_extreme",
        "trade_count",
        "share_volume",
        "quote_update_count",
        "last_spread",
        "mean_spread",
        "last_bid_delta",
        "last_ask_delta",
        "last_mid_delta",
        "bid_same_dir",
        "ask_same_dir",
        "mid_same_dir",
        "spread_widened",
    ]

    trade_join = trades.merge(joined_unique[map_cols], on="sec_ms", how="inner")
    trade_join["is_tick_up"] = trade_join["tick_direction"].eq("up")
    trade_join["is_tick_down"] = trade_join["tick_direction"].eq("down")
    trade_join["is_tick_flat"] = trade_join["tick_direction"].eq("flat")
    trade_join["is_bar_up"] = trade_join["direction"].eq("up")
    trade_join["is_bar_down"] = trade_join["direction"].eq("down")
    trade_join["is_bar_flat"] = trade_join["direction"].eq("flat")

    baseline = {
        "matched_trades": len(trade_join),
        "tick_up_rate": trade_join["is_tick_up"].mean(),
        "bar_up_rate": trade_join["is_bar_up"].mean(),
        "large_rate": trade_join["is_large"].mean(),
        "busy_rate": trade_join["is_busy"].mean(),
        "extreme_rate": trade_join["is_extreme"].mean(),
    }

    cond_trade = trade_join[
        [
            "sec_ms",
            "cond_tuple",
            "is_tick_up",
            "is_tick_down",
            "is_tick_flat",
            "is_bar_up",
            "is_bar_down",
            "is_bar_flat",
            "is_large",
            "is_busy",
            "is_extreme",
            "abs_close_delta",
            "trade_count",
            "share_volume",
            "quote_update_count",
            "last_spread",
            "direction",
            "mid_same_dir",
            "bid_same_dir",
            "ask_same_dir",
            "spread_widened",
        ]
    ].explode("cond_tuple")

    cond_trade = cond_trade[cond_trade["cond_tuple"].notna()].rename(
        columns={"cond_tuple": "condition_code"}
    )

    no_trade = trade_join[trade_join["has_no_condition"]][
        [
            "sec_ms",
            "is_tick_up",
            "is_tick_down",
            "is_tick_flat",
            "is_bar_up",
            "is_bar_down",
            "is_bar_flat",
            "is_large",
            "is_busy",
            "is_extreme",
            "abs_close_delta",
            "trade_count",
            "share_volume",
            "quote_update_count",
            "last_spread",
            "direction",
            "mid_same_dir",
            "bid_same_dir",
            "ask_same_dir",
            "spread_widened",
        ]
    ].copy()

    no_trade["condition_code"] = -1

    cond_trade = pd.concat([cond_trade, no_trade], ignore_index=True)
    cond_trade["condition_code"] = cond_trade["condition_code"].astype(int)

    def condition_agg(g: pd.DataFrame) -> pd.Series:
        nonflat = g[g["direction"] != "flat"]

        return pd.Series(
            {
                "trade_observations": len(g),
                "seconds_present": g["sec_ms"].nunique(),
                "matched_share_multi": len(g) / len(trade_join),
                "tick_up_rate": g["is_tick_up"].mean(),
                "bar_up_rate": g["is_bar_up"].mean(),
                "large_rate": g["is_large"].mean(),
                "busy_rate": g["is_busy"].mean(),
                "extreme_rate": g["is_extreme"].mean(),
                "median_abs_close_delta": g["abs_close_delta"].median(),
                "med_trades_same_sec": g["trade_count"].median(),
                "med_volume_same_sec": g["share_volume"].median(),
                "med_quotes_same_sec": g["quote_update_count"].median(),
                "med_spread_same_sec": g["last_spread"].median(),
                "mid_same_dir_rate": (
                    nonflat["mid_same_dir"].mean()
                    if len(nonflat)
                    else np.nan
                ),
                "spread_widen_rate": g["spread_widened"].mean(),
            }
        )

    condition_summary = cond_trade.groupby("condition_code").apply(condition_agg).reset_index()
    condition_summary["meaning"] = (
        condition_summary["condition_code"]
        .map(CONDITION_LABELS)
        .fillna("Unmapped condition")
    )
    condition_summary["tick_up_lift_pts"] = (
        condition_summary["tick_up_rate"] - baseline["tick_up_rate"]
    ) * 100
    condition_summary["bar_up_lift_pts"] = (
        condition_summary["bar_up_rate"] - baseline["bar_up_rate"]
    ) * 100
    condition_summary["large_lift_pts"] = (
        condition_summary["large_rate"] - baseline["large_rate"]
    ) * 100

    condition_summary = condition_summary.sort_values("trade_observations", ascending=False)

    combo_summary = (
        trade_join.groupby("cond_combo")
        .apply(
            lambda g: pd.Series(
                {
                    "trade_observations": len(g),
                    "seconds_present": g["sec_ms"].nunique(),
                    "matched_share": len(g) / len(trade_join),
                    "tick_up_rate": g["is_tick_up"].mean(),
                    "bar_up_rate": g["is_bar_up"].mean(),
                    "large_rate": g["is_large"].mean(),
                    "busy_rate": g["is_busy"].mean(),
                    "extreme_rate": g["is_extreme"].mean(),
                    "median_abs_close_delta": g["abs_close_delta"].median(),
                    "med_trades_same_sec": g["trade_count"].median(),
                    "med_quotes_same_sec": g["quote_update_count"].median(),
                    "med_spread_same_sec": g["last_spread"].median(),
                    "spread_widen_rate": g["spread_widened"].mean(),
                }
            )
        )
        .reset_index()
    )

    combo_summary["tick_up_lift_pts"] = (
        combo_summary["tick_up_rate"] - baseline["tick_up_rate"]
    ) * 100
    combo_summary["bar_up_lift_pts"] = (
        combo_summary["bar_up_rate"] - baseline["bar_up_rate"]
    ) * 100
    combo_summary["large_lift_pts"] = (
        combo_summary["large_rate"] - baseline["large_rate"]
    ) * 100

    combo_summary = combo_summary.sort_values("trade_observations", ascending=False)

    condition_cols = [
        c for c in joined.columns
        if c.startswith("cond_") and c.endswith("_trade_count")
    ]

    cond_codes = sorted(
        int(re.match(r"cond_(\d+)_trade_count", c).group(1))
        for c in condition_cols
        if re.match(r"cond_(\d+)_trade_count", c)
    )

    presence_rows = []

    for label, mask in [
        ("normal", joined["is_normal"]),
        ("large", joined["is_large"]),
        ("busy", joined["is_busy"]),
        ("extreme", joined["is_extreme"]),
    ]:
        d = joined[mask]

        for code in cond_codes:
            col = f"cond_{code}_trade_count"
            present = d[col] > 0

            presence_rows.append(
                {
                    "regime": label,
                    "condition_code": code,
                    "meaning": CONDITION_LABELS.get(code, "Unmapped condition"),
                    "seconds_in_regime": len(d),
                    "seconds_present": present.sum(),
                    "presence_rate": present.mean(),
                    "med_count_present": (
                        d.loc[present, col].median() if present.any() else np.nan
                    ),
                    "mean_count_present": (
                        d.loc[present, col].mean() if present.any() else np.nan
                    ),
                }
            )

        present = d["no_condition_trade_count"] > 0

        presence_rows.append(
            {
                "regime": label,
                "condition_code": -1,
                "meaning": "No condition array",
                "seconds_in_regime": len(d),
                "seconds_present": present.sum(),
                "presence_rate": present.mean(),
                "med_count_present": (
                    d.loc[present, "no_condition_trade_count"].median()
                    if present.any()
                    else np.nan
                ),
                "mean_count_present": (
                    d.loc[present, "no_condition_trade_count"].mean()
                    if present.any()
                    else np.nan
                ),
            }
        )

    cond_presence = pd.DataFrame(presence_rows)

    u = joined_unique.sort_values("sec_ms").reset_index(drop=True).copy()
    u["next_sec_ms"] = u["sec_ms"].shift(-1)
    u["next_gap_ms"] = u["next_sec_ms"] - u["sec_ms"]

    for col in [
        "abs_close_delta",
        "is_large",
        "is_busy",
        "is_extreme",
        "direction",
    ]:
        u[f"next_{col}"] = u[col].shift(-1)

    corr_rows = []

    for feat in [
        "trade_count",
        "share_volume",
        "quote_update_count",
        "last_spread",
        "mean_spread",
        "last_mid_delta",
        "last_bid_delta",
        "last_ask_delta",
    ]:
        valid = u[[feat, "abs_close_delta"]].dropna()

        corr_rows.append(
            {
                "relationship": "same_sec_vs_abs_move",
                "feature": feat,
                "spearman_corr": valid[feat].corr(valid["abs_close_delta"], method="spearman"),
                "pearson_corr": valid[feat].corr(valid["abs_close_delta"], method="pearson"),
                "n": len(valid),
            }
        )

        valid_next = u.loc[
            u["next_gap_ms"].le(max_prev_gap_ms),
            [feat, "next_abs_close_delta"],
        ].dropna()

        corr_rows.append(
            {
                "relationship": "feat_vs_next_abs_move",
                "feature": feat,
                "spearman_corr": valid_next[feat].corr(
                    valid_next["next_abs_close_delta"],
                    method="spearman",
                ),
                "pearson_corr": valid_next[feat].corr(
                    valid_next["next_abs_close_delta"],
                    method="pearson",
                ),
                "n": len(valid_next),
            }
        )

    corr_summary = pd.DataFrame(corr_rows)

    lead_base = u[u["next_gap_ms"].le(max_prev_gap_ms)].copy()

    lead_profile_summary = pd.DataFrame(
        [
            {
                "lead_profile": label,
                "rows": len(d),
                "med_curr_abs_delta": d["abs_close_delta"].median(),
                "med_curr_trades": d["trade_count"].median(),
                "med_curr_volume": d["share_volume"].median(),
                "med_curr_quotes": d["quote_update_count"].median(),
                "med_curr_spread": d["last_spread"].median(),
                "med_curr_mean_spread": d["mean_spread"].median(),
                "med_next_abs_delta": d["next_abs_close_delta"].median(),
                "next_up_rate": (d["next_direction"] == "up").mean(),
                "next_down_rate": (d["next_direction"] == "down").mean(),
            }
            for label, d in [
                ("pre_next_normal", lead_base[lead_base["next_abs_close_delta"] <= p50]),
                ("pre_next_large", lead_base[lead_base["next_abs_close_delta"] >= p90]),
                ("pre_next_busy", lead_base[lead_base["next_abs_close_delta"] >= p95]),
                ("pre_next_extreme", lead_base[lead_base["next_abs_close_delta"] >= p99]),
            ]
        ]
    )

    summary = {
        "document_type": "px_move_symbol",
        "run_id": run_id,
        "generated_at_utc": utc_now_iso(),
        "symbol": symbol,
        "input_dir": str(input_dir),
        "input_files": {
            "trades": str(trades_path),
            "quotes": str(quotes_path),
            "1secagg": str(agg_path),
        },
        "input_row_counts": {
            "trades": len(trades),
            "quotes": len(quotes),
            "1secagg": len(agg),
        },
        "joined_rows": len(joined),
        "joined_secs": len(joined_unique),
        "max_prev_gap_ms": max_prev_gap_ms,
        "thresholds": {
            "normal_p50_max": p50,
            "large_p90_min": p90,
            "busy_p95_min": p95,
            "extreme_p99_min": p99,
        },
        "trade_baseline": baseline,
        "regime_summary": records(regime_summary),
        "quote_reprice_by_dir": records(direction_summary),
        "top_cond_moves": records(condition_summary, top_n_conditions),
        "top_cond_combos": records(combo_summary, top_n_condition_combos),
        "cond_presence": records(cond_presence),
        "lead_lag_corr": records(corr_summary),
        "lead_profiles": records(lead_profile_summary),
        "limits": {
            "top_n_conditions": top_n_conditions,
            "top_n_condition_combos": top_n_condition_combos,
        },
        "conclusion": {
            "main_driver": "Regime change, not a single condition-code driver.",
            "pattern": "Material price moves coincide with higher trades/sec, volume, quote updates, wider spreads, and quote repricing in the move direction.",
        },
    }

    return as_jsonable(summary)


# =========================
# Batch summary
# =========================

def make_batch_row(summary: dict) -> dict:
    thresholds = summary.get("thresholds", {})
    baseline = summary.get("trade_baseline", {})
    input_row_counts = summary.get("input_row_counts", {})

    return {
        "symbol": summary.get("symbol"),
        "status": "ok",
        "joined_rows": summary.get("joined_rows"),
        "joined_secs": summary.get(
            "joined_secs"
        ),
        "trades_rows": input_row_counts.get("trades"),
        "quotes_rows": input_row_counts.get("quotes"),
        "1secagg_rows": input_row_counts.get("1secagg"),
        "normal_p50_max": thresholds.get("normal_p50_max"),
        "large_p90_min": thresholds.get("large_p90_min"),
        "busy_p95_min": thresholds.get("busy_p95_min"),
        "extreme_p99_min": thresholds.get("extreme_p99_min"),
        "matched_trades": baseline.get("matched_trades"),
        "tick_up_rate": baseline.get("tick_up_rate"),
        "bar_up_rate": baseline.get("bar_up_rate"),
        "large_rate": baseline.get("large_rate"),
        "busy_rate": baseline.get("busy_rate"),
        "extreme_rate": baseline.get("extreme_rate"),
        "error": "",
    }


def make_error_batch_row(symbol: str, exc: Exception) -> dict:
    return {
        "symbol": symbol,
        "status": "error",
        "joined_rows": None,
        "joined_secs": None,
        "trades_rows": None,
        "quotes_rows": None,
        "1secagg_rows": None,
        "normal_p50_max": None,
        "large_p90_min": None,
        "busy_p95_min": None,
        "extreme_p99_min": None,
        "matched_trades": None,
        "tick_up_rate": None,
        "bar_up_rate": None,
        "large_rate": None,
        "busy_rate": None,
        "extreme_rate": None,
        "error": str(exc),
    }


# =========================
# MongoDB writes
# =========================

async def assert_existing_mongo_target(client, db_name: str, collection_name: str) -> None:
    """
    Refuse to create MongoDB database or collection.

    MongoDB creates databases/collections lazily on first write.
    This guard checks they already exist before any write is attempted.
    """
    existing_databases = await client.list_database_names()

    if db_name not in existing_databases:
        raise RuntimeError(
            f"Mongo database does not exist: {db_name!r}. "
            "Refusing to create a new database. "
            "Create the database/collection first, or fix --mongo-db."
        )

    db = client[db_name]
    existing_collections = await db.list_collection_names()

    if collection_name not in existing_collections:
        raise RuntimeError(
            f"Mongo collection does not exist: {db_name!r}.{collection_name!r}. "
            "Refusing to create a new collection. "
            "Create the collection first, or fix --mongo-collection."
        )


async def connect_mongo(args):
    client = AsyncIOMotorClient(args.mongo_uri)
    await client.admin.command("ping")

    await assert_existing_mongo_target(
        client=client,
        db_name=args.mongo_db,
        collection_name=args.mongo_collection,
    )

    db = client[args.mongo_db]
    collection = db[args.mongo_collection]

    return client, collection


async def write_symbol_summary(collection, summary: dict, write_mode: str) -> dict:
    if write_mode == "insert-history":
        summary["_id"] = f"symbol:{summary['symbol']}:{summary['run_id']}"
        result = await collection.insert_one(summary)

        return {
            "write_mode": write_mode,
            "mongo_id": str(result.inserted_id),
        }

    if write_mode == "upsert-latest":
        summary["_id"] = f"symbol:{summary['symbol']}:latest"
        summary["is_latest"] = True

        result = await collection.replace_one(
            {"_id": summary["_id"]},
            summary,
            upsert=True,
        )

        return {
            "write_mode": write_mode,
            "matched_count": result.matched_count,
            "modified_count": result.modified_count,
            "upserted_id": (
                str(result.upserted_id)
                if result.upserted_id is not None
                else None
            ),
        }

    raise ValueError(f"Unknown write mode: {write_mode}")



async def write_quote_reversal_summary(collection, summary: dict, write_mode: str) -> dict:
    if write_mode == "insert-history":
        summary["_id"] = f"quote_reversal_symbol:{summary['symbol']}:{summary['run_id']}"
        result = await collection.insert_one(summary)

        return {
            "write_mode": write_mode,
            "mongo_id": str(result.inserted_id),
        }

    if write_mode == "upsert-latest":
        summary["_id"] = f"quote_reversal_symbol:{summary['symbol']}:latest"
        summary["is_latest"] = True

        result = await collection.replace_one(
            {"_id": summary["_id"]},
            summary,
            upsert=True,
        )

        return {
            "write_mode": write_mode,
            "matched_count": result.matched_count,
            "modified_count": result.modified_count,
            "upserted_id": (
                str(result.upserted_id)
                if result.upserted_id is not None
                else None
            ),
        }

    raise ValueError(f"Unknown write mode: {write_mode}")


async def write_batch_summary(collection, batch_summary: dict, write_mode: str) -> dict:
    if write_mode == "insert-history":
        batch_summary["_id"] = f"batch:{batch_summary['run_id']}"
        result = await collection.insert_one(batch_summary)

        return {
            "write_mode": write_mode,
            "mongo_id": str(result.inserted_id),
        }

    if write_mode == "upsert-latest":
        batch_summary["_id"] = "batch:latest"
        batch_summary["is_latest"] = True

        result = await collection.replace_one(
            {"_id": batch_summary["_id"]},
            batch_summary,
            upsert=True,
        )

        return {
            "write_mode": write_mode,
            "matched_count": result.matched_count,
            "modified_count": result.modified_count,
            "upserted_id": (
                str(result.upserted_id)
                if result.upserted_id is not None
                else None
            ),
        }

    raise ValueError(f"Unknown write mode: {write_mode}")


async def run_one_symbol(
    symbol: str,
    input_dir: Path,
    run_id: str,
    collection,
    args,
) -> dict:
    print(f"Running {symbol}...", flush=True)

    summary = run_symbol_analysis(
        symbol=symbol,
        input_dir=input_dir,
        run_id=run_id,
        max_prev_gap_ms=args.max_prev_gap_ms,
        top_n_conditions=args.top_n_conditions,
        top_n_condition_combos=args.top_n_condition_combos,
    )

    mongo_write = await write_symbol_summary(
        collection=collection,
        summary=summary,
        write_mode=args.write_mode,
    )

    summary["mongo_write"] = mongo_write

    if not args.disable_reversal_analysis:
        reversal_summary = run_symbol_reversal_analysis(
            symbol=symbol,
            input_dir=input_dir,
            run_id=run_id,
            args=args,
        )
        reversal_mongo_write = await write_quote_reversal_summary(
            collection=collection,
            summary=reversal_summary,
            write_mode=args.write_mode,
        )
        reversal_summary["mongo_write"] = reversal_mongo_write
        summary["quote_rev_ref"] = {
            "symbol": reversal_summary.get("symbol"),
            "run_id": reversal_summary.get("run_id"),
            "mongo_id": reversal_summary.get("_id"),
            "candidate_count": reversal_summary.get("candidate_count"),
            "confidence": (reversal_summary.get("quality") or {}).get("confidence"),
        }
        summary["quote_rev_write"] = reversal_mongo_write

    print(
        f"Finished {symbol}: {summary.get('joined_rows')} joined rows. "
        f"Mongo write: {mongo_write.get('write_mode')}. "
        f"Reversal analysis: {'disabled' if args.disable_reversal_analysis else 'written'}.",
        flush=True,
    )

    return summary




def build_worker_args_dict(args) -> dict[str, Any]:
    """
    Convert argparse Namespace into a process-safe plain dict.

    Mongo objects are intentionally not included in worker state. Workers do
    CPU/file analysis only; the parent process performs Mongo writes.
    """
    out: dict[str, Any] = {}

    for key, value in vars(args).items():
        if isinstance(value, Path):
            out[key] = str(value)
        else:
            out[key] = value

    return out


def run_symbol_analysis_worker(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Process one symbol inside a separate worker process.

    This avoids trying to pickle Motor/Mongo clients. The returned summaries are
    written to Mongo by the parent process as each worker completes.
    """
    symbol = str(payload["symbol"])
    input_dir = Path(payload["input_dir"])
    run_id = str(payload["run_id"])
    args = SimpleNamespace(**payload["args"])

    try:
        summary = run_symbol_analysis(
            symbol=symbol,
            input_dir=input_dir,
            run_id=run_id,
            max_prev_gap_ms=args.max_prev_gap_ms,
            top_n_conditions=args.top_n_conditions,
            top_n_condition_combos=args.top_n_condition_combos,
        )

        reversal_summary = None
        reversal_error_record = None

        if not args.disable_reversal_analysis:
            try:
                reversal_summary = run_symbol_reversal_analysis(
                    symbol=symbol,
                    input_dir=input_dir,
                    run_id=run_id,
                    args=args,
                )
            except Exception as exc:
                reversal_error_record = {
                    "symbol": symbol,
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                }
                if args.print_tracebacks:
                    reversal_error_record["traceback"] = traceback.format_exc()

                if args.fail_fast:
                    raise

        return {
            "symbol": symbol,
            "summary": summary,
            "reversal_summary": reversal_summary,
            "reversal_error": reversal_error_record,
            "error": None,
        }

    except Exception as exc:
        error_record = {
            "symbol": symbol,
            "error": str(exc),
            "error_type": type(exc).__name__,
        }
        if args.print_tracebacks:
            error_record["traceback"] = traceback.format_exc()

        return {
            "symbol": symbol,
            "summary": None,
            "reversal_summary": None,
            "reversal_error": None,
            "error": error_record,
        }

async def run_batch(
    input_dir: Path,
    run_id: str,
    collection,
    args,
) -> dict:
    symbols, incomplete_symbols = discover_symbols(input_dir)

    if not symbols:
        raise RuntimeError(
            f"No complete symbol file sets found in {input_dir}. "
            f"Expected trades_<SYMBOL>.csv, quotes_<SYMBOL>.csv, and 1secagg_<SYMBOL>.csv."
        )

    batch_rows = []
    summaries = []
    errors = []
    reversal_errors = []
    reversal_summaries = []

    workers = max(1, int(getattr(args, "workers", 1) or 1))

    print(f"Found {len(symbols)} complete symbol set(s).", flush=True)
    print(f"Input directory: {input_dir}", flush=True)
    print(f"Mongo database:  {args.mongo_db}", flush=True)
    print(f"Mongo collection: {args.mongo_collection}", flush=True)
    print(
        f"Quote reversal analysis: {'disabled' if args.disable_reversal_analysis else 'enabled'}",
        flush=True,
    )
    print(f"Symbol workers: {workers}", flush=True)

    if incomplete_symbols:
        print(f"Skipping {len(incomplete_symbols)} incomplete symbol set(s).", flush=True)

    if workers <= 1:
        for index, symbol in enumerate(symbols, start=1):
            print(f"[{index}/{len(symbols)}] Running {symbol}...", flush=True)

            try:
                summary = run_symbol_analysis(
                    symbol=symbol,
                    input_dir=input_dir,
                    run_id=run_id,
                    max_prev_gap_ms=args.max_prev_gap_ms,
                    top_n_conditions=args.top_n_conditions,
                    top_n_condition_combos=args.top_n_condition_combos,
                )

                mongo_write = await write_symbol_summary(
                    collection=collection,
                    summary=summary,
                    write_mode=args.write_mode,
                )

                summary["mongo_write"] = mongo_write

                reversal_summary = None
                reversal_error = None

                if not args.disable_reversal_analysis:
                    try:
                        reversal_summary = run_symbol_reversal_analysis(
                            symbol=symbol,
                            input_dir=input_dir,
                            run_id=run_id,
                            args=args,
                        )
                        reversal_mongo_write = await write_quote_reversal_summary(
                            collection=collection,
                            summary=reversal_summary,
                            write_mode=args.write_mode,
                        )
                        reversal_summary["mongo_write"] = reversal_mongo_write
                        reversal_summaries.append(reversal_summary)
                        summary["quote_rev_ref"] = {
                            "symbol": reversal_summary.get("symbol"),
                            "run_id": reversal_summary.get("run_id"),
                            "mongo_id": reversal_summary.get("_id"),
                            "candidate_count": reversal_summary.get("candidate_count"),
                            "confidence": (reversal_summary.get("quality") or {}).get("confidence"),
                        }
                        summary["quote_rev_write"] = reversal_mongo_write
                    except Exception as exc:
                        reversal_error = exc
                        reversal_error_record = {
                            "symbol": symbol,
                            "error": str(exc),
                            "error_type": type(exc).__name__,
                        }
                        if args.print_tracebacks:
                            reversal_error_record["traceback"] = traceback.format_exc()
                        reversal_errors.append(reversal_error_record)
                        print(f"[{index}/{len(symbols)}] REVERSAL ERROR {symbol}: {exc}", flush=True)
                        if args.print_tracebacks:
                            traceback.print_exc()
                        if args.fail_fast:
                            raise

                summaries.append(summary)
                batch_row = make_batch_row(summary)
                batch_row.update(make_reversal_batch_fields(reversal_summary, reversal_error))
                batch_rows.append(batch_row)

                reversal_text = "disabled"
                if reversal_summary is not None:
                    reversal_text = f"{reversal_summary.get('candidate_count')} candidates"
                elif reversal_error is not None:
                    reversal_text = "error"

                print(
                    f"[{index}/{len(symbols)}] Finished {symbol}: "
                    f"{summary.get('joined_rows')} joined rows. "
                    f"Mongo write: {mongo_write.get('write_mode')}. "
                    f"Reversal: {reversal_text}.",
                    flush=True,
                )

            except Exception as exc:
                error_row = make_error_batch_row(symbol, exc)
                error_row.update(make_reversal_batch_fields(None, None))
                error_record = {
                    "symbol": symbol,
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                }

                if args.print_tracebacks:
                    error_record["traceback"] = traceback.format_exc()

                batch_rows.append(error_row)
                errors.append(error_record)

                print(f"[{index}/{len(symbols)}] ERROR {symbol}: {exc}", flush=True)

                if args.print_tracebacks:
                    traceback.print_exc()

                if args.fail_fast:
                    raise

            finally:
                gc.collect()

    else:
        worker_args = build_worker_args_dict(args)
        futures = {}

        print(f"Submitting {len(symbols)} symbol task(s) to {workers} worker process(es)...", flush=True)

        with ProcessPoolExecutor(max_workers=workers) as executor:
            for index, symbol in enumerate(symbols, start=1):
                payload = {
                    "symbol": symbol,
                    "input_dir": str(input_dir),
                    "run_id": run_id,
                    "args": worker_args,
                }
                future = executor.submit(run_symbol_analysis_worker, payload)
                futures[future] = (index, symbol)

            for completed_index, future in enumerate(as_completed(futures), start=1):
                index, symbol = futures[future]

                try:
                    result = future.result()

                    if result.get("error"):
                        error_record = result["error"]
                        error_row = make_error_batch_row(symbol, RuntimeError(error_record.get("error")))
                        error_row.update(make_reversal_batch_fields(None, None))
                        batch_rows.append(error_row)
                        errors.append(error_record)

                        print(
                            f"[{completed_index}/{len(symbols)} complete] ERROR {symbol}: "
                            f"{error_record.get('error')}",
                            flush=True,
                        )

                        if args.fail_fast:
                            raise RuntimeError(error_record.get("error"))

                        continue

                    summary = result["summary"]
                    reversal_summary = result.get("reversal_summary")
                    reversal_error_record = result.get("reversal_error")
                    reversal_error = None

                    mongo_write = await write_symbol_summary(
                        collection=collection,
                        summary=summary,
                        write_mode=args.write_mode,
                    )
                    summary["mongo_write"] = mongo_write

                    if reversal_error_record is not None:
                        reversal_error = RuntimeError(reversal_error_record.get("error"))
                        reversal_errors.append(reversal_error_record)
                        print(
                            f"[{completed_index}/{len(symbols)} complete] REVERSAL ERROR {symbol}: "
                            f"{reversal_error_record.get('error')}",
                            flush=True,
                        )
                        if args.fail_fast:
                            raise reversal_error

                    if reversal_summary is not None:
                        reversal_mongo_write = await write_quote_reversal_summary(
                            collection=collection,
                            summary=reversal_summary,
                            write_mode=args.write_mode,
                        )
                        reversal_summary["mongo_write"] = reversal_mongo_write
                        reversal_summaries.append(reversal_summary)
                        summary["quote_rev_ref"] = {
                            "symbol": reversal_summary.get("symbol"),
                            "run_id": reversal_summary.get("run_id"),
                            "mongo_id": reversal_summary.get("_id"),
                            "candidate_count": reversal_summary.get("candidate_count"),
                            "confidence": (reversal_summary.get("quality") or {}).get("confidence"),
                        }
                        summary["quote_rev_write"] = reversal_mongo_write

                    summaries.append(summary)
                    batch_row = make_batch_row(summary)
                    batch_row.update(make_reversal_batch_fields(reversal_summary, reversal_error))
                    batch_rows.append(batch_row)

                    reversal_text = "disabled"
                    if reversal_summary is not None:
                        reversal_text = f"{reversal_summary.get('candidate_count')} candidates"
                    elif reversal_error is not None:
                        reversal_text = "error"

                    print(
                        f"[{completed_index}/{len(symbols)} complete] Finished {symbol}: "
                        f"{summary.get('joined_rows')} joined rows. "
                        f"Mongo write: {mongo_write.get('write_mode')}. "
                        f"Reversal: {reversal_text}.",
                        flush=True,
                    )

                except Exception as exc:
                    error_row = make_error_batch_row(symbol, exc)
                    error_row.update(make_reversal_batch_fields(None, None))
                    error_record = {
                        "symbol": symbol,
                        "error": str(exc),
                        "error_type": type(exc).__name__,
                    }
                    if args.print_tracebacks:
                        error_record["traceback"] = traceback.format_exc()

                    batch_rows.append(error_row)
                    errors.append(error_record)

                    print(f"[{completed_index}/{len(symbols)} complete] ERROR {symbol}: {exc}", flush=True)

                    if args.print_tracebacks:
                        traceback.print_exc()

                    if args.fail_fast:
                        raise

                finally:
                    gc.collect()

    batch_summary = {
        "document_type": "px_move_batch",
        "run_id": run_id,
        "generated_at_utc": utc_now_iso(),
        "input_dir": str(input_dir),
        "mongo": {
            "database": args.mongo_db,
            "collection": args.mongo_collection,
            "write_mode": args.write_mode,
        },
        "parallelism": {
            "workers": workers,
            "mode": "processes" if workers > 1 else "single_process",
        },
        "quote_rev": {
            "enabled": not args.disable_reversal_analysis,
            "symbols_succeeded": len(reversal_summaries),
            "symbols_failed": len(reversal_errors),
            "errors": reversal_errors,
        },
        "symbols_discovered": symbols,
        "symbols_attempted": len(symbols),
        "symbols_succeeded": len(summaries),
        "symbols_failed": len(errors),
        "incomplete_sets": incomplete_symbols,
        "errors": errors,
        "batch_rows": batch_rows,
        "symbol_refs": [
            {
                "symbol": summary.get("symbol"),
                "run_id": summary.get("run_id"),
                "mongo_id": summary.get("_id"),
                "joined_rows": summary.get("joined_rows"),
                "quote_rev_id": (summary.get("quote_rev_ref") or {}).get("mongo_id"),
                "qr_candidates": (summary.get("quote_rev_ref") or {}).get("candidate_count"),
            }
            for summary in summaries
        ],
    }

    batch_summary = as_jsonable(batch_summary)

    mongo_write = await write_batch_summary(
        collection=collection,
        batch_summary=batch_summary,
        write_mode=args.write_mode,
    )

    batch_summary["mongo_write"] = mongo_write

    return batch_summary


# =========================
# CLI
# =========================

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--symbol",
        default=None,
        help="Optional. Run one symbol only. Omit this to run every complete symbol set in the input directory.",
    )

    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("D:/redis_reports/270526"),
        help="Folder containing trades_<SYMBOL>.csv, quotes_<SYMBOL>.csv, and 1secagg_<SYMBOL>.csv. Defaults to F:/temp/redis_report/130526.",
    )

    parser.add_argument(
        "--max-prev-gap-ms",
        type=int,
        default=2000,
    )

    parser.add_argument(
        "--top-n-conditions",
        type=int,
        default=25,
        help="Maximum number of condition rows stored in each symbol JSON document.",
    )

    parser.add_argument(
        "--top-n-condition-combos",
        type=int,
        default=25,
        help="Maximum number of condition-combo rows stored in each symbol JSON document.",
    )


    parser.add_argument(
        "--disable-reversal-analysis",
        action="store_true",
        help="Disable quote reversal baseline documents. By default this script writes them alongside price movement baselines.",
    )

    parser.add_argument(
        "--reversal-lookback-ms",
        type=int,
        default=1000,
        help="Maximum quote lookback window used when scanning tick-level reversals.",
    )

    parser.add_argument(
        "--reversal-tick-counts",
        default="3,4,5,6,8",
        help="Comma-separated quote tick counts to test for reversal windows.",
    )

    parser.add_argument(
        "--reversal-min-persist-ms",
        type=int,
        default=100,
        help="Minimum elapsed milliseconds between first and last quote in a candidate reversal window.",
    )

    parser.add_argument(
        "--reversal-max-persist-ms",
        type=int,
        default=1500,
        help="Maximum elapsed milliseconds between first and last quote in a candidate reversal window. Use 0 to disable.",
    )

    parser.add_argument(
        "--reversal-min-mid-move",
        type=float,
        default=0.01,
        help="Minimum absolute mid-price move required for a quote reversal candidate.",
    )

    parser.add_argument(
        "--reversal-min-mid-move-pct",
        "--reversal-min-mid-move-percent",
        dest="reversal_min_mid_move_pct",
        type=float,
        default=0.003,
        help="Minimum percent mid-price move required for a quote reversal candidate.",
    )

    parser.add_argument(
        "--reversal-max-spread-percent",
        type=float,
        default=0.50,
        help="Maximum latest spread percent allowed for a quote reversal candidate.",
    )

    parser.add_argument(
        "--reversal-ft-seconds",
        "--reversal-followthrough-seconds",
        dest="reversal_ft_seconds",
        default="1,3,5",
        help="Comma-separated future aggregate horizons used to validate reversal follow-through.",
    )

    parser.add_argument(
        "--reversal-success-min-move",
        type=float,
        default=0.01,
        help="Minimum future price move for follow-through success.",
    )

    parser.add_argument(
        "--reversal-success-min-move-pct",
        "--reversal-success-min-move-percent",
        dest="reversal_success_min_move_pct",
        type=float,
        default=0.003,
        help="Minimum future percent price move for follow-through success.",
    )

    parser.add_argument(
        "--reversal-dedup-gap-ms",
        type=int,
        default=500,
        help="Minimum milliseconds between kept reversal candidates per side.",
    )

    parser.add_argument(
        "--reversal-candidate-example-limit",
        type=int,
        default=50,
        help="Maximum candidate examples stored in each quote_rev_symbol document.",
    )

    parser.add_argument(
        "--mongo-uri",
        default=os.getenv("MONGO_URI", "mongodb://192.168.1.126:27017"),
        help="MongoDB connection string. Can also be set with MONGO_URI.",
    )

    parser.add_argument(
        "--mongo-db",
        default=os.getenv("MONGO_DB", "trading_data"),
        help="Existing MongoDB database name. Defaults to trading_data.",
    )

    parser.add_argument(
        "--mongo-collection",
        default=os.getenv("MONGO_COLLECTION", "baselines_new"),
        help="Existing MongoDB collection name. Defaults to baselines.",
    )

    parser.add_argument(
        "--write-mode",
        choices=["upsert-latest", "insert-history"],
        default=os.getenv("MONGO_WRITE_MODE", "upsert-latest"),
        help="upsert-latest overwrites one latest doc per symbol. insert-history stores one doc per run_id.",
    )

    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional run id. Defaults to a UTC timestamp.",
    )

    parser.add_argument(
        "--workers",
        type=int,
        default=int(os.getenv("BASELINE_WORKERS", "1")),
        help=(
            "Number of parallel symbol worker processes. Use 1 for the original sequential behaviour. "
            "Start with 2-4; each worker loads full CSVs and can use a lot of RAM."
        ),
    )

    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop on the first symbol error instead of continuing with the remaining symbols.",
    )

    parser.add_argument(
        "--print-tracebacks",
        action="store_true",
        help="Print full Python tracebacks for per-symbol failures.",
    )

    parser.add_argument(
        "--no-print-json",
        action="store_true",
        help="Do not print the final JSON summary to stdout.",
    )

    return parser


async def async_main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    input_dir = args.input_dir
    run_id = args.run_id or make_run_id()

    client = None

    try:
        client, collection = await connect_mongo(args)

        if args.symbol:
            symbol = resolve_symbol_case(input_dir, args.symbol)

            summary = await run_one_symbol(
                symbol=symbol,
                input_dir=input_dir,
                run_id=run_id,
                collection=collection,
                args=args,
            )

            if not args.no_print_json:
                print(json.dumps(as_jsonable(summary), indent=2))

        else:
            batch_summary = await run_batch(
                input_dir=input_dir,
                run_id=run_id,
                collection=collection,
                args=args,
            )

            if not args.no_print_json:
                print(json.dumps(as_jsonable(batch_summary), indent=2))

    finally:
        if client is not None:
            client.close()


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()