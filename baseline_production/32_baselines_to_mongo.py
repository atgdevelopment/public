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

This script refuses to create a new MongoDB database or collection.
No Mongo indexes are created.
No CSV outputs are written.



If you want to change the default folder, it's here, otherwise use switches and direct the app to your folder
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("F:/temp/redis_report/130526"),
        help="Folder containing trades_<SYMBOL>.csv, quotes_<SYMBOL>.csv, and 1secagg_<SYMBOL>.csv. Defaults to F:/temp/redis_report/130526.",
    )

"""

from __future__ import annotations

import argparse
import asyncio
import gc
import json
import math
import os
import re
import traceback
from datetime import datetime, timezone
from pathlib import Path
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

    joined["mid_reprice_direction"] = np.select(
        [joined["last_mid_delta"] > 0, joined["last_mid_delta"] < 0],
        ["up", "down"],
        default="flat",
    )

    joined["bid_reprice_direction"] = np.select(
        [joined["last_bid_delta"] > 0, joined["last_bid_delta"] < 0],
        ["up", "down"],
        default="flat",
    )

    joined["ask_reprice_direction"] = np.select(
        [joined["last_ask_delta"] > 0, joined["last_ask_delta"] < 0],
        ["up", "down"],
        default="flat",
    )

    p50, p90, p95, p99 = joined["abs_close_delta"].quantile([0.5, 0.9, 0.95, 0.99]).values

    joined["is_normal_movement"] = joined["abs_close_delta"] <= p50
    joined["is_large_movement"] = joined["abs_close_delta"] >= p90
    joined["is_busy_price_movement"] = joined["abs_close_delta"] >= p95
    joined["is_extreme_movement"] = joined["abs_close_delta"] >= p99

    joined["movement_regime_exclusive"] = np.select(
        [
            joined["is_extreme_movement"],
            joined["is_busy_price_movement"],
            joined["is_large_movement"],
            joined["is_normal_movement"],
        ],
        ["extreme_movement", "busy_price_movement", "large_movement", "normal_movement"],
        default="middle_movement",
    )

    joined["mid_same_direction_as_close"] = (
        joined["mid_reprice_direction"] == joined["direction"]
    ) & (joined["direction"] != "flat")

    joined["bid_same_direction_as_close"] = (
        joined["bid_reprice_direction"] == joined["direction"]
    ) & (joined["direction"] != "flat")

    joined["ask_same_direction_as_close"] = (
        joined["ask_reprice_direction"] == joined["direction"]
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
            "median_quote_update_count": d["quote_update_count"].median(),
            "median_last_spread": d["last_spread"].median(),
            "median_abs_last_mid_delta": d["last_mid_delta"].abs().median(),
            "spread_widen_rate": d["spread_widened"].mean(),
            "up_rate": (d["direction"] == "up").mean(),
            "down_rate": (d["direction"] == "down").mean(),
            "flat_rate": (d["direction"] == "flat").mean(),
            "mid_same_direction_rate_nonflat": (
                nonflat["mid_same_direction_as_close"].mean() if len(nonflat) else np.nan
            ),
            "bid_same_direction_rate_nonflat": (
                nonflat["bid_same_direction_as_close"].mean() if len(nonflat) else np.nan
            ),
            "ask_same_direction_rate_nonflat": (
                nonflat["ask_same_direction_as_close"].mean() if len(nonflat) else np.nan
            ),
        }

    regime_summary = pd.DataFrame(
        [
            summarize_regime("normal_movement", joined["is_normal_movement"]),
            summarize_regime("large_movement", joined["is_large_movement"]),
            summarize_regime("busy_price_movement", joined["is_busy_price_movement"]),
            summarize_regime("extreme_movement", joined["is_extreme_movement"]),
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
                    "row_share_within_scope": len(d) / len(df),
                    "median_close_delta": d["close_delta"].median(),
                    "median_abs_close_delta": d["abs_close_delta"].median(),
                    "median_trade_count": d["trade_count"].median(),
                    "median_share_volume": d["share_volume"].median(),
                    "median_quote_update_count": d["quote_update_count"].median(),
                    "median_last_spread": d["last_spread"].median(),
                    "median_last_bid_delta": d["last_bid_delta"].median(),
                    "median_last_ask_delta": d["last_ask_delta"].median(),
                    "median_last_mid_delta": d["last_mid_delta"].median(),
                    "bid_same_direction_rate": (
                        d["bid_same_direction_as_close"].mean()
                        if direction != "flat"
                        else np.nan
                    ),
                    "ask_same_direction_rate": (
                        d["ask_same_direction_as_close"].mean()
                        if direction != "flat"
                        else np.nan
                    ),
                    "mid_same_direction_rate": (
                        d["mid_same_direction_as_close"].mean()
                        if direction != "flat"
                        else np.nan
                    ),
                    "spread_widen_rate": d["spread_widened"].mean(),
                }
            )

        return rows

    direction_summary = pd.DataFrame(
        direction_rows(joined, "all_joined")
        + direction_rows(joined[joined["is_large_movement"]], "large_movement")
        + direction_rows(joined[joined["is_busy_price_movement"]], "busy_price_movement")
        + direction_rows(joined[joined["is_extreme_movement"]], "extreme_movement")
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
        "is_normal_movement",
        "is_large_movement",
        "is_busy_price_movement",
        "is_extreme_movement",
        "trade_count",
        "share_volume",
        "quote_update_count",
        "last_spread",
        "mean_spread",
        "last_bid_delta",
        "last_ask_delta",
        "last_mid_delta",
        "bid_same_direction_as_close",
        "ask_same_direction_as_close",
        "mid_same_direction_as_close",
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
        "large_abs_move_rate": trade_join["is_large_movement"].mean(),
        "busy_abs_move_rate": trade_join["is_busy_price_movement"].mean(),
        "extreme_abs_move_rate": trade_join["is_extreme_movement"].mean(),
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
            "is_large_movement",
            "is_busy_price_movement",
            "is_extreme_movement",
            "abs_close_delta",
            "trade_count",
            "share_volume",
            "quote_update_count",
            "last_spread",
            "direction",
            "mid_same_direction_as_close",
            "bid_same_direction_as_close",
            "ask_same_direction_as_close",
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
            "is_large_movement",
            "is_busy_price_movement",
            "is_extreme_movement",
            "abs_close_delta",
            "trade_count",
            "share_volume",
            "quote_update_count",
            "last_spread",
            "direction",
            "mid_same_direction_as_close",
            "bid_same_direction_as_close",
            "ask_same_direction_as_close",
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
                "share_of_matched_trades_multilabel": len(g) / len(trade_join),
                "tick_up_rate": g["is_tick_up"].mean(),
                "bar_up_rate": g["is_bar_up"].mean(),
                "large_abs_move_rate": g["is_large_movement"].mean(),
                "busy_abs_move_rate": g["is_busy_price_movement"].mean(),
                "extreme_abs_move_rate": g["is_extreme_movement"].mean(),
                "median_abs_close_delta": g["abs_close_delta"].median(),
                "median_trade_count_in_same_second": g["trade_count"].median(),
                "median_share_volume_in_same_second": g["share_volume"].median(),
                "median_quote_updates_in_same_second": g["quote_update_count"].median(),
                "median_last_spread_in_same_second": g["last_spread"].median(),
                "mid_same_direction_rate_nonflat": (
                    nonflat["mid_same_direction_as_close"].mean()
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
    condition_summary["tick_up_lift_vs_baseline_pts"] = (
        condition_summary["tick_up_rate"] - baseline["tick_up_rate"]
    ) * 100
    condition_summary["bar_up_lift_vs_baseline_pts"] = (
        condition_summary["bar_up_rate"] - baseline["bar_up_rate"]
    ) * 100
    condition_summary["large_abs_move_lift_vs_baseline_pts"] = (
        condition_summary["large_abs_move_rate"] - baseline["large_abs_move_rate"]
    ) * 100

    condition_summary = condition_summary.sort_values("trade_observations", ascending=False)

    combo_summary = (
        trade_join.groupby("cond_combo")
        .apply(
            lambda g: pd.Series(
                {
                    "trade_observations": len(g),
                    "seconds_present": g["sec_ms"].nunique(),
                    "share_of_matched_trades": len(g) / len(trade_join),
                    "tick_up_rate": g["is_tick_up"].mean(),
                    "bar_up_rate": g["is_bar_up"].mean(),
                    "large_abs_move_rate": g["is_large_movement"].mean(),
                    "busy_abs_move_rate": g["is_busy_price_movement"].mean(),
                    "extreme_abs_move_rate": g["is_extreme_movement"].mean(),
                    "median_abs_close_delta": g["abs_close_delta"].median(),
                    "median_trade_count_in_same_second": g["trade_count"].median(),
                    "median_quote_updates_in_same_second": g["quote_update_count"].median(),
                    "median_last_spread_in_same_second": g["last_spread"].median(),
                    "spread_widen_rate": g["spread_widened"].mean(),
                }
            )
        )
        .reset_index()
    )

    combo_summary["tick_up_lift_vs_baseline_pts"] = (
        combo_summary["tick_up_rate"] - baseline["tick_up_rate"]
    ) * 100
    combo_summary["bar_up_lift_vs_baseline_pts"] = (
        combo_summary["bar_up_rate"] - baseline["bar_up_rate"]
    ) * 100
    combo_summary["large_abs_move_lift_vs_baseline_pts"] = (
        combo_summary["large_abs_move_rate"] - baseline["large_abs_move_rate"]
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
        ("normal_movement", joined["is_normal_movement"]),
        ("large_movement", joined["is_large_movement"]),
        ("busy_price_movement", joined["is_busy_price_movement"]),
        ("extreme_movement", joined["is_extreme_movement"]),
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
                    "median_count_when_present": (
                        d.loc[present, col].median() if present.any() else np.nan
                    ),
                    "mean_count_when_present": (
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
                "median_count_when_present": (
                    d.loc[present, "no_condition_trade_count"].median()
                    if present.any()
                    else np.nan
                ),
                "mean_count_when_present": (
                    d.loc[present, "no_condition_trade_count"].mean()
                    if present.any()
                    else np.nan
                ),
            }
        )

    condition_presence_by_regime = pd.DataFrame(presence_rows)

    u = joined_unique.sort_values("sec_ms").reset_index(drop=True).copy()
    u["next_sec_ms"] = u["sec_ms"].shift(-1)
    u["next_gap_ms"] = u["next_sec_ms"] - u["sec_ms"]

    for col in [
        "abs_close_delta",
        "is_large_movement",
        "is_busy_price_movement",
        "is_extreme_movement",
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
                "relationship": "same_second_feature_vs_abs_move",
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
                "relationship": "feature_t_vs_next_second_abs_move",
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
                "median_current_abs_close_delta": d["abs_close_delta"].median(),
                "median_current_trade_count": d["trade_count"].median(),
                "median_current_share_volume": d["share_volume"].median(),
                "median_current_quote_update_count": d["quote_update_count"].median(),
                "median_current_last_spread": d["last_spread"].median(),
                "median_current_mean_spread": d["mean_spread"].median(),
                "median_next_abs_close_delta": d["next_abs_close_delta"].median(),
                "next_up_rate": (d["next_direction"] == "up").mean(),
                "next_down_rate": (d["next_direction"] == "down").mean(),
            }
            for label, d in [
                ("before_next_normal_movement", lead_base[lead_base["next_abs_close_delta"] <= p50]),
                ("before_next_large_movement", lead_base[lead_base["next_abs_close_delta"] >= p90]),
                ("before_next_busy_price_movement", lead_base[lead_base["next_abs_close_delta"] >= p95]),
                ("before_next_extreme_movement", lead_base[lead_base["next_abs_close_delta"] >= p99]),
            ]
        ]
    )

    summary = {
        "document_type": "price_movement_symbol_summary",
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
        "unique_joined_seconds_for_trade_condition_join": len(joined_unique),
        "max_prev_gap_ms": max_prev_gap_ms,
        "thresholds": {
            "normal_p50_max": p50,
            "large_p90_min": p90,
            "busy_p95_min": p95,
            "extreme_p99_min": p99,
        },
        "matched_trade_baseline_for_condition_tables": baseline,
        "regime_summary": records(regime_summary),
        "direction_quote_repricing_summary": records(direction_summary),
        "top_condition_movement_summary": records(condition_summary, top_n_conditions),
        "top_condition_combo_movement_summary": records(combo_summary, top_n_condition_combos),
        "condition_presence_by_regime": records(condition_presence_by_regime),
        "price_movement_lead_lag_correlations": records(corr_summary),
        "price_movement_lead_profiles": records(lead_profile_summary),
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
    baseline = summary.get("matched_trade_baseline_for_condition_tables", {})
    input_row_counts = summary.get("input_row_counts", {})

    return {
        "symbol": summary.get("symbol"),
        "status": "ok",
        "joined_rows": summary.get("joined_rows"),
        "unique_joined_seconds_for_trade_condition_join": summary.get(
            "unique_joined_seconds_for_trade_condition_join"
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
        "large_abs_move_rate": baseline.get("large_abs_move_rate"),
        "busy_abs_move_rate": baseline.get("busy_abs_move_rate"),
        "extreme_abs_move_rate": baseline.get("extreme_abs_move_rate"),
        "error": "",
    }


def make_error_batch_row(symbol: str, exc: Exception) -> dict:
    return {
        "symbol": symbol,
        "status": "error",
        "joined_rows": None,
        "unique_joined_seconds_for_trade_condition_join": None,
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
        "large_abs_move_rate": None,
        "busy_abs_move_rate": None,
        "extreme_abs_move_rate": None,
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

    print(
        f"Finished {symbol}: {summary.get('joined_rows')} joined rows. "
        f"Mongo write: {mongo_write.get('write_mode')}.",
        flush=True,
    )

    return summary


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

    print(f"Found {len(symbols)} complete symbol set(s).", flush=True)
    print(f"Input directory: {input_dir}", flush=True)
    print(f"Mongo database:  {args.mongo_db}", flush=True)
    print(f"Mongo collection: {args.mongo_collection}", flush=True)

    if incomplete_symbols:
        print(f"Skipping {len(incomplete_symbols)} incomplete symbol set(s).", flush=True)

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
            summaries.append(summary)
            batch_rows.append(make_batch_row(summary))

            print(
                f"[{index}/{len(symbols)}] Finished {symbol}: "
                f"{summary.get('joined_rows')} joined rows. "
                f"Mongo write: {mongo_write.get('write_mode')}.",
                flush=True,
            )

        except Exception as exc:
            error_row = make_error_batch_row(symbol, exc)
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

    batch_summary = {
        "document_type": "price_movement_batch_summary",
        "run_id": run_id,
        "generated_at_utc": utc_now_iso(),
        "input_dir": str(input_dir),
        "mongo": {
            "database": args.mongo_db,
            "collection": args.mongo_collection,
            "write_mode": args.write_mode,
        },
        "symbols_discovered": symbols,
        "symbols_attempted": len(symbols),
        "symbols_succeeded": len(summaries),
        "symbols_failed": len(errors),
        "incomplete_symbol_file_sets": incomplete_symbols,
        "errors": errors,
        "batch_rows": batch_rows,
        "symbol_summary_refs": [
            {
                "symbol": summary.get("symbol"),
                "run_id": summary.get("run_id"),
                "mongo_id": summary.get("_id"),
                "joined_rows": summary.get("joined_rows"),
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
        default=Path("F:/temp/redis_report/130526"),
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
        "--mongo-uri",
        default=os.getenv("MONGO_URI", "mongodb://192.168.1.10:27017"),
        help="MongoDB connection string. Can also be set with MONGO_URI.",
    )

    parser.add_argument(
        "--mongo-db",
        default=os.getenv("MONGO_DB", "trading_data"),
        help="Existing MongoDB database name. Defaults to trading_data.",
    )

    parser.add_argument(
        "--mongo-collection",
        default=os.getenv("MONGO_COLLECTION", "baselines"),
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