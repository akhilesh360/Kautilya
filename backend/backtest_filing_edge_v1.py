"""
Backtest harness for Filing Edge Engine v1 (10-K / 10-Q section-diff signals).

Purpose
- Evaluate whether section-level SEC filing edge scores have predictive value
  over forward horizons (30d / 90d / 252d by default).
- Uses recent filing pairs only (newest-first) and historical prices for forward returns.

Notes
- This is not a portfolio strategy backtest yet; it's a signal-validation harness.
- SEC requests are rate-limited; keep the symbol list modest and avoid frequent reruns.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import yfinance as yf

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.services.sec_service import SECService


DEFAULT_UNIVERSE = ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA"]


@dataclass
class FilingEdgeRecord:
    symbol: str
    latest_form: str
    prev_form: str
    filing_date: str
    prev_date: str
    edge_score: float
    edge_label: str
    drift_alert: bool
    similarity: float
    sentiment_drift: float
    horizon_days: int
    forward_return_pct: Optional[float]
    hit_directional: Optional[bool]
    hit_abs_gt_0: Optional[bool]
    signal_direction: str
    top_signal_types: str
    edge_summary: str


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Backtest Filing Edge Engine v1 on recent SEC filing pairs.")
    p.add_argument("--symbols", nargs="+", default=DEFAULT_UNIVERSE, help="Ticker symbols")
    p.add_argument("--max-pairs", type=int, default=4, help="Max filing pairs per symbol (newest-first)")
    p.add_argument("--horizons", nargs="+", type=int, default=[30, 90, 252], help="Forward horizons (trading days)")
    p.add_argument("--min-abs-edge-score", type=float, default=0.0, help="Filter low-signal filing edges")
    p.add_argument("--output", default="data/backtests/filing_edge", help="Output directory")
    return p.parse_args()


def ensure_output_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def fetch_price_history(symbol: str, years: int = 10) -> pd.DataFrame:
    df = yf.download(symbol, period=f"{years}y", interval="1d", auto_adjust=True, progress=False)
    if df is None or df.empty:
        return pd.DataFrame()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    if "Close" not in df.columns:
        return pd.DataFrame()
    df = df[["Close"]].dropna().copy()
    df.index = pd.to_datetime(df.index).tz_localize(None)
    return df


def nearest_index_on_or_after(index: pd.DatetimeIndex, dt: pd.Timestamp) -> Optional[int]:
    if len(index) == 0:
        return None
    pos = index.searchsorted(dt, side="left")
    if pos >= len(index):
        return None
    return int(pos)


def score_direction(edge_score: float) -> str:
    if edge_score >= 1.5:
        return "positive"
    if edge_score <= -1.5:
        return "negative"
    return "neutral"


def build_records_for_symbol(
    sec: SECService,
    symbol: str,
    max_pairs: int,
    horizons: List[int],
    min_abs_edge_score: float,
) -> List[FilingEdgeRecord]:
    edges = sec.identify_alpha_edge_series(symbol, max_pairs=max_pairs)
    if not edges:
        return []

    px = fetch_price_history(symbol, years=10)
    if px.empty:
        return []
    idx = pd.DatetimeIndex(px.index)

    out: List[FilingEdgeRecord] = []
    for edge in edges:
        if edge.get("error"):
            continue
        filing_date = edge.get("latest_date")
        if not filing_date:
            continue

        edge_score = float(edge.get("edgeScore", 0) or 0)
        if abs(edge_score) < float(min_abs_edge_score):
            continue

        filing_dt = pd.Timestamp(filing_date)
        entry_pos = nearest_index_on_or_after(idx, filing_dt)
        if entry_pos is None:
            continue

        signal_dir = score_direction(edge_score)
        top_signal_types = ",".join([
            str((s or {}).get("type", ""))
            for s in (edge.get("filingSignals") or [])[:4]
            if (s or {}).get("type")
        ])

        for h in horizons:
            exit_pos = min(entry_pos + int(h), len(idx) - 1)
            if exit_pos <= entry_pos:
                fwd_ret = None
                hit_dir = None
                hit_abs = None
            else:
                p0 = float(px["Close"].iloc[entry_pos])
                p1 = float(px["Close"].iloc[exit_pos])
                if p0 <= 0:
                    fwd_ret = None
                    hit_dir = None
                    hit_abs = None
                else:
                    fwd_ret = (p1 / p0 - 1.0) * 100.0
                    hit_abs = bool(fwd_ret > 0)
                    if signal_dir == "positive":
                        hit_dir = bool(fwd_ret > 0)
                    elif signal_dir == "negative":
                        hit_dir = bool(fwd_ret < 0)
                    else:
                        hit_dir = None

            out.append(FilingEdgeRecord(
                symbol=symbol,
                latest_form=str(edge.get("latest_form", "")),
                prev_form=str(edge.get("prev_form", "")),
                filing_date=str(filing_date),
                prev_date=str(edge.get("prev_date", "")),
                edge_score=round(edge_score, 2),
                edge_label=str(edge.get("edgeLabel", "")),
                drift_alert=bool(edge.get("drift_alert")),
                similarity=float(edge.get("similarity", 0) or 0),
                sentiment_drift=float(edge.get("sentiment_drift", 0) or 0),
                horizon_days=int(h),
                forward_return_pct=round(float(fwd_ret), 2) if fwd_ret is not None else None,
                hit_directional=hit_dir,
                hit_abs_gt_0=hit_abs,
                signal_direction=signal_dir,
                top_signal_types=top_signal_types,
                edge_summary=str(edge.get("edge_summary", "")),
            ))
    return out


def summarize(records_df: pd.DataFrame, args: argparse.Namespace) -> Dict[str, Any]:
    if records_df.empty:
        return {
            "config": vars(args),
            "coverage": {"records": 0},
            "results": [],
            "notes": ["No filing-edge records were generated. SEC availability or price coverage may be insufficient."],
        }

    results = []
    for h, grp in records_df.groupby("horizon_days"):
        evaluable_dir = grp.dropna(subset=["hit_directional"]).copy()
        positive = grp[grp["signal_direction"] == "positive"]
        negative = grp[grp["signal_direction"] == "negative"]
        neutral = grp[grp["signal_direction"] == "neutral"]

        corr = None
        valid_corr = grp.dropna(subset=["forward_return_pct"]).copy()
        if len(valid_corr) >= 3:
            try:
                corr = float(valid_corr["edge_score"].corr(valid_corr["forward_return_pct"]))
            except Exception:
                corr = None

        bucket_rows = []
        valid_bucket = grp.dropna(subset=["forward_return_pct"]).copy()
        if not valid_bucket.empty:
            valid_bucket["edge_bucket"] = pd.cut(
                valid_bucket["edge_score"],
                bins=[-100, -6, -2, 2, 6, 100],
                labels=["<=-6", "(-6,-2]", "(-2,2]", "(2,6]", ">6"],
                include_lowest=True,
            )
            for b, bgrp in valid_bucket.groupby("edge_bucket", observed=False):
                if len(bgrp) == 0:
                    continue
                bucket_rows.append({
                    "bucket": str(b),
                    "count": int(len(bgrp)),
                    "avg_forward_return_pct": round(float(bgrp["forward_return_pct"].mean()), 2),
                    "pct_positive_returns": round(float((bgrp["forward_return_pct"] > 0).mean()) * 100, 2),
                })

        results.append({
            "horizon_days": int(h),
            "records": int(len(grp)),
            "records_directional": int(len(evaluable_dir)),
            "directional_hit_rate_pct": round(float(evaluable_dir["hit_directional"].astype(bool).mean()) * 100, 2) if len(evaluable_dir) else None,
            "edge_score_vs_forward_return_corr": round(corr, 4) if corr is not None and not np.isnan(corr) else None,
            "avg_forward_return_pct_all": round(float(grp["forward_return_pct"].dropna().mean()), 2) if grp["forward_return_pct"].notna().any() else None,
            "positive_signal_hit_rate_pct": round(float(positive["hit_directional"].dropna().astype(bool).mean()) * 100, 2) if positive["hit_directional"].dropna().any() else None,
            "negative_signal_hit_rate_pct": round(float(negative["hit_directional"].dropna().astype(bool).mean()) * 100, 2) if negative["hit_directional"].dropna().any() else None,
            "neutral_signal_avg_return_pct": round(float(neutral["forward_return_pct"].dropna().mean()), 2) if neutral["forward_return_pct"].notna().any() else None,
            "edge_buckets": bucket_rows,
        })

    return {
        "config": vars(args),
        "coverage": {
            "records": int(len(records_df)),
            "symbols": sorted(records_df["symbol"].dropna().unique().tolist()),
            "date_min": str(records_df["filing_date"].min()),
            "date_max": str(records_df["filing_date"].max()),
        },
        "results": sorted(results, key=lambda x: x["horizon_days"]),
        "notes": [
            "Signal-validation backtest for Filing Edge Engine v1 (not a full portfolio strategy).",
            "Uses recent filing pairs returned by SEC and historical prices for forward-return measurement.",
            "SEC and filing text extraction quality may vary by issuer/form formatting.",
        ],
    }


def save_outputs(summary: Dict[str, Any], records_df: pd.DataFrame, outdir: str) -> None:
    ensure_output_dir(outdir)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    summary_path = os.path.join(outdir, f"filing_edge_summary_{ts}.json")
    records_path = os.path.join(outdir, f"filing_edge_records_{ts}.csv")

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    records_df.to_csv(records_path, index=False)

    print(f"Saved summary: {summary_path}")
    print(f"Saved records: {records_path}")


def main() -> None:
    args = parse_args()
    sec = SECService()

    all_records: List[FilingEdgeRecord] = []
    for symbol in [s.upper() for s in args.symbols]:
        try:
            recs = build_records_for_symbol(
                sec=sec,
                symbol=symbol,
                max_pairs=int(args.max_pairs),
                horizons=[int(h) for h in args.horizons],
                min_abs_edge_score=float(args.min_abs_edge_score),
            )
            all_records.extend(recs)
            print(f"[{symbol}] records={len(recs)}")
        except Exception as e:
            print(f"[{symbol}] failed: {e}")

    records_df = pd.DataFrame([asdict(r) for r in all_records]) if all_records else pd.DataFrame()
    summary = summarize(records_df, args)
    print(json.dumps(summary, indent=2))
    save_outputs(summary, records_df, args.output)


if __name__ == "__main__":
    main()

