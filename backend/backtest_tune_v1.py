"""
Grid search tuner for Kautilya v1 backtest filters.

Runs multiple backtests over combinations of:
- tech_score_min
- volatility_max
- require_uptrend
- disable_sell_signals
- rebalance_days / hold_days (optional)

Ranks results using a simple composite score emphasizing Sharpe and drawdown.
"""

from __future__ import annotations

import argparse
import csv
import itertools
import json
import os
import sys
import warnings
from datetime import datetime
from types import SimpleNamespace
from typing import Any, Dict, Iterable, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.backtest_v1 import backtest, ensure_output_dir, fetch_universe  # noqa: E402


def parse_csv_floats(value: str) -> List[Optional[float]]:
    out: List[Optional[float]] = []
    for item in value.split(","):
        s = item.strip().lower()
        if not s:
            continue
        if s in {"none", "null"}:
            out.append(None)
        else:
            out.append(float(s))
    return out


def parse_csv_ints(value: str) -> List[int]:
    return [int(x.strip()) for x in value.split(",") if x.strip()]


def parse_csv_bools(value: str) -> List[bool]:
    out = []
    for item in value.split(","):
        s = item.strip().lower()
        if not s:
            continue
        out.append(s in {"1", "true", "yes", "y", "on"})
    return out


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Tune Kautilya v1 backtest filters via grid search.")
    p.add_argument("--symbols", nargs="+", default=["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA"])
    p.add_argument("--start", default="2021-01-01")
    p.add_argument("--end", default=None)
    p.add_argument("--warmup-days", type=int, default=260)
    p.add_argument("--min-history", type=int, default=220)
    p.add_argument("--cost-bps", type=float, default=10.0)
    p.add_argument("--tech-score-min-grid", default="0,55,60,65,70")
    p.add_argument("--volatility-max-grid", default="none,0.50,0.45,0.40")
    p.add_argument("--require-uptrend-grid", default="false,true")
    p.add_argument("--disable-sell-signals-grid", default="true")
    p.add_argument("--rebalance-days-grid", default="5")
    p.add_argument("--hold-days-grid", default="20")
    p.add_argument("--top-n", type=int, default=10)
    p.add_argument("--output", default="data/backtests/tuning")
    return p.parse_args()


def composite_score(summary: Dict[str, Any]) -> float:
    pm = summary["portfolio_metrics"]
    sa = summary["signal_accuracy"]

    sharpe = float(pm.get("sharpe") or 0.0)
    cagr = float(pm.get("cagr_pct") or 0.0)
    max_dd = abs(float(pm.get("max_drawdown_pct") or 0.0))
    hit = float(sa.get("overall_hit_rate_pct") or 0.0)

    # Weighted score (higher is better); penalize drawdown strongly.
    return (
        sharpe * 40.0
        + cagr * 0.8
        + hit * 0.4
        - max_dd * 1.3
    )


def iter_grid(args: argparse.Namespace) -> Iterable[Dict[str, Any]]:
    tech_grid = parse_csv_floats(args.tech_score_min_grid)
    vol_grid = parse_csv_floats(args.volatility_max_grid)
    uptrend_grid = parse_csv_bools(args.require_uptrend_grid)
    sell_grid = parse_csv_bools(args.disable_sell_signals_grid)
    reb_grid = parse_csv_ints(args.rebalance_days_grid)
    hold_grid = parse_csv_ints(args.hold_days_grid)

    for tech, vol, uptrend, disable_sells, reb, hold in itertools.product(
        tech_grid, vol_grid, uptrend_grid, sell_grid, reb_grid, hold_grid
    ):
        yield {
            "tech_score_min": float(tech or 0.0),
            "volatility_max": vol,
            "require_uptrend": bool(uptrend),
            "disable_sell_signals": bool(disable_sells),
            "rebalance_days": int(reb),
            "hold_days": int(hold),
        }


def build_backtest_namespace(base: argparse.Namespace, cfg: Dict[str, Any]) -> SimpleNamespace:
    return SimpleNamespace(
        symbols=base.symbols,
        start=base.start,
        end=base.end,
        warmup_days=base.warmup_days,
        rebalance_days=cfg["rebalance_days"],
        hold_days=cfg["hold_days"],
        cost_bps=base.cost_bps,
        min_history=base.min_history,
        output=base.output,
        tech_score_min=cfg["tech_score_min"],
        volatility_max=cfg["volatility_max"],
        require_uptrend=cfg["require_uptrend"],
        disable_sell_signals=cfg["disable_sell_signals"],
    )


def main() -> None:
    args = parse_args()
    ensure_output_dir(args.output)

    rows: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    warnings.filterwarnings("ignore", category=FutureWarning, module="yfinance")

    grid = list(iter_grid(args))
    total = len(grid)
    print(f"Running {total} tuning combinations...")
    print("Fetching price history once for all combinations...")
    shared_price_map = fetch_universe([s.upper() for s in args.symbols], args.start, args.end, args.warmup_days)

    for i, cfg in enumerate(grid, start=1):
        print(f"[{i}/{total}] {cfg}")
        bt_args = build_backtest_namespace(args, cfg)
        try:
            summary, _, _ = backtest(bt_args, price_map_override=shared_price_map)
            pm = summary["portfolio_metrics"]
            sa = summary["signal_accuracy"]
            row = {
                **cfg,
                "composite_score": round(composite_score(summary), 3),
                "total_return_pct": pm.get("total_return_pct"),
                "cagr_pct": pm.get("cagr_pct"),
                "sharpe": pm.get("sharpe"),
                "max_drawdown_pct": pm.get("max_drawdown_pct"),
                "annualized_volatility_pct": pm.get("annualized_volatility_pct"),
                "overall_hit_rate_pct": sa.get("overall_hit_rate_pct"),
                "avg_forward_return_pct": sa.get("avg_forward_return_pct"),
                "gated_action_counts": json.dumps(sa.get("gated_action_counts", {}), sort_keys=True),
            }
            rows.append(row)
        except Exception as e:
            failures.append({"config": cfg, "error": str(e)})

    rows.sort(key=lambda r: (r["composite_score"], r.get("sharpe") or -999, r.get("cagr_pct") or -999), reverse=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = os.path.join(args.output, f"tuning_results_{ts}.csv")
    json_path = os.path.join(args.output, f"tuning_summary_{ts}.json")

    fieldnames = [
        "composite_score",
        "tech_score_min",
        "volatility_max",
        "require_uptrend",
        "disable_sell_signals",
        "rebalance_days",
        "hold_days",
        "total_return_pct",
        "cagr_pct",
        "sharpe",
        "max_drawdown_pct",
        "annualized_volatility_pct",
        "overall_hit_rate_pct",
        "avg_forward_return_pct",
        "gated_action_counts",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k) for k in fieldnames})

    summary_payload = {
        "ran_at": datetime.now().isoformat(),
        "grid_size": total,
        "successful_runs": len(rows),
        "failed_runs": len(failures),
        "top_n": args.top_n,
        "top_results": rows[: args.top_n],
        "failures": failures[:20],
        "csv_path": csv_path,
    }
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(summary_payload, f, indent=2)

    print(json.dumps(summary_payload, indent=2))
    print(f"Saved tuning CSV: {csv_path}")
    print(f"Saved tuning summary: {json_path}")


if __name__ == "__main__":
    main()
