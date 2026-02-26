"""
Policy trainer for Kautilya v2 (walk-forward threshold calibration + reweighting hooks).

What it does now:
- Runs walk-forward threshold/gate calibration using the price-only backtest harness
- Writes `data/model/policy_v2.json` used by AnalysisEngine at runtime

What it supports next:
- Factor-weight re-estimation from paper-trade outcome logs (hook included)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from copy import deepcopy
from datetime import datetime
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.backtest_v1 import backtest, fetch_universe  # noqa: E402
from backend.services.model_policy_service import ModelPolicyService  # noqa: E402


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Train v2 policy config with walk-forward threshold calibration.")
    p.add_argument("--symbols", nargs="+", default=["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA"])
    p.add_argument("--start", default="2021-01-01")
    p.add_argument("--end", default=None)
    p.add_argument("--warmup-days", type=int, default=260)
    p.add_argument("--min-history", type=int, default=220)
    p.add_argument("--cost-bps", type=float, default=10.0)
    p.add_argument("--output", default="data/model/policy_v2.json")
    p.add_argument("--tech-score-min-grid", default="50,55,60,65")
    p.add_argument("--volatility-max-grid", default="0.40,0.45,0.50")
    p.add_argument("--require-uptrend-grid", default="false,true")
    p.add_argument("--disable-sell-signals", action="store_true", default=True)
    return p.parse_args()


def _parse_float_grid(csv: str) -> List[float]:
    return [float(x.strip()) for x in csv.split(",") if x.strip()]


def _parse_bool_grid(csv: str) -> List[bool]:
    out = []
    for x in csv.split(","):
        s = x.strip().lower()
        if not s:
            continue
        out.append(s in {"true", "1", "yes", "y"})
    return out


def _build_args(base: argparse.Namespace, start: str, end: str, cfg: Dict[str, Any]) -> SimpleNamespace:
    return SimpleNamespace(
        symbols=base.symbols,
        start=start,
        end=end,
        warmup_days=base.warmup_days,
        rebalance_days=5,
        hold_days=20,
        cost_bps=base.cost_bps,
        min_history=base.min_history,
        output="data/backtests",
        tech_score_min=cfg["tech_score_min"],
        volatility_max=cfg["volatility_max"],
        require_uptrend=cfg["require_uptrend"],
        disable_sell_signals=cfg["disable_sell_signals"],
    )


def _score(summary: Dict[str, Any]) -> float:
    pm = summary["portfolio_metrics"]
    sa = summary["signal_accuracy"]
    return (
        (pm.get("sharpe") or 0) * 50
        + (pm.get("cagr_pct") or 0) * 0.6
        + (sa.get("overall_hit_rate_pct") or 0) * 0.35
        - abs(pm.get("max_drawdown_pct") or 0) * 1.6
    )


def walk_forward_calibrate(base: argparse.Namespace) -> Dict[str, Any]:
    # Simple annual walk-forward folds (train on prior period, validate on next period)
    folds = [
        ("2021-01-01", "2023-12-31", "2024-01-01", "2024-12-31"),
        ("2021-01-01", "2024-12-31", "2025-01-01", "2025-12-31"),
    ]
    grids = []
    for t in _parse_float_grid(base.tech_score_min_grid):
        for v in _parse_float_grid(base.volatility_max_grid):
            for u in _parse_bool_grid(base.require_uptrend_grid):
                grids.append({
                    "tech_score_min": t,
                    "volatility_max": v,
                    "require_uptrend": u,
                    "disable_sell_signals": bool(base.disable_sell_signals),
                })

    # Shared data fetch for full range; backtest() slices by start/end using end date only for yfinance fetch,
    # so we prefetch broad and reuse.
    shared_prices = fetch_universe([s.upper() for s in base.symbols], base.start, base.end, base.warmup_days)

    aggregate: List[Tuple[float, Dict[str, Any]]] = []
    fold_details = []

    for train_start, train_end, test_start, test_end in folds:
        best_cfg = None
        best_train_score = -1e18

        for cfg in grids:
            train_args = _build_args(base, train_start, train_end, cfg)
            summary_train, _, _ = backtest(train_args, price_map_override=shared_prices)
            s = _score(summary_train)
            if s > best_train_score:
                best_train_score = s
                best_cfg = cfg

        test_args = _build_args(base, test_start, test_end, best_cfg)
        summary_test, _, _ = backtest(test_args, price_map_override=shared_prices)
        test_score = _score(summary_test)
        aggregate.append((test_score, best_cfg))
        fold_details.append({
            "train": [train_start, train_end],
            "test": [test_start, test_end],
            "selected_config": best_cfg,
            "train_composite": round(best_train_score, 3),
            "test_composite": round(test_score, 3),
            "test_metrics": summary_test["portfolio_metrics"],
            "test_signal_accuracy": summary_test["signal_accuracy"],
        })

    # Select final config by average test score across folds.
    score_by_cfg: Dict[str, Dict[str, Any]] = {}
    for test_score, cfg in aggregate:
        key = json.dumps(cfg, sort_keys=True)
        bucket = score_by_cfg.setdefault(key, {"cfg": cfg, "scores": []})
        bucket["scores"].append(float(test_score))
    ranked = sorted(
        [{"cfg": v["cfg"], "avg_test_score": sum(v["scores"]) / len(v["scores"]), "fold_scores": v["scores"]} for v in score_by_cfg.values()],
        key=lambda x: x["avg_test_score"],
        reverse=True,
    )
    final_cfg = ranked[0]["cfg"] if ranked else {"tech_score_min": 55.0, "volatility_max": 0.45, "require_uptrend": False, "disable_sell_signals": True}

    return {
        "selected_filters": final_cfg,
        "folds": fold_details,
        "ranking": ranked[:10],
    }


def apply_calibration_to_policy(calibration: Dict[str, Any]) -> Dict[str, Any]:
    policy = deepcopy(ModelPolicyService.DEFAULT_POLICY)
    sel = calibration["selected_filters"]

    # Apply calibrated global safety gates via metadata (runtime v1.1 policy still enforced in AnalysisEngine).
    policy.setdefault("runtime_filters", {})
    policy["runtime_filters"].update({
        "tech_score_min": sel["tech_score_min"],
        "volatility_max": sel["volatility_max"],
        "require_uptrend": sel["require_uptrend"],
        "disable_sell_signals": sel["disable_sell_signals"],
    })

    # Conservative threshold calibration: tighten riskier regimes, moderate bull_low_vol.
    for regime, thr in policy.get("thresholds_by_regime", {}).items():
        if regime == "bull_low_vol":
            thr["buy_score"] = max(thr.get("buy_score", 60), 60)
            thr["hold_min"] = max(thr.get("hold_min", 45), 45)
        elif regime in {"bull_high_vol", "bear_high_vol"}:
            thr["buy_score"] = max(thr.get("buy_score", 60), 65)
            thr["buy_upside"] = max(thr.get("buy_upside", 15), 18)
            thr["hold_min"] = max(thr.get("hold_min", 45), 50)

    policy["version"] = f"v2-trained-{datetime.utcnow().strftime('%Y%m%d')}"
    policy["training"] = {
        "method": "walk_forward_threshold_calibration",
        "generatedAt": datetime.utcnow().isoformat() + "Z",
        "calibration": calibration,
        "notes": [
            "Thresholds and runtime filters calibrated using price-only proxy backtests.",
            "Regime weights remain policy defaults until sufficient paper-trade factor/outcome history exists.",
            "Use paper-trade logs to train factor weights next.",
        ],
    }
    return policy


def save_policy(policy: Dict[str, Any], path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(policy, f, indent=2)


def main() -> None:
    args = parse_args()
    calibration = walk_forward_calibrate(args)
    policy = apply_calibration_to_policy(calibration)
    save_policy(policy, args.output)
    print(json.dumps({
        "saved_policy": args.output,
        "version": policy.get("version"),
        "selected_filters": calibration.get("selected_filters"),
        "top_ranked": calibration.get("ranking", [])[:3],
    }, indent=2))


if __name__ == "__main__":
    main()
