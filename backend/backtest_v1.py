"""
Backtest harness for Kautilya v1 (point-in-time-safe price-based proxy).

What this does
- Uses historical OHLCV only (no live fundamentals/news snapshots) to avoid leakage.
- Reuses AnalysisEngine technical scoring and recommendation logic.
- Simulates a weekly rebalanced long-only portfolio (BUY / STRONG BUY = long, else cash).
- Reports signal "accuracy" over a forward hold horizon (default 20 trading days).

This is intentionally conservative and should be treated as a validation harness for v1 signal
behavior, not a production execution engine.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
import yfinance as yf

# Allow running as: python backend/backtest_v1.py
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.services.analysis_engine import AnalysisEngine


DEFAULT_UNIVERSE = ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA"]


@dataclass
class SignalRecord:
    date: str
    symbol: str
    action: str
    gated_action: str
    score: float
    tech_score: float
    upside_1y: float
    forward_return_pct: float | None
    hit: bool | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backtest Kautilya v1 price-based proxy signal.")
    parser.add_argument("--symbols", nargs="+", default=DEFAULT_UNIVERSE, help="Ticker symbols")
    parser.add_argument("--start", default="2021-01-01", help="Backtest start date (YYYY-MM-DD)")
    parser.add_argument("--end", default=None, help="Backtest end date (YYYY-MM-DD)")
    parser.add_argument("--warmup-days", type=int, default=260, help="Extra history days before start")
    parser.add_argument("--rebalance-days", type=int, default=5, help="Trading-day rebalance interval")
    parser.add_argument("--hold-days", type=int, default=20, help="Forward horizon for signal hit-rate")
    parser.add_argument("--cost-bps", type=float, default=10.0, help="One-way transaction cost in basis points")
    parser.add_argument("--min-history", type=int, default=220, help="Minimum trailing bars for signal")
    parser.add_argument("--output", default="data/backtests", help="Output directory")
    parser.add_argument("--tech-score-min", type=float, default=0.0, help="Minimum technical score to trade, else NO TRADE")
    parser.add_argument("--volatility-max", type=float, default=None, help="Max annualized volatility allowed (e.g. 0.45)")
    parser.add_argument("--require-uptrend", action="store_true", help="Require price > 200D SMA for long trades")
    parser.add_argument("--disable-sell-signals", action="store_true", help="Convert SELL/STRONG SELL into NO TRADE")
    return parser.parse_args()


def ensure_output_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        # yfinance can return either (Ticker, Field) or (Field, Ticker) depending on options/version.
        known_fields = {"Open", "High", "Low", "Close", "Adj Close", "Volume"}
        flat = []
        for col in df.columns:
            if isinstance(col, tuple):
                a, b = str(col[0]), str(col[1])
                if a in known_fields and b not in known_fields:
                    flat.append(f"{b}__{a}")
                elif b in known_fields and a not in known_fields:
                    flat.append(f"{a}__{b}")
                else:
                    flat.append(f"{a}__{b}")
            else:
                flat.append(str(col))
        df = df.copy()
        df.columns = flat
    return df


def fetch_universe(symbols: List[str], start: str, end: str | None, warmup_days: int) -> Dict[str, pd.DataFrame]:
    start_ts = pd.Timestamp(start) - pd.Timedelta(days=warmup_days)
    raw = yf.download(
        tickers=symbols,
        start=start_ts.strftime("%Y-%m-%d"),
        end=end,
        interval="1d",
        auto_adjust=True,
        progress=False,
        group_by="ticker",
    )
    if raw is None or raw.empty:
        raise RuntimeError("No price data returned from yfinance")

    raw = _flatten_columns(raw)
    by_symbol: Dict[str, pd.DataFrame] = {}
    for symbol in symbols:
        cols = {f"{symbol}__Open": "Open", f"{symbol}__High": "High", f"{symbol}__Low": "Low",
                f"{symbol}__Close": "Close", f"{symbol}__Volume": "Volume"}
        available = [c for c in cols if c in raw.columns]
        if not available and all(c in raw.columns for c in ["Open", "High", "Low", "Close", "Volume"]) and len(symbols) == 1:
            sdf = raw[["Open", "High", "Low", "Close", "Volume"]].copy()
        else:
            sdf = raw[available].rename(columns=cols).copy()

        if sdf.empty or "Close" not in sdf.columns:
            continue

        sdf = sdf.dropna(subset=["Close"]).copy()
        if "Volume" not in sdf.columns:
            sdf["Volume"] = 0
        for c in ["Open", "High", "Low", "Close", "Volume"]:
            if c not in sdf.columns:
                sdf[c] = np.nan if c != "Volume" else 0
        sdf.index = pd.to_datetime(sdf.index).tz_localize(None)
        by_symbol[symbol] = sdf[["Open", "High", "Low", "Close", "Volume"]]

    if not by_symbol:
        raise RuntimeError("Failed to build symbol price panels from yfinance output")
    return by_symbol


def price_window_to_engine_payload(df: pd.DataFrame) -> Dict[str, Any]:
    data = []
    for idx, row in df.iterrows():
        data.append({
            "date": idx.strftime("%Y-%m-%d"),
            "open": float(row["Open"]) if pd.notna(row["Open"]) else float(row["Close"]),
            "high": float(row["High"]) if pd.notna(row["High"]) else float(row["Close"]),
            "low": float(row["Low"]) if pd.notna(row["Low"]) else float(row["Close"]),
            "close": float(row["Close"]),
            "volume": int(row["Volume"]) if pd.notna(row["Volume"]) else 0,
        })
    return {"data": data, "dataPoints": len(data)}


def build_price_only_signal(engine: AnalysisEngine, window_df: pd.DataFrame) -> Dict[str, Any]:
    current_price = float(window_df["Close"].iloc[-1])
    historical = price_window_to_engine_payload(window_df)
    technical = engine._analyze_technicals(historical, current_price)

    # Point-in-time-safe proxy: neutral placeholders for unavailable data sources.
    neutral_sentiment = {"score": 50}
    neutral_growth = {"score": 50}
    info = {
        "symbol": "",
        "currentPrice": current_price,
        "previousClose": float(window_df["Close"].iloc[-2]) if len(window_df) > 1 else current_price,
        "targetHighPrice": 0,
        "targetLowPrice": 0,
        "targetMeanPrice": 0,
        "targetMedianPrice": 0,
        "revenueGrowth": 0,
        "earningsGrowth": 0,
    }

    targets = engine._calculate_price_targets(
        info=info,
        historical=historical,
        financials={},
        technical=technical,
        sentiment=neutral_sentiment,
        growth=neutral_growth,
    )
    recommendation = engine._get_recommendation(technical.get("score", 50), targets, current_price)

    return {
        "technical": technical,
        "targets": targets,
        "recommendation": recommendation,
        "current_price": current_price,
    }


def apply_trade_filters(raw_action: str, signal: Dict[str, Any], args: argparse.Namespace) -> str:
    """Apply risk/signal-quality gates and return final action or NO TRADE."""
    tech = signal.get("technical", {}) or {}
    indicators = tech.get("indicators", {}) or {}
    tech_score = float(tech.get("score", 0) or 0)
    current_price = float(signal.get("current_price", 0) or 0)

    if args.disable_sell_signals and raw_action in {"SELL", "STRONG SELL"}:
        return "NO TRADE"

    if tech_score < float(args.tech_score_min or 0):
        return "NO TRADE"

    if args.volatility_max is not None:
        vol = indicators.get("volatility")
        if vol is not None and float(vol) > float(args.volatility_max):
            return "NO TRADE"

    if args.require_uptrend and raw_action in {"BUY", "STRONG BUY"}:
        sma200 = indicators.get("sma200")
        if sma200 is None or current_price <= float(sma200):
            return "NO TRADE"

    return raw_action


def choose_rebalance_dates(calendar: pd.DatetimeIndex, start: str, step: int) -> List[pd.Timestamp]:
    start_ts = pd.Timestamp(start)
    eligible = [d for d in calendar if d >= start_ts]
    return eligible[::max(step, 1)]


def compute_metrics(equity_curve: pd.Series, turnover_costs: pd.Series) -> Dict[str, Any]:
    daily_returns = equity_curve.pct_change().fillna(0.0)
    total_return = float(equity_curve.iloc[-1] - 1.0) if len(equity_curve) else 0.0

    if len(equity_curve) > 1:
        days = (equity_curve.index[-1] - equity_curve.index[0]).days or 1
        years = days / 365.25
        cagr = float(equity_curve.iloc[-1] ** (1 / years) - 1) if years > 0 else 0.0
    else:
        cagr = 0.0

    vol = float(daily_returns.std() * np.sqrt(252)) if len(daily_returns) > 2 else 0.0
    sharpe = float((daily_returns.mean() / daily_returns.std()) * np.sqrt(252)) if daily_returns.std() > 0 else 0.0

    running_max = equity_curve.cummax() if len(equity_curve) else pd.Series(dtype=float)
    drawdown = (equity_curve / running_max - 1.0) if len(equity_curve) else pd.Series(dtype=float)
    max_dd = float(drawdown.min()) if len(drawdown) else 0.0

    return {
        "total_return_pct": round(total_return * 100, 2),
        "cagr_pct": round(cagr * 100, 2),
        "annualized_volatility_pct": round(vol * 100, 2),
        "sharpe": round(sharpe, 2),
        "max_drawdown_pct": round(max_dd * 100, 2),
        "avg_daily_return_pct": round(float(daily_returns.mean()) * 100, 4) if len(daily_returns) else 0.0,
        "total_turnover_cost_pct": round(float(turnover_costs.sum()) * 100, 2) if len(turnover_costs) else 0.0,
    }


def backtest(
    args: argparse.Namespace,
    price_map_override: Dict[str, pd.DataFrame] | None = None,
) -> Tuple[Dict[str, Any], pd.DataFrame, pd.DataFrame]:
    symbols = [s.upper() for s in args.symbols]
    price_map = price_map_override if price_map_override is not None else fetch_universe(symbols, args.start, args.end, args.warmup_days)
    symbols = [s for s in symbols if s in price_map]
    if not symbols:
        raise RuntimeError("None of the requested symbols returned usable data")

    common_calendar = sorted(set.intersection(*[set(df.index) for df in price_map.values()]))
    calendar = pd.DatetimeIndex(common_calendar)
    if len(calendar) < args.min_history + args.hold_days + 5:
        raise RuntimeError("Not enough common history across symbols for requested backtest settings")

    engine = AnalysisEngine()
    rebalance_dates = choose_rebalance_dates(calendar, args.start, args.rebalance_days)

    signals: List[SignalRecord] = []
    weights_by_date: Dict[pd.Timestamp, Dict[str, float]] = {}

    for dt in rebalance_dates:
        per_symbol = []
        for symbol in symbols:
            sdf = price_map[symbol]
            if dt not in sdf.index:
                continue
            pos = sdf.index.get_loc(dt)
            if isinstance(pos, slice):
                pos = pos.stop - 1
            if pos < args.min_history - 1:
                continue

            window = sdf.iloc[: pos + 1].tail(max(args.min_history, 252))
            sig = build_price_only_signal(engine, window)
            action = sig["recommendation"]["action"]
            gated_action = apply_trade_filters(action, sig, args)
            score = float(sig["recommendation"]["score"])
            tech_score = float(sig["technical"]["score"])
            upside_1y = float(sig["recommendation"].get("upside1Y", 0))

            fwd_ret = None
            hit = None
            exit_pos = min(pos + args.hold_days, len(sdf.index) - 1)
            if exit_pos > pos:
                entry_close = float(sdf["Close"].iloc[pos])
                exit_close = float(sdf["Close"].iloc[exit_pos])
                fwd_ret = (exit_close / entry_close) - 1.0
                if gated_action in {"BUY", "STRONG BUY"}:
                    hit = fwd_ret > 0
                elif gated_action in {"SELL", "STRONG SELL"}:
                    hit = fwd_ret < 0

            signals.append(SignalRecord(
                date=dt.strftime("%Y-%m-%d"),
                symbol=symbol,
                action=action,
                gated_action=gated_action,
                score=round(score, 2),
                tech_score=round(tech_score, 2),
                upside_1y=round(upside_1y, 2),
                forward_return_pct=round(fwd_ret * 100, 2) if fwd_ret is not None else None,
                hit=hit,
            ))

            if gated_action in {"BUY", "STRONG BUY"}:
                per_symbol.append(symbol)

        if per_symbol:
            w = 1.0 / len(per_symbol)
            weights_by_date[dt] = {s: w for s in per_symbol}
        else:
            weights_by_date[dt] = {}

    # Portfolio simulation on common calendar using rebalance weights held until next rebalance.
    daily_rows = []
    equity = 1.0
    prev_weights: Dict[str, float] = {}

    if not rebalance_dates:
        raise RuntimeError("No rebalance dates generated")

    # Start after first rebalance date to avoid look-ahead; apply first weights on next trading day.
    rebalance_set = set(rebalance_dates)
    current_weights: Dict[str, float] = {}
    pending_rebalance = None

    for i in range(1, len(calendar)):
        d_prev = calendar[i - 1]
        d_cur = calendar[i]

        # Use signal generated on prior close when the rebalance date was yesterday.
        if d_prev in rebalance_set:
            pending_rebalance = d_prev

        turnover_cost = 0.0
        if pending_rebalance is not None:
            target = weights_by_date.get(pending_rebalance, {})
            all_keys = set(prev_weights) | set(target)
            turnover = sum(abs(target.get(k, 0.0) - prev_weights.get(k, 0.0)) for k in all_keys)
            turnover_cost = (turnover * (args.cost_bps / 10000.0))
            current_weights = target
            prev_weights = target
            pending_rebalance = None

        gross_return = 0.0
        for symbol, weight in current_weights.items():
            sdf = price_map[symbol]
            if d_prev in sdf.index and d_cur in sdf.index:
                p0 = float(sdf.loc[d_prev, "Close"])
                p1 = float(sdf.loc[d_cur, "Close"])
                if p0 > 0:
                    gross_return += weight * ((p1 / p0) - 1.0)

        net_return = gross_return - turnover_cost
        equity *= (1.0 + net_return)
        daily_rows.append({
            "date": d_cur,
            "portfolio_return": net_return,
            "gross_return": gross_return,
            "turnover_cost": turnover_cost,
            "equity": equity,
            "num_positions": len(current_weights),
            "positions": ",".join(sorted(current_weights.keys())),
        })

    daily_df = pd.DataFrame(daily_rows)
    if daily_df.empty:
        raise RuntimeError("No daily portfolio rows generated")
    daily_df["date"] = pd.to_datetime(daily_df["date"])
    daily_df = daily_df.set_index("date")

    signals_df = pd.DataFrame([s.__dict__ for s in signals])
    metrics = compute_metrics(daily_df["equity"], daily_df["turnover_cost"])

    # Accuracy breakdown (forward horizon)
    evaluable = signals_df.dropna(subset=["hit"]).copy()
    action_stats = []
    for action, grp in evaluable.groupby("gated_action"):
        hits = grp["hit"].astype(bool)
        action_stats.append({
            "action": action,
            "count": int(len(grp)),
            "hit_rate_pct": round(float(hits.mean()) * 100, 2) if len(grp) else None,
            "avg_forward_return_pct": round(float(grp["forward_return_pct"].mean()), 2) if len(grp) else None,
        })
    gated_counts = {str(k): int(v) for k, v in signals_df["gated_action"].value_counts().sort_index().items()} if not signals_df.empty else {}

    summary = {
        "config": {
            "symbols": symbols,
            "start": args.start,
            "end": args.end,
            "rebalance_days": args.rebalance_days,
            "hold_days": args.hold_days,
            "min_history": args.min_history,
            "cost_bps": args.cost_bps,
            "signal_model": "v1_price_only_technical_proxy",
            "portfolio_model": "long_only_equal_weight_buy_signals",
            "filters": {
                "tech_score_min": args.tech_score_min,
                "volatility_max": args.volatility_max,
                "require_uptrend": bool(args.require_uptrend),
                "disable_sell_signals": bool(args.disable_sell_signals),
            },
        },
        "coverage": {
            "calendar_start": calendar[0].strftime("%Y-%m-%d"),
            "calendar_end": calendar[-1].strftime("%Y-%m-%d"),
            "rebalance_count": int(len(rebalance_dates)),
            "signals_generated": int(len(signals_df)),
            "evaluable_signals": int(len(evaluable)),
        },
        "portfolio_metrics": metrics,
        "signal_accuracy": {
            "overall_hit_rate_pct": round(float(evaluable["hit"].astype(bool).mean()) * 100, 2) if len(evaluable) else None,
            "avg_forward_return_pct": round(float(evaluable["forward_return_pct"].mean()), 2) if len(evaluable) else None,
            "gated_action_counts": gated_counts,
            "by_action": sorted(action_stats, key=lambda x: x["action"]),
        },
        "notes": [
            "Point-in-time-safe proxy uses only historical prices (technical score + price-target logic).",
            "Current v1 full-stack fundamentals/news are not backtested here to avoid look-ahead leakage.",
            "Treat this as validation for signal behavior and threshold tuning before v2 risk gates.",
        ],
    }

    return summary, daily_df, signals_df


def save_outputs(summary: Dict[str, Any], daily_df: pd.DataFrame, signals_df: pd.DataFrame, outdir: str) -> None:
    ensure_output_dir(outdir)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    summary_path = os.path.join(outdir, f"backtest_summary_{ts}.json")
    daily_path = os.path.join(outdir, f"backtest_daily_{ts}.csv")
    signals_path = os.path.join(outdir, f"backtest_signals_{ts}.csv")

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    daily_df.to_csv(daily_path)
    signals_df.to_csv(signals_path, index=False)

    print(f"Saved summary: {summary_path}")
    print(f"Saved daily equity: {daily_path}")
    print(f"Saved signals: {signals_path}")


def main() -> None:
    args = parse_args()
    summary, daily_df, signals_df = backtest(args)
    print(json.dumps(summary, indent=2))
    save_outputs(summary, daily_df, signals_df, args.output)


if __name__ == "__main__":
    main()
