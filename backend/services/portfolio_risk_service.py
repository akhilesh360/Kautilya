import math
from datetime import datetime
from typing import Any, Dict, List


class PortfolioRiskService:
    """
    Simple portfolio guardrail evaluator for paper/live-prep workflows.
    Uses current holdings, prices, and optional AUM/drawdown inputs to assess risk breaches.
    """

    DEFAULT_LIMITS = {
        "max_position_weight": 0.20,
        "max_sector_weight": 0.40,
        "drawdown_kill_switch_pct": 15.0,
    }

    def evaluate(
        self,
        positions: List[Dict[str, Any]],
        company_data: Dict[str, Dict[str, Any]],
        aum_usd: float | None = None,
        current_drawdown_pct: float | None = None,
        limits: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        limits_cfg = dict(self.DEFAULT_LIMITS)
        if limits:
            limits_cfg.update({k: v for k, v in limits.items() if v is not None})

        normalized_positions = []
        total_market_value = 0.0

        for p in positions or []:
            symbol = str(p.get("symbol", "")).upper().strip()
            if not symbol:
                continue
            qty = float(p.get("qty", 0) or 0)
            avg_price = float(p.get("avgPrice", 0) or 0)
            info = company_data.get(symbol, {}) or {}
            current_price = float(info.get("currentPrice", 0) or info.get("previousClose", 0) or 0)
            market_value = current_price * qty if current_price > 0 and qty > 0 else 0.0
            total_market_value += market_value
            normalized_positions.append({
                "symbol": symbol,
                "qty": qty,
                "avgPrice": avg_price,
                "currentPrice": round(current_price, 4) if current_price else 0,
                "marketValue": round(market_value, 2),
                "sector": info.get("sector") or "Unknown",
                "industry": info.get("industry") or "Unknown",
            })

        portfolio_base = float(aum_usd or total_market_value or 0)
        if portfolio_base <= 0:
            portfolio_base = total_market_value

        sector_weights: Dict[str, float] = {}
        breaches = []
        warnings = []

        for p in normalized_positions:
            weight = (p["marketValue"] / portfolio_base) if portfolio_base > 0 else 0.0
            p["weightPct"] = round(weight * 100, 2)
            p["unrealizedPnl"] = round((p["currentPrice"] - p["avgPrice"]) * p["qty"], 2)
            p["unrealizedPnlPct"] = round((((p["currentPrice"] - p["avgPrice"]) / p["avgPrice"]) * 100) if p["avgPrice"] > 0 else 0.0, 2)
            sector_weights[p["sector"]] = sector_weights.get(p["sector"], 0.0) + weight

            if weight > float(limits_cfg["max_position_weight"]):
                breaches.append({
                    "type": "position_weight",
                    "symbol": p["symbol"],
                    "severity": "high",
                    "detail": f"{p['symbol']} weight {weight*100:.2f}% exceeds {float(limits_cfg['max_position_weight'])*100:.0f}% cap",
                })

        sector_exposure = []
        for sector, weight in sorted(sector_weights.items(), key=lambda x: x[1], reverse=True):
            sector_exposure.append({"sector": sector, "weightPct": round(weight * 100, 2)})
            if weight > float(limits_cfg["max_sector_weight"]):
                breaches.append({
                    "type": "sector_weight",
                    "sector": sector,
                    "severity": "high",
                    "detail": f"{sector} exposure {weight*100:.2f}% exceeds {float(limits_cfg['max_sector_weight'])*100:.0f}% cap",
                })

        kill_switch_triggered = False
        if current_drawdown_pct is not None:
            dd = float(current_drawdown_pct)
            if dd <= -abs(float(limits_cfg["drawdown_kill_switch_pct"])):
                kill_switch_triggered = True
                breaches.append({
                    "type": "drawdown_kill_switch",
                    "severity": "critical",
                    "detail": f"Portfolio drawdown {dd:.2f}% breached kill switch {-abs(float(limits_cfg['drawdown_kill_switch_pct'])):.2f}%",
                })
        else:
            warnings.append("current_drawdown_pct not provided; drawdown kill switch not evaluated.")

        if len(normalized_positions) == 0:
            warnings.append("No valid positions provided.")

        if portfolio_base > 0 and total_market_value / portfolio_base < 0.25:
            warnings.append("Portfolio is mostly cash relative to declared AUM.")

        status = "PASS"
        if any(b["severity"] == "critical" for b in breaches):
            status = "KILL_SWITCH"
        elif breaches:
            status = "FAIL"

        return {
            "status": status,
            "evaluatedAt": datetime.utcnow().isoformat() + "Z",
            "limits": limits_cfg,
            "portfolio": {
                "aumUsd": round(float(aum_usd), 2) if aum_usd is not None else None,
                "totalMarketValue": round(total_market_value, 2),
                "grossExposurePct": round((total_market_value / portfolio_base) * 100, 2) if portfolio_base > 0 else 0,
                "positionCount": len(normalized_positions),
                "drawdownKillSwitchTriggered": kill_switch_triggered,
                "currentDrawdownPct": round(float(current_drawdown_pct), 2) if current_drawdown_pct is not None else None,
            },
            "positions": normalized_positions,
            "sectorExposure": sector_exposure,
            "breaches": breaches,
            "warnings": warnings,
        }
