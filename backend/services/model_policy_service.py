import json
import os
from copy import deepcopy
from typing import Any, Dict


class ModelPolicyService:
    """
    Loads regime-aware model policy (weights + thresholds).
    Falls back to a safe default v2 policy if no external policy file exists.
    """

    DEFAULT_POLICY = {
        "version": "v2-default",
        "weights_by_regime": {
            "bull_low_vol": {
                "fundamental": 0.28,
                "technical": 0.30,
                "sentiment": 0.12,
                "valuation": 0.12,
                "growth": 0.10,
                "institutional": 0.08,
            },
            "bull_high_vol": {
                "fundamental": 0.23,
                "technical": 0.34,
                "sentiment": 0.16,
                "valuation": 0.10,
                "growth": 0.09,
                "institutional": 0.08,
            },
            "sideways": {
                "fundamental": 0.25,
                "technical": 0.28,
                "sentiment": 0.14,
                "valuation": 0.16,
                "growth": 0.09,
                "institutional": 0.08,
            },
            "bear_high_vol": {
                "fundamental": 0.22,
                "technical": 0.34,
                "sentiment": 0.18,
                "valuation": 0.08,
                "growth": 0.08,
                "institutional": 0.10,
            },
            "unknown": {
                "fundamental": 0.30,
                "technical": 0.25,
                "sentiment": 0.15,
                "valuation": 0.15,
                "growth": 0.10,
                "institutional": 0.05,
            },
        },
        "thresholds_by_regime": {
            "bull_low_vol": {
                "strong_buy_score": 72,
                "strong_buy_upside": 8,
                "buy_score": 60,
                "buy_score_with_upside": 52,
                "buy_upside": 12,
                "hold_min": 45,
                "sell_min": 35,
            },
            "bull_high_vol": {
                "strong_buy_score": 75,
                "strong_buy_upside": 12,
                "buy_score": 63,
                "buy_score_with_upside": 55,
                "buy_upside": 16,
                "hold_min": 48,
                "sell_min": 38,
            },
            "sideways": {
                "strong_buy_score": 74,
                "strong_buy_upside": 12,
                "buy_score": 62,
                "buy_score_with_upside": 54,
                "buy_upside": 15,
                "hold_min": 47,
                "sell_min": 37,
            },
            "bear_high_vol": {
                "strong_buy_score": 80,
                "strong_buy_upside": 18,
                "buy_score": 68,
                "buy_score_with_upside": 60,
                "buy_upside": 20,
                "hold_min": 52,
                "sell_min": 42,
            },
            "unknown": {
                "strong_buy_score": 70,
                "strong_buy_upside": 10,
                "buy_score": 60,
                "buy_score_with_upside": 50,
                "buy_upside": 15,
                "hold_min": 45,
                "sell_min": 35,
            },
        },
        "ui_mapping": {
            "map_no_trade_to_hold": True,
            "hold_display_label": "HOLD (Do Nothing)"
        }
    }

    def __init__(self, policy_path: str | None = None):
        self.policy_path = policy_path or os.environ.get("KAUTILYA_POLICY_PATH", "data/model/policy_v2.json")
        self._cache = None

    def load_policy(self) -> Dict[str, Any]:
        if self._cache is not None:
            return self._cache
        policy = deepcopy(self.DEFAULT_POLICY)
        try:
            if self.policy_path and os.path.exists(self.policy_path):
                with open(self.policy_path, "r", encoding="utf-8") as f:
                    user_policy = json.load(f)
                if isinstance(user_policy, dict):
                    self._deep_merge(policy, user_policy)
        except Exception:
            pass
        self._cache = policy
        return policy

    def get_weights(self, regime: str) -> Dict[str, float]:
        p = self.load_policy()
        weights = p.get("weights_by_regime", {}).get(regime) or p.get("weights_by_regime", {}).get("unknown", {})
        total = sum(float(v) for v in weights.values()) or 1.0
        return {k: float(v) / total for k, v in weights.items()}

    def get_thresholds(self, regime: str) -> Dict[str, Any]:
        p = self.load_policy()
        return dict(p.get("thresholds_by_regime", {}).get(regime) or p.get("thresholds_by_regime", {}).get("unknown", {}))

    def get_ui_mapping(self) -> Dict[str, Any]:
        return dict(self.load_policy().get("ui_mapping", {}))

    def _deep_merge(self, base: Dict[str, Any], patch: Dict[str, Any]) -> None:
        for k, v in patch.items():
            if isinstance(v, dict) and isinstance(base.get(k), dict):
                self._deep_merge(base[k], v)
            else:
                base[k] = v
