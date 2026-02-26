"""
Stock Data Service — Resilient Yahoo Finance fetcher.

Uses multiple strategies to get data:
1. yfinance.download() for price data (most reliable endpoint)
2. Ticker.info for fundamentals (now letting yfinance handle sessions natively)
3. Graceful degradation — proceeds with partial data rather than failing
"""

import yfinance as yf
import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import logging
import time
import random
import os

logger = logging.getLogger(__name__)


def _get_session():
    """Create a persistent session with rotating User-Agents and FORCED timeouts."""
    session = requests.Session()
    uas = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0'
    ]
    session.headers.update({'User-Agent': random.choice(uas)})
    
    # Extreme Guard: Force timeout on all calls made through this session
    orig_request = session.request
    def forced_timeout_request(*args, **kwargs):
        kwargs.setdefault('timeout', 15)
        return orig_request(*args, **kwargs)
    session.request = forced_timeout_request
    
    return session


def _safe_get(func, default=None, label=""):
    """Safely call a function, returning default on any error."""
    try:
        result = func()
        if result is None:
            return default
        return result
    except Exception as e:
        if label:
            logger.debug(f"Safe get failed for {label}: {e}")
        return default


class StockDataService:
    """Service for fetching and processing stock market data."""

    def __init__(self):
        print("Initializing StockDataService internals...")
        self._result_cache = {}
        self.fmp_api_key = (os.getenv("FMP_API_KEY") or "").strip()
        self.fmp_base_url = "https://financialmodelingprep.com/stable"

    def _has_fmp(self) -> bool:
        return bool(self.fmp_api_key)

    def _fmp_get(self, endpoint: str, params: Optional[Dict[str, Any]] = None, timeout: int = 12):
        """Call FMP stable API, returning parsed JSON or None on failure."""
        if not self._has_fmp():
            return None
        try:
            qp = dict(params or {})
            qp["apikey"] = self.fmp_api_key
            url = f"{self.fmp_base_url}/{endpoint.lstrip('/')}"
            resp = requests.get(url, params=qp, timeout=timeout)
            if resp.status_code == 429:
                logger.warning("FMP rate limit hit (429) for %s", endpoint)
                return None
            if resp.status_code in (401, 403):
                logger.warning("FMP auth error (%s) for %s", resp.status_code, endpoint)
                return None
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.debug(f"FMP request failed for {endpoint}: {e}")
            return None

    def _first_record(self, payload):
        if isinstance(payload, list):
            return payload[0] if payload else {}
        if isinstance(payload, dict):
            return payload
        return {}

    def _enrich_from_fmp(self, result: Dict[str, Any], symbol: str) -> None:
        """
        FMP primary enrichment for profile/quote/ratios/growth.
        Uses a minimal endpoint set to preserve free-tier calls.
        """
        if not self._has_fmp():
            return

        # Cache FMP merged data separately to avoid repeated paid calls.
        fmp_cache_key = f"fmp_info_{symbol}"
        if fmp_cache_key in self._result_cache:
            self._merge_non_empty(result, self._result_cache[fmp_cache_key])
            return

        merged: Dict[str, Any] = {}

        profile = self._first_record(self._fmp_get("profile", {"symbol": symbol}))
        quote = self._first_record(self._fmp_get("quote", {"symbol": symbol}))
        ratios_ttm = self._first_record(self._fmp_get("ratios-ttm", {"symbol": symbol}))
        metrics_ttm = self._first_record(self._fmp_get("key-metrics-ttm", {"symbol": symbol}))
        growth = self._first_record(self._fmp_get("financial-growth", {"symbol": symbol, "limit": 1}))

        if profile:
            self._merge_non_empty(merged, {
                'name': profile.get('companyName') or profile.get('name'),
                'sector': profile.get('sector'),
                'industry': profile.get('industry'),
                'description': profile.get('description'),
                'website': profile.get('website'),
                'employees': self._safe_int(profile.get('fullTimeEmployees')),
                'country': profile.get('country'),
                'exchange': profile.get('exchange') or profile.get('exchangeShortName'),
                'currency': profile.get('currency'),
                'marketCap': self._safe_int(profile.get('marketCap')),
                'beta': self._safe_float(profile.get('beta')),
                'dividendYield': self._safe_float(profile.get('lastDividend')),  # proxy, may be amount not yield
                'volume': self._safe_int(profile.get('volume')),
                'avgVolume': self._safe_int(profile.get('averageVolume')),
                'currentPrice': self._safe_float(profile.get('price')),
            })

        if quote:
            self._merge_non_empty(merged, {
                'currentPrice': self._safe_float(quote.get('price')),
                'previousClose': self._safe_float(quote.get('previousClose')),
                'open': self._safe_float(quote.get('open')),
                'dayLow': self._safe_float(quote.get('dayLow')),
                'dayHigh': self._safe_float(quote.get('dayHigh')),
                'volume': self._safe_int(quote.get('volume')),
                'avgVolume': self._safe_int(quote.get('avgVolume')),
                'marketCap': self._safe_int(quote.get('marketCap')),
                'yearLow': self._safe_float(quote.get('yearLow')),
                'yearHigh': self._safe_float(quote.get('yearHigh')),
                'trailingPE': self._safe_float(quote.get('pe')),
            })

        if ratios_ttm:
            self._merge_non_empty(merged, {
                'currentRatio': self._safe_float(ratios_ttm.get('currentRatioTTM') or ratios_ttm.get('currentRatio')),
                'quickRatio': self._safe_float(ratios_ttm.get('quickRatioTTM') or ratios_ttm.get('quickRatio')),
                'debtToEquity': self._safe_float(ratios_ttm.get('debtEquityRatioTTM') or ratios_ttm.get('debtEquityRatio')),
                'priceToBook': self._safe_float(ratios_ttm.get('priceToBookRatioTTM') or ratios_ttm.get('priceToBookRatio')),
                'profitMargins': self._safe_float(ratios_ttm.get('netProfitMarginTTM') or ratios_ttm.get('netProfitMargin')),
                'operatingMargins': self._safe_float(ratios_ttm.get('operatingProfitMarginTTM') or ratios_ttm.get('operatingProfitMargin')),
                'returnOnEquity': self._safe_float(ratios_ttm.get('returnOnEquityTTM') or ratios_ttm.get('returnOnEquity')),
                'returnOnAssets': self._safe_float(ratios_ttm.get('returnOnAssetsTTM') or ratios_ttm.get('returnOnAssets')),
                'payoutRatio': self._safe_float(ratios_ttm.get('payoutRatioTTM') or ratios_ttm.get('payoutRatio')),
            })

        if metrics_ttm:
            self._merge_non_empty(merged, {
                'forwardPE': self._safe_float(metrics_ttm.get('forwardPE') or metrics_ttm.get('peRatioTTM')),
                'trailingPE': self._safe_float(metrics_ttm.get('peRatioTTM') or metrics_ttm.get('peRatio')),
                'priceToBook': self._safe_float(metrics_ttm.get('pbRatioTTM') or metrics_ttm.get('pbRatio')),
                'bookValue': self._safe_float(metrics_ttm.get('bookValuePerShareTTM') or metrics_ttm.get('bookValuePerShare')),
                'enterpriseValue': self._safe_int(metrics_ttm.get('enterpriseValueTTM') or metrics_ttm.get('enterpriseValue')),
                'freeCashflow': self._safe_float(metrics_ttm.get('freeCashFlowPerShareTTM')),  # per-share fallback if total unavailable
            })

        if growth:
            self._merge_non_empty(merged, {
                'revenueGrowth': self._safe_float(growth.get('revenueGrowth')),
                'earningsGrowth': self._safe_float(growth.get('netIncomeGrowth') or growth.get('epsgrowth')),
            })

        # FMP quote/profile may use alternate 52w field names.
        if merged.get('yearLow') is not None and not result.get('fiftyTwoWeekLow'):
            merged['fiftyTwoWeekLow'] = merged.get('yearLow')
        if merged.get('yearHigh') is not None and not result.get('fiftyTwoWeekHigh'):
            merged['fiftyTwoWeekHigh'] = merged.get('yearHigh')

        if merged:
            self._result_cache[fmp_cache_key] = dict(merged)
            self._merge_non_empty(result, merged)
            logger.info(f"[{symbol}] Enriched company info via FMP")

    def get_top_gainers_today(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Use FMP biggest-gainers endpoint when available."""
        cache_key = f"fmp_biggest_gainers_{limit}"
        if cache_key in self._result_cache:
            cached = self._result_cache[cache_key]
            if isinstance(cached, dict):
                age = time.time() - cached.get("ts", 0)
                if age < 600:
                    return cached.get("data", [])

        payload = self._fmp_get("biggest-gainers")
        rows = payload if isinstance(payload, list) else []
        result = []
        for item in rows[: max(1, limit)]:
            try:
                result.append({
                    'symbol': item.get('symbol'),
                    'name': item.get('name') or item.get('companyName') or item.get('symbol'),
                    'currentPrice': self._safe_float(item.get('price')),
                    'changeAbs': self._safe_float(item.get('change')),
                    'changePct': self._safe_float(item.get('changesPercentage') or item.get('changePercentage')),
                    'volume': self._safe_int(item.get('volume')),
                })
            except Exception:
                continue
        self._result_cache[cache_key] = {"ts": time.time(), "data": result}
        return result

    def _get_ticker(self, symbol: str) -> yf.Ticker:
        """Get a fresh yfinance Ticker (isolated for multi-threading)."""
        return yf.Ticker(symbol)

    def _fresh_ticker(self, symbol: str) -> yf.Ticker:
        """Backward-compatible alias used by company-info fetch path."""
        return self._get_ticker(symbol)

    def _merge_non_empty(self, target: Dict[str, Any], updates: Dict[str, Any]) -> None:
        """Merge values but avoid overwriting non-empty values with missing/zero placeholders."""
        for k, v in (updates or {}).items():
            if v is None:
                continue
            if isinstance(v, str) and not v.strip():
                continue

            # For numeric fields, keep existing non-zero values unless new value is non-zero.
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                current = target.get(k)
                if (v == 0 or (isinstance(v, float) and np.isnan(v))) and current not in (None, 0, 0.0, '', 'N/A'):
                    continue
            target[k] = v

    def _safe_float(self, value):
        try:
            if value is None:
                return None
            if isinstance(value, str):
                value = value.replace('%', '').replace(',', '').strip()
                if not value:
                    return None
            x = float(value)
            if np.isnan(x):
                return None
            return x
        except Exception:
            return None

    def _safe_int(self, value):
        try:
            if value is None:
                return None
            x = int(float(value))
            return x
        except Exception:
            return None

    def _compute_fundamentals_coverage(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Coverage score for core long-term metrics. Distinguishes missing from loaded.
        """
        fields = [
            'marketCap', 'beta', 'trailingPE', 'forwardPE', 'priceToBook',
            'currentRatio', 'debtToEquity', 'profitMargins', 'operatingMargins',
            'returnOnEquity', 'revenueGrowth', 'earningsGrowth', 'freeCashflow',
            'bookValue', 'targetMeanPrice'
        ]
        loaded = 0
        missing = []
        for f in fields:
            v = result.get(f)
            is_loaded = v not in (None, '', 'N/A')
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                # Zero can be valid for some fields, but not for marketCap/ratios in this context.
                if f in {'marketCap', 'trailingPE', 'forwardPE', 'priceToBook', 'beta', 'currentRatio', 'debtToEquity', 'returnOnEquity'} and float(v) == 0:
                    is_loaded = False
            if is_loaded:
                loaded += 1
            else:
                missing.append(f)
        pct = round((loaded / len(fields)) * 100, 1) if fields else 0.0
        label = 'High' if pct >= 80 else 'Medium' if pct >= 50 else 'Low'
        return {'score': pct, 'label': label, 'loaded': loaded, 'total': len(fields), 'missingFields': missing}

    def search_stock(self, query: str) -> List[Dict[str, str]]:
        """Search for stocks by name or ticker symbol."""
        try:
            ticker = yf.Ticker(query.upper())
            # For info/search, we'll rely on the global ThreadPool timeout since yf.Ticker doesn't take timeout= directly
            info = _safe_get(lambda: ticker.info, {}, f"search {query}")

            if info and info.get('symbol'):
                return [{
                    'symbol': info.get('symbol', query.upper()),
                    'name': info.get('longName', info.get('shortName', query)),
                    'exchange': info.get('exchange', ''),
                    'sector': info.get('sector', ''),
                    'industry': info.get('industry', ''),
                    'currency': info.get('currency', 'USD'),
                }]

            # Fallback: return the symbol as-is so user can try the analysis
            return [{
                'symbol': query.upper(),
                'name': query.upper(),
                'exchange': '',
                'sector': '',
                'industry': '',
                'currency': 'USD',
            }]
        except Exception as e:
            logger.warning(f"Search failed for {query}: {e}")
            return [{'symbol': query.upper(), 'name': query.upper(), 'exchange': '', 'sector': '', 'industry': '', 'currency': 'USD'}]

    def get_company_info(self, symbol: str) -> Dict[str, Any]:
        """
        Get company information with aggressive fallbacks.
        Even if Yahoo rate-limits us, we build a partial info dict
        using download() for price data.
        """
        cache_key = f"info_{symbol}"
        if cache_key in self._result_cache:
            return self._result_cache[cache_key]

        # Start with a base result that always has price data
        result = self._build_base_info(symbol)

        # FMP primary fundamentals/profile/quote enrichment (cheap, stable) if API key exists.
        self._enrich_from_fmp(result, symbol)

        # Try to enrich with multiple Yahoo endpoints (info/get_info/fast_info) for resilience.
        try:
            ticker = self._fresh_ticker(symbol)
            info = _safe_get(lambda: ticker.info, {}, f"info {symbol}") or {}
            if not info:
                info = _safe_get(lambda: ticker.get_info(), {}, f"get_info {symbol}") or {}

            if info and info.get('symbol'):
                self._merge_non_empty(result, {
                    'name': info.get('longName', info.get('shortName', result['name'])),
                    'sector': info.get('sector', 'N/A'),
                    'industry': info.get('industry', 'N/A'),
                    'description': info.get('longBusinessSummary', ''),
                    'website': info.get('website', ''),
                    'employees': info.get('fullTimeEmployees', 0),
                    'country': info.get('country', 'N/A'),
                    'exchange': info.get('exchange', 'N/A'),
                    'currency': info.get('currency', 'USD'),
                    'marketCap': info.get('marketCap', 0),
                    'enterpriseValue': info.get('enterpriseValue', 0),
                    'currentPrice': info.get('currentPrice', info.get('regularMarketPrice', result.get('currentPrice', 0))),
                    'previousClose': info.get('previousClose', result.get('previousClose', 0)),
                    'open': info.get('open', 0),
                    'dayLow': info.get('dayLow', 0),
                    'dayHigh': info.get('dayHigh', 0),
                    'fiftyTwoWeekLow': info.get('fiftyTwoWeekLow', 0),
                    'fiftyTwoWeekHigh': info.get('fiftyTwoWeekHigh', 0),
                    'volume': info.get('volume', 0),
                    'avgVolume': info.get('averageVolume', 0),
                    'beta': info.get('beta', 0),
                    'trailingPE': info.get('trailingPE', 0),
                    'forwardPE': info.get('forwardPE', 0),
                    'dividendYield': info.get('dividendYield', 0),
                    'payoutRatio': info.get('payoutRatio', 0),
                    'profitMargins': info.get('profitMargins', 0),
                    'operatingMargins': info.get('operatingMargins', 0),
                    'returnOnEquity': info.get('returnOnEquity', 0),
                    'returnOnAssets': info.get('returnOnAssets', 0),
                    'revenueGrowth': info.get('revenueGrowth', 0),
                    'earningsGrowth': info.get('earningsGrowth', 0),
                    'debtToEquity': info.get('debtToEquity', 0),
                    'currentRatio': info.get('currentRatio', 0),
                    'quickRatio': info.get('quickRatio', 0),
                    'freeCashflow': info.get('freeCashflow', 0),
                    'totalCash': info.get('totalCash', 0),
                    'totalDebt': info.get('totalDebt', 0),
                    'bookValue': info.get('bookValue', 0),
                    'priceToBook': info.get('priceToBook', 0),
                    'targetHighPrice': info.get('targetHighPrice', 0),
                    'targetLowPrice': info.get('targetLowPrice', 0),
                    'targetMeanPrice': info.get('targetMeanPrice', 0),
                    'targetMedianPrice': info.get('targetMedianPrice', 0),
                    'recommendationKey': info.get('recommendationKey', 'none'),
                    'numberOfAnalystOpinions': info.get('numberOfAnalystOpinions', 0),
                })
                logger.info(f"[{symbol}] Got full company info via ticker.info")
            else:
                logger.warning(f"[{symbol}] ticker.info returned empty, using price-based fallback")
        except Exception as e:
            logger.warning(f"[{symbol}] ticker.info failed ({e}), using price-based fallback")

        # Additional Yahoo fallback surfaces (partial but often available when .info is degraded)
        try:
            ticker = self._fresh_ticker(symbol)
            fast = _safe_get(lambda: ticker.fast_info, {}, f"fast_info {symbol}") or {}
            self._merge_non_empty(result, {
                'marketCap': self._safe_int(fast.get('marketCap')),
                'beta': self._safe_float(fast.get('beta')),
                'currentPrice': self._safe_float(fast.get('lastPrice')),
                'previousClose': self._safe_float(fast.get('previousClose')),
                'open': self._safe_float(fast.get('open')),
                'dayLow': self._safe_float(fast.get('dayLow')),
                'dayHigh': self._safe_float(fast.get('dayHigh')),
                'fiftyTwoWeekLow': self._safe_float(fast.get('yearLow')),
                'fiftyTwoWeekHigh': self._safe_float(fast.get('yearHigh')),
                'volume': self._safe_int(fast.get('lastVolume')),
                'avgVolume': self._safe_int(fast.get('tenDayAverageVolume')),
            })
        except Exception as e:
            logger.debug(f"[{symbol}] fast_info enrichment failed: {e}")

        # Quote/summary modules can expose key ratios even when info is sparse.
        try:
            quote_type = _safe_get(lambda: ticker.quote_type, {}, f"quote_type {symbol}") or {}
            summary_detail = _safe_get(lambda: ticker.summary_detail, {}, f"summary_detail {symbol}") or {}
            financial_data = _safe_get(lambda: ticker.financial_data, {}, f"financial_data {symbol}") or {}
            key_stats = _safe_get(lambda: ticker.key_stats, {}, f"key_stats {symbol}") or {}
            self._merge_non_empty(result, {
                'name': quote_type.get('longName') or quote_type.get('shortName'),
                'exchange': quote_type.get('exchange'),
                'currency': quote_type.get('currency'),
                'marketCap': summary_detail.get('marketCap') or key_stats.get('marketCap') or financial_data.get('marketCap'),
                'beta': summary_detail.get('beta') or key_stats.get('beta'),
                'trailingPE': summary_detail.get('trailingPE') or key_stats.get('trailingPE'),
                'forwardPE': summary_detail.get('forwardPE') or key_stats.get('forwardPE'),
                'dividendYield': summary_detail.get('dividendYield') or key_stats.get('dividendYield'),
                'payoutRatio': summary_detail.get('payoutRatio'),
                'priceToBook': key_stats.get('priceToBook'),
                'bookValue': key_stats.get('bookValue'),
                'profitMargins': financial_data.get('profitMargins'),
                'operatingMargins': financial_data.get('operatingMargins'),
                'returnOnEquity': financial_data.get('returnOnEquity'),
                'returnOnAssets': financial_data.get('returnOnAssets'),
                'revenueGrowth': financial_data.get('revenueGrowth'),
                'earningsGrowth': financial_data.get('earningsGrowth'),
                'currentRatio': financial_data.get('currentRatio'),
                'quickRatio': financial_data.get('quickRatio'),
                'debtToEquity': financial_data.get('debtToEquity'),
                'freeCashflow': financial_data.get('freeCashflow'),
                'totalCash': financial_data.get('totalCash'),
                'totalDebt': financial_data.get('totalDebt'),
                'targetHighPrice': financial_data.get('targetHighPrice'),
                'targetLowPrice': financial_data.get('targetLowPrice'),
                'targetMeanPrice': financial_data.get('targetMeanPrice'),
                'targetMedianPrice': financial_data.get('targetMedianPrice'),
                'numberOfAnalystOpinions': financial_data.get('numberOfAnalystOpinions'),
                'recommendationKey': financial_data.get('recommendationKey'),
            })
        except Exception as e:
            logger.debug(f"[{symbol}] module enrichment failed: {e}")

        # Normalize obvious placeholders to None for long-term ratios (preserve valid zeros elsewhere).
        for f in [
            'marketCap', 'beta', 'trailingPE', 'forwardPE', 'priceToBook',
            'currentRatio', 'debtToEquity', 'returnOnEquity'
        ]:
            if result.get(f) in (0, 0.0):
                result[f] = None

        result['fundamentalsCoverage'] = self._compute_fundamentals_coverage(result)

        self._result_cache[cache_key] = result
        return result

    def _build_base_info(self, symbol: str) -> Dict[str, Any]:
        """
        Build baseline company info using yf.download() which is more reliable
        than Ticker.info under rate limiting.
        """
        result = {
            'symbol': symbol,
            'name': symbol,
            'sector': 'N/A',
            'industry': 'N/A',
            'description': '',
            'website': '',
            'employees': 0,
            'country': 'N/A',
            'exchange': 'N/A',
            'currency': 'USD',
            'marketCap': 0,
            'enterpriseValue': 0,
            'currentPrice': 0,
            'previousClose': 0,
            'open': 0,
            'dayLow': 0,
            'dayHigh': 0,
            'fiftyTwoWeekLow': 0,
            'fiftyTwoWeekHigh': 0,
            'volume': 0,
            'avgVolume': 0,
            'beta': 0,
            'trailingPE': 0,
            'forwardPE': 0,
            'dividendYield': 0,
            'payoutRatio': 0,
            'profitMargins': 0,
            'operatingMargins': 0,
            'returnOnEquity': 0,
            'returnOnAssets': 0,
            'revenueGrowth': 0,
            'earningsGrowth': 0,
            'debtToEquity': 0,
            'currentRatio': 0,
            'quickRatio': 0,
            'freeCashflow': 0,
            'totalCash': 0,
            'totalDebt': 0,
            'bookValue': 0,
            'priceToBook': 0,
            'targetHighPrice': 0,
            'targetLowPrice': 0,
            'targetMeanPrice': 0,
            'targetMedianPrice': 0,
            'recommendationKey': 'none',
            'numberOfAnalystOpinions': 0,
        }

        # Use a single yf.download() for 1y to get current price and 52-week range
        try:
            df = yf.download(symbol, period="1y", progress=False, auto_adjust=True, timeout=15)
            if df is not None and not df.empty:
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)

                last = df.iloc[-1]
                prev = df.iloc[-2] if len(df) > 1 else last

                result['currentPrice'] = round(float(last.get('Close', 0)), 2)
                result['previousClose'] = round(float(prev.get('Close', 0)), 2)
                result['open'] = round(float(last.get('Open', 0)), 2)
                result['dayLow'] = round(float(last.get('Low', 0)), 2)
                result['dayHigh'] = round(float(last.get('High', 0)), 2)
                result['volume'] = int(last.get('Volume', 0))
                result['fiftyTwoWeekLow'] = round(float(df['Low'].min()), 2)
                result['fiftyTwoWeekHigh'] = round(float(df['High'].max()), 2)

                logger.info(f"[{symbol}] Got core metrics from yf.download (1y)")
        except Exception as e:
            logger.warning(f"[{symbol}] yf.download core metrics failed: {e}")

        # Emergency Fallback: Ticker.fast_info (more resilient than download/info)
        if not result.get('currentPrice'):
            try:
                ticker = self._get_ticker(symbol)
                fast = ticker.fast_info
                result['currentPrice'] = round(float(fast.get('lastPrice', 0)), 2)
                result['previousClose'] = round(float(fast.get('previousClose', 0)), 2)
                result['marketCap'] = int(fast.get('marketCap', 0))
                result['open'] = round(float(fast.get('open', 0)), 2)
                result['dayLow'] = round(float(fast.get('dayLow', 0)), 2)
                result['dayHigh'] = round(float(fast.get('dayHigh', 0)), 2)
                if not result.get('fiftyTwoWeekLow'):
                    result['fiftyTwoWeekLow'] = round(float(fast.get('yearLow', 0)), 2)
                    result['fiftyTwoWeekHigh'] = round(float(fast.get('yearHigh', 0)), 2)
                logger.info(f"[{symbol}] Used Ticker.fast_info fallback for price.")
            except Exception as fe:
                logger.warning(f"[{symbol}] FastInfo fallback failed: {fe}")

        # Final Fallback: ticker.history(period='1d')
        if not result.get('currentPrice'):
            try:
                hist = ticker.history(period='1d')
                if not hist.empty:
                    result['currentPrice'] = round(float(hist.iloc[-1]['Close']), 2)
                    logger.info(f"[{symbol}] Used ticker.history fallback for price.")
            except Exception as he:
                logger.warning(f"[{symbol}] History fallback failed: {he}")

        return result

    def get_historical_prices(self, symbol: str, period: str = "5y") -> Dict[str, Any]:
        """Get historical price data using yf.download()."""
        cache_key = f"hist_{symbol}_{period}"
        if cache_key in self._result_cache:
            return self._result_cache[cache_key]

        try:
            interval = '1d'
            if period == '1d':
                interval = '5m'
            elif period in ['3d', '5d']:
                interval = '15m'

            df = yf.download(symbol, period=period, interval=interval, progress=False, auto_adjust=True, timeout=15)

            if df is None or df.empty:
                logger.warning(f"[{symbol}] No data for {period}, trying 2y")
                df = yf.download(symbol, period="2y", progress=False, auto_adjust=True, timeout=15)

            if df is None or df.empty:
                return {'error': 'No historical data', 'symbol': symbol, 'data': []}

            # Handle MultiIndex columns
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            data = []
            for date, row in df.iterrows():
                # Use format %Y-%m-%d %H:%M for intraday so Chart.js shows time
                date_str = date.strftime('%Y-%m-%d %H:%M') if interval in ['5m', '15m'] else date.strftime('%Y-%m-%d')
                data.append({
                    'date': date_str,
                    'open': round(float(row.get('Open', 0)), 2),
                    'high': round(float(row.get('High', 0)), 2),
                    'low': round(float(row.get('Low', 0)), 2),
                    'close': round(float(row.get('Close', 0)), 2),
                    'volume': int(row.get('Volume', 0)),
                })

            result = {
                'symbol': symbol,
                'period': period,
                'dataPoints': len(data),
                'data': data,
            }
            self._result_cache[cache_key] = result
            return result
        except Exception as e:
            logger.error(f"Error fetching historical prices for {symbol}: {e}")
            return {'error': str(e), 'symbol': symbol, 'data': []}

    def get_quarterly_financials(self, symbol: str) -> Dict[str, Any]:
        """Get quarterly financial statements."""
        cache_key = f"fin_{symbol}"
        if cache_key in self._result_cache:
            return self._result_cache[cache_key]

        result = {
            'symbol': symbol,
            'incomeStatement': {'quarterly': [], 'annual': []},
            'balanceSheet': {'quarterly': [], 'annual': []},
            'cashFlow': {'quarterly': [], 'annual': []},
        }

        ticker = self._get_ticker(symbol)

        def _process_statement(df):
            rows = []
            if df is None or df.empty:
                return rows
            for col in df.columns:
                period_data = {'period': col.strftime('%Y-%m-%d') if hasattr(col, 'strftime') else str(col)}
                for idx in df.index:
                    val = df.loc[idx, col]
                    period_data[str(idx)] = None if pd.isna(val) else float(val)
                rows.append(period_data)
            return rows

        result['incomeStatement']['quarterly'] = _safe_get(
            lambda: _process_statement(ticker.quarterly_income_stmt), [], "quarterly income")
        
        result['incomeStatement']['annual'] = _safe_get(
            lambda: _process_statement(ticker.income_stmt), [], "annual income")
        
        result['balanceSheet']['quarterly'] = _safe_get(
            lambda: _process_statement(ticker.quarterly_balance_sheet), [], "quarterly balance")
        
        result['balanceSheet']['annual'] = _safe_get(
            lambda: _process_statement(ticker.balance_sheet), [], "annual balance")
        
        result['cashFlow']['quarterly'] = _safe_get(
            lambda: _process_statement(ticker.quarterly_cashflow), [], "quarterly cashflow")
        
        result['cashFlow']['annual'] = _safe_get(
            lambda: _process_statement(ticker.cashflow), [], "annual cashflow")

        self._result_cache[cache_key] = result
        return result

    def get_sec_filings(self, symbol: str) -> List[Dict[str, Any]]:
        """Get SEC filing links."""
        filings = []
        ticker = self._get_ticker(symbol)
        try:
            sec_filings = _safe_get(lambda: ticker.sec_filings, None, f"sec filings {symbol}")
            if sec_filings:
                for filing in sec_filings:
                    if isinstance(filing, dict):
                        ft = filing.get('type', '')
                        if ft in ['10-K', '10-Q', '10-K/A', '10-Q/A']:
                            filings.append({
                                'type': ft,
                                'date': str(filing.get('date', '')),
                                'title': filing.get('title', ''),
                                'edgarUrl': filing.get('edgarUrl', ''),
                            })
        except Exception as e:
            logger.debug(f"SEC filings not available: {e}")

        if not filings:
            filings = [
                {
                    'type': 'SEC EDGAR',
                    'date': '',
                    'title': f'Search all {symbol} filings on SEC EDGAR',
                    'edgarUrl': f'https://efts.sec.gov/LATEST/search-index?q=%22{symbol}%22&forms=10-K,10-Q',
                },
                {
                    'type': '10-K / 10-Q',
                    'date': '',
                    'title': f'View {symbol} annual & quarterly reports',
                    'edgarUrl': f'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&company={symbol}&type=10-&dateb=&owner=include&count=40&search_text=&action=getcompany',
                },
            ]
        return filings

    def get_institutional_holders(self, symbol: str) -> List[Dict[str, Any]]:
        """Get institutional holder data."""
        ticker = self._get_ticker(symbol)
        holders = []
        inst = _safe_get(lambda: ticker.institutional_holders, None, f"inst holders {symbol}")
        if inst is not None and not inst.empty:
            for _, row in inst.iterrows():
                holders.append({
                    'holder': str(row.get('Holder', '')),
                    'shares': int(row.get('Shares', 0)) if not pd.isna(row.get('Shares', 0)) else 0,
                    'dateReported': str(row.get('Date Reported', '')),
                    'pctOut': float(row.get('% Out', 0)) if not pd.isna(row.get('% Out', 0)) else 0,
                    'value': float(row.get('Value', 0)) if not pd.isna(row.get('Value', 0)) else 0,
                })
        return holders

    def get_insider_transactions(self, symbol: str) -> List[Dict[str, Any]]:
        """Get insider transaction data."""
        ticker = self._get_ticker(symbol)
        transactions = []
        insider_tx = _safe_get(lambda: ticker.insider_transactions, None, f"insider tx {symbol}")
        if insider_tx is not None and not insider_tx.empty:
            for _, row in insider_tx.head(20).iterrows():
                transactions.append({
                    'insider': str(row.get('Insider', row.get('insider', ''))),
                    'relation': str(row.get('Relation', row.get('relation', ''))),
                    'transaction': str(row.get('Transaction', row.get('transaction', ''))),
                    'date': str(row.get('Start Date', row.get('startDate', ''))),
                    'shares': str(row.get('Shares', row.get('shares', ''))),
                    'value': str(row.get('Value', row.get('value', ''))),
                })
        return transactions

    def get_analyst_recommendations(self, symbol: str) -> List[Dict[str, Any]]:
        """Get analyst recommendations."""
        ticker = self._get_ticker(symbol)
        recommendations = []
        recs = _safe_get(lambda: ticker.recommendations, None, f"recs {symbol}")
        if recs is not None and not recs.empty:
            recent = recs.tail(20)
            for date, row in recent.iterrows():
                recommendations.append({
                    'date': date.strftime('%Y-%m-%d') if hasattr(date, 'strftime') else str(date),
                    'firm': str(row.get('Firm', '')),
                    'toGrade': str(row.get('To Grade', '')),
                    'fromGrade': str(row.get('From Grade', '')),
                    'action': str(row.get('Action', '')),
                })
        return recommendations

    def get_earnings_data(self, symbol: str) -> Dict[str, Any]:
        """Get earnings history and estimates."""
        ticker = self._get_ticker(symbol)
        result = {'history': [], 'dates': []}
        hist = _safe_get(lambda: ticker.earnings_history, None, f"earnings hist {symbol}")
        if hist is not None and not hist.empty:
            for _, row in hist.iterrows():
                result['history'].append({
                    'quarter': str(row.get('quarter', row.get('Quarter', ''))),
                    'epsEstimate': float(row.get('epsEstimate', row.get('EPS Estimate', 0)))
                                  if not pd.isna(row.get('epsEstimate', row.get('EPS Estimate', 0))) else 0,
                    'epsActual': float(row.get('epsActual', row.get('Reported EPS', 0)))
                                if not pd.isna(row.get('epsActual', row.get('Reported EPS', 0))) else 0,
                    'surprise': float(row.get('epsSurprise', row.get('Surprise(%)', 0)))
                               if not pd.isna(row.get('epsSurprise', row.get('Surprise(%)', 0))) else 0,
                })
        dates = _safe_get(lambda: ticker.earnings_dates, None, f"earnings dates {symbol}")
        if dates is not None and not dates.empty:
            for date, row in dates.head(8).iterrows():
                result['dates'].append({
                    'date': date.strftime('%Y-%m-%d') if hasattr(date, 'strftime') else str(date),
                    'epsEstimate': float(row.get('EPS Estimate', 0)) if not pd.isna(row.get('EPS Estimate', 0)) else None,
                    'epsActual': float(row.get('Reported EPS', 0)) if not pd.isna(row.get('Reported EPS', 0)) else None,
                    'surprisePercent': float(row.get('Surprise(%)', 0)) if not pd.isna(row.get('Surprise(%)', 0)) else None,
                })
        return result
