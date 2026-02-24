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
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import logging
import time
import random

logger = logging.getLogger(__name__)


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
        self._ticker_cache = {}
        self._result_cache = {}

    def _get_ticker(self, symbol: str) -> yf.Ticker:
        """Get a yfinance Ticker."""
        if symbol not in self._ticker_cache:
            ticker = yf.Ticker(symbol)
            self._ticker_cache[symbol] = ticker
        return self._ticker_cache[symbol]

    def _fresh_ticker(self, symbol: str) -> yf.Ticker:
        """Force-create a fresh Ticker."""
        ticker = yf.Ticker(symbol)
        self._ticker_cache[symbol] = ticker
        return ticker

    def search_stock(self, query: str) -> List[Dict[str, str]]:
        """Search for stocks by name or ticker symbol."""
        try:
            ticker = self._get_ticker(query.upper())
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

        # Try to enrich with Ticker.info (may fail under rate limits)
        try:
            ticker = self._fresh_ticker(symbol)
            info = _safe_get(lambda: ticker.info, {}, f"info {symbol}")

            if info and info.get('symbol'):
                result.update({
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

        # Use yf.download()
        try:
            df = yf.download(symbol, period="5d", progress=False, auto_adjust=True)
            if df is not None and not df.empty:
                # Handle MultiIndex columns
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

                logger.info(f"[{symbol}] Got price from yf.download: ${result['currentPrice']}")
        except Exception as e:
            logger.warning(f"[{symbol}] yf.download for base price failed: {e}")

        # Try to get 52-week range from 1y data
        try:
            df_1y = yf.download(symbol, period="1y", progress=False, auto_adjust=True)
            if df_1y is not None and not df_1y.empty:
                if isinstance(df_1y.columns, pd.MultiIndex):
                    df_1y.columns = df_1y.columns.get_level_values(0)
                result['fiftyTwoWeekLow'] = round(float(df_1y['Low'].min()), 2)
                result['fiftyTwoWeekHigh'] = round(float(df_1y['High'].max()), 2)
        except Exception as e:
            logger.debug(f"[{symbol}] 52-week range fetch failed: {e}")

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

            df = yf.download(symbol, period=period, interval=interval, progress=False, auto_adjust=True)

            if df is None or df.empty:
                logger.warning(f"[{symbol}] No data for {period}, trying 2y")
                df = yf.download(symbol, period="2y", progress=False, auto_adjust=True)

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
