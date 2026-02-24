"""
Analysis Engine
Core analysis module that combines all data sources to provide:
- Buy/Hold/Sell recommendations
- Price targets (30 days, 6 months, 1 year)
- Comprehensive fundamental and technical analysis
- Risk assessment
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.preprocessing import StandardScaler
import logging

logger = logging.getLogger(__name__)


class AnalysisEngine:
    """
    Comprehensive stock analysis engine.
    Combines fundamental analysis (10-K/10-Q), technical analysis,
    sentiment analysis, and institutional flow to generate recommendations.
    """

    # Scoring weights for the overall recommendation
    WEIGHTS = {
        'fundamental': 0.30,
        'technical': 0.25,
        'sentiment': 0.15,
        'valuation': 0.15,
        'growth': 0.10,
        'institutional': 0.05,
    }

    def run_full_analysis(
        self,
        company_info: Dict[str, Any],
        financials: Dict[str, Any],
        historical_prices: Dict[str, Any],
        news_sentiment: Dict[str, Any],
        institutional_holders: List[Dict],
        insider_transactions: List[Dict],
        analyst_recs: List[Dict],
        earnings_data: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Run full comprehensive analysis and generate recommendations.
        """
        symbol = company_info.get('symbol', 'UNKNOWN')
        current_price = company_info.get('currentPrice', 0) or company_info.get('previousClose', 0)

        if not current_price:
            return {'error': 'Unable to determine current price for analysis'}

        # Run individual analyses
        fundamental_score = self._analyze_fundamentals(company_info, financials)
        technical_analysis = self._analyze_technicals(historical_prices, current_price)
        sentiment_score = self._analyze_sentiment(news_sentiment)
        valuation_score = self._analyze_valuation(company_info)
        growth_score = self._analyze_growth(company_info, financials)
        institutional_score = self._analyze_institutional(institutional_holders, insider_transactions)

        # Calculate price targets
        price_targets = self._calculate_price_targets(
            company_info, historical_prices, financials,
            technical_analysis, sentiment_score, growth_score
        )

        # Generate overall score (0-100)
        overall_score = (
            fundamental_score['score'] * self.WEIGHTS['fundamental'] +
            technical_analysis['score'] * self.WEIGHTS['technical'] +
            sentiment_score['score'] * self.WEIGHTS['sentiment'] +
            valuation_score['score'] * self.WEIGHTS['valuation'] +
            growth_score['score'] * self.WEIGHTS['growth'] +
            institutional_score['score'] * self.WEIGHTS['institutional']
        )

        # Determine recommendation
        recommendation = self._get_recommendation(overall_score, price_targets, current_price)

        # Compile risk factors
        risk_factors = self._assess_risks(company_info, financials, technical_analysis)

        # Build key metrics summary
        key_metrics = self._build_key_metrics(company_info, financials)

        return {
            'symbol': symbol,
            'companyName': company_info.get('name', symbol),
            'currentPrice': current_price,
            'analysisDate': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'overallScore': round(overall_score, 1),
            'recommendation': recommendation,
            'priceTargets': price_targets,
            'scores': {
                'fundamental': fundamental_score,
                'technical': technical_analysis,
                'sentiment': sentiment_score,
                'valuation': valuation_score,
                'growth': growth_score,
                'institutional': institutional_score,
            },
            'riskFactors': risk_factors,
            'keyMetrics': key_metrics,
            'analystConsensus': self._parse_analyst_consensus(analyst_recs, company_info),
            'earningsSummary': self._parse_earnings(earnings_data),
        }

    def _analyze_fundamentals(self, info: Dict, financials: Dict) -> Dict[str, Any]:
        """Analyze fundamental financial health from 10-K/10-Q data."""
        score = 50  # Start neutral
        factors = []

        # Profitability
        profit_margin = info.get('profitMargins', 0) or 0
        if profit_margin > 0.20:
            score += 12
            factors.append({'factor': 'Profit Margin', 'value': f'{profit_margin*100:.1f}%', 'impact': 'positive', 'detail': 'Strong profitability above 20%'})
        elif profit_margin > 0.10:
            score += 6
            factors.append({'factor': 'Profit Margin', 'value': f'{profit_margin*100:.1f}%', 'impact': 'positive', 'detail': 'Healthy profitability above 10%'})
        elif profit_margin > 0:
            factors.append({'factor': 'Profit Margin', 'value': f'{profit_margin*100:.1f}%', 'impact': 'neutral', 'detail': 'Modest profitability'})
        else:
            score -= 10
            factors.append({'factor': 'Profit Margin', 'value': f'{profit_margin*100:.1f}%', 'impact': 'negative', 'detail': 'Company is not profitable'})

        # Return on Equity
        roe = info.get('returnOnEquity', 0) or 0
        if roe > 0.20:
            score += 10
            factors.append({'factor': 'Return on Equity', 'value': f'{roe*100:.1f}%', 'impact': 'positive', 'detail': 'Excellent capital efficiency'})
        elif roe > 0.10:
            score += 5
            factors.append({'factor': 'Return on Equity', 'value': f'{roe*100:.1f}%', 'impact': 'positive', 'detail': 'Good capital efficiency'})
        elif roe > 0:
            factors.append({'factor': 'Return on Equity', 'value': f'{roe*100:.1f}%', 'impact': 'neutral', 'detail': 'Below average ROE'})
        else:
            score -= 8
            factors.append({'factor': 'Return on Equity', 'value': f'{roe*100:.1f}%', 'impact': 'negative', 'detail': 'Negative ROE indicates losses'})

        # Debt to Equity
        de_ratio = info.get('debtToEquity', 0) or 0
        if de_ratio < 50:
            score += 8
            factors.append({'factor': 'Debt/Equity', 'value': f'{de_ratio:.1f}', 'impact': 'positive', 'detail': 'Conservative debt levels'})
        elif de_ratio < 100:
            score += 3
            factors.append({'factor': 'Debt/Equity', 'value': f'{de_ratio:.1f}', 'impact': 'neutral', 'detail': 'Moderate debt levels'})
        else:
            score -= 8
            factors.append({'factor': 'Debt/Equity', 'value': f'{de_ratio:.1f}', 'impact': 'negative', 'detail': 'High debt levels, increased risk'})

        # Current Ratio (Liquidity)
        current_ratio = info.get('currentRatio', 0) or 0
        if current_ratio > 2.0:
            score += 6
            factors.append({'factor': 'Current Ratio', 'value': f'{current_ratio:.2f}', 'impact': 'positive', 'detail': 'Strong liquidity position'})
        elif current_ratio > 1.0:
            score += 3
            factors.append({'factor': 'Current Ratio', 'value': f'{current_ratio:.2f}', 'impact': 'neutral', 'detail': 'Adequate liquidity'})
        else:
            score -= 8
            factors.append({'factor': 'Current Ratio', 'value': f'{current_ratio:.2f}', 'impact': 'negative', 'detail': 'Potential liquidity concerns'})

        # Free Cash Flow
        fcf = info.get('freeCashflow', 0) or 0
        if fcf > 0:
            score += 8
            factors.append({'factor': 'Free Cash Flow', 'value': f'${fcf/1e9:.2f}B' if abs(fcf) >= 1e9 else f'${fcf/1e6:.1f}M', 'impact': 'positive', 'detail': 'Positive cash generation'})
        else:
            score -= 6
            factors.append({'factor': 'Free Cash Flow', 'value': f'${fcf/1e9:.2f}B' if abs(fcf) >= 1e9 else f'${fcf/1e6:.1f}M', 'impact': 'negative', 'detail': 'Negative free cash flow'})

        # Operating Margin
        op_margin = info.get('operatingMargins', 0) or 0
        if op_margin > 0.25:
            score += 6
            factors.append({'factor': 'Operating Margin', 'value': f'{op_margin*100:.1f}%', 'impact': 'positive', 'detail': 'Excellent operational efficiency'})
        elif op_margin > 0.10:
            score += 3
            factors.append({'factor': 'Operating Margin', 'value': f'{op_margin*100:.1f}%', 'impact': 'neutral', 'detail': 'Decent operational efficiency'})

        score = max(0, min(100, score))

        return {
            'score': score,
            'label': self._score_label(score),
            'factors': factors,
        }

    def _analyze_technicals(self, historical: Dict, current_price: float) -> Dict[str, Any]:
        """Analyze technical indicators from price data."""
        score = 50
        factors = []
        indicators = {}

        data = historical.get('data', [])
        if not data or len(data) < 50:
            return {'score': 50, 'label': 'Insufficient Data', 'factors': [], 'indicators': {}}

        closes = [d['close'] for d in data]
        volumes = [d['volume'] for d in data]

        # Moving Averages
        if len(closes) >= 200:
            sma_50 = np.mean(closes[-50:])
            sma_200 = np.mean(closes[-200:])
            sma_20 = np.mean(closes[-20:])
            ema_12 = self._ema(closes, 12)
            ema_26 = self._ema(closes, 26)

            indicators['sma20'] = round(sma_20, 2)
            indicators['sma50'] = round(sma_50, 2)
            indicators['sma200'] = round(sma_200, 2)

            # Golden Cross / Death Cross
            if sma_50 > sma_200:
                score += 10
                factors.append({'factor': 'Moving Average Crossover', 'value': 'Golden Cross', 'impact': 'positive', 'detail': '50-day SMA above 200-day SMA'})
            else:
                score -= 8
                factors.append({'factor': 'Moving Average Crossover', 'value': 'Death Cross', 'impact': 'negative', 'detail': '50-day SMA below 200-day SMA'})

            # Price relative to MAs
            if current_price > sma_50 and current_price > sma_200:
                score += 8
                factors.append({'factor': 'Price vs MAs', 'value': 'Above Both', 'impact': 'positive', 'detail': 'Price above both 50 & 200-day MAs'})
            elif current_price < sma_50 and current_price < sma_200:
                score -= 8
                factors.append({'factor': 'Price vs MAs', 'value': 'Below Both', 'impact': 'negative', 'detail': 'Price below both 50 & 200-day MAs'})

            # MACD
            macd_line = ema_12 - ema_26
            indicators['macd'] = round(macd_line, 2)
            if macd_line > 0:
                score += 5
                factors.append({'factor': 'MACD', 'value': f'{macd_line:.2f}', 'impact': 'positive', 'detail': 'Bullish MACD signal'})
            else:
                score -= 5
                factors.append({'factor': 'MACD', 'value': f'{macd_line:.2f}', 'impact': 'negative', 'detail': 'Bearish MACD signal'})

        # RSI
        if len(closes) >= 14:
            rsi = self._calculate_rsi(closes)
            indicators['rsi'] = round(rsi, 2)
            if rsi > 70:
                score -= 8
                factors.append({'factor': 'RSI', 'value': f'{rsi:.1f}', 'impact': 'negative', 'detail': 'Overbought territory (>70)'})
            elif rsi < 30:
                score += 8
                factors.append({'factor': 'RSI', 'value': f'{rsi:.1f}', 'impact': 'positive', 'detail': 'Oversold territory (<30) – potential bounce'})
            else:
                factors.append({'factor': 'RSI', 'value': f'{rsi:.1f}', 'impact': 'neutral', 'detail': 'Neutral RSI range'})

        # Volume trend
        if len(volumes) >= 50:
            avg_vol_recent = np.mean(volumes[-10:])
            avg_vol_50 = np.mean(volumes[-50:])
            vol_ratio = avg_vol_recent / avg_vol_50 if avg_vol_50 > 0 else 1
            indicators['volumeRatio'] = round(vol_ratio, 2)
            if vol_ratio > 1.5:
                factors.append({'factor': 'Volume', 'value': f'{vol_ratio:.2f}x', 'impact': 'neutral', 'detail': 'Significantly elevated volume vs 50-day avg'})
            elif vol_ratio < 0.5:
                factors.append({'factor': 'Volume', 'value': f'{vol_ratio:.2f}x', 'impact': 'neutral', 'detail': 'Below average trading volume'})

        # Volatility (annualized)
        if len(closes) >= 31:
            prices = np.array(closes[-31:])
            returns = prices[1:] / prices[:-1] - 1
            volatility = np.std(returns) * np.sqrt(252)
            indicators['volatility'] = round(volatility, 4)
            if volatility > 0.50:
                score -= 5
                factors.append({'factor': 'Volatility', 'value': f'{volatility*100:.1f}%', 'impact': 'negative', 'detail': 'High volatility increases risk'})
            elif volatility < 0.20:
                score += 3
                factors.append({'factor': 'Volatility', 'value': f'{volatility*100:.1f}%', 'impact': 'positive', 'detail': 'Low volatility, more predictable'})

        # 52-week position
        high_52 = max(closes[-min(252, len(closes)):])
        low_52 = min(closes[-min(252, len(closes)):])
        if high_52 > low_52:
            position_52 = (current_price - low_52) / (high_52 - low_52)
            indicators['week52Position'] = round(position_52, 4)
            if position_52 > 0.8:
                factors.append({'factor': '52-Week Position', 'value': f'{position_52*100:.0f}%', 'impact': 'neutral', 'detail': 'Near 52-week high'})
            elif position_52 < 0.2:
                factors.append({'factor': '52-Week Position', 'value': f'{position_52*100:.0f}%', 'impact': 'neutral', 'detail': 'Near 52-week low'})

        # Support and resistance levels
        indicators['support'] = round(low_52 + (high_52 - low_52) * 0.236, 2)
        indicators['resistance'] = round(high_52 - (high_52 - low_52) * 0.236, 2)

        score = max(0, min(100, score))

        return {
            'score': score,
            'label': self._score_label(score),
            'factors': factors,
            'indicators': indicators,
        }

    def _analyze_sentiment(self, news_sentiment: Dict) -> Dict[str, Any]:
        """Convert news sentiment into a score."""
        sentiment_data = news_sentiment.get('sentiment', {})
        overall = sentiment_data.get('overallSentiment', 0)
        pos_count = sentiment_data.get('positiveCount', 0)
        neg_count = sentiment_data.get('negativeCount', 0)
        total = sentiment_data.get('totalArticles', 0)

        # Map polarity (-1 to 1) to score (0 to 100)
        score = 50 + (overall * 50)
        score = max(0, min(100, score))

        factors = []
        if total > 0:
            pos_pct = pos_count / total * 100
            neg_pct = neg_count / total * 100
            factors.append({
                'factor': 'News Sentiment',
                'value': f'{overall:.3f}',
                'impact': 'positive' if overall > 0.1 else 'negative' if overall < -0.1 else 'neutral',
                'detail': f'{pos_count} positive, {neg_count} negative out of {total} articles',
            })
            factors.append({
                'factor': 'Sentiment Distribution',
                'value': f'{pos_pct:.0f}% pos / {neg_pct:.0f}% neg',
                'impact': 'positive' if pos_pct > 60 else 'negative' if neg_pct > 60 else 'neutral',
                'detail': 'Distribution of news article sentiments',
            })
        else:
            factors.append({
                'factor': 'News Coverage',
                'value': 'Limited',
                'impact': 'neutral',
                'detail': 'Insufficient news data for sentiment analysis',
            })

        return {
            'score': round(score, 1),
            'label': self._score_label(score),
            'factors': factors,
        }

    def _analyze_valuation(self, info: Dict) -> Dict[str, Any]:
        """Analyze stock valuation metrics."""
        score = 50
        factors = []

        # P/E Ratio
        pe = info.get('trailingPE', 0) or 0
        forward_pe = info.get('forwardPE', 0) or 0
        if pe > 0:
            if pe < 15:
                score += 12
                factors.append({'factor': 'Trailing P/E', 'value': f'{pe:.1f}', 'impact': 'positive', 'detail': 'Attractively valued relative to earnings'})
            elif pe < 25:
                score += 5
                factors.append({'factor': 'Trailing P/E', 'value': f'{pe:.1f}', 'impact': 'neutral', 'detail': 'Fair valuation'})
            elif pe < 40:
                score -= 5
                factors.append({'factor': 'Trailing P/E', 'value': f'{pe:.1f}', 'impact': 'negative', 'detail': 'Elevated valuation, priced for growth'})
            else:
                score -= 10
                factors.append({'factor': 'Trailing P/E', 'value': f'{pe:.1f}', 'impact': 'negative', 'detail': 'Expensive valuation, high expectations'})

        # Forward P/E vs Trailing P/E
        if forward_pe > 0 and pe > 0:
            if forward_pe < pe:
                score += 5
                factors.append({'factor': 'Forward P/E', 'value': f'{forward_pe:.1f}', 'impact': 'positive', 'detail': 'Expected earnings growth (forward PE < trailing PE)'})
            else:
                score -= 3
                factors.append({'factor': 'Forward P/E', 'value': f'{forward_pe:.1f}', 'impact': 'negative', 'detail': 'Earnings expected to decline'})

        # Price to Book
        pb = info.get('priceToBook', 0) or 0
        if pb > 0:
            if pb < 3:
                score += 6
                factors.append({'factor': 'Price/Book', 'value': f'{pb:.2f}', 'impact': 'positive', 'detail': 'Reasonable price relative to book value'})
            elif pb > 10:
                score -= 5
                factors.append({'factor': 'Price/Book', 'value': f'{pb:.2f}', 'impact': 'negative', 'detail': 'High premium over book value'})

        # Dividend Yield
        div_yield = info.get('dividendYield', 0) or 0
        if div_yield > 0.03:
            score += 5
            factors.append({'factor': 'Dividend Yield', 'value': f'{div_yield*100:.2f}%', 'impact': 'positive', 'detail': 'Attractive dividend yield above 3%'})
        elif div_yield > 0:
            factors.append({'factor': 'Dividend Yield', 'value': f'{div_yield*100:.2f}%', 'impact': 'neutral', 'detail': 'Pays a dividend'})

        # Analyst Target vs Current Price
        target_mean = info.get('targetMeanPrice', 0) or 0
        current = info.get('currentPrice', 0) or 0
        if target_mean > 0 and current > 0:
            upside = (target_mean - current) / current * 100
            if upside > 20:
                score += 10
                factors.append({'factor': 'Analyst Upside', 'value': f'{upside:.1f}%', 'impact': 'positive', 'detail': f'Analysts see significant upside to ${target_mean:.2f}'})
            elif upside > 5:
                score += 5
                factors.append({'factor': 'Analyst Upside', 'value': f'{upside:.1f}%', 'impact': 'positive', 'detail': f'Analysts see moderate upside to ${target_mean:.2f}'})
            elif upside < -10:
                score -= 8
                factors.append({'factor': 'Analyst Downside', 'value': f'{upside:.1f}%', 'impact': 'negative', 'detail': f'Analysts see downside risk to ${target_mean:.2f}'})

        score = max(0, min(100, score))
        return {
            'score': score,
            'label': self._score_label(score),
            'factors': factors,
        }

    def _analyze_growth(self, info: Dict, financials: Dict) -> Dict[str, Any]:
        """Analyze growth trajectory from financial statements."""
        score = 50
        factors = []

        # Revenue Growth
        rev_growth = info.get('revenueGrowth', 0) or 0
        if rev_growth > 0.20:
            score += 15
            factors.append({'factor': 'Revenue Growth', 'value': f'{rev_growth*100:.1f}%', 'impact': 'positive', 'detail': 'Strong revenue growth above 20%'})
        elif rev_growth > 0.05:
            score += 8
            factors.append({'factor': 'Revenue Growth', 'value': f'{rev_growth*100:.1f}%', 'impact': 'positive', 'detail': 'Moderate revenue growth'})
        elif rev_growth > 0:
            score += 3
            factors.append({'factor': 'Revenue Growth', 'value': f'{rev_growth*100:.1f}%', 'impact': 'neutral', 'detail': 'Slow revenue growth'})
        else:
            score -= 10
            factors.append({'factor': 'Revenue Growth', 'value': f'{rev_growth*100:.1f}%', 'impact': 'negative', 'detail': 'Revenue declining'})

        # Earnings Growth
        earn_growth = info.get('earningsGrowth', 0) or 0
        if earn_growth > 0.20:
            score += 12
            factors.append({'factor': 'Earnings Growth', 'value': f'{earn_growth*100:.1f}%', 'impact': 'positive', 'detail': 'Strong earnings growth'})
        elif earn_growth > 0:
            score += 5
            factors.append({'factor': 'Earnings Growth', 'value': f'{earn_growth*100:.1f}%', 'impact': 'positive', 'detail': 'Positive earnings growth'})
        elif earn_growth < -0.10:
            score -= 10
            factors.append({'factor': 'Earnings Growth', 'value': f'{earn_growth*100:.1f}%', 'impact': 'negative', 'detail': 'Significant earnings decline'})

        # Analyze quarterly revenue trend from financials
        quarterly_income = financials.get('incomeStatement', {}).get('quarterly', [])
        if len(quarterly_income) >= 4:
            revenues = []
            for q in quarterly_income[:8]:
                rev = q.get('Total Revenue', q.get('TotalRevenue', None))
                if rev is not None:
                    revenues.append(rev)

            if len(revenues) >= 4:
                # Check QoQ trend
                if revenues[0] > revenues[1] > revenues[2]:
                    score += 5
                    factors.append({'factor': 'Revenue Trend', 'value': 'Accelerating', 'impact': 'positive', 'detail': 'Revenue showing consecutive quarterly increases'})
                elif revenues[0] < revenues[1] < revenues[2]:
                    score -= 5
                    factors.append({'factor': 'Revenue Trend', 'value': 'Decelerating', 'impact': 'negative', 'detail': 'Revenue showing consecutive quarterly declines'})

        score = max(0, min(100, score))
        return {
            'score': score,
            'label': self._score_label(score),
            'factors': factors,
        }

    def _analyze_institutional(self, holders: List[Dict], insider_tx: List[Dict]) -> Dict[str, Any]:
        """Analyze institutional ownership and insider activity."""
        score = 50
        factors = []

        # Institutional holders
        if holders:
            total_inst_value = sum(h.get('value', 0) for h in holders)
            factors.append({
                'factor': 'Institutional Ownership',
                'value': f'{len(holders)} major holders',
                'impact': 'positive' if len(holders) >= 5 else 'neutral',
                'detail': f'Major institutional investors hold significant positions',
            })
            if len(holders) >= 10:
                score += 8
            elif len(holders) >= 5:
                score += 4

        # Insider transactions analysis
        if insider_tx:
            buys = sum(1 for tx in insider_tx if 'purchase' in str(tx.get('transaction', '')).lower() or 'buy' in str(tx.get('transaction', '')).lower())
            sells = sum(1 for tx in insider_tx if 'sale' in str(tx.get('transaction', '')).lower() or 'sell' in str(tx.get('transaction', '')).lower())

            if buys > sells:
                score += 10
                factors.append({'factor': 'Insider Activity', 'value': f'{buys} buys vs {sells} sells', 'impact': 'positive', 'detail': 'Net insider buying is a bullish signal'})
            elif sells > buys * 2:
                score -= 8
                factors.append({'factor': 'Insider Activity', 'value': f'{buys} buys vs {sells} sells', 'impact': 'negative', 'detail': 'Heavy insider selling may indicate concerns'})
            else:
                factors.append({'factor': 'Insider Activity', 'value': f'{buys} buys vs {sells} sells', 'impact': 'neutral', 'detail': 'Mixed insider activity'})

        score = max(0, min(100, score))
        return {
            'score': score,
            'label': self._score_label(score),
            'factors': factors,
        }

    def _calculate_price_targets(
        self, info: Dict, historical: Dict, financials: Dict,
        technical: Dict, sentiment: Dict, growth: Dict
    ) -> Dict[str, Any]:
        """
        Calculate dynamic price targets for 30 days, 6 months, and 1 year.
        Uses multiple methods: linear regression, momentum, analyst consensus, and ML ensemble.
        """
        current_price = info.get('currentPrice', 0) or info.get('previousClose', 0)
        if not current_price:
            return {}

        data = historical.get('data', [])
        closes = [d['close'] for d in data] if data else []

        # Method 1: Linear Regression Projection
        lr_targets = self._linear_regression_target(closes, current_price)

        # Method 2: Analyst Consensus
        analyst_targets = {
            'high': info.get('targetHighPrice', 0) or 0,
            'low': info.get('targetLowPrice', 0) or 0,
            'mean': info.get('targetMeanPrice', 0) or 0,
            'median': info.get('targetMedianPrice', 0) or 0,
        }

        # Method 3: Momentum-based projection
        momentum_targets = self._momentum_targets(closes, current_price)

        # Method 4: Fundamental-adjusted targets
        growth_rate = info.get('revenueGrowth', 0) or 0
        earnings_growth = info.get('earningsGrowth', 0) or 0

        # Combine methods with weights
        sentiment_multiplier = 1 + (sentiment.get('score', 50) - 50) / 500
        growth_multiplier = 1 + (growth.get('score', 50) - 50) / 500
        technical_multiplier = 1 + (technical.get('score', 50) - 50) / 500

        combined_multiplier = sentiment_multiplier * growth_multiplier * technical_multiplier

        # 30-day targets
        target_30d_base = self._blend_estimates([
            lr_targets.get('30d', current_price),
            momentum_targets.get('30d', current_price),
            current_price * (1 + growth_rate / 12),  # Monthly growth rate
        ], [0.4, 0.4, 0.2])
        target_30d = target_30d_base * combined_multiplier

        # 6-month targets
        target_6m_base = self._blend_estimates([
            lr_targets.get('180d', current_price),
            momentum_targets.get('180d', current_price),
            analyst_targets.get('mean', current_price) if analyst_targets.get('mean') else current_price,
        ], [0.3, 0.3, 0.4])
        target_6m = target_6m_base * combined_multiplier

        # 1-year targets
        target_1y_base = self._blend_estimates([
            lr_targets.get('365d', current_price),
            momentum_targets.get('365d', current_price),
            analyst_targets.get('mean', current_price) if analyst_targets.get('mean') else current_price,
            current_price * (1 + earnings_growth) if earnings_growth else current_price,
        ], [0.2, 0.2, 0.4, 0.2])
        target_1y = target_1y_base * combined_multiplier

        # Calculate confidence ranges based on volatility
        if len(closes) >= 31:
            prices = np.array(closes[-31:])
            returns = prices[1:] / prices[:-1] - 1
            daily_vol = np.std(returns)
        else:
            daily_vol = 0.02  # Default 2% daily vol

        vol_30d = daily_vol * np.sqrt(30)
        vol_6m = daily_vol * np.sqrt(126)
        vol_1y = daily_vol * np.sqrt(252)

        return {
            '30day': {
                'target': round(target_30d, 2),
                'low': round(target_30d * (1 - vol_30d), 2),
                'high': round(target_30d * (1 + vol_30d), 2),
                'upside': round((target_30d - current_price) / current_price * 100, 2),
                'confidence': self._estimate_confidence(vol_30d),
            },
            '6month': {
                'target': round(target_6m, 2),
                'low': round(target_6m * (1 - vol_6m), 2),
                'high': round(target_6m * (1 + vol_6m), 2),
                'upside': round((target_6m - current_price) / current_price * 100, 2),
                'confidence': self._estimate_confidence(vol_6m),
            },
            '1year': {
                'target': round(target_1y, 2),
                'low': round(target_1y * (1 - vol_1y), 2),
                'high': round(target_1y * (1 + vol_1y), 2),
                'upside': round((target_1y - current_price) / current_price * 100, 2),
                'confidence': self._estimate_confidence(vol_1y),
            },
            'analystConsensus': analyst_targets,
        }

    def _linear_regression_target(self, closes: list, current_price: float) -> Dict[str, float]:
        """Project future prices using linear regression on historical data."""
        result = {}
        if len(closes) < 60:
            return {'30d': current_price, '180d': current_price, '365d': current_price}

        X = np.arange(len(closes)).reshape(-1, 1)
        y = np.array(closes)

        model = LinearRegression()
        model.fit(X, y)

        last_idx = len(closes) - 1
        result['30d'] = float(model.predict([[last_idx + 30]])[0])
        result['180d'] = float(model.predict([[last_idx + 126]])[0])
        result['365d'] = float(model.predict([[last_idx + 252]])[0])

        return result

    def _momentum_targets(self, closes: list, current_price: float) -> Dict[str, float]:
        """Calculate targets based on recent momentum."""
        result = {}
        if len(closes) < 60:
            return {'30d': current_price, '180d': current_price, '365d': current_price}

        # Recent momentum (30-day return)
        momentum_30 = (closes[-1] - closes[-30]) / closes[-30]
        # Medium-term momentum (90-day return)
        momentum_90 = (closes[-1] - closes[-min(90, len(closes))]) / closes[-min(90, len(closes))]

        # Project forward using decaying momentum
        result['30d'] = current_price * (1 + momentum_30 * 0.5)
        result['180d'] = current_price * (1 + momentum_90 * 1.5)
        result['365d'] = current_price * (1 + momentum_90 * 2.5)

        return result

    def _blend_estimates(self, estimates: list, weights: list) -> float:
        """Blend multiple price estimates using weights."""
        valid_estimates = [(e, w) for e, w in zip(estimates, weights) if e and e > 0]
        if not valid_estimates:
            return 0

        total_weight = sum(w for _, w in valid_estimates)
        if total_weight == 0:
            return 0

        return sum(e * w for e, w in valid_estimates) / total_weight

    def _estimate_confidence(self, volatility: float) -> str:
        """Estimate confidence level based on volatility."""
        if volatility < 0.10:
            return 'high'
        elif volatility < 0.25:
            return 'medium'
        else:
            return 'low'

    def _get_recommendation(self, score: float, targets: Dict, current_price: float) -> Dict[str, Any]:
        """Generate buy/hold/sell recommendation."""
        target_1y = targets.get('1year', {}).get('target', current_price) if targets else current_price
        upside_1y = ((target_1y - current_price) / current_price * 100) if current_price > 0 else 0

        # Combine score and upside potential
        if score >= 70 and upside_1y > 10:
            action = 'STRONG BUY'
            color = '#00c853'
            reasoning = 'Strong fundamentals combined with significant upside potential'
        elif score >= 60 or (score >= 50 and upside_1y > 15):
            action = 'BUY'
            color = '#4caf50'
            reasoning = 'Favorable analysis with reasonable upside potential'
        elif score >= 45 and score < 60:
            action = 'HOLD'
            color = '#ff9800'
            reasoning = 'Mixed signals suggest holding current position'
        elif score >= 35:
            action = 'SELL'
            color = '#f44336'
            reasoning = 'Weakening fundamentals suggest reducing position'
        else:
            action = 'STRONG SELL'
            color = '#b71c1c'
            reasoning = 'Significant concerns across multiple factors'

        return {
            'action': action,
            'color': color,
            'score': round(score, 1),
            'reasoning': reasoning,
            'upside1Y': round(upside_1y, 2),
        }

    def _assess_risks(self, info: Dict, financials: Dict, technical: Dict) -> List[Dict[str, str]]:
        """Identify key risk factors."""
        risks = []

        beta = info.get('beta', 0) or 0
        if beta > 1.5:
            risks.append({'risk': 'High Beta', 'severity': 'high', 'detail': f'Beta of {beta:.2f} indicates high market sensitivity'})
        elif beta > 1.2:
            risks.append({'risk': 'Elevated Beta', 'severity': 'medium', 'detail': f'Beta of {beta:.2f} indicates above-average volatility'})

        de_ratio = info.get('debtToEquity', 0) or 0
        if de_ratio > 200:
            risks.append({'risk': 'High Leverage', 'severity': 'high', 'detail': f'Debt/Equity of {de_ratio:.0f} poses significant financial risk'})

        current_ratio = info.get('currentRatio', 0) or 0
        if current_ratio < 1:
            risks.append({'risk': 'Liquidity Risk', 'severity': 'high', 'detail': f'Current ratio of {current_ratio:.2f} indicates potential cash flow issues'})

        pe = info.get('trailingPE', 0) or 0
        if pe > 50:
            risks.append({'risk': 'Valuation Risk', 'severity': 'medium', 'detail': f'P/E of {pe:.1f} requires significant growth to justify'})

        indicators = technical.get('indicators', {})
        rsi = indicators.get('rsi', 50)
        if rsi > 75:
            risks.append({'risk': 'Overbought', 'severity': 'medium', 'detail': f'RSI of {rsi:.1f} suggests potential pullback'})

        market_cap = info.get('marketCap', 0) or 0
        if market_cap < 2e9:
            risks.append({'risk': 'Small Cap Risk', 'severity': 'medium', 'detail': 'Small-cap stocks carry higher volatility and liquidity risk'})

        if not risks:
            risks.append({'risk': 'Standard Market Risk', 'severity': 'low', 'detail': 'Standard investment risks apply'})

        return risks

    def _build_key_metrics(self, info: Dict, financials: Dict) -> Dict[str, Any]:
        """Build summary of key financial metrics."""
        market_cap = info.get('marketCap', 0) or 0
        return {
            'marketCap': f'${market_cap/1e12:.2f}T' if market_cap >= 1e12 else f'${market_cap/1e9:.2f}B' if market_cap >= 1e9 else f'${market_cap/1e6:.0f}M',
            'peRatio': info.get('trailingPE', 'N/A'),
            'forwardPE': info.get('forwardPE', 'N/A'),
            'priceToBook': info.get('priceToBook', 'N/A'),
            'dividendYield': f"{(info.get('dividendYield', 0) or 0)*100:.2f}%",
            'beta': info.get('beta', 'N/A'),
            'profitMargin': f"{(info.get('profitMargins', 0) or 0)*100:.1f}%",
            'operatingMargin': f"{(info.get('operatingMargins', 0) or 0)*100:.1f}%",
            'roe': f"{(info.get('returnOnEquity', 0) or 0)*100:.1f}%",
            'debtToEquity': info.get('debtToEquity', 'N/A'),
            'currentRatio': info.get('currentRatio', 'N/A'),
            'revenueGrowth': f"{(info.get('revenueGrowth', 0) or 0)*100:.1f}%",
            'earningsGrowth': f"{(info.get('earningsGrowth', 0) or 0)*100:.1f}%",
            'freeCashFlow': self._format_large_number(info.get('freeCashflow', 0) or 0),
        }

    def _parse_analyst_consensus(self, recs: List[Dict], info: Dict) -> Dict[str, Any]:
        """Parse analyst recommendation consensus."""
        consensus = {
            'numAnalysts': info.get('numberOfAnalystOpinions', 0),
            'recommendation': info.get('recommendationKey', 'none'),
            'targetHigh': info.get('targetHighPrice', 0),
            'targetLow': info.get('targetLowPrice', 0),
            'targetMean': info.get('targetMeanPrice', 0),
            'recentActions': [],
        }

        for rec in recs[:5]:
            consensus['recentActions'].append({
                'date': rec.get('date', ''),
                'firm': rec.get('firm', ''),
                'grade': rec.get('toGrade', ''),
                'action': rec.get('action', ''),
            })

        return consensus

    def _parse_earnings(self, earnings: Dict) -> Dict[str, Any]:
        """Parse earnings data summary."""
        history = earnings.get('history', [])
        dates = earnings.get('dates', [])

        beats = sum(1 for h in history if h.get('surprise', 0) > 0)
        misses = sum(1 for h in history if h.get('surprise', 0) < 0)

        return {
            'totalQuarters': len(history),
            'beats': beats,
            'misses': misses,
            'beatRate': f'{beats/len(history)*100:.0f}%' if history else 'N/A',
            'upcomingDates': dates[:2],
            'recentHistory': history[:4],
        }

    def _ema(self, data: list, period: int) -> float:
        """Calculate Exponential Moving Average."""
        if len(data) < period:
            return data[-1] if data else 0
        multiplier = 2 / (period + 1)
        ema = data[-period]
        for price in data[-period + 1:]:
            ema = (price - ema) * multiplier + ema
        return ema

    def _calculate_rsi(self, closes: list, period: int = 14) -> float:
        """Calculate Relative Strength Index."""
        if len(closes) < period + 1:
            return 50

        changes = np.diff(closes[-period - 1:])
        gains = np.where(changes > 0, changes, 0)
        losses = np.where(changes < 0, -changes, 0)

        avg_gain = np.mean(gains)
        avg_loss = np.mean(losses)

        if avg_loss == 0:
            return 100

        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    @staticmethod
    def _score_label(score: float) -> str:
        """Convert numeric score to label."""
        if score >= 75:
            return 'Very Strong'
        elif score >= 60:
            return 'Strong'
        elif score >= 45:
            return 'Neutral'
        elif score >= 30:
            return 'Weak'
        else:
            return 'Very Weak'

    @staticmethod
    def _format_large_number(num: float) -> str:
        """Format large numbers for display."""
        if abs(num) >= 1e12:
            return f'${num/1e12:.2f}T'
        elif abs(num) >= 1e9:
            return f'${num/1e9:.2f}B'
        elif abs(num) >= 1e6:
            return f'${num/1e6:.1f}M'
        else:
            return f'${num:,.0f}'
