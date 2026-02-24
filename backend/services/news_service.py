"""
News Service
Fetches and analyzes business news for sentiment analysis.
Uses multiple free sources: Google News RSS, Yahoo Finance, and financial news APIs.
"""

import feedparser
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from typing import Dict, Any, List
from textblob import TextBlob
import logging
import re
import json

logger = logging.getLogger(__name__)


class NewsService:
    """Service for fetching and analyzing financial news."""

    GOOGLE_NEWS_RSS = "https://news.google.com/rss/search?q={query}+stock+market&hl=en-US&gl=US&ceid=US:en"
    YAHOO_NEWS_URL = "https://finance.yahoo.com/quote/{symbol}/news"

    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        }
        self._insight_cache = None
        self._insight_cache_time = None

    def get_stock_news(self, symbol: str, company_name: str = "", max_articles: int = 20) -> List[Dict[str, Any]]:
        """Fetch news articles from multiple sources."""
        articles = []

        # Source 1: Google News RSS
        try:
            query = f"{symbol} {company_name}".strip()
            google_articles = self._fetch_google_news(query, max_articles)
            articles.extend(google_articles)
        except Exception as e:
            logger.warning(f"Google News fetch failed: {e}")

        # Source 2: Yahoo Finance RSS
        try:
            yahoo_articles = self._fetch_yahoo_news(symbol)
            articles.extend(yahoo_articles)
        except Exception as e:
            logger.warning(f"Yahoo News fetch failed: {e}")

        # Deduplicate by title similarity
        seen_titles = set()
        unique_articles = []
        for article in articles:
            title_key = re.sub(r'[^a-z0-9]', '', article['title'].lower())[:50]
            if title_key not in seen_titles:
                seen_titles.add(title_key)
                unique_articles.append(article)

        # Sort by date (newest first) and limit
        unique_articles.sort(key=lambda x: x.get('publishedDate', ''), reverse=True)
        return unique_articles[:max_articles]

    def _fetch_google_news(self, query: str = "stock market", max_articles: int = 15) -> List[Dict[str, Any]]:
        """Fetch news from Google News RSS."""
        articles = []
        try:
            url = self.GOOGLE_NEWS_RSS.format(query=query.replace(' ', '+'))
            feed = feedparser.parse(url)

            for entry in feed.entries[:max_articles]:
                published = ''
                if hasattr(entry, 'published'):
                    try:
                        dt = datetime(*entry.published_parsed[:6])
                        published = dt.strftime('%Y-%m-%d %H:%M:%S')
                    except Exception:
                        published = entry.published

                # Clean title (remove source suffix)
                title = entry.get('title', '')
                source = ''
                if ' - ' in title:
                    parts = title.rsplit(' - ', 1)
                    title = parts[0]
                    source = parts[1] if len(parts) > 1 else ''

                articles.append({
                    'title': title,
                    'source': source,
                    'url': entry.get('link', ''),
                    'publishedDate': published,
                    'summary': entry.get('summary', ''),
                    'origin': 'google_news',
                })
        except Exception as e:
            logger.error(f"Google News RSS error: {e}")

        return articles

    def _fetch_yahoo_news(self, symbol: str, max_articles: int = 20) -> List[Dict[str, Any]]:
        """Fetch news from Yahoo Finance (via RSS)."""
        articles = []
        try:
            yahoo_rss = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={symbol}&region=US&lang=en-US"
            feed = feedparser.parse(yahoo_rss)

            for entry in feed.entries[:max_articles]:
                published = ''
                if hasattr(entry, 'published'):
                    try:
                        dt = datetime(*entry.published_parsed[:6])
                        published = dt.strftime('%Y-%m-%d %H:%M:%S')
                    except Exception:
                        published = entry.published

                articles.append({
                    'title': entry.get('title', ''),
                    'source': 'Yahoo Finance',
                    'url': entry.get('link', ''),
                    'publishedDate': published,
                    'summary': entry.get('summary', ''),
                    'origin': 'yahoo_finance',
                })
        except Exception as e:
            logger.error(f"Yahoo News RSS error: {e}")

        return articles

    def analyze_sentiment(self, articles: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyze sentiment of news articles using TextBlob.
        Returns aggregate and per-article sentiment scores.
        """
        if not articles:
            return {
                'overallSentiment': 0,
                'sentimentLabel': 'neutral',
                'positiveCount': 0,
                'negativeCount': 0,
                'neutralCount': 0,
                'articleSentiments': [],
            }

        sentiments = []
        positive_count = 0
        negative_count = 0
        neutral_count = 0

        for article in articles:
            text = f"{article.get('title', '')} {article.get('summary', '')}"
            if not text.strip():
                continue

            blob = TextBlob(text)
            polarity = blob.sentiment.polarity
            subjectivity = blob.sentiment.subjectivity

            if polarity > 0.1:
                label = 'positive'
                positive_count += 1
            elif polarity < -0.1:
                label = 'negative'
                negative_count += 1
            else:
                label = 'neutral'
                neutral_count += 1

            sentiments.append({
                'title': article.get('title', ''),
                'polarity': round(polarity, 4),
                'subjectivity': round(subjectivity, 4),
                'label': label,
            })

        # Calculate overall sentiment
        if sentiments:
            avg_polarity = sum(s['polarity'] for s in sentiments) / len(sentiments)
        else:
            avg_polarity = 0

        if avg_polarity > 0.1:
            overall_label = 'positive'
        elif avg_polarity < -0.1:
            overall_label = 'negative'
        else:
            overall_label = 'neutral'

        return {
            'overallSentiment': round(avg_polarity, 4),
            'sentimentLabel': overall_label,
            'positiveCount': positive_count,
            'negativeCount': negative_count,
            'neutralCount': neutral_count,
            'totalArticles': len(sentiments),
            'articleSentiments': sentiments,
        }

    def get_news_with_sentiment(self, symbol: str, company_name: str = "") -> Dict[str, Any]:
        """
        Fetch news and analyze sentiment in one call.
        Returns both the articles and sentiment analysis.
        """
        articles = self.get_stock_news(symbol, company_name)
        sentiment = self.analyze_sentiment(articles)

        return {
            'articles': articles,
            'sentiment': sentiment,
        }

    def get_market_insights(self) -> List[Dict[str, Any]]:
        """
        Fetch top 10 general market news and analyze their sector/stock impact.
        Optimized with caching for production stability.
        """
        # Return cache if less than 15 minutes old
        if self._insight_cache and self._insight_cache_time:
            if datetime.now() - self._insight_cache_time < timedelta(minutes=15):
                return self._insight_cache

        import random
        
        # Try multiple queries for robustness
        queries = ["financial markets news", "economy stock market", "S&P 500 movement"]
        google_news = []
        for q in queries:
            try:
                google_news.extend(self._fetch_google_news(f"{q} when:1d", max_articles=8))
            except Exception: pass
            if len(google_news) >= 15: break
            
        yahoo_news = self._fetch_yahoo_news("SPY,QQQ,DIA,IWM", max_articles=15)
        
        raw_news = yahoo_news + google_news
        random.shuffle(raw_news)
        
        # Deduplicate and limit to 10
        seen_titles = set()
        news_items = []
        for item in raw_news:
            title_key = re.sub(r'[^a-z0-9]', '', item['title'].lower())[:40]
            if title_key not in seen_titles and len(news_items) < 10:
                seen_titles.add(title_key)
                news_items.append(item)
        
        if not news_items:
            # Emergency fallback news if everything fails
            news_items = [{
                'title': 'Market analysis indicates continued focus on inflation and tech earnings.',
                'source': 'Kautilya AI',
                'url': '#',
                'publishedDate': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'summary': 'General market sentiment remains focused on macroeconomic indicators and corporate guidance.',
            }]

        # Keyword mappings for "Intelligence" simulation
        sector_keywords = {
            'Technology': ['apple', 'microsoft', 'google', 'nvidia', 'ai', 'semiconductor', 'software', 'tech'],
            'Defense': ['war', 'conflict', 'defense', 'military', 'contract', 'missile', 'pentagon', 'cartel', 'border'],
            'Energy': ['oil', 'gas', 'renewable', 'solar', 'energy', 'crude', 'gasoline'],
            'Financial': ['bank', 'fed', 'interest rate', 'inflation', 'yield', 'finance', 'goldman', 'jp morgan'],
            'Healthcare': ['biotech', 'pharma', 'vaccine', 'medical', 'healthcare', 'fda'],
            'Retail': ['consumer', 'retail', 'amazon', 'walmart', 'ecommerce', 'spending'],
        }

        stock_keywords = {
            'Technology': ['NVDA', 'AAPL', 'MSFT', 'GOOGL', 'META', 'TSLA'],
            'Defense': ['LMT', 'RTX', 'NOC', 'GD', 'BA'],
            'Energy': ['XOM', 'CVX', 'BP', 'SHEL'],
            'Financial': ['JPM', 'BAC', 'GS', 'MS'],
            'Healthcare': ['PFE', 'JNJ', 'UNH', 'ABBV'],
            'Retail': ['AMZN', 'WMT', 'HD', 'COST'],
        }

        import random

        insights = []
        
        # Explicitly negative words that TextBlob might miss
        negative_keywords = [
            'tumble', 'tumbles', 'tumbled', 'drop', 'drops', 'dropped', 
            'shed', 'sheds', 'uncertainty', 'fear', 'fears', 'roil', 'roils', 
            'sink', 'sinks', 'lower', 'plunge', 'plunges', 'crash', 'crashes', 
            'bear', 'worst', 'dip', 'dips', 'friction'
        ]
        
        for item in news_items:
            try:
                title = item.get('title', 'Market Update')
                raw_summary = item.get('summary', '') or ''
                
                # Clean summary from HTML safely
                if raw_summary:
                    clean_summary = BeautifulSoup(raw_summary, "html.parser").get_text(separator=" ").strip()
                else:
                    clean_summary = ""
                
                # Extract 2-sentence summary
                if clean_summary:
                    tb_summary = TextBlob(clean_summary)
                    if hasattr(tb_summary, 'sentences') and tb_summary.sentences:
                        short_summary = " ".join(str(s) for s in tb_summary.sentences[:2])
                    else:
                        short_summary = clean_summary[:150]
                else:
                    short_summary = ""
                    
                # Fallback if summary is empty or just repeats title
                if not short_summary or title.lower() in short_summary.lower():
                    short_summary = f"Recent financial update surrounding {title}."
                
                # Identify sectors based on title AND the new clean summary
                identified_sectors = []
                for sector, keywords in sector_keywords.items():
                    # Use regex \b to avoid substring matches like "ai" in "said"
                    if any(re.search(rf'\b{re.escape(k)}\b', title.lower()) or re.search(rf'\b{re.escape(k)}\b', clean_summary.lower()) for k in keywords):
                        identified_sectors.append(sector)
                
                if not identified_sectors:
                    identified_sectors = ['General Market']

                # Determine sentiment
                blob = TextBlob(f"{title} {short_summary}")
                sentiment_val = blob.sentiment.polarity
                sentiment = "positive" if sentiment_val >= 0 else "negative"
                
                # Override with explicit negative keywords using word boundaries
                if any(re.search(rf'\b{re.escape(word)}\b', title.lower()) or re.search(rf'\b{re.escape(word)}\b', short_summary.lower()) for word in negative_keywords):
                    sentiment = "negative"

                # Identify potential stocks based on matched sectors specifically
                suggested_stocks_pool = []
                
                if 'General Market' in identified_sectors:
                    # Provide a mix of large caps
                    suggested_stocks_pool = ['SPY', 'QQQ', 'DIA']
                else:
                    for sector in identified_sectors:
                        stocks = stock_keywords.get(sector, [])
                        suggested_stocks_pool.extend(stocks)
                
                # Remove duplicates
                suggested_stocks_pool = list(set(suggested_stocks_pool))
                
                # Dynamically select up to 3 random distinct stocks from the relevant pool
                num_to_select = min(3, len(suggested_stocks_pool))
                suggested_stocks = random.sample(suggested_stocks_pool, num_to_select)

                insights.append({
                    'title': title,
                    'source': item.get('source', 'Unknown'),
                    'url': item.get('url', '#'),
                    'publishedDate': item.get('publishedDate', ''),
                    'sectors': identified_sectors,
                    'sentiment': sentiment,
                    'description': f"{short_summary}", 
                    'stocks': suggested_stocks
                })
            except Exception as e:
                logger.error(f"Error processing news item: {e}")
                continue

        self._insight_cache = insights
        self._insight_cache_time = datetime.now()
        return insights
