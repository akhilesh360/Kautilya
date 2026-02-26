"""
NifiPilot API Server
Flask-based REST API that serves stock analysis data to the frontend.
"""

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import logging
import os
import sys
from datetime import datetime, timedelta
import re
from bs4 import BeautifulSoup

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _load_local_env():
    """Lightweight .env loader so local secrets (e.g., FMP_API_KEY) work with load_dotenv=False."""
    try:
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        env_path = os.path.join(root, ".env")
        if not os.path.exists(env_path):
            return
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = value
    except Exception as e:
        logging.getLogger(__name__).warning(f".env load skipped: {e}")


_load_local_env()

from backend.services.stock_data import StockDataService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__, static_folder='../frontend', static_url_path='')
CORS(app)

# Initialize services
stock_service = StockDataService()
analysis_engine = None
news_service = None
sec_service = None
portfolio_risk_service = None
paper_trading_service = None
top_gainers_cache = None
top_gainers_cache_time = None


def to_json_safe(value):
    """Convert numpy/pandas scalars and nested containers to plain JSON-safe Python types."""
    if isinstance(value, dict):
        return {str(k): to_json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [to_json_safe(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value

    # numpy scalars often expose .item(); arrays often expose .tolist().
    if hasattr(value, "item"):
        try:
            return to_json_safe(value.item())
        except Exception:
            pass
    if hasattr(value, "tolist"):
        try:
            return to_json_safe(value.tolist())
        except Exception:
            pass
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass

    return str(value)


def get_news_service():
    """Lazily initialize NewsService to avoid blocking app startup."""
    global news_service
    if news_service is None:
        from backend.services.news_service import NewsService
        news_service = NewsService()
    return news_service


def get_sec_service():
    """Lazily initialize SECService to avoid blocking app startup."""
    global sec_service
    if sec_service is None:
        from backend.services.sec_service import SECService
        sec_service = SECService()
    return sec_service


def get_analysis_engine():
    """Lazily initialize AnalysisEngine to avoid heavy import on startup."""
    global analysis_engine
    if analysis_engine is None:
        from backend.services.analysis_engine import AnalysisEngine
        analysis_engine = AnalysisEngine()
    return analysis_engine


def get_portfolio_risk_service():
    global portfolio_risk_service
    if portfolio_risk_service is None:
        from backend.services.portfolio_risk_service import PortfolioRiskService
        portfolio_risk_service = PortfolioRiskService()
    return portfolio_risk_service


def get_paper_trading_service():
    global paper_trading_service
    if paper_trading_service is None:
        from backend.services.paper_trading_service import PaperTradingService
        paper_trading_service = PaperTradingService()
    return paper_trading_service


SECTOR_BENCHMARKS = {
    'Technology': 'XLK',
    'Financial': 'XLF',
    'Healthcare': 'XLV',
    'Energy': 'XLE',
    'Retail': 'XLY',
    'Consumer Cyclical': 'XLY',
    'Consumer Defensive': 'XLP',
    'Industrials': 'XLI',
    'Basic Materials': 'XLB',
    'Utilities': 'XLU',
    'Real Estate': 'XLRE',
    'Communication Services': 'XLC',
}

# Tracked liquid universe for "Top Gainers Today" (free-data approximation).
TOP_GAINERS_UNIVERSE = [
    'AAPL', 'MSFT', 'NVDA', 'AMZN', 'GOOGL', 'META', 'TSLA', 'AMD', 'NFLX', 'AVGO',
    'ORCL', 'CRM', 'ADBE', 'QCOM', 'INTC', 'MU', 'ARM', 'SMCI', 'PLTR', 'SNOW',
    'SHOP', 'UBER', 'ABNB', 'COIN', 'HOOD', 'SQ', 'PYPL', 'JPM', 'BAC', 'GS',
    'MS', 'WFC', 'V', 'MA', 'AXP', 'BRK-B', 'XOM', 'CVX', 'SLB', 'COP',
    'LLY', 'UNH', 'JNJ', 'MRK', 'PFE', 'ABBV', 'NVO', 'AMGN', 'CAT', 'GE',
    'BA', 'RTX', 'LMT', 'DE', 'COST', 'WMT', 'HD', 'LOW', 'MCD', 'DIS'
]


def _classify_gainer_reason(title: str) -> str:
    """Heuristic label to summarize why a stock may be moving today."""
    t = (title or "").lower()
    rules = [
        ("earnings beat / guidance", [r"\bearnings\b", r"\bprofit\b", r"\bguidance\b", r"\brevenue\b", r"\bresults?\b"]),
        ("analyst upgrade / target raise", [r"\bupgrade\b", r"\braised target\b", r"\bprice target\b", r"\boutperform\b", r"\bbuy rating\b"]),
        ("AI / product momentum", [r"\bai\b", r"\bchip\b", r"\bsemiconductor\b", r"\bproduct\b", r"\blaunch\b"]),
        ("deal / acquisition / partnership", [r"\bdeal\b", r"\bacquisition\b", r"\bmerge(r|d)?\b", r"\bpartnership\b", r"\bcontract\b"]),
        ("regulatory / legal update", [r"\bfda\b", r"\bapproval\b", r"\bregulator(y|s)?\b", r"\bcourt\b", r"\blawsuit\b"]),
        ("macro / rates / market rally", [r"\bfed\b", r"\brates?\b", r"\binflation\b", r"\brally\b", r"\bmarket\b", r"\bindex\b"]),
    ]
    for label, patterns in rules:
        if any(re.search(p, t) for p in patterns):
            return label
    return "news-driven momentum (headline catalyst)"


def _safe_pct_change(current: float, prev: float):
    try:
        current = float(current or 0)
        prev = float(prev or 0)
        if current <= 0 or prev <= 0:
            return None
        return ((current - prev) / prev) * 100.0
    except Exception:
        return None


def _benchmark_for_sector(sector: str) -> str:
    if not sector:
        return 'SPY'
    return SECTOR_BENCHMARKS.get(sector, 'SPY')


def _build_benchmark_relative_5y(company_info: dict, stock_historical: dict, price_targets: dict):
    """Attach benchmark-relative context for the 5Y target (broad/sector ETF comparison)."""
    try:
        target_5y = (price_targets or {}).get('5year')
        if not target_5y:
            return None

        sector = (company_info or {}).get('sector', '')
        benchmark_symbol = _benchmark_for_sector(sector)
        benchmark_hist = stock_service.get_historical_prices(benchmark_symbol, period='5y')
        bench_data = (benchmark_hist or {}).get('data', []) or []
        if len(bench_data) < 30:
            return {
                'benchmarkSymbol': benchmark_symbol,
                'benchmarkName': benchmark_symbol,
                'relativeUpsideVsBenchmarkPct': None,
                'status': 'insufficient_benchmark_data',
            }

        bench_current = float(bench_data[-1].get('close', 0) or 0)
        lookback = min(len(bench_data) - 1, 252 * 5)
        bench_start = float(bench_data[-lookback - 1].get('close', 0) or 0) if len(bench_data) > lookback else float(bench_data[0].get('close', 0) or 0)
        years = max(lookback / 252.0, 1.0)
        if bench_start <= 0 or bench_current <= 0:
            return None
        bench_cagr = (bench_current / bench_start) ** (1 / years) - 1
        bench_5y_target = bench_current * ((1 + max(-0.05, min(0.20, bench_cagr))) ** 5)
        bench_upside = ((bench_5y_target - bench_current) / bench_current) * 100 if bench_current else 0
        stock_upside = float(target_5y.get('upside', 0) or 0)
        return {
            'benchmarkSymbol': benchmark_symbol,
            'benchmarkName': benchmark_symbol,
            'benchmarkCurrent': round(bench_current, 2),
            'benchmark5YTarget': round(bench_5y_target, 2),
            'benchmark5YUpsidePct': round(bench_upside, 2),
            'benchmarkEstimatedCagrPct': round(bench_cagr * 100, 2),
            'relativeUpsideVsBenchmarkPct': round(stock_upside - bench_upside, 2),
            'status': 'ok',
        }
    except Exception as e:
        logger.warning(f"5Y benchmark-relative computation failed: {e}")
        return {'status': 'error', 'error': str(e)}


@app.route('/')
def index():
    """Serve the frontend."""
    return send_from_directory(app.static_folder, 'index.html')


@app.route('/analyze')
@app.route('/insights')
@app.route('/bucket')
@app.route('/about')
def app_pages():
    """Serve SPA entry for top-level page routes."""
    return send_from_directory(app.static_folder, 'index.html')


@app.route('/<path:path>')
def static_files(path):
    """Serve static files."""
    return send_from_directory(app.static_folder, path)


@app.route('/api/health')
def health():
    """Health check endpoint."""
    return jsonify({'status': 'ok', 'service': 'NifiPilot Stock Analyzer'})


@app.route('/api/search')
def search():
    """Search for stocks by name or ticker."""
    query = request.args.get('q', '').strip()
    if not query:
        return jsonify({'error': 'Query parameter "q" is required'}), 400

    try:
        results = stock_service.search_stock(query)
        return jsonify({'results': results, 'query': query})
    except Exception as e:
        logger.error(f"Search error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/analyze/<symbol>')
def analyze(symbol):
    """
    Run comprehensive analysis for a stock.
    Always returns 200 with whatever data we can get.
    Uses yf.download() (most reliable) for prices, gracefully degrades on other data.
    """
    import time

    symbol = symbol.upper().strip()
    logger.info(f"=== Starting analysis for {symbol} ===")

    try:
        # Step 1: Base Ticker Info (Fastest)
        logger.info(f"[{symbol}] Starting parallel fetching...")
        import concurrent.futures
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            # Task definitions
            future_info = executor.submit(stock_service.get_company_info, symbol)
            future_hist = executor.submit(stock_service.get_historical_prices, symbol, period="5y")
            future_fin = executor.submit(stock_service.get_quarterly_financials, symbol)
            future_inst = executor.submit(stock_service.get_institutional_holders, symbol)
            future_insider = executor.submit(stock_service.get_insider_transactions, symbol)
            future_recs = executor.submit(stock_service.get_analyst_recommendations, symbol)
            future_earn = executor.submit(stock_service.get_earnings_data, symbol)
            future_sec = executor.submit(stock_service.get_sec_filings, symbol)
            future_sec_edge = executor.submit(get_sec_service().identify_alpha_edge, symbol)
            
            # Helper to get result with timeout and fallback
            def get_safe(future, default, name):
                try:
                    return future.result(timeout=25)
                except Exception as e:
                    logger.warning(f"[{symbol}] {name} fetch failed or timed out: {e}")
                    return default

            # Wait for base info first
            company_info = get_safe(future_info, {}, "company info")
            
            # Now fetch news with name
            future_news = executor.submit(get_news_service().get_news_with_sentiment, symbol, company_info.get('name', ''))
            
            # Collect results (gracefully)
            historical = get_safe(future_hist, {'data': [], 'dataPoints': 0}, "historical prices")
            financials = get_safe(future_fin, {}, "financials")
            institutional = get_safe(future_inst, [], "institutional")
            insider_tx = get_safe(future_insider, [], "insider tx")
            analyst_recs = get_safe(future_recs, [], "analyst recs")
            earnings = get_safe(future_earn, {}, "earnings")
            sec_filings = get_safe(future_sec, [], "sec filings")
            sec_edge = get_safe(future_sec_edge, {}, "sec edge")
            news_data = get_safe(future_news, {'articles': [], 'sentiment': {}}, "news data")

        # Step 10: Run analysis engine
        logger.info(f"[{symbol}] Step 10: Running analysis engine...")
        analysis = get_analysis_engine().run_full_analysis(
            company_info=company_info,
            financials=financials,
            historical_prices=historical,
            news_sentiment=news_data,
            institutional_holders=institutional,
            insider_transactions=insider_tx,
            analyst_recs=analyst_recs,
            earnings_data=earnings,
            sec_edge=sec_edge,
        )

        # Compile response — always return 200
        response = {
            'analysis': analysis,
            'companyInfo': company_info,
            'financials': financials,
            'historicalPrices': historical,
            'news': news_data,
            'institutionalHolders': institutional,
            'insiderTransactions': insider_tx,
            'analystRecommendations': analyst_recs,
            'earnings': earnings,
            'secFilings': sec_filings,
        }

        # Add benchmark-relative context for the 5Y target (long-term investing view).
        try:
            benchmark_ctx = _build_benchmark_relative_5y(company_info, historical, analysis.get('priceTargets', {}))
            if benchmark_ctx and isinstance(analysis.get('priceTargets'), dict) and '5year' in analysis['priceTargets']:
                analysis['priceTargets']['5year']['benchmark'] = benchmark_ctx
                analysis['benchmarkComparison5Y'] = benchmark_ctx
        except Exception as e:
            logger.warning(f"[{symbol}] benchmark-relative 5Y context failed: {e}")

        logger.info(f"=== [{symbol}] Analysis complete! Score: {analysis.get('overallScore', 'N/A')}, "
                     f"Rec: {analysis.get('recommendation', {}).get('action', 'N/A')} ===")
        return jsonify(to_json_safe(response))

    except Exception as e:
        logger.error(f"Analysis error for {symbol}: {e}", exc_info=True)
        return jsonify({'error': f'Analysis failed: {str(e)}'}), 500


@app.route('/api/company/<symbol>')
def company_info(symbol):
    """Get company information."""
    try:
        info = stock_service.get_company_info(symbol.upper())
        return jsonify(info)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/financials/<symbol>')
def financials(symbol):
    """Get quarterly financial statements."""
    try:
        data = stock_service.get_quarterly_financials(symbol.upper())
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/prices/<symbol>')
def prices(symbol):
    """Get historical price data."""
    period = request.args.get('period', '5y')
    try:
        data = stock_service.get_historical_prices(symbol.upper(), period)
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/news/<symbol>')
def news(symbol):
    """Get news and sentiment."""
    try:
        data = get_news_service().get_news_with_sentiment(symbol.upper())
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/sec-filings/<symbol>')
def sec_filings(symbol):
    """Get SEC filing links."""
    try:
        filings = stock_service.get_sec_filings(symbol.upper())
        return jsonify({'filings': filings})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/sec-edge/<symbol>')
def sec_edge(symbol):
    """Get pattern-based 'edge' from 10-K/10-Q text analysis."""
    try:
        edge_data = get_sec_service().identify_alpha_edge(symbol.upper())
        return jsonify(to_json_safe(edge_data))
    except Exception as e:
        logger.error(f"SEC Edge error for {symbol}: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/market-insights')
def market_insights():
    """Get top 10 financial news and sector/stock analysis."""
    try:
        insights = get_news_service().get_market_insights()
        return jsonify({'insights': insights})
    except Exception as e:
        logger.error(f"Market insights error: {e}")
        return jsonify({'insights': [], 'error': str(e)})


@app.route('/api/top-gainers-today')
def top_gainers_today():
    """Top 10 gainers today from a tracked liquid universe + headline-based reason."""
    global top_gainers_cache, top_gainers_cache_time
    try:
        # Short cache to reduce Yahoo/news load during UI refreshes.
        if top_gainers_cache and top_gainers_cache_time and (datetime.now() - top_gainers_cache_time) < timedelta(minutes=10):
            return jsonify(to_json_safe(top_gainers_cache))

        import concurrent.futures

        # Prefer FMP biggest-gainers if API key exists; fallback to tracked-universe ranking.
        fmp_gainers = []
        try:
            fmp_gainers = stock_service.get_top_gainers_today(limit=10)
        except Exception as e:
            logger.warning(f"FMP biggest-gainers fetch failed: {e}")

        if fmp_gainers:
            top10 = []
            for row in fmp_gainers:
                symbol = str(row.get('symbol', '')).upper().strip()
                if not symbol:
                    continue
                info = {}
                try:
                    info = stock_service.get_company_info(symbol)
                except Exception:
                    info = {}
                top10.append({
                    'symbol': symbol,
                    'name': row.get('name') or info.get('name', symbol),
                    'sector': info.get('sector', 'Unknown'),
                    'currentPrice': row.get('currentPrice') or info.get('currentPrice', 0),
                    'previousClose': info.get('previousClose', 0),
                    'changePct': row.get('changePct'),
                    'changeAbs': row.get('changeAbs'),
                    'volume': row.get('volume') or info.get('volume', 0),
                })
            source_method = 'fmp_biggest_gainers'
        else:
            top10 = []
            source_method = 'tracked_universe_prev_close_change'

        def fetch_quote(symbol: str):
            info = stock_service.get_company_info(symbol)
            pct = _safe_pct_change(info.get('currentPrice'), info.get('previousClose'))
            return {
                'symbol': symbol,
                'name': info.get('name', symbol),
                'sector': info.get('sector', 'Unknown'),
                'currentPrice': info.get('currentPrice', 0),
                'previousClose': info.get('previousClose', 0),
                'changePct': pct,
                'changeAbs': (float(info.get('currentPrice', 0) or 0) - float(info.get('previousClose', 0) or 0)) if info else None,
                'volume': info.get('volume', 0),
            }

        if not top10:
            quotes = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
                futures = {ex.submit(fetch_quote, s): s for s in TOP_GAINERS_UNIVERSE}
                for f in concurrent.futures.as_completed(futures):
                    try:
                        item = f.result(timeout=20)
                        if item.get('changePct') is not None:
                            quotes.append(item)
                    except Exception as e:
                        logger.warning(f"Top gainers quote fetch failed for {futures.get(f)}: {e}")

            top10 = sorted(
                [q for q in quotes if (q.get('changePct') or -999) > 0],
                key=lambda x: x.get('changePct') or -999,
                reverse=True
            )[:10]

        def attach_reason(row: dict):
            symbol = row.get('symbol')
            company_name = row.get('name', '')
            try:
                news_data = get_news_service().get_news_with_sentiment(symbol, company_name)
                articles = (news_data or {}).get('articles', []) or []
                best = articles[0] if articles else {}
                title = best.get('title', '')
                summary = best.get('summary', '') or ''
                row['reasonCategory'] = _classify_gainer_reason(title)
                row['reason'] = title or f"Price momentum on elevated trading activity (+{(row.get('changePct') or 0):.2f}%)."
                row['reasonSource'] = best.get('source', 'News')
                row['reasonUrl'] = best.get('url', '#')
                row['reasonPublishedDate'] = best.get('publishedDate', '')
                row['newsSentiment'] = ((news_data or {}).get('sentiment') or {}).get('sentimentLabel', 'neutral')
                if summary:
                    row['reasonSummary'] = BeautifulSoup(summary, "html.parser").get_text(separator=" ").strip()[:220]
            except Exception as e:
                logger.warning(f"Top gainers reason fetch failed for {symbol}: {e}")
                row['reasonCategory'] = 'price momentum'
                row['reason'] = f"No fresh headline available; stock is up {(row.get('changePct') or 0):.2f}% vs previous close."
                row['reasonSource'] = 'Kautilya'
                row['newsSentiment'] = 'neutral'
            return row

        if top10:
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
                top10 = list(ex.map(attach_reason, top10))

        payload = {
            'asOf': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'universeSize': len(TOP_GAINERS_UNIVERSE),
            'method': source_method,
            'gainers': [
                {
                    **g,
                    'rank': idx + 1,
                    'changePct': round(float(g.get('changePct') or 0), 2),
                    'changeAbs': round(float(g.get('changeAbs') or 0), 2),
                    'currentPrice': round(float(g.get('currentPrice') or 0), 2),
                }
                for idx, g in enumerate(top10)
            ]
        }
        top_gainers_cache = payload
        top_gainers_cache_time = datetime.now()
        return jsonify(to_json_safe(payload))
    except Exception as e:
        logger.error(f"Top gainers today error: {e}", exc_info=True)
        return jsonify({'gainers': [], 'error': str(e)}), 500


@app.route('/api/portfolio/guardrails', methods=['POST'])
def portfolio_guardrails():
    """Evaluate portfolio-level risk guardrails for a list of positions."""
    try:
        payload = request.get_json(silent=True) or {}
        positions = payload.get('positions', []) or []
        aum_usd = payload.get('aumUsd')
        current_drawdown_pct = payload.get('currentDrawdownPct')
        limits = payload.get('limits', {}) or {}

        company_data = {}
        for p in positions:
            symbol = str((p or {}).get('symbol', '')).upper().strip()
            if not symbol or symbol in company_data:
                continue
            try:
                company_data[symbol] = stock_service.get_company_info(symbol)
            except Exception as e:
                logger.warning(f"Portfolio guardrails company info fetch failed for {symbol}: {e}")
                company_data[symbol] = {'symbol': symbol, 'currentPrice': 0, 'previousClose': 0, 'sector': 'Unknown', 'industry': 'Unknown'}

        result = get_portfolio_risk_service().evaluate(
            positions=positions,
            company_data=company_data,
            aum_usd=float(aum_usd) if aum_usd is not None else None,
            current_drawdown_pct=float(current_drawdown_pct) if current_drawdown_pct is not None else None,
            limits=limits,
        )
        return jsonify(to_json_safe(result))
    except Exception as e:
        logger.error(f"Portfolio guardrails error: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500


@app.route('/api/paper-trade/log', methods=['POST'])
def paper_trade_log():
    """Append a paper-trade audit event to the daily JSONL log."""
    try:
        payload = request.get_json(silent=True) or {}
        event_type = str(payload.get('eventType', 'decision'))
        body = payload.get('payload', payload)
        result = get_paper_trading_service().log_event(event_type=event_type, payload=to_json_safe(body))
        return jsonify(to_json_safe(result))
    except Exception as e:
        logger.error(f"Paper trade log error: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500


@app.route('/api/paper-trade/logs')
def paper_trade_logs():
    """Fetch paper-trade audit logs for a given date (UTC)."""
    try:
        date_str = request.args.get('date')  # YYYY-MM-DD
        limit = int(request.args.get('limit', 100))
        result = get_paper_trading_service().read_logs(date_str=date_str, limit=limit)
        return jsonify(to_json_safe(result))
    except Exception as e:
        logger.error(f"Paper trade logs error: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500


@app.route('/api/portfolio/long-term-view', methods=['POST'])
def portfolio_long_term_view():
    """Aggregate long-term allocation view for the bucket (quality + 5Y upside + guardrails)."""
    try:
        payload = request.get_json(silent=True) or {}
        positions = payload.get('positions', []) or []
        aum_usd = payload.get('aumUsd')
        current_drawdown_pct = payload.get('currentDrawdownPct')
        limits = payload.get('limits', {}) or {}

        rows = []
        company_data = {}
        for p in positions:
            symbol = str((p or {}).get('symbol', '')).upper().strip()
            if not symbol:
                continue
            qty = float((p or {}).get('qty', 0) or 0)
            avg_price = float((p or {}).get('avgPrice', 0) or 0)

            company_info = stock_service.get_company_info(symbol)
            historical = stock_service.get_historical_prices(symbol, period="2y")
            financials = stock_service.get_quarterly_financials(symbol)
            institutional = stock_service.get_institutional_holders(symbol)
            insider_tx = stock_service.get_insider_transactions(symbol)
            analyst_recs = stock_service.get_analyst_recommendations(symbol)
            earnings = stock_service.get_earnings_data(symbol)
            news_data = {'articles': [], 'sentiment': {}}  # long-term view ignores short-term news for speed/stability
            sec_edge = {}

            analysis = get_analysis_engine().run_full_analysis(
                company_info=company_info,
                financials=financials,
                historical_prices=historical,
                news_sentiment=news_data,
                institutional_holders=institutional,
                insider_transactions=insider_tx,
                analyst_recs=analyst_recs,
                earnings_data=earnings,
                sec_edge=sec_edge,
            )
            benchmark_ctx = _build_benchmark_relative_5y(company_info, historical, analysis.get('priceTargets', {}))
            if benchmark_ctx and '5year' in (analysis.get('priceTargets') or {}):
                analysis['priceTargets']['5year']['benchmark'] = benchmark_ctx

            current_price = float(company_info.get('currentPrice', 0) or company_info.get('previousClose', 0) or 0)
            market_value = current_price * qty if current_price and qty else 0.0
            company_data[symbol] = company_info
            rows.append({
                'symbol': symbol,
                'qty': qty,
                'avgPrice': avg_price,
                'currentPrice': round(current_price, 2) if current_price else 0,
                'marketValue': round(market_value, 2),
                'sector': company_info.get('sector', 'Unknown'),
                'longTermQualityScore': analysis.get('longTermQualityScore'),
                'longTermQualityLabel': analysis.get('longTermQualityLabel'),
                'confidenceScore': analysis.get('confidenceScore'),
                'dataQualityScore': (analysis.get('dataQuality') or {}).get('score'),
                'fiveYearTarget': ((analysis.get('priceTargets') or {}).get('5year') or {}),
                'recommendation': analysis.get('recommendation', {}),
            })

        total_mv = sum(float(r.get('marketValue', 0) or 0) for r in rows) or 0
        aum_base = float(aum_usd) if aum_usd is not None else total_mv
        ranked = []
        raw_scores = []
        for r in rows:
            q = float(r.get('longTermQualityScore', 50) or 50)
            f5 = r.get('fiveYearTarget', {}) or {}
            upside5 = float(f5.get('upside', 0) or 0)
            bench_rel = float(((f5.get('benchmark') or {}).get('relativeUpsideVsBenchmarkPct', 0) or 0))
            confidence = float(r.get('confidenceScore', 50) or 50)
            data_q = float(r.get('dataQualityScore', 50) or 50)
            allocation_signal = (q * 0.45) + (min(max(upside5, -50), 150) * 0.15) + (bench_rel * 0.15) + (confidence * 0.15) + (data_q * 0.10)
            raw_scores.append(max(0.0, allocation_signal))
            r['allocationSignal'] = round(allocation_signal, 2)

        score_sum = sum(raw_scores) or 1.0
        for idx, r in enumerate(rows):
            target_weight = (raw_scores[idx] / score_sum) if raw_scores[idx] > 0 else 0
            current_weight = (float(r.get('marketValue', 0) or 0) / aum_base) if aum_base > 0 else 0
            r['currentWeightPct'] = round(current_weight * 100, 2)
            r['targetWeightPct'] = round(target_weight * 100, 2)
            r['weightGapPct'] = round((target_weight - current_weight) * 100, 2)
            ranked.append(r)

        ranked.sort(key=lambda x: x.get('targetWeightPct', 0), reverse=True)

        guardrails = get_portfolio_risk_service().evaluate(
            positions=positions,
            company_data=company_data,
            aum_usd=float(aum_usd) if aum_usd is not None else None,
            current_drawdown_pct=float(current_drawdown_pct) if current_drawdown_pct is not None else None,
            limits=limits,
        )

        response = {
            'portfolioAumUsd': round(aum_base, 2) if aum_base else None,
            'positions': ranked,
            'guardrails': guardrails,
            'method': {
                'name': 'long_term_allocation_view_v1',
                'description': 'Ranks holdings by long-term quality, 5Y upside, benchmark-relative spread, confidence, and data quality.',
            }
        }
        return jsonify(to_json_safe(response))
    except Exception as e:
        logger.error(f"Portfolio long-term view error: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_DEBUG', 'true').lower() == 'true'
    app.run(host='0.0.0.0', port=port, debug=debug, load_dotenv=False)
