"""
NifiPilot API Server
Flask-based REST API that serves stock analysis data to the frontend.
"""

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import logging
import os
import sys

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.services.stock_data import StockDataService
from backend.services.news_service import NewsService
from backend.services.analysis_engine import AnalysisEngine

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
news_service = NewsService()
analysis_engine = AnalysisEngine()


@app.route('/')
def index():
    """Serve the frontend."""
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
            future_news = executor.submit(news_service.get_news_with_sentiment, symbol, company_info.get('name', ''))
            
            # Collect results (gracefully)
            historical = get_safe(future_hist, {'data': [], 'dataPoints': 0}, "historical prices")
            financials = get_safe(future_fin, {}, "financials")
            institutional = get_safe(future_inst, [], "institutional")
            insider_tx = get_safe(future_insider, [], "insider tx")
            analyst_recs = get_safe(future_recs, [], "analyst recs")
            earnings = get_safe(future_earn, {}, "earnings")
            sec_filings = get_safe(future_sec, [], "sec filings")
            news_data = get_safe(future_news, {'articles': [], 'sentiment': {}}, "news data")

        # Step 10: Run analysis engine
        logger.info(f"[{symbol}] Step 10: Running analysis engine...")
        analysis = analysis_engine.run_full_analysis(
            company_info=company_info,
            financials=financials,
            historical_prices=historical,
            news_sentiment=news_data,
            institutional_holders=institutional,
            insider_transactions=insider_tx,
            analyst_recs=analyst_recs,
            earnings_data=earnings,
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

        logger.info(f"=== [{symbol}] Analysis complete! Score: {analysis.get('overallScore', 'N/A')}, "
                     f"Rec: {analysis.get('recommendation', {}).get('action', 'N/A')} ===")
        return jsonify(response)

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
        data = news_service.get_news_with_sentiment(symbol.upper())
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


@app.route('/api/market-insights')
def market_insights():
    """Get top 10 financial news and sector/stock analysis."""
    try:
        insights = news_service.get_market_insights()
        return jsonify({'insights': insights})
    except Exception as e:
        logger.error(f"Market insights error: {e}")
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_DEBUG', 'true').lower() == 'true'
    app.run(host='0.0.0.0', port=port, debug=debug, load_dotenv=False)
