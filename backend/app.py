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
        # Step 1: Company Info + Price Data (uses yf.download as base — very reliable)
        logger.info(f"[{symbol}] Step 1: Fetching company info & price data...")
        company_info = stock_service.get_company_info(symbol)
        logger.info(f"[{symbol}] → Price: ${company_info.get('currentPrice', 0)}, Name: {company_info.get('name', 'N/A')}")

        # Step 2: Historical prices (uses yf.download — very reliable)
        logger.info(f"[{symbol}] Step 2: Fetching all-time historical prices...")
        historical = stock_service.get_historical_prices(symbol, period="max")
        logger.info(f"[{symbol}] → Got {historical.get('dataPoints', 0)} price data points")

        # Step 3: Quarterly financials
        logger.info(f"[{symbol}] Step 3: Fetching quarterly financials...")
        time.sleep(0.5)
        financials = stock_service.get_quarterly_financials(symbol)

        # Step 4: News & Sentiment (RSS feeds, no Yahoo rate limit)
        logger.info(f"[{symbol}] Step 4: Fetching news and sentiment...")
        news_data = news_service.get_news_with_sentiment(symbol, company_info.get('name', ''))

        # Step 5: Institutional holdings
        logger.info(f"[{symbol}] Step 5: Fetching institutional data...")
        time.sleep(0.5)
        institutional = stock_service.get_institutional_holders(symbol)

        # Step 6: Insider transactions
        logger.info(f"[{symbol}] Step 6: Fetching insider transactions...")
        insider_tx = stock_service.get_insider_transactions(symbol)

        # Step 7: Analyst recommendations
        logger.info(f"[{symbol}] Step 7: Fetching analyst recommendations...")
        time.sleep(0.5)
        analyst_recs = stock_service.get_analyst_recommendations(symbol)

        # Step 8: Earnings
        logger.info(f"[{symbol}] Step 8: Fetching earnings data...")
        earnings = stock_service.get_earnings_data(symbol)

        # Step 9: SEC filings
        logger.info(f"[{symbol}] Step 9: Getting SEC filing links...")
        sec_filings = stock_service.get_sec_filings(symbol)

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
