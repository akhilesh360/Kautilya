# NifiPilot — AI-Powered Stock Intelligence Platform

<div align="center">

**Comprehensive stock analysis using SEC 10-K/10-Q filings, real-time news sentiment, technical indicators, and institutional flow data.**

Get dynamic price targets and actionable **Buy / Hold / Sell** recommendations.

</div>

---

## 🚀 Features

### Core Analysis Engine
- **5 Years of Quarterly Financial Data** — Income statements, balance sheets, and cash flow statements (equivalent to 10-Q/10-K filing data)
- **Buy / Hold / Sell Recommendations** — AI-driven scoring system combining 6 analytical dimensions
- **Dynamic Price Targets** — 30-day, 6-month, and 1-year projections with confidence ranges
- **SEC Filing Integration** — Direct links to 10-K and 10-Q filings on SEC EDGAR

### Multi-Factor Analysis
| Factor | Weight | Description |
|--------|--------|-------------|
| 📋 **Fundamental** | 30% | Profit margins, ROE, debt ratios, free cash flow |
| 📈 **Technical** | 25% | RSI, MACD, moving averages, volatility, momentum |
| 💬 **Sentiment** | 15% | News sentiment from Google & Yahoo Finance |
| 💎 **Valuation** | 15% | P/E, P/B, dividend yield, analyst targets |
| 🚀 **Growth** | 10% | Revenue growth, earnings growth, quarterly trends |
| 🏛️ **Institutional** | 5% | Institutional holdings, insider transactions |

### Data Sources
- **Financial Data**: Yahoo Finance (via yfinance)
- **SEC Filings**: SEC EDGAR integration
- **News**: Google News RSS, Yahoo Finance RSS
- **Sentiment**: TextBlob NLP analysis
- **Technical Indicators**: Custom RSI, MACD, SMA, EMA, Bollinger calculations

---

## 🛠 Installation

### Prerequisites
- Python 3.10+
- pip

### Setup

```bash
# Clone the repository
git clone <your-repo-url>
cd NIFIPILOT

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Mac/Linux
# venv\Scripts\activate   # On Windows

# Install dependencies
pip install -r requirements.txt

# Download TextBlob corpora (first time only)
python3 -m textblob.download_corpora
```

### Run

```bash
# Start the server
python3 backend/app.py
```

Visit **http://localhost:5000** in your browser.

---

## 🚀 Deploy (Render)

This repo includes a `render.yaml` blueprint for easy deployment on Render.

### Before Deploying

1. **Rotate your FMP API key** if it was ever shared or exposed.
2. Set your real `FMP_API_KEY` only in Render environment variables (not in git).

### Deploy Steps (Blueprint)

1. Push this repo to GitHub.
2. In Render, click **New** → **Blueprint**.
3. Select your repository.
4. Render will read `render.yaml` and create the web service.
5. In Render service settings, add:
   - `FMP_API_KEY` = your rotated FMP key

### Local Environment Example

Use `.env.example` as a template:

```bash
cp .env.example .env
```

Then edit `.env` and set your real `FMP_API_KEY`.

---

## 📁 Project Structure

```
NIFIPILOT/
├── backend/
│   ├── __init__.py
│   ├── app.py                    # Flask API server
│   ├── services/
│   │   ├── __init__.py
│   │   ├── stock_data.py         # Stock data & SEC filing service
│   │   ├── news_service.py       # News fetching & sentiment analysis
│   │   └── analysis_engine.py    # Core analysis & recommendation engine
│   └── utils/
│       └── __init__.py
├── frontend/
│   ├── index.html                # Main HTML
│   ├── css/
│   │   └── styles.css            # Premium dark-mode design system
│   ├── js/
│   │   └── app.js                # Frontend application logic
│   └── assets/
├── data/
│   └── cache/
├── requirements.txt
├── .gitignore
└── README.md
```

---

## 🔌 API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/search?q={query}` | Search stocks by name/ticker |
| `GET` | `/api/analyze/{symbol}` | Full comprehensive analysis |
| `GET` | `/api/company/{symbol}` | Company information |
| `GET` | `/api/financials/{symbol}` | Quarterly financial statements |
| `GET` | `/api/prices/{symbol}?period=5y` | Historical price data |
| `GET` | `/api/news/{symbol}` | News + sentiment analysis |
| `GET` | `/api/sec-filings/{symbol}` | SEC filing links |

---

## 📊 How Recommendations Work

1. **Fundamental Analysis (30%)** — Evaluates profitability, efficiency, liquidity, and cash flow
2. **Technical Analysis (25%)** — Moving average crossovers, RSI, MACD, volatility
3. **Sentiment Analysis (15%)** — NLP on recent business news articles
4. **Valuation Analysis (15%)** — P/E, P/B, analyst consensus, dividend yield
5. **Growth Analysis (10%)** — Revenue/earnings growth trends from quarterly statements
6. **Institutional Flow (5%)** — Institutional ownership changes, insider buying/selling

The weighted score (0-100) maps to recommendations:
- **70+ with >10% upside** → 🟢 STRONG BUY
- **60+ or 50+ with >15% upside** → 🟢 BUY
- **45-60** → 🟡 HOLD
- **35-45** → 🔴 SELL
- **<35** → 🔴 STRONG SELL

---

## ⚠️ Disclaimer

This platform is for **informational purposes only** and does not constitute financial advice. Past performance does not guarantee future results. Always consult with a qualified financial advisor before making investment decisions.

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.
