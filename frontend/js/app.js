/**
 * Kautilya — AI Stock Analyzer Frontend
 * Main application logic
 */

// Auto-detect environment for API routing
const isLocal = window.location.hostname === 'localhost' || window.location.protocol === 'file:';
const API_BASE = isLocal ? 'http://localhost:5000/api' : window.location.origin + '/api';

// State
let currentData = null;
let priceChart = null;
let allHistoricalData = [];
let portfolioBucket = JSON.parse(localStorage.getItem('kautilya-bucket') || '[]');

// ============================================
// INITIALIZATION
// ============================================

document.addEventListener('DOMContentLoaded', () => {
    initSearch();
    initQuickPicks();
    initNavigation();
    initBucket();
});

function initSearch() {
    const searchInput = document.getElementById('stock-search-input');
    const searchBtn = document.getElementById('search-btn');

    searchInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
            e.preventDefault();
            performSearch();
        }
    });

    searchBtn.addEventListener('click', () => performSearch());

    // Debounced search suggestions
    let debounceTimer;
    searchInput.addEventListener('input', () => {
        clearTimeout(debounceTimer);
        debounceTimer = setTimeout(() => {
            const query = searchInput.value.trim();
            if (query.length >= 1) {
                fetchSuggestions(query);
            } else {
                hideSuggestions();
            }
        }, 300);
    });

    // Hide suggestions on click outside
    document.addEventListener('click', (e) => {
        if (!e.target.closest('.search-container')) {
            hideSuggestions();
        }
    });
}

function initQuickPicks() {
    document.querySelectorAll('.quick-pick-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            const symbol = btn.dataset.symbol;
            document.getElementById('stock-search-input').value = symbol;
            analyzeStock(symbol);
        });
    });
}

function initNavigation() {
    const logo = document.getElementById('logo');
    logo.addEventListener('click', () => {
        showSection('hero');
    });

    document.querySelectorAll('.nav-link').forEach(link => {
        link.addEventListener('click', (e) => {
            e.preventDefault();
            const section = link.dataset.section;
            if (section === 'search') {
                showSection('hero');
            } else if (section === 'insights') {
                showSection('insights');
                fetchMarketInsights();
            } else if (section === 'bucket') {
                showSection('bucket');
                renderPortfolioList();
            }
        });
    });
}

// ============================================
// SEARCH & SUGGESTIONS
// ============================================

async function fetchSuggestions(query) {
    try {
        const res = await fetch(`${API_BASE}/search?q=${encodeURIComponent(query)}`);
        const data = await res.json();
        if (data.results && data.results.length > 0) {
            showSuggestions(data.results);
        } else {
            hideSuggestions();
        }
    } catch (e) {
        console.error('Search error:', e);
    }
}

function showSuggestions(results) {
    const container = document.getElementById('search-suggestions');
    container.innerHTML = results.slice(0, 5).map(r => `
        <div class="suggestion-item" data-symbol="${r.symbol}">
            <div>
                <span class="suggestion-symbol">${r.symbol}</span>
                <span class="suggestion-name">${r.name || ''}</span>
            </div>
            <span class="suggestion-exchange">${r.exchange || ''}</span>
        </div>
    `).join('');

    container.classList.remove('hidden');

    container.querySelectorAll('.suggestion-item').forEach(item => {
        item.addEventListener('click', () => {
            const symbol = item.dataset.symbol;
            document.getElementById('stock-search-input').value = symbol;
            hideSuggestions();
            analyzeStock(symbol);
        });
    });
}

function hideSuggestions() {
    document.getElementById('search-suggestions').classList.add('hidden');
}

function performSearch() {
    const query = document.getElementById('stock-search-input').value.trim();
    if (!query) return;
    hideSuggestions();
    analyzeStock(query.toUpperCase());
}

// ============================================
// ANALYSIS
// ============================================

async function analyzeStock(symbol) {
    showSection('loading');
    document.getElementById('loading-symbol').textContent = symbol;

    // Animate loading steps — each step takes ~4-6s on the backend now
    const steps = ['step-financials', 'step-technicals', 'step-news', 'step-institutional', 'step-targets'];
    let currentStep = 0;

    const stepInterval = setInterval(() => {
        if (currentStep > 0) {
            document.getElementById(steps[currentStep - 1]).classList.remove('active');
            document.getElementById(steps[currentStep - 1]).classList.add('done');
        }
        if (currentStep < steps.length) {
            document.getElementById(steps[currentStep]).classList.add('active');
            const statusTexts = [
                'Fetching all historical financial data from SEC filings...',
                'Computing RSI, MACD, moving averages & volatility...',
                'Analyzing business news sentiment from multiple sources...',
                'Reviewing institutional holdings & insider activity...',
                'Generating dynamic price targets & recommendation...',
            ];
            document.getElementById('loading-status').textContent = statusTexts[currentStep];
            currentStep++;
        } else {
            document.getElementById('loading-status').textContent = 'Finalizing analysis — almost done...';
        }
    }, 4000);

    // Timeout after 2 minutes
    const timeout = setTimeout(() => {
        clearInterval(stepInterval);
        showError(symbol, 'Analysis took too long. Yahoo Finance may be rate limiting requests. Please wait a minute and try again.');
    }, 120000);

    try {
        const res = await fetch(`${API_BASE}/analyze/${encodeURIComponent(symbol)}`);

        // Immediate cleanup
        clearInterval(stepInterval);
        clearTimeout(timeout);

        if (!res.ok) {
            let errMsg = 'Analysis failed';
            try {
                const errData = await res.json();
                errMsg = errData.error || errMsg;
            } catch (e) { }
            throw new Error(errMsg);
        }

        const data = await res.json();

        // Robust data validation
        if (!data || !data.analysis) {
            throw new Error('Received malformed data from server');
        }

        if (data.analysis.error) {
            throw new Error(data.analysis.error);
        }

        currentData = data;
        allHistoricalData = data.historicalPrices?.data || [];

        // Success: Mark metrics done and render
        steps.forEach(s => {
            document.getElementById(s).classList.remove('active');
            document.getElementById(s).classList.add('done');
        });

        renderResults(data);
        showSection('results');

    } catch (err) {
        clearInterval(stepInterval);
        clearTimeout(timeout);
        console.error('Analysis failed:', err);
        showError(symbol, err.message);
    }
}

// ============================================
// RENDERING
// ============================================

function renderResults(data) {
    const analysis = data.analysis;
    const info = data.companyInfo;

    renderCompanyHeader(info);
    renderRecommendation(analysis);
    renderPriceTargets(analysis.priceTargets);
    renderAlphaSignals(analysis);
    renderScores(analysis.scores);
    renderChart(data.historicalPrices);
    renderKeyMetrics(analysis.keyMetrics);
    renderTechnicalIndicators(analysis.scores.technical);
    renderNewsSentiment(data.news);
    renderRisks(analysis.riskFactors);
    renderSecFilings(data.secFilings);
    renderHolders(data.institutionalHolders, data.insiderTransactions);
    renderEarnings(analysis.earningsSummary);
    renderFinancialStatements(data.financials);
    initStatementTabs(data.financials);
    initChartPeriods();
}

function renderCompanyHeader(info) {
    document.getElementById('company-name').innerHTML = `
        ${info.name || info.symbol} <span class="header-symbol-inline">${info.symbol}</span>
    `;
    document.getElementById('company-sector').textContent = `${info.sector || ''} • ${info.industry || ''} • ${info.exchange || ''}`;

    const currentPrice = info.currentPrice || info.previousClose || 0;
    document.getElementById('current-price').textContent = `$${currentPrice.toFixed(2)}`;

    const change = currentPrice - (info.previousClose || currentPrice);
    const changePct = info.previousClose ? ((change / info.previousClose) * 100) : 0;
    const priceChangeEl = document.getElementById('price-change');
    priceChangeEl.textContent = `${change >= 0 ? '+' : ''}$${change.toFixed(2)} (${changePct >= 0 ? '+' : ''}${changePct.toFixed(2)}%)`;
    priceChangeEl.className = `price-change ${change >= 0 ? 'positive' : 'negative'}`;
}

function renderRecommendation(analysis) {
    const rec = analysis.recommendation || { action: 'UNKNOWN', color: '#999', reasoning: 'Insufficient data' };
    const badge = document.getElementById('rec-badge');
    badge.style.background = (rec.color || '#999') + '22';
    badge.style.color = rec.color || '#999';
    badge.style.border = `2px solid ${rec.color || '#999'}`;
    document.getElementById('rec-action').textContent = rec.action;

    // Score ring
    const score = analysis.overallScore;
    const circle = document.getElementById('score-circle');
    const circumference = 2 * Math.PI * 54;
    const offset = circumference - (score / 100) * circumference;

    const scoreColor = score >= 70 ? '#10b981' : score >= 50 ? '#f59e0b' : '#f43f5e';
    circle.style.stroke = scoreColor;

    // Animate score ring
    setTimeout(() => {
        circle.style.transition = 'stroke-dashoffset 1.5s ease-out';
        circle.style.strokeDashoffset = offset;
    }, 100);

    document.getElementById('rec-score-value').textContent = Math.round(score);
    document.getElementById('rec-score-value').style.color = scoreColor;

    document.getElementById('rec-reasoning').textContent = rec.reasoning;
    document.getElementById('rec-date').textContent = `Analysis Date: ${analysis.analysisDate}`;
}

function renderPriceTargets(targets) {
    const grid = document.getElementById('price-targets-grid');
    const periods = [
        { key: '30day', label: '30-Day Target', icon: '📅' },
        { key: '6month', label: '6-Month Target', icon: '📊' },
        { key: '1year', label: '1-Year Target', icon: '🎯' },
    ];

    grid.innerHTML = periods.map(p => {
        const t = targets[p.key] || {};
        const upsideClass = (t.upside || 0) >= 0 ? 'positive' : 'negative';
        const confidenceClass = `confidence-${t.confidence || 'medium'}`;
        return `
            <div class="target-card animate-in">
                <div class="target-period">${p.icon} ${p.label}</div>
                <div class="target-price">$${(t.target || 0).toFixed(2)}</div>
                <div class="target-upside ${upsideClass}">
                    ${(t.upside || 0) >= 0 ? '▲' : '▼'} ${(t.upside || 0).toFixed(2)}%
                </div>
                <div class="target-range">Range: $${(t.low || 0).toFixed(2)} — $${(t.high || 0).toFixed(2)}</div>
                <span class="target-confidence ${confidenceClass}">${t.confidence || 'N/A'} confidence</span>
            </div>
        `;
    }).join('');
}

function renderAlphaSignals(analysis) {
    const container = document.getElementById('alpha-signals-container');
    const badge = document.getElementById('conviction-badge');
    const signals = analysis.alphaSignals || [];

    // Render Badge
    if (analysis.convictionScore) {
        badge.textContent = `${analysis.convictionLabel} (Conviction: ${analysis.convictionScore}%)`;
        badge.style.display = 'inline-block';
    } else {
        badge.style.display = 'none';
    }

    if (signals.length === 0) {
        container.innerHTML = '<div class="alpha-empty-state">No specific high-conviction alpha patterns detected for this quarter.</div>';
        return;
    }

    container.innerHTML = signals.map(s => `
        <div class="alpha-card animate-in">
            <div class="alpha-header">
                <span class="alpha-type">${s.type}</span>
                <span class="alpha-strength">${s.strength}% Intensity</span>
            </div>
            <h4 class="alpha-title">${s.signal} Development Detected</h4>
            <p class="alpha-detail">${s.detail}</p>
            <div class="alpha-edge">
                <strong>Intelligence Edge:</strong> ${s.edge}
            </div>
        </div>
    `).join('');
}

function renderScores(scores) {
    const grid = document.getElementById('scores-grid');
    const scoreNames = {
        fundamental: { label: 'Fundamental', icon: '📋' },
        technical: { label: 'Technical', icon: '📈' },
        sentiment: { label: 'Sentiment', icon: '💬' },
        valuation: { label: 'Valuation', icon: '💎' },
        growth: { label: 'Growth', icon: '🚀' },
        institutional: { label: 'Institutional', icon: '🏛️' },
    };

    grid.innerHTML = Object.entries(scores).map(([key, score]) => {
        const info = scoreNames[key] || { label: key, icon: '📊' };
        const color = score.score >= 70 ? 'var(--accent-emerald)' : score.score >= 50 ? 'var(--accent-amber)' : 'var(--accent-rose)';
        const factors = (score.factors || []).slice(0, 4);

        return `
            <div class="score-card animate-in">
                <div class="score-card-header">
                    <span class="score-card-title">${info.icon} ${info.label}</span>
                    <span class="score-value" style="color: ${color}">${Math.round(score.score)}</span>
                </div>
                <div class="score-bar">
                    <div class="score-bar-fill" style="width: ${score.score}%; background: ${color}"></div>
                </div>
                <div class="score-label">${score.label}</div>
                <div class="score-factors">
                    ${factors.map(f => `
                        <div class="factor-item">
                            <span class="factor-name">${f.factor}</span>
                            <span class="factor-value factor-${f.impact}">${f.value}</span>
                        </div>
                    `).join('')}
                </div>
            </div>
        `;
    }).join('');
}

function renderChart(historicalData) {
    const canvas = document.getElementById('price-chart');
    const data = historicalData?.data || [];

    if (priceChart) {
        priceChart.destroy();
    }

    if (data.length === 0) {
        canvas.parentElement.innerHTML = '<p style="text-align:center; color:var(--text-muted); padding:40px;">No historical data available</p>';
        return;
    }

    const labels = data.map(d => d.date);
    const prices = data.map(d => d.close);
    const volumes = data.map(d => d.volume);

    const ctx = canvas.getContext('2d');

    // Create gradient
    const gradient = ctx.createLinearGradient(0, 0, 0, 400);
    gradient.addColorStop(0, 'rgba(99, 102, 241, 0.3)');
    gradient.addColorStop(1, 'rgba(99, 102, 241, 0.01)');

    priceChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: 'Price',
                data: prices,
                borderColor: '#6366f1',
                backgroundColor: gradient,
                borderWidth: 2,
                fill: true,
                tension: 0.2,
                pointRadius: 0,
                pointHoverRadius: 6,
                pointHoverBackgroundColor: '#6366f1',
                pointHoverBorderColor: '#fff',
                pointHoverBorderWidth: 2,
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: {
                intersect: false,
                mode: 'index',
            },
            plugins: {
                legend: {
                    display: false,
                },
                tooltip: {
                    backgroundColor: 'rgba(18, 18, 26, 0.95)',
                    titleColor: '#f1f1f5',
                    bodyColor: '#a1a1b5',
                    borderColor: 'rgba(99, 102, 241, 0.3)',
                    borderWidth: 1,
                    padding: 12,
                    displayColors: false,
                    callbacks: {
                        title: (items) => items[0].label,
                        label: (item) => `$${item.parsed.y.toFixed(2)}`,
                    }
                }
            },
            scales: {
                x: {
                    grid: {
                        color: 'rgba(255,255,255,0.04)',
                    },
                    ticks: {
                        color: '#6b6b80',
                        font: { family: 'JetBrains Mono', size: 10 },
                        maxTicksLimit: 12,
                        maxRotation: 0,
                    },
                    border: { display: false },
                },
                y: {
                    position: 'right',
                    grid: {
                        color: 'rgba(255,255,255,0.04)',
                    },
                    ticks: {
                        color: '#6b6b80',
                        font: { family: 'JetBrains Mono', size: 10 },
                        callback: (val) => '$' + val.toFixed(0),
                    },
                    border: { display: false },
                }
            },
            elements: {
                line: {
                    borderJoinStyle: 'round',
                }
            }
        }
    });
}

function renderKeyMetrics(metrics) {
    const grid = document.getElementById('metrics-grid');
    const metricLabels = {
        marketCap: 'Market Cap',
        peRatio: 'P/E Ratio',
        forwardPE: 'Forward P/E',
        priceToBook: 'Price/Book',
        dividendYield: 'Dividend Yield',
        beta: 'Beta',
        profitMargin: 'Profit Margin',
        operatingMargin: 'Operating Margin',
        roe: 'Return on Equity',
        debtToEquity: 'Debt/Equity',
        currentRatio: 'Current Ratio',
        revenueGrowth: 'Revenue Growth',
        earningsGrowth: 'Earnings Growth',
        freeCashFlow: 'Free Cash Flow',
    };

    grid.innerHTML = Object.entries(metrics).map(([key, val]) => {
        const label = metricLabels[key] || key;
        const displayVal = val === 'N/A' || val === 0 || val === '0' ? 'N/A' :
            typeof val === 'number' ? val.toFixed(2) : val;
        return `
            <div class="metric-card animate-in">
                <div class="metric-label">${label}</div>
                <div class="metric-value">${displayVal}</div>
            </div>
        `;
    }).join('');
}

function renderTechnicalIndicators(technical) {
    const grid = document.getElementById('indicators-grid');
    const indicators = technical.indicators || {};

    const indicatorLabels = {
        sma20: 'SMA 20',
        sma50: 'SMA 50',
        sma200: 'SMA 200',
        rsi: 'RSI (14)',
        macd: 'MACD',
        volatility: 'Volatility (Ann.)',
        volumeRatio: 'Volume Ratio',
        week52Position: '52W Position',
        support: 'Support Level',
        resistance: 'Resistance Level',
    };

    grid.innerHTML = Object.entries(indicators).map(([key, val]) => {
        const label = indicatorLabels[key] || key;
        let displayVal = typeof val === 'number' ?
            (key === 'volatility' || key === 'week52Position' ? (val * 100).toFixed(1) + '%' :
                key === 'volumeRatio' ? val.toFixed(2) + 'x' :
                    '$' + val.toFixed(2)) : val;

        if (key === 'rsi' || key === 'macd') {
            displayVal = typeof val === 'number' ? val.toFixed(2) : val;
        }

        let colorClass = '';
        if (key === 'rsi') {
            colorClass = val > 70 ? 'factor-negative' : val < 30 ? 'factor-positive' : '';
        }

        return `
            <div class="indicator-card animate-in">
                <span class="indicator-name">${label}</span>
                <span class="indicator-value ${colorClass}">${displayVal}</span>
            </div>
        `;
    }).join('');
}

function renderNewsSentiment(newsData) {
    const overview = document.getElementById('sentiment-overview');
    const sentiment = newsData?.sentiment || {};
    const articles = newsData?.articles || [];

    // Sentiment overview cards
    const sentimentColor = sentiment.overallSentiment > 0.1 ? 'var(--accent-emerald)' :
        sentiment.overallSentiment < -0.1 ? 'var(--accent-rose)' : 'var(--accent-amber)';

    overview.innerHTML = `
        <div class="sentiment-card">
            <div class="sentiment-card-label">Overall Sentiment</div>
            <div class="sentiment-card-value" style="color: ${sentimentColor}">
                ${sentiment.sentimentLabel ? sentiment.sentimentLabel.toUpperCase() : 'N/A'}
            </div>
        </div>
        <div class="sentiment-card">
            <div class="sentiment-card-label">Polarity Score</div>
            <div class="sentiment-card-value" style="color: ${sentimentColor}">
                ${(sentiment.overallSentiment || 0).toFixed(3)}
            </div>
        </div>
        <div class="sentiment-card">
            <div class="sentiment-card-label">Positive / Negative</div>
            <div class="sentiment-card-value">
                <span style="color: var(--accent-emerald)">${sentiment.positiveCount || 0}</span>
                <span style="color: var(--text-muted);font-size:0.9rem"> / </span>
                <span style="color: var(--accent-rose)">${sentiment.negativeCount || 0}</span>
            </div>
        </div>
        <div class="sentiment-card">
            <div class="sentiment-card-label">Articles Analyzed</div>
            <div class="sentiment-card-value">${sentiment.totalArticles || 0}</div>
        </div>
    `;

    // News cards
    const grid = document.getElementById('news-grid');
    const sentimentMap = {};
    (sentiment.articleSentiments || []).forEach(s => {
        sentimentMap[s.title] = s;
    });

    grid.innerHTML = articles.slice(0, 10).map(article => {
        const s = sentimentMap[article.title] || {};
        const badgeClass = s.label === 'positive' ? 'badge-positive' : s.label === 'negative' ? 'badge-negative' : 'badge-neutral';

        return `
            <a href="${article.url}" target="_blank" rel="noopener" class="news-card animate-in">
                <div class="news-card-header">
                    <span class="news-source">${article.source || 'News'}</span>
                    <span class="news-sentiment-badge ${badgeClass}">${s.label || 'neutral'}</span>
                </div>
                <h4 class="news-title">${article.title}</h4>
                <span class="news-date">${article.publishedDate || ''}</span>
            </a>
        `;
    }).join('');
}

function renderRisks(risks) {
    const grid = document.getElementById('risks-grid');
    const severityIcons = {
        high: '🔴',
        medium: '🟡',
        low: '🟢',
    };

    grid.innerHTML = risks.map(r => `
        <div class="risk-card risk-${r.severity} animate-in">
            <div class="risk-icon">${severityIcons[r.severity] || '⚪'}</div>
            <div class="risk-content">
                <h4>${r.risk}</h4>
                <p>${r.detail}</p>
            </div>
        </div>
    `).join('');
}

function renderSecFilings(filings) {
    const grid = document.getElementById('filings-grid');

    if (!filings || filings.length === 0) {
        grid.innerHTML = '<div class="filing-card"><p style="color:var(--text-muted)">No SEC filings data available</p></div>';
        return;
    }

    grid.innerHTML = filings.map(f => `
        <div class="filing-card animate-in">
            <div class="filing-type">${f.type}</div>
            <div class="filing-date">${f.date || 'Date N/A'}</div>
            ${f.title ? `<p style="font-size:0.82rem;color:var(--text-secondary);margin-bottom:var(--space-sm)">${f.title}</p>` : ''}
            ${f.edgarUrl ? `<a href="${f.edgarUrl}" target="_blank" rel="noopener" class="filing-link">
                View on SEC EDGAR →
            </a>` : ''}
        </div>
    `).join('');
}

function renderHolders(holders, insiderTx) {
    const grid = document.getElementById('holders-grid');
    let html = '';

    // Institutional holders
    if (holders && holders.length > 0) {
        html += holders.slice(0, 8).map(h => `
            <div class="holder-card animate-in">
                <div class="holder-name">${h.holder}</div>
                <div class="holder-details">
                    <div class="holder-detail">
                        <span class="label">Shares:</span>
                        <span class="value">${(h.shares || 0).toLocaleString()}</span>
                    </div>
                    <div class="holder-detail">
                        <span class="label">% Out:</span>
                        <span class="value">${(h.pctOut || 0).toFixed(2)}%</span>
                    </div>
                </div>
            </div>
        `).join('');
    }

    // Insider transactions
    if (insiderTx && insiderTx.length > 0) {
        html += '<div style="grid-column: 1/-1; margin-top:var(--space-md)"><h4 style="color:var(--text-secondary);font-size:0.9rem;margin-bottom:var(--space-md)">Recent Insider Transactions</h4></div>';
        html += insiderTx.slice(0, 6).map(tx => `
            <div class="holder-card animate-in">
                <div class="holder-name">${tx.insider || 'Insider'}</div>
                <div class="holder-details" style="flex-direction:column;gap:4px">
                    <div class="holder-detail">
                        <span class="label">Type:</span>
                        <span class="value">${tx.transaction || 'N/A'}</span>
                    </div>
                    <div class="holder-detail">
                        <span class="label">Date:</span>
                        <span class="value">${tx.date || 'N/A'}</span>
                    </div>
                </div>
            </div>
        `).join('');
    }

    if (!html) {
        html = '<div class="holder-card"><p style="color:var(--text-muted)">No institutional or insider data available</p></div>';
    }

    grid.innerHTML = html;
}

function renderEarnings(earnings) {
    const grid = document.getElementById('earnings-grid');

    grid.innerHTML = `
        <div class="earnings-card animate-in">
            <div class="earnings-label">Quarters Tracked</div>
            <div class="earnings-value">${earnings.totalQuarters}</div>
        </div>
        <div class="earnings-card animate-in">
            <div class="earnings-label">EPS Beats</div>
            <div class="earnings-value" style="color:var(--accent-emerald)">${earnings.beats}</div>
        </div>
        <div class="earnings-card animate-in">
            <div class="earnings-label">EPS Misses</div>
            <div class="earnings-value" style="color:var(--accent-rose)">${earnings.misses}</div>
        </div>
        <div class="earnings-card animate-in">
            <div class="earnings-label">Beat Rate</div>
            <div class="earnings-value" style="color:var(--accent-indigo)">${earnings.beatRate}</div>
        </div>
    `;
}

function renderFinancialStatements(financials, type = 'income') {
    const container = document.getElementById('financials-table-container');

    let data;
    let title;
    switch (type) {
        case 'balance':
            data = financials?.balanceSheet?.quarterly || [];
            title = 'Balance Sheet';
            break;
        case 'cashflow':
            data = financials?.cashFlow?.quarterly || [];
            title = 'Cash Flow Statement';
            break;
        default:
            data = financials?.incomeStatement?.quarterly || [];
            title = 'Income Statement';
    }

    if (!data || data.length === 0) {
        container.innerHTML = '<p style="padding:20px;color:var(--text-muted);text-align:center">No quarterly data available for this statement</p>';
        return;
    }

    // Get periods and build table
    const periods = data.map(d => d.period);

    // Collect all row keys (financial line items)
    const allKeys = new Set();
    data.forEach(d => {
        Object.keys(d).forEach(k => {
            if (k !== 'period') allKeys.add(k);
        });
    });

    // Filter for important rows (top 20)
    const importantKeys = Array.from(allKeys).slice(0, 25);

    const formatVal = (val) => {
        if (val === null || val === undefined) return '—';
        const num = Number(val);
        if (isNaN(num)) return val;
        if (Math.abs(num) >= 1e9) return `$${(num / 1e9).toFixed(2)}B`;
        if (Math.abs(num) >= 1e6) return `$${(num / 1e6).toFixed(1)}M`;
        if (Math.abs(num) >= 1e3) return `$${(num / 1e3).toFixed(1)}K`;
        return `$${num.toFixed(0)}`;
    };

    let html = `<table class="financials-table">
        <thead>
            <tr>
                <th style="min-width:200px">${title}</th>
                ${periods.map(p => `<th>${p}</th>`).join('')}
            </tr>
        </thead>
        <tbody>
    `;

    importantKeys.forEach(key => {
        html += `<tr>
            <td class="row-label" title="${key}">${key}</td>
            ${data.map(d => `<td>${formatVal(d[key])}</td>`).join('')}
        </tr>`;
    });

    html += '</tbody></table>';
    container.innerHTML = html;
}

// ============================================
// INTERACTIVE FEATURES
// ============================================

function initChartPeriods() {
    document.querySelectorAll('.period-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('.period-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');

            const period = btn.dataset.period;
            filterChartData(period);
        });
    });
}

let intradayCache = {};

async function filterChartData(period) {
    if (!currentData || !allHistoricalData || allHistoricalData.length === 0) return;

    if (['1d', '3d', '5d'].includes(period)) {
        const symbol = currentData.companyInfo?.symbol || currentData.symbol;
        if (!symbol) return;

        const cacheKey = `${symbol}_${period}`;
        let filtered;

        if (intradayCache[cacheKey]) {
            filtered = intradayCache[cacheKey];
        } else {
            try {
                const res = await fetch(`${API_BASE}/prices/${symbol}?period=${period}`);
                const data = await res.json();
                filtered = data.data || [];
                intradayCache[cacheKey] = filtered;
            } catch (e) {
                console.error("Failed to fetch intraday data", e);
                return;
            }
        }
        updateChartDisplay(filtered);
        return;
    }

    const now = new Date();
    let cutoff;
    switch (period) {
        case '1mo': cutoff = new Date(new Date().setMonth(now.getMonth() - 1)); break;
        case '3mo': cutoff = new Date(new Date().setMonth(now.getMonth() - 3)); break;
        case '6mo': cutoff = new Date(new Date().setMonth(now.getMonth() - 6)); break;
        case 'ytd': cutoff = new Date(now.getFullYear(), 0, 1); break;
        case '1y': cutoff = new Date(new Date().setFullYear(now.getFullYear() - 1)); break;
        case '3y': cutoff = new Date(new Date().setFullYear(now.getFullYear() - 3)); break;
        case '5y': cutoff = new Date(new Date().setFullYear(now.getFullYear() - 5)); break;
        case 'max': cutoff = new Date('1900-01-01'); break;
        default: cutoff = new Date('1900-01-01');
    }

    const filtered = allHistoricalData.filter(d => new Date(d.date) >= cutoff);
    updateChartDisplay(filtered);
}

function updateChartDisplay(filtered) {
    const perfDisplay = document.getElementById('chart-performance');
    if (filtered.length >= 2 && perfDisplay) {
        const startPrice = filtered[0].close;
        const endPrice = filtered[filtered.length - 1].close;
        const diff = endPrice - startPrice;
        const pct = (diff / startPrice) * 100;

        const sign = diff >= 0 ? '+' : '';
        const color = diff >= 0 ? '#10b981' : '#f43f5e';
        perfDisplay.style.color = color;
        perfDisplay.textContent = `${sign}$${Math.abs(diff).toFixed(2)} (${sign}${pct.toFixed(2)}%)`;
    } else if (perfDisplay) {
        perfDisplay.textContent = '';
    }

    if (filtered.length > 0 && priceChart) {
        priceChart.data.labels = filtered.map(d => d.date);
        priceChart.data.datasets[0].data = filtered.map(d => d.close);
        priceChart.update('none');
    }
}

function initStatementTabs(financials) {
    document.querySelectorAll('.stmt-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('.stmt-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            renderFinancialStatements(financials, btn.dataset.stmt);
        });
    });
}

// ============================================
// NAVIGATION & UI
// ============================================

function showSection(section) {
    document.getElementById('hero-section').classList.add('hidden');
    document.getElementById('loading-section').classList.add('hidden');
    document.getElementById('results-section').classList.add('hidden');

    // Reset loading steps
    document.querySelectorAll('.loading-step').forEach(s => {
        s.classList.remove('active', 'done');
    });

    switch (section) {
        case 'hero':
            document.getElementById('hero-section').classList.remove('hidden');
            break;
        case 'loading':
            document.getElementById('loading-section').classList.remove('hidden');
            break;
        case 'results':
            document.getElementById('results-section').classList.remove('hidden');
            window.scrollTo({ top: 0, behavior: 'smooth' });
            break;
        case 'bucket':
            document.getElementById('bucket-section').classList.remove('hidden');
            document.getElementById('hero-section').classList.add('hidden');
            document.getElementById('results-section').classList.add('hidden');
            break;
        case 'insights':
            document.getElementById('insights-section').classList.remove('hidden');
            document.getElementById('hero-section').classList.add('hidden');
            document.getElementById('results-section').classList.add('hidden');
            break;
    }
}

function showError(symbol, message) {
    showSection('results');
    document.getElementById('results-section').innerHTML = `
        <div style="
            display: flex; flex-direction: column; align-items: center; justify-content: center;
            min-height: 50vh; text-align: center; padding: 40px;
        ">
            <div style="
                font-size: 4rem; margin-bottom: 20px;
            ">⚠️</div>
            <h2 style="
                font-size: 1.5rem; color: var(--text-primary); margin-bottom: 12px;
            ">Analysis Failed</h2>
            <p style="
                color: var(--text-secondary); font-size: 0.95rem; max-width: 500px;
                line-height: 1.6; margin-bottom: 24px;
            ">Could not analyze <strong style="color:var(--accent-indigo)">${symbol}</strong>: ${message}</p>
            <button onclick="showSection('hero')" style="
                padding: 12px 32px; border-radius: 12px; border: 1px solid var(--accent-indigo);
                background: rgba(99,102,241,0.1); color: var(--accent-indigo);
                font-size: 0.95rem; cursor: pointer; transition: all 0.2s;
                font-family: inherit;
            " onmouseover="this.style.background='rgba(99,102,241,0.2)'"
               onmouseout="this.style.background='rgba(99,102,241,0.1)'"
            >← Try Again</button>
        </div>
    `;
}

// ============================================
// BUCKET / PORTFOLIO LOGIC
// ============================================

function initBucket() {
    const addBtn = document.getElementById('add-to-bucket-btn');
    if (!addBtn) return;

    addBtn.addEventListener('click', () => {
        const symbolInput = document.getElementById('portfolio-symbol');
        const avgPriceInput = document.getElementById('portfolio-avg-price');
        const qtyInput = document.getElementById('portfolio-qty');

        const symbol = symbolInput.value.trim().toUpperCase();
        const avgPrice = parseFloat(avgPriceInput.value);
        const qty = parseFloat(qtyInput.value);

        if (!symbol || isNaN(avgPrice) || isNaN(qty)) {
            alert('Please fill in all fields (Symbol, Avg Price, Qty)');
            return;
        }

        const newItem = { symbol, avgPrice, qty, id: Date.now() };
        portfolioBucket.push(newItem);
        localStorage.setItem('kautilya-bucket', JSON.stringify(portfolioBucket));

        // Clear inputs
        symbolInput.value = '';
        avgPriceInput.value = '';
        qtyInput.value = '';

        renderPortfolioList();
    });
}

function renderPortfolioList() {
    const list = document.getElementById('portfolio-list');
    if (!list) return;

    if (portfolioBucket.length === 0) {
        list.innerHTML = '<p style="text-align:center;color:var(--text-muted);margin-top:20px;font-size:0.9rem">Bucket is empty. Add a stock to start tracking.</p>';
        return;
    }

    list.innerHTML = portfolioBucket.map(item => `
        <div class="portfolio-item animate-in" onclick="analyzePortfolioItem('${item.symbol}', ${item.avgPrice}, ${item.qty}, ${item.id})">
            <div class="portfolio-item-header">
                <span class="portfolio-item-symbol">${item.symbol}</span>
                <span class="portfolio-item-qty">${item.qty} Shares</span>
                <button onclick="event.stopPropagation(); removeFromBucket(${item.id})" style="background:none;border:none;color:var(--accent-rose);cursor:pointer;font-size:1.1rem;padding:0 5px">×</button>
            </div>
            <div class="portfolio-item-stats">
                <span class="portfolio-item-avg">Avg: $${item.avgPrice.toFixed(2)}</span>
            </div>
        </div>
    `).join('');
}

function removeFromBucket(id) {
    portfolioBucket = portfolioBucket.filter(p => p.id !== id);
    localStorage.setItem('kautilya-bucket', JSON.stringify(portfolioBucket));
    renderPortfolioList();

    // If deleted item was active, reset view
    const view = document.getElementById('bucket-analysis-view');
    const placeholder = document.getElementById('bucket-analysis-placeholder');
    view.classList.add('hidden');
    placeholder.classList.remove('hidden');
}

async function analyzePortfolioItem(symbol, avgPrice, qty, id) {
    // UI state
    document.querySelectorAll('.portfolio-item').forEach(el => el.classList.remove('active'));
    // Find item element and mark active
    const items = document.querySelectorAll('.portfolio-item');
    portfolioBucket.forEach((p, idx) => {
        if (p.id === id) items[idx]?.classList.add('active');
    });

    const view = document.getElementById('bucket-analysis-view');
    const placeholder = document.getElementById('bucket-analysis-placeholder');

    placeholder.innerHTML = `
        <div style="display:flex; flex-direction:column; align-items:center; justify-content:center; height:100%">
            <div class="placeholder-icon" style="animation: pulse 2s infinite">🔄</div>
            <h3 class="placeholder-title">Analyzing ${symbol}...</h3>
            <p style="color:var(--text-muted)">Fetching latest financials and running portfolio risk calculations.</p>
        </div>
    `;
    placeholder.classList.remove('hidden');
    view.classList.add('hidden');

    try {
        const res = await fetch(`${API_BASE}/analyze/${encodeURIComponent(symbol)}`);
        if (!res.ok) throw new Error('Analysis failed');
        const data = await res.json();
        const info = data.companyInfo;
        const analysis = data.analysis;

        const currentPrice = info.currentPrice || info.previousClose;
        const invested = avgPrice * qty;
        const currentVal = currentPrice * qty;
        const pnl = currentVal - invested;
        const pnlPct = (pnl / invested) * 100;
        const colorClass = pnl >= 0 ? 'positive' : 'negative';

        // Recommendation logic
        let actionTitle = "Hold Position";
        let actionDesc = "Your current position is well-aligned with neutral market sentiment and current valuations.";

        if (analysis.overallScore >= 70) {
            actionTitle = "Increase Position";
            actionDesc = "Strong fundamental and technical indicators suggest significant upside potential. Consider adding to your stakes.";
        } else if (analysis.overallScore <= 45) {
            actionTitle = "Decrease / Trim Position";
            actionDesc = "Warning: Multiple bearish indicators and risks detected. Consider trimming your exposure to protect capital.";
        }

        view.innerHTML = `
            <div class="brief-header animate-in">
                <div>
                    <h2 style="font-size:1.8rem;margin-bottom:4px;color:var(--text-primary)">${info.name}</h2>
                    <p style="color:var(--text-muted)">Portfolio Analysis • ${symbol}</p>
                </div>
                <div class="brief-performance">
                    <span class="brief-pnl-label">Net Gain/Loss</span>
                    <span class="brief-pnl ${colorClass}">${pnl >= 0 ? '+' : ''}$${Math.abs(pnl).toFixed(2)}</span>
                    <span style="color:${pnl >= 0 ? 'var(--accent-emerald)' : 'var(--accent-rose)'};font-weight:700">
                        ${pnl >= 0 ? '▲' : '▼'} ${Math.abs(pnlPct).toFixed(2)}%
                    </span>
                </div>
            </div>

            <div class="brief-grid animate-in" style="animation-delay:0.1s">
                <div class="brief-card">
                    <div class="brief-card-title">✅ Strategic Pros</div>
                    <ul class="brief-list">
                        ${(analysis.scores.fundamental.factors || []).filter(f => f.impact === 'positive').slice(0, 3).map(f => `<li class="brief-list-item pros-item">${f.factor}</li>`).join('')}
                        ${(analysis.scores.technical.factors || []).filter(f => f.impact === 'positive').slice(0, 2).map(f => `<li class="brief-list-item pros-item">${f.factor}</li>`).join('')}
                    </ul>
                </div>
                <div class="brief-card">
                    <div class="brief-card-title">❌ Risks & Cons</div>
                    <ul class="brief-list">
                        ${analysis.riskFactors.slice(0, 4).map(r => `<li class="brief-list-item cons-item">${r.risk}</li>`).join('')}
                    </ul>
                </div>
            </div>

            <div class="brief-grid animate-in" style="animation-delay:0.2s">
                <div class="brief-card">
                    <div class="brief-card-title">🎯 Price Forecasts</div>
                    <div class="forecast-grid">
                        <div class="forecast-item">
                            <div class="forecast-label">6-Month Target</div>
                            <div class="forecast-val">$${analysis.priceTargets['6month'].target.toFixed(2)}</div>
                            <div style="font-size:0.75rem; color:${analysis.priceTargets['6month'].upside >= 0 ? 'var(--accent-emerald)' : 'var(--accent-rose)'}">
                                ${analysis.priceTargets['6month'].upside >= 0 ? '+' : ''}${analysis.priceTargets['6month'].upside.toFixed(1)}%
                            </div>
                        </div>
                        <div class="forecast-item">
                            <div class="forecast-label">1-Year Target</div>
                            <div class="forecast-val">$${analysis.priceTargets['1year'].target.toFixed(2)}</div>
                             <div style="font-size:0.75rem; color:${analysis.priceTargets['1year'].upside >= 0 ? 'var(--accent-emerald)' : 'var(--accent-rose)'}">
                                ${analysis.priceTargets['1year'].upside >= 0 ? '+' : ''}${analysis.priceTargets['1year'].upside.toFixed(1)}%
                            </div>
                        </div>
                    </div>
                </div>
                <div class="brief-card">
                    <div class="brief-card-title">🛡️ Risk Assessment</div>
                    <div class="risk-level" style="background:${analysis.overallScore >= 70 ? 'var(--accent-emerald)22' : 'var(--accent-rose)22'}; color:${analysis.overallScore >= 70 ? 'var(--accent-emerald)' : 'var(--accent-rose)'}">
                        ${analysis.overallScore >= 70 ? 'Low Risk Selection' : 'High Risk Exposure'}
                    </div>
                    <p style="font-size:0.85rem;color:var(--text-secondary); line-height:1.5; margin-top:8px">
                        Current volatility is ${analysis.scores.technical.indicators.volatility ? (analysis.scores.technical.indicators.volatility * 100).toFixed(1) : 'N/A'}% annually.
                        Major support detected at $${analysis.scores.technical.indicators.support?.toFixed(2) || 'N/A'}.
                    </p>
                </div>
            </div>

            <div class="action-banner animate-in" style="animation-delay:0.3s">
                <div class="action-title">💡 Position Advice: ${actionTitle}</div>
                <div class="action-desc">${actionDesc}</div>
            </div>
            
            <div style="margin-top:40px; text-align:center" class="animate-in" style="animation-delay:0.4s">
                <button onclick="analyzeStock('${symbol}')" class="search-btn" style="padding:12px 32px; border-radius:12px">View Deep Engine Report</button>
            </div>
        `;

        placeholder.classList.add('hidden');
        view.classList.remove('hidden');

    } catch (err) {
        console.error('Portfolio analysis failed:', err);
        placeholder.innerHTML = `<div class="placeholder-icon">⚠️</div><h3 class="placeholder-title">Analysis Error</h3><p style="color:var(--text-muted)">${err.message}</p>`;
    }
}

// ============================================
// MARKET INSIGHTS LOGIC
// ============================================

async function fetchMarketInsights() {
    const grid = document.getElementById('insights-grid');
    const loading = document.getElementById('insights-loading');

    grid.innerHTML = '';
    loading.classList.remove('hidden');

    try {
        const res = await fetch(`${API_BASE}/market-insights`);
        const data = await res.json();

        loading.classList.add('hidden');

        if (!data.insights || data.insights.length === 0) {
            grid.innerHTML = '<p style="grid-column: 1/-1; text-align:center; color:var(--text-muted)">No market insights available right now.</p>';
            return;
        }

        grid.innerHTML = data.insights.map((item, idx) => `
            <div class="insight-card ${item.sentiment} animate-in" style="animation-delay: ${idx * 0.1}s">
                <div class="insight-header">
                    <div class="insight-sectors">
                        ${item.sectors.map(s => `<span class="sector-tag">${s}</span>`).join('')}
                    </div>
                    <span class="insight-sentiment ${item.sentiment}">${item.sentiment}</span>
                </div>
                
                <h3 class="insight-title">${item.title}</h3>
                
                <p class="insight-description">
                    ${item.description}
                </p>
                
                <div class="insight-footer">
                    <div class="insight-stocks">
                        ${item.stocks.map(ticker => `<span class="stock-pill" onclick="analyzeStock('${ticker}')" style="cursor:pointer">${ticker}</span>`).join('')}
                    </div>
                    <a href="${item.url}" target="_blank" class="insight-link">Read Source →</a>
                </div>
            </div>
        `).join('');

    } catch (err) {
        console.error('Failed to fetch insights:', err);
        loading.innerHTML = `<div class="placeholder-icon">⚠️</div><h3>Intelligence Offline</h3><p>${err.message}</p>`;
    }
}
