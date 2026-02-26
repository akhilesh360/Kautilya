/**
 * Kautilya — AI Stock Analyzer Frontend
 * Main application logic
 */

// API routing: use same-origin by default so local runs work on any port (e.g. 5001/5100).
const API_BASE = window.location.protocol === 'file:'
    ? 'http://localhost:5000/api'
    : `${window.location.origin}/api`;

// State
let currentData = null;
let priceChart = null;
let allHistoricalData = [];
let portfolioBucket = JSON.parse(localStorage.getItem('kautilya-bucket') || '[]');
let resultsSectionTemplate = '';
let currentInsightsTab = 'news';
const SECTION_PATHS = {
    search: '/analyze',
    insights: '/insights',
    bucket: '/bucket',
    about: '/about',
    hero: '/analyze',
};

// ============================================
// INITIALIZATION
// ============================================

document.addEventListener('DOMContentLoaded', () => {
    const resultsSection = document.getElementById('results-section');
    if (resultsSection) {
        resultsSectionTemplate = resultsSection.innerHTML;
    }
    initSearch();
    initResultsQuickSearch();
    initQuickPicks();
    initNavigation();
    initInsightsTabs();
    initBucket();
    initPageRouting();
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

function initResultsQuickSearch() {
    // Delegated listeners survive results-section template restoration after error screens.
    document.addEventListener('click', (e) => {
        const btn = e.target.closest('#results-search-btn');
        if (!btn) return;
        const input = document.getElementById('results-search-input');
        const symbol = (input?.value || '').trim().toUpperCase();
        if (!symbol) return;
        analyzeStock(symbol);
    });

    document.addEventListener('keydown', (e) => {
        const input = e.target;
        if (!(input instanceof HTMLElement)) return;
        if (input.id !== 'results-search-input') return;
        if (e.key !== 'Enter') return;
        e.preventDefault();
        const symbol = (input.value || '').trim().toUpperCase();
        if (!symbol) return;
        analyzeStock(symbol);
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
        navigateToSection('search');
    });

    document.querySelectorAll('.nav-link').forEach(link => {
        link.addEventListener('click', (e) => {
            e.preventDefault();
            const section = link.dataset.section;
            navigateToSection(section || 'search');
        });
    });
}

function initPageRouting() {
    window.addEventListener('popstate', () => {
        showSectionFromPath(window.location.pathname, false);
    });
    showSectionFromPath(window.location.pathname, false);
}

function normalizePath(pathname) {
    if (!pathname) return '/analyze';
    let p = pathname.trim();
    if (p.length > 1 && p.endsWith('/')) p = p.slice(0, -1);
    return p || '/analyze';
}

function pathToSection(pathname) {
    const p = normalizePath(pathname).toLowerCase();
    if (p === '/' || p === '/analyze') return 'search';
    if (p === '/insights') return 'insights';
    if (p === '/bucket') return 'bucket';
    if (p === '/about') return 'about';
    return 'search';
}

function sectionToPath(section) {
    return SECTION_PATHS[section] || '/analyze';
}

function navigateToSection(section, push = true) {
    const path = sectionToPath(section);
    if (push && window.location.pathname !== path) {
        window.history.pushState({}, '', path);
    }
    showSectionFromPath(path, false);
}

function showSectionFromPath(pathname, pushState = false) {
    const section = pathToSection(pathname);
    if (pushState) {
        navigateToSection(section, true);
        return;
    }
    if (section === 'search') {
        showSection('hero');
    } else if (section === 'insights') {
        showSection('insights');
        loadInsightsTab(currentInsightsTab);
    } else if (section === 'bucket') {
        showSection('bucket');
        renderPortfolioList();
        renderPortfolioLongTermSummary();
    } else if (section === 'about') {
        showSection('about');
    }
}

function initInsightsTabs() {
    document.querySelectorAll('[data-insights-tab]').forEach((btn) => {
        btn.addEventListener('click', () => {
            const tab = btn.dataset.insightsTab || 'news';
            setInsightsTab(tab);
            loadInsightsTab(tab);
        });
    });
}

function setInsightsTab(tab) {
    currentInsightsTab = tab === 'gainers' ? 'gainers' : 'news';
    document.querySelectorAll('[data-insights-tab]').forEach((btn) => {
        btn.classList.toggle('active', btn.dataset.insightsTab === currentInsightsTab);
    });

    const newsPane = document.getElementById('insights-news-pane');
    const gainersPane = document.getElementById('insights-gainers-pane');
    const summary = document.getElementById('insights-summary');
    if (newsPane) newsPane.classList.toggle('hidden', currentInsightsTab !== 'news');
    if (gainersPane) gainersPane.classList.toggle('hidden', currentInsightsTab !== 'gainers');
    if (summary) summary.classList.toggle('hidden', currentInsightsTab !== 'news');
}

function loadInsightsTab(tab) {
    if (tab === 'gainers') {
        fetchTopGainersToday();
    } else {
        fetchMarketInsights();
    }
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
    restoreResultsSectionIfNeeded();

    const analysis = data.analysis;
    const info = data.companyInfo;

    renderCompanyHeader(info);
    renderRecommendation(analysis);
    renderScoreDiagnostics(analysis);
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

function restoreResultsSectionIfNeeded() {
    const resultsSection = document.getElementById('results-section');
    if (!resultsSection || !resultsSectionTemplate) return;

    // showError() replaces the entire section markup; restore the original template before rendering again.
    if (!document.getElementById('company-name') || !document.getElementById('scores-grid')) {
        resultsSection.innerHTML = resultsSectionTemplate;
        if (priceChart) {
            try { priceChart.destroy(); } catch (e) { }
            priceChart = null;
        }
    }
}

function renderCompanyHeader(info) {
    const quickInput = document.getElementById('results-search-input');
    if (quickInput && info?.symbol) {
        quickInput.value = info.symbol;
    }
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
    const displayAction = rec.action === 'NO TRADE' ? 'HOLD (Do Nothing)' : rec.action;
    const badge = document.getElementById('rec-badge');
    badge.style.background = (rec.color || '#999') + '22';
    badge.style.color = rec.color || '#999';
    badge.style.border = `2px solid ${rec.color || '#999'}`;
    document.getElementById('rec-action').textContent = displayAction;

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

    const reasoning = rec.reasoning || 'Insufficient data';
    document.getElementById('rec-reasoning').textContent = reasoning;
    const extraMeta = [];
    if (analysis.confidenceScore !== undefined) {
        extraMeta.push(`Confidence: ${analysis.confidenceScore} (${analysis.confidenceLabel || 'N/A'})`);
    }
    if (analysis.dataQuality?.score !== undefined) {
        extraMeta.push(`Data Quality: ${analysis.dataQuality.score} (${analysis.dataQuality.label || 'N/A'})`);
    }
    if (analysis.regime?.name) {
        extraMeta.push(`Regime: ${analysis.regime.name}`);
    }
    if (analysis.longTermQualityScore !== undefined) {
        extraMeta.push(`Investment View: ${analysis.longTermQualityLabel || 'N/A'} (${analysis.longTermQualityScore})`);
    }
    document.getElementById('rec-date').textContent = `Analysis Date: ${analysis.analysisDate}${extraMeta.length ? ' • ' + extraMeta.join(' • ') : ''}`;
}

function renderPriceTargets(targets) {
    const grid = document.getElementById('price-targets-grid');
    const periods = [
        { key: '30day', label: '30-Day Target' },
        { key: '6month', label: '6-Month Target' },
        { key: '1year', label: '1-Year Target' },
        { key: '5year', label: '5-Year Target' },
    ];

    grid.innerHTML = periods.map(p => {
        const t = targets[p.key] || {};
        const upsideClass = (t.upside || 0) >= 0 ? 'positive' : 'negative';
        const confidenceClass = `confidence-${t.confidence || 'medium'}`;
        return `
            <div class="target-card animate-in">
                <div class="target-period">${p.label}</div>
                <div class="target-price">$${(t.target || 0).toFixed(2)}</div>
                <div class="target-upside ${upsideClass}">
                    ${(t.upside || 0) >= 0 ? '▲' : '▼'} ${(t.upside || 0).toFixed(2)}%
                </div>
                <div class="target-range">Range: $${(t.low || 0).toFixed(2)} — $${(t.high || 0).toFixed(2)}</div>
                ${p.key === '5year' && t.benchmark ? `
                    <div class="target-range" style="margin-top:4px">
                        vs ${t.benchmark.benchmarkSymbol || 'Benchmark'}: ${(t.benchmark.relativeUpsideVsBenchmarkPct ?? 0) >= 0 ? '+' : ''}${(t.benchmark.relativeUpsideVsBenchmarkPct ?? 0).toFixed?.(2) ?? t.benchmark.relativeUpsideVsBenchmarkPct}% relative
                    </div>
                ` : ''}
                <span class="target-confidence ${confidenceClass}">${t.confidence || 'N/A'} confidence</span>
            </div>
        `;
    }).join('');
}

function renderScoreDiagnostics(analysis) {
    const panel = document.getElementById('score-diagnostics-panel');
    if (!panel) return;
    const diag = analysis?.scoreDiagnostics;
    if (!diag) {
        panel.classList.add('hidden');
        panel.innerHTML = '';
        return;
    }

    const drags = (diag.topDrags || []).map(d => `
        <div class="diag-item">
            <span class="diag-name">${d.factor}</span>
            <span class="diag-val factor-negative">${d.score} (${d.weightedDelta})</span>
        </div>
    `).join('');
    const supports = (diag.topSupports || []).map(d => `
        <div class="diag-item">
            <span class="diag-name">${d.factor}</span>
            <span class="diag-val factor-positive">${d.score} (+${d.weightedDelta})</span>
        </div>
    `).join('');
    const penalties = (diag.missingDataPenalties || []).map(p => `
        <li>${p.name}: ${p.status} (${p.detail || 'n/a'})</li>
    `).join('');
    const gates = (diag.gatingReasons || []).map(g => `<li>${g}</li>`).join('');

    panel.innerHTML = `
        <div class="score-diagnostics-header">
            <h3>Why Score Is ${diag.isLowScore ? 'Low' : 'What Drives the Score'}</h3>
            <p>${diag.headline || ''}</p>
        </div>
        <div class="score-diagnostics-grid">
            <div class="score-diagnostics-card">
                <div class="score-diagnostics-title">Factor Drags</div>
                ${drags || '<div style="color:var(--text-muted)">No major negative drags.</div>'}
            </div>
            <div class="score-diagnostics-card">
                <div class="score-diagnostics-title">Factor Supports</div>
                ${supports || '<div style="color:var(--text-muted)">No strong supports yet.</div>'}
            </div>
            <div class="score-diagnostics-card">
                <div class="score-diagnostics-title">Data / Coverage Penalties</div>
                ${penalties ? `<ul class="diag-list">${penalties}</ul>` : '<div style="color:var(--text-muted)">No material data penalties.</div>'}
            </div>
            <div class="score-diagnostics-card">
                <div class="score-diagnostics-title">Risk Gate Notes</div>
                ${gates ? `<ul class="diag-list">${gates}</ul>` : '<div style="color:var(--text-muted)">No active risk gate overrides.</div>'}
            </div>
        </div>
    `;
    panel.classList.remove('hidden');
}

function renderAlphaSignals(analysis) {
    const container = document.getElementById('alpha-signals-container');
    const badge = document.getElementById('conviction-badge');
    const signals = analysis.alphaSignals || [];
    const filingEdge = analysis.secEdge || null;

    // Render Badge
    if (analysis.convictionScore) {
        badge.textContent = `${analysis.convictionLabel} (Conviction: ${analysis.convictionScore}%)`;
        badge.style.display = 'inline-block';
    } else {
        badge.style.display = 'none';
    }

    const filingPanel = filingEdge && !filingEdge.error ? `
        <div class="alpha-card animate-in">
            <div class="alpha-header">
                <span class="alpha-type">Filing Edge (10-K/10-Q)</span>
                <span class="alpha-strength" style="color:${(filingEdge.edgeScore || 0) >= 0 ? 'var(--accent-emerald)' : 'var(--accent-rose)'}">
                    ${typeof filingEdge.edgeScore === 'number' ? (filingEdge.edgeScore >= 0 ? '+' : '') + filingEdge.edgeScore.toFixed(1) : 'N/A'}
                </span>
            </div>
            <h4 class="alpha-title">${escapeHtml(filingEdge.edgeLabel || 'Filing Drift Analysis')}</h4>
            <p class="alpha-detail">${escapeHtml(filingEdge.edge_summary || 'No filing summary available.')}</p>
            ${Array.isArray(filingEdge.filingSignals) && filingEdge.filingSignals.length ? `
                <div class="alpha-edge">
                    <strong>Key Filing Signals:</strong>
                    ${filingEdge.filingSignals.slice(0, 4).map(f => `${escapeHtml(f.section || 'section')}: ${escapeHtml(f.detail || f.type || 'signal')}`).join(' • ')}
                </div>
            ` : ''}
        </div>
    ` : '';

    const alphaCards = signals.map(s => `
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

    if (!filingPanel && signals.length === 0) {
        container.innerHTML = '<div class="alpha-empty-state">No specific high-conviction alpha patterns or filing-edge shifts detected for this period.</div>';
        return;
    }

    container.innerHTML = `${filingPanel}${alphaCards}`;
}

function renderScores(scores) {
    const grid = document.getElementById('scores-grid');
    const scoreNames = {
        fundamental: { label: 'Fundamental' },
        technical: { label: 'Technical' },
        sentiment: { label: 'Sentiment' },
        valuation: { label: 'Valuation' },
        growth: { label: 'Growth' },
        institutional: { label: 'Institutional' },
    };

    grid.innerHTML = Object.entries(scores).map(([key, score]) => {
        const info = scoreNames[key] || { label: key };
        const color = score.score >= 70 ? 'var(--accent-emerald)' : score.score >= 50 ? 'var(--accent-amber)' : 'var(--accent-rose)';
        const factors = (score.factors || []).slice(0, 4);

        return `
            <div class="score-card animate-in">
                <div class="score-card-header">
                    <span class="score-card-title">${info.label}</span>
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
    const firstPrice = Number(prices[0] || 0);
    const lastPrice = Number(prices[prices.length - 1] || 0);
    const isUp = lastPrice >= firstPrice;
    const lineColor = isUp ? '#10b981' : '#f43f5e';
    const fillTop = isUp ? 'rgba(16, 185, 129, 0.18)' : 'rgba(244, 63, 94, 0.16)';
    const fillBottom = isUp ? 'rgba(16, 185, 129, 0.02)' : 'rgba(244, 63, 94, 0.02)';

    const ctx = canvas.getContext('2d');

    // Create gradient
    const gradient = ctx.createLinearGradient(0, 0, 0, 400);
    gradient.addColorStop(0, fillTop);
    gradient.addColorStop(1, fillBottom);

    priceChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: 'Price',
                data: prices,
                borderColor: lineColor,
                backgroundColor: gradient,
                borderWidth: 2,
                fill: true,
                tension: 0.2,
                pointRadius: 0,
                pointHoverRadius: 6,
                pointHoverBackgroundColor: lineColor,
                pointHoverBorderColor: '#ffffff',
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
                    backgroundColor: 'rgba(255, 255, 255, 0.98)',
                    titleColor: '#0f172a',
                    bodyColor: '#475569',
                    borderColor: isUp ? 'rgba(16, 185, 129, 0.25)' : 'rgba(244, 63, 94, 0.25)',
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
                        color: 'rgba(15,23,42,0.05)',
                    },
                    ticks: {
                        color: '#64748b',
                        font: { family: 'JetBrains Mono', size: 10 },
                        maxTicksLimit: 12,
                        maxRotation: 0,
                    },
                    border: { display: false },
                },
                y: {
                    position: 'right',
                    grid: {
                        color: 'rgba(15,23,42,0.05)',
                    },
                    ticks: {
                        color: '#64748b',
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

    const parseChartDate = (value) => {
        if (!value) return null;
        // Handles YYYY-MM-DD and YYYY-MM-DD HH:mm consistently across browsers.
        const normalized = String(value).includes(' ') ? String(value).replace(' ', 'T') : `${value}T00:00:00`;
        const dt = new Date(normalized);
        return Number.isNaN(dt.getTime()) ? null : dt;
    };

    let filtered = allHistoricalData.filter(d => {
        const dt = parseChartDate(d.date);
        return dt && dt >= cutoff;
    });
    if (!filtered.length && ['3y', '5y', 'max'].includes(period)) {
        // Fallback to available history instead of blank chart if parsing or range is limited.
        filtered = allHistoricalData.slice();
    }
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
        const startPrice = Number(filtered[0].close || 0);
        const endPrice = Number(filtered[filtered.length - 1].close || 0);
        const isUp = endPrice >= startPrice;
        const lineColor = isUp ? '#10b981' : '#f43f5e';
        const ctx = priceChart.ctx;
        let gradient = lineColor;
        if (ctx && typeof ctx.createLinearGradient === 'function') {
            gradient = ctx.createLinearGradient(0, 0, 0, 400);
            gradient.addColorStop(0, isUp ? 'rgba(16, 185, 129, 0.18)' : 'rgba(244, 63, 94, 0.16)');
            gradient.addColorStop(1, isUp ? 'rgba(16, 185, 129, 0.02)' : 'rgba(244, 63, 94, 0.02)');
        }
        priceChart.data.labels = filtered.map(d => d.date);
        priceChart.data.datasets[0].data = filtered.map(d => d.close);
        priceChart.data.datasets[0].borderColor = lineColor;
        priceChart.data.datasets[0].backgroundColor = gradient;
        priceChart.data.datasets[0].pointHoverBackgroundColor = lineColor;
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
    ['hero-section', 'loading-section', 'results-section', 'bucket-section', 'insights-section', 'about-section']
        .forEach((id) => {
            const el = document.getElementById(id);
            if (el) el.classList.add('hidden');
        });

    document.querySelectorAll('.nav-link').forEach((link) => {
        const target = link.dataset.section;
        const isActive =
            (section === 'hero' && target === 'search') ||
            (section === 'results' && target === 'search') ||
            target === section;
        link.classList.toggle('active', !!isActive);
    });

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
            break;
        case 'insights':
            document.getElementById('insights-section').classList.remove('hidden');
            break;
        case 'about':
            document.getElementById('about-section').classList.remove('hidden');
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

    renderPortfolioLongTermSummary();
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

    renderPortfolioLongTermSummary();
}

async function renderPortfolioLongTermSummary() {
    const container = document.getElementById('portfolio-longterm-summary');
    if (!container) return;
    if (!portfolioBucket.length) {
        container.classList.add('hidden');
        container.innerHTML = '';
        return;
    }

    container.classList.remove('hidden');
    container.innerHTML = `<div class="placeholder-icon" style="font-size:1.4rem">⏳</div><p style="color:var(--text-muted)">Building long-term allocation view...</p>`;

    try {
        const res = await fetch(`${API_BASE}/portfolio/long-term-view`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                aumUsd: 1000000,
                positions: portfolioBucket.map(p => ({ symbol: p.symbol, qty: p.qty, avgPrice: p.avgPrice }))
            })
        });
        if (!res.ok) throw new Error(`Long-term view failed (${res.status})`);
        const data = await res.json();

        const rows = (data.positions || []).slice(0, 8);
        const guardrails = data.guardrails || {};
        container.innerHTML = `
            <div class="portfolio-longterm-header">
                <div>
                    <h3>Long-Term Allocation View</h3>
                    <p>5Y-oriented ranking by quality, benchmark-relative upside, confidence, and data quality.</p>
                </div>
                <div class="portfolio-longterm-status ${String(guardrails.status || 'PASS').toLowerCase()}">${guardrails.status || 'N/A'}</div>
            </div>
            <div class="portfolio-longterm-table">
                ${rows.map(r => `
                    <div class="portfolio-longterm-row">
                        <div>
                            <div class="portfolio-longterm-symbol">${r.symbol}</div>
                            <div class="portfolio-longterm-meta">${r.sector || 'Unknown'} • ${r.longTermQualityLabel || 'N/A'} (${r.longTermQualityScore ?? 'N/A'})</div>
                        </div>
                        <div class="portfolio-longterm-metrics">
                            <span>Now: ${r.currentWeightPct ?? 0}%</span>
                            <span>Target: ${r.targetWeightPct ?? 0}%</span>
                            <span class="${(r.weightGapPct ?? 0) >= 0 ? 'factor-positive' : 'factor-negative'}">
                                Gap: ${(r.weightGapPct ?? 0) >= 0 ? '+' : ''}${r.weightGapPct ?? 0}%
                            </span>
                            <span>5Y: ${(r.fiveYearTarget?.upside ?? 0) >= 0 ? '+' : ''}${(r.fiveYearTarget?.upside ?? 0).toFixed?.(1) ?? r.fiveYearTarget?.upside}%</span>
                        </div>
                    </div>
                `).join('')}
            </div>
            ${(guardrails.breaches || []).length ? `
                <div class="portfolio-longterm-breaches">
                    <strong>Guardrail breaches:</strong>
                    <ul class="diag-list">${(guardrails.breaches || []).slice(0, 4).map(b => `<li>${b.detail}</li>`).join('')}</ul>
                </div>
            ` : ''}
        `;
    } catch (err) {
        console.error('Long-term allocation view failed:', err);
        container.innerHTML = `<div class="placeholder-icon">⚠️</div><p style="color:var(--text-muted)">${err.message}</p>`;
    }
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
        const confidenceScore = analysis.confidenceScore ?? analysis.recommendation?.confidenceScore ?? 'N/A';
        const confidenceLabel = analysis.confidenceLabel ?? analysis.recommendation?.confidenceLabel ?? 'N/A';
        const dataQualityScore = analysis.dataQuality?.score ?? analysis.recommendation?.dataQualityScore ?? 'N/A';
        const dataQualityLabel = analysis.dataQuality?.label ?? 'N/A';

        let guardrails = null;
        try {
            const grRes = await fetch(`${API_BASE}/portfolio/guardrails`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    aumUsd: 1000000,
                    positions: portfolioBucket.map(p => ({
                        symbol: p.symbol,
                        qty: p.qty,
                        avgPrice: p.avgPrice
                    }))
                })
            });
            if (grRes.ok) {
                guardrails = await grRes.json();
            }
        } catch (e) {
            console.warn('Guardrails check failed:', e);
        }

        // Recommendation logic (maps internal NO TRADE -> HOLD (Do Nothing))
        const recAction = analysis.recommendation?.action || 'HOLD';
        let actionTitle = "Hold (Do Nothing)";
        let actionDesc = "Risk gates or mixed signals suggest waiting for a better setup before changing this position.";

        if (recAction === 'STRONG BUY' || recAction === 'BUY') {
            actionTitle = "Increase Position";
            actionDesc = "Strong fundamental and technical indicators suggest significant upside potential. Consider adding to your stakes.";
        } else if (recAction === 'SELL' || recAction === 'STRONG SELL') {
            actionTitle = "Decrease / Trim Position";
            actionDesc = "Warning: Multiple bearish indicators and risks detected. Consider trimming your exposure to protect capital.";
        } else if (recAction === 'HOLD') {
            actionTitle = "Hold Position";
            actionDesc = "Signals are mixed. Maintain position size while monitoring risk and confidence changes.";
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
                    <div class="brief-card-title">Strategic Pros</div>
                    <ul class="brief-list">
                        ${(analysis.scores.fundamental.factors || []).filter(f => f.impact === 'positive').slice(0, 3).map(f => `<li class="brief-list-item pros-item">${f.factor}</li>`).join('')}
                        ${(analysis.scores.technical.factors || []).filter(f => f.impact === 'positive').slice(0, 2).map(f => `<li class="brief-list-item pros-item">${f.factor}</li>`).join('')}
                    </ul>
                </div>
                <div class="brief-card">
                    <div class="brief-card-title">Risks & Cons</div>
                    <ul class="brief-list">
                        ${analysis.riskFactors.slice(0, 4).map(r => `<li class="brief-list-item cons-item">${r.risk}</li>`).join('')}
                    </ul>
                </div>
            </div>

            <div class="brief-grid animate-in" style="animation-delay:0.2s">
                <div class="brief-card">
                    <div class="brief-card-title">Price Forecasts</div>
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
                    <div class="brief-card-title">Risk Assessment</div>
                    <div class="risk-level" style="background:${analysis.overallScore >= 70 ? 'var(--accent-emerald)22' : 'var(--accent-rose)22'}; color:${analysis.overallScore >= 70 ? 'var(--accent-emerald)' : 'var(--accent-rose)'}">
                        ${analysis.overallScore >= 70 ? 'Low Risk Selection' : 'High Risk Exposure'}
                    </div>
                    <p style="font-size:0.85rem;color:var(--text-secondary); line-height:1.5; margin-top:8px">
                        Current volatility is ${analysis.scores.technical.indicators.volatility ? (analysis.scores.technical.indicators.volatility * 100).toFixed(1) : 'N/A'}% annually.
                        Major support detected at $${analysis.scores.technical.indicators.support?.toFixed(2) || 'N/A'}.
                    </p>
                    <p style="font-size:0.85rem;color:var(--text-secondary); line-height:1.5; margin-top:8px">
                        Confidence: <strong style="color:var(--text-primary)">${confidenceScore}</strong> (${confidenceLabel}) • Data Quality:
                        <strong style="color:var(--text-primary)">${dataQualityScore}</strong> (${dataQualityLabel})
                    </p>
                </div>
            </div>

            ${guardrails ? `
            <div class="brief-card animate-in" style="animation-delay:0.25s; margin-top:16px">
                <div class="brief-card-title">🏦 Portfolio Guardrails (${guardrails.status})</div>
                <p style="font-size:0.85rem;color:var(--text-secondary);margin-bottom:10px">
                    Exposure: ${guardrails.portfolio?.grossExposurePct ?? 'N/A'}% • Positions: ${guardrails.portfolio?.positionCount ?? 0}
                </p>
                ${(guardrails.breaches || []).length ? `
                    <ul class="brief-list">
                        ${(guardrails.breaches || []).slice(0, 5).map(b => `<li class="brief-list-item cons-item">${b.detail}</li>`).join('')}
                    </ul>
                ` : `<div style="color:var(--accent-emerald);font-weight:700">No guardrail breaches detected.</div>`}
            </div>` : ''}

            <div class="action-banner animate-in" style="animation-delay:0.3s">
                <div class="action-title">💡 Position Advice: ${actionTitle}</div>
                <div class="action-desc">${actionDesc}</div>
            </div>
            
            <div style="margin-top:40px; text-align:center" class="animate-in" style="animation-delay:0.4s">
                <button onclick="analyzeStock('${symbol}')" class="search-btn" style="padding:12px 32px; border-radius:12px">View Deep Engine Report</button>
                <button onclick="logPaperDecision('${symbol}')" class="search-btn" style="padding:12px 24px; border-radius:12px; margin-left:8px">
                    Log Paper Decision
                </button>
            </div>
        `;

        placeholder.classList.add('hidden');
        view.classList.remove('hidden');

    } catch (err) {
        console.error('Portfolio analysis failed:', err);
        placeholder.innerHTML = `<div class="placeholder-icon">⚠️</div><h3 class="placeholder-title">Analysis Error</h3><p style="color:var(--text-muted)">${err.message}</p>`;
    }
}

async function logPaperDecision(symbol) {
    try {
        const payload = {
            symbol,
            timestampLocal: new Date().toISOString(),
            source: 'frontend_manual',
            bucket: portfolioBucket,
            currentAnalysis: currentData?.analysis || null,
            currentCompanyInfo: currentData?.companyInfo || null
        };
        const res = await fetch(`${API_BASE}/paper-trade/log`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ eventType: 'paper_decision', payload })
        });
        if (!res.ok) throw new Error(`Paper log failed (${res.status})`);
        alert(`Paper-trade decision logged for ${symbol}.`);
    } catch (err) {
        console.error('Paper-trade log failed:', err);
        alert(`Failed to log paper trade: ${err.message}`);
    }
}

// ============================================
// MARKET INSIGHTS LOGIC
// ============================================

function escapeHtml(value) {
    return String(value ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

function formatInsightTimestamp(value) {
    if (!value) return 'Latest';
    const parsed = new Date(value.replace(' ', 'T'));
    if (Number.isNaN(parsed.getTime())) return value;
    return parsed.toLocaleString([], {
        month: 'short',
        day: 'numeric',
        hour: 'numeric',
        minute: '2-digit'
    });
}

function renderInsightsSummary(insights) {
    const summaryEl = document.getElementById('insights-summary');
    if (!summaryEl) return;

    if (!Array.isArray(insights) || insights.length === 0) {
        summaryEl.classList.add('hidden');
        summaryEl.innerHTML = '';
        return;
    }

    const sentimentCounts = insights.reduce((acc, item) => {
        const key = ['positive', 'negative', 'neutral'].includes(item?.sentiment) ? item.sentiment : 'neutral';
        acc[key] = (acc[key] || 0) + 1;
        return acc;
    }, { positive: 0, negative: 0, neutral: 0 });

    const sectorCounts = {};
    const stockSet = new Set();
    for (const item of insights) {
        (item?.sectors || []).forEach((sector) => {
            sectorCounts[sector] = (sectorCounts[sector] || 0) + 1;
        });
        (item?.stocks || []).forEach((ticker) => stockSet.add(ticker));
    }

    const topSectors = Object.entries(sectorCounts)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 4)
        .map(([name, count]) => `<span class="sector-tag">${escapeHtml(name)} (${count})</span>`)
        .join('');

    summaryEl.innerHTML = `
        <div class="insight-summary-card">
            <div class="insight-summary-label">Headlines Tracked</div>
            <div class="insight-summary-value">${insights.length}</div>
            <div class="insight-summary-sub">Top financial stories of the day</div>
        </div>
        <div class="insight-summary-card">
            <div class="insight-summary-label">Sentiment Split</div>
            <div class="insight-summary-value">
                <span class="is-positive">${sentimentCounts.positive}</span>
                <span class="insight-summary-divider">/</span>
                <span class="is-negative">${sentimentCounts.negative}</span>
                <span class="insight-summary-divider">/</span>
                <span class="is-neutral">${sentimentCounts.neutral}</span>
            </div>
            <div class="insight-summary-sub">Bullish / Bearish / Neutral</div>
        </div>
        <div class="insight-summary-card insight-summary-card--wide">
            <div class="insight-summary-label">Most Affected Sectors</div>
            <div class="insight-summary-tags">${topSectors || '<span class="sector-tag">General Market</span>'}</div>
            <div class="insight-summary-sub">${stockSet.size} unique stocks flagged for follow-up</div>
        </div>
    `;
    summaryEl.classList.remove('hidden');
}

async function fetchMarketInsights() {
    const grid = document.getElementById('insights-grid');
    const loading = document.getElementById('insights-loading');
    const summary = document.getElementById('insights-summary');
    const gainersMeta = document.getElementById('gainers-table-meta');
    const gainersTable = document.getElementById('gainers-table-container');

    grid.innerHTML = '';
    if (summary) {
        summary.classList.add('hidden');
        summary.innerHTML = '';
    }
    if (gainersMeta) {
        gainersMeta.classList.add('hidden');
        gainersMeta.innerHTML = '';
    }
    if (gainersTable) {
        gainersTable.innerHTML = '';
    }
    loading.classList.remove('hidden');

    try {
        const res = await fetch(`${API_BASE}/market-insights`);
        if (!res.ok) throw new Error(`Insights request failed (${res.status})`);
        const data = await res.json();

        loading.classList.add('hidden');

        if (!data.insights || data.insights.length === 0) {
            grid.innerHTML = '<p style="grid-column: 1/-1; text-align:center; color:var(--text-muted)">No market insights available right now.</p>';
            return;
        }

        renderInsightsSummary(data.insights);

        grid.innerHTML = data.insights.map((item, idx) => `
            <div class="insight-card ${item.sentiment} animate-in" style="animation-delay: ${idx * 0.1}s">
                <div class="insight-header">
                    <div class="insight-sectors">
                        <span class="insight-rank">#${idx + 1}</span>
                        ${(item.sectors || []).map(s => `<span class="sector-tag">${escapeHtml(s)}</span>`).join('')}
                    </div>
                    <span class="insight-sentiment ${item.sentiment || 'neutral'}">${escapeHtml(item.sentiment || 'neutral')}</span>
                </div>

                <div class="insight-meta">
                    <span>${escapeHtml(item.source || 'Market Feed')}</span>
                    <span>•</span>
                    <span>${escapeHtml(formatInsightTimestamp(item.publishedDate))}</span>
                </div>

                <h3 class="insight-title">${escapeHtml(item.title || 'Market Update')}</h3>

                <p class="insight-description">
                    ${escapeHtml(item.description || 'No summary available.')}
                </p>

                <div class="insight-footer">
                    <div class="insight-stocks">
                        ${(item.stocks || []).map(ticker => `
                            <button type="button" class="stock-pill stock-pill-btn" data-ticker="${escapeHtml(ticker)}" title="Analyze ${escapeHtml(ticker)}">
                                ${escapeHtml(ticker)}
                            </button>
                        `).join('')}
                    </div>
                    <a href="${escapeHtml(item.url || '#')}" target="_blank" rel="noopener" class="insight-link">Read Source →</a>
                </div>
            </div>
        `).join('');

        grid.querySelectorAll('.stock-pill-btn').forEach((btn) => {
            btn.addEventListener('click', () => {
                const ticker = btn.dataset.ticker;
                if (ticker) analyzeStock(ticker);
            });
        });

    } catch (err) {
        console.error('Failed to fetch insights:', err);
        loading.innerHTML = `<div class="placeholder-icon">⚠️</div><h3>Intelligence Offline</h3><p>${escapeHtml(err.message)}</p>`;
    }
}

async function fetchTopGainersToday() {
    const loading = document.getElementById('insights-loading');
    const summary = document.getElementById('insights-summary');
    const grid = document.getElementById('insights-grid');
    const meta = document.getElementById('gainers-table-meta');
    const container = document.getElementById('gainers-table-container');

    if (summary) {
        summary.classList.add('hidden');
        summary.innerHTML = '';
    }
    if (grid) grid.innerHTML = '';
    if (meta) {
        meta.classList.add('hidden');
        meta.innerHTML = '';
    }
    container.innerHTML = '';
    loading.classList.remove('hidden');

    try {
        const res = await fetch(`${API_BASE}/top-gainers-today`);
        if (!res.ok) throw new Error(`Top gainers request failed (${res.status})`);
        const data = await res.json();
        loading.classList.add('hidden');

        const gainers = Array.isArray(data.gainers) ? data.gainers : [];
        if (gainers.length === 0) {
            container.innerHTML = '<p style="text-align:center; color:var(--text-muted); padding:16px">No gainers available right now.</p>';
            return;
        }

        if (meta) {
            meta.innerHTML = `
                <div><strong>Top 10 Gainers Today</strong> (tracked universe)</div>
                <div>As of ${escapeHtml(data.asOf || '')} • Universe: ${Number(data.universeSize || 0)}</div>
            `;
            meta.classList.remove('hidden');
        }

        container.innerHTML = `
            <table class="gainers-table">
                <thead>
                    <tr>
                        <th>#</th>
                        <th>Symbol</th>
                        <th>Price</th>
                        <th>Change</th>
                        <th>Sector</th>
                        <th>Why It Soared Today</th>
                    </tr>
                </thead>
                <tbody>
                    ${gainers.map((g) => {
                        const pct = Number(g.changePct || 0);
                        const abs = Number(g.changeAbs || 0);
                        const pctClass = pct >= 0 ? 'is-positive' : 'is-negative';
                        const title = g.reason || 'No headline reason available';
                        const category = g.reasonCategory ? `<span class="gainer-reason-tag">${escapeHtml(g.reasonCategory)}</span>` : '';
                        const sourceLine = [g.reasonSource, formatInsightTimestamp(g.reasonPublishedDate || '')].filter(Boolean).join(' • ');
                        return `
                            <tr>
                                <td>${Number(g.rank || 0)}</td>
                                <td>
                                    <button type="button" class="gainer-symbol-btn" data-ticker="${escapeHtml(g.symbol || '')}">
                                        ${escapeHtml(g.symbol || '')}
                                    </button>
                                    <div class="gainer-company-name">${escapeHtml(g.name || '')}</div>
                                </td>
                                <td>$${Number(g.currentPrice || 0).toFixed(2)}</td>
                                <td class="${pctClass}">
                                    +${pct.toFixed(2)}%<br>
                                    <span class="gainer-change-abs">+$${abs.toFixed(2)}</span>
                                </td>
                                <td>${escapeHtml(g.sector || 'Unknown')}</td>
                                <td>
                                    <div class="gainer-reason-cell">
                                        ${category}
                                        <div class="gainer-reason-title">${escapeHtml(title)}</div>
                                        ${sourceLine ? `<div class="gainer-reason-meta">${escapeHtml(sourceLine)}</div>` : ''}
                                        ${g.reasonUrl ? `<a class="gainer-reason-link" href="${escapeHtml(g.reasonUrl)}" target="_blank" rel="noopener">Read source →</a>` : ''}
                                    </div>
                                </td>
                            </tr>
                        `;
                    }).join('')}
                </tbody>
            </table>
        `;

        container.querySelectorAll('.gainer-symbol-btn').forEach((btn) => {
            btn.addEventListener('click', () => {
                const ticker = btn.dataset.ticker;
                if (ticker) analyzeStock(ticker);
            });
        });
    } catch (err) {
        console.error('Failed to fetch top gainers:', err);
        loading.innerHTML = `<div class="placeholder-icon">⚠️</div><h3>Top Gainers Unavailable</h3><p>${escapeHtml(err.message)}</p>`;
    }
}
