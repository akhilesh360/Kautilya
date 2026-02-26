import requests
import re
import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Any, Optional
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

class SECService:
    """
    Service for fetching and analyzing SEC 10-K and 10-Q filings.
    Identifies patterns, sentiment shifts, and 'alpha' signals from text.
    """
    
    BASE_URL_SUBMISSIONS = "https://data.sec.gov/submissions/CIK{cik}.json"
    BASE_URL_ARCHIVE = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{filename}"
    TICKER_CIK_URL = "https://www.sec.gov/files/company_tickers.json"
    
    HEADERS = {
        "User-Agent": "NifiPilot/1.0 (akhilesh@vsg-holdings.com)"  # SEC requires identifiable User-Agent
    }

    SECTION_PATTERNS = {
        "risk_factors": [
            r"Item\s+1A\.?\s+Risk\s+Factors",
            r"Risk\s+Factors",
        ],
        "mda": [
            r"Item\s+7\.?\s+Management[’']?s\s+Discussion\s+and\s+Analysis",
            r"Management[’']?s\s+Discussion\s+and\s+Analysis",
            r"Item\s+2\.?\s+Management[’']?s\s+Discussion\s+and\s+Analysis",  # 10-Q
        ],
        "liquidity": [
            r"Liquidity\s+and\s+Capital\s+Resources",
            r"Capital\s+Resources",
        ],
    }

    SECTION_END_PATTERNS = [
        r"Item\s+1B\.?",
        r"Item\s+2\.?",
        r"Item\s+3\.?",
        r"Item\s+7A\.?",
        r"Item\s+8\.?",
        r"Quantitative\s+and\s+Qualitative\s+Disclosures",
        r"Controls\s+and\s+Procedures",
    ]

    PATTERN_LEXICONS = {
        "risk_negative": [
            "weak demand", "demand softness", "macroeconomic", "uncertainty", "headwind", "pressure",
            "inflation", "higher interest rates", "liquidity", "impairment", "restructuring", "litigation",
            "regulatory", "customer concentration", "inventory build", "inventory write-down",
            "supply chain disruption", "covenant", "refinancing", "delinquency", "charge-off"
        ],
        "opportunity_positive": [
            "pricing power", "backlog", "pipeline", "expansion", "market share", "efficiency",
            "productivity", "ai", "generative ai", "demand acceleration", "margin expansion",
            "capacity expansion", "new product", "bookings growth", "secular growth",
            "cross-sell", "upsell", "strong execution", "free cash flow"
        ],
        "caution_tone": [
            "may", "could", "adversely", "risk", "uncertain", "volatility", "challenging", "cautious"
        ],
        "confidence_tone": [
            "strong", "improved", "momentum", "disciplined", "confident", "resilient", "opportunity", "outperform"
        ],
    }

    def __init__(self):
        self.ticker_to_cik_map = {}
        # Lazy load on first use instead of at startup
        # self._load_ticker_map()

    def _load_ticker_map(self):
        """Fetch and cache ticker to CIK mapping from SEC."""
        try:
            response = requests.get(self.TICKER_CIK_URL, headers=self.HEADERS, timeout=10)
            if response.status_code == 200:
                data = response.json()
                for key in data:
                    item = data[key]
                    ticker = item['ticker'].upper()
                    # CIK should be 10 digits padded with zeros
                    cik = str(item['cik_str']).zfill(10)
                    self.ticker_to_cik_map[ticker] = cik
            logger.info(f"Loaded {len(self.ticker_to_cik_map)} tickers from SEC.")
        except Exception as e:
            logger.error(f"Failed to load ticker map: {e}")

    def get_cik(self, ticker: str) -> Optional[str]:
        """Get the padded CIK for a given ticker."""
        if not self.ticker_to_cik_map:
            self._load_ticker_map()
        return self.ticker_to_cik_map.get(ticker.upper())

    def get_recent_filings(self, ticker: str, count: int = 5) -> List[Dict[str, Any]]:
        """Fetch list of recent 10-K and 10-Q filings."""
        cik = self.get_cik(ticker)
        if not cik:
            logger.warning(f"No CIK found for ticker {ticker}")
            return []
        
        try:
            url = self.BASE_URL_SUBMISSIONS.format(cik=cik)
            response = requests.get(url, headers=self.HEADERS, timeout=10)
            if response.status_code != 200:
                logger.error(f"SEC API error {response.status_code} for {ticker}")
                return []
            
            data = response.json()
            recent_data = data.get('filings', {}).get('recent', {})
            
            filings = []
            for i in range(len(recent_data.get('form', []))):
                form = recent_data['form'][i]
                if form in ['10-K', '10-Q']:
                    accession = recent_data['accessionNumber'][i].replace('-', '')
                    primary_doc = recent_data['primaryDocument'][i]
                    filings.append({
                        'form': form,
                        'accession': accession,
                        'file_name': primary_doc,
                        'filing_date': recent_data['filingDate'][i],
                        'report_date': recent_data['reportDate'][i],
                        'cik': cik
                    })
                if len(filings) >= count:
                    break
            return filings
        except Exception as e:
            logger.error(f"Error fetching filings list for {ticker}: {e}")
            return []

    def fetch_filing_content(self, cik: str, accession: str, filename: str) -> str:
        """Fetch the raw text/HTML content of a filing."""
        try:
            url = self.BASE_URL_ARCHIVE.format(cik=cik.lstrip('0'), accession=accession, filename=filename)
            response = requests.get(url, headers=self.HEADERS, timeout=20)
            if response.status_code == 200:
                return response.text
            return ""
        except Exception as e:
            logger.error(f"Error fetching filing content: {e}")
            return ""

    def _normalize_filing_text(self, content: str) -> str:
        """Strip HTML and normalize whitespace for section parsing / diffing."""
        if not content:
            return ""
        try:
            soup = BeautifulSoup(content, 'html.parser')
            text = soup.get_text(separator=' ')
        except Exception:
            text = content
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def _extract_section(self, text: str, section_key: str, max_chars: int = 40000) -> str:
        """Generic section extractor using start/end regex heuristics."""
        if not text:
            return ""
        starts = self.SECTION_PATTERNS.get(section_key, [])
        start_match = None
        for pat in starts:
            matches = list(re.finditer(pat, text, re.IGNORECASE))
            if matches:
                # often first is TOC and later one is body; pick last reasonable one
                start_match = matches[-1]
                break
        if not start_match:
            return ""

        start_idx = start_match.end()
        end_idx = min(len(text), start_idx + max_chars)
        for end_pat in self.SECTION_END_PATTERNS:
            m = re.search(end_pat, text[start_idx:end_idx], re.IGNORECASE)
            if m and m.start() > 300:
                end_idx = start_idx + m.start()
                break
        return text[start_idx:end_idx].strip()

    def extract_risk_factors(self, content: str) -> str:
        """
        Extract 'Item 1A. Risk Factors' section from a filing.
        Uses a heuristic approach with regex.
        """
        text = self._normalize_filing_text(content)
        return self._extract_section(text, "risk_factors", max_chars=30000)

    def _extract_mda(self, content: str) -> str:
        return self._extract_section(self._normalize_filing_text(content), "mda", max_chars=50000)

    def _extract_liquidity(self, content: str) -> str:
        return self._extract_section(self._normalize_filing_text(content), "liquidity", max_chars=25000)

    def _count_pattern_hits(self, text: str, phrases: List[str]) -> Dict[str, int]:
        text_l = (text or "").lower()
        hits = {}
        for p in phrases:
            # phrase-based counting; cap each count to avoid runaway repeated words
            count = len(re.findall(re.escape(p.lower()), text_l))
            if count > 0:
                hits[p] = min(count, 8)
        return hits

    def _analyze_pattern_shift(self, current_text: str, previous_text: str) -> Dict[str, Any]:
        """
        Rule-based pattern delta engine for filing edge extraction.
        Returns positive/negative/caution/confidence phrase shifts and signal flags.
        """
        curr = current_text or ""
        prev = previous_text or ""
        result = {
            "riskDelta": 0,
            "opportunityDelta": 0,
            "cautionToneDelta": 0,
            "confidenceToneDelta": 0,
            "riskPhraseAdds": [],
            "opportunityPhraseAdds": [],
            "flags": [],
        }
        if not curr:
            return result

        curr_risk = self._count_pattern_hits(curr, self.PATTERN_LEXICONS["risk_negative"])
        prev_risk = self._count_pattern_hits(prev, self.PATTERN_LEXICONS["risk_negative"])
        curr_opp = self._count_pattern_hits(curr, self.PATTERN_LEXICONS["opportunity_positive"])
        prev_opp = self._count_pattern_hits(prev, self.PATTERN_LEXICONS["opportunity_positive"])
        curr_caution = self._count_pattern_hits(curr, self.PATTERN_LEXICONS["caution_tone"])
        prev_caution = self._count_pattern_hits(prev, self.PATTERN_LEXICONS["caution_tone"])
        curr_conf = self._count_pattern_hits(curr, self.PATTERN_LEXICONS["confidence_tone"])
        prev_conf = self._count_pattern_hits(prev, self.PATTERN_LEXICONS["confidence_tone"])

        risk_delta = sum(curr_risk.values()) - sum(prev_risk.values())
        opp_delta = sum(curr_opp.values()) - sum(prev_opp.values())
        caution_delta = sum(curr_caution.values()) - sum(prev_caution.values())
        confidence_delta = sum(curr_conf.values()) - sum(prev_conf.values())

        risk_adds = sorted(
            [k for k, v in curr_risk.items() if v > prev_risk.get(k, 0)],
            key=lambda k: (curr_risk.get(k, 0) - prev_risk.get(k, 0)),
            reverse=True
        )[:8]
        opp_adds = sorted(
            [k for k, v in curr_opp.items() if v > prev_opp.get(k, 0)],
            key=lambda k: (curr_opp.get(k, 0) - prev_opp.get(k, 0)),
            reverse=True
        )[:8]

        flags = []
        if risk_delta >= 4:
            flags.append({"type": "risk_rising", "severity": "high", "detail": "Negative risk language increased materially"})
        elif risk_delta >= 2:
            flags.append({"type": "risk_rising", "severity": "medium", "detail": "Negative risk language increased"})

        if opp_delta >= 3:
            flags.append({"type": "opportunity_emerging", "severity": "medium", "detail": "Opportunity / growth language increased"})

        if caution_delta >= 6 and confidence_delta <= 0:
            flags.append({"type": "tone_cautious", "severity": "high", "detail": "Cautionary tone strengthened"})
        elif confidence_delta >= 4 and caution_delta <= 1:
            flags.append({"type": "tone_improving", "severity": "medium", "detail": "Confidence / execution tone strengthened"})

        result.update({
            "riskDelta": int(risk_delta),
            "opportunityDelta": int(opp_delta),
            "cautionToneDelta": int(caution_delta),
            "confidenceToneDelta": int(confidence_delta),
            "riskPhraseAdds": risk_adds,
            "opportunityPhraseAdds": opp_adds,
            "flags": flags,
        })
        return result

    def _analyze_section_edge(self, section_name: str, current_text: str, previous_text: str) -> Dict[str, Any]:
        """Combine semantic/text shift + pattern shifts into a section-level edge object."""
        text_shift = self.analyze_text_shift(current_text, previous_text)
        pattern_shift = self._analyze_pattern_shift(current_text, previous_text)

        # Edge score: positive means improving/opportunity, negative means rising risk/caution.
        similarity = float(text_shift.get("similarity", 1.0) or 1.0)
        sent_drift = float(text_shift.get("sentiment_drift", 0) or 0)
        score = 0.0
        score += max(-15, min(15, sent_drift * 120))
        score += max(-10, min(10, pattern_shift.get("opportunityDelta", 0) * 2))
        score -= max(-10, min(18, pattern_shift.get("riskDelta", 0) * 2))
        score -= max(-8, min(12, pattern_shift.get("cautionToneDelta", 0)))
        score += max(-8, min(10, pattern_shift.get("confidenceToneDelta", 0)))
        if similarity < 0.80:
            score += 4  # meaningful information delta can be useful; sign determined by text/pattern changes

        direction = "neutral"
        if score >= 6:
            direction = "positive"
        elif score <= -6:
            direction = "negative"

        return {
            "section": section_name,
            "lengthCurrent": len(current_text or ""),
            "lengthPrevious": len(previous_text or ""),
            "textShift": text_shift,
            "patternShift": pattern_shift,
            "edgeScore": round(float(score), 2),
            "direction": direction,
            "available": bool(current_text),
        }

    def _summarize_filing_edge(self, sections: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Aggregate section-level outputs into a top-level edge score and summary."""
        available = [s for s in sections if s.get("available")]
        if not available:
            return {
                "edgeScore": 0.0,
                "edgeLabel": "No filing text extracted",
                "drift_alert": False,
                "edge_summary": "Insufficient filing text for section-level comparison.",
                "signals": [],
            }

        weights = {"risk_factors": 0.35, "mda": 0.45, "liquidity": 0.20}
        weighted = 0.0
        for s in available:
            weighted += float(s.get("edgeScore", 0) or 0) * weights.get(s.get("section"), 0.25)

        all_flags = []
        for s in available:
            for flag in ((s.get("patternShift") or {}).get("flags") or []):
                all_flags.append({**flag, "section": s.get("section")})

        # Sort by severity then section edge magnitude
        sev_rank = {"high": 2, "medium": 1, "low": 0}
        all_flags.sort(key=lambda f: sev_rank.get(f.get("severity", "low"), 0), reverse=True)

        label = "Neutral filing drift"
        if weighted >= 7:
            label = "Positive filing drift"
        elif weighted <= -7:
            label = "Negative filing drift"

        top_bits = []
        for s in sorted(available, key=lambda x: abs(float(x.get("edgeScore", 0) or 0)), reverse=True)[:2]:
            if s.get("direction") == "positive":
                top_bits.append(f"{s['section']} tone/opportunity signals improved")
            elif s.get("direction") == "negative":
                top_bits.append(f"{s['section']} language became more cautious/risk-heavy")

        if not top_bits and all_flags:
            top_bits.append(all_flags[0].get("detail", "Notable filing language shift detected"))
        if not top_bits:
            top_bits.append("Narrative remains broadly consistent with prior filing")

        drift_alert = bool(weighted <= -5 or any(f.get("severity") == "high" for f in all_flags))

        return {
            "edgeScore": round(float(weighted), 2),
            "edgeLabel": label,
            "drift_alert": drift_alert,
            "edge_summary": ". ".join(top_bits).strip() + ".",
            "signals": all_flags[:8],
        }

    def analyze_text_shift(self, current_text: str, previous_text: str) -> Dict[str, Any]:
        """
        Compare current filing section with previous one to identify 'alpha' signals.
        - Similarity score
        - Keywords that were added (new risks or opportunities)
        - Sentiment shifts
        """
        if not current_text or not previous_text:
            return {"similarity": 1.0, "sentiment_drift": 0, "added_keywords": [], "status": "Insufficient data"}

        try:
            # Lazy imports to avoid startup hang
            from textblob import TextBlob
            from sklearn.feature_extraction.text import TfidfVectorizer
            from sklearn.metrics.pairwise import cosine_similarity

            # Sentiment Analysis
            curr_blob = TextBlob(current_text[:50000]) # Cap for performance
            prev_blob = TextBlob(previous_text[:50000])
            
            curr_sent = curr_blob.sentiment.polarity
            prev_sent = prev_blob.sentiment.polarity
            sent_drift = curr_sent - prev_sent

            # TF-IDF Comparison
            vectorizer = TfidfVectorizer(stop_words='english', max_features=500)
            tfidf = vectorizer.fit_transform([previous_text, current_text])
            similarity = cosine_similarity(tfidf[0:1], tfidf[1:2])[0][0]
            
            # Find words that increased in importance (highest TF-IDF in current but not in previous)
            feature_names = vectorizer.get_feature_names_out()
            prev_weights = tfidf[0].toarray()[0]
            curr_weights = tfidf[1].toarray()[0]
            
            diff = curr_weights - prev_weights
            top_added_indices = diff.argsort()[-10:][::-1]
            added_keywords = [feature_names[i] for i in top_added_indices if diff[i] > 0.1]

            return {
                "similarity": float(similarity),
                "sentiment_drift": float(sent_drift),
                "current_sentiment": float(curr_sent),
                "added_keywords": added_keywords,
                "status": "Success",
                "drift_alert": similarity < 0.85 or sent_drift < -0.05
            }
        except Exception as e:
            logger.error(f"Error in text analysis: {e}")
            return {"similarity": 0, "sentiment_drift": 0, "added_keywords": [], "status": f"Error: {e}"}

    def identify_alpha_edge(self, ticker: str) -> Dict[str, Any]:
        """
        Main entry point for finding the 'edge'.
        Compares latest 10-K/Q with the one before it.
        """
        filings = self.get_recent_filings(ticker, count=2)
        if len(filings) < 2:
            return {"error": "Could not find enough recent filings for comparison."}

        latest = filings[0]
        prev = filings[1]
        return self.analyze_filing_pair(ticker, latest, prev)

    def analyze_filing_pair(self, ticker: str, latest: Dict[str, Any], prev: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze a specific pair of filings (latest vs previous) for a ticker.
        Useful for building backtests over multiple historical filing pairs.
        """
        logger.info(f"Comparing {latest['form']} ({latest['filing_date']}) with {prev['form']} ({prev['filing_date']})")

        latest_content = self.fetch_filing_content(latest['cik'], latest['accession'], latest['file_name'])
        prev_content = self.fetch_filing_content(prev['cik'], prev['accession'], prev['file_name'])

        latest_risks = self.extract_risk_factors(latest_content)
        prev_risks = self.extract_risk_factors(prev_content)
        latest_mda = self._extract_mda(latest_content)
        prev_mda = self._extract_mda(prev_content)
        latest_liquidity = self._extract_liquidity(latest_content)
        prev_liquidity = self._extract_liquidity(prev_content)

        # Keep backward-compatible top-level text-shift fields using risk factor section
        base_analysis = self.analyze_text_shift(latest_risks, prev_risks)

        section_edges = [
            self._analyze_section_edge("risk_factors", latest_risks, prev_risks),
            self._analyze_section_edge("mda", latest_mda, prev_mda),
            self._analyze_section_edge("liquidity", latest_liquidity, prev_liquidity),
        ]
        aggregate = self._summarize_filing_edge(section_edges)

        base_analysis.update({
            'ticker': ticker,
            'latest_form': latest['form'],
            'latest_date': latest['filing_date'],
            'prev_form': prev['form'],
            'prev_date': prev['filing_date'],
            'edge_summary': aggregate['edge_summary'],
            'edgeScore': aggregate['edgeScore'],
            'edgeLabel': aggregate['edgeLabel'],
            'drift_alert': aggregate['drift_alert'] or bool(base_analysis.get('drift_alert')),
            'sectionEdges': section_edges,
            'filingSignals': aggregate['signals'],
            'filingEdgeEngine': {
                'version': 'v1',
                'sectionsAnalyzed': [s['section'] for s in section_edges if s.get('available')],
                'notes': 'Section-level filing diff using semantic shift + pattern lexicons (risk/opportunity/tone).'
            }
        })

        # Concise human-readable edge detail for UI fallback.
        if not base_analysis.get('edge_summary'):
            base_analysis['edge_summary'] = "Narrative remains consistent with previous filings. Low information delta."

        return base_analysis

    def identify_alpha_edge_series(self, ticker: str, max_pairs: int = 6) -> List[Dict[str, Any]]:
        """
        Analyze multiple recent filing pairs for a ticker.
        Returns list ordered newest-first, each comparing filing[i] vs filing[i+1].
        """
        filings = self.get_recent_filings(ticker, count=max_pairs + 1)
        if len(filings) < 2:
            return []

        results: List[Dict[str, Any]] = []
        for i in range(len(filings) - 1):
            try:
                latest = filings[i]
                prev = filings[i + 1]
                edge = self.analyze_filing_pair(ticker, latest, prev)
                edge["pair_index"] = i
                results.append(edge)
            except Exception as e:
                logger.error(f"Filing edge series pair failed for {ticker} at {i}: {e}")
                results.append({
                    "ticker": ticker,
                    "pair_index": i,
                    "latest_date": filings[i].get("filing_date"),
                    "prev_date": filings[i + 1].get("filing_date"),
                    "latest_form": filings[i].get("form"),
                    "prev_form": filings[i + 1].get("form"),
                    "error": str(e),
                })
        return results
