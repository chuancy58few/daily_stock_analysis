# Broker Ratings + RSS News Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add broker ratings to summary output, implement RSS-based free news provider with fallback, and fix/clarify dividend_yield mapping.

**Architecture:** Introduce a cached CSV loader for broker ratings and inject its fields into `market_snapshot` when enabled. Add an RSS fetch/parse path to `SearchService` preferred for `search_stock_news` and `latest_news` dimension with fallback to existing providers, keeping output in `SearchResponse` format.

**Tech Stack:** Python 3.10+, requests, existing SearchService/Analyzer/Notification stack.

---

### Task 1: Add broker ratings config, CSV, and loader tests

**Files:**
- Create: `broker_ratings.csv`
- Modify: `src/config.py`
- Modify: `.env.example`
- Modify: `README.md`
- Create: `src/broker_ratings.py`
- Test: `tests/test_broker_ratings_loader.py`

**Step 1: Write the failing test**

```python
from src.broker_ratings import BrokerRatingsLoader

def test_broker_ratings_loader_basic(tmp_path):
    csv_path = tmp_path / "broker_ratings.csv"
    csv_path.write_text(
        "code,ms_stance,ms_as_of,ubs_stance,ubs_as_of,citi_stance,citi_as_of,updated_at,note\n"
        "600519,看多,2026-02-24,中性,2026-02-20,看空,2026-02-18,2026-02-24,example\n",
        encoding="utf-8",
    )
    loader = BrokerRatingsLoader(str(csv_path))
    data = loader.get_stances("600519")
    assert data["ms_stance"] == "看多"
    assert data["ubs_stance"] == "中性"
    assert data["citi_stance"] == "看空"


def test_broker_ratings_loader_missing_code(tmp_path):
    csv_path = tmp_path / "broker_ratings.csv"
    csv_path.write_text(
        "code,ms_stance,ms_as_of,ubs_stance,ubs_as_of,citi_stance,citi_as_of,updated_at,note\n",
        encoding="utf-8",
    )
    loader = BrokerRatingsLoader(str(csv_path))
    data = loader.get_stances("000001")
    assert data["ms_stance"] == "N/A"
    assert data["ubs_stance"] == "N/A"
    assert data["citi_stance"] == "N/A"
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_broker_ratings_loader.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.broker_ratings'`

**Step 3: Write minimal implementation**

```python
# src/broker_ratings.py
from dataclasses import dataclass
from typing import Dict
import csv
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

_ALLOWED = {"看多", "中性", "看空", "N/A"}

@dataclass
class BrokerRatingsLoader:
    path: str
    _cache: Dict[str, Dict[str, str]] = None
    _loaded: bool = False

    def _load_once(self) -> None:
        if self._loaded:
            return
        self._loaded = True
        self._cache = {}
        try:
            p = Path(self.path)
            if not p.exists():
                logger.warning(f"broker_ratings.csv not found: {self.path}")
                return
            with p.open("r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    code = (row.get("code") or "").strip()
                    if not code:
                        continue
                    def _stance(key: str) -> str:
                        v = (row.get(key) or "").strip() or "N/A"
                        return v if v in _ALLOWED else "N/A"
                    self._cache[code] = {
                        "ms_stance": _stance("ms_stance"),
                        "ubs_stance": _stance("ubs_stance"),
                        "citi_stance": _stance("citi_stance"),
                    }
        except Exception as e:
            logger.warning(f"broker_ratings.csv load failed: {e}")

    def get_stances(self, code: str) -> Dict[str, str]:
        self._load_once()
        if not self._cache:
            return {"ms_stance": "N/A", "ubs_stance": "N/A", "citi_stance": "N/A"}
        return self._cache.get(code, {"ms_stance": "N/A", "ubs_stance": "N/A", "citi_stance": "N/A"})
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_broker_ratings_loader.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add broker_ratings.csv src/broker_ratings.py tests/test_broker_ratings_loader.py src/config.py .env.example README.md

git commit -m "feat: add broker ratings loader and config"
```

---

### Task 2: Wire broker ratings into analysis snapshot and summary line

**Files:**
- Modify: `src/analyzer.py`
- Modify: `src/notification.py`
- Test: `tests/test_notification_summary.py`

**Step 1: Write the failing test**

```python
from src.analyzer import AnalysisResult
from src.notification import NotificationService


def test_summary_includes_broker_stances():
    r = AnalysisResult(
        code="600519",
        name="贵州茅台",
        operation_advice="买入",
        sentiment_score=80,
        trend_prediction="看涨",
        analysis_summary="",
        news_summary="",
    )
    r.market_snapshot = {
        "price": "100.00",
        "pe_ratio": "10.00",
        "dividend_yield": "1.00%",
        "ms_stance": "看多",
        "ubs_stance": "中性",
        "citi_stance": "看空",
    }
    line = NotificationService._build_summary_line(r)
    assert "MS:看多" in line
    assert "UBS:中性" in line
    assert "Citi:看空" in line
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_notification_summary.py -v`
Expected: FAIL because summary line lacks fields

**Step 3: Write minimal implementation**

```python
# src/analyzer.py - in _build_market_snapshot
from src.broker_ratings import BrokerRatingsLoader
from src.config import get_config

# ... inside _build_market_snapshot after realtime snapshot update
config = get_config()
if getattr(config, "broker_ratings_enabled", False):
    path = getattr(config, "broker_ratings_path", "broker_ratings.csv")
    loader = BrokerRatingsLoader(path)
    stances = loader.get_stances(context.get("code") or "")
    snapshot.update(stances)

# src/notification.py - _build_summary_line
ms = snapshot.get("ms_stance", "N/A")
ubs = snapshot.get("ubs_stance", "N/A")
citi = snapshot.get("citi_stance", "N/A")

return (
    f"{emoji} **{stock_name}({r.code})**: {r.operation_advice} | "
    f"评分 {r.sentiment_score} | {r.trend_prediction} | "
    f"现价 {price} | PE {pe_ratio} | 股息率 {dividend_yield} | "
    f"MS:{ms} | UBS:{ubs} | Citi:{citi}"
)
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_notification_summary.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/analyzer.py src/notification.py tests/test_notification_summary.py

git commit -m "feat: include broker ratings in snapshot and summary"
```

---

### Task 3: Add RSS provider and tests

**Files:**
- Modify: `src/search_service.py`
- Test: `tests/test_search_rss_provider.py`

**Step 1: Write the failing test**

```python
import textwrap
from datetime import datetime, timedelta
from src.search_service import SearchService


def test_rss_parse_and_filter(monkeypatch):
    rss = textwrap.dedent("""\
    <rss version="2.0">
      <channel>
        <item>
          <title>News A</title>
          <link>https://example.com/a</link>
          <source>Example</source>
          <pubDate>Mon, 24 Feb 2026 10:00:00 GMT</pubDate>
          <description>desc a</description>
        </item>
      </channel>
    </rss>
    """)

    class DummyResp:
        status_code = 200
        text = rss

    def fake_get(*args, **kwargs):
        return DummyResp()

    monkeypatch.setattr("requests.get", fake_get)

    svc = SearchService(news_max_age_days=3)
    resp = svc.search_stock_news("600519", "贵州茅台")
    assert resp.results
    assert resp.results[0].title == "News A"
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_search_rss_provider.py -v`
Expected: FAIL because RSS is not implemented

**Step 3: Write minimal implementation**

```python
# src/search_service.py
# Add config access and RSS helpers in SearchService
from src.config import get_config
from email.utils import parsedate_to_datetime

class SearchService:
    # ... add RSS config in __init__
    def __init__(..., news_max_age_days: int = 3):
        # existing
        self._rss_enabled = True
        self._rss_feed_template = "https://news.google.com/rss/search?q={q}&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
        self._rss_max_results = 5
        cfg = get_config()
        self._rss_enabled = getattr(cfg, "rss_enabled", True)
        self._rss_feed_template = getattr(cfg, "rss_feed_template", self._rss_feed_template)
        self._rss_max_results = int(getattr(cfg, "rss_max_results", 5))

    def _build_rss_query(self, stock_code: str, stock_name: str) -> str:
        if self._is_foreign_stock(stock_code):
            return f"{stock_name} {stock_code} stock latest news"
        return f"{stock_name} {stock_code} 股票 最新消息"

    def _parse_rss(self, text: str) -> List[SearchResult]:
        # minimal XML parsing using ElementTree
        import xml.etree.ElementTree as ET
        root = ET.fromstring(text)
        items = []
        for item in root.findall(".//item"):
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            source = (item.findtext("source") or "").strip() or "Google News"
            pub = (item.findtext("pubDate") or "").strip()
            desc = (item.findtext("description") or "").strip()
            items.append(SearchResult(title=title, snippet=desc, url=link, source=source, published_date=pub))
        # Atom support
        if not items:
            for entry in root.findall(".//{http://www.w3.org/2005/Atom}entry"):
                title = (entry.findtext("{http://www.w3.org/2005/Atom}title") or "").strip()
                link_el = entry.find("{http://www.w3.org/2005/Atom}link")
                link = (link_el.get("href") if link_el is not None else "").strip()
                source = "Google News"
                pub = (entry.findtext("{http://www.w3.org/2005/Atom}published") or "").strip()
                desc = (entry.findtext("{http://www.w3.org/2005/Atom}summary") or "").strip()
                items.append(SearchResult(title=title, snippet=desc, url=link, source=source, published_date=pub))
        return items

    def _filter_rss(self, results: List[SearchResult]) -> List[SearchResult]:
        # freshness + dedupe
        max_age = self.news_max_age_days
        now = datetime.utcnow()
        seen = set()
        filtered = []
        for r in results:
            # parse date
            pub_dt = None
            if r.published_date:
                try:
                    pub_dt = parsedate_to_datetime(r.published_date)
                    if pub_dt.tzinfo is not None:
                        pub_dt = pub_dt.astimezone(tz=None).replace(tzinfo=None)
                except Exception:
                    pub_dt = None
            if pub_dt is not None:
                if now - pub_dt > timedelta(days=max_age):
                    continue
            # dedupe
            key = r.url or f"{r.title}|{r.source}|{r.published_date}"
            if key in seen:
                continue
            seen.add(key)
            filtered.append(r)
        return filtered

    def _search_rss(self, stock_code: str, stock_name: str) -> SearchResponse:
        query = self._build_rss_query(stock_code, stock_name)
        url = self._rss_feed_template.format(q=query)
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                return SearchResponse(query=query, results=[], provider="RSS", success=False, error_message=f"HTTP {resp.status_code}")
            results = self._filter_rss(self._parse_rss(resp.text))[: self._rss_max_results]
            return SearchResponse(query=query, results=results, provider="RSS", success=bool(results))
        except Exception as e:
            return SearchResponse(query=query, results=[], provider="RSS", success=False, error_message=str(e))
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_search_rss_provider.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/search_service.py tests/test_search_rss_provider.py

git commit -m "feat: add RSS news provider"
```

---

### Task 4: Prefer RSS in search flows and update docs

**Files:**
- Modify: `src/search_service.py`
- Modify: `src/config.py`
- Modify: `.env.example`
- Modify: `README.md`

**Step 1: Write the failing test**

```python
from src.search_service import SearchService


def test_rss_preferred_over_providers(monkeypatch):
    svc = SearchService(news_max_age_days=3)
    svc._rss_enabled = True
    svc._providers = []

    def fake_rss(*args, **kwargs):
        from src.search_service import SearchResponse
        return SearchResponse(query="q", results=[], provider="RSS", success=True)

    monkeypatch.setattr(svc, "_search_rss", fake_rss)
    resp = svc.search_stock_news("600519", "贵州茅台")
    assert resp.provider == "RSS"
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_search_rss_provider.py -v`
Expected: FAIL because RSS is not preferred

**Step 3: Write minimal implementation**

```python
# src/search_service.py - in search_stock_news and search_comprehensive_intel
if self._rss_enabled:
    rss_resp = self._search_rss(stock_code, stock_name)
    if rss_resp.success and rss_resp.results:
        return rss_resp

# For latest_news dimension in search_comprehensive_intel
if dim['name'] == 'latest_news' and self._rss_enabled:
    rss_resp = self._search_rss(stock_code, stock_name)
    if rss_resp.success and rss_resp.results:
        results[dim['name']] = rss_resp
        search_count += 1
        continue
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_search_rss_provider.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/search_service.py src/config.py .env.example README.md

git commit -m "feat: prefer RSS for latest news"
```

---

### Task 5: Dividend yield mapping verification and docs

**Files:**
- Modify: `data_provider/akshare_fetcher.py`
- Modify: `data_provider/efinance_fetcher.py`
- Modify: `data_provider/tushare_fetcher.py`
- Modify: `README.md`
- Test: `tests/test_analyzer_snapshot_and_provider.py`

**Step 1: Write the failing test**

```python
def test_dividend_yield_mapping_from_provider():
    from data_provider.realtime_types import UnifiedRealtimeQuote
    q = UnifiedRealtimeQuote(code="600519", dividend_yield=1.5)
    assert q.dividend_yield == 1.5
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_analyzer_snapshot_and_provider.py -v`
Expected: FAIL only if mapping is missing

**Step 3: Write minimal implementation**

```python
# data_provider/*_fetcher.py
# Map dividend yield if provider returns it
# Example:
# dividend_yield=safe_float(row.get('股息率'))
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_analyzer_snapshot_and_provider.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add data_provider/*.py README.md tests/test_analyzer_snapshot_and_provider.py

git commit -m "fix: map dividend yield or document limitation"
```

---

### Task 6: Final verification

**Files:**
- Test: `tests/`

**Step 1: Run full test suite**

Run: `python -m pytest -q`
Expected: PASS

**Step 2: Commit**

```bash
git add -A

git commit -m "test: update coverage for broker ratings and RSS"
```
