# LLM Summary Fields + OpenAI Default Provider Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add PE, dividend yield, and current price to summary outputs, and make OpenAI the default LLM provider when `OPENAI_API_KEY` is configured.

**Architecture:** Prefer OpenAI during analyzer initialization, with fallback to Anthropic then Gemini. Summary rendering pulls values from `AnalysisResult.market_snapshot` with `N/A` fallbacks when missing.

**Tech Stack:** Python 3.10+, dataclasses, notification rendering, realtime quote pipeline.

---

### Task 1: Add summary formatting helper + failing tests

**Files:**
- Create: `tests/test_notification_summary.py`
- Modify: `src/notification.py`

**Step 1: Write the failing test**

```python
from src.notification import NotificationService
from src.analyzer import AnalysisResult


def test_summary_line_includes_pe_yield_price():
    service = NotificationService()
    result = AnalysisResult(
        code="600519",
        name="贵州茅台",
        sentiment_score=80,
        trend_prediction="看多",
        operation_advice="买入",
    )
    result.market_snapshot = {
        "price": "1688.00",
        "pe_ratio": "18.5",
        "dividend_yield": "1.80%",
    }
    line = service._build_summary_line(result)
    assert "PE" in line
    assert "股息率" in line
    assert "现价" in line
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_notification_summary.py::test_summary_line_includes_pe_yield_price -v`
Expected: FAIL with `AttributeError: 'NotificationService' object has no attribute '_build_summary_line'`

**Step 3: Write minimal implementation**

```python
def _build_summary_line(self, result: AnalysisResult) -> str:
    snapshot = getattr(result, "market_snapshot", None) or {}
    price = snapshot.get("price", "N/A")
    pe_ratio = snapshot.get("pe_ratio", "N/A")
    dividend_yield = snapshot.get("dividend_yield", "N/A")
    emoji = result.get_emoji()
    return (
        f"{emoji} **{result.name}({result.code})**: {result.operation_advice} | "
        f"评分 {result.sentiment_score} | {result.trend_prediction} | "
        f"现价 {price} | PE {pe_ratio} | 股息率 {dividend_yield}"
    )
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_notification_summary.py::test_summary_line_includes_pe_yield_price -v`
Expected: PASS

**Step 5: Commit**

```bash
git add tests/test_notification_summary.py src/notification.py
git commit -m "feat: add summary line helper with pe and yield"
```

### Task 2: Pass PE/price/yield into market snapshot

**Files:**
- Modify: `src/analyzer.py`
- Modify: `src/core/pipeline.py`

**Step 1: Write failing test**

```python
from src.analyzer import GeminiAnalyzer


def test_market_snapshot_includes_pe_and_dividend_yield():
    analyzer = GeminiAnalyzer()
    context = {
        "today": {},
        "realtime": {"price": 10.5, "pe_ratio": 12.3, "dividend_yield": 1.2},
    }
    snapshot = analyzer._build_market_snapshot(context)
    assert "pe_ratio" in snapshot
    assert "dividend_yield" in snapshot
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_notification_summary.py::test_market_snapshot_includes_pe_and_dividend_yield -v`
Expected: FAIL (missing keys)

**Step 3: Write minimal implementation**

```python
if realtime:
    snapshot.update({
        "price": self._format_price(realtime.get("price")),
        "pe_ratio": realtime.get("pe_ratio", "N/A"),
        "dividend_yield": self._format_percent(realtime.get("dividend_yield")),
        ...
    })
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_notification_summary.py::test_market_snapshot_includes_pe_and_dividend_yield -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/analyzer.py src/core/pipeline.py tests/test_notification_summary.py
git commit -m "feat: include pe and dividend yield in snapshots"
```

### Task 3: Change provider priority to prefer OpenAI

**Files:**
- Modify: `src/analyzer.py`

**Step 1: Write failing test**

```python
def test_openai_preferred_when_key_present(monkeypatch):
    from src.config import get_config
    config = get_config()
    config.openai_api_key = "test"
    config.gemini_api_key = None
    config.anthropic_api_key = None
    analyzer = GeminiAnalyzer()
    assert analyzer._use_openai is True
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_notification_summary.py::test_openai_preferred_when_key_present -v`
Expected: FAIL (Gemini still primary)

**Step 3: Write minimal implementation**

```python
# Initialize OpenAI first when openai_api_key present.
# If init fails, fallback to Anthropic then Gemini.
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_notification_summary.py::test_openai_preferred_when_key_present -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/analyzer.py tests/test_notification_summary.py
git commit -m "feat: prefer openai provider when configured"
```

### Task 4: Wire summary helper into all summary outputs

**Files:**
- Modify: `src/notification.py`

**Step 1: Update summary-only sections**

Replace inline summary formatting with `_build_summary_line(result)` in:
- Daily report summary-only mode
- Decision dashboard summary section
- WeChat dashboard summary-only mode
- NotificationBuilder summary

**Step 2: Run tests**

Run: `pytest tests/test_notification_summary.py -v`
Expected: PASS

**Step 3: Commit**

```bash
git add src/notification.py
git commit -m "feat: include pe price yield in summaries"
```

### Task 5: Docs and changelog

**Files:**
- Modify: `README.md`
- Modify: `docs/CHANGELOG.md`
- Modify: `.env.example`

**Step 1: Update docs**
- Mention OpenAI preference and summary fields.
- Document dividend yield fallback to `N/A` when not available.

**Step 2: Run syntax check**

Run: `python -m py_compile src/analyzer.py src/notification.py src/core/pipeline.py`
Expected: No output.

**Step 3: Commit**

```bash
git add README.md docs/CHANGELOG.md .env.example
git commit -m "docs: document summary fields and openai default"
```

### Task 6: Full verification

**Files:**
- None

**Step 1: Run repo checks**

Run: `python -m py_compile main.py src/*.py data_provider/*.py`
Expected: No output.

**Step 2: Run tests**

Run: `pytest -v`
Expected: PASS.

**Step 3: Commit**

```bash
git add -A
git commit -m "chore: verify llm summary updates"
```
