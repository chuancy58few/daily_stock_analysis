from src.analyzer import GeminiAnalyzer
from src.config import Config


def test_market_snapshot_includes_pe_and_dividend_yield():
    analyzer = GeminiAnalyzer()
    context = {
        "today": {},
        "realtime": {"price": 10.5, "pe_ratio": 12.3, "dividend_yield": 1.2},
    }
    snapshot = analyzer._build_market_snapshot(context)
    assert "pe_ratio" in snapshot
    assert "dividend_yield" in snapshot


def test_openai_preferred_when_key_present(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test_openai_key_1234567890")
    monkeypatch.setenv("GEMINI_API_KEY", "test_gemini_key_1234567890")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "")
    Config.reset_instance()
    analyzer = GeminiAnalyzer()
    assert analyzer._use_openai is True
    assert analyzer._use_anthropic is False
