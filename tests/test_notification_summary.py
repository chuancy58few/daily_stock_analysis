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
