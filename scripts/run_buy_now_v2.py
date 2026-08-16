#!/usr/bin/env python3
"""Run the buy-now scanner with normalized ROE and short-history handling."""

from __future__ import annotations

from typing import Any, Dict, Optional, Sequence

import scan_buy_now_candidates as scanner


_original_score_record = scanner.score_record


def annualized_roe(latest: Dict[str, Any], annual: Dict[str, Any]) -> Optional[float]:
    annual_roe = scanner.finite_number(annual.get("净资产收益率"))
    if annual_roe is not None:
        return annual_roe
    latest_roe = scanner.finite_number(latest.get("净资产收益率"))
    period = str(latest.get("报告期") or "")
    if latest_roe is None:
        return None
    if period == "2026Q1":
        return latest_roe * 4.0
    if period == "2026H1":
        return latest_roe * 2.0
    return latest_roe


def price_metrics(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    valid = [
        row for row in rows
        if scanner.finite_number(row.get("close")) is not None
        and float(row["close"]) > 0
    ]
    closes = [float(row["close"]) for row in valid]
    if len(closes) < 3:
        raise RuntimeError("insufficient price history")
    dates = [str(row["date"]) for row in valid]
    window = closes[-min(61, len(closes)):]
    current = closes[-1]
    high5 = max(window)
    low5 = min(window)
    ma6 = scanner.average(closes[-min(6, len(closes)):])
    ma12 = scanner.average(closes[-min(12, len(closes)):])
    percentile = (current - low5) / (high5 - low5) if high5 > low5 else 0.5

    def trailing_return(months: int):
        if len(closes) <= months or closes[-months - 1] <= 0:
            return None
        return current / closes[-months - 1] - 1.0

    return {
        "价格日期": dates[-1],
        "月线收盘价": current,
        "五年高点": high5,
        "五年低点": low5,
        "当前占五年高点": current / high5 if high5 > 0 else None,
        "当前较五年低点倍数": current / low5 if low5 > 0 else None,
        "五年价格分位": percentile,
        "近3月涨跌幅": trailing_return(3),
        "近6月涨跌幅": trailing_return(6),
        "近12月涨跌幅": trailing_return(12),
        "当前相对6月均线": current / ma6 if ma6 and ma6 > 0 else None,
        "当前相对12月均线": current / ma12 if ma12 and ma12 > 0 else None,
        "五年最大回撤": scanner.maximum_drawdown(window),
        "价格口径": "腾讯/新浪月线",
        "有效月数": len(closes),
        "价格历史覆盖": "不足1年" if len(closes) < 12 else ("1—2年" if len(closes) < 24 else ("2—3年" if len(closes) < 36 else "3年以上")),
    }


def classify_after_history_adjustment(result: Dict[str, Any], history_months: int) -> Dict[str, Any]:
    total = float(result.get("买入总分") or 0.0)
    if history_months < 12:
        total -= 18.0
    elif history_months < 24:
        total -= 10.0
    elif history_months < 36:
        total -= 4.0
    total = scanner.clip(total)
    result["买入总分"] = total

    if history_months < 24:
        if total >= 66.0:
            result["最终判断"] = "新股/短历史，等待确认"
        else:
            result["最终判断"] = "暂不优先"
    else:
        # Preserve the original classification, but prevent borderline names from
        # remaining in the buyable bucket after the history penalty.
        if result.get("最终判断") in ("特别适合分批建首仓", "适合分批建首仓") and total < 74.0:
            result["最终判断"] = "等待基本面或价格确认"
        if result.get("最终判断") == "特别适合分批建首仓" and total < 80.0:
            result["最终判断"] = "适合分批建首仓"

    existing = str(result.get("研究备注") or "")
    note = f"价格历史{history_months}个月"
    if result.get("市场") == "港股通":
        note += "；港股增长指标为滚动环比口径"
    result["研究备注"] = "；".join(part for part in (existing, note) if part)
    result["数据完整度"] = min(float(result.get("数据完整度") or 0.0), min(1.0, history_months / 36.0))
    return result


def score_record(
    record: Dict[str, Any],
    latest: Dict[str, Any],
    annual: Dict[str, Any],
    dividend: Dict[str, Any],
    price: Dict[str, Any],
    l1_result: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    latest_copy = dict(latest)
    normalized_roe = annualized_roe(latest_copy, annual)
    if normalized_roe is not None:
        latest_copy["净资产收益率"] = normalized_roe
    result = _original_score_record(record, latest_copy, annual, dividend, price, l1_result)
    result["原始报告期ROE"] = scanner.finite_number(latest.get("净资产收益率"))
    result["净资产收益率"] = normalized_roe
    result["价格历史覆盖"] = price.get("价格历史覆盖")
    result["价格历史月数"] = int(price.get("有效月数") or 0)
    return classify_after_history_adjustment(result, int(price.get("有效月数") or 0))


scanner.price_metrics = price_metrics
scanner.score_record = score_record

# Make the extra audit fields available in every output CSV.
for field in ("原始报告期ROE", "价格历史覆盖", "价格历史月数"):
    if field not in scanner.OUTPUT_FIELDS:
        scanner.OUTPUT_FIELDS.append(field)

if __name__ == "__main__":
    raise SystemExit(scanner.main())
