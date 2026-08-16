#!/usr/bin/env python3
"""Run the A/HK pattern scanner with pure-shape ranking and separate risk flags."""

from __future__ import annotations

import scripts.scan_ah_double_spike_pattern as scanner


_original_analyze = scanner.analyze_record
_original_fields = scanner.result_fields


def analyze_record(record, config, target_profile):
    result, rows = _original_analyze(record, config, target_profile)
    if result.get("目标相似度") is None:
        return result, rows

    pure_score = 0.68 * float(result["目标相似度"]) + 0.32 * float(result["形态结构分"])
    risk_adjusted = pure_score - float(result.get("风险扣分") or 0.0)
    result["综合匹配分"] = pure_score
    result["风险调整分"] = risk_adjusted

    strict = (
        float(result["初始下跌倍数"]) >= 2.8
        and float(result["第一次拉升倍数"]) >= 3.0
        and float(result["中段回落倍数"]) >= 2.5
        and float(result["第二次拉升倍数"]) >= 3.0
        and int(result["初始下跌月数"]) >= 24
        and int(result["中段回落月数"]) >= 9
        and int(result["第二高点距今月数"]) <= 18
        and float(result["当前价占第二高点"]) >= 0.40
        and pure_score >= 60.0
    )
    broad = pure_score >= 44.0
    if strict and pure_score >= 85:
        grade = "A+"
    elif strict and pure_score >= 75:
        grade = "A"
    elif strict and pure_score >= 66:
        grade = "B+"
    elif strict:
        grade = "B"
    elif broad and pure_score >= 58:
        grade = "C+"
    elif broad:
        grade = "C"
    else:
        grade = "观察"

    result["是否严格候选"] = strict
    result["是否宽口径候选"] = broad
    result["匹配等级"] = grade
    result["筛选状态"] = "严格候选" if strict else ("宽口径候选" if broad else "低相似度")
    return result, rows


def result_fields():
    fields = _original_fields()
    if "风险调整分" not in fields:
        index = fields.index("综合匹配分") + 1
        fields.insert(index, "风险调整分")
    return fields


scanner.analyze_record = analyze_record
scanner.result_fields = result_fields

if __name__ == "__main__":
    raise SystemExit(scanner.main())
