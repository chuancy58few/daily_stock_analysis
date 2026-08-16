#!/usr/bin/env python3
"""Run the L1-stage scanner with tighter, forward-looking core-candidate rules."""

from __future__ import annotations

import scan_l1_setup_candidates as scanner


_original_analyze = scanner.analyze_record


def analyze_record(record, config):
    result = _original_analyze(record, config)
    if result.get("综合分") is None:
        return result

    score = min(100.0, float(result.get("综合分") or 0.0))
    result["综合分"] = score

    # Breakout confirmation is a stricter subset of "刚启动" and should be labelled first.
    if (
        1.55 <= float(result.get("当前价相对L1") or 0.0) <= 3.00
        and float(result.get("当前价相对H1") or 99.0) <= 1.02
        and float(result.get("当前相对前12月高点") or 0.0) >= 1.03
        and float(result.get("近3月涨跌幅") or 0.0) >= 0.10
    ):
        result["当前阶段"] = "突破确认"

    strict_structure = (
        float(result.get("初始下跌倍数") or 0.0) >= 2.8
        and float(result.get("第一次拉升倍数") or 0.0) >= 2.3
        and float(result.get("中段回落倍数") or 0.0) >= 2.0
        and int(result.get("初始下跌月数") or 0) >= 30
        and 0.60 <= float(result.get("L1相对L0") or 0.0) <= 1.75
        and float(result.get("当前价相对L1") or 99.0) <= 2.65
        and float(result.get("当前价相对H1") or 99.0) <= 0.95
    )
    technical = score >= 55.0
    core = (
        technical
        and strict_structure
        and result.get("当前阶段") in ("L1筑底", "刚启动", "突破确认")
        and not bool(record.get("是否ST"))
        and result.get("价格跳变风险") != "高"
        and result.get("流动性等级") in ("高", "中")
        and score >= 63.0
    )

    if core and score >= 78:
        grade = "A"
    elif core:
        grade = "B+"
    elif technical and score >= 68:
        grade = "B"
    elif technical:
        grade = "C+"
    else:
        grade = "观察"

    result["技术候选"] = technical
    result["核心候选"] = core
    result["候选等级"] = grade
    result["筛选状态"] = "核心候选" if core else ("技术候选" if technical else "低分形态")
    return result


scanner.analyze_record = analyze_record

if __name__ == "__main__":
    raise SystemExit(scanner.main())
