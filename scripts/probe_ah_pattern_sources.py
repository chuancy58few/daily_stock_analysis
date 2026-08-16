#!/usr/bin/env python3
from __future__ import annotations

import json
import math
import re
from pathlib import Path

import requests

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/151.0 Safari/537.36",
    "Referer": "https://gu.qq.com/",
}


def get_json(url: str, params: dict | None = None):
    r = requests.get(url, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()


def get_text(url: str, params: dict | None = None):
    r = requests.get(url, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.text


def fetch_tencent_month(code: str, count: int = 300):
    c = code.lower()
    url = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
    data = get_json(url, {"param": f"{c},month,,,{count},qfq"})
    root = (data.get("data") or {}).get(c) or {}
    rows = root.get("qfqmonth") or root.get("month") or []
    return rows, root


def main():
    out = Path("reports/ah_pattern_probe")
    out.mkdir(parents=True, exist_ok=True)
    result = {}

    # Target quote and history.
    quote_text = get_text("https://qt.gtimg.cn/q=hk03839")
    result["target_quote_prefix"] = quote_text[:300]
    rows, root = fetch_tencent_month("hk03839", 300)
    result["target_month_keys"] = list(root.keys())
    result["target_month_count"] = len(rows)
    result["target_month_first"] = rows[:5]
    result["target_month_last"] = rows[-12:]

    # A-share count.
    a_count = get_text(
        "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeStockCount",
        {"node": "hs_a"},
    )
    result["a_count_raw"] = a_count[:200]

    # HK universe variants.
    hk_url = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHKStockData"
    variants = []
    for num in (3000, 5000, 10000):
        try:
            rows_hk = get_json(hk_url, {
                "page": 1,
                "num": num,
                "sort": "symbol",
                "asc": 1,
                "node": "qbgg_hk",
                "_s_r_a": "page",
            })
            variants.append({
                "num": num,
                "type": type(rows_hk).__name__,
                "count": len(rows_hk) if isinstance(rows_hk, list) else None,
                "first": rows_hk[:3] if isinstance(rows_hk, list) else str(rows_hk)[:300],
                "last": rows_hk[-3:] if isinstance(rows_hk, list) else None,
            })
        except Exception as exc:
            variants.append({"num": num, "error": f"{type(exc).__name__}: {exc}"})
    result["hk_list_variants"] = variants

    # Sina HK count endpoint possibilities.
    count_url = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHKStockCount"
    try:
        result["hk_count_raw"] = get_text(count_url, {"node": "qbgg_hk"})[:300]
    except Exception as exc:
        result["hk_count_error"] = f"{type(exc).__name__}: {exc}"

    (out / "probe.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
