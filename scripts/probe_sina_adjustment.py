#!/usr/bin/env python3
"""Compare Sina daily prices with Tencent qfq prices on overlapping dates."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import requests

OUT = Path("reports/sina_adjustment_probe")
OUT.mkdir(parents=True, exist_ok=True)
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/151 Safari/537.36"
SYMBOLS = ["sh600000", "sz000002", "sh600519", "sz300750", "sz002594"]


def sina(symbol: str) -> Dict[str, List[float]]:
    response = requests.get(
        "https://quotes.sina.cn/cn/api/jsonp_v2.php/var%20_probe=/CN_MarketDataService.getKLineData",
        params={"symbol": symbol, "scale": "240", "ma": "no", "datalen": "1500"},
        headers={"User-Agent": UA, "Referer": "https://finance.sina.com.cn/"},
        timeout=30,
    )
    response.raise_for_status()
    text = response.text
    left, right = text.find("["), text.rfind("]")
    rows = json.loads(text[left : right + 1])
    return {
        str(row["day"]): [float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"])]
        for row in rows
    }


def tencent(symbol: str) -> Dict[str, List[float]]:
    output: Dict[str, List[float]] = {}
    for start, end in (("2021-08-16", "2023-12-21"), ("2023-12-22", "2026-08-14")):
        response = requests.get(
            "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get",
            params={"param": f"{symbol},day,{start},{end},1500,qfq"},
            headers={"User-Agent": UA, "Referer": "https://gu.qq.com/"},
            timeout=30,
        )
        response.raise_for_status()
        item = (response.json().get("data") or {}).get(symbol) or {}
        rows = item.get("qfqday") or item.get("day") or []
        for row in rows:
            # Tencent format: date, open, close, high, low, volume
            output[str(row[0])] = [float(row[1]), float(row[3]), float(row[4]), float(row[2])]
    return output


def compare(symbol: str) -> Dict[str, Any]:
    s = sina(symbol)
    t = tencent(symbol)
    dates = sorted(set(s).intersection(t))
    differences = []
    mismatches = []
    for date in dates:
        sv, tv = s[date], t[date]
        diffs = [abs(a - b) for a, b in zip(sv, tv)]
        differences.extend(diffs)
        if max(diffs) > 0.011:
            mismatches.append({"date": date, "sina": sv, "tencent_qfq": tv, "max_abs_diff": max(diffs)})
    sample_dates = [date for date in ("2021-08-16", "2022-06-30", "2023-06-30", "2024-06-28", "2025-06-30", "2026-08-14") if date in s or date in t]
    return {
        "sina_rows": len(s),
        "tencent_rows": len(t),
        "overlap_rows": len(dates),
        "max_abs_diff": max(differences) if differences else None,
        "mismatch_rows_gt_0.011": len(mismatches),
        "mismatch_examples": mismatches[:5],
        "samples": {date: {"sina": s.get(date), "tencent_qfq": t.get(date)} for date in sample_dates},
    }


def main() -> int:
    results = {}
    for symbol in SYMBOLS:
        try:
            results[symbol] = {"ok": True, **compare(symbol)}
        except Exception as exc:  # noqa: BLE001
            results[symbol] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
        print(symbol, json.dumps(results[symbol], ensure_ascii=False, default=str), flush=True)
    (OUT / "probe.json").write_text(json.dumps(results, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
