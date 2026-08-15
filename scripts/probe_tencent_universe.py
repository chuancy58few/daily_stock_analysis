#!/usr/bin/env python3
"""Probe Tencent stock-universe endpoints and chunked qfq history."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import requests

OUT = Path("reports/tencent_probe")
OUT.mkdir(parents=True, exist_ok=True)
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/151 Safari/537.36"


def attempt(label: str, fn) -> Dict[str, Any]:
    try:
        value = {"ok": True, **fn()}
    except Exception as exc:  # noqa: BLE001
        value = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    print(label, json.dumps(value, ensure_ascii=False, default=str), flush=True)
    return value


def rank(endpoint: str, scheme: str = "http") -> Dict[str, Any]:
    response = requests.get(
        f"{scheme}://stock.gtimg.cn/data/index.php",
        params={"appn": "rank", "t": f"{endpoint}/chr", "p": 1, "o": 0, "l": 10000, "v": "list_data"},
        headers={"User-Agent": UA, "Referer": "https://stockapp.finance.qq.com/"},
        timeout=15,
    )
    response.raise_for_status()
    text = response.text
    return {"status": response.status_code, "bytes": len(response.content), "prefix": text[:300], "suffix": text[-150:]}


def history(symbol: str, start: str, end: str, count: int = 1500) -> Dict[str, Any]:
    response = requests.get(
        "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get",
        params={"param": f"{symbol},day,{start},{end},{count},qfq"},
        headers={"User-Agent": UA, "Referer": "https://gu.qq.com/"},
        timeout=15,
    )
    response.raise_for_status()
    payload = response.json()
    item = (payload.get("data") or {}).get(symbol) or {}
    rows = item.get("qfqday") or item.get("day") or []
    return {"status": response.status_code, "bytes": len(response.content), "rows": len(rows), "first": rows[:2], "last": rows[-2:], "keys": list(item.keys())}


def bse_codes() -> Dict[str, Any]:
    response = requests.post(
        "https://www.bse.cn/nqxxController/nqxxCnzq.do",
        data={"page": "0", "typejb": "T", "xxfcbj[]": "2", "xxzqdm": "", "sortfield": "xxzqdm", "sorttype": "asc"},
        headers={"User-Agent": UA, "Referer": "https://www.bse.cn/"},
        timeout=15,
    )
    response.raise_for_status()
    text = response.text
    parsed = json.loads(text[text.find("[") : -1])
    content = parsed[0].get("content", [])
    codes = []
    for row in content:
        for key, value in row.items():
            text_value = str(value)
            if len(text_value) == 6 and text_value.isdigit() and text_value.startswith(("4", "8", "9")):
                codes.append(text_value)
    codes = sorted(set(codes))
    return {"rows": len(content), "candidate_codes": codes[:20], "keys": list(content[0].keys()) if content else []}


def main() -> int:
    results = {}
    for scheme in ("http", "https"):
        for endpoint in ("rankash", "rankasz", "ranka", "rankbj", "rankabj", "rankbjs"):
            label = f"{scheme}_{endpoint}"
            results[label] = attempt(label, lambda e=endpoint, s=scheme: rank(e, s))
    results["history_chunk_1"] = attempt("history_chunk_1", lambda: history("sh600000", "2021-08-16", "2023-12-21"))
    results["history_chunk_2"] = attempt("history_chunk_2", lambda: history("sh600000", "2023-12-22", "2026-08-14"))
    results["history_no_start"] = attempt("history_no_start", lambda: history("sh600000", "", "", 1500))
    bse = attempt("bse_codes", bse_codes)
    results["bse_codes"] = bse
    candidate_codes = bse.get("candidate_codes", []) if bse.get("ok") else []
    for code in candidate_codes[:5] or ["430047", "830799", "835185"]:
        label = f"history_bj_{code}"
        results[label] = attempt(label, lambda c=code: history(f"bj{c}", "2021-08-16", "2026-08-14"))
    (OUT / "probe.json").write_text(json.dumps(results, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
