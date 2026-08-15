#!/usr/bin/env python3
"""Probe Sina universe endpoints and BSE historical coverage."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict

import requests

OUT = Path("reports/sina_probe")
OUT.mkdir(parents=True, exist_ok=True)
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/151 Safari/537.36"


def attempt(label, fn) -> Dict[str, Any]:
    try:
        value = {"ok": True, **fn()}
    except Exception as exc:  # noqa: BLE001
        value = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    print(label, json.dumps(value, ensure_ascii=False, default=str), flush=True)
    return value


def sina_count(node: str) -> Dict[str, Any]:
    response = requests.get(
        "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeStockCount",
        params={"node": node},
        headers={"User-Agent": UA, "Referer": "https://finance.sina.com.cn/"},
        timeout=15,
    )
    response.raise_for_status()
    return {"status": response.status_code, "text": response.text[:200], "bytes": len(response.content)}


def sina_list(node: str, num: int = 10000, page: int = 1) -> Dict[str, Any]:
    response = requests.get(
        "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData",
        params={"page": page, "num": num, "sort": "symbol", "asc": 1, "node": node, "symbol": "", "_s_r_a": "page"},
        headers={"User-Agent": UA, "Referer": "https://finance.sina.com.cn/"},
        timeout=20,
    )
    response.raise_for_status()
    rows = response.json()
    return {"status": response.status_code, "rows": len(rows), "bytes": len(response.content), "first": rows[:2], "last": rows[-2:]}


def bse_first_row() -> Dict[str, Any]:
    response = requests.post(
        "https://www.bse.cn/nqxxController/nqxxCnzq.do",
        data={"page": "0", "typejb": "T", "xxfcbj[]": "2", "xxzqdm": "", "sortfield": "xxzqdm", "sorttype": "asc"},
        headers={"User-Agent": UA, "Referer": "https://www.bse.cn/"},
        timeout=15,
    )
    response.raise_for_status()
    text = response.text
    parsed = json.loads(text[text.find("[") : -1])
    block = parsed[0]
    return {"total_pages": block.get("totalPages"), "total_elements": block.get("totalElements"), "first_row": (block.get("content") or [{}])[0]}


def sina_history(symbol: str) -> Dict[str, Any]:
    response = requests.get(
        "https://quotes.sina.cn/cn/api/jsonp_v2.php/var%20_probe=/CN_MarketDataService.getKLineData",
        params={"symbol": symbol, "scale": "240", "ma": "no", "datalen": "1500"},
        headers={"User-Agent": UA, "Referer": "https://finance.sina.com.cn/"},
        timeout=20,
    )
    response.raise_for_status()
    match = re.search(r"var\s+_probe=(\[.*\])", response.text, flags=re.S)
    rows = json.loads(match.group(1)) if match else []
    return {"status": response.status_code, "rows": len(rows), "bytes": len(response.content), "first": rows[:1], "last": rows[-1:]}


def tencent_history(symbol: str) -> Dict[str, Any]:
    response = requests.get(
        "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get",
        params={"param": f"{symbol},day,2021-08-16,2026-08-14,1500,qfq"},
        headers={"User-Agent": UA, "Referer": "https://gu.qq.com/"},
        timeout=20,
    )
    response.raise_for_status()
    item = (response.json().get("data") or {}).get(symbol) or {}
    rows = item.get("qfqday") or item.get("day") or []
    return {"status": response.status_code, "rows": len(rows), "keys": list(item.keys()), "first": rows[:1], "last": rows[-1:]}


def main() -> int:
    results = {}
    for node in ("hs_a", "sh_a", "sz_a", "kcb", "cyb", "hs_bjs"):
        results[f"count_{node}"] = attempt(f"count_{node}", lambda n=node: sina_count(n))
        results[f"list_{node}"] = attempt(f"list_{node}", lambda n=node: sina_list(n))
    results["bse_first_row"] = attempt("bse_first_row", bse_first_row)

    bse_rows = results.get("list_hs_bjs", {}).get("first", []) if results.get("list_hs_bjs", {}).get("ok") else []
    symbols = [row.get("symbol") for row in bse_rows if row.get("symbol")]
    if not symbols:
        symbols = ["bj920000", "bj430047", "bj830799"]
    for symbol in symbols[:3]:
        results[f"sina_history_{symbol}"] = attempt(f"sina_history_{symbol}", lambda s=symbol: sina_history(s))
        results[f"tencent_history_{symbol}"] = attempt(f"tencent_history_{symbol}", lambda s=symbol: tencent_history(s))

    (OUT / "probe.json").write_text(json.dumps(results, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
