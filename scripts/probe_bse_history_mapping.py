#!/usr/bin/env python3
"""Probe BSE code mapping and historical-price coverage under old/new symbols."""

from __future__ import annotations

import json
import re
from io import StringIO
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import requests

OUT = Path("reports/bse_history_probe")
OUT.mkdir(parents=True, exist_ok=True)
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/151 Safari/537.36"
SYMBOLS = [
    "bj832000", "bj920000",  # 安徽凤凰 old/new
    "bj830799", "bj920799",  # 艾融软件 old/new trial conversion
    "bj872931", "bj920931",  # 无锡鼎邦 old/new mapped
    "bj920002",               # 万达轴承, originally listed with 920 code
]


def attempt(label, fn) -> Dict[str, Any]:
    try:
        value = {"ok": True, **fn()}
    except Exception as exc:  # noqa: BLE001
        value = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    print(label, json.dumps(value, ensure_ascii=False, default=str), flush=True)
    return value


def mapping() -> Dict[str, Any]:
    url = "https://www.bseinfo.net/service/code_mapping.html"
    response = requests.get(url, headers={"User-Agent": UA, "Referer": "https://www.bseinfo.net/"}, timeout=20, allow_redirects=True)
    response.raise_for_status()
    tables = pd.read_html(StringIO(response.text))
    output = {"status": response.status_code, "bytes": len(response.content), "table_count": len(tables)}
    if tables:
        table = tables[0]
        output.update({"rows": len(table), "columns": [str(c) for c in table.columns], "head": table.head(3).to_dict("records")})
        for company in ("安徽凤凰", "纬达光电", "无锡鼎邦", "艾融软件"):
            matches = table[table.astype(str).apply(lambda row: row.str.contains(company, regex=False).any(), axis=1)]
            output[f"match_{company}"] = matches.to_dict("records")
    return output


def tencent(symbol: str) -> Dict[str, Any]:
    response = requests.get(
        "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get",
        params={"param": f"{symbol},day,2021-08-16,2026-08-14,1500,qfq"},
        headers={"User-Agent": UA, "Referer": "https://gu.qq.com/"},
        timeout=20,
    )
    response.raise_for_status()
    payload = response.json()
    item = (payload.get("data") or {}).get(symbol) or {}
    rows = item.get("qfqday") or item.get("day") or []
    return {
        "status": response.status_code,
        "bytes": len(response.content),
        "keys": list(item.keys()),
        "rows": len(rows),
        "first": rows[:2],
        "last": rows[-2:],
        "qt": item.get("qt"),
    }


def sina(symbol: str) -> Dict[str, Any]:
    response = requests.get(
        "https://quotes.sina.cn/cn/api/jsonp_v2.php/var%20_probe=/CN_MarketDataService.getKLineData",
        params={"symbol": symbol, "scale": "240", "ma": "no", "datalen": "1500"},
        headers={"User-Agent": UA, "Referer": "https://finance.sina.com.cn/"},
        timeout=20,
    )
    response.raise_for_status()
    text = response.text
    left = text.find("[")
    right = text.rfind("]")
    parsed = []
    parse_error = ""
    if left >= 0 and right > left:
        try:
            parsed = json.loads(text[left : right + 1])
        except Exception as exc:  # noqa: BLE001
            parse_error = f"{type(exc).__name__}: {exc}"
    return {
        "status": response.status_code,
        "bytes": len(response.content),
        "prefix": text[:300],
        "suffix": text[-300:],
        "left": left,
        "right": right,
        "rows": len(parsed) if isinstance(parsed, list) else None,
        "first": parsed[:2] if isinstance(parsed, list) else None,
        "last": parsed[-2:] if isinstance(parsed, list) else None,
        "parse_error": parse_error,
    }


def main() -> int:
    results: Dict[str, Any] = {"mapping": attempt("mapping", mapping)}
    for symbol in SYMBOLS:
        results[f"tencent_{symbol}"] = attempt(f"tencent_{symbol}", lambda s=symbol: tencent(s))
        results[f"sina_{symbol}"] = attempt(f"sina_{symbol}", lambda s=symbol: sina(s))
    (OUT / "probe.json").write_text(json.dumps(results, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
