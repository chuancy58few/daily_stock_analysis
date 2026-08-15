#!/usr/bin/env python3
"""Fast, bounded network probe for A-share lists and adjusted history."""

from __future__ import annotations

import json
from io import BytesIO
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import requests

OUT = Path("reports/a_share_source_probe_short")
OUT.mkdir(parents=True, exist_ok=True)
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/151 Safari/537.36"


def attempt(name: str, fn) -> Dict[str, Any]:
    try:
        value = {"ok": True, **fn()}
    except Exception as exc:  # noqa: BLE001
        value = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    print(name, json.dumps(value, ensure_ascii=False, default=str), flush=True)
    return value


def sse(stock_type: str) -> Dict[str, Any]:
    response = requests.get(
        "https://query.sse.com.cn/sseQuery/commonQuery.do",
        params={
            "STOCK_TYPE": stock_type,
            "REG_PROVINCE": "",
            "CSRC_CODE": "",
            "STOCK_CODE": "",
            "sqlId": "COMMON_SSE_CP_GPJCTPZ_GPLB_GP_L",
            "COMPANY_STATUS": "2,4,5,7,8",
            "type": "inParams",
            "isPagination": "true",
            "pageHelp.cacheSize": "1",
            "pageHelp.beginPage": "1",
            "pageHelp.pageSize": "10000",
            "pageHelp.pageNo": "1",
            "pageHelp.endPage": "1",
        },
        headers={"User-Agent": UA, "Referer": "https://www.sse.com.cn/assortment/stock/list/share/"},
        timeout=12,
    )
    response.raise_for_status()
    payload = response.json()
    rows = payload.get("result") or []
    return {"status": response.status_code, "rows": len(rows), "sample": rows[:1]}


def szse() -> Dict[str, Any]:
    response = requests.get(
        "https://www.szse.cn/api/report/ShowReport",
        params={"SHOWTYPE": "xlsx", "CATALOGID": "1110", "TABKEY": "tab1", "random": "0.6935816432433362"},
        headers={"User-Agent": UA, "Referer": "https://www.szse.cn/"},
        timeout=12,
    )
    response.raise_for_status()
    frame = pd.read_excel(BytesIO(response.content))
    return {"status": response.status_code, "bytes": len(response.content), "rows": len(frame), "columns": list(frame.columns)}


def bse() -> Dict[str, Any]:
    response = requests.post(
        "https://www.bse.cn/nqxxController/nqxxCnzq.do",
        data={"page": "0", "typejb": "T", "xxfcbj[]": "2", "xxzqdm": "", "sortfield": "xxzqdm", "sorttype": "asc"},
        headers={"User-Agent": UA, "Referer": "https://www.bse.cn/"},
        timeout=12,
    )
    response.raise_for_status()
    text = response.text
    return {"status": response.status_code, "bytes": len(response.content), "prefix": text[:250]}


def tencent(host: str) -> Dict[str, Any]:
    response = requests.get(
        f"https://{host}/appstock/app/fqkline/get",
        params={"param": "sh600000,day,2021-08-16,2026-08-14,1500,qfq"},
        headers={"User-Agent": UA, "Referer": "https://gu.qq.com/"},
        timeout=12,
    )
    response.raise_for_status()
    payload = response.json()
    item = (payload.get("data") or {}).get("sh600000") or {}
    rows = item.get("qfqday") or item.get("day") or []
    return {"status": response.status_code, "bytes": len(response.content), "rows": len(rows), "first": rows[:1], "last": rows[-1:]}


def eastmoney_history(host: str, scheme: str = "https") -> Dict[str, Any]:
    response = requests.get(
        f"{scheme}://{host}/api/qt/stock/kline/get",
        params={
            "secid": "1.600000",
            "ut": "fa5fd1943c7b386f172d6893dbfba10b",
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "klt": 101,
            "fqt": 1,
            "beg": "20210816",
            "end": "20260814",
            "lmt": 1000000,
        },
        headers={"User-Agent": UA, "Referer": "https://quote.eastmoney.com/"},
        timeout=12,
    )
    response.raise_for_status()
    payload = response.json()
    rows = ((payload.get("data") or {}).get("klines") or [])
    return {"status": response.status_code, "bytes": len(response.content), "rows": len(rows), "sample": rows[:1]}


def main() -> int:
    results = {
        "sse_main": attempt("sse_main", lambda: sse("1")),
        "sse_star": attempt("sse_star", lambda: sse("8")),
        "szse": attempt("szse", szse),
        "bse": attempt("bse", bse),
        "tencent_web": attempt("tencent_web", lambda: tencent("web.ifzq.gtimg.cn")),
        "tencent_plain": attempt("tencent_plain", lambda: tencent("ifzq.gtimg.cn")),
        "eastmoney_main": attempt("eastmoney_main", lambda: eastmoney_history("push2his.eastmoney.com")),
        "eastmoney_33": attempt("eastmoney_33", lambda: eastmoney_history("33.push2his.eastmoney.com")),
        "eastmoney_http": attempt("eastmoney_http", lambda: eastmoney_history("push2his.eastmoney.com", "http")),
    }
    (OUT / "probe.json").write_text(json.dumps(results, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
