#!/usr/bin/env python3
"""Probe public A-share universe and historical-price sources from GitHub Actions."""

from __future__ import annotations

import json
import traceback
from io import BytesIO
from pathlib import Path
from typing import Any, Callable, Dict

import pandas as pd
import requests

OUTPUT = Path("reports/a_share_source_probe")
OUTPUT.mkdir(parents=True, exist_ok=True)
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/151 Safari/537.36"


def result_ok(**kwargs: Any) -> Dict[str, Any]:
    return {"ok": True, **kwargs}


def result_error(exc: BaseException) -> Dict[str, Any]:
    return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "traceback": traceback.format_exc()[-2500:]}


def run_probe(name: str, fn: Callable[[], Dict[str, Any]], results: Dict[str, Any]) -> None:
    print(f"\n=== {name} ===", flush=True)
    try:
        value = fn()
        results[name] = value
        print(json.dumps(value, ensure_ascii=False, indent=2, default=str), flush=True)
    except Exception as exc:  # noqa: BLE001
        value = result_error(exc)
        results[name] = value
        print(json.dumps(value, ensure_ascii=False, indent=2, default=str), flush=True)


def probe_sse_list() -> Dict[str, Any]:
    url = "https://query.sse.com.cn/sseQuery/commonQuery.do"
    headers = {"Referer": "https://www.sse.com.cn/assortment/stock/list/share/", "User-Agent": UA}
    params = {
        "STOCK_TYPE": "1",
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
    }
    response = requests.get(url, params=params, headers=headers, timeout=30)
    payload = response.json()
    rows = payload.get("result") or []
    return result_ok(status=response.status_code, rows=len(rows), sample=rows[:2])


def probe_szse_list() -> Dict[str, Any]:
    url = "https://www.szse.cn/api/report/ShowReport"
    params = {"SHOWTYPE": "xlsx", "CATALOGID": "1110", "TABKEY": "tab1", "random": "0.6935816432433362"}
    response = requests.get(url, params=params, headers={"User-Agent": UA, "Referer": "https://www.szse.cn/"}, timeout=30)
    response.raise_for_status()
    frame = pd.read_excel(BytesIO(response.content))
    return result_ok(status=response.status_code, bytes=len(response.content), rows=len(frame), columns=list(frame.columns), sample=frame.head(2).to_dict("records"))


def probe_bse_list() -> Dict[str, Any]:
    url = "https://www.bse.cn/nqxxController/nqxxCnzq.do"
    payload = {"page": "0", "typejb": "T", "xxfcbj[]": "2", "xxzqdm": "", "sortfield": "xxzqdm", "sorttype": "asc"}
    response = requests.post(url, data=payload, headers={"User-Agent": UA, "Referer": "https://www.bse.cn/"}, timeout=30)
    response.raise_for_status()
    text = response.text
    left = text.find("[")
    parsed = json.loads(text[left:-1]) if left >= 0 else None
    rows = parsed[0].get("content", []) if parsed else []
    return result_ok(status=response.status_code, bytes=len(response.content), rows=len(rows), prefix=text[:200])


def probe_tencent_qfq() -> Dict[str, Any]:
    urls = [
        "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get",
        "https://ifzq.gtimg.cn/appstock/app/fqkline/get",
    ]
    outputs = []
    for url in urls:
        try:
            params = {"param": "sh600000,day,2021-08-16,2026-08-14,1500,qfq"}
            response = requests.get(url, params=params, headers={"User-Agent": UA, "Referer": "https://gu.qq.com/"}, timeout=30)
            response.raise_for_status()
            payload = response.json()
            data = payload.get("data", {}).get("sh600000", {})
            series = data.get("qfqday") or data.get("day") or []
            outputs.append({"url": url, "status": response.status_code, "rows": len(series), "sample_first": series[:1], "sample_last": series[-1:]})
        except Exception as exc:  # noqa: BLE001
            outputs.append({"url": url, "error": f"{type(exc).__name__}: {exc}"})
    return result_ok(outputs=outputs)


def probe_eastmoney_kline() -> Dict[str, Any]:
    urls = [
        "https://push2his.eastmoney.com/api/qt/stock/kline/get",
        "https://33.push2his.eastmoney.com/api/qt/stock/kline/get",
        "http://push2his.eastmoney.com/api/qt/stock/kline/get",
    ]
    outputs = []
    params = {
        "secid": "1.600000",
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": 101,
        "fqt": 1,
        "beg": "20210816",
        "end": "20260814",
        "lmt": 1000000,
    }
    for url in urls:
        try:
            response = requests.get(url, params=params, headers={"User-Agent": UA, "Referer": "https://quote.eastmoney.com/"}, timeout=30)
            response.raise_for_status()
            payload = response.json()
            series = (payload.get("data") or {}).get("klines") or []
            outputs.append({"url": url, "status": response.status_code, "rows": len(series), "sample": series[:1]})
        except Exception as exc:  # noqa: BLE001
            outputs.append({"url": url, "error": f"{type(exc).__name__}: {exc}"})
    return result_ok(outputs=outputs)


def probe_sina_kline() -> Dict[str, Any]:
    url = "https://quotes.sina.cn/cn/api/jsonp_v2.php/var%20_probe=/CN_MarketDataService.getKLineData"
    params = {"symbol": "sh600000", "scale": "240", "ma": "no", "datalen": "1500"}
    response = requests.get(url, params=params, headers={"User-Agent": UA, "Referer": "https://finance.sina.com.cn/"}, timeout=30)
    response.raise_for_status()
    return result_ok(status=response.status_code, bytes=len(response.content), prefix=response.text[:300])


def probe_baostock() -> Dict[str, Any]:
    import baostock as bs

    login = bs.login()
    try:
        query = bs.query_history_k_data_plus(
            "sh.600000",
            "date,code,open,high,low,close,volume,amount,adjustflag",
            start_date="2021-08-16",
            end_date="2026-08-14",
            frequency="d",
            adjustflag="2",
        )
        rows = []
        while query.error_code == "0" and query.next():
            rows.append(query.get_row_data())
            if len(rows) >= 3:
                break
        all_stock = bs.query_all_stock(day="2026-08-14")
        stock_rows = 0
        samples = []
        while all_stock.error_code == "0" and all_stock.next():
            stock_rows += 1
            if len(samples) < 2:
                samples.append(all_stock.get_row_data())
        return result_ok(
            login_code=login.error_code,
            login_message=login.error_msg,
            history_code=query.error_code,
            history_message=query.error_msg,
            history_sample=rows,
            all_stock_code=all_stock.error_code,
            all_stock_message=all_stock.error_msg,
            all_stock_rows=stock_rows,
            all_stock_sample=samples,
        )
    finally:
        bs.logout()


def probe_akshare() -> Dict[str, Any]:
    import akshare as ak

    outputs: Dict[str, Any] = {}
    for label, fn in (
        ("stock_info_a_code_name", lambda: ak.stock_info_a_code_name()),
        ("stock_info_sh_name_code", lambda: ak.stock_info_sh_name_code(symbol="主板A股")),
        ("stock_info_sz_name_code", lambda: ak.stock_info_sz_name_code(symbol="A股列表")),
        ("stock_info_bj_name_code", lambda: ak.stock_info_bj_name_code()),
        ("stock_zh_a_daily_sina", lambda: ak.stock_zh_a_daily(symbol="sh600000", start_date="20210816", end_date="20260814", adjust="qfq")),
    ):
        try:
            frame = fn()
            outputs[label] = {"ok": True, "rows": len(frame), "columns": list(frame.columns), "sample": frame.head(2).to_dict("records")}
        except Exception as exc:  # noqa: BLE001
            outputs[label] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    return result_ok(outputs=outputs)


def main() -> int:
    results: Dict[str, Any] = {}
    run_probe("sse_list", probe_sse_list, results)
    run_probe("szse_list", probe_szse_list, results)
    run_probe("bse_list", probe_bse_list, results)
    run_probe("tencent_qfq", probe_tencent_qfq, results)
    run_probe("eastmoney_kline", probe_eastmoney_kline, results)
    run_probe("sina_kline", probe_sina_kline, results)
    run_probe("baostock", probe_baostock, results)
    run_probe("akshare", probe_akshare, results)
    with (OUTPUT / "source_probe.json").open("w", encoding="utf-8") as handle:
        json.dump(results, handle, ensure_ascii=False, indent=2, default=str)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
