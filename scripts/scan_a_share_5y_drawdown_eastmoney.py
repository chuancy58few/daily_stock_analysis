#!/usr/bin/env python3
"""Screen the current A-share universe for five-year peak-to-later-trough drawdowns.

Data source: Eastmoney public quote and adjusted daily-kline endpoints.
The main filter is strict-order peak/trough: the selected peak trading day must be
strictly earlier than the selected trough trading day. A 5x high/low ratio equals
an 80% decline from peak to trough.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests

LOGGER = logging.getLogger("eastmoney_a_share_drawdown")
LIST_URLS = (
    "https://82.push2.eastmoney.com/api/qt/clist/get",
    "https://push2.eastmoney.com/api/qt/clist/get",
)
KLINE_URLS = (
    "https://push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://33.push2his.eastmoney.com/api/qt/stock/kline/get",
)
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36"
)
THREAD_LOCAL = threading.local()


@dataclass(frozen=True)
class Config:
    start_date: str
    end_date: str
    threshold_ratio: float
    max_workers: int
    output_dir: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", default="20210816")
    parser.add_argument("--end-date", default="20260814")
    parser.add_argument("--threshold-ratio", type=float, default=5.0)
    parser.add_argument("--max-workers", type=int, default=18)
    parser.add_argument("--output-dir", default="reports/a_share_5y_drawdown")
    return parser.parse_args()


def setup_logging(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    LOGGER.setLevel(logging.INFO)
    LOGGER.handlers.clear()
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(formatter)
    LOGGER.addHandler(stream)
    file_handler = logging.FileHandler(output_dir / "scan.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    LOGGER.addHandler(file_handler)


def get_session() -> requests.Session:
    session = getattr(THREAD_LOCAL, "session", None)
    if session is None:
        session = requests.Session()
        session.trust_env = False
        session.headers.update(
            {
                "User-Agent": USER_AGENT,
                "Referer": "https://quote.eastmoney.com/",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Connection": "keep-alive",
            }
        )
        THREAD_LOCAL.session = session
    return session


def request_json(
    urls: Iterable[str],
    params: Dict[str, Any],
    *,
    attempts_per_url: int = 4,
    timeout: int = 25,
) -> Dict[str, Any]:
    last_error: Optional[BaseException] = None
    for url in urls:
        for attempt in range(1, attempts_per_url + 1):
            try:
                response = get_session().get(url, params=params, timeout=timeout)
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    raise RuntimeError("response is not a JSON object")
                return payload
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                wait = min(8.0, 0.45 * 2 ** (attempt - 1)) + random.random() * 0.35
                time.sleep(wait)
    raise RuntimeError(f"request failed for params={params}") from last_error


def fetch_stock_universe() -> pd.DataFrame:
    # Shenzhen main board + ChiNext, Shanghai main board + STAR, Beijing Stock Exchange.
    market_filter = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048"
    params = {
        "pn": 1,
        "pz": 10000,
        "po": 1,
        "np": 1,
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": 2,
        "invt": 2,
        "fid": "f3",
        "fs": market_filter,
        "fields": "f12,f13,f14,f20,f21,f100,f102,f103,f104,f105,f152",
    }
    payload = request_json(LIST_URLS, params, attempts_per_url=5)
    data = payload.get("data") or {}
    diff = data.get("diff") or []
    if isinstance(diff, dict):
        diff = list(diff.values())
    if not diff:
        raise RuntimeError("Eastmoney stock list returned no rows")

    records: List[Dict[str, Any]] = []
    for item in diff:
        code = str(item.get("f12", "")).zfill(6)
        market_id = item.get("f13")
        name = str(item.get("f14", "")).strip()
        if not code or market_id not in (0, 1):
            continue
        if code.startswith(("200", "900")):
            continue
        if code.startswith(("5", "15", "16", "18")):
            continue
        exchange = "SH" if market_id == 1 else ("BJ" if code.startswith(("4", "8", "9")) else "SZ")
        board = infer_board(code, exchange)
        records.append(
            {
                "证券代码": code,
                "股票代码": f"{code}.{exchange}",
                "股票名称": name,
                "市场编号": int(market_id),
                "交易所": exchange,
                "板块": board,
                "所属行业": item.get("f100") or "",
                "地区": item.get("f102") or "",
                "总市值": numeric(item.get("f20")),
                "流通市值": numeric(item.get("f21")),
            }
        )
    universe = pd.DataFrame(records).drop_duplicates("股票代码", keep="first")
    if universe.empty:
        raise RuntimeError("No A-share records remained after filtering")
    universe = universe.sort_values(["交易所", "证券代码"]).reset_index(drop=True)
    return universe


def infer_board(code: str, exchange: str) -> str:
    if exchange == "BJ":
        return "北交所"
    if code.startswith(("688", "689")):
        return "科创板"
    if code.startswith(("300", "301")):
        return "创业板"
    return "主板"


def numeric(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def fetch_kline(record: Dict[str, Any], config: Config) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    code = record["证券代码"]
    market_id = record["市场编号"]
    params = {
        "secid": f"{market_id}.{code}",
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": 101,
        "fqt": 1,
        "beg": config.start_date,
        "end": config.end_date,
        "lmt": 1000000,
    }
    try:
        payload = request_json(KLINE_URLS, params, attempts_per_url=4)
        data = payload.get("data")
        if not data or not data.get("klines"):
            return None, {"股票代码": record["股票代码"], "股票名称": record["股票名称"], "失败原因": "无日线数据"}
        result = analyze_klines(record, data, config)
        return result, None
    except Exception as exc:  # noqa: BLE001
        return None, {"股票代码": record["股票代码"], "股票名称": record["股票名称"], "失败原因": str(exc)[:500]}


def analyze_klines(record: Dict[str, Any], data: Dict[str, Any], config: Config) -> Dict[str, Any]:
    rows: List[Tuple[str, float, float, float, float, float]] = []
    invalid_rows = 0
    for line in data.get("klines", []):
        parts = str(line).split(",")
        if len(parts) < 7:
            invalid_rows += 1
            continue
        try:
            trade_date = parts[0].replace("-", "")
            close = float(parts[2])
            high = float(parts[3])
            low = float(parts[4])
            volume = float(parts[5])
            amount = float(parts[6])
        except (TypeError, ValueError):
            invalid_rows += 1
            continue
        if high <= 0 or low <= 0 or close <= 0:
            invalid_rows += 1
            continue
        rows.append((trade_date, high, low, close, volume, amount))

    if not rows:
        raise RuntimeError("日线解析后为空")
    rows.sort(key=lambda item: item[0])

    running_peak: Optional[float] = None
    running_peak_date = ""
    max_ratio = 1.0
    peak_price: Optional[float] = None
    trough_price: Optional[float] = None
    peak_date = ""
    trough_date = ""
    absolute_high = -math.inf
    absolute_high_date = ""
    absolute_low = math.inf
    absolute_low_date = ""

    for trade_date, high, low, close, volume, amount in rows:
        if high > absolute_high:
            absolute_high = high
            absolute_high_date = trade_date
        if low < absolute_low:
            absolute_low = low
            absolute_low_date = trade_date

        # Strict time order: compare today's low only with peaks from earlier days.
        if running_peak is not None:
            ratio = running_peak / low
            if ratio > max_ratio:
                max_ratio = ratio
                peak_price = running_peak
                trough_price = low
                peak_date = running_peak_date
                trough_date = trade_date
        if running_peak is None or high > running_peak:
            running_peak = high
            running_peak_date = trade_date

    last_date, _, _, latest_close, latest_volume, latest_amount = rows[-1]
    drawdown = 1.0 - 1.0 / max_ratio if max_ratio > 0 else None
    ordered = bool(peak_date and trough_date and peak_date < trough_date)
    elapsed = (
        datetime.strptime(trough_date, "%Y%m%d") - datetime.strptime(peak_date, "%Y%m%d")
    ).days if ordered else None

    output = dict(record)
    output.update(
        {
            "统计开始日": config.start_date,
            "统计结束日": config.end_date,
            "首个交易日": rows[0][0],
            "最后交易日": last_date,
            "有效交易日数": len(rows),
            "高点日期": peak_date,
            "低点日期": trough_date,
            "高点严格早于低点": ordered,
            "高低历时_自然日": elapsed,
            "前复权高点价": peak_price,
            "前复权低点价": trough_price,
            "高低倍数": max_ratio,
            "最大回撤": drawdown,
            "回撤等级": severity(max_ratio),
            "是否达到5倍": max_ratio >= config.threshold_ratio,
            "绝对最高日期": absolute_high_date,
            "绝对最低日期": absolute_low_date,
            "绝对最高前复权价": absolute_high,
            "绝对最低前复权价": absolute_low,
            "绝对高低倍数_忽略顺序": absolute_high / absolute_low if absolute_low > 0 else None,
            "最新收盘价": latest_close,
            "最新价较回撤高点": latest_close / peak_price - 1.0 if peak_price else None,
            "最新价较回撤低点": latest_close / trough_price - 1.0 if trough_price else None,
            "最新成交额": latest_amount,
            "最新成交量": latest_volume,
            "日线无效行数": invalid_rows,
            "数据源名称": data.get("name") or record["股票名称"],
        }
    )
    return output


def severity(ratio: float) -> str:
    if ratio >= 20:
        return "20倍及以上"
    if ratio >= 10:
        return "10—20倍"
    if ratio >= 7.5:
        return "7.5—10倍"
    if ratio >= 5:
        return "5—7.5倍"
    if ratio >= 3:
        return "3—5倍"
    if ratio >= 2:
        return "2—3倍"
    return "不足2倍"


def build_industry_summary(results: pd.DataFrame, threshold: float) -> pd.DataFrame:
    source = results.copy()
    source["所属行业"] = source["所属行业"].replace("", "未分类").fillna("未分类")
    summary = (
        source.groupby("所属行业", dropna=False)
        .agg(
            股票数=("股票代码", "nunique"),
            五倍回撤股票数=("高低倍数", lambda values: int((values >= threshold).sum())),
            中位高低倍数=("高低倍数", "median"),
            最大高低倍数=("高低倍数", "max"),
            中位最大回撤=("最大回撤", "median"),
        )
        .reset_index()
    )
    summary["五倍回撤占比"] = summary["五倍回撤股票数"] / summary["股票数"]
    summary = summary.sort_values(
        ["五倍回撤股票数", "五倍回撤占比", "最大高低倍数"], ascending=[False, False, False]
    ).reset_index(drop=True)
    summary.insert(0, "排名", np.arange(1, len(summary) + 1))
    return summary


def build_board_summary(results: pd.DataFrame, threshold: float) -> pd.DataFrame:
    summary = (
        results.groupby("板块", dropna=False)
        .agg(
            股票数=("股票代码", "nunique"),
            五倍回撤股票数=("高低倍数", lambda values: int((values >= threshold).sum())),
            中位高低倍数=("高低倍数", "median"),
            最大高低倍数=("高低倍数", "max"),
        )
        .reset_index()
    )
    summary["五倍回撤占比"] = summary["五倍回撤股票数"] / summary["股票数"]
    return summary.sort_values("五倍回撤股票数", ascending=False).reset_index(drop=True)


def write_outputs(
    universe: pd.DataFrame,
    results: pd.DataFrame,
    failures: pd.DataFrame,
    config: Config,
    elapsed_seconds: float,
) -> None:
    results = results.sort_values(["高低倍数", "最大回撤"], ascending=[False, False]).reset_index(drop=True)
    results.insert(0, "排名", np.arange(1, len(results) + 1))
    hits = results.loc[results["高低倍数"] >= config.threshold_ratio].copy()
    industries = build_industry_summary(results, config.threshold_ratio)
    boards = build_board_summary(results, config.threshold_ratio)

    output_map = {
        "A股近5年全市场最大回撤.csv": results,
        "A股近5年跌幅5倍名单.csv": hits,
        "A股近5年跌幅5倍_行业统计.csv": industries,
        "A股近5年跌幅5倍_板块统计.csv": boards,
        "A股当前股票池.csv": universe,
        "数据抓取失败清单.csv": failures,
    }
    for filename, frame in output_map.items():
        frame.to_csv(config.output_dir / filename, index=False, encoding="utf-8-sig")

    summary = {
        "生成时间": datetime.now().isoformat(timespec="seconds"),
        "统计区间": [config.start_date, config.end_date],
        "高低倍数阈值": config.threshold_ratio,
        "对应最大回撤": 1.0 - 1.0 / config.threshold_ratio,
        "股票池数量": int(len(universe)),
        "成功计算数量": int(len(results)),
        "失败数量": int(len(failures)),
        "达标数量": int(len(hits)),
        "达标占成功样本比例": float(len(hits) / len(results)) if len(results) else None,
        "最大高低倍数": float(results["高低倍数"].max()) if len(results) else None,
        "中位高低倍数": float(results["高低倍数"].median()) if len(results) else None,
        "运行耗时秒": elapsed_seconds,
        "口径": {
            "股票范围": "东方财富当前沪深北A股股票池，排除200开头深圳B股、900开头上海B股及基金代码。",
            "价格口径": "东方财富日线前复权（fqt=1）的日最高价与日最低价。",
            "时间顺序": "高点交易日严格早于低点交易日；同日高低不参与主筛选。",
            "筛选定义": "滚动历史最高价÷后续交易日最低价≥5.0，即最大回撤≥80%。",
            "补充字段": "同时保留忽略时间顺序的绝对高低倍数，便于复核反弹后创新高的股票。",
        },
    }
    with (config.output_dir / "筛选摘要.json").open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, ensure_ascii=False, indent=2)

    LOGGER.info(
        "Finished universe=%s success=%s failures=%s hits=%s elapsed=%.1fs",
        len(universe),
        len(results),
        len(failures),
        len(hits),
        elapsed_seconds,
    )


def main() -> int:
    args = parse_args()
    config = Config(
        start_date=args.start_date,
        end_date=args.end_date,
        threshold_ratio=args.threshold_ratio,
        max_workers=max(1, min(args.max_workers, 40)),
        output_dir=Path(args.output_dir),
    )
    setup_logging(config.output_dir)
    started = time.time()
    universe = fetch_stock_universe()
    LOGGER.info("A-share universe loaded: %s stocks", len(universe))

    records = universe.to_dict("records")
    results: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
        future_map = {executor.submit(fetch_kline, record, config): record for record in records}
        completed = 0
        for future in as_completed(future_map):
            completed += 1
            result, failure = future.result()
            if result is not None:
                results.append(result)
            if failure is not None:
                failures.append(failure)
            if completed % 100 == 0 or completed == len(records):
                provisional_hits = sum(1 for item in results if float(item["高低倍数"]) >= config.threshold_ratio)
                LOGGER.info(
                    "Progress %s/%s success=%s failures=%s provisional_hits=%s",
                    completed,
                    len(records),
                    len(results),
                    len(failures),
                    provisional_hits,
                )

    result_frame = pd.DataFrame(results)
    failure_frame = pd.DataFrame(failures, columns=["股票代码", "股票名称", "失败原因"])
    if result_frame.empty:
        raise RuntimeError("No stock was calculated successfully")
    write_outputs(universe, result_frame, failure_frame, config, time.time() - started)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
