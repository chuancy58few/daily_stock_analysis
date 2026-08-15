#!/usr/bin/env python3
"""Screen the current A-share universe for five-year peak-to-later-trough declines.

Universe: current Shanghai, Shenzhen and Beijing A-shares returned by Sina's hs_a node.
Price basis: Sina raw daily bars divided by Sina's cumulative qfq factor.
Ordering rule: the selected peak trading day must be strictly earlier than the trough day.
Threshold: peak/trough >= 5.0, equivalent to a decline of at least 80%.
"""

from __future__ import annotations

import argparse
import bisect
import json
import logging
import math
import os
import random
import re
import sys
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import requests

LOGGER = logging.getLogger("a_share_5y_drawdown")
USER_AGENTS = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/151.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/151.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/151.0 Safari/537.36",
)
SINA_COUNT_URL = (
    "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/"
    "Market_Center.getHQNodeStockCount"
)
SINA_LIST_URL = (
    "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/"
    "Market_Center.getHQNodeData"
)
SINA_DAILY_URL = (
    "https://quotes.sina.cn/cn/api/jsonp_v2.php/var%20_{callback}=/"
    "CN_MarketDataService.getKLineData"
)
SINA_QFQ_URLS = (
    "https://finance.sina.com.cn/realstock/company/{symbol}/qfq.js",
    "http://finance.sina.com.cn/realstock/company/{symbol}/qfq.js",
)
THREAD_LOCAL = threading.local()
CHECKPOINT_LOCK = threading.Lock()


@dataclass(frozen=True)
class Config:
    start_date: str
    end_date: str
    threshold_ratio: float
    output_dir: Path
    checkpoint_dir: Path
    max_workers: int
    max_requests_per_second: float
    retry_rounds: int
    limit: int

    @property
    def key(self) -> str:
        return (
            f"sina-current-hs-a-qfq-v1|{self.start_date}|{self.end_date}|"
            f"{self.threshold_ratio:.8f}|strict-order"
        )


class GlobalRateLimiter:
    """Serialize request starts to cap aggregate requests per second."""

    def __init__(self, requests_per_second: float) -> None:
        self.interval = 1.0 / max(0.1, requests_per_second)
        self.lock = threading.Lock()
        self.next_allowed = 0.0

    def wait(self) -> None:
        with self.lock:
            now = time.monotonic()
            wait_seconds = max(0.0, self.next_allowed - now)
            if wait_seconds:
                time.sleep(wait_seconds)
            self.next_allowed = max(self.next_allowed, time.monotonic()) + self.interval


RATE_LIMITER: Optional[GlobalRateLimiter] = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", default="20210816")
    parser.add_argument("--end-date", default="20260814")
    parser.add_argument("--threshold-ratio", type=float, default=5.0)
    parser.add_argument("--output-dir", default="reports/a_share_5y_drawdown")
    parser.add_argument("--checkpoint-dir", default=".cache/a_share_5y_drawdown_sina_qfq")
    parser.add_argument("--max-workers", type=int, default=10)
    parser.add_argument("--max-requests-per-second", type=float, default=10.0)
    parser.add_argument("--retry-rounds", type=int, default=2)
    parser.add_argument("--limit", type=int, default=0, help="For validation only; 0 means full universe")
    return parser.parse_args()


def configure_logging(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    LOGGER.setLevel(logging.INFO)
    LOGGER.handlers.clear()
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(formatter)
    LOGGER.addHandler(stream)
    file_handler = logging.FileHandler(output_dir / "运行日志.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    LOGGER.addHandler(file_handler)


def get_session() -> requests.Session:
    session = getattr(THREAD_LOCAL, "session", None)
    if session is None:
        session = requests.Session()
        session.trust_env = False
        session.headers.update(
            {
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Connection": "keep-alive",
            }
        )
        THREAD_LOCAL.session = session
    return session


def request_text(
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    referer: str,
    attempts: int = 5,
    connect_timeout: int = 8,
    read_timeout: int = 30,
) -> str:
    last_error: Optional[BaseException] = None
    for attempt in range(1, attempts + 1):
        try:
            if RATE_LIMITER is not None:
                RATE_LIMITER.wait()
            session = get_session()
            headers = {"Referer": referer, "User-Agent": random.choice(USER_AGENTS)}
            response = session.get(
                url,
                params=params,
                headers=headers,
                timeout=(connect_timeout, read_timeout),
                allow_redirects=True,
            )
            if response.status_code in (403, 408, 425, 429) or response.status_code >= 500:
                raise RuntimeError(f"HTTP {response.status_code}")
            response.raise_for_status()
            if not response.text.strip():
                raise RuntimeError("empty response")
            return response.text
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            wait_seconds = min(25.0, 0.9 * 2 ** (attempt - 1)) + random.random() * 0.8
            LOGGER.debug(
                "Request failed url=%s attempt=%s/%s error=%s sleep=%.1fs",
                url,
                attempt,
                attempts,
                exc,
                wait_seconds,
            )
            time.sleep(wait_seconds)
    raise RuntimeError(f"request failed after {attempts} attempts: {url}") from last_error


def parse_numeric(value: Any) -> Optional[float]:
    if value in (None, "", "--", "-"):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def clean_date(value: Any) -> str:
    text = str(value or "").strip()[:10].replace("-", "").replace("/", "")
    return text if len(text) == 8 and text.isdigit() else ""


def infer_exchange_and_board(symbol: str, code: str) -> Tuple[str, str]:
    if symbol.startswith("bj"):
        return "北交所", "北交所"
    if symbol.startswith("sh"):
        if code.startswith(("688", "689")):
            return "上交所", "科创板"
        return "上交所", "沪市主板"
    if code.startswith(("300", "301")):
        return "深交所", "创业板"
    return "深交所", "深市主板"


def fetch_universe() -> pd.DataFrame:
    count_text = request_text(
        SINA_COUNT_URL,
        params={"node": "hs_a"},
        referer="https://finance.sina.com.cn/stock/",
        attempts=6,
    )
    count_matches = re.findall(r"\d+", count_text)
    if not count_matches:
        raise RuntimeError(f"unable to parse hs_a count: {count_text[:200]}")
    expected_count = int(count_matches[0])
    page_size = 100
    page_count = math.ceil(expected_count / page_size)
    records: List[Dict[str, Any]] = []

    for page in range(1, page_count + 1):
        text = request_text(
            SINA_LIST_URL,
            params={
                "page": page,
                "num": page_size,
                "sort": "symbol",
                "asc": 1,
                "node": "hs_a",
                "symbol": "",
                "_s_r_a": "page",
            },
            referer="https://finance.sina.com.cn/stock/",
            attempts=6,
        )
        try:
            rows = json.loads(text)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"universe page {page} is not valid JSON") from exc
        if not isinstance(rows, list):
            raise RuntimeError(f"universe page {page} returned {type(rows).__name__}")

        for item in rows:
            symbol = str(item.get("symbol", "")).lower().strip()
            code = str(item.get("code", "")).zfill(6)
            if not symbol.startswith(("sh", "sz", "bj")) or len(code) != 6:
                continue
            exchange, board = infer_exchange_and_board(symbol, code)
            name = str(item.get("name", "")).strip()
            records.append(
                {
                    "股票代码": symbol,
                    "证券代码": code,
                    "股票名称": name,
                    "交易所": exchange,
                    "板块": board,
                    "是否ST": "ST" in name.upper(),
                    "最新价_列表": parse_numeric(item.get("trade")),
                    "涨跌幅_列表": parse_numeric(item.get("changepercent")),
                    "昨收_列表": parse_numeric(item.get("settlement")),
                    "今开_列表": parse_numeric(item.get("open")),
                    "最高_列表": parse_numeric(item.get("high")),
                    "最低_列表": parse_numeric(item.get("low")),
                    "成交量_列表": parse_numeric(item.get("volume")),
                    "成交额_列表": parse_numeric(item.get("amount")),
                    "市盈率_列表": parse_numeric(item.get("per")),
                    "市净率_列表": parse_numeric(item.get("pb")),
                    "总市值_新浪原始字段": parse_numeric(item.get("mktcap")),
                    "流通市值_新浪原始字段": parse_numeric(item.get("nmc")),
                    "换手率_列表": parse_numeric(item.get("turnoverratio")),
                    "行情时间_列表": str(item.get("ticktime", "")),
                }
            )
        if page % 10 == 0 or page == page_count:
            LOGGER.info("Universe progress %s/%s pages; collected=%s", page, page_count, len(records))

    universe = pd.DataFrame(records).drop_duplicates("股票代码", keep="first")
    universe = universe.sort_values(["交易所", "证券代码"]).reset_index(drop=True)
    if len(universe) < max(1, expected_count - 5):
        raise RuntimeError(
            f"universe appears incomplete: expected={expected_count}, collected={len(universe)}"
        )
    LOGGER.info("Universe loaded: expected=%s unique=%s", expected_count, len(universe))
    return universe


def parse_jsonp_array(text: str) -> List[Dict[str, Any]]:
    left = text.find("[")
    right = text.rfind("]")
    if left < 0 or right <= left:
        raise RuntimeError("JSONP response contains no array")
    value = json.loads(text[left : right + 1])
    if not isinstance(value, list):
        raise RuntimeError("JSONP payload is not a list")
    return value


def fetch_raw_daily(symbol: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    callback = f"kline_{symbol}_{random.randint(100000, 999999)}"
    text = request_text(
        SINA_DAILY_URL.format(callback=callback),
        params={"symbol": symbol, "scale": "240", "ma": "no", "datalen": "1500"},
        referer=f"https://finance.sina.com.cn/realstock/company/{symbol}/nc.shtml",
        attempts=6,
        read_timeout=35,
    )
    rows = parse_jsonp_array(text)
    output: List[Dict[str, Any]] = []
    for item in rows:
        trade_date = clean_date(item.get("day"))
        if not trade_date or trade_date < start_date or trade_date > end_date:
            continue
        values = {
            "date": trade_date,
            "open": parse_numeric(item.get("open")),
            "high": parse_numeric(item.get("high")),
            "low": parse_numeric(item.get("low")),
            "close": parse_numeric(item.get("close")),
            "volume": parse_numeric(item.get("volume")),
        }
        if any(values[field] is None or values[field] <= 0 for field in ("open", "high", "low", "close")):
            continue
        output.append(values)
    output.sort(key=lambda row: row["date"])
    if not output:
        raise RuntimeError("no valid daily bars inside the requested window")
    return output


def parse_qfq_payload(text: str) -> List[Tuple[str, float]]:
    if "=" not in text:
        raise RuntimeError("qfq response contains no assignment")
    payload_text = text.split("=", 1)[1].split("\n", 1)[0].strip().rstrip(";")
    payload = json.loads(payload_text)
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, list) or not data:
        raise RuntimeError("qfq factor list is empty")
    factors: List[Tuple[str, float]] = []
    for item in data:
        event_date = clean_date(item.get("d"))
        factor = parse_numeric(item.get("f"))
        if event_date and factor is not None and factor > 0:
            factors.append((event_date, factor))
    if not factors:
        raise RuntimeError("qfq factor list has no valid rows")
    factors.sort(key=lambda value: value[0])
    return factors


def fetch_qfq_factors(symbol: str) -> List[Tuple[str, float]]:
    last_error: Optional[BaseException] = None
    for template in SINA_QFQ_URLS:
        url = template.format(symbol=symbol)
        try:
            text = request_text(
                url,
                referer=f"https://finance.sina.com.cn/realstock/company/{symbol}/nc.shtml",
                attempts=4,
                read_timeout=25,
            )
            return parse_qfq_payload(text)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
    raise RuntimeError("qfq factor request failed on all endpoints") from last_error


def apply_qfq(
    raw_rows: Sequence[Dict[str, Any]],
    factors: Sequence[Tuple[str, float]],
) -> List[Dict[str, Any]]:
    event_dates = [item[0] for item in factors]
    event_factors = [item[1] for item in factors]
    adjusted: List[Dict[str, Any]] = []
    for raw in raw_rows:
        index = bisect.bisect_right(event_dates, raw["date"]) - 1
        if index < 0:
            index = 0
        factor = event_factors[index]
        adjusted.append(
            {
                **raw,
                "factor": factor,
                "qfq_open": raw["open"] / factor,
                "qfq_high": raw["high"] / factor,
                "qfq_low": raw["low"] / factor,
                "qfq_close": raw["close"] / factor,
            }
        )
    return adjusted


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


def days_between(start_date: str, end_date: str) -> Optional[int]:
    if not start_date or not end_date:
        return None
    return (
        datetime.strptime(end_date, "%Y%m%d") - datetime.strptime(start_date, "%Y%m%d")
    ).days


def analyze_record(record: Dict[str, Any], config: Config) -> Dict[str, Any]:
    symbol = record["股票代码"]
    raw_rows = fetch_raw_daily(symbol, config.start_date, config.end_date)
    factors = fetch_qfq_factors(symbol)
    rows = apply_qfq(raw_rows, factors)

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

    for row in rows:
        high = float(row["qfq_high"])
        low = float(row["qfq_low"])
        trade_date = row["date"]

        if high > absolute_high:
            absolute_high = high
            absolute_high_date = trade_date
        if low < absolute_low:
            absolute_low = low
            absolute_low_date = trade_date

        # Strict ordering: today's low is compared only with peaks from earlier trading days.
        if running_peak is not None and low > 0:
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

    last = rows[-1]
    latest_close = float(last["qfq_close"])
    drawdown = 1.0 - 1.0 / max_ratio if max_ratio > 0 else None
    latest_list_price = record.get("最新价_列表")
    latest_gap = (
        latest_list_price / latest_close - 1.0
        if latest_list_price is not None and latest_close > 0
        else None
    )
    first_date = rows[0]["date"]
    history_status = "完整覆盖统计起点" if first_date <= config.start_date else "统计期内上市/数据起点较晚"
    event_count = sum(1 for event_date, _ in factors if event_date != "19000101")

    output = dict(record)
    output.update(
        {
            "统计开始日": config.start_date,
            "统计结束日": config.end_date,
            "首个交易日": first_date,
            "最后交易日": last["date"],
            "有效交易日数": len(rows),
            "历史覆盖状态": history_status,
            "高点日期": peak_date,
            "低点日期": trough_date,
            "高点严格早于低点": bool(peak_date and trough_date and peak_date < trough_date),
            "高低历时_自然日": days_between(peak_date, trough_date),
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
            "最新前复权收盘价": latest_close,
            "最新价较回撤高点": latest_close / peak_price - 1.0 if peak_price else None,
            "最新价较回撤低点": latest_close / trough_price - 1.0 if trough_price else None,
            "列表价与最后收盘差异": latest_gap,
            "最后交易日复权因子": last["factor"],
            "前复权因子事件数": event_count,
            "最早复权因子日期": factors[0][0],
            "最新复权因子日期": factors[-1][0],
            "数据源": "新浪A股列表 + 新浪日线 + 新浪qfq因子",
        }
    )
    return output


def checkpoint_paths(config: Config) -> Tuple[Path, Path]:
    return config.checkpoint_dir / "meta.json", config.checkpoint_dir / "success.jsonl"


def load_checkpoint(config: Config) -> Dict[str, Dict[str, Any]]:
    meta_path, success_path = checkpoint_paths(config)
    if not meta_path.exists() or not success_path.exists():
        return {}
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        if meta.get("config_key") != config.key:
            LOGGER.warning("Ignoring checkpoint with a different configuration")
            return {}
        results: Dict[str, Dict[str, Any]] = {}
        for line in success_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                item = json.loads(line)
                results[str(item["股票代码"])] = item
            except Exception:  # noqa: BLE001
                continue
        LOGGER.info("Checkpoint loaded: %s completed symbols", len(results))
        return results
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Unable to load checkpoint: %s", exc)
        return {}


def initialize_checkpoint(config: Config) -> None:
    config.checkpoint_dir.mkdir(parents=True, exist_ok=True)
    meta_path, success_path = checkpoint_paths(config)
    meta_path.write_text(
        json.dumps(
            {
                "config_key": config.key,
                "start_date": config.start_date,
                "end_date": config.end_date,
                "method": "Sina raw daily divided by Sina qfq factor; strict peak-before-trough",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    success_path.touch(exist_ok=True)


def append_checkpoint(config: Config, result: Dict[str, Any]) -> None:
    _, success_path = checkpoint_paths(config)
    serialized = json.dumps(result, ensure_ascii=False, separators=(",", ":"))
    with CHECKPOINT_LOCK:
        with success_path.open("a", encoding="utf-8") as handle:
            handle.write(serialized + "\n")
            handle.flush()
            os.fsync(handle.fileno())


def run_batch(
    records: Sequence[Dict[str, Any]],
    config: Config,
    completed: Dict[str, Dict[str, Any]],
    workers: int,
    round_label: str,
) -> Dict[str, str]:
    failures: Dict[str, str] = {}
    if not records:
        return failures

    LOGGER.info("Starting %s: symbols=%s workers=%s", round_label, len(records), workers)
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="sina-qfq") as executor:
        future_map: Dict[Future[Dict[str, Any]], Dict[str, Any]] = {
            executor.submit(analyze_record, record, config): record for record in records
        }
        finished = 0
        for future in as_completed(future_map):
            record = future_map[future]
            symbol = record["股票代码"]
            finished += 1
            try:
                result = future.result()
                completed[symbol] = result
                append_checkpoint(config, result)
            except Exception as exc:  # noqa: BLE001
                failures[symbol] = f"{type(exc).__name__}: {exc}"
            if finished % 100 == 0 or finished == len(records):
                hits = sum(1 for item in completed.values() if float(item.get("高低倍数", 1.0)) >= config.threshold_ratio)
                LOGGER.info(
                    "%s progress %s/%s; total_success=%s; round_failures=%s; provisional_hits=%s",
                    round_label,
                    finished,
                    len(records),
                    len(completed),
                    len(failures),
                    hits,
                )
    return failures


def build_board_summary(results: pd.DataFrame, threshold: float) -> pd.DataFrame:
    summary = (
        results.groupby(["交易所", "板块"], dropna=False)
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
    return summary.sort_values(
        ["五倍回撤股票数", "五倍回撤占比"], ascending=[False, False]
    ).reset_index(drop=True)


def build_distribution(results: pd.DataFrame) -> pd.DataFrame:
    thresholds = [
        ("跌幅不足50%", 1.0, 2.0),
        ("跌幅50%—66.7%", 2.0, 3.0),
        ("跌幅66.7%—80%", 3.0, 5.0),
        ("跌幅80%—86.7%", 5.0, 7.5),
        ("跌幅86.7%—90%", 7.5, 10.0),
        ("跌幅90%—95%", 10.0, 20.0),
        ("跌幅95%以上", 20.0, math.inf),
    ]
    rows = []
    total = len(results)
    for label, lower, upper in thresholds:
        mask = (results["高低倍数"] >= lower) & (results["高低倍数"] < upper)
        count = int(mask.sum())
        rows.append(
            {
                "区间": label,
                "高低倍数下限": lower,
                "高低倍数上限": None if math.isinf(upper) else upper,
                "股票数": count,
                "占比": count / total if total else None,
            }
        )
    return pd.DataFrame(rows)


def write_outputs(
    universe: pd.DataFrame,
    completed: Dict[str, Dict[str, Any]],
    final_failures: Dict[str, str],
    config: Config,
    elapsed_seconds: float,
) -> Dict[str, Any]:
    config.output_dir.mkdir(parents=True, exist_ok=True)
    result = pd.DataFrame(completed.values())
    if result.empty:
        raise RuntimeError("No stock-level result was generated")
    result = result.sort_values(["高低倍数", "最大回撤"], ascending=[False, False]).reset_index(drop=True)
    result.insert(0, "排名", np.arange(1, len(result) + 1))
    hits = result.loc[result["高低倍数"] >= config.threshold_ratio].copy()
    board_summary = build_board_summary(result, config.threshold_ratio)
    distribution = build_distribution(result)

    universe_map = universe.set_index("股票代码").to_dict("index")
    failure_rows = []
    for symbol, reason in sorted(final_failures.items()):
        meta = universe_map.get(symbol, {})
        failure_rows.append(
            {
                "股票代码": symbol,
                "证券代码": meta.get("证券代码", ""),
                "股票名称": meta.get("股票名称", ""),
                "交易所": meta.get("交易所", ""),
                "板块": meta.get("板块", ""),
                "失败原因": reason,
            }
        )
    failures = pd.DataFrame(
        failure_rows,
        columns=["股票代码", "证券代码", "股票名称", "交易所", "板块", "失败原因"],
    )

    output_frames = {
        "A股近5年全市场最大回撤.csv": result,
        "A股近5年跌幅5倍名单.csv": hits,
        "A股当前股票池.csv": universe,
        "A股近5年跌幅5倍_板块统计.csv": board_summary,
        "A股近5年回撤分布.csv": distribution,
        "数据抓取失败清单.csv": failures,
    }
    for filename, frame in output_frames.items():
        frame.to_csv(config.output_dir / filename, index=False, encoding="utf-8-sig")

    success_rate = len(result) / len(universe) if len(universe) else 0.0
    summary = {
        "生成时间": datetime.now().isoformat(timespec="seconds"),
        "统计开始日": config.start_date,
        "统计结束日": config.end_date,
        "筛选阈值_高低倍数": config.threshold_ratio,
        "对应最大回撤": 1.0 - 1.0 / config.threshold_ratio,
        "当前A股股票池数量": int(len(universe)),
        "成功计算数量": int(len(result)),
        "失败数量": int(len(failures)),
        "成功率": success_rate,
        "跌幅5倍达标数量": int(len(hits)),
        "达标占成功样本比例": float(len(hits) / len(result)) if len(result) else None,
        "全市场最大高低倍数": float(result["高低倍数"].max()),
        "全市场中位高低倍数": float(result["高低倍数"].median()),
        "运行耗时秒": elapsed_seconds,
        "运行参数": {
            "并发数": config.max_workers,
            "全局请求速率上限": config.max_requests_per_second,
            "失败重试轮数": config.retry_rounds,
            "limit": config.limit,
        },
        "统计口径": {
            "股票范围": "新浪hs_a节点当前沪深北A股；属于当前上市股票截面，不包含已退市股票。",
            "价格口径": "新浪原始日最高/最低价除以新浪累计qfq因子，属于乘法前复权口径。",
            "时间顺序": "高点交易日严格早于低点交易日；同日高低不进入主筛选。",
            "五倍定义": "前复权高点价÷其后最低前复权价≥5.0，对应最大跌幅≥80%。",
            "上市不足五年": "统计期内新上市公司按上市后的全部可得交易日计算。",
            "北交所说明": "使用当前920代码连续历史；部分公司历史包含北交所开市前精选层交易记录。",
            "市值字段": "股票池中的总市值/流通市值保留新浪接口原始字段，未用于筛选。",
        },
    }
    (config.output_dir / "筛选摘要.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    LOGGER.info(
        "Output complete universe=%s success=%s failures=%s hits=%s success_rate=%.4f elapsed=%.1fs",
        len(universe),
        len(result),
        len(failures),
        len(hits),
        success_rate,
        elapsed_seconds,
    )
    return summary


def main() -> int:
    args = parse_args()
    config = Config(
        start_date=args.start_date,
        end_date=args.end_date,
        threshold_ratio=args.threshold_ratio,
        output_dir=Path(args.output_dir),
        checkpoint_dir=Path(args.checkpoint_dir),
        max_workers=max(1, min(args.max_workers, 24)),
        max_requests_per_second=max(0.5, args.max_requests_per_second),
        retry_rounds=max(0, args.retry_rounds),
        limit=max(0, args.limit),
    )
    configure_logging(config.output_dir)
    global RATE_LIMITER
    RATE_LIMITER = GlobalRateLimiter(config.max_requests_per_second)
    started = time.time()

    universe = fetch_universe()
    if config.limit:
        universe = universe.head(config.limit).copy()
        LOGGER.warning("Validation limit is active: %s symbols", len(universe))

    initialize_checkpoint(config)
    completed = load_checkpoint(config)
    valid_symbols = set(universe["股票代码"].astype(str))
    completed = {symbol: value for symbol, value in completed.items() if symbol in valid_symbols}
    records_by_symbol = {record["股票代码"]: record for record in universe.to_dict("records")}
    pending = [record for symbol, record in records_by_symbol.items() if symbol not in completed]

    failures = run_batch(pending, config, completed, config.max_workers, "首轮")
    for retry_round in range(1, config.retry_rounds + 1):
        retry_symbols = [symbol for symbol in failures if symbol not in completed]
        if not retry_symbols:
            break
        LOGGER.warning(
            "Retry round %s/%s begins after cooling down; symbols=%s",
            retry_round,
            config.retry_rounds,
            len(retry_symbols),
        )
        time.sleep(min(30, 8 * retry_round))
        retry_records = [records_by_symbol[symbol] for symbol in retry_symbols]
        failures = run_batch(
            retry_records,
            config,
            completed,
            max(2, min(4, config.max_workers)),
            f"重试{retry_round}",
        )

    final_failures = {
        symbol: failures.get(symbol, "未成功完成")
        for symbol in records_by_symbol
        if symbol not in completed
    }
    summary = write_outputs(
        universe,
        completed,
        final_failures,
        config,
        time.time() - started,
    )
    if float(summary["成功率"]) < 0.95:
        LOGGER.error("Success rate is below 95%%; marking the run as failed after preserving outputs")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
