#!/usr/bin/env python3
"""Find A/HK stocks with a long decline -> spike -> deep fall -> second spike pattern.

The pattern is calibrated against HK 03839 (CPBIO / 正大生物) using monthly closes.
Universe:
- Current Shanghai, Shenzhen and Beijing A-shares from Sina hs_a.
- Current Hong Kong listed shares from Sina qbgg_hk.
Price history:
- Tencent monthly K-lines. A-shares use qfqmonth when available.
- A-share Tencent failures fall back to Sina daily bars aggregated to months and
  adjusted with Sina qfq factors.

This script is a technical-pattern screen, not an investment recommendation.
"""

from __future__ import annotations

import argparse
import bisect
import csv
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

import requests

LOGGER = logging.getLogger("ah_double_spike")
THREAD_LOCAL = threading.local()
CHECKPOINT_LOCK = threading.Lock()
USER_AGENTS = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/151.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/151.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/151.0 Safari/537.36",
)

SINA_A_COUNT = "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeStockCount"
SINA_A_LIST = "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData"
SINA_HK_COUNT = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHKStockCount"
SINA_HK_LIST = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHKStockData"
TENCENT_KLINE = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
SINA_DAILY = "https://quotes.sina.cn/cn/api/jsonp_v2.php/var%20_{callback}=/CN_MarketDataService.getKLineData"
SINA_QFQ = (
    "https://finance.sina.com.cn/realstock/company/{symbol}/qfq.js",
    "http://finance.sina.com.cn/realstock/company/{symbol}/qfq.js",
)

TARGET_SYMBOL = "hk03839"


@dataclass(frozen=True)
class Config:
    output_dir: Path
    checkpoint_dir: Path
    workers: int
    requests_per_second: float
    limit_a: int
    limit_hk: int
    history_count: int
    min_history_months: int
    top_series_count: int

    @property
    def key(self) -> str:
        return (
            "ah-double-spike-monthly-v2|"
            f"{self.history_count}|{self.min_history_months}|strict-continuity"
        )


class GlobalRateLimiter:
    def __init__(self, requests_per_second: float) -> None:
        self.interval = 1.0 / max(0.1, requests_per_second)
        self.lock = threading.Lock()
        self.next_allowed = 0.0

    def wait(self) -> None:
        with self.lock:
            now = time.monotonic()
            wait = max(0.0, self.next_allowed - now)
            if wait:
                time.sleep(wait)
            self.next_allowed = max(self.next_allowed, time.monotonic()) + self.interval


RATE_LIMITER: Optional[GlobalRateLimiter] = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default="reports/ah_double_spike_pattern")
    parser.add_argument("--checkpoint-dir", default=".cache/ah_double_spike_pattern")
    parser.add_argument("--workers", type=int, default=14)
    parser.add_argument("--requests-per-second", type=float, default=12.0)
    parser.add_argument("--limit-a", type=int, default=0, help="Validation only; 0 means full A universe")
    parser.add_argument("--limit-hk", type=int, default=0, help="Validation only; 0 means full HK universe")
    parser.add_argument("--history-count", type=int, default=260)
    parser.add_argument("--min-history-months", type=int, default=60)
    parser.add_argument("--top-series-count", type=int, default=30)
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
    read_timeout: int = 30,
) -> str:
    last_error: Optional[BaseException] = None
    for attempt in range(1, attempts + 1):
        try:
            if RATE_LIMITER is not None:
                RATE_LIMITER.wait()
            headers = {"Referer": referer, "User-Agent": random.choice(USER_AGENTS)}
            response = get_session().get(
                url,
                params=params,
                headers=headers,
                timeout=(8, read_timeout),
                allow_redirects=True,
            )
            if response.status_code in (403, 408, 425, 429) or response.status_code >= 500:
                raise RuntimeError(f"HTTP {response.status_code}")
            response.raise_for_status()
            text = response.text
            if not text.strip():
                raise RuntimeError("empty response")
            return text
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            sleep = min(20.0, 0.8 * 2 ** (attempt - 1)) + random.random() * 0.7
            time.sleep(sleep)
    raise RuntimeError(f"request failed after {attempts} attempts: {url}") from last_error


def request_json(
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    referer: str,
    attempts: int = 5,
    read_timeout: int = 30,
) -> Any:
    text = request_text(
        url,
        params=params,
        referer=referer,
        attempts=attempts,
        read_timeout=read_timeout,
    )
    return json.loads(text)


def parse_number(value: Any) -> Optional[float]:
    if value in (None, "", "--", "-"):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def month_number(date_text: str) -> int:
    dt = datetime.strptime(date_text[:10], "%Y-%m-%d")
    return dt.year * 12 + dt.month


def months_between(date1: str, date2: str) -> int:
    return month_number(date2) - month_number(date1)


def infer_a_exchange_board(symbol: str, code: str) -> Tuple[str, str]:
    if symbol.startswith("bj"):
        return "A股", "北交所"
    if symbol.startswith("sh"):
        return "A股", "科创板" if code.startswith(("688", "689")) else "沪市主板"
    return "A股", "创业板" if code.startswith(("300", "301")) else "深市主板"


def fetch_a_universe() -> List[Dict[str, Any]]:
    raw_count = request_text(
        SINA_A_COUNT,
        params={"node": "hs_a"},
        referer="https://finance.sina.com.cn/stock/",
        attempts=6,
    )
    matches = re.findall(r"\d+", raw_count)
    if not matches:
        raise RuntimeError("unable to parse A-share count")
    expected = int(matches[0])
    page_size = 100
    pages = math.ceil(expected / page_size)
    records: List[Dict[str, Any]] = []
    for page in range(1, pages + 1):
        rows = request_json(
            SINA_A_LIST,
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
        if not isinstance(rows, list):
            raise RuntimeError(f"A-share page {page} is not a list")
        for item in rows:
            symbol = str(item.get("symbol", "")).lower().strip()
            code = str(item.get("code", "")).zfill(6)
            if not symbol.startswith(("sh", "sz", "bj")) or len(code) != 6:
                continue
            market, board = infer_a_exchange_board(symbol, code)
            name = str(item.get("name", "")).strip()
            records.append(
                {
                    "市场": market,
                    "板块": board,
                    "行情代码": symbol,
                    "证券代码": code,
                    "股票名称": name,
                    "是否ST": "ST" in name.upper(),
                    "最新价": parse_number(item.get("trade")),
                    "最新成交额": parse_number(item.get("amount")),
                    "最新成交量": parse_number(item.get("volume")),
                    "市盈率": parse_number(item.get("per")),
                    "市净率": parse_number(item.get("pb")),
                    "总市值_原始": parse_number(item.get("mktcap")),
                    "行情时间": str(item.get("ticktime", "")),
                }
            )
        if page % 10 == 0 or page == pages:
            LOGGER.info("A universe %s/%s pages, collected=%s", page, pages, len(records))
    unique = {record["行情代码"]: record for record in records}
    output = sorted(unique.values(), key=lambda value: value["行情代码"])
    if len(output) < expected - 5:
        raise RuntimeError(f"A universe incomplete: expected={expected} actual={len(output)}")
    return output


def fetch_hk_universe() -> List[Dict[str, Any]]:
    raw_count = request_text(
        SINA_HK_COUNT,
        params={"node": "qbgg_hk"},
        referer="https://finance.sina.com.cn/stock/hkstock/",
        attempts=6,
    )
    matches = re.findall(r"\d+", raw_count)
    if not matches:
        raise RuntimeError("unable to parse HK stock count")
    expected = int(matches[0])
    page_size = 60
    pages = math.ceil(expected / page_size)
    records: List[Dict[str, Any]] = []
    for page in range(1, pages + 1):
        rows = request_json(
            SINA_HK_LIST,
            params={
                "page": page,
                "num": page_size,
                "sort": "symbol",
                "asc": 1,
                "node": "qbgg_hk",
                "_s_r_a": "page",
            },
            referer="https://finance.sina.com.cn/stock/hkstock/",
            attempts=6,
        )
        if not isinstance(rows, list):
            raise RuntimeError(f"HK page {page} is not a list")
        for item in rows:
            code = str(item.get("symbol", "")).strip().zfill(5)
            if len(code) != 5 or not code.isdigit():
                continue
            name = str(item.get("name", "")).strip()
            records.append(
                {
                    "市场": "港股",
                    "板块": "香港主板/创业板",
                    "行情代码": f"hk{code}",
                    "证券代码": code,
                    "股票名称": name,
                    "是否ST": False,
                    "最新价": parse_number(item.get("lasttrade")),
                    "最新成交额": parse_number(item.get("amount")),
                    "最新成交量": parse_number(item.get("volume")),
                    "市盈率": parse_number(item.get("pe_ratio")),
                    "市净率": None,
                    "总市值_原始": parse_number(item.get("market_value")),
                    "行情时间": str(item.get("ticktime", "")),
                }
            )
        if page % 10 == 0 or page == pages:
            LOGGER.info("HK universe %s/%s pages, collected=%s", page, pages, len(records))
    unique = {record["行情代码"]: record for record in records}
    output = sorted(unique.values(), key=lambda value: value["行情代码"])
    if len(output) < expected - 5:
        raise RuntimeError(f"HK universe incomplete: expected={expected} actual={len(output)}")
    return output


def parse_tencent_rows(rows: Sequence[Any]) -> List[Dict[str, Any]]:
    parsed: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) < 5:
            continue
        date = str(row[0])[:10]
        try:
            datetime.strptime(date, "%Y-%m-%d")
            open_price = float(row[1])
            close_price = float(row[2])
            high = float(row[3])
            low = float(row[4])
            volume = float(row[5]) if len(row) > 5 and row[5] not in (None, "") else 0.0
        except (TypeError, ValueError):
            continue
        if min(open_price, close_price, high, low) <= 0:
            continue
        parsed[date] = {
            "date": date,
            "open": open_price,
            "close": close_price,
            "high": high,
            "low": low,
            "volume": volume,
        }
    output = sorted(parsed.values(), key=lambda value: value["date"])
    return output


def keep_latest_continuous_segment(rows: List[Dict[str, Any]], max_gap_months: int = 18) -> Tuple[List[Dict[str, Any]], int]:
    if len(rows) < 2:
        return rows, 0
    last_break = -1
    break_count = 0
    for index in range(1, len(rows)):
        gap = months_between(rows[index - 1]["date"], rows[index]["date"])
        if gap > max_gap_months:
            last_break = index
            break_count += 1
    if last_break >= 0:
        return rows[last_break:], break_count
    return rows, break_count


def fetch_tencent_monthly(symbol: str, count: int) -> Tuple[List[Dict[str, Any]], str, int]:
    code = symbol.lower()
    payload = request_json(
        TENCENT_KLINE,
        params={"param": f"{code},month,,,{count},qfq"},
        referer="https://gu.qq.com/",
        attempts=5,
        read_timeout=30,
    )
    if not isinstance(payload, dict) or int(payload.get("code", 0)) != 0:
        raise RuntimeError("Tencent returned a non-zero code")
    root = (payload.get("data") or {}).get(code)
    if not isinstance(root, dict):
        raise RuntimeError("Tencent response has no symbol root")
    rows: List[Any] = []
    price_basis = "腾讯月线（未明确复权）"
    if isinstance(root.get("qfqmonth"), list) and root.get("qfqmonth"):
        rows = root["qfqmonth"]
        price_basis = "腾讯前复权月线"
    elif isinstance(root.get("month"), list) and root.get("month"):
        rows = root["month"]
        price_basis = "腾讯港股月线"
    else:
        for key, value in root.items():
            if isinstance(value, list) and value and str(key).lower().endswith("month"):
                rows = value
                price_basis = f"腾讯月线({key})"
                break
    parsed = parse_tencent_rows(rows)
    parsed, breaks = keep_latest_continuous_segment(parsed)
    if not parsed:
        raise RuntimeError("Tencent monthly history is empty")
    return parsed, price_basis, breaks


def clean_date(value: Any) -> str:
    text = str(value or "").strip()[:10].replace("-", "").replace("/", "")
    return text if len(text) == 8 and text.isdigit() else ""


def parse_jsonp_array(text: str) -> List[Dict[str, Any]]:
    left = text.find("[")
    right = text.rfind("]")
    if left < 0 or right <= left:
        raise RuntimeError("JSONP response contains no array")
    value = json.loads(text[left : right + 1])
    if not isinstance(value, list):
        raise RuntimeError("JSONP payload is not a list")
    return value


def fetch_sina_qfq_factors(symbol: str) -> List[Tuple[str, float]]:
    last_error: Optional[BaseException] = None
    for template in SINA_QFQ:
        try:
            text = request_text(
                template.format(symbol=symbol),
                referer=f"https://finance.sina.com.cn/realstock/company/{symbol}/nc.shtml",
                attempts=4,
                read_timeout=25,
            )
            payload_text = text.split("=", 1)[1].split("\n", 1)[0].strip().rstrip(";")
            payload = json.loads(payload_text)
            data = payload.get("data") if isinstance(payload, dict) else None
            factors: List[Tuple[str, float]] = []
            for item in data or []:
                event = clean_date(item.get("d"))
                factor = parse_number(item.get("f"))
                if event and factor and factor > 0:
                    factors.append((event, factor))
            if factors:
                factors.sort(key=lambda value: value[0])
                return factors
        except Exception as exc:  # noqa: BLE001
            last_error = exc
    raise RuntimeError("Sina qfq factors unavailable") from last_error


def fetch_sina_monthly_fallback(symbol: str) -> Tuple[List[Dict[str, Any]], str, int]:
    callback = f"kline_{symbol}_{random.randint(100000, 999999)}"
    text = request_text(
        SINA_DAILY.format(callback=callback),
        params={"symbol": symbol, "scale": "240", "ma": "no", "datalen": "1500"},
        referer=f"https://finance.sina.com.cn/realstock/company/{symbol}/nc.shtml",
        attempts=6,
        read_timeout=35,
    )
    raw = parse_jsonp_array(text)
    factors = fetch_sina_qfq_factors(symbol)
    dates = [item[0] for item in factors]
    values = [item[1] for item in factors]
    monthly: Dict[str, Dict[str, Any]] = {}
    for item in raw:
        date8 = clean_date(item.get("day"))
        if not date8:
            continue
        index = bisect.bisect_right(dates, date8) - 1
        if index < 0:
            index = 0
        factor = values[index]
        try:
            date = datetime.strptime(date8, "%Y%m%d").strftime("%Y-%m-%d")
            o = float(item["open"]) / factor
            c = float(item["close"]) / factor
            h = float(item["high"]) / factor
            low = float(item["low"]) / factor
            volume = float(item.get("volume", 0) or 0)
        except (KeyError, TypeError, ValueError):
            continue
        if min(o, c, h, low) <= 0:
            continue
        key = date[:7]
        row = monthly.get(key)
        if row is None:
            monthly[key] = {"date": date, "open": o, "close": c, "high": h, "low": low, "volume": volume}
        else:
            row["date"] = date
            row["close"] = c
            row["high"] = max(row["high"], h)
            row["low"] = min(row["low"], low)
            row["volume"] += volume
    output = sorted(monthly.values(), key=lambda value: value["date"])
    output, breaks = keep_latest_continuous_segment(output)
    if not output:
        raise RuntimeError("Sina fallback monthly history is empty")
    return output, "新浪前复权日线聚合月线", breaks


def local_extrema(prices: Sequence[float], window: int = 2) -> Tuple[List[int], List[int]]:
    highs: List[int] = []
    lows: List[int] = []
    n = len(prices)
    for i, price in enumerate(prices):
        left = max(0, i - window)
        right = min(n, i + window + 1)
        chunk = prices[left:right]
        if price >= max(chunk):
            highs.append(i)
        if price <= min(chunk):
            lows.append(i)
    return sorted(set(highs)), sorted(set(lows))


def safe_log_ratio(value: float) -> float:
    return math.log(max(value, 1.000001))


def structural_score(metrics: Dict[str, float]) -> float:
    def log_component(value: float, target: float) -> float:
        return min(1.0, safe_log_ratio(value) / safe_log_ratio(target))

    score = 0.0
    score += 14.0 * log_component(metrics["初始下跌倍数"], 6.0)
    score += 20.0 * log_component(metrics["第一次拉升倍数"], 6.0)
    score += 20.0 * log_component(metrics["中段回落倍数"], 6.0)
    score += 22.0 * log_component(metrics["第二次拉升倍数"], 8.0)
    score += 8.0 * min(1.0, metrics["初始下跌月数"] / 48.0)
    score += 4.0 * min(1.0, metrics["中段回落月数"] / 36.0)
    score += 5.0 * max(0.0, 1.0 - metrics["第二高点距今月数"] / 30.0)
    score += 4.0 * min(1.0, metrics["当前价占第二高点"] / 0.75)
    score += 3.0 * min(1.0, metrics["第二高点相对第一高点"] / 1.0)
    return max(0.0, min(100.0, score))


def similarity_score(metrics: Dict[str, float], target: Dict[str, float]) -> float:
    terms = [
        ("初始下跌倍数", 0.10, 0.75, True),
        ("第一次拉升倍数", 0.17, 0.75, True),
        ("中段回落倍数", 0.18, 0.75, True),
        ("第二次拉升倍数", 0.22, 0.85, True),
        ("初始下跌月数", 0.07, 18.0, False),
        ("第一次拉升月数", 0.05, 8.0, False),
        ("中段回落月数", 0.08, 18.0, False),
        ("第二次拉升月数", 0.06, 10.0, False),
        ("第二高点距今月数", 0.025, 10.0, False),
        ("当前价占第二高点", 0.035, 0.25, False),
        ("第二高点相对第一高点", 0.035, 0.55, True),
    ]
    distance = 0.0
    for key, weight, scale, use_log in terms:
        a = float(metrics[key])
        b = float(target[key])
        diff = abs(math.log(max(a, 1e-9)) - math.log(max(b, 1e-9))) if use_log else abs(a - b)
        distance += weight * min(3.0, diff / scale)
    return 100.0 * math.exp(-1.15 * distance)


def sequence_metrics(
    rows: Sequence[Dict[str, Any]],
    h0: int,
    l0: int,
    h1: int,
    l1: int,
    h2: int,
) -> Dict[str, Any]:
    prices = [float(row["close"]) for row in rows]
    current = prices[-1]
    return {
        "初始高点日期": rows[h0]["date"],
        "初始高点价": prices[h0],
        "第一次低点日期": rows[l0]["date"],
        "第一次低点价": prices[l0],
        "第一次拉升高点日期": rows[h1]["date"],
        "第一次拉升高点价": prices[h1],
        "第二次低点日期": rows[l1]["date"],
        "第二次低点价": prices[l1],
        "第二次拉升高点日期": rows[h2]["date"],
        "第二次拉升高点价": prices[h2],
        "当前日期": rows[-1]["date"],
        "当前月线收盘价": current,
        "初始下跌倍数": prices[h0] / prices[l0],
        "第一次拉升倍数": prices[h1] / prices[l0],
        "中段回落倍数": prices[h1] / prices[l1],
        "第二次拉升倍数": prices[h2] / prices[l1],
        "初始下跌月数": l0 - h0,
        "第一次拉升月数": h1 - l0,
        "中段回落月数": l1 - h1,
        "第二次拉升月数": h2 - l1,
        "第二高点距今月数": len(rows) - 1 - h2,
        "当前价占第二高点": current / prices[h2],
        "第二高点相对第一高点": prices[h2] / prices[h1],
        "当前价较第二低点倍数": current / prices[l1],
        "完整形态月数": h2 - h0,
        "h0_index": h0,
        "l0_index": l0,
        "h1_index": h1,
        "l1_index": l1,
        "h2_index": h2,
    }


def enumerate_sequences(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    prices = [float(row["close"]) for row in rows]
    n = len(prices)
    if n < 60:
        return []
    highs, lows = local_extrema(prices, window=2)
    recent_start = max(0, n - 31)
    recent_highs = [i for i in highs if i >= recent_start]
    recent_highs.append(max(range(recent_start, n), key=lambda index: prices[index]))
    h2_candidates = sorted(set(recent_highs), key=lambda index: prices[index], reverse=True)[:7]

    sequences: List[Dict[str, Any]] = []
    for h2 in h2_candidates:
        if n - 1 - h2 > 24:
            continue
        l1_pool = [i for i in lows if max(0, h2 - 30) <= i <= h2 - 2]
        l1_pool = sorted(l1_pool, key=lambda index: prices[index])[:14]
        for l1 in l1_pool:
            if prices[h2] / prices[l1] < 2.2:
                continue
            h1_pool = [i for i in highs if max(0, l1 - 84) <= i <= l1 - 6]
            h1_pool = sorted(h1_pool, key=lambda index: prices[index], reverse=True)[:14]
            for h1 in h1_pool:
                if prices[h1] / prices[l1] < 1.8:
                    continue
                l0_pool = [i for i in lows if max(0, h1 - 24) <= i <= h1 - 1]
                l0_pool = sorted(l0_pool, key=lambda index: prices[index])[:10]
                for l0 in l0_pool:
                    if prices[h1] / prices[l0] < 2.2:
                        continue
                    prior_end = l0 - 24
                    if prior_end < 0:
                        continue
                    h0 = max(range(0, prior_end + 1), key=lambda index: prices[index])
                    if prices[h0] / prices[l0] < 1.8:
                        continue
                    if h2 - h0 < 60:
                        continue
                    metrics = sequence_metrics(rows, h0, l0, h1, l1, h2)
                    if metrics["当前价占第二高点"] < 0.30:
                        continue
                    if metrics["第二高点相对第一高点"] < 0.35:
                        continue
                    metrics["形态结构分"] = structural_score(metrics)
                    sequences.append(metrics)
    return sequences


def price_jump_risk(rows: Sequence[Dict[str, Any]]) -> Tuple[float, int, str]:
    closes = [float(row["close"]) for row in rows]
    max_gap = 1.0
    count_2x = 0
    for previous, current in zip(closes, closes[1:]):
        if previous <= 0 or current <= 0:
            continue
        gap = max(current / previous, previous / current)
        max_gap = max(max_gap, gap)
        if gap >= 2.0:
            count_2x += 1
    if max_gap >= 5.0 or count_2x >= 3:
        label = "高"
    elif max_gap >= 2.5 or count_2x >= 1:
        label = "中"
    else:
        label = "低"
    return max_gap, count_2x, label


def liquidity_label(amount: Optional[float]) -> str:
    if amount is None:
        return "未知"
    if amount >= 100_000_000:
        return "高"
    if amount >= 10_000_000:
        return "中"
    if amount >= 1_000_000:
        return "低"
    return "极低"


def choose_target_profile(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    sequences = enumerate_sequences(rows)
    if not sequences:
        raise RuntimeError("Target produced no valid pattern sequence")
    # Structural score plus a preference for a recent, retained second spike.
    return max(
        sequences,
        key=lambda item: item["形态结构分"] + 5.0 * item["当前价占第二高点"] + 2.0 * min(item["第二高点相对第一高点"], 2.0),
    )


def analyze_record(record: Dict[str, Any], config: Config, target_profile: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    symbol = record["行情代码"]
    fallback_used = False
    try:
        rows, basis, breaks = fetch_tencent_monthly(symbol, config.history_count)
    except Exception:
        if record["市场"] != "A股":
            raise
        rows, basis, breaks = fetch_sina_monthly_fallback(symbol)
        fallback_used = True

    max_gap, gap_count, jump_risk = price_jump_risk(rows)
    sequences = enumerate_sequences(rows)
    base = dict(record)
    base.update(
        {
            "历史起始月": rows[0]["date"],
            "历史结束月": rows[-1]["date"],
            "有效月数": len(rows),
            "历史断点数": breaks,
            "价格口径": basis,
            "使用备用数据源": fallback_used,
            "最大单月价格跳变倍数": max_gap,
            "单月2倍跳变次数": gap_count,
            "价格跳变风险": jump_risk,
        }
    )
    if not sequences:
        base.update({"是否宽口径候选": False, "是否严格候选": False, "筛选状态": "未形成完整形态"})
        return base, rows

    best: Optional[Dict[str, Any]] = None
    best_total = -math.inf
    for metrics in sequences:
        similarity = similarity_score(metrics, target_profile)
        penalty = 0.0
        if jump_risk == "中":
            penalty += 5.0
        elif jump_risk == "高":
            penalty += 14.0
        if metrics["第一次拉升月数"] <= 1 and metrics["第一次拉升倍数"] >= 3.0:
            penalty += 5.0
        if metrics["第二次拉升月数"] <= 1 and metrics["第二次拉升倍数"] >= 3.0:
            penalty += 5.0
        total = 0.68 * similarity + 0.32 * metrics["形态结构分"] - penalty
        candidate = {**metrics, "目标相似度": similarity, "综合匹配分": total, "风险扣分": penalty}
        if total > best_total:
            best_total = total
            best = candidate
    assert best is not None

    strict = (
        best["初始下跌倍数"] >= 2.8
        and best["第一次拉升倍数"] >= 3.0
        and best["中段回落倍数"] >= 2.5
        and best["第二次拉升倍数"] >= 3.0
        and best["初始下跌月数"] >= 24
        and best["中段回落月数"] >= 9
        and best["第二高点距今月数"] <= 18
        and best["当前价占第二高点"] >= 0.40
        and best["综合匹配分"] >= 58.0
        and jump_risk != "高"
    )
    broad = best["综合匹配分"] >= 42.0
    if strict and best["综合匹配分"] >= 76:
        grade = "A+"
    elif strict and best["综合匹配分"] >= 68:
        grade = "A"
    elif strict:
        grade = "B+"
    elif broad and best["综合匹配分"] >= 58:
        grade = "B"
    elif broad:
        grade = "C"
    else:
        grade = "观察"

    base.update(best)
    base.update(
        {
            "是否宽口径候选": broad,
            "是否严格候选": strict,
            "匹配等级": grade,
            "流动性等级": liquidity_label(record.get("最新成交额")),
            "筛选状态": "严格候选" if strict else ("宽口径候选" if broad else "低相似度"),
        }
    )
    return base, rows


def checkpoint_paths(config: Config) -> Tuple[Path, Path]:
    return config.checkpoint_dir / "meta.json", config.checkpoint_dir / "success.jsonl"


def initialize_checkpoint(config: Config) -> None:
    config.checkpoint_dir.mkdir(parents=True, exist_ok=True)
    meta_path, success_path = checkpoint_paths(config)
    meta_path.write_text(json.dumps({"config_key": config.key}, indent=2), encoding="utf-8")
    success_path.touch(exist_ok=True)


def load_checkpoint(config: Config) -> Dict[str, Dict[str, Any]]:
    meta_path, success_path = checkpoint_paths(config)
    if not meta_path.exists() or not success_path.exists():
        return {}
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        if meta.get("config_key") != config.key:
            return {}
        output: Dict[str, Dict[str, Any]] = {}
        for line in success_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            item = json.loads(line)
            output[str(item["行情代码"])] = item
        return output
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Unable to load checkpoint: %s", exc)
        return {}


def append_checkpoint(config: Config, result: Dict[str, Any]) -> None:
    _, path = checkpoint_paths(config)
    text = json.dumps(result, ensure_ascii=False, separators=(",", ":"))
    with CHECKPOINT_LOCK:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(text + "\n")


def csv_value(value: Any) -> Any:
    if isinstance(value, bool):
        return "是" if value else "否"
    return value


def write_csv(path: Path, rows: Sequence[Dict[str, Any]], fieldnames: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: csv_value(row.get(key)) for key in fieldnames})


def result_fields() -> List[str]:
    return [
        "总排名", "市场排名", "市场", "板块", "证券代码", "股票名称", "行情代码", "是否ST",
        "匹配等级", "筛选状态", "是否严格候选", "是否宽口径候选", "综合匹配分", "目标相似度", "形态结构分", "风险扣分",
        "初始高点日期", "初始高点价", "第一次低点日期", "第一次低点价", "第一次拉升高点日期", "第一次拉升高点价",
        "第二次低点日期", "第二次低点价", "第二次拉升高点日期", "第二次拉升高点价", "当前日期", "当前月线收盘价",
        "初始下跌倍数", "第一次拉升倍数", "中段回落倍数", "第二次拉升倍数", "初始下跌月数", "第一次拉升月数",
        "中段回落月数", "第二次拉升月数", "第二高点距今月数", "当前价占第二高点", "第二高点相对第一高点", "当前价较第二低点倍数",
        "完整形态月数", "历史起始月", "历史结束月", "有效月数", "历史断点数", "价格口径", "使用备用数据源",
        "最大单月价格跳变倍数", "单月2倍跳变次数", "价格跳变风险", "流动性等级", "最新价", "最新成交额", "最新成交量",
        "市盈率", "市净率", "总市值_原始", "行情时间",
    ]


def rank_results(results: List[Dict[str, Any]]) -> None:
    candidates = [item for item in results if item.get("综合匹配分") is not None]
    candidates.sort(key=lambda item: float(item.get("综合匹配分", -999)), reverse=True)
    for rank, item in enumerate(candidates, 1):
        item["总排名"] = rank
    by_market: Dict[str, List[Dict[str, Any]]] = {}
    for item in candidates:
        by_market.setdefault(str(item["市场"]), []).append(item)
    for market_rows in by_market.values():
        for rank, item in enumerate(market_rows, 1):
            item["市场排名"] = rank


def write_top_series(
    output_path: Path,
    selected: Sequence[Dict[str, Any]],
    config: Config,
) -> None:
    fields = ["总排名", "市场", "证券代码", "股票名称", "行情代码", "日期", "月收盘价", "标准化价格_初始高点100"]
    output: List[Dict[str, Any]] = []
    for item in selected:
        symbol = item["行情代码"]
        try:
            rows, _, _ = fetch_tencent_monthly(symbol, config.history_count)
        except Exception:
            if item["市场"] != "A股":
                continue
            try:
                rows, _, _ = fetch_sina_monthly_fallback(symbol)
            except Exception:
                continue
        base = float(item.get("初始高点价") or rows[0]["close"])
        for row in rows:
            output.append(
                {
                    "总排名": item.get("总排名"),
                    "市场": item["市场"],
                    "证券代码": item["证券代码"],
                    "股票名称": item["股票名称"],
                    "行情代码": symbol,
                    "日期": row["date"],
                    "月收盘价": row["close"],
                    "标准化价格_初始高点100": float(row["close"]) / base * 100.0,
                }
            )
    write_csv(output_path, output, fields)


def main() -> int:
    args = parse_args()
    config = Config(
        output_dir=Path(args.output_dir),
        checkpoint_dir=Path(args.checkpoint_dir),
        workers=max(1, args.workers),
        requests_per_second=max(0.5, args.requests_per_second),
        limit_a=max(0, args.limit_a),
        limit_hk=max(0, args.limit_hk),
        history_count=max(100, args.history_count),
        min_history_months=max(48, args.min_history_months),
        top_series_count=max(1, args.top_series_count),
    )
    configure_logging(config.output_dir)
    global RATE_LIMITER
    RATE_LIMITER = GlobalRateLimiter(config.requests_per_second)
    started = time.time()

    a_universe = fetch_a_universe()
    hk_universe = fetch_hk_universe()
    if config.limit_a:
        # Include a Beijing-stock sample when validating.
        regular = a_universe[: config.limit_a]
        bse = next((item for item in a_universe if item["行情代码"].startswith("bj")), None)
        a_universe = regular + ([bse] if bse and bse not in regular else [])
    if config.limit_hk:
        regular = hk_universe[: config.limit_hk]
        target_record = next((item for item in hk_universe if item["行情代码"] == TARGET_SYMBOL), None)
        hk_universe = regular + ([target_record] if target_record and target_record not in regular else [])

    target_rows, target_basis, target_breaks = fetch_tencent_monthly(TARGET_SYMBOL, config.history_count)
    target_profile = choose_target_profile(target_rows)
    target_profile.update(
        {
            "行情代码": TARGET_SYMBOL,
            "证券代码": "03839",
            "股票名称": "正大生物",
            "价格口径": target_basis,
            "历史断点数": target_breaks,
            "有效月数": len(target_rows),
        }
    )
    (config.output_dir / "正大生物基准形态.json").write_text(
        json.dumps(target_profile, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    LOGGER.info("Target profile: %s", json.dumps(target_profile, ensure_ascii=False))

    universe = a_universe + hk_universe
    initialize_checkpoint(config)
    completed = load_checkpoint(config)
    records = [record for record in universe if record["行情代码"] not in completed]
    failures: Dict[str, str] = {}
    LOGGER.info(
        "Starting scan: A=%s HK=%s total=%s remaining=%s workers=%s rate=%.1f/s",
        len(a_universe), len(hk_universe), len(universe), len(records), config.workers, config.requests_per_second,
    )

    with ThreadPoolExecutor(max_workers=config.workers, thread_name_prefix="ah-pattern") as executor:
        future_map: Dict[Future[Tuple[Dict[str, Any], List[Dict[str, Any]]]], Dict[str, Any]] = {
            executor.submit(analyze_record, record, config, target_profile): record for record in records
        }
        finished = 0
        for future in as_completed(future_map):
            record = future_map[future]
            finished += 1
            symbol = record["行情代码"]
            try:
                result, _ = future.result()
                completed[symbol] = result
                append_checkpoint(config, result)
            except Exception as exc:  # noqa: BLE001
                failures[symbol] = f"{type(exc).__name__}: {exc}"
            if finished % 200 == 0 or finished == len(records):
                strict_count = sum(1 for item in completed.values() if item.get("是否严格候选"))
                broad_count = sum(1 for item in completed.values() if item.get("是否宽口径候选"))
                LOGGER.info(
                    "Progress %s/%s; success=%s failures=%s strict=%s broad=%s",
                    finished, len(records), len(completed), len(failures), strict_count, broad_count,
                )

    results = list(completed.values())
    rank_results(results)
    candidate_results = [item for item in results if item.get("是否宽口径候选")]
    candidate_results.sort(key=lambda item: float(item.get("综合匹配分", -999)), reverse=True)
    strict_results = [item for item in candidate_results if item.get("是否严格候选")]
    a_candidates = [item for item in candidate_results if item.get("市场") == "A股"]
    hk_candidates = [item for item in candidate_results if item.get("市场") == "港股"]

    fields = result_fields()
    write_csv(config.output_dir / "全部宽口径候选.csv", candidate_results, fields)
    write_csv(config.output_dir / "高相似严格候选.csv", strict_results, fields)
    write_csv(config.output_dir / "A股候选.csv", a_candidates, fields)
    write_csv(config.output_dir / "港股候选.csv", hk_candidates, fields)
    write_csv(config.output_dir / "全市场扫描状态.csv", results, fields)
    failure_rows = [
        {"行情代码": symbol, "错误": error}
        for symbol, error in sorted(failures.items())
    ]
    write_csv(config.output_dir / "抓取失败清单.csv", failure_rows, ["行情代码", "错误"])

    selected_series: List[Dict[str, Any]] = []
    target_result = next((item for item in results if item.get("行情代码") == TARGET_SYMBOL), None)
    if target_result:
        selected_series.append(target_result)
    for item in strict_results + candidate_results:
        if item not in selected_series:
            selected_series.append(item)
        if len(selected_series) >= config.top_series_count:
            break
    write_top_series(config.output_dir / "顶部候选标准化月线.csv", selected_series, config)

    elapsed = time.time() - started
    summary = {
        "生成时间": datetime.now().isoformat(timespec="seconds"),
        "目标股票": "03839 正大生物",
        "目标形态": "多年下跌→第一次急拉→再次深跌→第二次急拉",
        "A股股票池": len(a_universe),
        "港股股票池": len(hk_universe),
        "合计股票池": len(universe),
        "成功扫描": len(results),
        "抓取失败": len(failures),
        "宽口径候选": len(candidate_results),
        "严格候选": len(strict_results),
        "A股候选": len(a_candidates),
        "港股候选": len(hk_candidates),
        "运行耗时秒": elapsed,
        "价格与算法口径": {
            "频率": "月线收盘价",
            "历史": f"最多{config.history_count}个月；发生超过18个月的历史断点时，仅保留最后连续段",
            "A股": "优先腾讯前复权月线；失败时新浪前复权日线聚合月线",
            "港股": "腾讯港股月线；公司行动造成的价格跳变以风险字段标注",
            "形态序列": "H0→L0→H1→L1→H2，H2需位于最近24个月",
            "主要宽口径阈值": "初跌≥1.8倍、首拉≥2.2倍、中跌≥1.8倍、再拉≥2.2倍、完整形态≥60个月",
            "严格阈值": "初跌≥2.8倍、首拉≥3倍、中跌≥2.5倍、再拉≥3倍，并通过相似度、持续时间、近期位置和跳变风险约束",
        },
        "目标基准指标": target_profile,
    }
    (config.output_dir / "筛选摘要.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    LOGGER.info("Completed: %s", json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
