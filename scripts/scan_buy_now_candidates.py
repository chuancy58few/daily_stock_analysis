#!/usr/bin/env python3
"""Full-market A-share + southbound Stock Connect multi-factor buy-now scanner.

This scanner deliberately separates four questions:
1. Is the business profitable and financially credible?
2. Are current earnings improving or at least stable?
3. Is valuation reasonable relative to profitability and shareholder return?
4. Is the current price position suitable for a first position rather than a chase?

The output is a research shortlist, not an automated trading signal.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import random
import statistics
import sys
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import akshare as ak
import pandas as pd

import scan_ah_double_spike_pattern as base
import scan_l1_setup_candidates as l1

LOGGER = logging.getLogger("buy_now_scan")
CHECKPOINT_LOCK = threading.Lock()
HK_FINANCE_LIMITER: Optional[base.GlobalRateLimiter] = None


@dataclass(frozen=True)
class Config:
    output_dir: Path
    checkpoint_dir: Path
    workers: int
    requests_per_second: float
    hk_finance_workers: int
    hk_finance_requests_per_second: float
    history_count: int
    limit_a: int
    limit_hk: int

    @property
    def key(self) -> str:
        return (
            "buy-now-v4|2026-08-16|"
            f"history={self.history_count}|A-current|HK-connect-current|"
            "A-2026H1-Q1-2025annual|HK-current-indicator"
        )


OUTPUT_FIELDS = [
    "总排名", "市场排名", "市场", "港股通来源", "板块", "行业", "证券代码", "股票名称", "行情代码", "是否ST",
    "最终判断", "候选类型", "买入总分", "估值分", "质量分", "增长分", "价格位置分", "流动性分", "风险扣分",
    "最新价", "市盈率", "市净率", "股息率", "净资产收益率", "收入同比增长", "净利润同比增长", "净利润", "每股收益",
    "每股经营现金流", "现金流质量", "毛利率或净利率", "最新财报期", "年度收入同比增长", "年度净利润同比增长", "年度净资产收益率",
    "五年高点", "五年低点", "当前占五年高点", "当前较五年低点倍数", "五年价格分位", "近3月涨跌幅", "近6月涨跌幅", "近12月涨跌幅",
    "当前相对6月均线", "当前相对12月均线", "五年最大回撤", "最新成交额", "最新成交量", "市值原始字段", "行情时间",
    "L1核心候选", "L1当前阶段", "L1综合分", "数据完整度", "价格口径", "研究备注",
]


CYCLICAL_KEYWORDS = (
    "煤炭", "焦炭", "钢铁", "有色", "贵金属", "工业金属", "小金属", "能源金属", "石油", "油气", "化纤",
    "化肥", "航运", "港口", "养殖", "农牧", "光伏", "锂电", "电池", "水泥", "玻璃", "基础化工",
)
PROPERTY_KEYWORDS = ("房地产", "房屋建设", "物业开发")
FINANCIAL_KEYWORDS = ("银行", "保险", "证券", "多元金融", "金融", "交易所")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default="reports/a_hkconnect_buy_now")
    parser.add_argument("--checkpoint-dir", default=".cache/a_hkconnect_buy_now")
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--requests-per-second", type=float, default=14.0)
    parser.add_argument("--hk-finance-workers", type=int, default=8)
    parser.add_argument("--hk-finance-requests-per-second", type=float, default=5.0)
    parser.add_argument("--history-count", type=int, default=72)
    parser.add_argument("--limit-a", type=int, default=0)
    parser.add_argument("--limit-hk", type=int, default=0)
    return parser.parse_args()


def configure_logging(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    LOGGER.handlers.clear()
    LOGGER.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(formatter)
    LOGGER.addHandler(stream)
    file_handler = logging.FileHandler(output_dir / "运行日志.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    LOGGER.addHandler(file_handler)


def finite_number(value: Any) -> Optional[float]:
    if value in (None, "", "--", "-", "nan", "None"):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def safe_ratio(numerator: Optional[float], denominator: Optional[float]) -> Optional[float]:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return numerator / denominator


def average(values: Iterable[float]) -> Optional[float]:
    cleaned = [float(value) for value in values if value is not None and math.isfinite(float(value))]
    return sum(cleaned) / len(cleaned) if cleaned else None


def clip(value: float, lower: float = 0.0, upper: float = 100.0) -> float:
    return max(lower, min(upper, value))


def retry_call(function, *args, attempts: int = 5, **kwargs):
    last_error: Optional[BaseException] = None
    for attempt in range(1, attempts + 1):
        try:
            return function(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            wait = min(20.0, 0.8 * 2 ** (attempt - 1)) + random.random()
            LOGGER.warning("Call failed function=%s attempt=%s/%s error=%s", getattr(function, "__name__", str(function)), attempt, attempts, exc)
            time.sleep(wait)
    raise RuntimeError(f"Call failed after {attempts} attempts: {getattr(function, '__name__', function)}") from last_error


def normalize_code(value: Any, width: int = 6) -> str:
    text = str(value or "").strip().split(".")[0]
    return text.zfill(width)


def frame_records(frame: Optional[pd.DataFrame]) -> List[Dict[str, Any]]:
    if frame is None or frame.empty:
        return []
    clean = frame.where(pd.notna(frame), None)
    return clean.to_dict("records")


def fetch_a_financial_tables(output_dir: Path) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Fetch broad A-share financial tables in a few market-wide calls."""
    tables: Dict[str, pd.DataFrame] = {}
    for label, date in (("h1_2026", "20260630"), ("q1_2026", "20260331"), ("annual_2025", "20251231")):
        try:
            frame = retry_call(ak.stock_yjbb_em, date=date, attempts=5)
            tables[label] = frame
            frame.to_csv(output_dir / f"A股业绩报表_{date}.csv", index=False, encoding="utf-8-sig")
            LOGGER.info("A financial table %s rows=%s columns=%s", label, len(frame), list(frame.columns))
        except Exception as exc:  # noqa: BLE001
            LOGGER.error("Unable to fetch A financial table %s: %s", label, exc)
            tables[label] = pd.DataFrame()

    try:
        dividend = retry_call(ak.stock_fhps_em, date="20251231", attempts=5)
        dividend.to_csv(output_dir / "A股2025年度分红配送.csv", index=False, encoding="utf-8-sig")
        LOGGER.info("A dividend table rows=%s columns=%s", len(dividend), list(dividend.columns))
    except Exception as exc:  # noqa: BLE001
        LOGGER.error("Unable to fetch A dividend table: %s", exc)
        dividend = pd.DataFrame()

    output: Dict[str, Dict[str, Dict[str, Any]]] = {"latest": {}, "annual": {}, "dividend": {}}

    def normalize_performance(frame: pd.DataFrame, report_period: str) -> Dict[str, Dict[str, Any]]:
        result: Dict[str, Dict[str, Any]] = {}
        for record in frame_records(frame):
            code = normalize_code(record.get("股票代码"), 6)
            if not code.isdigit():
                continue
            result[code] = {
                "报告期": report_period,
                "股票简称": record.get("股票简称"),
                "每股收益": finite_number(record.get("每股收益")),
                "营业总收入": finite_number(record.get("营业总收入-营业总收入")),
                "收入同比增长": finite_number(record.get("营业总收入-同比增长")),
                "收入季度环比": finite_number(record.get("营业总收入-季度环比增长")),
                "净利润": finite_number(record.get("净利润-净利润")),
                "净利润同比增长": finite_number(record.get("净利润-同比增长")),
                "净利润季度环比": finite_number(record.get("净利润-季度环比增长")),
                "每股净资产": finite_number(record.get("每股净资产")),
                "净资产收益率": finite_number(record.get("净资产收益率")),
                "每股经营现金流": finite_number(record.get("每股经营现金流量")),
                "销售毛利率": finite_number(record.get("销售毛利率")),
                "行业": str(record.get("所处行业") or ""),
                "最新公告日期": str(record.get("最新公告日期") or ""),
            }
        return result

    h1 = normalize_performance(tables["h1_2026"], "2026H1")
    q1 = normalize_performance(tables["q1_2026"], "2026Q1")
    annual = normalize_performance(tables["annual_2025"], "2025A")
    output["annual"] = annual
    output["latest"] = dict(q1)
    output["latest"].update(h1)  # Prefer H1 where already disclosed.

    for record in frame_records(dividend):
        code = normalize_code(record.get("代码"), 6)
        if not code.isdigit():
            continue
        output["dividend"][code] = {
            "股息率": finite_number(record.get("现金分红-股息率")),
            "每10股现金分红": finite_number(record.get("现金分红-现金分红比例")),
            "分红方案进度": str(record.get("方案进度") or ""),
            "分红最新公告日期": str(record.get("最新公告日期") or ""),
        }
    return output


def fetch_one_hk_finance(record: Dict[str, Any]) -> Dict[str, Any]:
    global HK_FINANCE_LIMITER
    if HK_FINANCE_LIMITER is not None:
        HK_FINANCE_LIMITER.wait()
    code = normalize_code(record.get("证券代码"), 5)
    frame = retry_call(ak.stock_hk_financial_indicator_em, symbol=code, attempts=4)
    rows = frame_records(frame)
    if not rows:
        raise RuntimeError("HK financial indicator returned no rows")
    row = rows[0]
    return {
        "证券代码": code,
        "每股收益": finite_number(row.get("基本每股收益(元)")),
        "每股净资产": finite_number(row.get("每股净资产(元)")),
        "每股股息": finite_number(row.get("每股股息TTM(港元)")),
        "派息比率": finite_number(row.get("派息比率(%)")),
        "每股经营现金流": finite_number(row.get("每股经营现金流(元)")),
        "股息率": finite_number(row.get("股息率TTM(%)")),
        "总市值": finite_number(row.get("总市值(港元)")),
        "营业总收入": finite_number(row.get("营业总收入")),
        "收入同比增长": finite_number(row.get("营业总收入滚动环比增长(%)")),
        "销售净利率": finite_number(row.get("销售净利率(%)")),
        "净利润": finite_number(row.get("净利润")),
        "净利润同比增长": finite_number(row.get("净利润滚动环比增长(%)")),
        "净资产收益率": finite_number(row.get("股东权益回报率(%)")),
        "市盈率": finite_number(row.get("市盈率")),
        "市净率": finite_number(row.get("市净率")),
        "总资产收益率": finite_number(row.get("总资产回报率(%)")),
        "报告期": "最新TTM/滚动指标",
    }


def fetch_hk_financials(records: Sequence[Dict[str, Any]], config: Config) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, str]]:
    global HK_FINANCE_LIMITER
    HK_FINANCE_LIMITER = base.GlobalRateLimiter(config.hk_finance_requests_per_second)
    output: Dict[str, Dict[str, Any]] = {}
    failures: Dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=config.hk_finance_workers, thread_name_prefix="hk-finance") as executor:
        future_map: Dict[Future[Dict[str, Any]], Dict[str, Any]] = {
            executor.submit(fetch_one_hk_finance, record): record for record in records
        }
        finished = 0
        for future in as_completed(future_map):
            record = future_map[future]
            finished += 1
            code = normalize_code(record.get("证券代码"), 5)
            try:
                output[code] = future.result()
            except Exception as exc:  # noqa: BLE001
                failures[code] = f"{type(exc).__name__}: {exc}"
            if finished % 100 == 0 or finished == len(records):
                LOGGER.info("HK financial progress %s/%s success=%s failures=%s", finished, len(records), len(output), len(failures))
    return output, failures


def maximum_drawdown(closes: Sequence[float]) -> float:
    peak = closes[0]
    worst = 0.0
    for close in closes:
        peak = max(peak, close)
        if peak > 0:
            worst = min(worst, close / peak - 1.0)
    return worst


def price_metrics(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    closes = [float(row["close"]) for row in rows if finite_number(row.get("close")) and float(row["close"]) > 0]
    if len(closes) < 24:
        raise RuntimeError("insufficient price history")
    dates = [row["date"] for row in rows][-len(closes):]
    window = closes[-min(61, len(closes)):]
    current = closes[-1]
    high5 = max(window)
    low5 = min(window)
    ma6 = average(closes[-6:])
    ma12 = average(closes[-12:])
    price_percentile = (current - low5) / (high5 - low5) if high5 > low5 else 0.5
    return {
        "价格日期": dates[-1],
        "月线收盘价": current,
        "五年高点": high5,
        "五年低点": low5,
        "当前占五年高点": current / high5 if high5 > 0 else None,
        "当前较五年低点倍数": current / low5 if low5 > 0 else None,
        "五年价格分位": price_percentile,
        "近3月涨跌幅": current / closes[-4] - 1.0 if len(closes) >= 4 else None,
        "近6月涨跌幅": current / closes[-7] - 1.0 if len(closes) >= 7 else None,
        "近12月涨跌幅": current / closes[-13] - 1.0 if len(closes) >= 13 else None,
        "当前相对6月均线": current / ma6 if ma6 and ma6 > 0 else None,
        "当前相对12月均线": current / ma12 if ma12 and ma12 > 0 else None,
        "五年最大回撤": maximum_drawdown(window),
        "价格口径": "腾讯前复权月线" if rows and "qfq" in str(rows[0].get("source", "")).lower() else "腾讯/新浪月线",
        "有效月数": len(closes),
    }


def fetch_one_price(record: Dict[str, Any], config: Config) -> Dict[str, Any]:
    symbol = record["行情代码"]
    fallback_used = False
    try:
        rows, basis, breaks = base.fetch_tencent_monthly(symbol, config.history_count)
    except Exception:
        if record.get("市场") != "A股":
            raise
        rows, basis, breaks = base.fetch_sina_monthly_fallback(symbol)
        fallback_used = True
    metrics = price_metrics(rows)
    metrics["价格口径"] = basis
    metrics["历史断点数"] = breaks
    metrics["使用备用数据源"] = fallback_used
    return metrics


def fetch_price_data(records: Sequence[Dict[str, Any]], config: Config) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, str]]:
    base.RATE_LIMITER = base.GlobalRateLimiter(config.requests_per_second)
    output: Dict[str, Dict[str, Any]] = {}
    failures: Dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=config.workers, thread_name_prefix="price") as executor:
        future_map: Dict[Future[Dict[str, Any]], Dict[str, Any]] = {
            executor.submit(fetch_one_price, record, config): record for record in records
        }
        finished = 0
        for future in as_completed(future_map):
            record = future_map[future]
            finished += 1
            symbol = record["行情代码"]
            try:
                output[symbol] = future.result()
            except Exception as exc:  # noqa: BLE001
                failures[symbol] = f"{type(exc).__name__}: {exc}"
            if finished % 300 == 0 or finished == len(records):
                LOGGER.info("Price progress %s/%s success=%s failures=%s", finished, len(records), len(output), len(failures))
    return output, failures


def pe_score(pe: Optional[float]) -> float:
    if pe is None or pe <= 0:
        return 0.0
    if pe < 5:
        return 72.0
    if pe <= 12:
        return 100.0
    if pe <= 20:
        return 90.0
    if pe <= 30:
        return 70.0
    if pe <= 45:
        return 42.0
    if pe <= 65:
        return 20.0
    return 5.0


def pb_score(pb: Optional[float]) -> float:
    if pb is None or pb <= 0:
        return 35.0
    if pb <= 0.8:
        return 85.0
    if pb <= 1.5:
        return 100.0
    if pb <= 2.5:
        return 85.0
    if pb <= 4.0:
        return 62.0
    if pb <= 6.0:
        return 35.0
    return 10.0


def dividend_score(dividend_yield: Optional[float]) -> float:
    if dividend_yield is None or dividend_yield <= 0:
        return 10.0
    if dividend_yield >= 6.0:
        return 100.0
    if dividend_yield >= 4.0:
        return 85.0
    if dividend_yield >= 2.0:
        return 65.0
    if dividend_yield >= 1.0:
        return 40.0
    return 18.0


def roe_score(roe: Optional[float]) -> float:
    if roe is None:
        return 30.0
    if roe < 0:
        return 0.0
    if roe < 5:
        return 20.0
    if roe < 8:
        return 45.0
    if roe < 12:
        return 65.0
    if roe < 18:
        return 85.0
    return 100.0


def revenue_growth_score(growth: Optional[float]) -> float:
    if growth is None:
        return 35.0
    if growth < -20:
        return 0.0
    if growth < -10:
        return 15.0
    if growth < 0:
        return 35.0
    if growth < 5:
        return 55.0
    if growth < 15:
        return 75.0
    if growth < 30:
        return 90.0
    return 100.0


def profit_growth_score(growth: Optional[float]) -> float:
    if growth is None:
        return 30.0
    if growth < -50:
        return 0.0
    if growth < -20:
        return 15.0
    if growth < 0:
        return 35.0
    if growth < 10:
        return 60.0
    if growth < 30:
        return 80.0
    if growth < 80:
        return 95.0
    return 100.0


def cash_quality_score(eps: Optional[float], cfo_per_share: Optional[float], financial_like: bool) -> Tuple[float, Optional[float]]:
    if financial_like:
        return 65.0, None
    if cfo_per_share is None:
        return 30.0, None
    if cfo_per_share <= 0:
        return 0.0, safe_ratio(cfo_per_share, abs(eps) if eps else None)
    if eps is None or eps <= 0:
        return 55.0, None
    ratio_value = cfo_per_share / eps
    if ratio_value >= 1.0:
        return 100.0, ratio_value
    if ratio_value >= 0.5:
        return 70.0, ratio_value
    return 38.0, ratio_value


def margin_score(margin: Optional[float], is_net_margin: bool) -> float:
    if margin is None:
        return 40.0
    if margin < 0:
        return 0.0
    if is_net_margin:
        if margin < 5:
            return 30.0
        if margin < 10:
            return 52.0
        if margin < 20:
            return 78.0
        return 100.0
    if margin < 10:
        return 22.0
    if margin < 20:
        return 45.0
    if margin < 35:
        return 68.0
    if margin < 50:
        return 85.0
    return 100.0


def high_position_score(current_high: Optional[float]) -> float:
    if current_high is None:
        return 30.0
    if current_high <= 0.25:
        return 55.0
    if current_high <= 0.40:
        return 82.0
    if current_high <= 0.65:
        return 100.0
    if current_high <= 0.80:
        return 75.0
    if current_high <= 0.95:
        return 40.0
    return 18.0


def low_multiple_score(current_low: Optional[float]) -> float:
    if current_low is None:
        return 30.0
    if current_low < 1.05:
        return 55.0
    if current_low <= 1.35:
        return 100.0
    if current_low <= 1.80:
        return 90.0
    if current_low <= 2.40:
        return 65.0
    if current_low <= 3.00:
        return 40.0
    return 15.0


def momentum_score(return_3m: Optional[float]) -> float:
    if return_3m is None:
        return 35.0
    if return_3m < -0.20:
        return 10.0
    if return_3m < -0.08:
        return 35.0
    if return_3m < 0.05:
        return 78.0
    if return_3m <= 0.20:
        return 100.0
    if return_3m <= 0.35:
        return 62.0
    return 18.0


def moving_average_score(current_ma6: Optional[float]) -> float:
    if current_ma6 is None:
        return 35.0
    if current_ma6 < 0.90:
        return 20.0
    if current_ma6 < 0.97:
        return 52.0
    if current_ma6 <= 1.10:
        return 100.0
    if current_ma6 <= 1.25:
        return 78.0
    return 35.0


def liquidity_score(record: Dict[str, Any]) -> float:
    amount = finite_number(record.get("最新成交额"))
    if amount is None:
        return 20.0
    if record.get("市场") == "A股":
        if amount >= 300_000_000:
            return 100.0
        if amount >= 100_000_000:
            return 85.0
        if amount >= 50_000_000:
            return 70.0
        if amount >= 30_000_000:
            return 55.0
        return 20.0
    if amount >= 100_000_000:
        return 100.0
    if amount >= 30_000_000:
        return 85.0
    if amount >= 10_000_000:
        return 70.0
    if amount >= 5_000_000:
        return 55.0
    return 20.0


def contains_keyword(text: str, keywords: Sequence[str]) -> bool:
    return any(keyword in text for keyword in keywords)


def score_record(
    record: Dict[str, Any],
    latest: Dict[str, Any],
    annual: Dict[str, Any],
    dividend: Dict[str, Any],
    price: Dict[str, Any],
    l1_result: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    market = record.get("市场")
    name = str(record.get("股票名称") or "")
    industry = str(latest.get("行业") or annual.get("行业") or "")
    descriptor = f"{industry}|{name}"
    financial_like = contains_keyword(descriptor, FINANCIAL_KEYWORDS)
    cyclical = contains_keyword(descriptor, CYCLICAL_KEYWORDS)
    property_like = contains_keyword(descriptor, PROPERTY_KEYWORDS) or (market == "港股通" and any(key in name for key in ("地产", "置地", "发展")))

    pe = finite_number(latest.get("市盈率"))
    pb = finite_number(latest.get("市净率"))
    if pe is None:
        pe = finite_number(record.get("市盈率"))
    if pb is None:
        pb = finite_number(record.get("市净率"))
    dividend_yield = finite_number(dividend.get("股息率"))
    roe = finite_number(latest.get("净资产收益率"))
    annual_roe = finite_number(annual.get("净资产收益率"))
    revenue_growth = finite_number(latest.get("收入同比增长"))
    profit_growth = finite_number(latest.get("净利润同比增长"))
    net_profit = finite_number(latest.get("净利润"))
    eps = finite_number(latest.get("每股收益"))
    cfo_per_share = finite_number(latest.get("每股经营现金流"))
    margin = finite_number(latest.get("销售净利率")) if market == "港股通" else finite_number(latest.get("销售毛利率"))
    annual_revenue_growth = finite_number(annual.get("收入同比增长"))
    annual_profit_growth = finite_number(annual.get("净利润同比增长"))
    annual_profit = finite_number(annual.get("净利润"))

    cash_score, cash_ratio = cash_quality_score(eps, cfo_per_share, financial_like)
    valuation = 0.45 * pe_score(pe) + 0.25 * pb_score(pb) + 0.30 * dividend_score(dividend_yield)
    quality = 0.48 * roe_score(roe if roe is not None else annual_roe) + 0.32 * cash_score + 0.20 * margin_score(margin, market == "港股通")
    growth = 0.40 * revenue_growth_score(revenue_growth) + 0.60 * profit_growth_score(profit_growth)
    price_score = (
        0.30 * high_position_score(finite_number(price.get("当前占五年高点")))
        + 0.28 * low_multiple_score(finite_number(price.get("当前较五年低点倍数")))
        + 0.22 * momentum_score(finite_number(price.get("近3月涨跌幅")))
        + 0.20 * moving_average_score(finite_number(price.get("当前相对6月均线")))
    )
    liquid = liquidity_score(record)

    penalty = 0.0
    if bool(record.get("是否ST")):
        penalty += 100.0
    if net_profit is None or net_profit <= 0:
        penalty += 28.0
    if pe is None or pe <= 0:
        penalty += 12.0
    if revenue_growth is not None and revenue_growth < 0 and profit_growth is not None and profit_growth < 0:
        penalty += 12.0
    if cyclical and profit_growth is not None and profit_growth < 0:
        penalty += 8.0
    if property_like:
        penalty += 18.0
    if finite_number(price.get("近3月涨跌幅")) is not None and float(price["近3月涨跌幅"]) > 0.35:
        penalty += 10.0
    if finite_number(price.get("当前占五年高点")) is not None and float(price["当前占五年高点"]) > 0.92:
        penalty += 8.0
    if liquid < 55:
        penalty += 8.0
    if annual_profit is not None and annual_profit <= 0:
        penalty += 10.0

    total = 0.28 * valuation + 0.27 * quality + 0.20 * growth + 0.20 * price_score + 0.05 * liquid - penalty
    total = clip(total)

    current_high = finite_number(price.get("当前占五年高点"))
    current_low = finite_number(price.get("当前较五年低点倍数"))
    return_3m = finite_number(price.get("近3月涨跌幅"))
    current_ma6 = finite_number(price.get("当前相对6月均线"))

    core_buy = (
        total >= 74.0
        and quality >= 58.0
        and valuation >= 55.0
        and price_score >= 58.0
        and liquid >= 55.0
        and net_profit is not None and net_profit > 0
        and pe is not None and 0 < pe <= 35
        and (roe is None or roe >= 7.0)
        and (current_high is None or current_high <= 0.82)
        and (current_low is None or current_low <= 2.20)
        and (return_3m is None or return_3m <= 0.28)
        and (current_ma6 is None or 0.93 <= current_ma6 <= 1.25)
        and not property_like
    )
    exceptional = core_buy and total >= 80.0 and quality >= 63.0 and valuation >= 60.0 and price_score >= 64.0
    wait_pullback = (
        total >= 72.0 and quality >= 60.0 and net_profit is not None and net_profit > 0
        and ((return_3m is not None and return_3m > 0.28) or (current_ma6 is not None and current_ma6 > 1.25) or (current_high is not None and current_high > 0.82))
    )
    wait_confirmation = total >= 66.0 and not core_buy and not wait_pullback

    if exceptional:
        final_judgment = "特别适合分批建首仓"
    elif core_buy:
        final_judgment = "适合分批建首仓"
    elif wait_pullback:
        final_judgment = "好公司但等回调"
    elif wait_confirmation:
        final_judgment = "等待基本面或价格确认"
    else:
        final_judgment = "暂不优先"

    types: List[str] = []
    if dividend_yield is not None and dividend_yield >= 4.0 and pe is not None and 0 < pe <= 15 and (roe or annual_roe or 0) >= 8:
        types.append("高股息价值")
    if (roe or annual_roe or 0) >= 12 and (revenue_growth or -999) >= 5 and (profit_growth or -999) >= 10 and pe is not None and 0 < pe <= 30:
        types.append("质量成长合理")
    if (profit_growth or -999) >= 30 and (revenue_growth or -999) >= 0 and current_high is not None and current_high <= 0.70:
        types.append("盈利拐点")
    if cyclical:
        types.append("周期价值")
    if financial_like:
        types.append("金融价值")
    if l1_result and str(l1_result.get("核心候选")) in ("是", "True", "true"):
        types.append("L1技术共振")
    if not types:
        types.append("综合价值")

    complete_fields = [pe, pb, roe, revenue_growth, profit_growth, net_profit, price.get("当前占五年高点"), price.get("当前较五年低点倍数")]
    data_completeness = sum(value is not None for value in complete_fields) / len(complete_fields)

    notes: List[str] = []
    if cyclical:
        notes.append("周期行业，低PE可能是盈利高位")
    if property_like:
        notes.append("地产相关，未纳入核心买入")
    if profit_growth is not None and profit_growth < 0:
        notes.append("利润仍下滑")
    if cash_ratio is not None and cash_ratio < 0.5:
        notes.append("经营现金流弱于利润")
    if return_3m is not None and return_3m > 0.28:
        notes.append("近3个月涨幅偏大")
    if l1_result:
        notes.append(f"L1阶段={l1_result.get('当前阶段') or ''}")

    return {
        **record,
        "行业": industry,
        "最终判断": final_judgment,
        "候选类型": "+".join(types),
        "买入总分": total,
        "估值分": valuation,
        "质量分": quality,
        "增长分": growth,
        "价格位置分": price_score,
        "流动性分": liquid,
        "风险扣分": penalty,
        "市盈率": pe,
        "市净率": pb,
        "股息率": dividend_yield,
        "净资产收益率": roe,
        "收入同比增长": revenue_growth,
        "净利润同比增长": profit_growth,
        "净利润": net_profit,
        "每股收益": eps,
        "每股经营现金流": cfo_per_share,
        "现金流质量": cash_ratio,
        "毛利率或净利率": margin,
        "最新财报期": latest.get("报告期"),
        "年度收入同比增长": annual_revenue_growth,
        "年度净利润同比增长": annual_profit_growth,
        "年度净资产收益率": annual_roe,
        **price,
        "L1核心候选": "是" if l1_result and str(l1_result.get("核心候选")) in ("是", "True", "true") else "否",
        "L1当前阶段": l1_result.get("当前阶段") if l1_result else "",
        "L1综合分": finite_number(l1_result.get("综合分")) if l1_result else None,
        "数据完整度": data_completeness,
        "研究备注": "；".join(notes),
    }


def csv_value(value: Any) -> Any:
    if isinstance(value, bool):
        return "是" if value else "否"
    return value


def write_csv(path: Path, rows: Sequence[Dict[str, Any]], fields: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: csv_value(row.get(field)) for field in fields})


def main() -> int:
    args = parse_args()
    config = Config(
        output_dir=Path(args.output_dir),
        checkpoint_dir=Path(args.checkpoint_dir),
        workers=max(1, args.workers),
        requests_per_second=max(0.5, args.requests_per_second),
        hk_finance_workers=max(1, args.hk_finance_workers),
        hk_finance_requests_per_second=max(0.5, args.hk_finance_requests_per_second),
        history_count=max(60, args.history_count),
        limit_a=max(0, args.limit_a),
        limit_hk=max(0, args.limit_hk),
    )
    configure_logging(config.output_dir)
    started = time.time()

    a_universe = [l1.normalize_a_record(record) for record in base.fetch_a_universe()]
    hk_universe, hk_counts = l1.fetch_hk_connect_universe()
    if config.limit_a:
        a_universe = a_universe[: config.limit_a]
    if config.limit_hk:
        hk_universe = hk_universe[: config.limit_hk]
    universe = a_universe + hk_universe
    LOGGER.info("Universe ready A=%s HKConnect=%s total=%s", len(a_universe), len(hk_universe), len(universe))

    a_financial = fetch_a_financial_tables(config.output_dir)
    hk_financial, hk_financial_failures = fetch_hk_financials(hk_universe, config)
    price_data, price_failures = fetch_price_data(universe, config)

    # Reuse the completed L1 scan when available on the branch artifact is not required;
    # compute only a lightweight map from current full-market status if the optional file exists.
    l1_map: Dict[str, Dict[str, Any]] = {}
    optional_l1_path = Path("reports/a_hkconnect_l1_setup/核心候选.csv")
    if optional_l1_path.exists():
        with optional_l1_path.open("r", encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                l1_map[str(row.get("行情代码") or "")] = row

    results: List[Dict[str, Any]] = []
    missing_financial = 0
    for record in universe:
        symbol = record["行情代码"]
        if symbol not in price_data:
            continue
        if record.get("市场") == "A股":
            code = normalize_code(record.get("证券代码"), 6)
            latest = a_financial["latest"].get(code, {})
            annual = a_financial["annual"].get(code, {})
            dividend = a_financial["dividend"].get(code, {})
            if not latest:
                missing_financial += 1
        else:
            code = normalize_code(record.get("证券代码"), 5)
            latest = hk_financial.get(code, {})
            annual = {}
            dividend = {"股息率": latest.get("股息率")}
            if not latest:
                missing_financial += 1
        result = score_record(record, latest, annual, dividend, price_data[symbol], l1_map.get(symbol))
        results.append(result)

    results.sort(key=lambda row: float(row.get("买入总分") or -999), reverse=True)
    for rank, row in enumerate(results, 1):
        row["总排名"] = rank
    by_market: Dict[str, List[Dict[str, Any]]] = {}
    for row in results:
        by_market.setdefault(str(row.get("市场")), []).append(row)
    for rows in by_market.values():
        for rank, row in enumerate(rows, 1):
            row["市场排名"] = rank

    exceptional = [row for row in results if row["最终判断"] == "特别适合分批建首仓"]
    buyable = [row for row in results if row["最终判断"] in ("特别适合分批建首仓", "适合分批建首仓")]
    pullback = [row for row in results if row["最终判断"] == "好公司但等回调"]
    confirmation = [row for row in results if row["最终判断"] == "等待基本面或价格确认"]
    a_buyable = [row for row in buyable if row.get("市场") == "A股"]
    hk_buyable = [row for row in buyable if row.get("市场") == "港股通"]
    high_dividend = [row for row in results if "高股息价值" in str(row.get("候选类型")) and float(row.get("买入总分") or 0) >= 65]
    quality_growth = [row for row in results if "质量成长合理" in str(row.get("候选类型")) and float(row.get("买入总分") or 0) >= 68]
    turnaround = [row for row in results if "盈利拐点" in str(row.get("候选类型")) and float(row.get("买入总分") or 0) >= 65]

    write_csv(config.output_dir / "特别适合建首仓.csv", exceptional, OUTPUT_FIELDS)
    write_csv(config.output_dir / "适合分批建首仓.csv", buyable, OUTPUT_FIELDS)
    write_csv(config.output_dir / "A股可买候选.csv", a_buyable, OUTPUT_FIELDS)
    write_csv(config.output_dir / "港股通可买候选.csv", hk_buyable, OUTPUT_FIELDS)
    write_csv(config.output_dir / "好公司等回调.csv", pullback, OUTPUT_FIELDS)
    write_csv(config.output_dir / "等待确认.csv", confirmation, OUTPUT_FIELDS)
    write_csv(config.output_dir / "高股息价值候选.csv", high_dividend, OUTPUT_FIELDS)
    write_csv(config.output_dir / "质量成长合理候选.csv", quality_growth, OUTPUT_FIELDS)
    write_csv(config.output_dir / "盈利拐点候选.csv", turnaround, OUTPUT_FIELDS)
    write_csv(config.output_dir / "全市场综合评分.csv", results, OUTPUT_FIELDS)
    write_csv(
        config.output_dir / "抓取失败清单.csv",
        [
            {"类型": "HK财务", "代码": code, "错误": error}
            for code, error in sorted(hk_financial_failures.items())
        ] + [
            {"类型": "价格", "代码": symbol, "错误": error}
            for symbol, error in sorted(price_failures.items())
        ],
        ["类型", "代码", "错误"],
    )

    summary = {
        "生成时间": datetime.now().isoformat(timespec="seconds"),
        "A股股票池": len(a_universe),
        "港股通股票池": len(hk_universe),
        "合计股票池": len(universe),
        "成功综合评分": len(results),
        "特别适合建首仓": len(exceptional),
        "适合分批建首仓": len(buyable),
        "A股可买候选": len(a_buyable),
        "港股通可买候选": len(hk_buyable),
        "好公司等回调": len(pullback),
        "等待确认": len(confirmation),
        "高股息价值候选": len(high_dividend),
        "质量成长合理候选": len(quality_growth),
        "盈利拐点候选": len(turnaround),
        "价格抓取失败": len(price_failures),
        "港股财务抓取失败": len(hk_financial_failures),
        "缺少最新财务记录": missing_financial,
        "港股通股票池统计": hk_counts,
        "运行耗时秒": time.time() - started,
        "模型权重": {
            "估值": "28%",
            "质量": "27%",
            "增长": "20%",
            "价格位置": "20%",
            "流动性": "5%",
        },
        "核心限制": {
            "盈利": "最新净利润必须为正",
            "估值": "PE为正且不高于35倍",
            "位置": "通常不高于五年高点82%，距五年低点不超过2.2倍",
            "追涨过滤": "近3个月涨幅通常不超过28%，价格不显著偏离6月均线",
            "地产": "地产开发类不纳入核心买入",
            "提醒": "低PE周期股可能是盈利高位，需单独看周期位置",
        },
    }
    (config.output_dir / "筛选摘要.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    LOGGER.info("Completed summary=%s", json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
