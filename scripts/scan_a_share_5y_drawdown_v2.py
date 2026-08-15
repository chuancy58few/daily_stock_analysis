#!/usr/bin/env python3
"""Scan all A-share stocks for five-year peak-to-later-trough declines."""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import pickle
import random
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import numpy as np
import pandas as pd
import tushare as ts

LOGGER = logging.getLogger("a_share_5y_drawdown")
DEFAULT_HTTP_URL = "http://124.222.60.121:8020/"


@dataclass(frozen=True)
class Config:
    start_date: str
    end_date: str
    threshold_ratio: float
    output_dir: Path
    checkpoint_dir: Path
    request_pause: float

    @property
    def key(self) -> str:
        return f"{self.start_date}_{self.end_date}_{self.threshold_ratio:.8f}"


class Gateway:
    """Tushare client with custom-endpoint priority and official fallback."""

    def __init__(self, token: str, custom_url: str, request_pause: float) -> None:
        self.primary = ts.pro_api(token)
        self.primary._DataApi__http_url = custom_url
        self.fallback = ts.pro_api(token)
        self.custom_url = custom_url
        self.request_pause = request_pause

    def call(self, method: str, *, allow_empty: bool = False, **kwargs: Any) -> pd.DataFrame:
        last_error: Optional[BaseException] = None
        endpoint_plan = (
            (self.primary, self.custom_url, 5),
            (self.fallback, "official", 3),
        )
        for client, endpoint, attempts in endpoint_plan:
            fn = getattr(client, method)
            for attempt in range(1, attempts + 1):
                try:
                    frame = fn(**kwargs)
                    if frame is None:
                        frame = pd.DataFrame()
                    if frame.empty and not allow_empty:
                        raise RuntimeError(f"{method} returned no rows")
                    if self.request_pause:
                        time.sleep(self.request_pause)
                    return frame
                except Exception as exc:  # noqa: BLE001
                    last_error = exc
                    wait = min(30.0, 1.2 * 2 ** (attempt - 1)) + random.random()
                    LOGGER.warning(
                        "API failure method=%s endpoint=%s attempt=%s/%s error=%s; sleeping %.1fs",
                        method,
                        endpoint,
                        attempt,
                        attempts,
                        exc,
                        wait,
                    )
                    time.sleep(wait)
        raise RuntimeError(f"Tushare call failed: {method}, kwargs={kwargs}") from last_error


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", default="20210816")
    parser.add_argument("--end-date", default="20260814")
    parser.add_argument("--threshold-ratio", type=float, default=5.0)
    parser.add_argument("--output-dir", default="reports/a_share_5y_drawdown")
    parser.add_argument("--checkpoint-dir", default=".cache/a_share_5y_drawdown")
    parser.add_argument("--request-pause", type=float, default=0.02)
    return parser.parse_args()


def setup_logging(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    LOGGER.setLevel(logging.INFO)
    LOGGER.handlers.clear()
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)
    LOGGER.addHandler(stdout_handler)
    file_handler = logging.FileHandler(output_dir / "scan.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    LOGGER.addHandler(file_handler)


def clean_date(value: Any) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    return str(value).replace("-", "").replace("/", "").strip()[:8]


def load_metadata(gateway: Gateway) -> pd.DataFrame:
    fields = "ts_code,symbol,name,area,industry,market,exchange,list_status,list_date,delist_date,is_hs"
    pieces: List[pd.DataFrame] = []
    for status in ("L", "P", "D"):
        frame = gateway.call(
            "stock_basic",
            allow_empty=True,
            exchange="",
            list_status=status,
            fields=fields,
        )
        if not frame.empty:
            frame = frame.copy()
            frame["list_status"] = status
            pieces.append(frame)
    if not pieces:
        raise RuntimeError("stock_basic returned no rows")

    metadata = pd.concat(pieces, ignore_index=True)
    metadata["symbol"] = metadata["symbol"].astype(str).str.zfill(6)
    metadata["list_date"] = metadata["list_date"].map(clean_date)
    metadata["delist_date"] = metadata["delist_date"].map(clean_date)

    # Keep exchange-listed stocks and exclude Shanghai/Shenzhen B-share code ranges.
    metadata = metadata[metadata["exchange"].isin(["SSE", "SZSE", "BSE"])].copy()
    metadata = metadata[~metadata["symbol"].str.startswith(("200", "900"))].copy()

    metadata["_priority"] = metadata["list_status"].map({"L": 0, "P": 1, "D": 2}).fillna(9)
    metadata = (
        metadata.sort_values(["ts_code", "_priority", "list_date"], ascending=[True, True, False])
        .drop_duplicates("ts_code", keep="first")
        .drop(columns=["_priority"])
        .reset_index(drop=True)
    )
    return metadata


def load_trade_dates(gateway: Gateway, start_date: str, end_date: str) -> List[str]:
    calendar = gateway.call(
        "trade_cal",
        exchange="SSE",
        start_date=start_date,
        end_date=end_date,
        is_open="1",
        fields="cal_date,is_open",
    )
    dates = sorted(calendar.loc[calendar["is_open"].astype(str) == "1", "cal_date"].astype(str))
    if not dates:
        raise RuntimeError("trade_cal returned no open dates")
    return dates


def empty_state() -> Dict[str, Any]:
    return {
        "trade_days": 0,
        "first_trade_date": "",
        "last_trade_date": "",
        "running_peak": None,
        "running_peak_date": "",
        "running_peak_raw_high": None,
        "running_peak_factor": None,
        "max_ratio": 1.0,
        "peak_date": "",
        "trough_date": "",
        "peak_adjusted": None,
        "trough_adjusted": None,
        "peak_raw_high": None,
        "trough_raw_low": None,
        "peak_factor": None,
        "trough_factor": None,
        "absolute_high": None,
        "absolute_high_date": "",
        "absolute_low": None,
        "absolute_low_date": "",
        "latest_close": None,
        "latest_adjusted_close": None,
        "latest_factor": None,
        "latest_amount": None,
        "latest_volume": None,
        "missing_factor_rows": 0,
    }


def fetch_day(gateway: Gateway, trade_date: str) -> pd.DataFrame:
    daily = gateway.call(
        "daily",
        trade_date=trade_date,
        fields="ts_code,trade_date,high,low,close,vol,amount",
    )
    factor = gateway.call(
        "adj_factor",
        trade_date=trade_date,
        fields="ts_code,trade_date,adj_factor",
    )
    daily = daily.drop_duplicates("ts_code", keep="last")
    factor = factor.drop_duplicates("ts_code", keep="last")
    merged = daily.merge(factor[["ts_code", "adj_factor"]], on="ts_code", how="left")
    for column in ("high", "low", "close", "vol", "amount", "adj_factor"):
        merged[column] = pd.to_numeric(merged[column], errors="coerce")
    return merged


def update_states(
    states: Dict[str, Dict[str, Any]],
    day: pd.DataFrame,
    trade_date: str,
    allowed_codes: Set[str],
) -> Dict[str, int]:
    stats = {"rows": 0, "skipped_non_a": 0, "invalid_prices": 0, "missing_factors": 0}
    for row in day.itertuples(index=False):
        stats["rows"] += 1
        code = str(row.ts_code)
        if code not in allowed_codes:
            stats["skipped_non_a"] += 1
            continue

        high = float(row.high) if pd.notna(row.high) else math.nan
        low = float(row.low) if pd.notna(row.low) else math.nan
        close = float(row.close) if pd.notna(row.close) else math.nan
        if not all(math.isfinite(value) for value in (high, low, close)) or high <= 0 or low <= 0:
            stats["invalid_prices"] += 1
            continue

        state = states.setdefault(code, empty_state())
        factor = float(row.adj_factor) if pd.notna(row.adj_factor) else math.nan
        if not math.isfinite(factor) or factor <= 0:
            factor = float(state["latest_factor"] or 1.0)
            state["missing_factor_rows"] += 1
            stats["missing_factors"] += 1

        adjusted_high = high * factor
        adjusted_low = low * factor
        adjusted_close = close * factor

        state["trade_days"] += 1
        if not state["first_trade_date"]:
            state["first_trade_date"] = trade_date
        state["last_trade_date"] = trade_date

        if state["absolute_high"] is None or adjusted_high > state["absolute_high"]:
            state["absolute_high"] = adjusted_high
            state["absolute_high_date"] = trade_date
        if state["absolute_low"] is None or adjusted_low < state["absolute_low"]:
            state["absolute_low"] = adjusted_low
            state["absolute_low_date"] = trade_date

        # Strict ordering: evaluate today's low only against peaks from earlier trading days.
        if state["running_peak"] is not None and adjusted_low > 0:
            ratio = state["running_peak"] / adjusted_low
            if ratio > state["max_ratio"]:
                state["max_ratio"] = ratio
                state["peak_date"] = state["running_peak_date"]
                state["trough_date"] = trade_date
                state["peak_adjusted"] = state["running_peak"]
                state["trough_adjusted"] = adjusted_low
                state["peak_raw_high"] = state["running_peak_raw_high"]
                state["trough_raw_low"] = low
                state["peak_factor"] = state["running_peak_factor"]
                state["trough_factor"] = factor

        if state["running_peak"] is None or adjusted_high > state["running_peak"]:
            state["running_peak"] = adjusted_high
            state["running_peak_date"] = trade_date
            state["running_peak_raw_high"] = high
            state["running_peak_factor"] = factor

        state["latest_close"] = close
        state["latest_adjusted_close"] = adjusted_close
        state["latest_factor"] = factor
        state["latest_amount"] = float(row.amount) if pd.notna(row.amount) else None
        state["latest_volume"] = float(row.vol) if pd.notna(row.vol) else None
    return stats


def save_checkpoint(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    with temporary.open("wb") as handle:
        pickle.dump(payload, handle, protocol=pickle.HIGHEST_PROTOCOL)
    temporary.replace(path)


def load_checkpoint(path: Path, config_key: str) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    try:
        with path.open("rb") as handle:
            payload = pickle.load(handle)
        return payload if payload.get("config_key") == config_key else None
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Checkpoint could not be loaded: %s", exc)
        return None


def divide(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None or b == 0:
        return None
    return a / b


def elapsed_days(start: str, end: str) -> Optional[int]:
    if not start or not end:
        return None
    return (datetime.strptime(end, "%Y%m%d") - datetime.strptime(start, "%Y%m%d")).days


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


def build_results(states: Dict[str, Dict[str, Any]], metadata: pd.DataFrame, config: Config) -> pd.DataFrame:
    metadata_map = metadata.set_index("ts_code").to_dict("index")
    rows: List[Dict[str, Any]] = []
    for code, state in states.items():
        meta = metadata_map.get(code)
        if not meta:
            continue
        latest_factor = float(state["latest_factor"] or 1.0)
        peak_qfq = divide(state["peak_adjusted"], latest_factor)
        trough_qfq = divide(state["trough_adjusted"], latest_factor)
        absolute_high_qfq = divide(state["absolute_high"], latest_factor)
        absolute_low_qfq = divide(state["absolute_low"], latest_factor)
        ratio = float(state["max_ratio"] or 1.0)
        drawdown = 1.0 - 1.0 / ratio if ratio > 0 else None
        latest_close = state["latest_close"]
        status = str(meta.get("list_status", ""))
        notes: List[str] = []
        if state["trade_days"] < 60:
            notes.append("有效交易日不足60天")
        if state["missing_factor_rows"]:
            notes.append(f"复权因子缺失{state['missing_factor_rows']}行")

        rows.append(
            {
                "股票代码": code,
                "证券代码": meta.get("symbol", code.split(".")[0]),
                "股票名称": meta.get("name", ""),
                "交易所": meta.get("exchange", ""),
                "板块": meta.get("market", ""),
                "所属行业": meta.get("industry", ""),
                "地区": meta.get("area", ""),
                "上市状态": status,
                "当前上市": status == "L",
                "上市日期": meta.get("list_date", ""),
                "退市日期": meta.get("delist_date", ""),
                "沪深港通": meta.get("is_hs", ""),
                "统计开始日": config.start_date,
                "统计结束日": config.end_date,
                "首个交易日": state["first_trade_date"],
                "最后交易日": state["last_trade_date"],
                "有效交易日数": state["trade_days"],
                "高点日期": state["peak_date"],
                "低点日期": state["trough_date"],
                "高点严格早于低点": bool(state["peak_date"] and state["trough_date"] and state["peak_date"] < state["trough_date"]),
                "高低历时_自然日": elapsed_days(state["peak_date"], state["trough_date"]),
                "前复权高点价": peak_qfq,
                "前复权低点价": trough_qfq,
                "高点原始最高价": state["peak_raw_high"],
                "低点原始最低价": state["trough_raw_low"],
                "高低倍数": ratio,
                "最大回撤": drawdown,
                "回撤等级": severity(ratio),
                "是否达到5倍": ratio >= config.threshold_ratio,
                "绝对最高日期": state["absolute_high_date"],
                "绝对最低日期": state["absolute_low_date"],
                "绝对最高前复权价": absolute_high_qfq,
                "绝对最低前复权价": absolute_low_qfq,
                "绝对高低倍数_忽略顺序": divide(absolute_high_qfq, absolute_low_qfq),
                "最新收盘价": latest_close,
                "最新价较回撤高点": (latest_close / peak_qfq - 1.0) if peak_qfq else None,
                "最新价较回撤低点": (latest_close / trough_qfq - 1.0) if trough_qfq else None,
                "最新成交额_千元": state["latest_amount"],
                "最新成交量_手": state["latest_volume"],
                "复权因子缺失行数": state["missing_factor_rows"],
                "数据质量备注": "；".join(notes),
            }
        )

    result = pd.DataFrame(rows)
    if result.empty:
        raise RuntimeError("No stock-level results were generated")
    result = result.sort_values(["高低倍数", "最大回撤"], ascending=[False, False]).reset_index(drop=True)
    result.insert(0, "排名", np.arange(1, len(result) + 1))
    return result


def industry_summary(results: pd.DataFrame, threshold: float) -> pd.DataFrame:
    current = results.loc[results["当前上市"]].copy()
    current["所属行业"] = current["所属行业"].replace("", "未分类").fillna("未分类")
    summary = (
        current.groupby("所属行业", dropna=False)
        .agg(
            当前上市股票数=("股票代码", "nunique"),
            五倍回撤股票数=("高低倍数", lambda values: int((values >= threshold).sum())),
            行业中位高低倍数=("高低倍数", "median"),
            行业最大高低倍数=("高低倍数", "max"),
            行业中位最大回撤=("最大回撤", "median"),
        )
        .reset_index()
    )
    summary["五倍回撤占比"] = summary["五倍回撤股票数"] / summary["当前上市股票数"]
    summary = summary.sort_values(
        ["五倍回撤股票数", "五倍回撤占比", "行业最大高低倍数"], ascending=[False, False, False]
    ).reset_index(drop=True)
    summary.insert(0, "排名", np.arange(1, len(summary) + 1))
    return summary


def write_outputs(results: pd.DataFrame, industries: pd.DataFrame, config: Config, diagnostics: Dict[str, Any]) -> None:
    config.output_dir.mkdir(parents=True, exist_ok=True)
    hits_all = results.loc[results["高低倍数"] >= config.threshold_ratio].copy()
    hits_current = hits_all.loc[hits_all["当前上市"]].copy()
    hits_non_current = hits_all.loc[~hits_all["当前上市"]].copy()

    outputs = {
        "A股近5年全市场最大回撤.csv": results,
        "A股近5年跌幅5倍名单_全部状态.csv": hits_all,
        "A股近5年跌幅5倍名单_当前上市.csv": hits_current,
        "A股近5年跌幅5倍名单_退市或非当前上市.csv": hits_non_current,
        "A股近5年跌幅5倍_行业统计.csv": industries,
    }
    for filename, frame in outputs.items():
        frame.to_csv(config.output_dir / filename, index=False, encoding="utf-8-sig")

    board_summary = (
        hits_current.groupby("板块", dropna=False)
        .agg(
            五倍回撤股票数=("股票代码", "nunique"),
            中位高低倍数=("高低倍数", "median"),
            最大高低倍数=("高低倍数", "max"),
        )
        .reset_index()
        .sort_values("五倍回撤股票数", ascending=False)
    )
    board_summary.to_csv(config.output_dir / "A股近5年跌幅5倍_板块统计.csv", index=False, encoding="utf-8-sig")

    summary = {
        "生成时间": datetime.now().isoformat(timespec="seconds"),
        "统计开始日": config.start_date,
        "统计结束日": config.end_date,
        "筛选阈值_高低倍数": config.threshold_ratio,
        "对应最大回撤": 1.0 - 1.0 / config.threshold_ratio,
        "有行情记录股票数": int(len(results)),
        "当前上市有行情股票数": int(results["当前上市"].sum()),
        "达标股票数_全部状态": int(len(hits_all)),
        "达标股票数_当前上市": int(len(hits_current)),
        "达标股票数_退市或非当前上市": int(len(hits_non_current)),
        "全市场最大高低倍数": float(results["高低倍数"].max()),
        "全市场中位高低倍数": float(results["高低倍数"].median()),
        "诊断信息": diagnostics,
        "口径": {
            "价格口径": "日最高价/最低价乘以Tushare复权因子，价格倍数不受统一缩放影响；输出价格按最后交易日因子归一化为前复权口径。",
            "主指标": "滚动历史最高前复权价 ÷ 后续交易日最低前复权价。",
            "时间顺序": "高点交易日必须严格早于低点交易日，同日高低不计入。",
            "五倍含义": "高低倍数达到5.0，对应从高点到低点下跌80%。",
            "股票范围": "沪深北交易所股票基础表，排除200开头深圳B股与900开头上海B股；包含窗口内有交易的退市股票。",
        },
    }
    with (config.output_dir / "筛选摘要.json").open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, ensure_ascii=False, indent=2)

    LOGGER.info(
        "Completed: all=%s current=%s hits_all=%s hits_current=%s hits_non_current=%s",
        len(results),
        int(results["当前上市"].sum()),
        len(hits_all),
        len(hits_current),
        len(hits_non_current),
    )


def main() -> int:
    args = parse_args()
    config = Config(
        start_date=args.start_date,
        end_date=args.end_date,
        threshold_ratio=args.threshold_ratio,
        output_dir=Path(args.output_dir),
        checkpoint_dir=Path(args.checkpoint_dir),
        request_pause=args.request_pause,
    )
    setup_logging(config.output_dir)

    token = os.getenv("TUSHARE_TOKEN", "").strip()
    if not token:
        raise RuntimeError("TUSHARE_TOKEN is not configured")
    custom_url = os.getenv("TUSHARE_HTTP_URL", DEFAULT_HTTP_URL).strip() or DEFAULT_HTTP_URL
    gateway = Gateway(token, custom_url, config.request_pause)

    metadata = load_metadata(gateway)
    allowed_codes = set(metadata["ts_code"].astype(str))
    dates = load_trade_dates(gateway, config.start_date, config.end_date)
    LOGGER.info(
        "Scan start=%s end=%s threshold=%.2fx metadata=%s trading_days=%s endpoint=%s",
        config.start_date,
        config.end_date,
        config.threshold_ratio,
        len(metadata),
        len(dates),
        custom_url,
    )

    checkpoint_path = config.checkpoint_dir / "checkpoint.pkl"
    checkpoint = load_checkpoint(checkpoint_path, config.key)
    if checkpoint:
        states = checkpoint.get("states", {})
        completed_dates = set(checkpoint.get("completed_dates", []))
        diagnostics = checkpoint.get("diagnostics", {})
        LOGGER.info("Resuming: completed_dates=%s states=%s", len(completed_dates), len(states))
    else:
        states: Dict[str, Dict[str, Any]] = {}
        completed_dates: Set[str] = set()
        diagnostics = {
            "daily_rows": 0,
            "skipped_non_a": 0,
            "invalid_prices": 0,
            "missing_factors": 0,
            "completed_dates": 0,
        }

    remaining = [trade_date for trade_date in dates if trade_date not in completed_dates]
    for position, trade_date in enumerate(remaining, 1):
        day = fetch_day(gateway, trade_date)
        day_stats = update_states(states, day, trade_date, allowed_codes)
        completed_dates.add(trade_date)
        diagnostics["daily_rows"] += day_stats["rows"]
        diagnostics["skipped_non_a"] += day_stats["skipped_non_a"]
        diagnostics["invalid_prices"] += day_stats["invalid_prices"]
        diagnostics["missing_factors"] += day_stats["missing_factors"]
        diagnostics["completed_dates"] = len(completed_dates)

        if position % 20 == 0 or position == len(remaining):
            provisional = sum(1 for state in states.values() if float(state["max_ratio"]) >= config.threshold_ratio)
            LOGGER.info(
                "Progress new=%s/%s total=%s/%s date=%s states=%s provisional_hits=%s",
                position,
                len(remaining),
                len(completed_dates),
                len(dates),
                trade_date,
                len(states),
                provisional,
            )
        if position % 25 == 0 or position == len(remaining):
            save_checkpoint(
                checkpoint_path,
                {
                    "config_key": config.key,
                    "states": states,
                    "completed_dates": sorted(completed_dates),
                    "diagnostics": diagnostics,
                },
            )

    diagnostics["metadata_rows"] = len(metadata)
    diagnostics["allowed_codes"] = len(allowed_codes)
    diagnostics["trade_dates"] = len(dates)
    diagnostics["stock_states"] = len(states)
    results = build_results(states, metadata, config)
    industries = industry_summary(results, config.threshold_ratio)
    write_outputs(results, industries, config, diagnostics)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
