#!/usr/bin/env python3
"""Screen the full A-share universe for five-year peak-to-subsequent-trough drawdowns."""

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
from typing import Any, Dict, Iterable, List, Optional

import numpy as np
import pandas as pd
import tushare as ts


LOGGER = logging.getLogger("a_share_drawdown")
DEFAULT_HTTP_URL = "http://124.222.60.121:8020/"


@dataclass(frozen=True)
class ScanConfig:
    start_date: str
    end_date: str
    threshold_ratio: float
    output_dir: Path
    checkpoint_dir: Path
    request_pause: float

    @property
    def config_id(self) -> str:
        return f"{self.start_date}_{self.end_date}_{self.threshold_ratio:.6f}"


class TushareGateway:
    """Call the custom Tushare endpoint first and fall back to the official endpoint."""

    def __init__(self, token: str, http_url: str, request_pause: float) -> None:
        self.request_pause = request_pause
        self.primary = ts.pro_api(token)
        self.primary._DataApi__http_url = http_url
        self.fallback = ts.pro_api(token)
        self.primary_label = http_url
        self.fallback_label = "official Tushare endpoint"

    def call(
        self,
        method_name: str,
        *,
        allow_empty: bool = False,
        primary_attempts: int = 5,
        fallback_attempts: int = 3,
        **kwargs: Any,
    ) -> pd.DataFrame:
        last_error: Optional[BaseException] = None
        endpoints = (
            (self.primary, self.primary_label, primary_attempts),
            (self.fallback, self.fallback_label, fallback_attempts),
        )

        for client, label, attempts in endpoints:
            method = getattr(client, method_name)
            for attempt in range(1, attempts + 1):
                try:
                    frame = method(**kwargs)
                    if frame is None:
                        frame = pd.DataFrame()
                    if frame.empty and not allow_empty:
                        raise RuntimeError(f"{method_name} returned an empty data frame")
                    if self.request_pause > 0:
                        time.sleep(self.request_pause)
                    return frame
                except Exception as exc:  # noqa: BLE001
                    last_error = exc
                    wait_seconds = min(30.0, 1.25 * (2 ** (attempt - 1))) + random.random()
                    LOGGER.warning(
                        "API call failed: method=%s endpoint=%s attempt=%s/%s error=%s; retry in %.1fs",
                        method_name,
                        label,
                        attempt,
                        attempts,
                        exc,
                        wait_seconds,
                    )
                    time.sleep(wait_seconds)

        raise RuntimeError(f"Tushare call failed: {method_name}, kwargs={kwargs}") from last_error


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", default="20210816", help="Inclusive start date in YYYYMMDD format")
    parser.add_argument("--end-date", default="20260814", help="Inclusive end date in YYYYMMDD format")
    parser.add_argument("--threshold-ratio", type=float, default=5.0, help="Peak/trough ratio threshold")
    parser.add_argument("--output-dir", default="reports/a_share_5y_drawdown", help="Output directory")
    parser.add_argument("--checkpoint-dir", default=".cache/a_share_drawdown", help="Checkpoint directory")
    parser.add_argument("--request-pause", type=float, default=0.03, help="Pause after a successful API call")
    return parser.parse_args()


def configure_logging(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    log_path = output_dir / "scan.log"
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    LOGGER.setLevel(logging.INFO)
    LOGGER.handlers.clear()

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    LOGGER.addHandler(stream_handler)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    LOGGER.addHandler(file_handler)


def normalize_date_text(value: Any) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    text = str(value).replace("-", "").replace("/", "").strip()
    return text[:8]


def load_stock_metadata(gateway: TushareGateway) -> pd.DataFrame:
    fields = "ts_code,symbol,name,area,industry,market,exchange,list_status,list_date,delist_date,is_hs"
    frames: List[pd.DataFrame] = []
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
            frames.append(frame)

    if not frames:
        raise RuntimeError("No stock metadata was returned")

    metadata = pd.concat(frames, ignore_index=True)
    metadata["list_date"] = metadata["list_date"].map(normalize_date_text)
    metadata["delist_date"] = metadata["delist_date"].map(normalize_date_text)
    metadata["status_priority"] = metadata["list_status"].map({"L": 0, "P": 1, "D": 2}).fillna(9)
    metadata = metadata.sort_values(
        ["ts_code", "status_priority", "list_date"], ascending=[True, True, False]
    ).drop_duplicates("ts_code", keep="first")
    metadata = metadata.drop(columns=["status_priority"])
    return metadata


def load_trade_dates(gateway: TushareGateway, start_date: str, end_date: str) -> List[str]:
    calendar = gateway.call(
        "trade_cal",
        exchange="SSE",
        start_date=start_date,
        end_date=end_date,
        is_open="1",
        fields="cal_date,is_open",
    )
    dates = sorted(calendar.loc[calendar["is_open"].astype(str) == "1", "cal_date"].astype(str).tolist())
    if not dates:
        raise RuntimeError("No open trading dates were returned")
    return dates


def new_state() -> Dict[str, Any]:
    return {
        "trade_days": 0,
        "first_trade_date": "",
        "last_trade_date": "",
        "running_peak_adj_high": None,
        "running_peak_date": "",
        "running_peak_raw_high": None,
        "running_peak_factor": None,
        "max_drawdown_ratio": 1.0,
        "drawdown_peak_date": "",
        "drawdown_trough_date": "",
        "drawdown_peak_adj_high": None,
        "drawdown_trough_adj_low": None,
        "drawdown_peak_raw_high": None,
        "drawdown_trough_raw_low": None,
        "drawdown_peak_factor": None,
        "drawdown_trough_factor": None,
        "absolute_max_adj_high": None,
        "absolute_max_date": "",
        "absolute_max_raw_high": None,
        "absolute_max_factor": None,
        "absolute_min_adj_low": None,
        "absolute_min_date": "",
        "absolute_min_raw_low": None,
        "absolute_min_factor": None,
        "latest_raw_close": None,
        "latest_adj_close": None,
        "latest_factor": None,
        "latest_amount": None,
        "latest_volume": None,
        "missing_factor_rows": 0,
    }


def save_checkpoint(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(path.suffix + ".tmp")
    with temporary_path.open("wb") as handle:
        pickle.dump(payload, handle, protocol=pickle.HIGHEST_PROTOCOL)
    temporary_path.replace(path)


def load_checkpoint(path: Path, config_id: str) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    try:
        with path.open("rb") as handle:
            payload = pickle.load(handle)
        if payload.get("config_id") != config_id:
            LOGGER.info("Ignoring checkpoint because its configuration does not match")
            return None
        return payload
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Unable to load checkpoint %s: %s", path, exc)
        return None


def fetch_one_day(gateway: TushareGateway, trade_date: str) -> pd.DataFrame:
    daily = gateway.call(
        "daily",
        trade_date=trade_date,
        fields="ts_code,trade_date,open,high,low,close,pre_close,vol,amount",
    )
    factors = gateway.call(
        "adj_factor",
        trade_date=trade_date,
        fields="ts_code,trade_date,adj_factor",
    )

    daily = daily.drop_duplicates("ts_code", keep="last")
    factors = factors.drop_duplicates("ts_code", keep="last")
    merged = daily.merge(factors[["ts_code", "adj_factor"]], on="ts_code", how="left")
    for column in ("high", "low", "close", "vol", "amount", "adj_factor"):
        merged[column] = pd.to_numeric(merged[column], errors="coerce")
    return merged


def update_states(states: Dict[str, Dict[str, Any]], daily: pd.DataFrame, trade_date: str) -> Dict[str, int]:
    counters = {"rows": 0, "invalid_price_rows": 0, "missing_factor_rows": 0}

    for row in daily.itertuples(index=False):
        counters["rows"] += 1
        ts_code = str(row.ts_code)
        high = float(row.high) if pd.notna(row.high) else math.nan
        low = float(row.low) if pd.notna(row.low) else math.nan
        close = float(row.close) if pd.notna(row.close) else math.nan

        if not (math.isfinite(high) and math.isfinite(low) and math.isfinite(close)) or high <= 0 or low <= 0:
            counters["invalid_price_rows"] += 1
            continue

        state = states.setdefault(ts_code, new_state())
        factor_value = float(row.adj_factor) if pd.notna(row.adj_factor) else math.nan
        if not math.isfinite(factor_value) or factor_value <= 0:
            factor_value = state["latest_factor"] if state["latest_factor"] else 1.0
            state["missing_factor_rows"] += 1
            counters["missing_factor_rows"] += 1

        adj_high = high * factor_value
        adj_low = low * factor_value
        adj_close = close * factor_value

        state["trade_days"] += 1
        if not state["first_trade_date"]:
            state["first_trade_date"] = trade_date
        state["last_trade_date"] = trade_date

        if state["absolute_max_adj_high"] is None or adj_high > state["absolute_max_adj_high"]:
            state["absolute_max_adj_high"] = adj_high
            state["absolute_max_date"] = trade_date
            state["absolute_max_raw_high"] = high
            state["absolute_max_factor"] = factor_value

        if state["absolute_min_adj_low"] is None or adj_low < state["absolute_min_adj_low"]:
            state["absolute_min_adj_low"] = adj_low
            state["absolute_min_date"] = trade_date
            state["absolute_min_raw_low"] = low
            state["absolute_min_factor"] = factor_value

        if state["running_peak_adj_high"] is None or adj_high > state["running_peak_adj_high"]:
            state["running_peak_adj_high"] = adj_high
            state["running_peak_date"] = trade_date
            state["running_peak_raw_high"] = high
            state["running_peak_factor"] = factor_value

        if state["running_peak_adj_high"] and adj_low > 0:
            ratio = state["running_peak_adj_high"] / adj_low
            if ratio > state["max_drawdown_ratio"]:
                state["max_drawdown_ratio"] = ratio
                state["drawdown_peak_date"] = state["running_peak_date"]
                state["drawdown_trough_date"] = trade_date
                state["drawdown_peak_adj_high"] = state["running_peak_adj_high"]
                state["drawdown_trough_adj_low"] = adj_low
                state["drawdown_peak_raw_high"] = state["running_peak_raw_high"]
                state["drawdown_trough_raw_low"] = low
                state["drawdown_peak_factor"] = state["running_peak_factor"]
                state["drawdown_trough_factor"] = factor_value

        state["latest_raw_close"] = close
        state["latest_adj_close"] = adj_close
        state["latest_factor"] = factor_value
        state["latest_amount"] = float(row.amount) if pd.notna(row.amount) else None
        state["latest_volume"] = float(row.vol) if pd.notna(row.vol) else None

    return counters


def safe_ratio(numerator: Optional[float], denominator: Optional[float]) -> Optional[float]:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return numerator / denominator


def date_diff_days(start_text: str, end_text: str) -> Optional[int]:
    if not start_text or not end_text:
        return None
    start = datetime.strptime(start_text, "%Y%m%d")
    end = datetime.strptime(end_text, "%Y%m%d")
    return (end - start).days


def build_result_frame(
    states: Dict[str, Dict[str, Any]],
    metadata: pd.DataFrame,
    config: ScanConfig,
) -> pd.DataFrame:
    metadata_map = metadata.set_index("ts_code").to_dict("index")
    rows: List[Dict[str, Any]] = []

    for ts_code, state in states.items():
        latest_factor = state["latest_factor"] or 1.0
        peak_qfq = safe_ratio(state["drawdown_peak_adj_high"], latest_factor)
        trough_qfq = safe_ratio(state["drawdown_trough_adj_low"], latest_factor)
        absolute_max_qfq = safe_ratio(state["absolute_max_adj_high"], latest_factor)
        absolute_min_qfq = safe_ratio(state["absolute_min_adj_low"], latest_factor)
        latest_close = state["latest_raw_close"]
        max_ratio = float(state["max_drawdown_ratio"] or 1.0)
        max_drawdown = 1.0 - (1.0 / max_ratio) if max_ratio > 0 else None
        meta = metadata_map.get(ts_code, {})
        list_status = str(meta.get("list_status", "未知"))
        current_listed = list_status == "L"
        absolute_ratio = safe_ratio(absolute_max_qfq, absolute_min_qfq)

        if max_ratio >= 10:
            severity = "10倍及以上"
        elif max_ratio >= 7.5:
            severity = "7.5—10倍"
        elif max_ratio >= 5:
            severity = "5—7.5倍"
        elif max_ratio >= 3:
            severity = "3—5倍"
        elif max_ratio >= 2:
            severity = "2—3倍"
        else:
            severity = "不足2倍"

        quality_notes: List[str] = []
        if state["trade_days"] < 60:
            quality_notes.append("交易日不足60天")
        if state["missing_factor_rows"]:
            quality_notes.append(f"复权因子缺失{state['missing_factor_rows']}行")
        if not meta:
            quality_notes.append("缺少股票基础信息")

        rows.append(
            {
                "股票代码": ts_code,
                "证券代码": meta.get("symbol", ts_code.split(".")[0]),
                "股票名称": meta.get("name", ""),
                "交易所": meta.get("exchange", ts_code.split(".")[-1]),
                "板块": meta.get("market", ""),
                "所属行业": meta.get("industry", ""),
                "地区": meta.get("area", ""),
                "上市状态": list_status,
                "当前上市": current_listed,
                "上市日期": meta.get("list_date", ""),
                "退市日期": meta.get("delist_date", ""),
                "沪深港通": meta.get("is_hs", ""),
                "统计开始日": config.start_date,
                "统计结束日": config.end_date,
                "首个交易日": state["first_trade_date"],
                "最后交易日": state["last_trade_date"],
                "有效交易日数": state["trade_days"],
                "高点日期": state["drawdown_peak_date"],
                "低点日期": state["drawdown_trough_date"],
                "高低历时_自然日": date_diff_days(state["drawdown_peak_date"], state["drawdown_trough_date"]),
                "前复权高点价": peak_qfq,
                "前复权低点价": trough_qfq,
                "高点原始最高价": state["drawdown_peak_raw_high"],
                "低点原始最低价": state["drawdown_trough_raw_low"],
                "高低倍数": max_ratio,
                "最大回撤": max_drawdown,
                "回撤等级": severity,
                "是否达到5倍": max_ratio >= config.threshold_ratio,
                "绝对最高日期": state["absolute_max_date"],
                "绝对最低日期": state["absolute_min_date"],
                "绝对最高前复权价": absolute_max_qfq,
                "绝对最低前复权价": absolute_min_qfq,
                "绝对高低倍数_不考虑顺序": absolute_ratio,
                "最新收盘价": latest_close,
                "最新价较回撤高点": safe_ratio(latest_close, peak_qfq) - 1 if safe_ratio(latest_close, peak_qfq) is not None else None,
                "最新价较回撤低点": safe_ratio(latest_close, trough_qfq) - 1 if safe_ratio(latest_close, trough_qfq) is not None else None,
                "最新成交额_千元": state["latest_amount"],
                "最新成交量_手": state["latest_volume"],
                "复权因子缺失行数": state["missing_factor_rows"],
                "数据质量备注": "；".join(quality_notes),
            }
        )

    result = pd.DataFrame(rows)
    if result.empty:
        raise RuntimeError("The scan produced no stock-level results")
    result = result.sort_values(["高低倍数", "最大回撤"], ascending=[False, False]).reset_index(drop=True)
    result.insert(0, "排名", np.arange(1, len(result) + 1))
    return result


def build_industry_summary(all_results: pd.DataFrame, threshold_ratio: float) -> pd.DataFrame:
    current = all_results.loc[all_results["当前上市"]].copy()
    current["所属行业"] = current["所属行业"].replace("", "未分类").fillna("未分类")
    grouped = current.groupby("所属行业", dropna=False)
    summary = grouped.agg(
        当前上市股票数=("股票代码", "nunique"),
        五倍回撤股票数=("高低倍数", lambda values: int((values >= threshold_ratio).sum())),
        行业中位高低倍数=("高低倍数", "median"),
        行业最大高低倍数=("高低倍数", "max"),
        行业中位最大回撤=("最大回撤", "median"),
    ).reset_index()
    summary["五倍回撤占比"] = summary["五倍回撤股票数"] / summary["当前上市股票数"]
    summary = summary.sort_values(
        ["五倍回撤股票数", "五倍回撤占比", "行业最大高低倍数"], ascending=[False, False, False]
    ).reset_index(drop=True)
    summary.insert(0, "排名", np.arange(1, len(summary) + 1))
    return summary


def write_outputs(
    all_results: pd.DataFrame,
    industry_summary: pd.DataFrame,
    config: ScanConfig,
    diagnostics: Dict[str, Any],
) -> None:
    config.output_dir.mkdir(parents=True, exist_ok=True)
    hits_all = all_results.loc[all_results["高低倍数"] >= config.threshold_ratio].copy()
    hits_current = hits_all.loc[hits_all["当前上市"]].copy()
    delisted_hits = hits_all.loc[~hits_all["当前上市"]].copy()

    files = {
        "a_share_5y_drawdown_all.csv": all_results,
        "a_share_5x_drawdown_hits_all.csv": hits_all,
        "a_share_5x_drawdown_hits_current.csv": hits_current,
        "a_share_5x_drawdown_hits_non_current.csv": delisted_hits,
        "a_share_5x_drawdown_industry_summary.csv": industry_summary,
    }
    for filename, frame in files.items():
        frame.to_csv(config.output_dir / filename, index=False, encoding="utf-8-sig")

    market_summary = (
        hits_current.groupby("板块", dropna=False)
        .agg(五倍回撤股票数=("股票代码", "nunique"), 中位高低倍数=("高低倍数", "median"), 最大高低倍数=("高低倍数", "max"))
        .reset_index()
        .sort_values("五倍回撤股票数", ascending=False)
    )
    market_summary.to_csv(
        config.output_dir / "a_share_5x_drawdown_market_summary.csv", index=False, encoding="utf-8-sig"
    )

    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "start_date": config.start_date,
        "end_date": config.end_date,
        "threshold_ratio": config.threshold_ratio,
        "threshold_drawdown_pct": 1.0 - 1.0 / config.threshold_ratio,
        "universe_with_price_history": int(len(all_results)),
        "current_listed_with_price_history": int(all_results["当前上市"].sum()),
        "all_hits": int(len(hits_all)),
        "current_listed_hits": int(len(hits_current)),
        "non_current_hits": int(len(delisted_hits)),
        "maximum_ratio": float(all_results["高低倍数"].max()),
        "median_ratio": float(all_results["高低倍数"].median()),
        "diagnostics": diagnostics,
        "methodology": {
            "price_basis": "Daily high and low multiplied by Tushare adj_factor; the stock-specific scaling constant cancels in ratios.",
            "main_metric": "Maximum running adjusted high divided by a same-day-or-later adjusted low.",
            "ordering_rule": "The selected high date must be on or before the selected low date.",
            "interpretation": "A ratio of 5.0 equals an 80% peak-to-trough decline.",
        },
    }
    with (config.output_dir / "summary.json").open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, ensure_ascii=False, indent=2)

    LOGGER.info(
        "Output complete: universe=%s current=%s hits_all=%s hits_current=%s non_current_hits=%s",
        len(all_results),
        int(all_results["当前上市"].sum()),
        len(hits_all),
        len(hits_current),
        len(delisted_hits),
    )


def main() -> int:
    args = parse_args()
    config = ScanConfig(
        start_date=args.start_date,
        end_date=args.end_date,
        threshold_ratio=args.threshold_ratio,
        output_dir=Path(args.output_dir),
        checkpoint_dir=Path(args.checkpoint_dir),
        request_pause=args.request_pause,
    )
    configure_logging(config.output_dir)

    token = os.getenv("TUSHARE_TOKEN", "").strip()
    if not token:
        raise RuntimeError("TUSHARE_TOKEN is not configured")
    http_url = os.getenv("TUSHARE_HTTP_URL", DEFAULT_HTTP_URL).strip() or DEFAULT_HTTP_URL
    gateway = TushareGateway(token=token, http_url=http_url, request_pause=config.request_pause)

    LOGGER.info(
        "Starting scan: start=%s end=%s threshold=%.2fx endpoint=%s",
        config.start_date,
        config.end_date,
        config.threshold_ratio,
        http_url,
    )

    metadata = load_stock_metadata(gateway)
    trade_dates = load_trade_dates(gateway, config.start_date, config.end_date)
    LOGGER.info("Metadata rows=%s; open trading dates=%s", len(metadata), len(trade_dates))

    checkpoint_path = config.checkpoint_dir / "checkpoint.pkl"
    checkpoint = load_checkpoint(checkpoint_path, config.config_id)
    if checkpoint:
        states = checkpoint.get("states", {})
        completed_dates = set(checkpoint.get("completed_dates", []))
        diagnostics = checkpoint.get(
            "diagnostics",
            {"daily_rows": 0, "invalid_price_rows": 0, "missing_factor_rows": 0, "completed_dates": 0},
        )
        LOGGER.info(
            "Resuming checkpoint: completed_dates=%s stock_states=%s",
            len(completed_dates),
            len(states),
        )
    else:
        states: Dict[str, Dict[str, Any]] = {}
        completed_dates: set[str] = set()
        diagnostics = {"daily_rows": 0, "invalid_price_rows": 0, "missing_factor_rows": 0, "completed_dates": 0}

    remaining_dates = [date for date in trade_dates if date not in completed_dates]
    for position, trade_date in enumerate(remaining_dates, 1):
        daily = fetch_one_day(gateway, trade_date)
        counters = update_states(states, daily, trade_date)
        completed_dates.add(trade_date)
        diagnostics["daily_rows"] += counters["rows"]
        diagnostics["invalid_price_rows"] += counters["invalid_price_rows"]
        diagnostics["missing_factor_rows"] += counters["missing_factor_rows"]
        diagnostics["completed_dates"] = len(completed_dates)

        if position % 20 == 0 or position == len(remaining_dates):
            current_hits = sum(
                1 for state in states.values() if float(state.get("max_drawdown_ratio", 1.0)) >= config.threshold_ratio
            )
            LOGGER.info(
                "Progress %s/%s new dates; total completed=%s/%s; states=%s; provisional hits=%s; date=%s",
                position,
                len(remaining_dates),
                len(completed_dates),
                len(trade_dates),
                len(states),
                current_hits,
                trade_date,
            )

        if position % 25 == 0 or position == len(remaining_dates):
            save_checkpoint(
                checkpoint_path,
                {
                    "config_id": config.config_id,
                    "states": states,
                    "completed_dates": sorted(completed_dates),
                    "diagnostics": diagnostics,
                },
            )

    all_results = build_result_frame(states, metadata, config)
    industry_summary = build_industry_summary(all_results, config.threshold_ratio)
    diagnostics["metadata_rows"] = int(len(metadata))
    diagnostics["trade_dates"] = int(len(trade_dates))
    diagnostics["stock_states"] = int(len(states))
    write_outputs(all_results, industry_summary, config, diagnostics)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
