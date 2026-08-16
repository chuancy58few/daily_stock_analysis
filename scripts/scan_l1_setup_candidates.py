#!/usr/bin/env python3
"""Screen all current A-shares and all current southbound Stock Connect stocks for L1-stage setups.

Target lifecycle:
    H0: old cycle high
    L0: first long-decline low
    H1: first sharp rebound / speculative rally
    L1: second deep pullback low
    NOW: still basing near L1, or only beginning the second rally

The script intentionally excludes already-completed second rallies from the core list.
It is a technical-stage screen, not an investment recommendation.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import os
import statistics
import sys
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import scan_ah_double_spike_pattern as base

LOGGER = logging.getLogger("l1_setup_scanner")
CHECKPOINT_LOCK = threading.Lock()


@dataclass(frozen=True)
class Config:
    output_dir: Path
    checkpoint_dir: Path
    workers: int
    requests_per_second: float
    history_count: int
    min_history_months: int
    limit_a: int
    limit_hk: int
    top_series_count: int

    @property
    def key(self) -> str:
        return (
            "l1-setup-v3|"
            f"history={self.history_count}|min={self.min_history_months}|"
            "A-current|HK-connect-union|monthly-close"
        )


CORE_FIELDS = [
    "总排名", "市场排名", "市场", "港股通来源", "板块", "证券代码", "股票名称", "行情代码", "是否ST",
    "当前阶段", "候选等级", "核心候选", "技术候选", "综合分", "结构分", "阶段分", "启动分", "风险扣分",
    "H0日期", "H0价格", "L0日期", "L0价格", "H1日期", "H1价格", "L1日期", "L1价格", "当前日期", "当前月线收盘价",
    "初始下跌倍数", "第一次拉升倍数", "中段回落倍数", "L1相对L0", "当前价相对L1", "当前价相对H1",
    "初始下跌月数", "第一次拉升月数", "中段回落月数", "L1距今月数", "完整形态月数",
    "近3月涨跌幅", "近6月涨跌幅", "当前相对前12月高点", "当前相对6月均线", "L1后区间振幅倍数",
    "第一次拉升量比", "近期量比", "价格跳变风险", "最大单月价格跳变倍数", "单月2倍跳变次数",
    "流动性等级", "最新价", "最新成交额", "最新成交量", "市盈率", "市净率", "总市值_原始", "行情时间",
    "历史起始月", "历史结束月", "有效月数", "历史断点数", "价格口径", "使用备用数据源",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default="reports/a_hkconnect_l1_setup")
    parser.add_argument("--checkpoint-dir", default=".cache/a_hkconnect_l1_setup")
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--requests-per-second", type=float, default=14.0)
    parser.add_argument("--history-count", type=int, default=260)
    parser.add_argument("--min-history-months", type=int, default=60)
    parser.add_argument("--limit-a", type=int, default=0)
    parser.add_argument("--limit-hk", type=int, default=0)
    parser.add_argument("--top-series-count", type=int, default=60)
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


def fetch_hk_connect_node(node: str) -> List[Dict[str, Any]]:
    """Fetch one current southbound list from Sina (hgt_hk or sgt_hk)."""
    records: List[Dict[str, Any]] = []
    page = 1
    page_size = 80
    while True:
        rows = base.request_json(
            base.SINA_HK_LIST,
            params={
                "page": page,
                "num": page_size,
                "sort": "symbol",
                "asc": 1,
                "node": node,
                "_s_r_a": "page",
            },
            referer="https://finance.sina.com.cn/stock/hkstock/",
            attempts=6,
        )
        if not isinstance(rows, list):
            raise RuntimeError(f"Sina node {node} page {page} is not a list")
        if not rows:
            break
        for item in rows:
            code = str(item.get("symbol", "")).strip().zfill(5)
            if len(code) != 5 or not code.isdigit():
                continue
            trade_type = str(item.get("tradetype", "")).strip().upper()
            name = str(item.get("name", "")).strip()
            # The two nodes can contain ETFs. The user requested stocks only.
            if trade_type not in ("", "STOCK", "EQUITY"):
                continue
            if "ETF" in name.upper() or "基金" in name:
                continue
            records.append(
                {
                    "市场": "港股通",
                    "港股通来源": "沪港通" if node == "hgt_hk" else "深港通",
                    "板块": "港股通股票",
                    "行情代码": f"hk{code}",
                    "证券代码": code,
                    "股票名称": name,
                    "是否ST": False,
                    "最新价": base.parse_number(item.get("lasttrade")),
                    "最新成交额": base.parse_number(item.get("amount")),
                    "最新成交量": base.parse_number(item.get("volume")),
                    "市盈率": base.parse_number(item.get("pe_ratio")),
                    "市净率": None,
                    "总市值_原始": base.parse_number(item.get("market_value")),
                    "行情时间": str(item.get("ticktime", "")),
                }
            )
        page += 1
        if page % 10 == 0:
            LOGGER.info("HK Connect node=%s pages=%s collected=%s", node, page - 1, len(records))
    LOGGER.info("HK Connect node=%s complete: %s rows", node, len(records))
    return records


def fetch_hk_connect_universe() -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    hgt = fetch_hk_connect_node("hgt_hk")
    sgt = fetch_hk_connect_node("sgt_hk")
    merged: Dict[str, Dict[str, Any]] = {}
    source_sets: Dict[str, set[str]] = {}
    for record in hgt + sgt:
        symbol = record["行情代码"]
        source_sets.setdefault(symbol, set()).add(record["港股通来源"])
        existing = merged.get(symbol)
        if existing is None:
            merged[symbol] = dict(record)
        else:
            # Prefer a non-zero, more recent market snapshot.
            if (record.get("最新成交额") or 0) > (existing.get("最新成交额") or 0):
                merged[symbol].update(record)
    for symbol, record in merged.items():
        record["港股通来源"] = "+".join(sorted(source_sets[symbol]))
    output = sorted(merged.values(), key=lambda item: item["行情代码"])
    counts = {
        "沪港通原始行数": len(hgt),
        "深港通原始行数": len(sgt),
        "港股通去重股票数": len(output),
        "两地均可买": sum(1 for sources in source_sets.values() if len(sources) == 2),
        "仅沪港通": sum(1 for sources in source_sets.values() if sources == {"沪港通"}),
        "仅深港通": sum(1 for sources in source_sets.values() if sources == {"深港通"}),
    }
    return output, counts


def normalize_a_record(record: Dict[str, Any]) -> Dict[str, Any]:
    output = dict(record)
    output["港股通来源"] = ""
    return output


def median(values: Iterable[float]) -> Optional[float]:
    cleaned = [float(value) for value in values if value is not None and math.isfinite(float(value)) and float(value) >= 0]
    return statistics.median(cleaned) if cleaned else None


def average(values: Iterable[float]) -> Optional[float]:
    cleaned = [float(value) for value in values if value is not None and math.isfinite(float(value))]
    return sum(cleaned) / len(cleaned) if cleaned else None


def ratio(numerator: Optional[float], denominator: Optional[float]) -> Optional[float]:
    if numerator is None or denominator is None or denominator <= 0:
        return None
    return numerator / denominator


def pct_return(prices: Sequence[float], months: int) -> Optional[float]:
    if len(prices) <= months or prices[-months - 1] <= 0:
        return None
    return prices[-1] / prices[-months - 1] - 1.0


def log_component(value: float, threshold: float, saturation: float) -> float:
    if value <= 1.0:
        return 0.0
    low = math.log(max(threshold, 1.000001))
    high = math.log(max(saturation, threshold + 0.000001))
    return max(0.0, min(1.0, (math.log(value) - low) / max(1e-9, high - low)))


def l1_relationship_score(value: float) -> float:
    # Prefer a double-bottom / modestly higher-low relationship.
    if 0.70 <= value <= 1.45:
        return 1.0
    if 0.55 <= value < 0.70:
        return (value - 0.45) / 0.25
    if 1.45 < value <= 1.85:
        return (2.05 - value) / 0.60
    return 0.0


def enumerate_l1_sequences(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    closes = [float(row["close"]) for row in rows]
    volumes = [float(row.get("volume") or 0.0) for row in rows]
    n = len(closes)
    if n < 60:
        return []
    highs, lows = base.local_extrema(closes, window=2)

    recent_start = max(0, n - 37)
    l1_candidates = [index for index in lows if index >= recent_start]
    l1_candidates.append(min(range(recent_start, n), key=lambda index: closes[index]))
    l1_candidates = sorted(set(l1_candidates), key=lambda index: (closes[index], -index))[:12]

    sequences: List[Dict[str, Any]] = []
    for l1 in l1_candidates:
        months_since_l1 = n - 1 - l1
        if months_since_l1 > 30:
            continue
        current = closes[-1]
        current_l1 = current / closes[l1]
        if current_l1 > 3.25:
            continue

        h1_pool = [index for index in highs if max(0, l1 - 54) <= index <= l1 - 4]
        h1_pool = sorted(h1_pool, key=lambda index: closes[index], reverse=True)[:16]
        for h1 in h1_pool:
            pullback = closes[h1] / closes[l1]
            if pullback < 1.60:
                continue

            l0_pool = [index for index in lows if max(0, h1 - 20) <= index <= h1 - 1]
            l0_pool = sorted(l0_pool, key=lambda index: closes[index])[:12]
            for l0 in l0_pool:
                first_rally = closes[h1] / closes[l0]
                if first_rally < 1.80:
                    continue
                prior_end = l0 - 24
                if prior_end < 0:
                    continue
                h0 = max(range(0, prior_end + 1), key=lambda index: closes[index])
                initial_decline = closes[h0] / closes[l0]
                if initial_decline < 2.00:
                    continue
                initial_months = l0 - h0
                if initial_months < 24:
                    continue
                l1_l0 = closes[l1] / closes[l0]
                if not (0.45 <= l1_l0 <= 2.10):
                    continue
                current_h1 = current / closes[h1]
                if current_h1 > 1.08:
                    # The old rebound high has already been fully recovered; likely H2, not L1.
                    continue

                recent3 = pct_return(closes, 3)
                recent6 = pct_return(closes, 6)
                ma6 = average(closes[-6:])
                prior12 = closes[max(0, n - 13): n - 1]
                prior12_high = max(prior12) if prior12 else current
                current_prior12 = current / prior12_high if prior12_high > 0 else None
                current_ma6 = current / ma6 if ma6 and ma6 > 0 else None
                post_l1 = closes[l1:]
                post_range = max(post_l1) / min(post_l1) if post_l1 and min(post_l1) > 0 else None

                baseline_volume = median(volumes[max(0, l0 - 12):l0])
                rally_volume = median(volumes[l0 + 1:h1 + 1])
                first_rally_volume_ratio = ratio(rally_volume, baseline_volume)
                recent_volume = average(volumes[-3:])
                prior_volume = median(volumes[max(0, n - 15): n - 3])
                recent_volume_ratio = ratio(recent_volume, prior_volume)

                if months_since_l1 <= 3 and current_l1 <= 1.40:
                    stage = "L1刚形成"
                elif (
                    3 <= months_since_l1 <= 30
                    and current_l1 <= 1.65
                    and current_h1 <= 0.78
                    and (post_range is None or post_range <= 2.05)
                ):
                    stage = "L1筑底"
                elif (
                    1.25 <= current_l1 <= 2.55
                    and current_h1 <= 0.95
                    and ((recent3 or 0.0) >= 0.08 or (current_prior12 or 0.0) >= 0.98)
                    and (current_ma6 or 0.0) >= 0.98
                ):
                    stage = "刚启动"
                elif (
                    1.55 <= current_l1 <= 3.00
                    and current_h1 <= 1.02
                    and (current_prior12 or 0.0) >= 1.03
                    and (recent3 or 0.0) >= 0.10
                ):
                    stage = "突破确认"
                else:
                    continue

                structure = 0.0
                structure += 16.0 * log_component(initial_decline, 2.0, 6.0)
                structure += 18.0 * log_component(first_rally, 1.8, 5.0)
                structure += 16.0 * log_component(pullback, 1.6, 4.5)
                structure += 8.0 * min(1.0, initial_months / 60.0)
                structure += 6.0 * min(1.0, (l1 - h1) / 24.0)
                structure += 8.0 * l1_relationship_score(l1_l0)
                structure = min(72.0, structure)

                stage_score = 0.0
                if stage == "L1刚形成":
                    stage_score = 18.0
                elif stage == "L1筑底":
                    stage_score = 22.0
                elif stage == "刚启动":
                    stage_score = 25.0
                elif stage == "突破确认":
                    stage_score = 23.0
                stage_score += 4.0 * max(0.0, 1.0 - months_since_l1 / 30.0)
                stage_score += 4.0 * max(0.0, min(1.0, (1.0 - current_h1) / 0.55))
                stage_score = min(30.0, stage_score)

                startup = 0.0
                startup += 5.0 * max(0.0, min(1.0, ((recent3 or 0.0) + 0.05) / 0.30))
                startup += 4.0 * max(0.0, min(1.0, ((recent6 or 0.0) + 0.10) / 0.55))
                startup += 4.0 * max(0.0, min(1.0, ((current_prior12 or 0.0) - 0.75) / 0.35))
                startup += 3.0 * max(0.0, min(1.0, ((current_ma6 or 0.0) - 0.90) / 0.25))
                startup += 2.0 * max(0.0, min(1.0, ((first_rally_volume_ratio or 1.0) - 1.0) / 2.0))
                startup += 2.0 * max(0.0, min(1.0, ((recent_volume_ratio or 1.0) - 1.0) / 1.5))
                startup = min(20.0, startup)

                sequences.append(
                    {
                        "H0_index": h0,
                        "L0_index": l0,
                        "H1_index": h1,
                        "L1_index": l1,
                        "H0日期": rows[h0]["date"],
                        "H0价格": closes[h0],
                        "L0日期": rows[l0]["date"],
                        "L0价格": closes[l0],
                        "H1日期": rows[h1]["date"],
                        "H1价格": closes[h1],
                        "L1日期": rows[l1]["date"],
                        "L1价格": closes[l1],
                        "当前日期": rows[-1]["date"],
                        "当前月线收盘价": current,
                        "初始下跌倍数": initial_decline,
                        "第一次拉升倍数": first_rally,
                        "中段回落倍数": pullback,
                        "L1相对L0": l1_l0,
                        "当前价相对L1": current_l1,
                        "当前价相对H1": current_h1,
                        "初始下跌月数": initial_months,
                        "第一次拉升月数": h1 - l0,
                        "中段回落月数": l1 - h1,
                        "L1距今月数": months_since_l1,
                        "完整形态月数": l1 - h0,
                        "近3月涨跌幅": recent3,
                        "近6月涨跌幅": recent6,
                        "当前相对前12月高点": current_prior12,
                        "当前相对6月均线": current_ma6,
                        "L1后区间振幅倍数": post_range,
                        "第一次拉升量比": first_rally_volume_ratio,
                        "近期量比": recent_volume_ratio,
                        "当前阶段": stage,
                        "结构分": structure,
                        "阶段分": stage_score,
                        "启动分": startup,
                    }
                )
    return sequences


def liquidity_level(record: Dict[str, Any]) -> str:
    amount = record.get("最新成交额")
    if amount is None:
        return "未知"
    amount = float(amount)
    if record.get("市场") == "A股":
        if amount >= 100_000_000:
            return "高"
        if amount >= 30_000_000:
            return "中"
        if amount >= 10_000_000:
            return "低"
        return "极低"
    if amount >= 50_000_000:
        return "高"
    if amount >= 5_000_000:
        return "中"
    if amount >= 1_000_000:
        return "低"
    return "极低"


def analyze_record(record: Dict[str, Any], config: Config) -> Dict[str, Any]:
    symbol = record["行情代码"]
    fallback_used = False
    try:
        rows, price_basis, breaks = base.fetch_tencent_monthly(symbol, config.history_count)
    except Exception:
        if record.get("市场") != "A股":
            raise
        rows, price_basis, breaks = base.fetch_sina_monthly_fallback(symbol)
        fallback_used = True

    max_gap, gap_count, jump_risk = base.price_jump_risk(rows)
    result = dict(record)
    result.update(
        {
            "历史起始月": rows[0]["date"],
            "历史结束月": rows[-1]["date"],
            "有效月数": len(rows),
            "历史断点数": breaks,
            "价格口径": price_basis,
            "使用备用数据源": fallback_used,
            "最大单月价格跳变倍数": max_gap,
            "单月2倍跳变次数": gap_count,
            "价格跳变风险": jump_risk,
            "流动性等级": liquidity_level(record),
            "技术候选": False,
            "核心候选": False,
            "筛选状态": "历史不足" if len(rows) < config.min_history_months else "未形成L1阶段",
        }
    )
    if len(rows) < config.min_history_months:
        return result

    sequences = enumerate_l1_sequences(rows)
    if not sequences:
        return result

    best: Optional[Dict[str, Any]] = None
    best_score = -math.inf
    for candidate in sequences:
        penalty = 0.0
        if jump_risk == "中":
            penalty += 5.0
        elif jump_risk == "高":
            penalty += 16.0
        if record.get("是否ST"):
            penalty += 22.0
        liquidity = result["流动性等级"]
        if liquidity == "低":
            penalty += 5.0
        elif liquidity == "极低":
            penalty += 12.0
        if candidate["当前价相对L1"] > 2.70:
            penalty += 6.0
        if candidate["L1相对L0"] < 0.60:
            penalty += 6.0
        total = candidate["结构分"] + candidate["阶段分"] + candidate["启动分"] - penalty
        if total > best_score:
            best_score = total
            best = {**candidate, "风险扣分": penalty, "综合分": total}
    assert best is not None

    strict_structure = (
        best["初始下跌倍数"] >= 2.8
        and best["第一次拉升倍数"] >= 2.3
        and best["中段回落倍数"] >= 2.0
        and best["初始下跌月数"] >= 30
        and 0.60 <= best["L1相对L0"] <= 1.75
        and best["当前价相对L1"] <= 2.65
        and best["当前价相对H1"] <= 0.95
    )
    technical = best["综合分"] >= 55.0
    core = (
        technical
        and strict_structure
        and not bool(record.get("是否ST"))
        and jump_risk != "高"
        and result["流动性等级"] in ("高", "中")
        and best["综合分"] >= 63.0
    )
    if core and best["综合分"] >= 78:
        grade = "A"
    elif core:
        grade = "B+"
    elif technical and best["综合分"] >= 68:
        grade = "B"
    elif technical:
        grade = "C+"
    else:
        grade = "观察"

    result.update(best)
    result.update(
        {
            "技术候选": technical,
            "核心候选": core,
            "候选等级": grade,
            "筛选状态": "核心候选" if core else ("技术候选" if technical else "低分形态"),
        }
    )
    return result


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
        LOGGER.info("Checkpoint loaded: %s completed", len(output))
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


def write_csv(path: Path, rows: Sequence[Dict[str, Any]], fields: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: csv_value(row.get(field)) for field in fields})


def rank_results(results: List[Dict[str, Any]]) -> None:
    candidates = [item for item in results if item.get("综合分") is not None]
    candidates.sort(key=lambda item: float(item.get("综合分") or -999), reverse=True)
    for rank, item in enumerate(candidates, 1):
        item["总排名"] = rank
    by_market: Dict[str, List[Dict[str, Any]]] = {}
    for item in candidates:
        by_market.setdefault(str(item.get("市场")), []).append(item)
    for market_rows in by_market.values():
        for rank, item in enumerate(market_rows, 1):
            item["市场排名"] = rank


def fetch_rows_for_series(record: Dict[str, Any], config: Config) -> List[Dict[str, Any]]:
    try:
        rows, _, _ = base.fetch_tencent_monthly(record["行情代码"], config.history_count)
        return rows
    except Exception:
        if record.get("市场") != "A股":
            return []
        try:
            rows, _, _ = base.fetch_sina_monthly_fallback(record["行情代码"])
            return rows
        except Exception:
            return []


def write_top_series(output_path: Path, rows: Sequence[Dict[str, Any]], config: Config) -> None:
    fields = ["总排名", "市场", "证券代码", "股票名称", "当前阶段", "行情代码", "日期", "月收盘价", "L1标准化价格_L1等于100"]
    output: List[Dict[str, Any]] = []
    for record in rows:
        history = fetch_rows_for_series(record, config)
        l1 = float(record.get("L1价格") or 0.0)
        if not history or l1 <= 0:
            continue
        for row in history:
            output.append(
                {
                    "总排名": record.get("总排名"),
                    "市场": record.get("市场"),
                    "证券代码": record.get("证券代码"),
                    "股票名称": record.get("股票名称"),
                    "当前阶段": record.get("当前阶段"),
                    "行情代码": record.get("行情代码"),
                    "日期": row["date"],
                    "月收盘价": row["close"],
                    "L1标准化价格_L1等于100": float(row["close"]) / l1 * 100.0,
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
        history_count=max(100, args.history_count),
        min_history_months=max(48, args.min_history_months),
        limit_a=max(0, args.limit_a),
        limit_hk=max(0, args.limit_hk),
        top_series_count=max(1, args.top_series_count),
    )
    configure_logging(config.output_dir)
    base.RATE_LIMITER = base.GlobalRateLimiter(config.requests_per_second)
    started = time.time()

    a_universe = [normalize_a_record(record) for record in base.fetch_a_universe()]
    hk_universe, hk_counts = fetch_hk_connect_universe()
    if config.limit_a:
        a_universe = a_universe[: config.limit_a]
    if config.limit_hk:
        hk_universe = hk_universe[: config.limit_hk]
    universe = a_universe + hk_universe

    initialize_checkpoint(config)
    completed = load_checkpoint(config)
    pending = [record for record in universe if record["行情代码"] not in completed]
    failures: Dict[str, str] = {}
    LOGGER.info(
        "Starting L1 scan: A=%s HKConnect=%s total=%s remaining=%s workers=%s rate=%.1f/s counts=%s",
        len(a_universe), len(hk_universe), len(universe), len(pending), config.workers, config.requests_per_second, hk_counts,
    )

    with ThreadPoolExecutor(max_workers=config.workers, thread_name_prefix="l1-scan") as executor:
        future_map: Dict[Future[Dict[str, Any]], Dict[str, Any]] = {
            executor.submit(analyze_record, record, config): record for record in pending
        }
        finished = 0
        for future in as_completed(future_map):
            record = future_map[future]
            finished += 1
            symbol = record["行情代码"]
            try:
                result = future.result()
                completed[symbol] = result
                append_checkpoint(config, result)
            except Exception as exc:  # noqa: BLE001
                failures[symbol] = f"{type(exc).__name__}: {exc}"
            if finished % 200 == 0 or finished == len(pending):
                core_count = sum(1 for item in completed.values() if item.get("核心候选"))
                technical_count = sum(1 for item in completed.values() if item.get("技术候选"))
                LOGGER.info(
                    "Progress %s/%s; success=%s failures=%s core=%s technical=%s",
                    finished, len(pending), len(completed), len(failures), core_count, technical_count,
                )

    results = list(completed.values())
    rank_results(results)
    technical = [item for item in results if item.get("技术候选")]
    technical.sort(key=lambda item: float(item.get("综合分") or -999), reverse=True)
    core = [item for item in technical if item.get("核心候选")]
    a_candidates = [item for item in technical if item.get("市场") == "A股"]
    hk_candidates = [item for item in technical if item.get("市场") == "港股通"]
    basing = [item for item in technical if item.get("当前阶段") in ("L1刚形成", "L1筑底")]
    starting = [item for item in technical if item.get("当前阶段") in ("刚启动", "突破确认")]

    write_csv(config.output_dir / "核心候选.csv", core, CORE_FIELDS)
    write_csv(config.output_dir / "全部技术候选.csv", technical, CORE_FIELDS)
    write_csv(config.output_dir / "A股候选.csv", a_candidates, CORE_FIELDS)
    write_csv(config.output_dir / "港股通候选.csv", hk_candidates, CORE_FIELDS)
    write_csv(config.output_dir / "L1筑底候选.csv", basing, CORE_FIELDS)
    write_csv(config.output_dir / "刚启动候选.csv", starting, CORE_FIELDS)
    write_csv(config.output_dir / "全市场扫描状态.csv", results, CORE_FIELDS)
    write_csv(
        config.output_dir / "抓取失败清单.csv",
        [{"行情代码": symbol, "错误": error} for symbol, error in sorted(failures.items())],
        ["行情代码", "错误"],
    )
    write_csv(
        config.output_dir / "港股通股票池.csv",
        hk_universe,
        ["市场", "港股通来源", "板块", "证券代码", "股票名称", "行情代码", "最新价", "最新成交额", "最新成交量", "市盈率", "总市值_原始", "行情时间"],
    )

    selected_series = core[: config.top_series_count]
    for item in technical:
        if item not in selected_series:
            selected_series.append(item)
        if len(selected_series) >= config.top_series_count:
            break
    write_top_series(config.output_dir / "顶部候选L1标准化月线.csv", selected_series, config)

    stage_counts: Dict[str, int] = {}
    for item in technical:
        stage = str(item.get("当前阶段") or "未知")
        stage_counts[stage] = stage_counts.get(stage, 0) + 1
    summary = {
        "生成时间": datetime.now().isoformat(timespec="seconds"),
        "筛选目标": "已经走到L1，正在筑底或刚启动，第二次主升尚未充分发生",
        "A股股票池": len(a_universe),
        "港股通股票池": len(hk_universe),
        "合计股票池": len(universe),
        "成功扫描": len(results),
        "抓取失败": len(failures),
        "技术候选": len(technical),
        "核心候选": len(core),
        "A股候选": len(a_candidates),
        "港股通候选": len(hk_candidates),
        "筑底候选": len(basing),
        "刚启动候选": len(starting),
        "阶段分布": stage_counts,
        "港股通股票池统计": hk_counts,
        "运行耗时秒": time.time() - started,
        "方法说明": {
            "频率": "月线收盘价",
            "历史长度": f"最多{config.history_count}个月",
            "形态": "H0长期高点→L0第一次低点→H1首次急拉高点→L1再次深跌低点→当前",
            "排除已经走完H2": "当前价格高于H1约8%以上，或相对L1上涨超过3.25倍时不纳入",
            "核心候选": "非ST、价格跳变风险非高、成交额达到可交易门槛、结构和阶段满足严格条件、综合分≥63",
            "港股通口径": "新浪hgt_hk与sgt_hk节点取并集，排除明确ETF/基金；名单动态变化，以上交所和深交所当日公告为最终依据",
        },
    }
    (config.output_dir / "筛选摘要.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    LOGGER.info("Completed: %s", json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
