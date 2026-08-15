#!/usr/bin/env python3
"""Probe Sina qfq factor endpoint for SH/SZ/BJ symbols."""

from __future__ import annotations

import ast
import json
from pathlib import Path
from typing import Any, Dict

import requests

OUT = Path("reports/sina_qfq_factor_probe")
OUT.mkdir(parents=True, exist_ok=True)
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/151 Safari/537.36"
SYMBOLS = ["sh600000", "sz000002", "sh600519", "sz300750", "sz002594", "bj920000", "bj920002", "bj920931"]


def fetch(symbol: str) -> Dict[str, Any]:
    url = f"https://finance.sina.com.cn/realstock/company/{symbol}/qfq.js"
    response = requests.get(url, headers={"User-Agent": UA, "Referer": f"https://finance.sina.com.cn/realstock/company/{symbol}/nc.shtml"}, timeout=20)
    response.raise_for_status()
    text = response.text
    parsed = None
    error = ""
    try:
        payload_text = text.split("=", 1)[1].split("\n", 1)[0].strip().rstrip(";")
        parsed = ast.literal_eval(payload_text)
    except Exception as exc:  # noqa: BLE001
        error = f"{type(exc).__name__}: {exc}"
    data = parsed.get("data", []) if isinstance(parsed, dict) else []
    return {
        "url": url,
        "status": response.status_code,
        "bytes": len(response.content),
        "prefix": text[:350],
        "suffix": text[-350:],
        "parse_error": error,
        "rows": len(data),
        "first": data[:3],
        "last": data[-3:],
    }


def main() -> int:
    results: Dict[str, Any] = {}
    for symbol in SYMBOLS:
        try:
            results[symbol] = {"ok": True, **fetch(symbol)}
        except Exception as exc:  # noqa: BLE001
            results[symbol] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
        print(symbol, json.dumps(results[symbol], ensure_ascii=False, default=str), flush=True)
    (OUT / "probe.json").write_text(json.dumps(results, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
