# -*- coding: utf-8 -*-
"""Broker ratings loader (manual CSV)."""

from __future__ import annotations

import csv
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict

logger = logging.getLogger(__name__)

_ALLOWED_STANCES = {"看多", "中性", "看空", "N/A"}


@dataclass
class BrokerRatingsLoader:
    path: str
    _cache: Dict[str, Dict[str, str]] = field(default_factory=dict)
    _loaded: bool = False

    def _load_once(self) -> None:
        if self._loaded:
            return
        self._loaded = True
        self._cache = {}

        csv_path = Path(self.path)
        if not csv_path.exists():
            logger.warning(f"broker_ratings.csv not found: {self.path}")
            return

        try:
            with csv_path.open("r", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    code = (row.get("code") or "").strip()
                    if not code:
                        continue
                    self._cache[code] = {
                        "ms_stance": self._normalize_stance(row.get("ms_stance")),
                        "ubs_stance": self._normalize_stance(row.get("ubs_stance")),
                        "citi_stance": self._normalize_stance(row.get("citi_stance")),
                    }
        except Exception as exc:
            logger.warning(f"broker_ratings.csv load failed: {exc}")

    @staticmethod
    def _normalize_stance(value: str | None) -> str:
        stance = (value or "").strip() or "N/A"
        return stance if stance in _ALLOWED_STANCES else "N/A"

    def get_stances(self, code: str) -> Dict[str, str]:
        self._load_once()
        if not self._cache:
            return {"ms_stance": "N/A", "ubs_stance": "N/A", "citi_stance": "N/A"}
        return self._cache.get(
            code, {"ms_stance": "N/A", "ubs_stance": "N/A", "citi_stance": "N/A"}
        )


_loader: BrokerRatingsLoader | None = None


def get_broker_ratings_loader(path: str) -> BrokerRatingsLoader:
    global _loader
    if _loader is None or _loader.path != path:
        _loader = BrokerRatingsLoader(path=path)
    return _loader
