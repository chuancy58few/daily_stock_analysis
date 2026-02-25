# -*- coding: utf-8 -*-
import sys
import textwrap
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from unittest.mock import MagicMock

# Mock newspaper before search_service import (optional dependency)
if "newspaper" not in sys.modules:
    mock_np = MagicMock()
    mock_np.Article = MagicMock()
    mock_np.Config = MagicMock()
    sys.modules["newspaper"] = mock_np

import requests

from src.search_service import SearchService


def _make_rss(now: datetime, old: datetime) -> str:
    return textwrap.dedent(
        f"""
        <rss version="2.0">
          <channel>
            <item>
              <title>News A</title>
              <link>https://example.com/a</link>
              <source>Example</source>
              <pubDate>{format_datetime(now)}</pubDate>
              <description>desc a</description>
            </item>
            <item>
              <title>News A (duplicate)</title>
              <link>https://example.com/a</link>
              <source>Example</source>
              <pubDate>{format_datetime(now)}</pubDate>
              <description>dup</description>
            </item>
            <item>
              <title>No Link</title>
              <link></link>
              <source>Example</source>
              <pubDate>{format_datetime(now)}</pubDate>
              <description>n1</description>
            </item>
            <item>
              <title>No Link</title>
              <link></link>
              <source>Example</source>
              <pubDate>{format_datetime(now)}</pubDate>
              <description>n2</description>
            </item>
            <item>
              <title>Old News</title>
              <link>https://example.com/old</link>
              <source>Example</source>
              <pubDate>{format_datetime(old)}</pubDate>
              <description>old</description>
            </item>
          </channel>
        </rss>
        """
    ).strip()


def test_rss_parse_filter_dedupe(monkeypatch):
    now = datetime.now(timezone.utc)
    old = now - timedelta(days=10)
    rss = _make_rss(now, old)

    class DummyResp:
        status_code = 200
        text = rss

    def fake_get(*args, **kwargs):
        return DummyResp()

    monkeypatch.setattr(requests, "get", fake_get)

    svc = SearchService(news_max_age_days=3, rss_enabled=True)
    resp = svc.search_stock_news("600519", "贵州茅台")
    assert resp.success is True
    assert len(resp.results) == 2
    assert resp.results[0].title == "News A"


def test_rss_preferred_over_providers(monkeypatch):
    class DummyResp:
        status_code = 200
        text = """<rss version='2.0'><channel>
        <item><title>T1</title><link>https://example.com/t1</link>
        <source>Example</source><pubDate>Mon, 24 Feb 2026 10:00:00 GMT</pubDate>
        <description>desc</description></item>
        </channel></rss>"""

    monkeypatch.setattr(requests, "get", lambda *a, **k: DummyResp())

    svc = SearchService(news_max_age_days=3, rss_enabled=True)
    svc._providers = []
    resp = svc.search_stock_news("600519", "贵州茅台")
    assert resp.provider == "RSS"
    assert resp.results
