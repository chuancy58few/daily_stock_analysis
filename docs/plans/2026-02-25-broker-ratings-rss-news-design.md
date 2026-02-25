# Broker Ratings + RSS News Design

Date: 2026-02-25

## Goals
- Append MS/UBS/Citi stances to analysis summary lines via a manually maintained CSV.
- Add a free RSS provider (Google News RSS) and prefer it for latest news searches.
- Investigate dividend_yield N/A and fix mapping if the current realtime provider supplies it.

## Non-Goals
- No new paid search APIs.
- No change to existing data providers beyond required field mapping.

## Architecture
- Add a broker ratings loader that reads `broker_ratings.csv` once and caches by stock code.
- Inject `ms_stance/ubs_stance/citi_stance` into `AnalysisResult.market_snapshot` when enabled.
- Add RSS fetch/parse in `SearchService`, returning `SearchResponse/SearchResult` for reuse.
- Prefer RSS for `search_stock_news` and `latest_news` in `search_comprehensive_intel`, fallback to existing providers.

## Data Flow
- Config loads new flags and paths.
- Broker ratings loaded on first use, lookup by `code`, missing defaults to `N/A`.
- Summary line appends `| MS:{ms} | UBS:{ubs} | Citi:{citi}` after price/PE/dividend.
- RSS query rule:
  - A/HK: `{stock_name} {stock_code} 股票 最新消息`
  - US: `{stock_name} {stock_code} stock latest news`
- RSS parsing: map to `SearchResult` fields, then filter by `NEWS_MAX_AGE_DAYS` and dedupe.

## Error Handling
- CSV missing/invalid: warn once, keep `N/A` stances.
- RSS fetch/parse failure: return `success=False`, fallback to existing providers if configured.
- No providers available: empty results with provider `None`.

## Testing
- Update summary tests to cover three stances and `N/A` defaults.
- Add loader tests for CSV parsing and cache behavior.
- Add RSS tests for parse, freshness filter, dedupe logic.

## Rollout
- Default `BROKER_RATINGS_ENABLED=false`.
- Default `RSS_ENABLED=true` with Google News RSS template.

## Open Questions
- None.
