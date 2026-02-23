# LLM Summary Fields + OpenAI Default Provider Design

## Goal
Add PE, dividend yield, and current price to the analysis result summary for the watchlist, and default the LLM provider to OpenAI when `OPENAI_API_KEY` is available.

## Scope
- Update provider selection logic to prefer OpenAI first when an OpenAI-compatible key is configured.
- Extend summary output lines to include PE, dividend yield, and current price.
- Surface the extra fields from existing realtime quote/context where available.

## Out of Scope
- Introducing new market data APIs for dividend yield.
- Changing the LLM response schema or prompt output format.
- UI/notification channel redesign beyond summary text changes.

## Recommended Approach (Option A)
Keep current data sources and render dividend yield as `N/A` when missing. Use existing realtime quote data for PE and current price when available.

### Rationale
- Minimal code changes and no new external dependencies.
- Works with current realtime data providers.
- Safe fallback behavior when fields are unavailable.

## Architecture
### Provider Selection
- Update `GeminiAnalyzer` initialization to attempt OpenAI first, then Anthropic, then Gemini.
- Preserve fallback behavior when the preferred provider fails.

### Summary Rendering
- Update summary sections in `src/notification.py` where per-stock summary lines are generated:
  - Daily report summary-only mode.
  - Decision dashboard summary section.
  - WeChat dashboard summary-only mode.
  - NotificationBuilder summary for quick alerts.

### Data Flow
- Realtime quote fields are already collected in `src/core/pipeline.py` and formatted in `src/analyzer.py`.
- Extend `market_snapshot` to include:
  - `price` (current price, already present)
  - `pe_ratio` (already in realtime quote)
  - `dividend_yield` (new field, default `None` when unavailable)
- Summary formatting reads from `result.market_snapshot` with safe fallbacks.

## Error Handling
- If realtime data is missing, display `N/A` for PE, dividend yield, and current price.
- Keep existing logging for provider initialization and LLM calls.

## Testing & Verification
- Run `python -m py_compile` against modified Python modules.
- Run existing repo tests if available.
- Manually sanity-check generated summary strings using existing notification builder paths.

## Files to Change (Expected)
- `src/analyzer.py` (provider priority and market snapshot fields)
- `src/core/pipeline.py` (ensure dividend yield field can pass through if present)
- `src/notification.py` (summary rendering lines)
- `src/config.py` (if default model selection needs adjustment in config metadata)

## Risks
- Some data sources may not supply PE or price; outputs will show `N/A`.
- Provider priority change could alter runtime behavior for users who rely on Gemini defaults.

## Rollback
- Revert the provider priority logic and summary format changes to restore previous behavior.
