# Historical Market-Cap Filter

## Purpose

The historical market-cap filter is an optional BUY-only eligibility gate for Backtests and Games.

- It can require a minimum market cap.
- It can require a maximum market cap.
- It never triggers SELL.
- It does not change GM behavior.
- It does not change latch behavior.

## Settings

Supported settings keys in `settings` JSON:

- `market_cap_min`
- `market_cap_max`
- `market_cap_missing_policy`

Behavior:

- empty `market_cap_min` means no lower bound
- empty `market_cap_max` means no upper bound
- if both are empty, the market-cap filter is inactive

## Historical lookup semantics

The filter uses the latest known local historical market capitalization at or before the BUY date.

- exact date match is used when available
- otherwise the latest previous local value is used
- future values are never used

This keeps backtest and game runs historically consistent and reproducible.

## Missing-data policy

Supported values:

- `BLOCK`
- `ALLOW`

Behavior when the market-cap filter is configured and no local historical value exists at or before the BUY date:

- `BLOCK`: BUY is blocked
- `ALLOW`: BUY remains allowed, subject to all other gates

If the market-cap filter is not configured, missing market-cap data does not matter.

## Runtime architecture

Historical market-cap data is read from the local `HistoricalMarketCap` cache.

- no runtime EODHD calls occur during simulations
- no runtime provider fallback is attempted
- engine paths preload market-cap series before simulation loops

This design avoids per-day provider calls and keeps results stable across reruns.

## Ingestion

Historical market-cap data is synced manually with the EODHD management command before running backtests or games that need it.

Environment:

- `EODHD_API_KEY`
- optional `EODHD_BASE_URL`

Example:

```bash
python manage.py sync_market_caps_eodhd \
  --symbols AAPL,MSFT \
  --from 2020-01-01 \
  --to 2026-05-14
```

Operational recommendation:

- sync required universes before large backtests
- resync periodically if backtest ranges extend
- monitor unsupported or unmapped symbols in command output

## Diagnostic chart

Full backtest results can show an optional per-ticker `Historical Market Cap` diagnostic panel.

- it uses local cached values only
- it shows the aligned market-cap series for the selected ticker
- it can show configured min/max thresholds
- it can show the missing-data policy
- KPI-only and Game outputs do not include this diagnostic payload
