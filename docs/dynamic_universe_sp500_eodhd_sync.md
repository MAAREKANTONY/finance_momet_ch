# Dynamic Universe S&P500 - EODHD sync

Dynamic Universe V1 keeps the CSV importer as an admin/dev fallback, but the recommended production ingestion path is EODHD Fundamentals.

The sync uses:

```text
/fundamentals/GSPC.INDX?filter=HistoricalTickerComponents
```

It does not call EODHD during backtests. Backtests only read local `UniverseMembership` and `UniverseCoverageSnapshot` rows.

## Dry-run

```bash
python manage.py sync_sp500_historical_memberships \
  --coverage-start 2020-01-01 \
  --coverage-end 2026-06-15
```

Dry-run fetches EODHD, transforms the response, resolves local `Symbol` mappings, and prints the final status without writing memberships, batches, or coverage snapshots.

## Apply

```bash
python manage.py sync_sp500_historical_memberships \
  --coverage-start 2020-01-01 \
  --coverage-end 2026-06-15 \
  --apply
```

Apply writes or updates:

- `UniverseDefinition` for `SP500`
- `UniverseMembership` intervals
- `UniverseImportBatch`
- `UniverseCoverageSnapshot`

The command is idempotent for the same provider rows and period.

## Warnings

Warnings must be reviewed before trusting a period:

- missing `StartDate`: the row is treated as active from `coverage_start` only when it intersects the requested period
- future `EndDate`: the date is capped to `coverage_end`
- unmapped symbol: no local `Symbol` exists for the provider ticker
- ambiguous symbol: multiple local `Symbol` rows match the provider ticker

Only fully mapped coverage can become `VALIDATED`. Partial coverage remains blocked by the Dynamic Universe resolver.
