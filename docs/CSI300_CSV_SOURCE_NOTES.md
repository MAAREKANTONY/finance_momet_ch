# CSI300 CSV Source Notes

## Sources

Primary source: `unliftedq/index-constitution` on GitHub. The CSI300 files currently used by the converter are:

- `history/csi300.csv`: historical membership spans with `symbol,name,opt-in,opt-out`.
- `latest/csi300.csv`: current active constituents with `symbol,name,opt-in`.
- `event/cn.csv`: audit trail for China ticker/name/delisting events.

Secondary control source: `yfiua/index-constituents` / `https://yfiua.github.io/index-constituents/`. CSI300 history there starts at 2023/07, so it is useful for recent snapshot overlap checks only, not as the long historical source.

## Limits

These are open-source, non-institutional datasets. They should be reviewed before serious backtests. In particular:

- verify the generated quality report before importing;
- inspect ticker/name changes and delistings when a backtest period is sensitive;
- do not use a current-only fallback for past dates;
- yfiua coverage starts too late to replace the long unliftedq history.

## StockAlert Mapping

The converter maps China tickers to StockAlert membership rows as follows:

- `SH600519`, `600519.SS`, `600519.SH`, `600519.SHG` -> `symbol=600519`, `exchange=SHG`, `mic=XSHG`, `provider_symbol=600519.SHG`.
- `SZ000001`, `000001.SZ`, `000001.SHE` -> `symbol=000001`, `exchange=SHE`, `mic=XSHE`, `provider_symbol=000001.SHE`.
- If no exchange is present, `6xxxxx` is inferred as Shanghai and `0xxxxx`/`3xxxxx` as Shenzhen. In `--strict` mode those inferred rows fail so the source can be fixed explicitly.

The generated StockAlert CSV columns are:

```csv
universe_code,symbol,exchange,mic,name,start_date,end_date,weight,provider_symbol,source,country,currency,sector,industry
```

## Recommended Process

1. Generate the CSV and report, preferably into `/tmp` or `data/generated/` which is ignored by Git.
2. Read the report: row counts, latest active members, exchange distribution, overlaps, duplicates, and yfiua overlap when provided.
3. Import via Trigger UI staff-only in dry-run mode, or CLI dry-run:

```bash
python manage.py import_universe_memberships --csv /tmp/csi300_stockalert_memberships.csv --universe-code CSI300 --universe-name "CSI 300" --dry-run
```

4. Apply explicitly only after dry-run review:

```bash
python manage.py import_universe_memberships --csv /tmp/csi300_stockalert_memberships.csv --universe-code CSI300 --universe-name "CSI 300" --apply
```

5. Run CSI300 readiness checks before any backtest.
