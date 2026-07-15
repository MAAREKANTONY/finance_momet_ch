# CSI300 CSV Source Notes

## Fenêtre historique V1 supportée

La V1 des Backtests CSI300 supporte une date de début à partir du **3 janvier 2023 inclus**. Cette borne correspond au début de la couverture OHLC locale exploitable pour les actions CSI300 : le produit ne revendique donc aucune profondeur historique supérieure aux prix réellement validés.

Le convertisseur conserve une approche point-in-time sans biais de survivants sur cette période supportée :

- les memberships entièrement terminés avant le 3 janvier 2023 sont exclus explicitement et déclarés dans le rapport ;
- les intervalles commencés avant cette date mais encore actifs au cutoff sont clippés au 3 janvier 2023 ;
- un intervalle incomplet qui touche la période supportée reste une erreur bloquante ;
- aucune garantie de composition ou de couverture OHLC n’est fournie avant le 3 janvier 2023.

Cette limite ne pourra être étendue que lorsque les memberships historiques et les OHLC correspondants auront été validés ensemble.

## Sources

Primary source: [`unliftedq/index-constitution`](https://github.com/unliftedq/index-constitution) on GitHub, tag `v0.6.2`, pinned to commit `16d9d69fc0bf7f0f5e9aace868e16e26f2ecb5c2`. The converter downloads only raw URLs containing that full commit and verifies these SHA-256 checksums before parsing:

- `history/csi300.csv`: `6a6bca260f4752cbe555337369915794c752ecc0f70ee9b0d1bac6f83e7df1b8`
- `latest/csi300.csv`: `5f2e086ab3a0db35f807af34c38571d555aabc69612fa11c28d7c47498224aaf`
- `event/cn.csv`: `060c54ee81403369a8522fc573de9243212975bcf58ea1be3aa3ecff6f4cd174`

The checksum is calculated on decoded UTF-8 content after removal of an optional UTF-8 BOM; the report also records the raw downloaded hash. The CSI300 files used by the converter are:

- `history/csi300.csv`: historical membership spans with `symbol,name,opt-in,opt-out`.
- `latest/csi300.csv`: current active constituents with `symbol,name,opt-in`.
- `event/cn.csv`: audit trail for China ticker/name/delisting events.

Secondary control source: `yfiua/index-constituents` / `https://yfiua.github.io/index-constituents/`. CSI300 history there starts at 2023/07, so it is useful for recent snapshot overlap checks only, not as the long historical source.

### Date semantics and source anomalies

In the pinned source, `opt-in` is inclusive and `opt-out` is exclusive. StockAlert membership dates are both inclusive, so conversion applies:

```text
start_date = opt-in
end_date = opt-out - 1 day
```

For example, a member with `opt-out=2026-06-12` is active through June 11 and inactive on June 12; an entrant with `opt-in=2026-06-12` is active on June 12. This prevents outgoing and incoming batches from overlapping on rebalance day.

The pinned history contains four intervals without `opt-in`: `SH600312`, `SH600501`, `SH600549`, and `SH600786`. Neither `latest/csi300.csv` nor `event/cn.csv` supplies a deterministic entry date for those historical intervals. The converter reports four blocking `missing_opt_in` errors and publishes no CSV; it never invents or silently drops their dates. A later active interval for the same symbol, such as `SH600549` from 2026-06-12, is not evidence for the start of an older closed interval.

`SH601006` has the numeric source name `000780`. The membership dates remain usable, the value is preserved verbatim, and the report emits `suspicious_company_name`; name correction belongs to a separate bilingual-data review.

### Attribution and rights

The converter and its report attribute `unliftedq/index-constitution`, distributed under the MIT license. That repository license does not by itself establish commercial redistribution rights for the underlying CSI constituent data. Any commercial redistribution requires a separate rights review.

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

1. Generate the CSV and report, preferably into `/tmp` or `data/generated/` which is ignored by Git. Writes use same-directory temporary files, `fsync`, and atomic replacement only after all blocking checks pass.
2. Read the report: it must have `status=valid`, matching checksums, exactly 300 latest members, matching latest/history active sets, no overlaps or duplicates, and no unconvertible rows. A failure report may be published, but no new CSV is published and any existing CSV is preserved.
3. Import via Trigger UI staff-only in dry-run mode, or CLI dry-run:

```bash
python manage.py import_universe_memberships --csv /tmp/csi300_stockalert_memberships.csv --universe-code CSI300 --universe-name "CSI 300" --dry-run
```

4. Apply explicitly only after dry-run review:

```bash
python manage.py import_universe_memberships --csv /tmp/csi300_stockalert_memberships.csv --universe-code CSI300 --universe-name "CSI 300" --apply
```

5. Run CSI300 readiness checks before any backtest.

## Procédure staff dans l’application

1. Dans l’écran **Tickers**, lancer **Générer le CSV historique CSI300**.
2. Vérifier le statut, les compteurs, les warnings et le rapport JSON avant de télécharger le CSV valide.
3. Si une synchronisation des memberships est nécessaire, ouvrir séparément l’interface d’import : la génération ne déclenche jamais d’import.
4. Utiliser **Rafraîchir les données Chine** pour les OHLC des actions, `000300.SHG` et les neuf benchmarks sectoriels supportés.
5. Lire les warnings de couverture dans le tableau de bord ; quelques OHLC absents donnent `READY_WITH_WARNINGS`, tandis qu’une incohérence memberships/snapshots donne `NOT_READY`.

La génération de composition et l’import sont deux opérations distinctes. Un CSV invalide ne remplace jamais la dernière génération validée.

En production, créer avant le déploiement le répertoire persistant
`/opt/stockalert/shared/csi300_artifacts` (ou définir
`CSI300_ARTIFACTS_HOST_PATH`) avec des droits d’écriture pour `web` et
`celery`. Il est monté dans les deux services sous `/data/exports/csi300` ;
les autres données continuent d’utiliser le montage `/data` existant.
