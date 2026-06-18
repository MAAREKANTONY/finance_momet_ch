# Dynamic Universe S&P500 V1 — runbook

Ce runbook décrit le parcours utilisateur et opérateur pour utiliser Dynamic Universe S&P500 V1 en backtest.

## Scope

V1 couvre uniquement :

- l'univers historique S&P500 ;
- les Backtests ;
- le mode scénario `SP500_HISTORICAL_DYNAMIC` ;
- la préparation OHLC explicite via job EODHD.

V1 ne couvre pas :

- Game ;
- Nasdaq ;
- secteurs ;
- Europe ;
- multi-indices ;
- forced sell à la sortie d'indice.

`STATIC_TICKERS` reste le mode par défaut et conserve le comportement historique.

## Règles métier

- L'univers dynamique est résolu historiquement à partir des `UniverseMembership`.
- Un BUY est autorisé seulement si le ticker est membre du S&P500 à la date d'achat.
- Un SELL naturel reste inchangé.
- Une sortie du S&P500 ne force pas la vente.
- `run_backtest` ne fait aucun appel provider.
- Un backtest dynamique ne doit pas tourner avec des OHLC incomplets.
- Si la readiness OHLC est incomplète, le backtest bloque avec un message utilisateur explicite.

## Données et providers

- EODHD fournit les constituants historiques S&P500.
- EODHD est le provider par défaut pour préparer les OHLC Dynamic Universe.
- TwelveData reste utilisé par les flux existants hors préparation OHLC dynamique.
- Le fallback CSV admin/dev Dynamic Universe est conservé.
- Le job OHLC supporte `exclude_tickers` pour éviter les faux/test symbols.

Faux/test symbols connus à exclure des préparations ciblées :

```text
DKEEP
DNEW
KEEP
NEW
OLD
DOLD
```

## Readiness OHLC

La readiness OHLC Dynamic Universe vérifie les données sur les intervalles de membership utiles :

```text
effective_start = max(backtest_start_or_warmup, membership.valid_from)
effective_end = min(backtest_end, membership.valid_to or backtest_end)
```

Règles importantes :

- les memberships fermés acceptent une tolérance de fin de cotation de 10 jours calendaires maximum ;
- cette tolérance ne s'applique pas aux memberships ouverts ;
- les actifs courants doivent couvrir la fin du backtest ;
- la tolérance de fin ne s'applique pas au début de période ;
- les faux/test symbols sans données restent bloquants tant qu'ils sont dans le scope.

## Parcours utilisateur

1. Créer ou configurer un scénario en mode `SP500_HISTORICAL_DYNAMIC`.
2. Créer ou ouvrir un backtest utilisant ce scénario.
3. Consulter le bloc :

```text
Préparation des données OHLC — Univers dynamique
```

4. Si le bloc indique des OHLC manquants, cliquer sur `Préparer les données OHLC`.
5. Attendre la fin du job.
6. Revenir sur la page Backtest.
7. Vérifier que l'état indique que les données OHLC sont prêtes.
8. Relancer le backtest.
9. Vérifier que le backtest termine en `DONE`.

## Fresh install

Après une fresh install ou un `docker compose down -v`, appliquer d'abord les migrations :

```bash
python manage.py migrate
```

Puis initialiser les données de référence minimales :

```bash
python manage.py init_reference_data
```

Cette commande est idempotente. Elle crée ou réactive uniquement les références minimales :

```text
UniverseDefinition(code="SP500", active=True)
Symbol ETF de référence marchés US : SPY, QQQ, DIA, IWM
Symbol ETF de référence secteurs US : XLK, XLF, XLE, XLV, XLY, XLP, XLI, XLB, XLU, XLRE, XLC
Symbol ETF de référence Europe listés US : VGK, FEZ, EZU, EWU, EWQ, EWG, EWI, EWP, EWN, EWD, EWL
```

Les symbols créés sont des références de tickers seulement. Ils n'ont pas d'OHLC, pas de market cap, pas de membership et pas de coverage validée.

Elle ne fait pas :

- d'appel EODHD ;
- d'appel TwelveData ;
- d'import CSV ;
- d'import de constituants historiques ;
- d'import massif NYSE/NASDAQ ;
- de création de `UniverseMembership` ;
- de création de `UniverseCoverageSnapshot` validée ;
- de création de `DailyBar` ou de données OHLC ;
- de création de market cap ;
- de préparation OHLC ;
- de lancement de job ou backtest.

Elle évite l'erreur de configuration de base :

```text
UniverseDefinition SP500 is missing or inactive.
```

Pour rendre un backtest Dynamic Universe réellement exploitable, il faut ensuite importer ou synchroniser les constituants historiques S&P500 avec la commande dédiée, puis préparer les OHLC via l'UI ou le job explicite.

Dry-run :

```bash
python manage.py init_reference_data --dry-run
```

## UI Phase 6C

La page Backtest dynamique affiche :

- état prêt/incomplet ;
- compteur `ready / checked` ;
- nombre de tickers manquants ;
- liste des 20 premiers tickers manquants ;
- `+N autres` si la liste dépasse 20 ;
- dernier job OHLC ;
- bouton `Préparer les données OHLC` si `missing > 0` et aucun job actif.

Le GET de la page est strictement passif :

- aucun job lancé ;
- aucun provider appelé ;
- aucune préparation automatique.

Le POST du bouton lance explicitement le job existant avec :

- `provider="eodhd"` ;
- `force_refresh=False` ;
- `max_symbols=50` ;
- `exclude_tickers=["DKEEP", "DNEW", "KEEP", "NEW", "OLD", "DOLD"]`.

## Validation finale V1

Validation réalisée sur `backtest_id=7`.

Backtest :

- scénario : `test 1 snp` ;
- mode : `SP500_HISTORICAL_DYNAMIC` ;
- période : `2022-01-01` à `2026-06-16`.

Readiness finale :

- `checked=582` ;
- `ready=582` ;
- `missing=0`.

Job OHLC final :

- `ProcessingJob id=107` ;
- `status=DONE` ;
- `fetched_symbols=21` ;
- `inserted_bars=18444` ;
- `provider_errors=0` ;
- `network_errors=0`.

Backtest dynamique final :

- `ProcessingJob id=108` ;
- `status=DONE` ;
- `did_fetch_bars=False` ;
- `did_compute_metrics=True`.

Résultats :

- `portfolio_daily rows=1151` ;
- dates : `2022-01-03` à `2026-06-16` ;
- `equity_start=10000` ;
- `invested_end=1950000` ;
- `equity_end=2402829.18558` ;
- `bt_return≈23.22%` ;
- `max_drawdown≈-12.67%` ;
- total trades : 962.

Audit métier :

- BUY hors membership détecté : 0 ;
- faux/test symbols dans resolver/snapshot/results : 0 ;
- aucun forced sell lié à une sortie d'indice détecté ;
- UI Phase 6C validée ;
- GET page Backtest sans job/provider ;
- 619 tests OK lors de l'audit Phase 6C.

## Cleanup data validé

Avant la validation finale :

- 8 `UniverseMembership` `manual_csv` faux/test ont été supprimés du vrai univers `SP500` ;
- aucun `Symbol` n'a été supprimé ;
- aucune `DailyBar` n'a été supprimée ;
- le fallback CSV admin/dev a été conservé.

## Points de vigilance

Non bloquants pour V1 :

- le message suivant est ambigu et devra être clarifié :

```text
Insufficient metrics depth for date range: missing coverage on 582/582 symbols
```

- `max_symbols=50` est hardcodé côté UI ;
- `provider=eodhd` est hardcodé côté UI ;
- `exclude_tickers` est hardcodé côté UI ;
- le debug des daily rows détaillées est limité en large-result mode ;
- le multi-indices reste hors V1 ;
- la décision de ne pas faire de forced sell à la sortie d'indice est assumée.

## Vérifications rapides

Avant de considérer un backtest dynamique comme exploitable :

1. Le scénario doit être en mode `SP500_HISTORICAL_DYNAMIC`.
2. La readiness OHLC doit être complète.
3. Le dernier job OHLC doit être `DONE` si une préparation était nécessaire.
4. Le backtest final doit être `DONE`.
5. `did_fetch_bars` doit rester `False` pendant le backtest final.
6. Les faux/test symbols ne doivent pas apparaître dans resolver, snapshot ou résultats.
