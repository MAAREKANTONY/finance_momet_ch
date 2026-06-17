# Dynamic Universe V1 — audit de faisabilite et d'impact

Source produit prioritaire : `docs/DYNAMIC_UNIVERSE_V1_DECISIONS.md`.

Ce document est un audit. Il ne constitue pas une implementation.

## 1. Resume executif

Dynamic Universe V1 est faisable, mais touche un axe structurant du produit : aujourd'hui, StockAlert suppose presque partout qu'un scenario/backtest opere sur une liste de tickers statique.

Les decisions produit a respecter sont :

- V1 = S&P500 historique uniquement.
- Deux modes scenario :
  - `STATIC_TICKERS`, comportement actuel et defaut.
  - `SP500_HISTORICAL_DYNAMIC`, nouvel univers historique.
- Applicable Backtest uniquement.
- Game hors scope V1 : aucun univers dynamique, aucun resolver, aucune modification UI/export/moteur.
- Survivorship bias interdit.
- Historique de composition incomplet = blocage.
- Une sortie du S&P500 bloque les nouveaux achats, mais ne force pas la vente des positions deja ouvertes.

Conclusion technique :

- Il faut introduire une couche de resolution d'univers avant le moteur.
- Le moteur peut rester proche de son architecture actuelle si on lui fournit :
  - une liste superset de tickers a charger ;
  - une fonction/table `is_member(ticker, date)` appliquee uniquement aux BUY ;
  - des metadonnees d'audit dans les resultats.
- Le Backtest doit passer par un resolver dedie avant le moteur.
- Le Game doit rester strictement sur son comportement actuel : tous les `Symbol.active=True`.
- Une migration sera necessaire dans un sprint ulterieur pour stocker les definitions et memberships historiques, mais elle n'est pas a faire dans cet audit.

Recommandation : GO pour un sprint d'implementation Backtest-only, sous reserve de valider d'abord la disponibilite locale/importable des constituents historiques S&P500.

## 1.1 Cloture de livraison

Dynamic Universe S&P500 V1 est maintenant implemente, valide et livre sur `main`.

Merges de reference :

- `7d53f59` - Phase 6B.4, tolerance de fin de cotation OHLC pour memberships fermes ;
- `27b92a3194fe87a60e3660db4adba604488b1d25` - Phase 6C, UI minimale de preparation OHLC Dynamic Universe.

Commits fonctionnels principaux :

- `e5dbf1f56da58dae67b6a0172899ab10ac4b4726` - job backend explicite de preparation OHLC Dynamic Universe ;
- `47cbc0bf09f79e2e6ba554e354fbef88893aa4ec` - readiness OHLC par intervalles de membership ;
- `2606d32cc5ad5919508f4d6464c4f66f670444c6` - support `exclude_tickers` ;
- `5bb9cbc22465063668ee75119f3abea9ffeeb4a4` - tolerance fin de cotation pour memberships fermes ;
- `3a745ce03e4cb36162babc2faccc0ef2fb7655f3` - action UI de preparation OHLC.

Validation finale sur `backtest_id=7` :

- scenario : `test 1 snp` ;
- mode : `SP500_HISTORICAL_DYNAMIC` ;
- periode : `2022-01-01` a `2026-06-16` ;
- readiness finale : `checked=582`, `ready=582`, `missing=0` ;
- job OHLC final : `ProcessingJob id=107`, `DONE`, `fetched_symbols=21`, `inserted_bars=18444`, `provider_errors=0`, `network_errors=0` ;
- backtest final : `ProcessingJob id=108`, `DONE`, `did_fetch_bars=False`, `did_compute_metrics=True`.

Resultats observes :

- `portfolio_daily` : 1151 lignes ;
- dates portefeuille : `2022-01-03` a `2026-06-16` ;
- `equity_start=10000` ;
- `invested_end=1950000` ;
- `equity_end=2402829.18558` ;
- `bt_return` environ `23.22%` ;
- `max_drawdown` environ `-12.67%` ;
- total trades : 962.

Controle metier final :

- BUY hors membership detecte : 0 ;
- faux/test symbols dans resolver/snapshot/results : 0 ;
- aucun forced sell specifique a une sortie d'indice detecte ;
- GET de la page Backtest : aucun job, aucun provider ;
- UI Phase 6C : bloc OHLC visible, etat pret visible, bouton absent quand `missing=0` ;
- audit Phase 6C : 619 tests OK.

Cleanup data realise avant validation finale :

- 8 `UniverseMembership` `manual_csv` faux/test supprimes du vrai univers `SP500` ;
- aucun `Symbol` supprime ;
- aucune `DailyBar` supprimee ;
- fallback CSV admin/dev conserve.

## 2. Fichiers et fonctions inspectes

Modeles :

- `core/models.py`
  - `Symbol`
  - `Scenario`
  - `Universe`
  - `GameScenario`
  - `SymbolScenario`
  - `Backtest`
  - `BacktestPortfolioDaily`
  - `BacktestPortfolioKPI`
  - `HistoricalMarketCap`

Formulaires :

- `core/forms.py`
  - `ScenarioForm`
  - `UniverseForm`
  - `BacktestForm`
  - `GameScenarioForm`
  - `StudyScenarioForm` lu uniquement pour verifier les dependances, sans recommandation de scope V1.

Vues :

- `core/views.py`
  - `_refresh_backtest_universe_snapshot`
  - `_apply_universe_to_scenario`
  - `backtest_create`
  - `backtest_update`
  - `backtest_run`
  - `backtest_detail`
  - `game_scenario_create`
  - `game_scenario_edit`
  - `game_scenario_detail`
  - exports Backtest XLS compact/full.

Backtest :

- `core/services/backtesting/prep.py`
  - `prepare_backtest_data`
- `core/services/backtesting/engine.py`
  - `_preload_backtest_ticker_data`
  - `run_backtest`
  - `run_backtest_kpi_only`
  - BUY allocation phase
  - BUY execution phase
  - SELL phase
  - forced sell final
- `core/services/backtesting/diagnostic.py`
  - diagnostics GM/GM_push/market cap et usage de `universe_snapshot`.
- `core/services/backtesting/results_offload.py`
  - offload des series daily pour grands univers.

Game :

- `core/services/game_scenarios/runner.py`
  - `run_game_scenario_now`
- `core/services/game_scenarios/sync.py`
  - `sync_game_engine_scenario`
- `core/tasks.py`
  - `daily_system_refresh_job_task`
  - `run_backtest_task`
  - `run_game_scenario_task`
  - `sync_market_caps_job_task`
  - `export_game_scenario_xlsx_task`

Exports / UI :

- `templates/scenario_form.html`
- `templates/backtest_create.html`
- `templates/backtest_edit.html`
- `templates/backtest_detail.html`
- `templates/backtest_results.html`
- `templates/game_scenario_form.html`
- `templates/game_scenario_detail.html`
- `core/backtest_debug.py`
- `core/exports.py`
- `core/views.py` fonctions `_build_backtest_workbook_full` et `_build_backtest_workbook_compact`.

Providers / donnees :

- `core/services/provider_eodhd.py`
- `core/services/market_cap_sync.py`
- `core/services/provider_twelvedata.py`
- `core/services/symbol_enrichment.py`
- `core/services/benchmark_etf_sync.py`

## 3. Realite actuelle du flux tickers

### 3.1 Stockage des tickers

`Symbol` est la table de reference locale :

- `ticker`
- `exchange`
- `name`
- `instrument_type`
- `country`
- `currency`
- `sector`
- `active`

La cle unique est `(ticker, exchange)`. Il n'existe pas actuellement d'identifiant permanent fournisseur, ni de table de renommage/delisting, ni de table de composition historique d'indice.

`Scenario.symbols` est un `ManyToManyField` via `SymbolScenario`. C'est le coeur du mode actuel `STATIC_TICKERS`.

`Universe` est un groupe statique reutilisable de `Symbol`. L'application d'un `Universe` a un `Scenario` copie les symboles dans `Scenario.symbols`. Ce n'est pas un univers dynamique.

`Backtest.universe_snapshot` est un `JSONField` qui stocke une copie de tickers au moment de la creation/rerun. Le snapshot actuel est une liste statique de dicts :

```json
[
  {"ticker": "AAPL", "exchange": "NASDAQ", "sector": "Technology"}
]
```

`GameScenario` n'a pas aujourd'hui de relation explicite vers une liste de tickers. Le Game runtime utilise tous les `Symbol.active=True`.

### 3.2 Selection et validation

`ScenarioForm` expose `symbols` via `SymbolPickerWidget`. Le queryset est `Symbol.objects.filter(active=True).order_by("ticker", "exchange")`.

`UniverseForm` gere aussi une selection statique de symboles.

`templates/scenario_form.html` permet :

- selection manuelle massive ;
- filtres courants par exchange/secteur ;
- application d'un `Universe` existant par copie client-side.

Il n'existe pas de champ UI ou backend pour un mode `STATIC_TICKERS` / `SP500_HISTORICAL_DYNAMIC`.

`BacktestForm` choisit un `Scenario`. Il ne choisit pas l'univers directement.

`GameScenarioForm` ne choisit aucun univers. Il configure indicateurs, signaux, capital, filtres et horizon.

### 3.3 Transmission au moteur Backtest

`core/views.py::_refresh_backtest_universe_snapshot` reconstruit `Backtest.universe_snapshot` depuis `bt.scenario.symbols.all()` et l'enregistre.

Cette fonction est appelee notamment :

- apres creation Backtest ;
- apres edition Backtest ;
- avant lancement Backtest ;
- dans plusieurs actions de fetch/compute/sync liees au Backtest.

`core/services/backtesting/engine.py::run_backtest` lit :

```python
raw_universe = backtest.universe_snapshot or list(backtest.scenario.symbols.values_list("ticker", flat=True))
```

puis le transforme en liste simple `tickers`.

`run_backtest_kpi_only` fait la meme chose.

### 3.4 Transmission au moteur Game actuel, hors scope V1

`core/services/game_scenarios/runner.py::run_game_scenario_now` fait :

```python
symbols = Symbol.objects.filter(active=True).order_by("ticker")
tickers = list(symbols.values_list("ticker", flat=True))
bt = Backtest(..., include_all_tickers=True, universe_snapshot=tickers, ...)
out = run_backtest_kpi_only(bt, max_days=game.study_days)
```

Le Game fabrique donc un Backtest transient en memoire, avec un snapshot statique compose de tous les symboles actifs.

Decision V1 :

- ne pas modifier ce flux ;
- ne pas brancher `UniverseResolver` dans Game ;
- ne pas modifier `run_backtest_kpi_only` pour Dynamic Universe ;
- conserver le comportement `Symbol.active=True`.

## 4. Reponse aux questions Backtest

### Ou la liste des tickers est utilisee

Backtest utilise `Backtest.universe_snapshot` dans :

- `core/views.py::_refresh_backtest_universe_snapshot`
- `core/services/backtesting/prep.py::prepare_backtest_data`
- `core/services/backtesting/engine.py::run_backtest`
- `core/services/backtesting/engine.py::run_backtest_kpi_only`
- `core/tasks.py::_backtest_universe_size`
- `core/tasks.py::sync_market_caps_job_task`
- `core/tasks.py::run_backtest_task`
- `core/services/backtesting/diagnostic.py`
- `templates/backtest_detail.html`
- exports XLS compact/full dans `core/views.py`
- debug CSV/XLS dans `core/tasks.py` et `core/backtest_debug.py`.

### Ou elle est transformee en univers tradable

Dans `run_backtest` et `run_backtest_kpi_only`, la liste est aplatie en `tickers`, puis resolue par :

```python
symbols = list(Symbol.objects.filter(ticker__in=tickers))
sym_by_ticker = {s.ticker: s for s in symbols}
```

Ensuite `_preload_backtest_ticker_data` charge les `DailyBar`, `DailyMetric` et `Alert` pour tout le superset de symboles.

La tradabilite journaliere est ensuite calculee par `_ratio_tradable`, qui combine :

- `include_all_tickers`
- `ratio_threshold`
- prix min/max
- market cap min/max
- politique de market cap manquante.

Aujourd'hui, l'appartenance a l'univers ne fait pas partie de `_ratio_tradable`.

### Ou sont faites les allocations

Dans `run_backtest`, phase `BUY allocation selection phase` :

- les signaux BUY sont evalues ;
- les filtres BUY sont evalues ;
- `_ratio_tradable` est appelee ;
- si la ligne n'est pas encore allouee, l'allocation CT est accordee immediatement en capital infini, ou mise en competition via `candidates_need_alloc` en capital limite.

En capital limite, les candidats sont tries par `ratio_pct` descendant, puis alloues tant que `global_cash >= CT`.

Dans `run_backtest_kpi_only`, le meme schema existe mais sans daily rows ni portfolio detaille.

### Ou sont evalues les BUY

Dans le moteur Backtest complet `run_backtest`, et aussi dans le chemin KPI-only actuel hors scope V1 :

1. phase allocation BUY ;
2. phase execution BUY.

Les deux phases repetent les gates essentiels :

- signal BUY / latch state ;
- `_ratio_tradable` ;
- `_trend_filter_allows_buy` legacy neutralise ;
- `_line_market_conditions_allow_buy` ;
- `_line_gm_push_conditions_allow_buy` ;
- allocation disponible ;
- prix valide ;
- nombre de shares > 0.

Dynamic Universe V1 devra ajouter un gate BUY supplementaire dans les deux phases du Backtest complet :

```text
si mode SP500_HISTORICAL_DYNAMIC :
  BUY autorise seulement si ticker membre S&P500 a la date d
```

Ce gate ne doit pas etre applique a la phase SELL.

### Ou sont evalues les SELL

La phase SELL precede les BUY.

Les sorties actuelles incluent :

- SELL ticker explicite ;
- vente automatique par invalidation selon le trading model ;
- protection marche GM ;
- protection GM_push ;
- forced sell de fin si `close_positions_at_end=True`.

Selon la decision produit, la sortie du S&P500 ne doit pas creer de SELL. Il ne faut donc pas l'integrer dans la phase SELL.

### Ou sont crees les snapshots

`Backtest.universe_snapshot` est cree et rafraichi par `core/views.py::_refresh_backtest_universe_snapshot`.

Les snapshots de resultats sont construits dans :

- `run_backtest`, `results["meta"]`, `results["tickers"]`, `results["portfolio"]`.
- `run_backtest_kpi_only`, dict par ticker.

### Le moteur suppose-t-il un univers constant ?

Oui.

Hypothese explicite :

- `tickers` est determine une seule fois au demarrage.
- `symbols` est resolu une seule fois.
- `data_by_ticker` est precharge pour toute la periode.
- `state` est initialise pour chaque `(ticker, line)` avant la boucle journaliere.
- l'appartenance a l'univers n'est jamais reevaluee dans la boucle quotidienne.

Impacts :

- Dynamic V1 doit charger un superset historique des membres S&P500 sur la periode.
- Puis le moteur doit verifier l'appartenance par date uniquement avant un BUY.
- Les positions ouvertes doivent rester dans `state` apres sortie de l'indice.
- Le superset ne peut pas etre simplement reduit aux membres du jour courant.

## 5. Game hors scope V1

### Creation Game actuelle

`GameScenarioForm` n'a aucun champ de selection d'univers. Il expose uniquement :

- horizon `study_days` ;
- parametres indicateurs ;
- capital ;
- signal lines ;
- filtres prix/market cap ;
- settings GM/GM_push.

### Synchronisation Scenario -> Game actuelle

`core/services/game_scenarios/sync.py::sync_game_engine_scenario` copie les parametres de `GameScenario` vers un `Scenario` interne.

Ce Scenario interne ne recoit pas de symboles. Il sert a stocker `DailyMetric` et `Alert` avec les bons parametres.

### Runtime Game actuel

`run_game_scenario_now` utilise tous les symboles actifs :

```python
symbols = Symbol.objects.filter(active=True).order_by("ticker")
```

Il fetch les bars, calcule les metrics, puis construit un `Backtest` transient avec :

- `include_all_tickers=True`
- `ratio_threshold=0`
- `universe_snapshot=tickers`

Le Game suppose donc aussi un univers fixe, et meme plus largement : il suppose actuellement un univers global "tous symboles actifs".

### Impact Game V1

Aucun changement.

En V1, le Game doit conserver :

- tous les `Symbol.active=True` ;
- aucun univers dynamique ;
- aucun `UniverseResolver` ;
- aucune evolution UI ;
- aucune evolution export ;
- aucune evolution moteur.

Les fichiers Game restent cites dans cet audit uniquement pour documenter le non-impact et eviter une modification accidentelle.

Tests de garde recommandes :

- creation Game inchangee ;
- `run_game_scenario_now` continue d'utiliser `Symbol.active=True` ;
- aucun champ Dynamic Universe n'apparait dans l'UI Game ;
- exports Game inchanges.

## 6. Exports, KPI, results, courbes, diagnostics

### Backtest Results

`templates/backtest_results.html` et `core/views.py::backtest_results` utilisent :

- `results["tickers"]` ;
- `results["portfolio"]` ;
- `bt.include_all_tickers` ;
- `bt.universe_snapshot` indirectement via diagnostics.

Les courbes et diagnostics actuels n'ont pas de notion de membership historique.

Impact V1 :

- afficher le mode d'univers dans la page resultats ;
- afficher `SP500_HISTORICAL_DYNAMIC` et la periode de couverture validee ;
- expliquer que les sorties d'indice bloquent les nouveaux BUY mais ne vendent pas les positions ouvertes ;
- idealement exposer un marqueur ou colonne `universe_member` dans debug/daily, au moins quand le mode dynamique est actif.

### Backtest Detail

`templates/backtest_detail.html` affiche `Univers (snapshot)` comme liste statique `Ticker / Exchange / Secteur`.

Impact V1 :

- en mode dynamique, ce bloc doit devenir un recap :
  - mode `S&P500 historique` ;
  - nombre de tickers distincts dans le superset ;
  - couverture date min/max ;
  - pas une simple liste de membres du jour courant.

### Exports XLS Backtest

`core/views.py::_build_backtest_workbook_full` et `_build_backtest_workbook_compact` creent une feuille `Universe` depuis `bt.universe_snapshot`.

Impact V1 :

- ajouter mode d'univers ;
- ajouter metadata de resolution ;
- ne pas afficher une composition unique comme si elle etait constante ;
- potentiellement ajouter une feuille `UniverseMembership` ou `UniverseCoverage` pour audit.

### Debug CSV/XLS

`core/backtest_debug.py` et `core/tasks.py::export_backtest_debug_csv_task` exportent les lignes detaillees d'un ticker/line.

Impact V1 :

- ajouter une colonne `universe_member` ou `sp500_member` ;
- ajouter une colonne `buy_blocked_by_universe` si un BUY aurait ete vrai mais bloque par sortie/non-appartenance ;
- ne pas modifier la signification des colonnes `tradable` sans clarification, car `tradable` signifie deja prix/ratio/market cap.

### KPI portefeuille

Les KPI sont agregees a partir des tickers joues et des lignes. Si l'univers dynamique bloque uniquement les BUY, les KPI existantes peuvent rester coherentes.

Risque :

- les compteurs `TRADABLE_DAYS` actuels ne representent pas l'appartenance a l'univers. En mode dynamique, un ticker hors S&P500 ne devrait probablement pas compter comme jour tradable pour les jours sans position.
- Produit a trancher : conserver `TRADABLE_DAYS` comme eligibilite prix/ratio uniquement, ou l'etendre a "eligible universe + filtres". Recommendation V1 : ajouter une metrique separee `UNIVERSE_ELIGIBLE_DAYS` pour ne pas casser l'historique des KPI.

### Game results

`GameScenario.today_results` contient :

- `date`
- `rows`
- thresholds
- une ligne par ticker present dans le resultat KPI.

Impact V1 : aucun.

Le Game ne doit pas afficher de mode dynamique, de couverture S&P500 ou de membership historique en V1.

### Exports Game

`core/tasks.py::export_game_scenario_xlsx_task` exporte les champs Game et `today_results`.

Impact V1 : aucun.

Les exports Game doivent rester inchanges.

## 7. Architecture minimale proposee

### 7.1 Constantes de mode

Ajouter un enum metier partage :

- `STATIC_TICKERS`
- `SP500_HISTORICAL_DYNAMIC`

Ces valeurs doivent etre explicites et user-facing via libelles francais :

- "Selection manuelle de tickers"
- "S&P500 historique dynamique"

### 7.2 UniverseDefinition

Responsabilite :

- decrire un univers dynamique disponible.

Champs conceptuels minimaux :

- `code`: `SP500`
- `name`: `S&P500 historique`
- `provider`: par exemple `eodhd` ou `manual_import`
- `active`
- `metadata`

Pour V1, il peut n'y avoir qu'une definition `SP500`.

### 7.3 UniverseMembership

Responsabilite :

- stocker la membership point-in-time.

Champs conceptuels minimaux :

- `universe_definition`
- `symbol`
- `ticker`
- `exchange`
- `provider_symbol` si disponible
- `valid_from`
- `valid_to` nullable
- `source`
- `source_timestamp`

Contraintes importantes :

- permettre un ticker present, sorti, puis eventuellement revenu ;
- indexer par `(universe_definition, symbol, valid_from, valid_to)` ;
- indexer par dates pour resolver rapidement.

### 7.4 UniverseResolver

Responsabilite :

- recevoir :
  - scenario ;
  - start/end ;
  - warmup ;
  - mode d'univers ;
- valider la couverture historique ;
- produire :
  - `symbols`: superset distinct des membres sur la periode ;
  - `tickers`: representation stable pour le moteur ;
  - `active_by_date`: mapping date -> set de tickers membres ;
  - `membership_by_ticker`: intervalles utiles au debug ;
  - metadata de couverture.

Le resolver doit bloquer si :

- aucune membership S&P500 disponible ;
- couverture incomplete pour la periode demandee ;
- composition impossible a reconstruire pour une date de backtest.

### 7.5 Integration moteur

Ne pas remplacer toute la logique moteur.

Integration minimale :

- en amont, resoudre le superset et le stocker/transmettre ;
- dans `run_backtest`, ajouter un predicate :

```text
universe_allows_new_buy(ticker, date)
```

Ce predicate retourne `True` pour `STATIC_TICKERS`.

Pour `SP500_HISTORICAL_DYNAMIC`, il retourne `True` uniquement si le ticker est membre S&P500 a la date.

Il doit etre applique dans :

- phase allocation BUY ;
- phase execution BUY.

Il ne doit pas etre applique dans :

- phase SELL ;
- forced sell final ;
- portfolio valuation ;
- positions deja ouvertes.

`run_backtest_kpi_only` ne fait pas partie du scope Dynamic Universe V1, car le Game est hors scope et conserve son comportement actuel.

### 7.6 Stockage du snapshot Backtest

Le champ `Backtest.universe_snapshot` peut rester utile, mais doit changer de signification en mode dynamique.

Recommendation :

- conserver `universe_snapshot` pour le superset resolu et les metadonnees d'audit ;
- ajouter des champs dedies dans un sprint avec migration :
  - `universe_mode`
  - `universe_definition`
  - ou `universe_settings` JSON.

Alternative sans champ Backtest :

- stocker le mode uniquement dans `Scenario`.

Risque : un Backtest historique pourrait changer de sens si le Scenario est modifie ensuite. Pour la reproductibilite, un snapshot Backtest du mode/resolution est preferable.

## 8. Strategie de compatibilite

### STATIC_TICKERS

Doit rester strictement identique :

- `Scenario.symbols` reste la source ;
- `Backtest.universe_snapshot` reste une liste statique ;
- aucun changement implicite de resultats.

### Scenarios existants

Tous les scenarios sans champ mode explicite doivent etre interpretes comme `STATIC_TICKERS`.

### Backtests existants

Backtests existants :

- gardent leur `universe_snapshot` ;
- ne sont pas reinterpretes ;
- resultats existants inchanges.

### Games existants

Games existants :

- restent en comportement actuel ;
- aucun champ dynamique obligatoire ;
- aucun mode dynamique ;
- aucun `UniverseResolver` ;
- aucune reinterpretation de `today_results`.

Le Game actuel n'a pas de selection manuelle et continue d'utiliser tous les `Symbol.active=True`. Cette logique ne doit pas etre modifiee en V1.

## 9. Strategie de deploiement proposee

### Phase 1 — Configuration scenario

Objectif :

- ajouter le choix de mode sur Scenario uniquement :
  - `STATIC_TICKERS` ;
  - `SP500_HISTORICAL_DYNAMIC` ;
- conserver `STATIC_TICKERS` par defaut ;
- ne pas changer le moteur ;
- ne pas changer Game.

Impact attendu :

- migration probablement necessaire dans le sprint d'implementation ;
- UI Backtest/Scenario uniquement ;
- tests de non-regression sur les scenarios existants.

### Phase 2 — UniverseResolver

Objectif :

- creer `UniverseDefinition` / `UniverseMembership` ou equivalents ;
- importer un jeu fixture/test S&P500 historique ;
- implementer `UniverseResolver` ;
- valider couverture, superset, `active_by_date`.

Pas d'integration moteur encore.

### Phase 3 — Integration Backtest

Objectif :

- adapter `_refresh_backtest_universe_snapshot` ;
- adapter `prepare_backtest_data` si necessaire ;
- adapter `run_backtest` ;
- appliquer le gate membership uniquement aux BUY ;
- ne jamais forcer une vente sur sortie d'indice ;
- ajouter metadata dans `results["meta"]`.

### Phase 4 — Resultats / exports Backtest

Objectif :

- Backtest detail : afficher mode et couverture ;
- Backtest results : afficher mode et avertissements ;
- Debug CSV/XLS : colonnes membership ;
- Exports XLS : feuille universe dynamique ou metadata de couverture ;
- aucune evolution Game.

### Phase 5 — Tests

Objectif :

- tests Backtest ;
- tests resolver ;
- tests non-regression static ;
- tests data incomplete blocking ;
- tests performance sur superset type S&P500 ;
- tests de garde prouvant que Game reste inchange.

## 10. Tests a ajouter

### Modeles / forms

- Scenario sans champ dynamique explicite = `STATIC_TICKERS`.
- Nouveau Scenario peut choisir `SP500_HISTORICAL_DYNAMIC`.
- BacktestForm conserve comportement existant si Scenario static.
- GameScenarioForm conserve strictement son comportement existant et n'expose aucun mode Dynamic Universe.
- UI affiche clairement les deux modes.

### Resolver historique

- Resolution S&P500 sur une periode couverte retourne le superset attendu.
- `active_by_date` varie selon entrees/sorties.
- Historique incomplet bloque avec erreur explicite.
- Pas de fallback vers composition actuelle.
- Ticker sorti puis revenu est gere par intervalles.
- Doublons ticker/exchange geres.

### Backtest

- Static ticker no-regression : resultats identiques avant/apres.
- Mode S&P500 dynamique : ticker non membre ne peut pas acheter.
- Ticker entre dans S&P500 : achat possible seulement a partir de l'entree.
- Ticker sort du S&P500 sans position : plus aucun nouvel achat apres sortie.
- Ticker sort du S&P500 avec position ouverte : pas de vente forcee.
- Position ouverte apres sortie : SELL naturel fonctionne.
- Position ouverte apres sortie : forced sell final fonctionne si active.
- BUY memory existante + sortie indice : signal reste memorise mais BUY bloque tant que hors univers.
- Re-entree indice : BUY possible si signaux toujours actifs et autres gates OK.
- GM BUY ne peut toujours pas acheter seul.
- GM SELL/GM_push SELL peut vendre une position ouverte meme si ticker a quitte l'indice.
- RHD inchangé.
- `trend_filter_*` toujours ignore.

### Game no-regression

- Game legacy no-regression.
- `run_game_scenario_now` continue d'utiliser tous les `Symbol.active=True`.
- `run_backtest_kpi_only` n'est pas modifie pour Dynamic Universe V1.
- `today_results` ne contient pas de metadata Dynamic Universe.
- Game detail/export restent inchanges.

### Exports / results

- Backtest detail affiche mode static/dynamique.
- Backtest XLS feuille Universe ne presente pas l'univers dynamique comme constant.
- Debug CSV contient membership ou blocage universe.
- Game export ne contient pas de mode Dynamic Universe en V1.
- Results offload continue de fonctionner sur grand superset.

### Survivorship bias

- Test explicite : un ticker present aujourd'hui mais absent historiquement en 2022 ne peut pas etre achete en 2022.
- Test explicite : absence de membership pour une date de backtest bloque le run.

## 11. Risques principaux

### Risque donnees S&P500 historique

Le code actuel n'a aucun endpoint de constituents historiques. `provider_eodhd.py` ne gere que `fetch_historical_market_cap`.

Twelve Data est utilise pour OHLC et metadata, EODHD pour market cap. Aucun service local ne synchronise aujourd'hui :

- constituents d'indice ;
- dates d'entree ;
- dates de sortie ;
- delistings ;
- renommages.

Sans source fiable de membership historique, Dynamic Universe V1 doit rester bloque.

### Risque identifiants ticker

Le modele `Symbol` est base sur `(ticker, exchange)`. Cela peut etre insuffisant pour :

- ticker changes ;
- delisted securities ;
- collisions entre exchanges ;
- provider symbols differents.

Pour V1 S&P500, cela peut etre acceptable seulement si la source historique fournit des tickers compatibles avec les `Symbol` locaux et que les tickers delistes necessaires existent localement.

### Risque performance

Un S&P500 historique sur plusieurs annees peut depasser 500 tickers distincts a cause des entrees/sorties.

Impacts :

- fetch OHLC massif ;
- compute metrics massif ;
- JSON results volumineux ;
- diagnostics lourds ;
- exports lourds.

Le code a deja des protections :

- preloads bulk dans engine ;
- `large_result_mode`;
- `results_offload`.

Mais V1 doit tester explicitement le cas S&P500.

### Risque Game

Le risque Game V1 n'est plus un risque fonctionnel d'integration, car Game est hors scope.

Le risque restant est un risque de regression accidentelle :

- modification involontaire de `run_game_scenario_now` ;
- modification involontaire de `run_backtest_kpi_only` pour Dynamic Universe ;
- ajout accidentel de champs Dynamic Universe dans `GameScenarioForm` ou les exports Game.

Les tests de garde doivent confirmer que Game conserve `Symbol.active=True`.

### Risque metriques

Les compteurs `TRADABLE_DAYS` ne representent pas l'appartenance universe. Ajouter le gate universe dedans pourrait changer les KPI historiques. Il vaut mieux ajouter une metrique separee au debut.

### Risque explicabilite

Si un BUY est bloque par sortie S&P500, l'utilisateur doit le voir. Sinon la strategie semblera incoherente.

## 12. Hypotheses a valider avant implementation

1. La source S&P500 historique fournit une composition point-in-time fiable.
2. Les tickers historiques peuvent etre mappes vers `Symbol`.
3. Les OHLC existent pour les membres historiques, y compris sortis/delistés si presents dans la periode.
4. La periode de couverture doit inclure warmup ou seulement fenetre visible ? Recommendation : inclure warmup, car les signaux progressifs utilisent la memoire.
5. Faut-il afficher les tickers sortis dans les results meme s'ils n'ont jamais ete achetes ? Recommendation : oui seulement s'ils font partie du superset charge, mais les vues doivent pouvoir filtrer.

## 13. Estimation de complexite

Complexite globale : moyenne a elevee.

Complexite par lot :

- Modeles + migrations : moyenne.
- Import/sync constituents S&P500 : elevee, dependante data provider.
- Resolver : moyenne.
- Integration Backtest : moyenne a elevee.
- UI/results/exports : moyenne.
- Tests : moyenne a elevee.

Estimation prudente : 3 a 5 sprints courts si la donnee S&P500 historique est disponible et fiable ; plus si la donnee doit etre negociee/importee manuellement.

## 14. Zones a ne pas toucher sans demande explicite

- RHD.
- GM / GM SELL Market Exit.
- GM_push.
- `trend_filter_*`.
- `sell_gm_filter`.
- Study.
- Calculs d'indicateurs.
- Semantique des trading models.
- Forced sell final, sauf affichage/audit.

## 15. GO / NO-GO

GO pour passer a un sprint d'implementation Backtest-only Phase 1/2, avec reserves.

Reserve bloquante avant runtime :

- disposer d'une source S&P500 historique complete et testable.

Recommendation de premier sprint :

1. Ne pas toucher au moteur.
2. Ajouter uniquement les modeles/configuration/resolver avec fixtures de test.
3. Prouver :
   - `STATIC_TICKERS` default ;
   - coverage incomplete bloque ;
   - resolution S&P500 produit un superset et `active_by_date`.

NO-GO pour brancher directement le moteur Backtest sans resolver teste et donnees historiques validees.

NO-GO pour toute modification Game en V1.
