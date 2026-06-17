# Dynamic Universe V1 — décisions produit

Ce document est la source de vérité produit pour Dynamic Universe V1.

Il ne décrit pas une implémentation validée. Il fixe les décisions métier à préserver lors des prochains audits et sprints.

## Périmètre V1

Dynamic Universe V1 couvre uniquement :

- l'univers historique S&P500 ;
- les Backtests ;
- la coexistence avec le mode actuel de sélection manuelle.

Dynamic Universe V1 ne couvre pas :

- Game ;
- Nasdaq ;
- secteurs ou sous-secteurs ;
- Europe ;
- multi-indices ;
- univers custom historiques ;
- migration de données ;
- modification du moteur dans ce document.

## Modes de scénario

Le scénario devra permettre de choisir explicitement entre deux modes :

1. `STATIC_TICKERS` / sélection manuelle actuelle ;
2. univers dynamique S&P500 historique.

`STATIC_TICKERS` reste le comportement par défaut.

Les scénarios existants doivent continuer à fonctionner exactement comme aujourd'hui.

## Compatibilité

Exigence ferme :

- zéro régression sur les scénarios existants ;
- zéro changement implicite pour les Backtests existants ;
- zéro changement implicite pour les Games existants ;
- aucun fallback silencieux vers un nouveau mode.

Un scénario existant sans configuration dynamique doit rester en sélection manuelle.

Le Game conserve son fonctionnement actuel en V1 :

- tous les `Symbol.active=True` ;
- aucun univers dynamique ;
- aucun `UniverseResolver` ;
- aucune évolution UI ;
- aucune évolution export ;
- aucune évolution moteur.

## Correctitude historique

Le survivorship bias est interdit.

Dynamic Universe V1 ne doit jamais utiliser la composition S&P500 actuelle pour simuler une période passée.

La composition de l'univers doit être évaluée historiquement, à la date concernée.

Si l'historique de composition est incomplet sur la période demandée :

- le Backtest doit être bloqué ;
- aucun fallback silencieux n'est autorisé ;
- un message d'erreur explicite doit expliquer que l'historique S&P500 est insuffisant.

## Sortie du S&P500

Quand une action sort du S&P500 :

- ne pas forcer la vente ;
- bloquer uniquement les nouveaux achats si le ticker n'est plus membre de l'univers ;
- si une position est déjà ouverte, elle reste ouverte ;
- la position ouverte se clôture uniquement via :
  - vente naturelle selon la stratégie ;
  - protection déjà configurée ;
  - force sell de fin de backtest si ce comportement est déjà actif.

La sortie de l'indice ne doit donc pas créer de `FORCED_SELL` spécifique en V1.

## Backtest uniquement

Dynamic Universe V1 est applicable uniquement au chemin Backtest.

Une future implémentation devra vérifier explicitement que :

- le mode dynamique S&P500 historique est transmis au Backtest ;
- le moteur Backtest applique l'appartenance historique uniquement comme blocage de nouveaux achats ;
- les résultats, diagnostics et exports Backtest expliquent le mode dynamique.

## Game hors scope V1

Dynamic Universe ne s'applique pas au Game en V1.

Le Game ne doit pas :

- proposer de mode S&P500 historique ;
- utiliser `UniverseResolver` ;
- modifier sa sélection actuelle basée sur `Symbol.active=True` ;
- ajouter de nouveaux champs UI ;
- ajouter de nouveaux exports ;
- modifier `run_backtest_kpi_only` pour ce besoin.

## Non-objectifs techniques immédiats

Ce document ne demande pas :

- d'implémenter Dynamic Universe ;
- de modifier le moteur ;
- de créer une migration ;
- de modifier Game ;
- de changer RHD ;
- de changer GM ;
- de réactiver `trend_filter_*` ;
- de réutiliser `sell_gm_filter` ;
- de toucher Study ;
- de modifier les exports, graphiques ou résultats existants.

## Décision de merge de ce document

Ce document est uniquement une clarification produit.

Il ne doit introduire aucun changement fonctionnel.

## Clôture V1 livrable

Dynamic Universe S&P500 V1 est livré sur `main`.

Commits de référence :

- `e5dbf1f56da58dae67b6a0172899ab10ac4b4726` - job explicite de préparation OHLC Dynamic Universe ;
- `47cbc0bf09f79e2e6ba554e354fbef88893aa4ec` - readiness OHLC par intervalles de membership ;
- `2606d32cc5ad5919508f4d6464c4f66f670444c6` - `exclude_tickers` pour la préparation OHLC ;
- `5bb9cbc22465063668ee75119f3abea9ffeeb4a4` - tolérance de fin de cotation pour memberships fermés ;
- `3a745ce03e4cb36162babc2faccc0ef2fb7655f3` - UI de préparation OHLC Dynamic Universe.

Merges de clôture :

- `7d53f59` - Phase 6B.4 ;
- `27b92a3194fe87a60e3660db4adba604488b1d25` - Phase 6C.

### Scope final V1

V1 couvre :

- S&P500 historique uniquement ;
- Backtests uniquement ;
- mode scénario `SP500_HISTORICAL_DYNAMIC` ;
- mode `STATIC_TICKERS` conservé comme comportement par défaut ;
- UI minimale de préparation OHLC sur la page Backtest.

V1 ne couvre pas :

- Game ;
- Nasdaq ;
- secteurs ou sous-secteurs ;
- Europe ;
- multi-indices ;
- univers custom historiques ;
- forced sell à la sortie d'indice.

### Règles métier finales

- L'univers est résolu historiquement via `UniverseMembership`.
- Un BUY est autorisé uniquement si le ticker est membre de l'univers à la date d'achat.
- Les SELL naturels restent inchangés.
- La sortie du S&P500 ne force pas la vente.
- `run_backtest` ne fait aucun appel provider.
- Aucun backtest dynamique ne doit tourner partiellement avec des OHLC manquants.
- Le backtest dynamique bloque tant que la readiness OHLC est incomplète.

### Données et providers

- EODHD est utilisé pour les constituants historiques S&P500.
- EODHD est le provider par défaut de la préparation OHLC Dynamic Universe.
- TwelveData reste inchangé pour les flux existants hors préparation dynamique.
- Le fallback CSV admin/dev Dynamic Universe est conservé.
- `exclude_tickers` permet d'éviter les faux/test symbols lors des jobs OHLC ciblés.

### Readiness OHLC

- La readiness Dynamic Universe est vérifiée sur les intervalles utiles de membership.
- Pour un membership fermé, la borne de fin accepte une tolérance maximale de 10 jours calendaires.
- Cette tolérance ne s'applique pas aux memberships ouverts ni aux actifs courants.
- La tolérance ne s'applique pas au début de période au-delà de la tolérance existante.
- Les faux/test symbols sans données restent bloquants tant qu'ils sont dans le scope.

### UI Phase 6C

La page Backtest dynamique affiche un bloc :

```text
Préparation des données OHLC — Univers dynamique
```

Le bloc indique :

- l'état prêt/incomplet ;
- le compteur ready/checked/missing ;
- les premiers tickers manquants ;
- le dernier job OHLC ;
- un bouton `Préparer les données OHLC` si `missing > 0`.

Le GET de la page ne lance aucun job et n'appelle aucun provider.

Le POST de préparation lance explicitement le job OHLC existant avec :

- `provider="eodhd"` ;
- `force_refresh=False` ;
- `max_symbols=50` ;
- exclusion des faux/test symbols connus : `DKEEP`, `DNEW`, `KEEP`, `NEW`, `OLD`, `DOLD`.

### Dette post-MVP

- Le message metrics depth suivant est ambigu et doit être clarifié :

```text
Insufficient metrics depth for date range: missing coverage on 582/582 symbols
```

- `max_symbols=50` est hardcodé côté UI.
- `provider=eodhd` est hardcodé côté UI.
- La liste `exclude_tickers` est hardcodée côté UI.
- Le debug des daily rows détaillées est limité en large-result mode.
- Le multi-indices reste hors V1.
