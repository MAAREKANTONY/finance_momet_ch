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
