# Politique de tests StockAlert

## Objectif
Garantir la non-régression sur les écrans et le moteur les plus fragiles, sans refacto lourd.

## Phase 1 mise en place dans ce ZIP
Cette version ajoute une première base de tests unitaires avec `Django TestCase` pour couvrir les points qui ont déjà cassé en production :

- parsing du sélecteur de tickers volumineux
- création d'univers avec beaucoup de tickers
- création de scénarios avec beaucoup de tickers
- édition de Study avec beaucoup de tickers
- endpoint de recherche de symboles
- endpoint `universes/<id>/symbols.json`
- nettoyage des `signal_lines` et opérateurs GM

## Pourquoi `Django TestCase` et pas pytest pour commencer
- aucune dépendance supplémentaire
- s'intègre directement à `manage.py test`
- plus simple à faire adopter dans votre workflow actuel Docker/Django
- migration plus facile vers pytest plus tard si souhaité

## Commande standard
En local ou dans Docker :

```bash
python manage.py test
```

ou :

```bash
docker compose exec web python manage.py test
```

## Base de données de test
Par défaut, les tests utilisent SQLite automatiquement pour éviter de dépendre du conteneur PostgreSQL. Cela accélère fortement les runs et réduit les faux échecs d'infra.

Pour forcer PostgreSQL :

```bash
DJANGO_TEST_USE_SQLITE=0 python manage.py test
```

## Méthodologie recommandée à partir de maintenant

### Règle 1 — chaque bug de prod doit générer un test
Dès qu'un bug réel est corrigé, ajouter un test qui reproduit le bug avant le fix.

Exemples immédiats :
- TooManyFieldsSent sur univers/scénarios/studies
- disparition du secteur dans les payloads UI
- régression de `signal_lines` / GM

### Règle 2 — protéger d'abord les zones critiques
Ordre recommandé :
1. formulaires volumineux et endpoints de sélection de tickers
2. moteur de backtest
3. calcul du GM
4. warmup
5. exports et offload parquet

### Règle 3 — avant toute nouvelle feature
Workflow minimal obligatoire :
1. lancer la suite de tests existante
2. développer la feature
3. ajouter les tests de la nouvelle feature
4. relancer toute la suite
5. seulement ensuite livrer le ZIP

### Règle 4 — granularité des tests
Faire surtout :
- tests unitaires purs sur fonctions de nettoyage / calcul
- tests de formulaires
- tests de vues ciblées avec `Client`

Éviter au début :
- gros tests end-to-end fragiles
- dépendances réseau
- appels Twelve Data réels

## Commandes utiles
Lancer seulement les nouveaux tests :

```bash
python manage.py test core.tests
```

Lancer un fichier précis :

```bash
python manage.py test core.tests.test_large_symbol_views
```

Verbosité utile :

```bash
python manage.py test core.tests -v 2
```

## Étape suivante recommandée
Ajouter une phase 2 dédiée au moteur métier :
- buy / sell
- AND / OR
- accumulation non simultanée
- warmup
- GM filter
- snapshots de non-régression sur backtests connus
