# Politique de tests StockAlert

## Objectif
Garantir la non-régression sur les zones les plus fragiles :
- sélection massive de tickers
- calculs d'indicateurs
- alertes enrichies GM
- moteur de backtest
- games
- garde-fous de scalabilité

## Ce que couvre maintenant la suite
### UI / formulaires volumineux
- création d'univers avec beaucoup de tickers
- création de scénarios avec beaucoup de tickers
- sélection massive via CSV caché
- endpoints de recherche / preview / univers JSON
- duplication de scénario avec bootstrap correct

### Indicateurs
- cohérence entre calcul incrémental et full recompute sur les champs critiques :
  `P, M, M1, X, X1, T, Q, S, K1..K4, Kf, sum_slope, slope_vrai, sum_slope_basse, slope_vrai_basse`
- cohérence des alertes produites par les deux chemins de calcul

### Global Momentum
- calcul des valeurs GM par date
- discrétisation `GM_POS / GM_NEG / GM_NEU`
- enrichissement des alertes avec suppression des anciens codes GM

### Backtest
- mémoire AND non simultanée
- filtres GM sur BUY / SELL
- garde-fou de scalabilité sur `run_backtest_kpi_only` : on vérifie que la charge multi-tickers ne redéclenche pas un schéma de requêtes N+1

### Games
- snapshot journalier correctement persisté
- interprétation correcte des seuils `BMD` (ratio) vs seuil UI saisi en pourcentage

## Pourquoi `Django TestCase`
- zéro dépendance supplémentaire
- simple à lancer dans ton workflow actuel Docker / Django
- suffisamment robuste pour bâtir la base anti-régression
- migration vers `pytest` possible plus tard, mais non nécessaire pour verrouiller le moteur maintenant

## Commandes
### Suite complète
```bash
python manage.py test
```

ou dans Docker :
```bash
docker compose exec web python manage.py test
```

### Suite StockAlert ciblée
```bash
python manage.py test core.tests -v 2
```

### Un fichier précis
```bash
python manage.py test core.tests.test_engine_and_metrics -v 2
```

### Forcer PostgreSQL au lieu de SQLite
```bash
DJANGO_TEST_USE_SQLITE=0 python manage.py test core.tests -v 2
```

## Règles de méthodologie à suivre
### 1. Chaque bug réel doit créer un test
Workflow obligatoire :
1. reproduire le bug dans un test
2. constater l'échec
3. corriger
4. relancer toute la suite
5. seulement ensuite livrer le ZIP

### 2. Toute nouvelle feature doit arriver avec ses tests
Minimum attendu :
- un test métier positif
- un test d'edge case
- un test de non-régression si une zone fragile est touchée

### 3. Ne pas se contenter des formulaires
Pour StockAlert, il faut protéger en priorité :
1. calculs d'indicateurs
2. signaux / alertes
3. moteur de backtest
4. GM
5. games
6. performance des chemins multi-tickers

### 4. Scalabilité : ce qu'on vérifie en pratique
Les tests unitaires ne prouvent pas qu'un run prod de 7000 tickers sera "rapide", mais ils permettent d'éviter les catastrophes structurelles.

On doit donc surveiller :
- les patterns N+1 en base
- les boucles Python inutiles par ticker
- les conversions massives d'objets lourds
- les recomputes complets non nécessaires

Dans cette version, la suite protège déjà un point important :
- `run_backtest_kpi_only` ne doit pas repartir sur 3 requêtes par ticker

## Discipline de run recommandée
### Avant de coder
```bash
python manage.py test core.tests -v 2
```

### Après ton développement
```bash
python manage.py test core.tests -v 2
```

### Avant livraison finale du ZIP
```bash
python manage.py test -v 2
```

## Prochaine phase recommandée
La prochaine couche utile est un sprint de tests métier encore plus profonds sur :
- backtests avec warmup
- comparaison de snapshots de résultats connus
- alert definitions / scheduling
- export debug / export Excel
- tests de charge contrôlés sur 500 à 2000 tickers en environnement Docker local
