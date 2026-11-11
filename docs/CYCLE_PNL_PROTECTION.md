# Protection du PnL par cycle

Le formulaire de création et d’édition d’un backtest propose trois modes :

- **Désactivée** : aucune liquidation déclenchée par cette protection.
- **Recul de la valeur du portefeuille** : mode historique, fondé sur un indice de valeur corrigé des apports et un sommet propre à chaque cycle.
- **Repli du PnL par cycle** : surveillance du bénéfice réalisé et latent atteint depuis l’ouverture du cycle, en montant dans la devise du backtest.

Les configurations protégées sans champ `mode` restent dans le mode historique. Les résultats déjà calculés ne sont ni recalculés ni réécrits. Leur affichage utilise le mode et les paramètres enregistrés dans le résultat, avec le mode historique comme valeur par défaut pour les anciens résultats.

## Règle du nouveau mode

Avant les ordres de chaque séance :

1. Le PnL du cycle vaut la variation du PnL global net des apports depuis l’ouverture du cycle. Il comprend le réalisé du cycle et le latent des positions ouvertes.
2. Le sommet du PnL est le maximum observé avant ordres depuis l’ouverture, initialisé à zéro.
3. Avec un sommet positif, la borne est `sommet × (1 − X/100)`.
4. Sans sommet positif, la borne est `−L`.
5. Le déclenchement a lieu si et seulement si le PnL est **strictement inférieur** à la borne. L’égalité ne déclenche pas.

X est un pourcentage strictement compris entre 0 et 100. L est une perte maximale initiale **strictement positive en montant**, dans la devise du backtest, sans conversion. Dès le premier sommet positif, la règle de repli du bénéfice remplace la limite −L.

### Exemple discriminant

Capital initial 10 000, X = 1 %, L = 100 :

| Equity | PnL du cycle | Sommet du PnL | Borne | Déclenchement PnL |
|---:|---:|---:|---:|---|
| 10 000 | 0 | 0 | −100 | Non |
| 12 000 | +2 000 | +2 000 | +1 980 | Non |
| 11 980 | +1 980 | +2 000 | +1 980 | Non : égalité |
| 11 970 | +1 970 | +2 000 | +1 980 | Oui |

Au dernier point, le mode historique mesure seulement `(12 000 − 11 970) / 12 000 = 0,25 %`, inférieur à 1 % : il ne déclenche pas.

### Exemple sans bénéfice positif

Avec L = 100 et un sommet nul :

| PnL du cycle | Borne | Déclenchement |
|---:|---:|---|
| −100 | −100 | Non : égalité |
| −101 | −100 | Oui |

## Comptes et cycles

Les comptes du moteur restent la source des montants. Les apports supplémentaires en capital illimité augmentent les financements du cycle ; ils ne créent ni bénéfice ni sommet. Une vente ordinaire transfère le latent au réalisé sans remettre le cycle à zéro. La réserve signée FIXED et le cash REINVEST gardent leurs conventions existantes.

Une fois déclenchée, la liquidation vend toutes les positions aux cours exécutables. Les positions sans cotation restent en attente, même si le PnL se rétablit. Tous les achats sont bloqués jusqu’à la liquidation complète. Les latches et les règles de la méthode de trading continuent de s’appliquer. Un nouveau cycle peut s’ouvrir au plus tôt à la séance suivante, sous réserve du financement disponible, avec PnL et sommet remis à zéro. Le PnL global et le cumul des gains/pertes réalisés continuent.

## Lecture des résultats

Le mode enregistré, X et L figurent au-dessus du tableau des cycles. Pour le mode PnL, le tableau et le graphique affichent le PnL du cycle, son sommet et sa borne en montant dans la devise du backtest. Les anciennes courbes Equity/Drawdown sont conservées.

- La série du PnL du cycle est enregistrée en fin de séance, après les ordres.
- Le sommet et la borne sont observés avant les ordres.
- Le premier déclenchement conserve sa date, son PnL, son sommet, sa borne et la règle appliquée, indépendamment des valeurs constatées les jours suivants.
- Les losanges orange sont les déclenchements, à leur valeur exacte de décision. Les triangles verts du graphique Equity sont les ventes exécutées ; une vente différée peut donc avoir une autre date.
- Les valeurs exactes sont enregistrées sous forme de chaînes décimales ; le tableau les rend accessibles au survol des cellules et l’infobulle des déclenchements les reprend.
- Les changements de cycle sont séparés sur le graphique. Les points absents ne sont pas estimés. Un résultat PnL sans données journalières demande un recalcul.

Le graphique utilise la série complète déjà envoyée pour le graphique de capital, sans dupliquer le chargement des lignes quotidiennes. Les formules des KPI, notamment BMD et son agrégat KPI-only `best_bmd`, ne changent pas.

## Configuration persistée

```json
{
  "portfolio_protection": {
    "enabled": true,
    "mode": "cycle_pnl",
    "threshold_pct": "1",
    "initial_loss_amount": "100"
  }
}
```

Le mode historique explicite est `portfolio_value`. Son omission conserve ce mode. `enabled: false` désactive la protection. Les réglages sont inclus dans les snapshots de configuration ; changer le mode, X ou L invalide le résultat du backtest édité et nécessite un nouveau calcul.

Dans le nouveau résultat, `portfolio.protection` conserve le mode et les paramètres utilisés. Ses cycles enregistrent `pnl`, `pnl_peak`, `pnl_bound`, puis `trigger_date`, `trigger_pnl`, `trigger_peak_pnl`, `trigger_bound`, `trigger_rule`. Les lignes quotidiennes contiennent `protection_cycle_pnl`, `protection_pnl_peak`, `protection_pnl_bound` et les valeurs exactes avant ordres `protection_decision_pnl`, `protection_decision_peak`, `protection_decision_bound`.

## Recette

1. Charger cette branche dans une application de recette. Préserver le backtest de référence et en préparer trois copies, avec la même période, les mêmes cours, signaux, capitaux et méthode de trading. Ne pas rafraîchir les données entre les trois calculs.
2. Choisir respectivement Désactivée, Recul de la valeur du portefeuille, et Repli du PnL par cycle. Saisir X = 1 % pour les modes actifs et L = 100 pour le mode PnL. Vérifier que L apparaît uniquement pour ce dernier.
3. Vérifier les erreurs de formulaire pour X = 0 ou 100 et pour L = 0, négatif ou absent. Vérifier qu’un ancien backtest protégé s’ouvre en mode historique.
4. Lancer les calculs. Vérifier le mode **enregistré** dans chaque résultat, puis les montants du tableau, le graphique PnL et les marqueurs distincts de déclenchement et d’exécution. Une simple modification ultérieure du formulaire ne doit pas servir à réinterpréter un ancien résultat.
5. Rejouer les cas déterministes des tests, qui ne font appel à aucun fournisseur :

```bash
python manage.py test core.tests.test_cycle_pnl_protection --verbosity 2
```

Le cas discriminant utilise 100 actions achetées à 100, puis les cours 120, 119.8 et 119.7 : +1 980 le 03/01/2024 ne déclenche pas ; +1 970 le 04/01 déclenche. Le cas initial utilise les cours 100, 99 et 98.99 : −100 le 02/01 ne déclenche pas ; −101 le 03/01 déclenche.

Le cas sans cotation déclenche le 03/01 à un PnL de +1 940, un sommet de +2 000 et une borne de +1 980. BBB est vendu ce jour-là ; AAA attend le 05/01. Aucun rachat n’a lieu les 03–05/01 ; la reprise est possible le 06/01. Les valeurs du premier déclenchement restent inchangées dans le tableau.

Les autres tests couvrent plusieurs cycles, une vente ordinaire, les financements en capital illimité, FIXED/REINVEST, les résultats détaillés et allégés, les protections historique/désactivée, les snapshots et le contrat `best_bmd`.
