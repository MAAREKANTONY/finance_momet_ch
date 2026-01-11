# Stock Alert App V2.1 (Django + Postgres + Celery)

Adds on top of V2:
- Scenario variable: `history_years` (years of history to fetch)
- Excel export (.xlsx) for:
  - raw daily bars (Twelve Data ingested)
  - computed metrics (P, M/M1, X/X1, Q/S, K1..K4) + alerts
  - all in one workbook (multiple sheets)

## Run
1) Copy `.env.example` -> `.env` and fill values (API key + SMTP).
2) Start:
```bash
docker compose up --build
```
3) Create admin/user:
```bash
docker compose exec web python manage.py createsuperuser
```
4) Open:
- App: http://localhost:8000


## Lancer manuellement (utile pour tester)
```bash
# récupérer l'historique (selon history_years max des scénarios actifs)
docker compose exec web python manage.py fetch_daily_bars

# calculer les métriques + alertes
docker compose exec web python manage.py compute_metrics

# envoyer l'email du dernier jour d'alertes
docker compose exec web python manage.py send_daily_alerts
```


## Planification email (V3)
- L'heure d'envoi est configurée dans l'UI (page Emails) et stockée en base.
- Celery Beat exécute chaque minute `check_and_send_scheduled_alerts_task` et déclenche l'envoi au bon moment.
- Boutons UI: "Lancer les calculs" et "Envoyer l’email maintenant".
