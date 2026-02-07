## Version

- V5.2.6: Page "Aide indicateurs" (formules + alertes), ajout du lien UI, et correctif: persistance de K1f (indispensable pour A1f/B1f + exports) + filtres Alertes incluent A1f/B1f.
- V5.2.10: Ajout FL (facteur de lissage) pour la correction K1f. Formule: C = (VC - ratio_p) * FL * (M1 - X1).
- V5.2.4: Ajout VC + indicateur K1f + alertes A1f/B1f + intégration backtests.

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

## V5.2.10
- Fix: backtests now match alert codes case-insensitively (A1f/B1f work correctly).
- Packaging: clean single-root zip.


---

## Parquet storage (scalable) — Step 1/2

### Enable
Set in `.env` (or real env):
- `ENABLE_PARQUET_STORAGE=1`

Files are written **in addition** to existing JSON results:
- `/data/backtests/<backtest_id>/<scenario>/<ticker>.parquet`

### Details export (ZIP)
On backtest detail page:
- **Exporter Détails (Parquet ZIP)**
- **Exporter Détails (CSV ZIP)**

This export reads from `/data` Parquet files.

---

## Volume guards (Step 3) — Excel stays usable on large universes (optional)

Goal: when the backtest includes many tickers, avoid generating gigantic Excel files.

### Enable
- `ENABLE_VOLUME_GUARDS=1`

### Parameters
- `EXCEL_FULL_TICKERS_THRESHOLD=150`  (default 150)
- `EXCEL_TOP_N=50`                   (default 50)

### Behavior (only if enabled)
If `nb_tickers > EXCEL_FULL_TICKERS_THRESHOLD`:
- Excel exports (**both** “Excel” and “Excel compact”) keep:
  - Settings / Universe / Summary (+ Portfolio sheets if present)
- Excel daily details are limited to **Top N tickers** (by best final BT across lines)
- Full details are available via **Exporter Détails** (Parquet/CSV ZIP)

If not enabled (default): exports remain unchanged.

---

## .env reference (prod-ready)

Create a `.env` at project root (same level as `manage.py`):

```env
# --- Django
APP_VERSION=V5.2.22
DJANGO_SECRET_KEY=change-me
DJANGO_DEBUG=0
DJANGO_ALLOWED_HOSTS=localhost,127.0.0.1,example.com
DJANGO_CSRF_TRUSTED_ORIGINS=https://example.com

# Cookies security (recommended in prod)
CSRF_COOKIE_SECURE=1
SESSION_COOKIE_SECURE=1
CSRF_COOKIE_SAMESITE=Lax
SESSION_COOKIE_SAMESITE=Lax

# --- Postgres
POSTGRES_DB=stockalert
POSTGRES_USER=stockalert
POSTGRES_PASSWORD=stockalert
POSTGRES_HOST=db
POSTGRES_PORT=5432

# --- Redis / Celery
REDIS_URL=redis://redis:6379/0

# --- Twelve Data
TWELVE_DATA_API_KEY=your_key
DEFAULT_EXCHANGE=

# --- Scheduler hours (server local time)
FETCH_BARS_HOUR=1
COMPUTE_HOUR=2
EMAIL_HOUR=7

# --- Email (alerts)
EMAIL_HOST=smtp.example.com
EMAIL_PORT=587
EMAIL_HOST_USER=
EMAIL_HOST_PASSWORD=
EMAIL_USE_TLS=1
EMAIL_FROM=alerts@example.com

# --- Parquet storage (Step 1/2)
ENABLE_PARQUET_STORAGE=1
BACKTEST_DATA_DIR=/data

# --- Volume guards (Step 3)
ENABLE_VOLUME_GUARDS=1
EXCEL_FULL_TICKERS_THRESHOLD=150
EXCEL_TOP_N=50
```

---

## DevOps (Vultr) — make `/data` persistent + perf basics

### 1) Persist `/data`
Recommended: mount a dedicated disk (or block storage) to `/opt/finance-momet/data` and bind-mount it to the containers.

**docker-compose** in this repo already mounts:
- `./data:/data`

So on the server:
```bash
mkdir -p /opt/finance-momet
cd /opt/finance-momet
# put the project here, then:
mkdir -p data
sudo chown -R 1000:1000 data || true
```

If you use a separate disk:
- format + mount it to `/opt/finance-momet/data`
- ensure it is mounted at boot (fstab)
- then keep the compose mount as-is (`./data:/data`)

### 2) Storage sizing guidance
For 600 tickers × 10 years daily:
- Parquet is typically **much smaller** than JSON (especially with compression)
- plan a few GB to be comfortable (depends on how many columns you store)

### 3) Postgres tuning (simple, safe defaults for 2 vCPU / 8GB)
Keep DB for **metadata + aggregates**, not daily series.
Suggested env in `db` container (optional):
- `shared_buffers` ~ 1GB
- `work_mem` modest (8–16MB)
- ensure `postgres_data` is on SSD

### 4) Celery sizing
For 2 vCPU:
- start with `--concurrency=2` (worker)
- keep `celery-beat` separate (already)

### 5) Housekeeping
- rotate logs (docker json logs can grow)
- monitor disk usage of `/data/backtests`
- optional: retention policy (cron) to delete old backtests’ parquet folders (ONLY if you decide; not implemented here)
