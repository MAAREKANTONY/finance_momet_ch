## Version

- V6.0.0: Ajout de la ligne flottante K2f (pré-ligne + moyenne mobile) + alertes A2f/B2f + paramètres scénario (N5, K2J, CR) + intégration Backtests/Alertes.
- V6.0.0: Page "Aide indicateurs" (formules + alertes), ajout du lien UI, et correctif: persistance de K1f (indispensable pour A1f/B1f + exports) + filtres Alertes incluent A1f/B1f.
- V6.0.0: Ajout FL (facteur de lissage) pour la correction K1f. Formule: C = (VC - ratio_p) * FL * (M1 - X1).
- V6.0.0: Ajout VC + indicateur K1f + alertes A1f/B1f + intégration backtests.

---

## K2f (ligne flottante) + A2f / B2f (V6.0.0)

### Paramètres scénario (UI)
- **N5** (`scenario.n5`, défaut **100**) : fenêtre de calcul des pentes basée sur la variation journalière.
- **K2J** (`scenario.k2j`, défaut **10**) : fenêtre de lissage (moyenne mobile) de la pré-ligne K2f.
- **CR** (`scenario.cr`, défaut **10**) : indice de correction.
- **e** (`scenario.e`) : variable existante réutilisée dans le facteur de correction.

### Définition (ratio, pas en %)
Soit **P** le prix d’étude du jour, **P(-1)** celui de la veille.

1) **Variation journalière** : `dv = (P - P(-1)) / P(-1)`

2) **Pente 1** : `slope1 = sum_{N5 jours}(dv) * 100`

3) **Pente rapportée à 90°** : `slope_deg = slope1 / 90`

4) **Facteur de correction** : `FC = slope_deg * e * CR`

5) **Pré-ligne** : `K2f_pre = K1 - FC` (K1 est l’indicateur existant)

6) **Ligne flottante** : `K2f = moyenne_{K2J jours}(K2f_pre)`

7) **Pente 2** : `slope2 = sum_{N5/2 jours}(dv) * 100`

8) **Différence de pentes** : `diff = slope2 - slope1`

### Signaux
- **A2f (Achat prudent)** : K1 croise K2f de bas en haut **et** `diff > 0`.
- **B2f (Vente rapide)** : K1 croise K2f de haut en bas **ou** `diff < 0`.

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

## V6.0.0
- Fix: backtests now match alert codes case-insensitively (A1f/B1f work correctly).
- Packaging: clean single-root zip.

---

## K2f floating line (A2f/B2f) — V6.0.0

### Paramètres scénario
Tous les paramètres ci-dessous sont **dans le modèle Scenario**, modifiables via l'UI.

- `N5` (`scenario.n5`, défaut 100): fenêtre (jours) pour la somme des variations journalières.
- `K2J` (`scenario.k2j`, défaut 10): fenêtre (jours) de lissage (moyenne mobile) de la pré-ligne.
- `CR` (`scenario.cr`, défaut 10): indice de correction.
- `e` (`scenario.e`): variable existante, utilisée dans le facteur de correction.

### Formules
Notations:
- `P` = prix d'étude du jour (déjà calculé par le scénario)
- `P(-1)` = prix d'étude de la veille

1) Variation journalière (ratio, pas en %):
`var = (P - P(-1)) / P(-1)`

2) Pente 1 (sur N5 jours):
`slope1 = (Σ var sur N5 jours) * 100`

3) Pente corrigée rapportée à 90°:
`slope_deg = slope1 / 90`

4..6) Facteur de correction:
`FC = slope_deg * e * CR`

7) Pré-ligne:
`K2f_pre = K1 - FC`  (K1 est déjà calculée)

8) Ligne flottante:
`K2f = moyenne mobile sur K2J jours de K2f_pre`

9) Pente 2 (sur N5/2 jours):
`slope2 = (Σ var sur (N5/2) jours) * 100`

10) Différence:
`diff = slope2 - slope1`

### Alertes
- **A2f (Achat prudent)**: `K1` croise `K2f` de bas en haut **ET** `diff > 0`.
- **B2f (Vente rapide)**: `K1` croise `K2f` de haut en bas **OU** `diff < 0`.


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
APP_VERSION=V6.0.0
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
