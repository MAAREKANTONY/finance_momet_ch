-- StockAlert — cleaning SAFE des données calculées uniquement
-- Conserve: tickers/symbols, scenarios, games, backtests, daily bars/source data, alert definitions, universes
-- Supprime / réinitialise: métriques, alertes calculées, snapshots games, résultats backtests, agrégats portefeuille

BEGIN;

-- 1) Supprimer les tables calculées liées aux scénarios / games
DELETE FROM core_alert;
DELETE FROM core_dailymetric;

-- 2) Supprimer les agrégats portefeuille calculés des backtests
DELETE FROM core_backtestportfoliodaily;
DELETE FROM core_backtestportfoliokpi;

-- 3) Réinitialiser les résultats calculés stockés dans les objets conservés
UPDATE core_backtest
SET
    results = '{}'::jsonb,
    status = 'PENDING',
    error_message = '';

UPDATE core_gamescenario
SET
    today_results = '{}'::jsonb,
    last_run_at = NULL,
    last_run_status = '',
    last_run_message = '';

UPDATE core_scenario
SET
    last_computed_config_hash = '',
    last_full_recompute_at = NULL;

COMMIT;

-- Optionnel: purge des jobs/logs techniques si tu veux repartir visuellement de zéro aussi
-- DELETE FROM core_joblog;
-- DELETE FROM core_processingjob;
