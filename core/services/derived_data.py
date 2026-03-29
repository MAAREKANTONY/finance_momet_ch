from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Iterable

from django.db import transaction

from core.models import (
    Alert,
    Backtest,
    BacktestPortfolioDaily,
    BacktestPortfolioKPI,
    DailyMetric,
    GameScenario,
    Scenario,
)


SCENARIO_IMPACT_FIELDS = (
    "a", "b", "c", "d", "e",
    "vc", "fl",
    "n1", "n2", "n3", "n4",
    "n5", "k2j", "cr",
    "n5f3", "crf3", "nampL3", "baseL3", "periodeL3",
    "npente", "slope_threshold", "npente_basse", "slope_threshold_basse",
    "nglobal", "m_v", "history_years", "active",
)

BACKTEST_IMPACT_FIELDS = (
    "scenario_id",
    "start_date", "end_date",
    "capital_total", "capital_per_ticker", "capital_mode",
    "ratio_threshold", "include_all_tickers",
    "signal_lines", "warmup_days", "close_positions_at_end", "settings",
)

GAME_IMPACT_FIELDS = (
    "study_days", "active",
    "tradability_threshold", "npente", "slope_threshold", "npente_basse", "slope_threshold_basse",
    "nglobal", "presence_threshold_pct",
    "a", "b", "c", "d", "e", "vc", "fl",
    "n1", "n2", "n3", "n4",
    "n5", "k2j", "cr", "m_v",
    "n5f3", "crf3", "nampL3", "baseL3", "periodeL3",
    "capital_total", "capital_per_ticker", "capital_mode",
    "signal_lines", "warmup_days", "close_positions_at_end", "settings",
)


def _normalize(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, list):
        return [_normalize(v) for v in value]
    if isinstance(value, tuple):
        return tuple(_normalize(v) for v in value)
    if isinstance(value, dict):
        return {str(k): _normalize(v) for k, v in sorted(value.items(), key=lambda kv: str(kv[0]))}
    return value


def snapshot_fields(instance: Any, field_names: Iterable[str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for name in field_names:
        out[name] = _normalize(getattr(instance, name))
    return out


def changed_fields(before: dict[str, Any], after: dict[str, Any]) -> list[str]:
    keys = set(before.keys()) | set(after.keys())
    diff: list[str] = []
    for key in sorted(keys):
        if before.get(key) != after.get(key):
            diff.append(key)
    return diff


def scenario_impactful_changes(*, instance: Scenario, cleaned_data: dict[str, Any], old_symbol_ids: set[int] | None = None) -> list[str]:
    before = snapshot_fields(instance, SCENARIO_IMPACT_FIELDS)
    after = {name: _normalize(cleaned_data.get(name, getattr(instance, name))) for name in SCENARIO_IMPACT_FIELDS}
    diff = changed_fields(before, after)

    if old_symbol_ids is not None and "symbols" in cleaned_data:
        try:
            new_symbol_ids = {int(obj.pk) for obj in (cleaned_data.get("symbols") or [])}
        except Exception:
            new_symbol_ids = set(old_symbol_ids)
        if new_symbol_ids != old_symbol_ids:
            diff.append("symbols")
    return diff


def backtest_impactful_changes(*, instance: Backtest, cleaned_data: dict[str, Any]) -> list[str]:
    before = snapshot_fields(instance, BACKTEST_IMPACT_FIELDS)
    after: dict[str, Any] = {}
    for name in BACKTEST_IMPACT_FIELDS:
        val = cleaned_data.get(name, getattr(instance, name))
        if name == "scenario_id" and "scenario" in cleaned_data:
            scenario = cleaned_data.get("scenario")
            val = getattr(scenario, "id", None)
        after[name] = _normalize(val)
    return changed_fields(before, after)


def game_impactful_changes(*, instance: GameScenario, cleaned_data: dict[str, Any]) -> list[str]:
    before = snapshot_fields(instance, GAME_IMPACT_FIELDS)
    after = {name: _normalize(cleaned_data.get(name, getattr(instance, name))) for name in GAME_IMPACT_FIELDS}
    return changed_fields(before, after)


@transaction.atomic
def purge_backtest_derived_data(backtest: Backtest) -> None:
    BacktestPortfolioDaily.objects.filter(backtest=backtest).delete()
    BacktestPortfolioKPI.objects.filter(backtest=backtest).delete()
    Backtest.objects.filter(pk=backtest.pk).update(
        results={},
        status=Backtest.Status.PENDING,
        error_message="",
    )
    backtest.results = {}
    backtest.status = Backtest.Status.PENDING
    backtest.error_message = ""


@transaction.atomic
def purge_scenario_derived_data(scenario: Scenario, *, reset_backtests: bool = True) -> None:
    DailyMetric.objects.filter(scenario=scenario).delete()
    Alert.objects.filter(scenario=scenario).delete()
    Scenario.objects.filter(pk=scenario.pk).update(
        last_computed_config_hash="",
        last_full_recompute_at=None,
    )
    scenario.last_computed_config_hash = ""
    scenario.last_full_recompute_at = None

    if reset_backtests:
        for bt in Backtest.objects.filter(scenario=scenario).only("id", "status", "results", "error_message"):
            purge_backtest_derived_data(bt)


@transaction.atomic
def purge_game_derived_data(game: GameScenario) -> None:
    if game.engine_scenario_id:
        purge_scenario_derived_data(game.engine_scenario, reset_backtests=False)

    GameScenario.objects.filter(pk=game.pk).update(
        today_results={},
        last_run_at=None,
        last_run_status="",
        last_run_message="",
    )
    game.today_results = {}
    game.last_run_at = None
    game.last_run_status = ""
    game.last_run_message = ""
