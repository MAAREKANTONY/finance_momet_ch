from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from django.core.exceptions import ValidationError
from django.db import transaction
from django.utils import timezone

from core.models import Backtest, GameScenario, RunConfigurationSnapshot, Scenario, Symbol
from core.services.derived_data import BACKTEST_IMPACT_FIELDS, GAME_IMPACT_FIELDS, SCENARIO_IMPACT_FIELDS


SCENARIO_EXTRA_FIELDS = (
    "name",
    "description",
    "universe_mode",
)
BACKTEST_EXTRA_FIELDS = (
    "name",
    "description",
    "universe_snapshot",
)
GAME_EXTRA_FIELDS = (
    "name",
    "description",
    "email_recipients",
)
GAME_SCENARIO_FIELDS = (
    "a", "b", "c", "d", "e", "vc", "fl",
    "n1", "n2", "n3", "n4",
    "n5", "k2j", "cr", "m_v",
    "n5f3", "crf3", "nampL3", "baseL3", "periodeL3",
    "npente", "slope_threshold", "slope_sell_threshold", "npente_basse", "slope_threshold_basse", "slope_sell_threshold_basse",
    "recent_high_drawdown_lookback_days", "recent_high_drawdown_max_drop_pct",
    "rhd_ok_reactivation_mode", "rhd_ok_rebound_threshold", "rhd_ok_confirmation_days", "rhd_ok_reentry_max_drawdown",
    "nglobal", "presence_threshold_pct",
)
GAME_RUN_FIELDS = (
    "name", "description", "active", "study_days", "tradability_threshold", "email_recipients",
    "capital_total", "capital_per_ticker", "capital_mode",
    "signal_lines", "warmup_days", "close_positions_at_end", "settings",
)


def _normalize(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, list):
        return [_normalize(item) for item in value]
    if isinstance(value, tuple):
        return [_normalize(item) for item in value]
    if isinstance(value, dict):
        return {str(k): _normalize(v) for k, v in sorted(value.items(), key=lambda item: str(item[0]))}
    return value


def _snapshot_fields(instance: Any, field_names: tuple[str, ...]) -> dict[str, Any]:
    return {name: _normalize(getattr(instance, name)) for name in field_names}


def _scenario_symbols_snapshot(scenario: Scenario) -> list[dict[str, Any]]:
    return [
        {"ticker": ticker, "exchange": exchange, "sector": sector, "name": name}
        for ticker, exchange, sector, name in scenario.symbols.order_by("ticker", "exchange").values_list(
            "ticker", "exchange", "sector", "name"
        )
    ]


def _clean_lines(value: Any) -> list[dict[str, Any]]:
    from core.forms import _clean_signal_lines_json

    return _clean_signal_lines_json(value or [])


def _lines_from_snapshot(value: Any) -> list[dict[str, Any]]:
    try:
        return _clean_lines(value)
    except ValidationError:
        if isinstance(value, list):
            return _normalize(value)
        raise


def _snapshot_label(prefix: str, name: str) -> str:
    now = timezone.localtime(timezone.now()).strftime("%Y-%m-%d %H:%M")
    return f"{prefix}: {name} ({now})"[:255]


def _restored_name(name: str) -> str:
    now = timezone.localtime(timezone.now()).strftime("%Y-%m-%d %H:%M")
    return f"Restored {name} {now}"[:120]


def compute_config_hash(kind: str, scenario_snapshot: dict[str, Any], run_snapshot: dict[str, Any]) -> str:
    payload = {"kind": kind, "scenario_snapshot": scenario_snapshot, "run_snapshot": run_snapshot}
    raw = json.dumps(_normalize(payload), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def build_backtest_snapshot_payload(backtest: Backtest) -> tuple[dict[str, Any], dict[str, Any]]:
    scenario = backtest.scenario
    scenario_fields = tuple(dict.fromkeys(SCENARIO_EXTRA_FIELDS + SCENARIO_IMPACT_FIELDS))
    backtest_fields = tuple(dict.fromkeys(BACKTEST_EXTRA_FIELDS + BACKTEST_IMPACT_FIELDS))
    scenario_snapshot = _snapshot_fields(scenario, scenario_fields)
    scenario_snapshot["symbols"] = _scenario_symbols_snapshot(scenario)
    run_snapshot = _snapshot_fields(backtest, backtest_fields)
    run_snapshot["scenario_name"] = scenario.name
    run_snapshot["signal_lines"] = _clean_lines(run_snapshot.get("signal_lines"))
    return scenario_snapshot, run_snapshot


def build_game_snapshot_payload(game_scenario: GameScenario) -> tuple[dict[str, Any], dict[str, Any]]:
    scenario_snapshot = _snapshot_fields(game_scenario, GAME_SCENARIO_FIELDS)
    run_fields = tuple(dict.fromkeys(GAME_EXTRA_FIELDS + GAME_RUN_FIELDS))
    run_snapshot = _snapshot_fields(game_scenario, run_fields)
    run_snapshot["signal_lines"] = _clean_lines(run_snapshot.get("signal_lines"))
    return scenario_snapshot, run_snapshot


def _purge_old_snapshots(kind: str, keep: int = 50) -> None:
    keep_ids = list(
        RunConfigurationSnapshot.objects.filter(kind=kind)
        .order_by("-created_at", "-id")
        .values_list("id", flat=True)[:keep]
    )
    RunConfigurationSnapshot.objects.filter(kind=kind).exclude(id__in=keep_ids).delete()


def capture_backtest_configuration(backtest: Backtest) -> RunConfigurationSnapshot | None:
    kind = RunConfigurationSnapshot.Kind.BACKTEST
    scenario_snapshot, run_snapshot = build_backtest_snapshot_payload(backtest)
    config_hash = compute_config_hash(kind, scenario_snapshot, run_snapshot)
    latest = (
        RunConfigurationSnapshot.objects.filter(
            kind=kind,
            source_scenario=backtest.scenario,
            source_backtest=backtest,
        )
        .order_by("-created_at", "-id")
        .first()
    )
    if latest and latest.config_hash == config_hash:
        return None
    snapshot = RunConfigurationSnapshot.objects.create(
        kind=kind,
        label=_snapshot_label("Backtest", backtest.name),
        config_hash=config_hash,
        scenario_snapshot=scenario_snapshot,
        run_snapshot=run_snapshot,
        source_scenario=backtest.scenario,
        source_backtest=backtest,
    )
    _purge_old_snapshots(kind)
    return snapshot


def capture_game_configuration(game_scenario: GameScenario) -> RunConfigurationSnapshot | None:
    kind = RunConfigurationSnapshot.Kind.GAME
    scenario_snapshot, run_snapshot = build_game_snapshot_payload(game_scenario)
    config_hash = compute_config_hash(kind, scenario_snapshot, run_snapshot)
    latest = (
        RunConfigurationSnapshot.objects.filter(kind=kind, source_game_scenario=game_scenario)
        .order_by("-created_at", "-id")
        .first()
    )
    if latest and latest.config_hash == config_hash:
        return None
    snapshot = RunConfigurationSnapshot.objects.create(
        kind=kind,
        label=_snapshot_label("Game", game_scenario.name),
        config_hash=config_hash,
        scenario_snapshot=scenario_snapshot,
        run_snapshot=run_snapshot,
        source_scenario=game_scenario.engine_scenario,
        source_game_scenario=game_scenario,
    )
    _purge_old_snapshots(kind)
    return snapshot


def _set_symbols_from_snapshot(scenario: Scenario, symbols_snapshot: list[dict[str, Any]]) -> None:
    symbols: list[Symbol] = []
    for entry in symbols_snapshot or []:
        ticker = str((entry or {}).get("ticker") or "").strip()
        exchange = str((entry or {}).get("exchange") or "").strip()
        if not ticker:
            continue
        symbol = Symbol.objects.filter(ticker=ticker, exchange=exchange).first()
        if symbol:
            symbols.append(symbol)
    scenario.symbols.set(symbols)


@transaction.atomic
def restore_backtest_snapshot(snapshot: RunConfigurationSnapshot, *, created_by=None) -> Backtest:
    if snapshot.kind != RunConfigurationSnapshot.Kind.BACKTEST:
        raise ValueError("Snapshot type must be BACKTEST")
    scenario_data = dict(snapshot.scenario_snapshot or {})
    run_data = dict(snapshot.run_snapshot or {})
    symbols_snapshot = scenario_data.pop("symbols", [])
    scenario_fields = tuple(dict.fromkeys(SCENARIO_EXTRA_FIELDS + SCENARIO_IMPACT_FIELDS))
    scenario_kwargs = {field: scenario_data[field] for field in scenario_fields if field in scenario_data}
    scenario_kwargs["name"] = _restored_name(str(scenario_kwargs.get("name") or "Scenario"))
    scenario_kwargs["is_default"] = False
    scenario_kwargs["is_study_clone"] = False
    scenario = Scenario.objects.create(**scenario_kwargs)
    _set_symbols_from_snapshot(scenario, symbols_snapshot)

    backtest_fields = tuple(dict.fromkeys(BACKTEST_EXTRA_FIELDS + BACKTEST_IMPACT_FIELDS))
    backtest_kwargs = {field: run_data[field] for field in backtest_fields if field in run_data and field != "scenario_id"}
    backtest_kwargs["name"] = _restored_name(str(backtest_kwargs.get("name") or "Backtest"))
    backtest_kwargs["scenario"] = scenario
    backtest_kwargs["signal_lines"] = _lines_from_snapshot(backtest_kwargs.get("signal_lines"))
    backtest_kwargs["results"] = {}
    backtest_kwargs["status"] = Backtest.Status.PENDING
    backtest_kwargs["error_message"] = ""
    if created_by is not None:
        backtest_kwargs["created_by"] = created_by
    return Backtest.objects.create(**backtest_kwargs)


@transaction.atomic
def restore_game_snapshot(snapshot: RunConfigurationSnapshot) -> GameScenario:
    if snapshot.kind != RunConfigurationSnapshot.Kind.GAME:
        raise ValueError("Snapshot type must be GAME")
    scenario_data = dict(snapshot.scenario_snapshot or {})
    run_data = dict(snapshot.run_snapshot or {})
    game_kwargs = {field: scenario_data[field] for field in GAME_SCENARIO_FIELDS if field in scenario_data}
    for field in GAME_RUN_FIELDS:
        if field in run_data:
            game_kwargs[field] = run_data[field]
    for field in GAME_EXTRA_FIELDS:
        if field in run_data:
            game_kwargs[field] = run_data[field]
    game_kwargs["name"] = _restored_name(str(game_kwargs.get("name") or "Game"))
    game_kwargs["signal_lines"] = _lines_from_snapshot(game_kwargs.get("signal_lines"))
    game_kwargs["today_results"] = {}
    game_kwargs["last_run_status"] = ""
    game_kwargs["last_run_message"] = ""
    game_kwargs["last_run_at"] = None
    game_kwargs["engine_scenario"] = None
    return GameScenario.objects.create(**game_kwargs)


def snapshot_preview_payload(snapshot: RunConfigurationSnapshot) -> dict[str, Any]:
    return {
        "id": snapshot.id,
        "label": snapshot.label,
        "kind": snapshot.kind,
        "created_at": timezone.localtime(snapshot.created_at).strftime("%Y-%m-%d %H:%M"),
        "scenario_snapshot": snapshot.scenario_snapshot,
        "run_snapshot": snapshot.run_snapshot,
    }
