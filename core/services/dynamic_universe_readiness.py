from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, timedelta
from typing import Any

from django.db.models import Q

from core.models import (
    Backtest,
    Scenario,
    Symbol,
    UniverseCoverageSnapshot,
    UniverseCoverageStatus,
    UniverseDefinition,
    UniverseImportBatch,
    UniverseMembership,
)
from core.services.backtesting.ohlc_readiness import get_missing_ohlc_symbols_for_dynamic_universe
from core.services.trend_filters import market_benchmark_ticker_for_symbol
from core.services.universe_resolver import (
    CSI300_UNIVERSE_CODE,
    ResolvedMembershipInterval,
    SP500_UNIVERSE_CODE,
    universe_code_for_historical_dynamic_mode,
)


CHECK_OK = "OK"
CHECK_WARNING = "WARNING"
CHECK_ERROR = "ERROR"
CHECK_SKIPPED = "SKIPPED"
REPORT_READY = "READY"
REPORT_READY_WITH_WARNINGS = "READY_WITH_WARNINGS"
REPORT_NOT_READY = "NOT_READY"

SECTOR_ETF_TICKERS = (
    "XLK", "XLF", "XLE", "XLV", "XLY", "XLP", "XLI", "XLB", "XLU", "XLRE", "XLC",
)


@dataclass(frozen=True)
class ReadinessAction:
    code: str
    label: str
    command: str = ""

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ReadinessCheck:
    code: str
    label: str
    status: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)
    suggested_actions: list[ReadinessAction] = field(default_factory=list)
    suggested_commands: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["suggested_actions"] = [action.as_dict() for action in self.suggested_actions]
        return payload


@dataclass
class ReadinessReport:
    universe: str
    start: date
    end: date
    ready: bool
    status: str
    checks: list[ReadinessCheck] = field(default_factory=list)
    suggested_actions: list[ReadinessAction] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "universe": self.universe,
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "ready": self.ready,
            "status": self.status,
            "checks": [check.as_dict() for check in self.checks],
            "suggested_actions": [action.as_dict() for action in self.suggested_actions],
            "metadata": self.metadata,
        }


@dataclass(frozen=True)
class _MappedMembership:
    membership: UniverseMembership
    symbol: Symbol


INIT_REFERENCE_DATA_ACTION = ReadinessAction(
    code="init_reference_data",
    label="Initialiser le référentiel minimal",
    command="python manage.py init_reference_data",
)
BOOTSTRAP_SYMBOLS_ACTION = ReadinessAction(
    code="bootstrap_sp500_symbols_from_eodhd",
    label="Créer les Symbols S&P500 historiques manquants",
    command="python manage.py bootstrap_sp500_symbols_from_eodhd --coverage-start {start} --coverage-end {end} --apply",
)
SYNC_MEMBERSHIPS_ACTION = ReadinessAction(
    code="sync_sp500_historical_memberships",
    label="Synchroniser les memberships historiques S&P500",
    command="python manage.py sync_sp500_historical_memberships --coverage-start {start} --coverage-end {end} --apply",
)
IMPORT_MEMBERSHIPS_ACTION = ReadinessAction(
    code="import_sp500_memberships",
    label="Importer les memberships historiques S&P500 depuis CSV",
    command="python manage.py import_sp500_memberships --file PATH --coverage-start {start} --coverage-end {end} --apply",
)
IMPORT_GENERIC_MEMBERSHIPS_ACTION = ReadinessAction(
    code="import_universe_memberships",
    label="Importer les memberships historiques depuis CSV",
    command='python manage.py import_universe_memberships --csv <file.csv> --universe-code CSI300 --universe-name "CSI 300" --apply',
)
PREPARE_OHLC_ACTION = ReadinessAction(
    code="prepare_dynamic_universe_ohlc",
    label="Préparer explicitement les OHLC Dynamic Universe",
    command="Lancer prepare_dynamic_universe_ohlc_job_task depuis l'action opérationnelle dédiée.",
)
SYNC_BENCHMARK_ACTION = ReadinessAction(
    code="sync_benchmark_etfs",
    label="Synchroniser les ETFs benchmark requis",
    command="python manage.py sync_benchmark_etfs",
)
FETCH_DAILY_BARS_ACTION = ReadinessAction(
    code="fetch_daily_bars",
    label="Récupérer les DailyBars manquantes",
    command="python manage.py fetch_daily_bars",
)

SUPPORTED_READINESS_UNIVERSE_CODES = {SP500_UNIVERSE_CODE, CSI300_UNIVERSE_CODE}


def _membership_recovery_actions(universe_code: str) -> list[ReadinessAction]:
    if universe_code == SP500_UNIVERSE_CODE:
        return [SYNC_MEMBERSHIPS_ACTION, IMPORT_MEMBERSHIPS_ACTION]
    return [IMPORT_GENERIC_MEMBERSHIPS_ACTION]


def _symbol_recovery_actions(universe_code: str) -> list[ReadinessAction]:
    if universe_code == SP500_UNIVERSE_CODE:
        return [BOOTSTRAP_SYMBOLS_ACTION]
    return []


def _formatted_action_commands(actions: list[ReadinessAction], coverage_start: date | None, end: date | None) -> list[str]:
    return [_format_action_command(action, coverage_start, end) for action in actions]


def check_dynamic_universe_readiness(
    *,
    universe: str = SP500_UNIVERSE_CODE,
    start: date,
    end: date,
    warmup_days: int = 0,
    require_gm_market: bool = False,
    require_gm_sector: bool = False,
    scenario_id: int | None = None,
    backtest_id: int | None = None,
) -> ReadinessReport:
    universe_code = str(universe or "").strip().upper()
    if end < start:
        raise ValueError("end must be greater than or equal to start.")
    if universe_code not in SUPPORTED_READINESS_UNIVERSE_CODES:
        raise ValueError(f"Unsupported universe={universe_code}. V1 supports SP500 and CSI300.")

    scenario = None
    backtest = None
    if backtest_id is not None:
        backtest = Backtest.objects.select_related("scenario").get(id=backtest_id)
        scenario = backtest.scenario
        start = backtest.start_date or start
        end = backtest.end_date or end
        warmup_days = int(getattr(backtest, "warmup_days", 0) or warmup_days or 0)
    elif scenario_id is not None:
        scenario = Scenario.objects.get(id=scenario_id)

    if scenario is not None:
        scenario_universe_code = universe_code_for_historical_dynamic_mode(
            getattr(scenario, "universe_mode", Scenario.UniverseMode.STATIC_TICKERS)
        )
        if scenario_universe_code:
            universe_code = scenario_universe_code
        inferred_market, inferred_sector = _gm_requirements_from_signal_lines(getattr(scenario, "signal_lines", None))
        require_gm_market = bool(require_gm_market or inferred_market)
        require_gm_sector = bool(require_gm_sector or inferred_sector)

    coverage_start = start - timedelta(days=max(0, int(warmup_days or 0)))
    metadata = {
        "coverage_start": coverage_start.isoformat(),
        "calendar_days_required": (end - coverage_start).days + 1,
        "coverage_policy": "UniverseCoverageSnapshot is expected for every calendar day in coverage_start..end.",
        "scenario_id": scenario.id if scenario is not None else None,
        "backtest_id": backtest.id if backtest is not None else None,
        "require_gm_market": bool(require_gm_market),
        "require_gm_sector": bool(require_gm_sector),
    }

    checks: list[ReadinessCheck] = []
    universe_obj = UniverseDefinition.objects.filter(code=universe_code).first()
    checks.append(_check_universe_definition(universe_code, universe_obj))

    memberships = list(_membership_qs(universe_obj, coverage_start, end)) if universe_obj else []
    checks.append(_check_memberships(universe_code, universe_obj, memberships, coverage_start, end))
    checks.append(_check_import_batch(universe_code, universe_obj, coverage_start, end))

    coverage_check = _check_coverage_snapshots(universe_code, universe_obj, coverage_start, end)
    checks.append(coverage_check)
    coverage_ready = coverage_check.status == CHECK_OK

    mapping_check, mapped_memberships = _check_historical_symbols(universe_code, universe_obj, memberships, coverage_ready)
    checks.append(mapping_check)
    mappings_ready = mapping_check.status == CHECK_OK

    membership_by_ticker = _membership_by_ticker(mapped_memberships) if memberships and mappings_ready else {}
    symbols = _unique_symbols(mapped_memberships) if mappings_ready else []
    checks.append(
        _check_member_daily_bars(
            symbols=symbols,
            membership_by_ticker=membership_by_ticker,
            coverage_start=coverage_start,
            end=end,
            prerequisites_ready=bool(memberships and coverage_ready and mappings_ready),
        )
    )

    if require_gm_market:
        checks.append(_check_gm_market_daily_bars(universe_code, symbols, coverage_start, end, prerequisites_ready=mappings_ready))
    else:
        checks.append(_skipped_check("gm_market_daily_bars", "DailyBars GM_market", "GM_market non demandé."))

    if require_gm_sector:
        checks.append(_check_gm_sector_daily_bars(universe_code, coverage_start, end))
    else:
        checks.append(_skipped_check("gm_sector_daily_bars", "DailyBars GM_sector", "GM_sector non demandé."))

    suggested_actions = _dedupe_actions(
        action
        for check in checks
        for action in check.suggested_actions
    )
    has_error = any(check.status == CHECK_ERROR for check in checks)
    has_warning = any(check.status == CHECK_WARNING for check in checks)
    ready = not has_error
    status = REPORT_NOT_READY if has_error else (REPORT_READY_WITH_WARNINGS if has_warning else REPORT_READY)
    return ReadinessReport(
        universe=universe_code,
        start=start,
        end=end,
        ready=ready,
        status=status,
        checks=checks,
        suggested_actions=suggested_actions,
        metadata=metadata,
    )


def _check_universe_definition(universe_code: str, universe: UniverseDefinition | None) -> ReadinessCheck:
    if universe is None:
        return ReadinessCheck(
            code="universe_definition",
            label=f"Référentiel {universe_code}",
            status=CHECK_ERROR,
            message=f"UniverseDefinition {universe_code} est absent.",
            suggested_actions=_membership_recovery_actions(universe_code) if universe_code != SP500_UNIVERSE_CODE else [INIT_REFERENCE_DATA_ACTION],
            suggested_commands=(
                _formatted_action_commands(_membership_recovery_actions(universe_code), None, None)
                if universe_code != SP500_UNIVERSE_CODE
                else [INIT_REFERENCE_DATA_ACTION.command]
            ),
        )
    if not universe.active:
        return ReadinessCheck(
            code="universe_definition",
            label=f"Référentiel {universe_code}",
            status=CHECK_ERROR,
            message=f"UniverseDefinition {universe_code} existe mais est inactif.",
            details={"universe_id": universe.id},
            suggested_actions=[INIT_REFERENCE_DATA_ACTION],
            suggested_commands=[INIT_REFERENCE_DATA_ACTION.command],
        )
    return ReadinessCheck(
        code="universe_definition",
        label=f"Référentiel {universe_code}",
        status=CHECK_OK,
        message=f"Référentiel {universe_code} actif.",
        details={"universe_id": universe.id, "source": universe.source},
    )


def _membership_qs(universe: UniverseDefinition | None, coverage_start: date, end: date):
    if universe is None:
        return UniverseMembership.objects.none()
    return (
        UniverseMembership.objects.filter(universe=universe, valid_from__lte=end)
        .filter(Q(valid_to__isnull=True) | Q(valid_to__gte=coverage_start))
        .select_related("symbol", "universe")
        .order_by("ticker", "exchange", "valid_from")
    )


def _check_memberships(
    universe_code: str,
    universe: UniverseDefinition | None,
    memberships: list[UniverseMembership],
    coverage_start: date,
    end: date,
) -> ReadinessCheck:
    if universe is None:
        return _skipped_check(
            "memberships",
            "Memberships historiques",
            f"Impossible d'évaluer les memberships tant que le référentiel {universe_code} est absent.",
            actions=_membership_recovery_actions(universe_code),
        )
    if not memberships:
        return ReadinessCheck(
            code="memberships",
            label="Memberships historiques",
            status=CHECK_ERROR,
            message=f"Aucun UniverseMembership ne recouvre {coverage_start.isoformat()}..{end.isoformat()}.",
            suggested_actions=_membership_recovery_actions(universe_code),
            suggested_commands=_formatted_action_commands(_membership_recovery_actions(universe_code), coverage_start, end),
        )
    active_missing_days = _active_membership_missing_days(memberships, coverage_start, end)
    if active_missing_days:
        examples = [day.isoformat() for day in active_missing_days[:10]]
        return ReadinessCheck(
            code="memberships",
            label="Memberships historiques",
            status=CHECK_ERROR,
            message=f"Memberships partiels: aucun membre actif sur {examples[0]}.",
            details={
                "membership_count": len(memberships),
                "missing_active_member_days": len(active_missing_days),
                "examples": examples,
            },
            suggested_actions=_membership_recovery_actions(universe_code),
            suggested_commands=_formatted_action_commands(_membership_recovery_actions(universe_code), coverage_start, end),
        )
    return ReadinessCheck(
        code="memberships",
        label="Memberships historiques",
        status=CHECK_OK,
        message=f"{len(memberships)} memberships recouvrent la période.",
        details={"membership_count": len(memberships)},
    )


def _check_import_batch(universe_code: str, universe: UniverseDefinition | None, coverage_start: date, end: date) -> ReadinessCheck:
    if universe is None:
        return _skipped_check("import_batch", "Batch d'import", "Référentiel absent.")
    batches = list(
        UniverseImportBatch.objects.filter(
            universe=universe,
            period_start__lte=end,
            period_end__gte=coverage_start,
        ).order_by("-period_start", "-id")
    )
    validated = [
        batch
        for batch in batches
        if batch.status == UniverseCoverageStatus.VALIDATED
        and batch.period_start <= coverage_start
        and batch.period_end >= end
    ]
    if validated:
        batch = validated[0]
        return ReadinessCheck(
            code="import_batch",
            label="Batch d'import validé",
            status=CHECK_OK,
            message=f"UniverseImportBatch VALIDATED couvre {coverage_start.isoformat()}..{end.isoformat()}.",
            details={"batch_id": batch.id, "status": batch.status},
        )
    if batches:
        return ReadinessCheck(
            code="import_batch",
            label="Batch d'import validé",
            status=CHECK_ERROR,
            message="Aucun UniverseImportBatch VALIDATED ne couvre toute la période demandée.",
            details={
                "batches": [
                    {
                        "id": batch.id,
                        "status": batch.status,
                        "period_start": batch.period_start.isoformat(),
                        "period_end": batch.period_end.isoformat(),
                    }
                    for batch in batches[:10]
                ],
            },
            suggested_actions=_membership_recovery_actions(universe_code),
            suggested_commands=_formatted_action_commands(_membership_recovery_actions(universe_code), coverage_start, end),
        )
    return ReadinessCheck(
        code="import_batch",
        label="Batch d'import validé",
        status=CHECK_ERROR,
        message="Aucun UniverseImportBatch trouvé pour la période demandée.",
        suggested_actions=_membership_recovery_actions(universe_code),
        suggested_commands=_formatted_action_commands(_membership_recovery_actions(universe_code), coverage_start, end),
    )


def _check_coverage_snapshots(universe_code: str, universe: UniverseDefinition | None, coverage_start: date, end: date) -> ReadinessCheck:
    if universe is None:
        return _skipped_check("coverage_snapshots", "Coverage snapshots", "Référentiel absent.")

    snapshots = {
        snapshot.coverage_date: snapshot
        for snapshot in UniverseCoverageSnapshot.objects.filter(
            universe=universe,
            coverage_date__gte=coverage_start,
            coverage_date__lte=end,
        ).select_related("import_batch")
    }
    missing = []
    invalid = []
    current = coverage_start
    while current <= end:
        snapshot = snapshots.get(current)
        if snapshot is None:
            missing.append(current)
        else:
            reason = _snapshot_invalid_reason(snapshot)
            if reason:
                invalid.append((current, snapshot, reason))
        current += timedelta(days=1)

    if missing:
        first = missing[0].isoformat()
        return ReadinessCheck(
            code="coverage_snapshots",
            label="Coverage snapshots",
            status=CHECK_ERROR,
            message=f"Coverage non validée: missing coverage snapshot for {first}.",
            details={
                "expected_calendar_days": (end - coverage_start).days + 1,
                "found_snapshots": len(snapshots),
                "missing_count": len(missing),
                "missing_examples": [day.isoformat() for day in missing[:10]],
                "policy": "La logique actuelle exige un UniverseCoverageSnapshot par jour calendaire.",
            },
            suggested_actions=_membership_recovery_actions(universe_code),
            suggested_commands=_formatted_action_commands(_membership_recovery_actions(universe_code), coverage_start, end),
        )
    if invalid:
        day, snapshot, reason = invalid[0]
        return ReadinessCheck(
            code="coverage_snapshots",
            label="Coverage snapshots",
            status=CHECK_ERROR,
            message=f"Coverage non validée: {day.isoformat()} {reason}.",
            details={
                "invalid_count": len(invalid),
                "invalid_examples": [
                    {
                        "date": item_day.isoformat(),
                        "snapshot_status": item_snapshot.status,
                        "batch_status": item_snapshot.import_batch.status,
                        "actual_member_count": item_snapshot.actual_member_count,
                        "expected_member_count": item_snapshot.expected_member_count,
                        "mapped_member_count": item_snapshot.mapped_member_count,
                        "unmapped_member_count": item_snapshot.unmapped_member_count,
                        "reason": item_reason,
                    }
                    for item_day, item_snapshot, item_reason in invalid[:10]
                ],
            },
            suggested_actions=_membership_recovery_actions(universe_code),
            suggested_commands=_formatted_action_commands(_membership_recovery_actions(universe_code), coverage_start, end),
        )
    return ReadinessCheck(
        code="coverage_snapshots",
        label="Coverage snapshots",
        status=CHECK_OK,
        message=f"{len(snapshots)} coverage snapshots validés.",
        details={"validated_snapshots": len(snapshots)},
    )


def _check_historical_symbols(
    universe_code: str,
    universe: UniverseDefinition | None,
    memberships: list[UniverseMembership],
    coverage_ready: bool,
) -> tuple[ReadinessCheck, list[_MappedMembership]]:
    if universe is None:
        return (
            _skipped_check(
                "historical_symbols",
                "Symbols historiques",
                "Impossible d'évaluer les symbols historiques tant que le référentiel est absent.",
            ),
            [],
        )
    if not memberships:
        return (
            _skipped_check(
                "historical_symbols",
                "Symbols historiques",
                "Impossible d'évaluer les symbols historiques tant que les memberships ne sont pas disponibles.",
                actions=_symbol_recovery_actions(universe_code),
            ),
            [],
        )
    if not coverage_ready:
        return (
            _skipped_check(
                "historical_symbols",
                "Symbols historiques",
                "Impossible d'évaluer les symbols historiques tant que la coverage n'est pas validée.",
                actions=_symbol_recovery_actions(universe_code),
            ),
            [],
        )

    mapped: list[_MappedMembership] = []
    missing = []
    ambiguous = []
    for membership in memberships:
        symbol, error = _resolve_membership_symbol_read_only(membership)
        if symbol is not None:
            mapped.append(_MappedMembership(membership=membership, symbol=symbol))
        elif error == "missing":
            missing.append(_membership_label(membership))
        else:
            ambiguous.append(_membership_label(membership))

    if missing or ambiguous:
        return (
            ReadinessCheck(
                code="historical_symbols",
                label="Symbols historiques",
                status=CHECK_ERROR,
                message=f"Mappings symbols incomplets: missing={len(missing)} ambiguous={len(ambiguous)}.",
                details={
                    "mapped": len(mapped),
                    "missing": len(missing),
                    "ambiguous": len(ambiguous),
                    "missing_examples": missing[:10],
                    "ambiguous_examples": ambiguous[:10],
                },
                suggested_actions=_symbol_recovery_actions(universe_code),
                suggested_commands=_formatted_action_commands(_symbol_recovery_actions(universe_code), None, None),
            ),
            mapped,
        )
    return (
        ReadinessCheck(
            code="historical_symbols",
            label="Symbols historiques",
            status=CHECK_OK,
            message=f"{len(mapped)} memberships sont mappés vers des Symbols.",
            details={"mapped_memberships": len(mapped), "symbol_count": len(_unique_symbols(mapped))},
        ),
        mapped,
    )


def _check_member_daily_bars(
    *,
    symbols: list[Symbol],
    membership_by_ticker: dict[str, tuple[ResolvedMembershipInterval, ...]],
    coverage_start: date,
    end: date,
    prerequisites_ready: bool,
) -> ReadinessCheck:
    if not prerequisites_ready:
        return _skipped_check(
            "member_daily_bars",
            "DailyBars membres",
            "Impossible de vérifier les DailyBars membres tant que l'univers historique n'est pas résolu.",
            actions=[PREPARE_OHLC_ACTION],
        )
    missing = get_missing_ohlc_symbols_for_dynamic_universe(
        symbols=symbols,
        start_date=coverage_start,
        end_date=end,
        membership_by_ticker=membership_by_ticker,
    )
    if missing:
        return ReadinessCheck(
            code="member_daily_bars",
            label="DailyBars membres",
            status=CHECK_WARNING,
            message=f"Prix manquants pour {len(missing)} actions historiques.",
            details={
                "expected_symbols": len(symbols),
                "ready_symbols": len(symbols) - len(missing),
                "missing_symbols": len(missing),
                "missing_examples": [symbol.ticker for symbol in missing[:20]],
            },
            suggested_actions=[PREPARE_OHLC_ACTION],
            suggested_commands=[PREPARE_OHLC_ACTION.command],
        )
    return ReadinessCheck(
        code="member_daily_bars",
        label="DailyBars membres",
        status=CHECK_OK,
        message=f"DailyBars membres prêtes pour {len(symbols)} symbols.",
        details={"expected_symbols": len(symbols), "missing_symbols": 0},
    )


def _check_gm_market_daily_bars(
    universe_code: str,
    symbols: list[Symbol],
    coverage_start: date,
    end: date,
    *,
    prerequisites_ready: bool,
) -> ReadinessCheck:
    if universe_code != SP500_UNIVERSE_CODE:
        return ReadinessCheck(
            code="gm_market_daily_bars",
            label="DailyBars GM_market",
            status=CHECK_ERROR,
            message=f"GM market non supporté pour {universe_code} V1 sans benchmark {universe_code} explicite.",
        )
    if not prerequisites_ready:
        return _skipped_check(
            "gm_market_daily_bars",
            "DailyBars GM_market",
            "Impossible de vérifier GM_market tant que les symbols historiques ne sont pas résolus.",
            actions=[SYNC_BENCHMARK_ACTION, FETCH_DAILY_BARS_ACTION],
        )
    tickers = sorted({ticker for ticker in (market_benchmark_ticker_for_symbol(symbol) for symbol in symbols) if ticker})
    if not tickers:
        tickers = ["SPY"]
    return _check_benchmark_daily_bars(
        code="gm_market_daily_bars",
        label="DailyBars GM_market",
        tickers=tickers,
        coverage_start=coverage_start,
        end=end,
    )


def _check_gm_sector_daily_bars(universe_code: str, coverage_start: date, end: date) -> ReadinessCheck:
    if universe_code != SP500_UNIVERSE_CODE:
        return ReadinessCheck(
            code="gm_sector_daily_bars",
            label="DailyBars GM_sector",
            status=CHECK_ERROR,
            message=f"GM sectoriel non supporté pour {universe_code} V1 : les benchmarks sectoriels actuels sont US.",
        )
    return _check_benchmark_daily_bars(
        code="gm_sector_daily_bars",
        label="DailyBars GM_sector",
        tickers=list(SECTOR_ETF_TICKERS),
        coverage_start=coverage_start,
        end=end,
    )


def _check_benchmark_daily_bars(
    *,
    code: str,
    label: str,
    tickers: list[str],
    coverage_start: date,
    end: date,
) -> ReadinessCheck:
    symbols_by_ticker = {}
    for symbol in Symbol.objects.filter(ticker__in=tickers).order_by("-active", "ticker", "id"):
        symbols_by_ticker.setdefault(symbol.ticker, symbol)

    missing_symbols = [ticker for ticker in tickers if ticker not in symbols_by_ticker]
    existing_symbols = [symbols_by_ticker[ticker] for ticker in tickers if ticker in symbols_by_ticker]
    missing_ohlc = get_missing_ohlc_symbols_for_dynamic_universe(
        symbols=existing_symbols,
        start_date=coverage_start,
        end_date=end,
    )
    if missing_symbols or missing_ohlc:
        return ReadinessCheck(
            code=code,
            label=label,
            status=CHECK_ERROR,
            message=f"{label} incomplet: symbols_absents={len(missing_symbols)} ohlc_manquants={len(missing_ohlc)}.",
            details={
                "required_tickers": tickers,
                "missing_symbols": missing_symbols,
                "missing_ohlc": [symbol.ticker for symbol in missing_ohlc],
            },
            suggested_actions=[SYNC_BENCHMARK_ACTION, FETCH_DAILY_BARS_ACTION],
            suggested_commands=[SYNC_BENCHMARK_ACTION.command, FETCH_DAILY_BARS_ACTION.command],
        )
    return ReadinessCheck(
        code=code,
        label=label,
        status=CHECK_OK,
        message=f"{label} prêt pour {len(tickers)} benchmarks.",
        details={"required_tickers": tickers, "missing_symbols": [], "missing_ohlc": []},
    )


def _snapshot_invalid_reason(snapshot: UniverseCoverageSnapshot) -> str:
    if snapshot.status != UniverseCoverageStatus.VALIDATED:
        return f"snapshot_status={snapshot.status}"
    if snapshot.import_batch.status != UniverseCoverageStatus.VALIDATED:
        return f"batch_status={snapshot.import_batch.status}"
    if snapshot.actual_member_count < snapshot.expected_member_count:
        return f"actual_member_count={snapshot.actual_member_count} expected_member_count={snapshot.expected_member_count}"
    if snapshot.mapped_member_count < snapshot.actual_member_count:
        return f"mapped_member_count={snapshot.mapped_member_count}"
    if snapshot.unmapped_member_count != 0:
        return f"unmapped_member_count={snapshot.unmapped_member_count}"
    return ""


def _resolve_membership_symbol_read_only(membership: UniverseMembership) -> tuple[Symbol | None, str]:
    if membership.symbol_id:
        return membership.symbol, ""
    qs = Symbol.objects.filter(ticker=membership.ticker)
    if membership.exchange:
        qs = qs.filter(exchange=membership.exchange)
    count = qs.count()
    if count == 1:
        return qs.get(), ""
    if count == 0:
        return None, "missing"
    return None, "ambiguous"


def _active_membership_missing_days(memberships: list[UniverseMembership], coverage_start: date, end: date) -> list[date]:
    missing = []
    current = coverage_start
    while current <= end:
        if not any(
            membership.valid_from <= current and (membership.valid_to is None or current <= membership.valid_to)
            for membership in memberships
        ):
            missing.append(current)
        current += timedelta(days=1)
    return missing


def _membership_by_ticker(mapped_memberships: list[_MappedMembership]) -> dict[str, tuple[ResolvedMembershipInterval, ...]]:
    intervals: dict[str, list[ResolvedMembershipInterval]] = {}
    for item in mapped_memberships:
        membership = item.membership
        symbol = item.symbol
        intervals.setdefault(membership.ticker, []).append(
            ResolvedMembershipInterval(
                ticker=membership.ticker,
                exchange=membership.exchange,
                symbol_id=symbol.id,
                valid_from=membership.valid_from,
                valid_to=membership.valid_to,
                provider_symbol=membership.provider_symbol,
                source=membership.source,
            )
        )
    return {ticker: tuple(values) for ticker, values in sorted(intervals.items())}


def _unique_symbols(mapped_memberships: list[_MappedMembership]) -> list[Symbol]:
    symbols_by_id = {item.symbol.id: item.symbol for item in mapped_memberships}
    return sorted(symbols_by_id.values(), key=lambda symbol: (symbol.ticker, symbol.exchange, symbol.id))


def _gm_requirements_from_signal_lines(signal_lines: Any) -> tuple[bool, bool]:
    require_market = False
    require_sector = False
    if not isinstance(signal_lines, list):
        return False, False
    for line in signal_lines:
        if not isinstance(line, dict):
            continue
        require_market = require_market or _direct_gm_active(line.get("buy_market_gm_market"))
        require_sector = require_sector or _direct_gm_active(line.get("buy_market_gm_sector"))
        for config_key in (
            "gm_buy_conditions",
            "gm_sell_market_exit_conditions",
            "gm_push_buy_conditions",
            "gm_push_sell_market_exit_conditions",
        ):
            config = line.get(config_key)
            if not isinstance(config, dict):
                continue
            require_market = require_market or _direct_gm_active(_family_mode(config, "market"))
            require_sector = require_sector or _direct_gm_active(_family_mode(config, "sector"))
    return require_market, require_sector


def _family_mode(config: dict[str, Any], family: str) -> Any:
    entry = config.get(family)
    if isinstance(entry, dict):
        return entry.get("mode") or entry.get("direction") or entry.get("code")
    return entry


def _direct_gm_active(value: Any) -> bool:
    code = str(value or "IGNORE").strip().upper()
    if code.startswith("GM_"):
        code = code[3:]
    return code not in {"", "IGNORE"}


def _membership_label(membership: UniverseMembership) -> str:
    return f"{membership.ticker}{(':' + membership.exchange) if membership.exchange else ''}"


def _skipped_check(
    code: str,
    label: str,
    message: str,
    *,
    actions: list[ReadinessAction] | None = None,
) -> ReadinessCheck:
    actions = actions or []
    return ReadinessCheck(
        code=code,
        label=label,
        status=CHECK_SKIPPED,
        message=message,
        suggested_actions=actions,
        suggested_commands=[action.command for action in actions if action.command],
    )


def _format_action_command(action: ReadinessAction, start: date | None, end: date | None) -> str:
    if start is None or end is None:
        return action.command
    return action.command.format(start=start.isoformat(), end=end.isoformat())


def _dedupe_actions(actions) -> list[ReadinessAction]:
    out: list[ReadinessAction] = []
    seen = set()
    for action in actions:
        if action.code in seen:
            continue
        seen.add(action.code)
        out.append(action)
    return out
