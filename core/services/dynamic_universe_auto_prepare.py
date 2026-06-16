from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

from core.models import Scenario
from core.services.provider_eodhd import EODHDError
from core.services.sp500_symbol_bootstrap import bootstrap_sp500_symbols_from_eodhd
from core.services.universe_eodhd_sync import sync_sp500_historical_memberships_from_eodhd
from core.services.universe_import import UniverseImportError
from core.services.universe_resolver import ResolvedUniverse, UniverseResolver, UniverseResolverError


DYNAMIC_UNIVERSE_AUTO_PREPARE_USER_ERROR = (
    "Impossible de préparer automatiquement l’historique S&P 500 pour cette période. "
    "Certains symboles ne sont pas disponibles ou la couverture n’est pas validée. "
    "Vérifiez la synchronisation S&P 500 dans l’administration."
)


class DynamicUniverseAutoPrepareError(RuntimeError):
    def __init__(self, message: str, *, technical_detail: str = ""):
        super().__init__(message)
        self.technical_detail = technical_detail


@dataclass
class DynamicUniverseAutoPrepareResult:
    already_ready: bool
    bootstrap_created: int = 0
    sync_status: str = ""
    warnings: list[str] = field(default_factory=list)
    resolved_universe: ResolvedUniverse | None = None
    technical_detail: str = ""


def ensure_sp500_historical_universe_ready(
    *,
    scenario: Scenario,
    start_date: date,
    end_date: date,
    warmup_start_date: date | None = None,
    allow_provider_sync: bool = True,
) -> DynamicUniverseAutoPrepareResult:
    if getattr(scenario, "universe_mode", Scenario.UniverseMode.STATIC_TICKERS) != Scenario.UniverseMode.SP500_HISTORICAL_DYNAMIC:
        return DynamicUniverseAutoPrepareResult(already_ready=True)

    coverage_start = warmup_start_date or start_date
    resolver = UniverseResolver()
    try:
        resolved = resolver.resolve(
            scenario=scenario,
            start_date=start_date,
            end_date=end_date,
            warmup_start_date=warmup_start_date,
        )
        return DynamicUniverseAutoPrepareResult(already_ready=True, resolved_universe=resolved)
    except UniverseResolverError as initial_exc:
        if not allow_provider_sync:
            _raise_user_error(initial_exc)
        initial_detail = str(initial_exc)

    try:
        bootstrap_result = bootstrap_sp500_symbols_from_eodhd(
            coverage_start=coverage_start,
            coverage_end=end_date,
            dry_run=False,
        )
        sync_result = sync_sp500_historical_memberships_from_eodhd(
            coverage_start=coverage_start,
            coverage_end=end_date,
            dry_run=False,
        )
    except (EODHDError, UniverseImportError) as exc:
        raise DynamicUniverseAutoPrepareError(
            DYNAMIC_UNIVERSE_AUTO_PREPARE_USER_ERROR,
            technical_detail=str(exc),
        ) from exc

    warnings = [
        *(getattr(bootstrap_result, "warnings", None) or []),
        *(getattr(sync_result, "warnings", None) or []),
    ]
    try:
        resolved = resolver.resolve(
            scenario=scenario,
            start_date=start_date,
            end_date=end_date,
            warmup_start_date=warmup_start_date,
        )
    except UniverseResolverError as exc:
        detail = _sync_failure_detail(
            initial_detail=initial_detail,
            final_detail=str(exc),
            sync_result=sync_result,
        )
        raise DynamicUniverseAutoPrepareError(
            DYNAMIC_UNIVERSE_AUTO_PREPARE_USER_ERROR,
            technical_detail=detail,
        ) from exc

    return DynamicUniverseAutoPrepareResult(
        already_ready=False,
        bootstrap_created=int(getattr(bootstrap_result, "created", 0) or 0),
        sync_status=str(getattr(sync_result, "status", "") or ""),
        warnings=warnings,
        resolved_universe=resolved,
        technical_detail=initial_detail,
    )


def _raise_user_error(exc: Exception) -> None:
    raise DynamicUniverseAutoPrepareError(
        DYNAMIC_UNIVERSE_AUTO_PREPARE_USER_ERROR,
        technical_detail=str(exc),
    ) from exc


def _sync_failure_detail(*, initial_detail: str, final_detail: str, sync_result: Any) -> str:
    parts = [
        f"initial={initial_detail}",
        f"final={final_detail}",
        f"sync_status={getattr(sync_result, 'status', '')}",
        f"mapped={getattr(sync_result, 'mapped_member_count', '')}",
        f"unmapped={getattr(sync_result, 'unmapped_member_count', '')}",
    ]
    return " ".join(part for part in parts if part and not part.endswith("="))
