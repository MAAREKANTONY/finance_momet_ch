from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

from django.db.models import Q

from core.models import (
    Scenario,
    Symbol,
    UniverseCoverageSnapshot,
    UniverseCoverageStatus,
    UniverseDefinition,
    UniverseMembership,
)

SP500_UNIVERSE_CODE = "SP500"
CSI300_UNIVERSE_CODE = "CSI300"

HISTORICAL_DYNAMIC_UNIVERSE_CODES = {
    Scenario.UniverseMode.SP500_HISTORICAL_DYNAMIC: SP500_UNIVERSE_CODE,
    Scenario.UniverseMode.CSI300_HISTORICAL_DYNAMIC: CSI300_UNIVERSE_CODE,
}

HISTORICAL_DYNAMIC_UNIVERSE_LABELS = {
    Scenario.UniverseMode.SP500_HISTORICAL_DYNAMIC: "S&P500 historique dynamique",
    Scenario.UniverseMode.CSI300_HISTORICAL_DYNAMIC: "CSI 300 historique dynamique — via CSV",
}


def _normalize_universe_mode(mode: str) -> str:
    return str(mode or "").strip()


def is_historical_dynamic_universe_mode(mode: str) -> bool:
    return _normalize_universe_mode(mode) in HISTORICAL_DYNAMIC_UNIVERSE_CODES


def universe_code_for_historical_dynamic_mode(mode: str) -> str | None:
    return HISTORICAL_DYNAMIC_UNIVERSE_CODES.get(_normalize_universe_mode(mode))


def label_for_historical_dynamic_mode(mode: str) -> str:
    normalized = _normalize_universe_mode(mode)
    return HISTORICAL_DYNAMIC_UNIVERSE_LABELS.get(normalized, normalized or "Univers historique dynamique")


def historical_dynamic_mode_for_universe_code(universe_code: str) -> str | None:
    normalized_code = str(universe_code or "").strip().upper()
    for mode, code in HISTORICAL_DYNAMIC_UNIVERSE_CODES.items():
        if code == normalized_code:
            return mode
    return None

class UniverseResolverError(Exception):
    pass


class UniverseConfigurationError(UniverseResolverError):
    pass


class UniverseCoverageError(UniverseResolverError):
    pass


class UniverseMappingError(UniverseResolverError):
    pass


@dataclass(frozen=True)
class ResolvedMembershipInterval:
    ticker: str
    exchange: str
    symbol_id: int
    valid_from: date
    valid_to: date | None
    provider_symbol: str
    source: str


@dataclass(frozen=True)
class ResolvedUniverse:
    mode: str
    universe_code: str | None
    start_date: date
    end_date: date
    coverage_start: date
    coverage_end: date
    tickers: tuple[str, ...]
    symbols: tuple[Symbol, ...]
    active_by_date: dict[date, frozenset[str]]
    membership_by_ticker: dict[str, tuple[ResolvedMembershipInterval, ...]]
    metadata: dict


def _iter_calendar_days(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _effective_start(start_date: date, warmup_start_date: date | None) -> date:
    return warmup_start_date or start_date


def _resolve_membership_symbol(membership: UniverseMembership) -> Symbol:
    if membership.symbol_id:
        return membership.symbol

    qs = Symbol.objects.filter(ticker=membership.ticker)
    if membership.exchange:
        qs = qs.filter(exchange=membership.exchange)
    count = qs.count()
    if count == 1:
        return qs.get()
    if count == 0:
        exchange = f":{membership.exchange}" if membership.exchange else ""
        raise UniverseMappingError(
            f"Ticker {membership.ticker}{exchange} from universe {membership.universe.code} is not mapped to a local Symbol."
        )
    raise UniverseMappingError(
        f"Ticker {membership.ticker} from universe {membership.universe.code} maps to multiple local Symbols; "
        "store an explicit symbol or exchange."
    )


def _active_memberships_for_day(memberships: list[UniverseMembership], day: date) -> list[UniverseMembership]:
    return [
        membership
        for membership in memberships
        if membership.valid_from <= day and (membership.valid_to is None or day <= membership.valid_to)
    ]


def _coverage_error_message(day: date, snapshot: UniverseCoverageSnapshot | None, *, universe_code: str) -> str:
    if snapshot is None:
        return (
            f"Historical {universe_code} coverage is not validated: "
            f"missing coverage snapshot for {day.isoformat()}."
        )
    batch = snapshot.import_batch
    return (
        f"Historical {universe_code} coverage is not validated: "
        f"date={day.isoformat()} "
        f"snapshot_status={snapshot.status} "
        f"batch_status={batch.status} "
        f"actual_member_count={snapshot.actual_member_count} "
        f"expected_member_count={snapshot.expected_member_count} "
        f"mapped_member_count={snapshot.mapped_member_count} "
        f"unmapped_member_count={snapshot.unmapped_member_count}."
    )


class UniverseResolver:
    def resolve(
        self,
        scenario: Scenario,
        start_date: date,
        end_date: date,
        warmup_start_date: date | None = None,
    ) -> ResolvedUniverse:
        if end_date < start_date:
            raise UniverseCoverageError("Universe resolution requires end_date greater than or equal to start_date.")

        mode = _normalize_universe_mode(getattr(scenario, "universe_mode", Scenario.UniverseMode.STATIC_TICKERS))
        if mode == Scenario.UniverseMode.STATIC_TICKERS:
            return self._resolve_static_tickers(scenario, start_date, end_date, warmup_start_date)
        universe_code = universe_code_for_historical_dynamic_mode(mode)
        if universe_code:
            return self._resolve_historical_dynamic(
                scenario,
                start_date,
                end_date,
                warmup_start_date,
                mode=mode,
                universe_code=universe_code,
            )
        raise UniverseConfigurationError(f"Unsupported universe mode: {mode}")

    def _resolve_static_tickers(
        self,
        scenario: Scenario,
        start_date: date,
        end_date: date,
        warmup_start_date: date | None,
    ) -> ResolvedUniverse:
        coverage_start = _effective_start(start_date, warmup_start_date)
        symbols = tuple(scenario.symbols.order_by("ticker", "exchange", "id"))
        tickers = tuple(symbol.ticker for symbol in symbols)
        active = frozenset(tickers)
        active_by_date = {day: active for day in _iter_calendar_days(coverage_start, end_date)}
        membership_by_ticker = {
            symbol.ticker: (
                ResolvedMembershipInterval(
                    ticker=symbol.ticker,
                    exchange=symbol.exchange,
                    symbol_id=symbol.id,
                    valid_from=coverage_start,
                    valid_to=end_date,
                    provider_symbol="",
                    source="scenario_static_tickers",
                ),
            )
            for symbol in symbols
        }
        return ResolvedUniverse(
            mode=Scenario.UniverseMode.STATIC_TICKERS,
            universe_code=None,
            start_date=start_date,
            end_date=end_date,
            coverage_start=coverage_start,
            coverage_end=end_date,
            tickers=tickers,
            symbols=symbols,
            active_by_date=active_by_date,
            membership_by_ticker=membership_by_ticker,
            metadata={"source": "scenario.symbols", "symbol_count": len(symbols)},
        )

    def _resolve_historical_dynamic(
        self,
        scenario: Scenario,
        start_date: date,
        end_date: date,
        warmup_start_date: date | None,
        *,
        mode: str,
        universe_code: str,
    ) -> ResolvedUniverse:
        coverage_start = _effective_start(start_date, warmup_start_date)
        try:
            universe = UniverseDefinition.objects.get(code=universe_code, active=True)
        except UniverseDefinition.DoesNotExist as exc:
            raise UniverseConfigurationError(f"UniverseDefinition {universe_code} is missing or inactive.") from exc

        self._validate_historical_coverage(universe, coverage_start, end_date, universe_code=universe_code)

        memberships = list(
            UniverseMembership.objects.filter(
                universe=universe,
                valid_from__lte=end_date,
            )
            .filter(Q(valid_to__isnull=True) | Q(valid_to__gte=coverage_start))
            .select_related("symbol", "universe")
            .order_by("ticker", "exchange", "valid_from")
        )
        if not memberships:
            raise UniverseCoverageError(
                f"Historical {universe_code} membership is incomplete for {coverage_start.isoformat()}..{end_date.isoformat()}: "
                "no memberships overlap the requested period."
            )

        symbol_by_membership_id = {membership.id: _resolve_membership_symbol(membership) for membership in memberships}
        active_by_date: dict[date, frozenset[str]] = {}
        for day in _iter_calendar_days(coverage_start, end_date):
            active_memberships = _active_memberships_for_day(memberships, day)
            if not active_memberships:
                raise UniverseCoverageError(
                    f"Historical {universe_code} membership is incomplete for {coverage_start.isoformat()}..{end_date.isoformat()}: "
                    f"no active members on {day.isoformat()}."
                )
            active_by_date[day] = frozenset(membership.ticker for membership in active_memberships)

        intervals_by_ticker: dict[str, list[ResolvedMembershipInterval]] = {}
        symbols_by_id: dict[int, Symbol] = {}
        for membership in memberships:
            symbol = symbol_by_membership_id[membership.id]
            symbols_by_id[symbol.id] = symbol
            intervals_by_ticker.setdefault(membership.ticker, []).append(
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

        membership_by_ticker = {
            ticker: tuple(intervals)
            for ticker, intervals in sorted(intervals_by_ticker.items())
        }
        tickers = tuple(membership_by_ticker.keys())
        symbols = tuple(sorted(symbols_by_id.values(), key=lambda symbol: (symbol.ticker, symbol.exchange, symbol.id)))
        first_valid_from = min(membership.valid_from for membership in memberships)
        open_ended = any(membership.valid_to is None for membership in memberships)
        max_valid_to = None if open_ended else max(membership.valid_to for membership in memberships if membership.valid_to)
        coverage_end = end_date if open_ended or max_valid_to is None or max_valid_to >= end_date else max_valid_to

        return ResolvedUniverse(
            mode=mode,
            universe_code=universe.code,
            start_date=start_date,
            end_date=end_date,
            coverage_start=coverage_start,
            coverage_end=coverage_end,
            tickers=tickers,
            symbols=symbols,
            active_by_date=active_by_date,
            membership_by_ticker=membership_by_ticker,
            metadata={
                "universe_name": universe.name,
                "source": universe.source,
                "coverage_start": coverage_start.isoformat(),
                "coverage_end": coverage_end.isoformat(),
                "first_membership_valid_from": first_valid_from.isoformat(),
                "membership_count": len(memberships),
                "ticker_count": len(tickers),
            },
        )

    def _validate_historical_coverage(self, universe: UniverseDefinition, coverage_start: date, end_date: date, *, universe_code: str) -> None:
        snapshots = {
            snapshot.coverage_date: snapshot
            for snapshot in UniverseCoverageSnapshot.objects.filter(
                universe=universe,
                coverage_date__gte=coverage_start,
                coverage_date__lte=end_date,
            ).select_related("import_batch")
        }
        for day in _iter_calendar_days(coverage_start, end_date):
            snapshot = snapshots.get(day)
            if snapshot is None:
                raise UniverseCoverageError(_coverage_error_message(day, None, universe_code=universe_code))
            if (
                snapshot.status != UniverseCoverageStatus.VALIDATED
                or snapshot.import_batch.status != UniverseCoverageStatus.VALIDATED
                or snapshot.actual_member_count < snapshot.expected_member_count
                or snapshot.mapped_member_count < snapshot.actual_member_count
                or snapshot.unmapped_member_count != 0
            ):
                raise UniverseCoverageError(_coverage_error_message(day, snapshot, universe_code=universe_code))


def resolve_universe_for_backtest(
    scenario: Scenario,
    start_date: date,
    end_date: date,
    warmup_start_date: date | None = None,
) -> ResolvedUniverse:
    return UniverseResolver().resolve(
        scenario=scenario,
        start_date=start_date,
        end_date=end_date,
        warmup_start_date=warmup_start_date,
    )
