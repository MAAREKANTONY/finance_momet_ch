from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from django.test import TestCase, override_settings

from core.models import Backtest, DailyBar, Scenario, Symbol
from core.services.backtesting.ohlc_readiness import (
    OHLC_READINESS_USER_MESSAGE,
    OHLC_READINESS_TOO_MANY_MISSING_MESSAGE,
    OHLCReadinessError,
    ensure_ohlc_ready_for_backtest,
    get_missing_ohlc_symbols_for_dynamic_universe,
)


class OHLCReadinessTests(TestCase):
    def setUp(self):
        self.start = date(2024, 1, 1)
        self.end = date(2024, 1, 5)
        self.scenario = Scenario.objects.create(
            name="Dynamic SP500",
            universe_mode=Scenario.UniverseMode.SP500_HISTORICAL_DYNAMIC,
            active=True,
        )
        self.backtest = Backtest.objects.create(
            name="OHLC readiness",
            scenario=self.scenario,
            start_date=self.start,
            end_date=self.end,
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            capital_mode=Backtest.CapitalMode.FIXED,
            ratio_threshold=Decimal("0"),
            include_all_tickers=True,
            signal_lines=[{"buy": ["A1"], "sell": ["B1"]}],
            universe_snapshot=[],
            warmup_days=0,
        )
        self.ready_symbol = Symbol.objects.create(ticker="READY", exchange="NASDAQ", active=True)
        self.missing_symbol = Symbol.objects.create(ticker="MISS", exchange="US", instrument_type="Common Stock", active=True)

    def _create_boundary_bars(self, symbol: Symbol):
        for current in [self.start, self.end]:
            DailyBar.objects.create(
                symbol=symbol,
                date=current,
                open=Decimal("10"),
                high=Decimal("10"),
                low=Decimal("10"),
                close=Decimal("10"),
                volume=1000,
            )

    def _create_boundary_bars_for_range(self, symbol: Symbol, start: date, end: date):
        for current in [start, end]:
            DailyBar.objects.create(
                symbol=symbol,
                date=current,
                open=Decimal("10"),
                high=Decimal("10"),
                low=Decimal("10"),
                close=Decimal("10"),
                volume=1000,
            )

    def _interval(self, symbol: Symbol, valid_from: date, valid_to: date | None = None):
        return SimpleNamespace(
            ticker=symbol.ticker,
            exchange=symbol.exchange,
            symbol_id=symbol.id,
            valid_from=valid_from,
            valid_to=valid_to,
        )

    def test_ready_scope_does_not_fetch(self):
        self._create_boundary_bars(self.ready_symbol)

        result = ensure_ohlc_ready_for_backtest(
            backtest=self.backtest,
            symbols=[self.ready_symbol],
            start_date=self.start,
            end_date=self.end,
        )

        self.assertTrue(result.ready)
        self.assertFalse(result.did_fetch)

    def test_missing_scope_blocks_without_fetch(self):
        self._create_boundary_bars(self.ready_symbol)

        with self.assertRaises(OHLCReadinessError) as ctx:
            ensure_ohlc_ready_for_backtest(
                backtest=self.backtest,
                symbols=[self.ready_symbol, self.missing_symbol],
                start_date=self.start,
                end_date=self.end,
            )

        self.assertIn(OHLC_READINESS_USER_MESSAGE, str(ctx.exception))
        self.assertEqual(ctx.exception.missing_tickers, ["MISS"])

    @patch("core.tasks._fetch_daily_bars_for_symbols")
    def test_missing_scope_does_not_call_hidden_fetch_even_if_allowed(self, fetch_mock):
        with self.assertRaises(OHLCReadinessError) as ctx:
            ensure_ohlc_ready_for_backtest(
                backtest=self.backtest,
                symbols=[self.missing_symbol],
                start_date=self.start,
                end_date=self.end,
                allow_fetch=True,
            )

        fetch_mock.assert_not_called()
        self.assertIn("préparation OHLC dynamique dédiée", str(ctx.exception))
        self.assertEqual(ctx.exception.missing_tickers, ["MISS"])

    def test_missing_raises_user_friendly_error(self):
        with self.assertRaises(OHLCReadinessError) as ctx:
            ensure_ohlc_ready_for_backtest(
                backtest=self.backtest,
                symbols=[self.missing_symbol],
                start_date=self.start,
                end_date=self.end,
            )

        self.assertIn(OHLC_READINESS_USER_MESSAGE, str(ctx.exception))
        self.assertEqual(ctx.exception.missing_tickers, ["MISS"])

    def test_allow_fetch_false_blocks_without_fetch(self):
        with self.assertRaises(OHLCReadinessError) as ctx:
            ensure_ohlc_ready_for_backtest(
                backtest=self.backtest,
                symbols=[self.missing_symbol],
                start_date=self.start,
                end_date=self.end,
                allow_fetch=False,
            )

        self.assertEqual(ctx.exception.missing_tickers, ["MISS"])

    @override_settings(DYNAMIC_UNIVERSE_OHLC_AUTO_FETCH_MAX_SYMBOLS=1)
    def test_guardrail_blocks_oversized_scoped_fetch(self):
        extra = Symbol.objects.create(ticker="MISS2", exchange="US", instrument_type="Common Stock", active=True)

        with self.assertRaises(OHLCReadinessError) as ctx:
            ensure_ohlc_ready_for_backtest(
                backtest=self.backtest,
                symbols=[self.missing_symbol, extra],
                start_date=self.start,
                end_date=self.end,
            )

        self.assertEqual(ctx.exception.missing_tickers, ["MISS", "MISS2"])
        self.assertIn(OHLC_READINESS_TOO_MANY_MISSING_MESSAGE, str(ctx.exception))
        self.assertIn("Symboles manquants: 2", str(ctx.exception))
        self.assertIn("2024-01-01", str(ctx.exception))
        self.assertIn("2024-01-05", str(ctx.exception))

    def test_default_symbol_guardrail_is_twenty_five(self):
        symbols = [
            Symbol.objects.create(ticker=f"MISS{i:02d}", exchange="US", instrument_type="Common Stock", active=True)
            for i in range(26)
        ]

        with self.assertRaises(OHLCReadinessError) as ctx:
            ensure_ohlc_ready_for_backtest(
                backtest=self.backtest,
                symbols=symbols,
                start_date=self.start,
                end_date=self.end,
            )

        self.assertIn(OHLC_READINESS_TOO_MANY_MISSING_MESSAGE, str(ctx.exception))
        self.assertIn("Symboles manquants: 26", str(ctx.exception))

    @override_settings(DYNAMIC_UNIVERSE_OHLC_AUTO_FETCH_MAX_DAYS=2)
    def test_guardrail_blocks_oversized_period_without_fetch(self):
        with self.assertRaises(OHLCReadinessError) as ctx:
            ensure_ohlc_ready_for_backtest(
                backtest=self.backtest,
                symbols=[self.missing_symbol],
                start_date=self.start,
                end_date=self.end,
            )

        self.assertIn(OHLC_READINESS_TOO_MANY_MISSING_MESSAGE, str(ctx.exception))
        self.assertEqual(ctx.exception.missing_tickers, ["MISS"])

    def test_default_period_guardrail_is_seven_hundred_thirty_days(self):
        with self.assertRaises(OHLCReadinessError) as ctx:
            ensure_ohlc_ready_for_backtest(
                backtest=self.backtest,
                symbols=[self.missing_symbol],
                start_date=date(2020, 1, 1),
                end_date=date(2022, 1, 1),
            )

        self.assertIn(OHLC_READINESS_TOO_MANY_MISSING_MESSAGE, str(ctx.exception))
        self.assertIn("Période: 2020-01-01", str(ctx.exception))
        self.assertIn("2022-01-01", str(ctx.exception))

    def test_weekend_start_accepts_first_market_bar_shortly_after_start(self):
        weekend_start = date(2022, 1, 1)
        end = date(2022, 1, 7)
        DailyBar.objects.create(
            symbol=self.ready_symbol,
            date=date(2022, 1, 3),
            open=Decimal("10"),
            high=Decimal("10"),
            low=Decimal("10"),
            close=Decimal("10"),
            volume=1000,
        )
        DailyBar.objects.create(
            symbol=self.ready_symbol,
            date=end,
            open=Decimal("10"),
            high=Decimal("10"),
            low=Decimal("10"),
            close=Decimal("10"),
            volume=1000,
        )

        result = ensure_ohlc_ready_for_backtest(
            backtest=self.backtest,
            symbols=[self.ready_symbol],
            start_date=weekend_start,
            end_date=end,
        )

        self.assertTrue(result.ready)
        self.assertFalse(result.did_fetch)

    def test_weekend_start_still_blocks_when_first_bar_is_too_late(self):
        weekend_start = date(2022, 1, 1)
        end = date(2022, 1, 10)
        DailyBar.objects.create(
            symbol=self.ready_symbol,
            date=date(2022, 1, 6),
            open=Decimal("10"),
            high=Decimal("10"),
            low=Decimal("10"),
            close=Decimal("10"),
            volume=1000,
        )
        DailyBar.objects.create(
            symbol=self.ready_symbol,
            date=end,
            open=Decimal("10"),
            high=Decimal("10"),
            low=Decimal("10"),
            close=Decimal("10"),
            volume=1000,
        )

        with self.assertRaises(OHLCReadinessError) as ctx:
            ensure_ohlc_ready_for_backtest(
                backtest=self.backtest,
                symbols=[self.ready_symbol],
                start_date=weekend_start,
                end_date=end,
            )

        self.assertEqual(ctx.exception.missing_tickers, ["READY"])

    def test_dynamic_delisted_member_ready_on_membership_interval_only(self):
        global_start = date(2022, 1, 1)
        global_end = date(2026, 6, 16)
        valid_to = date(2022, 12, 23)
        self._create_boundary_bars_for_range(self.ready_symbol, date(2022, 1, 3), valid_to)

        missing = get_missing_ohlc_symbols_for_dynamic_universe(
            symbols=[self.ready_symbol],
            start_date=global_start,
            end_date=global_end,
            membership_by_ticker={
                "READY": (self._interval(self.ready_symbol, global_start, valid_to),),
            },
        )

        self.assertEqual(missing, [])

    def test_dynamic_acquired_member_does_not_require_bars_after_valid_to(self):
        global_start = date(2022, 1, 1)
        global_end = date(2026, 6, 16)
        valid_to = date(2023, 10, 13)
        self._create_boundary_bars_for_range(self.ready_symbol, date(2022, 1, 3), valid_to)

        missing = get_missing_ohlc_symbols_for_dynamic_universe(
            symbols=[self.ready_symbol],
            start_date=global_start,
            end_date=global_end,
            membership_by_ticker={
                "READY": (self._interval(self.ready_symbol, global_start, valid_to),),
            },
        )

        self.assertEqual(missing, [])

    def test_dynamic_new_entrant_does_not_require_bars_before_valid_from(self):
        global_start = date(2022, 1, 1)
        global_end = date(2026, 6, 16)
        valid_from = date(2024, 1, 2)
        self._create_boundary_bars_for_range(self.ready_symbol, valid_from, global_end)

        missing = get_missing_ohlc_symbols_for_dynamic_universe(
            symbols=[self.ready_symbol],
            start_date=global_start,
            end_date=global_end,
            membership_by_ticker={
                "READY": (self._interval(self.ready_symbol, valid_from, None),),
            },
        )

        self.assertEqual(missing, [])

    def test_dynamic_active_member_still_requires_end_date_coverage(self):
        global_start = date(2022, 1, 1)
        global_end = date(2026, 6, 16)
        self._create_boundary_bars_for_range(self.ready_symbol, date(2022, 1, 3), date(2025, 12, 31))

        missing = get_missing_ohlc_symbols_for_dynamic_universe(
            symbols=[self.ready_symbol],
            start_date=global_start,
            end_date=global_end,
            membership_by_ticker={
                "READY": (self._interval(self.ready_symbol, global_start, None),),
            },
        )

        self.assertEqual([symbol.ticker for symbol in missing], ["READY"])

    def test_dynamic_weekend_membership_start_accepts_first_market_bar(self):
        weekend_start = date(2022, 1, 1)
        end = date(2022, 1, 7)
        self._create_boundary_bars_for_range(self.ready_symbol, date(2022, 1, 3), end)

        missing = get_missing_ohlc_symbols_for_dynamic_universe(
            symbols=[self.ready_symbol],
            start_date=weekend_start,
            end_date=end,
            membership_by_ticker={
                "READY": (self._interval(self.ready_symbol, weekend_start, None),),
            },
        )

        self.assertEqual(missing, [])

    def test_dynamic_fake_symbol_remains_missing_without_bars(self):
        fake = Symbol.objects.create(ticker="OLD", exchange="NYSE", active=True)

        missing = get_missing_ohlc_symbols_for_dynamic_universe(
            symbols=[fake],
            start_date=self.start,
            end_date=self.end,
            membership_by_ticker={
                "OLD": (self._interval(fake, self.start, None),),
            },
        )

        self.assertEqual([symbol.ticker for symbol in missing], ["OLD"])

    def test_ensure_ohlc_ready_uses_resolved_membership_intervals(self):
        valid_to = self.start + timedelta(days=1)
        self._create_boundary_bars_for_range(self.ready_symbol, self.start, valid_to)
        resolved = SimpleNamespace(
            membership_by_ticker={
                "READY": (self._interval(self.ready_symbol, self.start, valid_to),),
            }
        )

        with patch("core.services.universe_resolver.UniverseResolver") as resolver_cls:
            resolver_cls.return_value.resolve.return_value = resolved
            result = ensure_ohlc_ready_for_backtest(
                backtest=self.backtest,
                symbols=[self.ready_symbol],
                start_date=self.start,
                end_date=self.end,
            )

        self.assertTrue(result.ready)
        self.assertFalse(result.did_fetch)
