from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import patch

from django.test import TestCase, override_settings

from core.models import Backtest, DailyBar, Scenario, Symbol
from core.services.backtesting.ohlc_readiness import (
    OHLC_READINESS_USER_MESSAGE,
    OHLC_READINESS_TOO_MANY_MISSING_MESSAGE,
    OHLCReadinessError,
    ensure_ohlc_ready_for_backtest,
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
