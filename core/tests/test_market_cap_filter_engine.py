from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext
from unittest.mock import patch

from core.models import Alert, Backtest, DailyBar, DailyMetric, HistoricalMarketCap, Scenario, Symbol
from core.services.backtesting.engine import (
    _market_cap_from_cache,
    _preload_market_cap_cache,
    run_backtest,
    run_backtest_kpi_only,
)


class MarketCapBuyFilterEngineTests(TestCase):
    def setUp(self):
        self.symbol = Symbol.objects.create(ticker="AAA", exchange="NYSE", active=True)
        self.scenario = Scenario.objects.create(
            name="Market Cap Filter Scenario",
            active=True,
            a=1,
            b=1,
            c=1,
            d=1,
            e=1,
            n1=2,
            n2=2,
            npente=2,
            slope_threshold=Decimal("0.01"),
            npente_basse=1,
            slope_threshold_basse=Decimal("0.005"),
            nglobal=1,
            history_years=2,
        )

    def _create_bars_metrics_and_alerts(
        self,
        *,
        prices: list[str] | None = None,
        alerts_by_offset: dict[int, str] | None = None,
        start: date | None = None,
    ) -> date:
        start = start or date(2024, 1, 1)
        prices = prices or ["10", "11", "12"]
        alerts_by_offset = alerts_by_offset or {0: "Af", 1: "SPVa_basse"}
        DailyBar.objects.bulk_create([
            DailyBar(
                symbol=self.symbol,
                date=start + timedelta(days=i),
                open=Decimal(price),
                high=Decimal(price),
                low=Decimal(price),
                close=Decimal(price),
                volume=1000,
            )
            for i, price in enumerate(prices)
        ])
        DailyMetric.objects.bulk_create([
            DailyMetric(
                symbol=self.symbol,
                scenario=self.scenario,
                date=start + timedelta(days=i),
                P=Decimal(price),
                ratio_P=None,
            )
            for i, price in enumerate(prices)
        ])
        Alert.objects.bulk_create([
            Alert(
                symbol=self.symbol,
                scenario=self.scenario,
                date=start + timedelta(days=offset),
                alerts=alerts,
            )
            for offset, alerts in alerts_by_offset.items()
        ])
        return start

    def _create_backtest(
        self,
        *,
        settings: dict | None = None,
        prices: list[str] | None = None,
        alerts_by_offset: dict[int, str] | None = None,
        close_positions_at_end: bool = False,
        buy_gm_filter: str = "IGNORE",
    ) -> tuple[Backtest, date]:
        start = self._create_bars_metrics_and_alerts(prices=prices, alerts_by_offset=alerts_by_offset)
        line = {
            "trading_model": "LATCH_STATEFUL",
            "buy": ["Af", "SPVa_basse"],
            "buy_logic": "AND",
            "sell": [],
            "buy_gm_filter": buy_gm_filter,
        }
        bt = Backtest.objects.create(
            name="Market Cap Filter Backtest",
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=(len(prices or ["10", "11", "12"]) - 1)),
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[line],
            universe_snapshot=[self.symbol.ticker],
            warmup_days=0,
            close_positions_at_end=close_positions_at_end,
            settings=settings or {},
        )
        return bt, start

    def _add_market_cap(self, day: date, value: str):
        return HistoricalMarketCap.objects.create(
            symbol=self.symbol,
            date=day,
            market_cap=Decimal(value),
            provider="eodhd",
            provider_symbol="AAA.US",
            currency="USD",
        )

    def _line(self, bt: Backtest) -> dict:
        ticker = bt.universe_snapshot[0]
        return run_backtest(bt).results["tickers"][ticker]["lines"][0]

    def _actions(self, bt: Backtest) -> list[str | None]:
        return [row.get("action") for row in self._line(bt)["daily"]]

    def _historical_market_cap_query_count(self, func) -> int:
        with CaptureQueriesContext(connection) as ctx:
            func()
        return sum("core_historicalmarketcap" in query["sql"].lower() for query in ctx.captured_queries)

    def assert_buy_occurs(self, bt: Backtest):
        self.assertIn("BUY", {action for action in self._actions(bt) if action})

    def assert_no_buy_occurs(self, bt: Backtest):
        self.assertNotIn("BUY", {action for action in self._actions(bt) if action})

    def test_market_cap_buy_allowed_when_cap_is_within_range(self):
        bt, start = self._create_backtest(settings={"market_cap_min": "100", "market_cap_max": "200"})
        self._add_market_cap(start + timedelta(days=1), "150")

        self.assert_buy_occurs(bt)

    def test_market_cap_buy_blocked_below_min(self):
        bt, start = self._create_backtest(settings={"market_cap_min": "100"})
        self._add_market_cap(start + timedelta(days=1), "90")

        self.assert_no_buy_occurs(bt)

    def test_market_cap_buy_blocked_above_max(self):
        bt, start = self._create_backtest(settings={"market_cap_max": "200"})
        self._add_market_cap(start + timedelta(days=1), "250")

        self.assert_no_buy_occurs(bt)

    def test_market_cap_min_only_allows_above_min_and_blocks_below_min(self):
        bt_allowed, start_allowed = self._create_backtest(settings={"market_cap_min": "100"})
        self._add_market_cap(start_allowed + timedelta(days=1), "150")

        other = Symbol.objects.create(ticker="BBB", exchange="NYSE", active=True)
        self.symbol = other
        bt_blocked, start_blocked = self._create_backtest(settings={"market_cap_min": "100"})
        self._add_market_cap(start_blocked + timedelta(days=1), "90")

        self.assert_buy_occurs(bt_allowed)
        self.assert_no_buy_occurs(bt_blocked)

    def test_market_cap_max_only_allows_below_max_and_blocks_above_max(self):
        bt_allowed, start_allowed = self._create_backtest(settings={"market_cap_max": "200"})
        self._add_market_cap(start_allowed + timedelta(days=1), "150")

        blocked_symbol = Symbol.objects.create(ticker="CCC", exchange="NYSE", active=True)
        self.symbol = blocked_symbol
        bt_blocked, start_blocked = self._create_backtest(settings={"market_cap_max": "200"}, prices=["20", "21", "22"])
        self._add_market_cap(start_blocked + timedelta(days=1), "250")

        self.assert_buy_occurs(bt_allowed)
        self.assert_no_buy_occurs(bt_blocked)

    def test_market_cap_empty_settings_mean_no_constraint(self):
        bt, _start = self._create_backtest(settings={})

        self.assert_buy_occurs(bt)

    def test_market_cap_missing_blocks_only_when_filter_configured(self):
        filtered, _start = self._create_backtest(settings={"market_cap_min": "100"})
        unfiltered_symbol = Symbol.objects.create(ticker="DDD", exchange="NYSE", active=True)
        self.symbol = unfiltered_symbol
        unfiltered, _ = self._create_backtest(settings={})

        self.assert_no_buy_occurs(filtered)
        self.assert_buy_occurs(unfiltered)

    def test_market_cap_missing_default_policy_blocks_buy(self):
        bt, _start = self._create_backtest(settings={"market_cap_min": "100"})

        self.assert_no_buy_occurs(bt)

    def test_market_cap_missing_explicit_block_policy_blocks_buy(self):
        bt, _start = self._create_backtest(
            settings={"market_cap_min": "100", "market_cap_missing_policy": "BLOCK"}
        )

        self.assert_no_buy_occurs(bt)

    def test_market_cap_missing_allow_policy_allows_buy(self):
        bt, _start = self._create_backtest(
            settings={"market_cap_min": "100", "market_cap_missing_policy": "ALLOW"}
        )

        self.assert_buy_occurs(bt)

    def test_market_cap_missing_allow_policy_does_not_bypass_existing_out_of_range_cap(self):
        bt, start = self._create_backtest(
            settings={"market_cap_min": "100", "market_cap_missing_policy": "ALLOW"}
        )
        self._add_market_cap(start + timedelta(days=1), "50")

        self.assert_no_buy_occurs(bt)

    def test_market_cap_missing_invalid_policy_defaults_to_block(self):
        bt, _start = self._create_backtest(
            settings={"market_cap_min": "100", "market_cap_missing_policy": "unexpected"}
        )

        self.assert_no_buy_occurs(bt)

    def test_market_cap_future_value_is_not_used(self):
        bt, start = self._create_backtest(settings={"market_cap_min": "100", "market_cap_max": "200"})
        self._add_market_cap(start + timedelta(days=3), "150")

        self.assert_no_buy_occurs(bt)

    def test_market_cap_latest_previous_value_is_used_and_future_value_ignored(self):
        bt, start = self._create_backtest(settings={"market_cap_min": "100", "market_cap_max": "200"})
        self._add_market_cap(start, "150")
        self._add_market_cap(start + timedelta(days=2), "250")

        self.assert_buy_occurs(bt)

    def test_market_cap_change_after_buy_does_not_trigger_sell(self):
        bt, start = self._create_backtest(
            settings={"market_cap_min": "100"},
            prices=["10", "11", "12", "13"],
            close_positions_at_end=False,
        )
        self._add_market_cap(start + timedelta(days=1), "150")
        self._add_market_cap(start + timedelta(days=2), "50")

        actions = self._actions(bt)

        self.assertIn("BUY", actions)
        self.assertNotIn("SELL", {action for action in actions if action})

    def test_market_cap_missing_allow_policy_does_not_trigger_sell_after_buy(self):
        bt, _start = self._create_backtest(
            settings={"market_cap_min": "100", "market_cap_missing_policy": "ALLOW"},
            prices=["10", "11", "12", "13"],
            close_positions_at_end=False,
        )

        actions = self._actions(bt)

        self.assertIn("BUY", actions)
        self.assertNotIn("SELL", {action for action in actions if action})

    def test_market_cap_filter_is_not_latched_and_can_allow_later_buy(self):
        bt, start = self._create_backtest(
            settings={"market_cap_min": "100"},
            prices=["10", "11", "12"],
        )
        self._add_market_cap(start + timedelta(days=1), "50")
        self._add_market_cap(start + timedelta(days=2), "150")

        actions = self._actions(bt)

        self.assertIsNone(actions[1])
        self.assertEqual(actions[2], "BUY")

    def test_market_cap_absence_does_not_change_existing_gm_filter_behavior(self):
        bt, _start = self._create_backtest(settings={}, buy_gm_filter="GM_POS")

        self.assert_buy_occurs(bt)

    def test_market_cap_and_gm_combine_as_buy_gates(self):
        bt, start = self._create_backtest(
            settings={"market_cap_min": "100"},
            buy_gm_filter="GM_POS",
        )
        self._add_market_cap(start + timedelta(days=1), "50")

        self.assert_no_buy_occurs(bt)

    def test_price_filter_and_market_cap_filter_combine_with_and_semantics(self):
        cap_invalid, start = self._create_backtest(settings={"min_price": "10", "market_cap_min": "100"})
        self._add_market_cap(start + timedelta(days=1), "50")

        price_invalid_symbol = Symbol.objects.create(ticker="EEE", exchange="NYSE", active=True)
        self.symbol = price_invalid_symbol
        price_invalid, start_price_invalid = self._create_backtest(
            settings={"min_price": "100", "market_cap_min": "100"},
            prices=["50", "50", "50"],
        )
        self._add_market_cap(start_price_invalid + timedelta(days=1), "150")

        valid_symbol = Symbol.objects.create(ticker="FFF", exchange="NYSE", active=True)
        self.symbol = valid_symbol
        both_valid, start_both_valid = self._create_backtest(
            settings={"min_price": "10", "market_cap_min": "100"},
            prices=["50", "50", "50"],
        )
        self._add_market_cap(start_both_valid + timedelta(days=1), "150")

        self.assert_no_buy_occurs(cap_invalid)
        self.assert_no_buy_occurs(price_invalid)
        self.assert_buy_occurs(both_valid)

    def test_market_cap_filter_applies_to_kpi_only_path(self):
        bt, start = self._create_backtest(settings={"market_cap_min": "100"})
        self._add_market_cap(start + timedelta(days=1), "50")

        line = run_backtest_kpi_only(bt)[self.symbol.ticker]["lines"][0]

        self.assertEqual(line["final"]["N"], 0)
        self.assertEqual(line["final"]["TRADABLE_DAYS"], 0)

    def test_market_cap_missing_allow_policy_applies_to_kpi_only_path(self):
        bt, _start = self._create_backtest(
            settings={"market_cap_min": "100", "market_cap_missing_policy": "allow"}
        )

        line = run_backtest_kpi_only(bt)[self.symbol.ticker]["lines"][0]

        self.assertGreater(line["final"]["TRADABLE_DAYS"], 0)

    def test_market_cap_preload_cache_lookup_handles_exact_previous_missing_and_future_dates(self):
        other = Symbol.objects.create(ticker="ZZZ", exchange="NYSE", active=True)
        start = date(2024, 1, 1)
        HistoricalMarketCap.objects.bulk_create([
            HistoricalMarketCap(symbol=self.symbol, date=start, market_cap=Decimal("100"), provider="eodhd"),
            HistoricalMarketCap(symbol=self.symbol, date=start + timedelta(days=2), market_cap=Decimal("120"), provider="eodhd"),
            HistoricalMarketCap(symbol=self.symbol, date=start + timedelta(days=5), market_cap=Decimal("999"), provider="eodhd"),
            HistoricalMarketCap(symbol=other, date=start + timedelta(days=1), market_cap=Decimal("200"), provider="eodhd"),
        ])

        cache = _preload_market_cap_cache([self.symbol, other], start + timedelta(days=3))

        self.assertEqual(_market_cap_from_cache(cache[self.symbol.id], start), Decimal("100"))
        self.assertEqual(_market_cap_from_cache(cache[self.symbol.id], start + timedelta(days=1)), Decimal("100"))
        self.assertEqual(_market_cap_from_cache(cache[self.symbol.id], start + timedelta(days=3)), Decimal("120"))
        self.assertIsNone(_market_cap_from_cache(cache[self.symbol.id], start - timedelta(days=1)))
        self.assertEqual(_market_cap_from_cache(cache[other.id], start + timedelta(days=3)), Decimal("200"))

    def test_market_cap_filter_uses_bounded_preload_query_in_full_backtest_hot_loop(self):
        prices = [str(10 + i) for i in range(12)]
        bt, start = self._create_backtest(settings={"market_cap_min": "100"}, prices=prices)
        HistoricalMarketCap.objects.bulk_create([
            HistoricalMarketCap(
                symbol=self.symbol,
                date=start + timedelta(days=i),
                market_cap=Decimal("150"),
                provider="eodhd",
            )
            for i in range(len(prices))
        ])

        query_count = self._historical_market_cap_query_count(lambda: run_backtest(bt))

        self.assertGreater(query_count, 0)
        self.assertLessEqual(query_count, 1)

    def test_market_cap_filter_uses_bounded_preload_query_in_kpi_only_hot_loop(self):
        prices = [str(10 + i) for i in range(12)]
        bt, start = self._create_backtest(settings={"market_cap_min": "100"}, prices=prices)
        HistoricalMarketCap.objects.bulk_create([
            HistoricalMarketCap(
                symbol=self.symbol,
                date=start + timedelta(days=i),
                market_cap=Decimal("150"),
                provider="eodhd",
            )
            for i in range(len(prices))
        ])

        query_count = self._historical_market_cap_query_count(lambda: run_backtest_kpi_only(bt))

        self.assertGreater(query_count, 0)
        self.assertLessEqual(query_count, 1)

    def test_market_cap_preload_is_skipped_when_filter_is_not_configured(self):
        bt, _start = self._create_backtest(settings={}, prices=[str(10 + i) for i in range(12)])

        query_count = self._historical_market_cap_query_count(lambda: run_backtest(bt))

        self.assertEqual(query_count, 0)

    @patch("core.services.provider_eodhd.EODHDClient.fetch_historical_market_cap")
    def test_backtests_do_not_call_eodhd_provider(self, mock_fetch):
        bt, start = self._create_backtest(settings={"market_cap_min": "100"})
        self._add_market_cap(start + timedelta(days=1), "150")

        run_backtest(bt)
        run_backtest_kpi_only(bt)

        mock_fetch.assert_not_called()
