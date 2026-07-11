from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import patch

from django.test import TestCase
from django.utils import timezone

from core.models import (
    Backtest,
    DailyBar,
    ProcessingJob,
    Scenario,
    Symbol,
    UniverseCoverageSnapshot,
    UniverseCoverageStatus,
    UniverseDefinition,
    UniverseImportBatch,
    UniverseMembership,
)
from core.services.dynamic_universe_ohlc_prepare import prepare_dynamic_universe_ohlc
from core.services.provider_eodhd import EODHDError, to_eodhd_symbol_from_parts
from core.tasks import prepare_dynamic_universe_ohlc_job_task


class FakeEODHDClient:
    def __init__(self, responses: dict[str, object]):
        self.responses = responses
        self.calls: list[tuple[str, date, date]] = []

    def fetch_historical_ohlc(self, provider_symbol, from_date, to_date):
        self.calls.append((provider_symbol, from_date, to_date))
        response = self.responses.get(provider_symbol, [])
        if isinstance(response, Exception):
            raise response
        return response


class DynamicUniverseOHLCTestCase(TestCase):
    start = date(2024, 1, 1)
    end = date(2024, 1, 5)

    def setUp(self):
        self.scenario = Scenario.objects.create(
            name="Dynamic SP500",
            universe_mode=Scenario.UniverseMode.SP500_HISTORICAL_DYNAMIC,
            active=True,
        )
        self.backtest = Backtest.objects.create(
            name="Dynamic OHLC",
            scenario=self.scenario,
            start_date=self.start,
            end_date=self.end,
            capital_total=Decimal("10000"),
            capital_per_ticker=Decimal("1000"),
            capital_mode=Backtest.CapitalMode.FIXED,
            ratio_threshold=Decimal("0"),
            include_all_tickers=True,
            signal_lines=[{"buy": ["A1"], "sell": ["B1"]}],
            warmup_days=0,
        )
        self.ready = Symbol.objects.create(ticker="READY", exchange="NYSE", instrument_type="Common Stock", active=True)
        self.missing = Symbol.objects.create(ticker="MISS", exchange="US", instrument_type="Common Stock", active=True)
        self.extra = Symbol.objects.create(ticker="EXTRA", exchange="NASDAQ", instrument_type="Common Stock", active=True)
        self._validated_sp500(self.ready, self.missing, self.extra)

    def _validated_sp500(self, *symbols: Symbol):
        universe = UniverseDefinition.objects.create(
            code="SP500",
            name="S&P 500",
            source="test",
            active=True,
        )
        self._validated_universe(universe, symbols, provider_symbol_for=lambda symbol: f"{symbol.ticker}.US")

    def _validated_csi300(self, *symbols: Symbol, provider_symbol_for=None):
        universe = UniverseDefinition.objects.create(
            code="CSI300",
            name="CSI 300",
            source="manual_csv",
            active=True,
        )
        self._validated_universe(universe, symbols, provider_symbol_for=provider_symbol_for or (lambda symbol: ""))
        return universe

    def _validated_universe(self, universe: UniverseDefinition, symbols, *, provider_symbol_for):
        for symbol in symbols:
            UniverseMembership.objects.create(
                universe=universe,
                symbol=symbol,
                ticker=symbol.ticker,
                exchange=symbol.exchange,
                provider_symbol=provider_symbol_for(symbol),
                valid_from=self.start,
                valid_to=None,
                source="test",
            )
        batch = UniverseImportBatch.objects.create(
            universe=universe,
            provider="test",
            source_name="test",
            period_start=self.start,
            period_end=self.end,
            expected_member_count=len(symbols),
            imported_member_count=len(symbols),
            mapped_member_count=len(symbols),
            unmapped_member_count=0,
            status=UniverseCoverageStatus.VALIDATED,
            validated_at=timezone.now(),
        )
        current = self.start
        while current <= self.end:
            UniverseCoverageSnapshot.objects.create(
                universe=universe,
                import_batch=batch,
                coverage_date=current,
                expected_member_count=len(symbols),
                actual_member_count=len(symbols),
                mapped_member_count=len(symbols),
                unmapped_member_count=0,
                status=UniverseCoverageStatus.VALIDATED,
            )
            current += timedelta(days=1)

    def _bars(self, symbol: Symbol, *, close: str = "10"):
        for current in (self.start, self.end):
            DailyBar.objects.create(
                symbol=symbol,
                date=current,
                open=Decimal(close),
                high=Decimal(close),
                low=Decimal(close),
                close=Decimal(close),
                volume=1000,
                source="test",
            )

    def _rows(self, *, close: str = "20"):
        return [
            {
                "date": self.start,
                "open": Decimal(close),
                "high": Decimal(close),
                "low": Decimal(close),
                "close": Decimal(close),
                "volume": 2000,
                "provider_symbol": "TEST.US",
                "source_payload": {},
            },
            {
                "date": self.end,
                "open": Decimal(close),
                "high": Decimal(close),
                "low": Decimal(close),
                "close": Decimal(close),
                "volume": 2000,
                "provider_symbol": "TEST.US",
                "source_payload": {},
            },
        ]

    def _add_universe_symbol(self, ticker: str, *, exchange: str = "NYSE") -> Symbol:
        symbol = Symbol.objects.create(
            ticker=ticker,
            exchange=exchange,
            instrument_type="Common Stock",
            active=True,
        )
        universe = UniverseDefinition.objects.get(code="SP500")
        UniverseMembership.objects.create(
            universe=universe,
            symbol=symbol,
            ticker=ticker,
            exchange=exchange,
            provider_symbol=f"{ticker}.US",
            valid_from=self.start,
            valid_to=None,
            source="test",
        )
        return symbol


class DynamicUniverseOHLCPrepareServiceTests(DynamicUniverseOHLCTestCase):
    def test_already_ready_symbols_do_not_fetch(self):
        for symbol in (self.ready, self.missing, self.extra):
            self._bars(symbol)
        client = FakeEODHDClient({})

        result = prepare_dynamic_universe_ohlc(
            backtest_id=self.backtest.id,
            client=client,
        )

        self.assertEqual(result.missing_before, [])
        self.assertEqual(client.calls, [])
        self.assertEqual(result.ready_after, 3)

    def test_missing_symbols_fetch_eodhd_and_upsert_daily_bars(self):
        self._bars(self.ready)
        client = FakeEODHDClient({
            "MISS.US": self._rows(),
            "EXTRA.US": self._rows(close="30"),
        })

        result = prepare_dynamic_universe_ohlc(backtest_id=self.backtest.id, client=client)

        self.assertEqual([call[0] for call in client.calls], ["EXTRA.US", "MISS.US"])
        self.assertEqual(result.inserted_bars, 4)
        self.assertEqual(result.missing_after, [])
        self.assertEqual(DailyBar.objects.filter(symbol=self.missing).count(), 2)

    def test_delisted_symbol_ready_on_membership_interval_is_not_fetched(self):
        delist_date = self.start + timedelta(days=1)
        UniverseMembership.objects.filter(ticker=self.missing.ticker).update(valid_to=delist_date)
        self._bars(self.ready)
        for current in (self.start, delist_date):
            DailyBar.objects.create(
                symbol=self.missing,
                date=current,
                open=Decimal("10"),
                high=Decimal("10"),
                low=Decimal("10"),
                close=Decimal("10"),
                volume=1000,
            )
        client = FakeEODHDClient({
            "EXTRA.US": self._rows(close="30"),
        })

        result = prepare_dynamic_universe_ohlc(backtest_id=self.backtest.id, client=client)

        self.assertEqual([call[0] for call in client.calls], ["EXTRA.US"])
        self.assertNotIn("MISS", result.missing_before)
        self.assertEqual(result.missing_after, [])

    def test_delisted_symbol_with_small_end_gap_is_not_fetched(self):
        UniverseMembership.objects.filter(ticker=self.missing.ticker).update(valid_to=self.end)
        self._bars(self.ready)
        DailyBar.objects.create(
            symbol=self.missing,
            date=self.start,
            open=Decimal("10"),
            high=Decimal("10"),
            low=Decimal("10"),
            close=Decimal("10"),
            volume=1000,
        )
        client = FakeEODHDClient({
            "EXTRA.US": self._rows(close="30"),
            "MISS.US": self._rows(),
        })

        result = prepare_dynamic_universe_ohlc(backtest_id=self.backtest.id, client=client)

        self.assertEqual([call[0] for call in client.calls], ["EXTRA.US"])
        self.assertNotIn("MISS", result.missing_before)
        self.assertEqual(result.missing_after, [])

    def test_active_symbol_with_end_gap_is_still_fetched(self):
        self._bars(self.ready)
        self._bars(self.extra)
        DailyBar.objects.create(
            symbol=self.missing,
            date=self.start,
            open=Decimal("10"),
            high=Decimal("10"),
            low=Decimal("10"),
            close=Decimal("10"),
            volume=1000,
        )
        client = FakeEODHDClient({
            "MISS.US": [
                {
                    "date": self.end,
                    "open": Decimal("20"),
                    "high": Decimal("20"),
                    "low": Decimal("20"),
                    "close": Decimal("20"),
                    "volume": 2000,
                    "provider_symbol": "MISS.US",
                    "source_payload": {},
                },
            ],
        })

        result = prepare_dynamic_universe_ohlc(backtest_id=self.backtest.id, client=client)

        self.assertEqual([call[0] for call in client.calls], ["MISS.US"])
        self.assertNotIn("MISS", result.missing_after)

    def test_new_entrant_fetch_uses_membership_window(self):
        valid_from = self.start + timedelta(days=2)
        UniverseMembership.objects.filter(ticker=self.extra.ticker).update(valid_from=valid_from)
        self._bars(self.ready)
        self._bars(self.missing)
        client = FakeEODHDClient({
            "EXTRA.US": [
                {
                    "date": valid_from,
                    "open": Decimal("30"),
                    "high": Decimal("30"),
                    "low": Decimal("30"),
                    "close": Decimal("30"),
                    "volume": 2000,
                    "provider_symbol": "EXTRA.US",
                    "source_payload": {},
                },
                {
                    "date": self.end,
                    "open": Decimal("30"),
                    "high": Decimal("30"),
                    "low": Decimal("30"),
                    "close": Decimal("30"),
                    "volume": 2000,
                    "provider_symbol": "EXTRA.US",
                    "source_payload": {},
                },
            ],
        })

        result = prepare_dynamic_universe_ohlc(backtest_id=self.backtest.id, client=client)

        self.assertEqual(client.calls, [("EXTRA.US", valid_from, self.end)])
        self.assertEqual(result.missing_after, [])

    def test_prepare_is_idempotent_when_rerun(self):
        self._bars(self.ready)
        client = FakeEODHDClient({
            "MISS.US": self._rows(),
            "EXTRA.US": self._rows(close="30"),
        })

        first = prepare_dynamic_universe_ohlc(backtest_id=self.backtest.id, client=client)
        second = prepare_dynamic_universe_ohlc(backtest_id=self.backtest.id, client=client)

        self.assertEqual(first.missing_after, [])
        self.assertEqual(second.missing_before, [])
        self.assertEqual(DailyBar.objects.filter(symbol=self.missing, date=self.start).count(), 1)

    def test_empty_provider_response_records_no_data_symbol(self):
        self._bars(self.ready)
        self._bars(self.extra)
        client = FakeEODHDClient({"MISS.US": []})

        result = prepare_dynamic_universe_ohlc(backtest_id=self.backtest.id, client=client)

        self.assertEqual(result.no_data_symbols, ["MISS"])
        self.assertEqual(result.missing_after, ["MISS"])

    def test_provider_exception_is_reported_without_raw_api_key(self):
        self._bars(self.ready)
        self._bars(self.extra)
        client = FakeEODHDClient({
            "MISS.US": EODHDError("boom api_token=secret-token"),
        })

        result = prepare_dynamic_universe_ohlc(backtest_id=self.backtest.id, client=client)

        self.assertIn("MISS", result.provider_error_symbols)
        self.assertIn("api_token=***", result.provider_error_symbols["MISS"])
        self.assertNotIn("secret-token", result.provider_error_symbols["MISS"])

    def test_network_exception_is_classified_separately(self):
        self._bars(self.ready)
        self._bars(self.extra)
        client = FakeEODHDClient({
            "MISS.US": EODHDError("Failed to resolve api.eodhd.com"),
        })

        result = prepare_dynamic_universe_ohlc(backtest_id=self.backtest.id, client=client)

        self.assertIn("MISS", result.network_error_symbols)
        self.assertEqual(result.provider_error_symbols, {})

    def test_max_symbols_limits_fetch_scope(self):
        self._bars(self.ready)
        client = FakeEODHDClient({
            "EXTRA.US": self._rows(),
            "MISS.US": self._rows(),
        })

        result = prepare_dynamic_universe_ohlc(
            backtest_id=self.backtest.id,
            client=client,
            max_symbols=1,
        )

        self.assertEqual(len(client.calls), 1)
        self.assertEqual(result.inserted_bars, 2)
        self.assertEqual(len(result.missing_after), 1)

    def test_exclude_tickers_removes_requested_symbols_from_fetch_scope(self):
        fake_tickers = ["DKEEP", "DNEW", "KEEP", "NEW"]
        for ticker in fake_tickers:
            self._add_universe_symbol(ticker)
        self._bars(self.ready)
        client = FakeEODHDClient({
            "EXTRA.US": self._rows(close="30"),
            "MISS.US": self._rows(),
            **{f"{ticker}.US": self._rows(close="99") for ticker in fake_tickers},
        })

        result = prepare_dynamic_universe_ohlc(
            backtest_id=self.backtest.id,
            client=client,
            exclude_tickers=fake_tickers,
        )

        called_provider_symbols = [call[0] for call in client.calls]
        self.assertEqual(called_provider_symbols, ["EXTRA.US", "MISS.US"])
        for ticker in fake_tickers:
            self.assertEqual(result.skipped_symbols[ticker], "excluded_by_request")
            self.assertIn(ticker, result.missing_after)
            self.assertNotIn(f"{ticker}.US", called_provider_symbols)
            self.assertFalse(DailyBar.objects.filter(symbol__ticker=ticker).exists())
        self.assertEqual(result.provider_error_symbols, {})
        self.assertEqual(result.no_data_symbols, [])
        self.assertEqual(result.network_error_symbols, {})

    def test_max_symbols_applies_after_exclude_tickers(self):
        self._bars(self.ready)
        client = FakeEODHDClient({
            "EXTRA.US": self._rows(close="30"),
            "MISS.US": self._rows(),
        })

        result = prepare_dynamic_universe_ohlc(
            backtest_id=self.backtest.id,
            client=client,
            exclude_tickers=["EXTRA"],
            max_symbols=1,
        )

        self.assertEqual([call[0] for call in client.calls], ["MISS.US"])
        self.assertEqual(result.skipped_symbols, {"EXTRA": "excluded_by_request"})
        self.assertIn("EXTRA", result.missing_after)
        self.assertNotIn("MISS", result.missing_after)

    def test_force_refresh_updates_existing_rows_without_duplicates(self):
        for symbol in (self.ready, self.missing, self.extra):
            self._bars(symbol, close="10")
        client = FakeEODHDClient({
            "READY.US": self._rows(close="42"),
            "MISS.US": self._rows(close="42"),
            "EXTRA.US": self._rows(close="42"),
        })

        result = prepare_dynamic_universe_ohlc(
            backtest_id=self.backtest.id,
            client=client,
            force_refresh=True,
        )

        self.assertEqual(len(client.calls), 3)
        self.assertEqual(result.updated_bars, 6)
        self.assertEqual(DailyBar.objects.filter(symbol=self.ready, date=self.start).count(), 1)
        self.assertEqual(DailyBar.objects.get(symbol=self.ready, date=self.start).close, Decimal("42"))

    def test_static_scenario_is_rejected_without_mutating_backtest(self):
        static = Scenario.objects.create(name="Static", universe_mode=Scenario.UniverseMode.STATIC_TICKERS)
        self.backtest.scenario = static
        self.backtest.save(update_fields=["scenario"])

        with self.assertRaisesMessage(Exception, "SP500_HISTORICAL_DYNAMIC"):
            prepare_dynamic_universe_ohlc(backtest_id=self.backtest.id, client=FakeEODHDClient({}))

        self.backtest.refresh_from_db()
        self.assertEqual(self.backtest.scenario, static)

    def test_eodhd_china_mapping_preserves_numeric_tickers(self):
        self.assertEqual(to_eodhd_symbol_from_parts(ticker="600519", exchange="SHG"), "600519.SHG")
        self.assertEqual(to_eodhd_symbol_from_parts(ticker="000001", exchange="SHE"), "000001.SHE")
        self.assertEqual(to_eodhd_symbol_from_parts(ticker="300750", exchange="SHE"), "300750.SHE")
        self.assertEqual(to_eodhd_symbol_from_parts(ticker="600519", exchange="XSHG"), "600519.SHG")
        self.assertEqual(to_eodhd_symbol_from_parts(ticker="000001", exchange="XSHE"), "000001.SHE")
        self.assertEqual(to_eodhd_symbol_from_parts(ticker="AAPL", exchange="NASDAQ"), "AAPL.US")

    def test_csi300_ohlc_prepare_uses_csv_provider_symbols_without_constituents_provider(self):
        csi = Scenario.objects.create(name="CSI300", universe_mode=Scenario.UniverseMode.CSI300_HISTORICAL_DYNAMIC)
        self.backtest.scenario = csi
        self.backtest.save(update_fields=["scenario"])
        shg = Symbol.objects.create(ticker="600519", exchange="SHG", instrument_type="Common Stock", active=True)
        she = Symbol.objects.create(ticker="000001", exchange="SHE", instrument_type="Common Stock", active=True)
        self._validated_csi300(shg, she, provider_symbol_for=lambda symbol: f"{symbol.ticker}.{symbol.exchange}")
        client = FakeEODHDClient({
            "600519.SHG": self._rows(close="100"),
            "000001.SHE": self._rows(close="20"),
        })

        result = prepare_dynamic_universe_ohlc(backtest_id=self.backtest.id, client=client)

        self.assertEqual({call[0] for call in client.calls}, {"600519.SHG", "000001.SHE"})
        self.assertEqual(result.missing_after, [])
        self.assertEqual(DailyBar.objects.filter(symbol=she).count(), 2)

    def test_csi300_ohlc_prepare_auto_maps_unmapped_memberships_before_fake_eodhd_call(self):
        csi = Scenario.objects.create(name="CSI300", universe_mode=Scenario.UniverseMode.CSI300_HISTORICAL_DYNAMIC)
        self.backtest.scenario = csi
        self.backtest.save(update_fields=["scenario"])
        universe = UniverseDefinition.objects.create(code="CSI300", name="CSI 300", source="manual_csv", active=True)
        UniverseMembership.objects.create(
            universe=universe,
            symbol=None,
            ticker="600519",
            exchange="SHG",
            provider_symbol="600519.SHG",
            valid_from=self.start,
            valid_to=None,
            source="manual_csv",
            source_payload={"company_name": "Kweichow Moutai", "row": {"country": "CN", "currency": "CNY"}},
        )
        batch = UniverseImportBatch.objects.create(
            universe=universe,
            provider="manual_csv",
            source_name="manual_csv",
            period_start=self.start,
            period_end=self.end,
            expected_member_count=1,
            imported_member_count=1,
            mapped_member_count=0,
            unmapped_member_count=1,
            status=UniverseCoverageStatus.PARTIAL,
        )
        current = self.start
        while current <= self.end:
            UniverseCoverageSnapshot.objects.create(
                universe=universe,
                import_batch=batch,
                coverage_date=current,
                expected_member_count=1,
                actual_member_count=1,
                mapped_member_count=0,
                unmapped_member_count=1,
                status=UniverseCoverageStatus.PARTIAL,
            )
            current += timedelta(days=1)
        client = FakeEODHDClient({"600519.SHG": self._rows(close="100")})

        result = prepare_dynamic_universe_ohlc(backtest_id=self.backtest.id, client=client)

        self.assertEqual(client.calls, [("600519.SHG", self.start, self.end)])
        self.assertEqual(result.missing_after, [])
        membership = UniverseMembership.objects.get(universe=universe, ticker="600519")
        self.assertIsNotNone(membership.symbol_id)
        self.assertTrue(Symbol.objects.filter(ticker="600519", exchange="SHG", country="CN", currency="CNY").exists())
        batch.refresh_from_db()
        self.assertEqual(batch.status, UniverseCoverageStatus.VALIDATED)
        self.assertEqual(batch.unmapped_member_count, 0)

    def test_csi300_ohlc_prepare_builds_provider_symbol_from_exchange_when_missing(self):
        csi = Scenario.objects.create(name="CSI300", universe_mode=Scenario.UniverseMode.CSI300_HISTORICAL_DYNAMIC)
        self.backtest.scenario = csi
        self.backtest.save(update_fields=["scenario"])
        symbol = Symbol.objects.create(ticker="300750", exchange="SHE", instrument_type="Common Stock", active=True)
        self._validated_csi300(symbol)
        client = FakeEODHDClient({"300750.SHE": self._rows(close="30")})

        result = prepare_dynamic_universe_ohlc(backtest_id=self.backtest.id, client=client)

        self.assertEqual(client.calls, [("300750.SHE", self.start, self.end)])
        self.assertEqual(result.missing_after, [])

    def test_csi300_ohlc_prepare_skips_unsupported_exchange_without_provider_call(self):
        csi = Scenario.objects.create(name="CSI300", universe_mode=Scenario.UniverseMode.CSI300_HISTORICAL_DYNAMIC)
        self.backtest.scenario = csi
        self.backtest.save(update_fields=["scenario"])
        symbol = Symbol.objects.create(ticker="123456", exchange="HK", instrument_type="Common Stock", active=True)
        self._validated_csi300(symbol)
        client = FakeEODHDClient({})

        result = prepare_dynamic_universe_ohlc(backtest_id=self.backtest.id, client=client)

        self.assertEqual(client.calls, [])
        self.assertIn("123456", result.skipped_symbols)
        self.assertIn("Unsupported EODHD exchange mapping: HK", result.skipped_symbols["123456"])

    def test_csi300_ohlc_prepare_missing_universe_is_clear_without_provider_call(self):
        csi = Scenario.objects.create(name="CSI300", universe_mode=Scenario.UniverseMode.CSI300_HISTORICAL_DYNAMIC)
        self.backtest.scenario = csi
        self.backtest.save(update_fields=["scenario"])
        client = FakeEODHDClient({})

        with self.assertRaisesMessage(Exception, "UniverseDefinition CSI300 is missing or inactive"):
            prepare_dynamic_universe_ohlc(backtest_id=self.backtest.id, client=client)

        self.assertEqual(client.calls, [])


class DynamicUniverseOHLCPrepareJobTests(DynamicUniverseOHLCTestCase):
    def test_job_marks_done_when_preparation_completes(self):
        self._bars(self.ready)
        fake_client = FakeEODHDClient({
            "MISS.US": self._rows(),
            "EXTRA.US": self._rows(close="30"),
        })
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.FETCH_BARS,
            status=ProcessingJob.Status.PENDING,
            backtest=self.backtest,
            scenario=self.scenario,
        )

        with patch("core.services.dynamic_universe_ohlc_prepare.EODHDClient", return_value=fake_client), \
                patch("core.tasks._fetch_daily_bars_for_symbols") as twelvedata_fetch, \
                patch("core.tasks.run_backtest_task") as run_backtest:
            message = prepare_dynamic_universe_ohlc_job_task.apply(kwargs={
                "job_id": job.id,
                "backtest_id": self.backtest.id,
            }).get(propagate=True)

        job.refresh_from_db()
        self.assertEqual(job.status, ProcessingJob.Status.DONE)
        self.assertIn("Dynamic Universe OHLC preparation", message)
        self.assertIn("missing_after=0", job.message)
        self.assertTrue(job.last_checkpoint.startswith("dynamic_universe_ohlc"))
        twelvedata_fetch.assert_not_called()
        run_backtest.assert_not_called()

    def test_job_marks_done_with_warning_when_some_prices_remain_missing(self):
        self._bars(self.ready)
        self._bars(self.extra)
        fake_client = FakeEODHDClient({"MISS.US": []})
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.FETCH_BARS,
            status=ProcessingJob.Status.PENDING,
            backtest=self.backtest,
            scenario=self.scenario,
        )

        with patch("core.services.dynamic_universe_ohlc_prepare.EODHDClient", return_value=fake_client), \
                patch("core.tasks._fetch_daily_bars_for_symbols") as twelvedata_fetch:
            message = prepare_dynamic_universe_ohlc_job_task.apply(kwargs={
                "job_id": job.id,
                "backtest_id": self.backtest.id,
            }).get(propagate=True)

        job.refresh_from_db()
        self.assertEqual(job.status, ProcessingJob.Status.DONE)
        self.assertIn("WARNING", message)
        self.assertIn("2 actions sur 3", job.message)
        self.assertIn("MISS", job.message)
        self.assertEqual(job.error, "")
        twelvedata_fetch.assert_not_called()

    def test_job_warning_message_lists_real_missing_symbols(self):
        self._bars(self.ready)
        self._bars(self.extra)
        for ticker in ("AGN", "BF.B", "BRK.B"):
            self._add_universe_symbol(ticker)
        fake_client = FakeEODHDClient({"MISS.US": [], "AGN.US": [], "BF.B.US": [], "BRK.B.US": []})
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.FETCH_BARS,
            status=ProcessingJob.Status.PENDING,
            backtest=self.backtest,
            scenario=self.scenario,
        )

        with patch("core.services.dynamic_universe_ohlc_prepare.EODHDClient", return_value=fake_client), \
                patch("core.tasks._fetch_daily_bars_for_symbols") as twelvedata_fetch:
            prepare_dynamic_universe_ohlc_job_task.apply(kwargs={
                "job_id": job.id,
                "backtest_id": self.backtest.id,
            }).get(propagate=True)

        job.refresh_from_db()
        self.assertEqual(job.status, ProcessingJob.Status.DONE)
        self.assertIn("WARNING", job.message)
        self.assertIn("actions sur", job.message)
        self.assertIn("AGN", job.message)
        self.assertIn("BF.B", job.message)
        self.assertIn("BRK.B", job.message)
        twelvedata_fetch.assert_not_called()

    def test_job_marks_failed_when_no_member_prices_are_usable(self):
        fake_client = FakeEODHDClient({"READY.US": [], "MISS.US": [], "EXTRA.US": []})
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.FETCH_BARS,
            status=ProcessingJob.Status.PENDING,
            backtest=self.backtest,
            scenario=self.scenario,
        )

        with patch("core.services.dynamic_universe_ohlc_prepare.EODHDClient", return_value=fake_client), \
                patch("core.tasks._fetch_daily_bars_for_symbols") as twelvedata_fetch:
            message = prepare_dynamic_universe_ohlc_job_task.apply(kwargs={
                "job_id": job.id,
                "backtest_id": self.backtest.id,
            }).get(propagate=True)

        job.refresh_from_db()
        self.assertEqual(job.status, ProcessingJob.Status.FAILED)
        self.assertIn("no usable member prices", message)
        self.assertIn("READY", job.error)
        twelvedata_fetch.assert_not_called()

    def test_job_passes_exclude_tickers_and_does_not_fetch_excluded_symbols(self):
        self._bars(self.ready)
        self._bars(self.extra)
        fake_client = FakeEODHDClient({"MISS.US": self._rows()})
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.FETCH_BARS,
            status=ProcessingJob.Status.PENDING,
            backtest=self.backtest,
            scenario=self.scenario,
        )

        with patch("core.services.dynamic_universe_ohlc_prepare.EODHDClient", return_value=fake_client), \
                patch("core.tasks._fetch_daily_bars_for_symbols") as twelvedata_fetch, \
                patch("core.tasks.run_backtest_task") as run_backtest:
            message = prepare_dynamic_universe_ohlc_job_task.apply(kwargs={
                "job_id": job.id,
                "backtest_id": self.backtest.id,
                "exclude_tickers": ["MISS"],
            }).get(propagate=True)

        job.refresh_from_db()
        self.assertEqual(fake_client.calls, [])
        self.assertEqual(job.status, ProcessingJob.Status.DONE)
        self.assertIn("WARNING", message)
        self.assertIn("MISS", job.message)
        self.assertEqual(job.error, "")
        self.assertIn("skipped=1", job.message)
        self.assertIn("excluded=1", job.message)
        twelvedata_fetch.assert_not_called()
        run_backtest.assert_not_called()
