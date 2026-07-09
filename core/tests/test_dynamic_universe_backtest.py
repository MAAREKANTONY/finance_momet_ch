from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from django.test import TestCase
from django.utils import timezone

from core.models import (
    Alert,
    Backtest,
    DailyBar,
    DailyMetric,
    ProcessingJob,
    Scenario,
    Symbol,
    UniverseCoverageSnapshot,
    UniverseCoverageStatus,
    UniverseDefinition,
    UniverseImportBatch,
    UniverseMembership,
)
from core.services.backtesting.engine import run_backtest, run_backtest_kpi_only
from core.services.backtesting.prep import _missing_bar_coverage_symbols, _static_ohlc_coverage_diagnostic, prepare_backtest_data
from core.services.metrics_depth import check_metrics_depth
from core.services.provider_eodhd import EODHDError
from core.services.universe_resolver import UniverseResolver
from core.tasks import DYNAMIC_UNIVERSE_USER_ERROR, compute_metrics_job_task, run_backtest_task


class DynamicUniverseBacktestIntegrationTests(TestCase):
    def setUp(self):
        self.start = date(2024, 1, 1)
        self.end = date(2024, 1, 4)
        self.old = Symbol.objects.create(ticker="OLD", exchange="NYSE", sector="Tech", active=True)
        self.new = Symbol.objects.create(ticker="NEW", exchange="NYSE", sector="Energy", active=True)
        self.keep = Symbol.objects.create(ticker="KEEP", exchange="NASDAQ", sector="Health", active=True)

    def _scenario(self, *, dynamic: bool) -> Scenario:
        scenario = Scenario.objects.create(
            name="Dynamic" if dynamic else "Static",
            universe_mode=(
                Scenario.UniverseMode.SP500_HISTORICAL_DYNAMIC
                if dynamic
                else Scenario.UniverseMode.STATIC_TICKERS
            ),
            active=True,
            nglobal=2,
        )
        if not dynamic:
            scenario.symbols.add(self.old)
        return scenario

    def _backtest(self, scenario: Scenario, *, close_positions_at_end: bool = False) -> Backtest:
        return Backtest.objects.create(
            name="Dynamic universe backtest",
            scenario=scenario,
            start_date=self.start,
            end_date=self.end,
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            capital_mode=Backtest.CapitalMode.FIXED,
            ratio_threshold=Decimal("0"),
            include_all_tickers=True,
            signal_lines=[{"buy": ["A1"], "sell": ["B1"]}],
            universe_snapshot=[] if scenario.universe_mode != Scenario.UniverseMode.STATIC_TICKERS else [self.old.ticker],
            warmup_days=0,
            close_positions_at_end=close_positions_at_end,
        )

    def _bars_metrics(self, symbol: Symbol, closes: list[str] | None = None):
        closes = closes or ["10", "11", "12", "13"]
        for offset, raw_close in enumerate(closes):
            current = self.start + timedelta(days=offset)
            close = Decimal(raw_close)
            DailyBar.objects.create(
                symbol=symbol,
                date=current,
                open=close,
                high=close,
                low=close,
                close=close,
                volume=1000 + offset,
            )
            DailyMetric.objects.create(
                symbol=symbol,
                scenario=self.scenario,
                date=current,
                P=close,
                ratio_P=Decimal("1"),
            )

    def _market_data(self):
        for symbol in [self.old, self.new, self.keep]:
            self._bars_metrics(symbol)

    def _validated_sp500_universe(self):
        universe = UniverseDefinition.objects.create(
            code="SP500",
            name="S&P 500",
            source="test",
            active=True,
        )
        UniverseMembership.objects.create(
            universe=universe,
            symbol=self.old,
            ticker=self.old.ticker,
            exchange=self.old.exchange,
            provider_symbol="OLD.US",
            valid_from=self.start,
            valid_to=self.start + timedelta(days=1),
            source="test",
        )
        UniverseMembership.objects.create(
            universe=universe,
            symbol=self.keep,
            ticker=self.keep.ticker,
            exchange=self.keep.exchange,
            provider_symbol="KEEP.US",
            valid_from=self.start,
            valid_to=None,
            source="test",
        )
        UniverseMembership.objects.create(
            universe=universe,
            symbol=self.new,
            ticker=self.new.ticker,
            exchange=self.new.exchange,
            provider_symbol="NEW.US",
            valid_from=self.start + timedelta(days=2),
            valid_to=None,
            source="test",
        )
        batch = UniverseImportBatch.objects.create(
            universe=universe,
            provider="test",
            source_name="test",
            period_start=self.start,
            period_end=self.end,
            expected_member_count=1,
            imported_member_count=3,
            mapped_member_count=3,
            unmapped_member_count=0,
            status=UniverseCoverageStatus.VALIDATED,
            validated_at=timezone.now(),
        )
        for offset in range((self.end - self.start).days + 1):
            current = self.start + timedelta(days=offset)
            UniverseCoverageSnapshot.objects.create(
                universe=universe,
                import_batch=batch,
                coverage_date=current,
                expected_member_count=1,
                actual_member_count=2,
                mapped_member_count=2,
                unmapped_member_count=0,
                status=UniverseCoverageStatus.VALIDATED,
            )
        return universe

    def _validated_csi300_universe(self):
        universe = UniverseDefinition.objects.create(
            code="CSI300",
            name="CSI 300",
            source="manual_csv",
            active=True,
        )
        UniverseMembership.objects.create(
            universe=universe,
            symbol=self.old,
            ticker=self.old.ticker,
            exchange=self.old.exchange,
            provider_symbol="",
            valid_from=self.start,
            valid_to=self.start + timedelta(days=1),
            source="manual_csv",
        )
        UniverseMembership.objects.create(
            universe=universe,
            symbol=self.keep,
            ticker=self.keep.ticker,
            exchange=self.keep.exchange,
            provider_symbol="",
            valid_from=self.start,
            valid_to=None,
            source="manual_csv",
        )
        UniverseMembership.objects.create(
            universe=universe,
            symbol=self.new,
            ticker=self.new.ticker,
            exchange=self.new.exchange,
            provider_symbol="",
            valid_from=self.start + timedelta(days=2),
            valid_to=None,
            source="manual_csv",
        )
        batch = UniverseImportBatch.objects.create(
            universe=universe,
            provider="manual_csv",
            source_name="manual_csv",
            period_start=self.start,
            period_end=self.end,
            expected_member_count=1,
            imported_member_count=3,
            mapped_member_count=3,
            unmapped_member_count=0,
            status=UniverseCoverageStatus.VALIDATED,
            validated_at=timezone.now(),
        )
        for offset in range((self.end - self.start).days + 1):
            current = self.start + timedelta(days=offset)
            UniverseCoverageSnapshot.objects.create(
                universe=universe,
                import_batch=batch,
                coverage_date=current,
                expected_member_count=1,
                actual_member_count=2,
                mapped_member_count=2,
                unmapped_member_count=0,
                status=UniverseCoverageStatus.VALIDATED,
            )
        return universe

    def _fake_prep(self):
        return SimpleNamespace(did_fetch_bars=False, did_compute_metrics=False, notes=["test prep"])

    def test_static_backtest_does_not_use_dynamic_resolver_or_add_universe_meta(self):
        self.scenario = self._scenario(dynamic=False)
        self._market_data()
        Alert.objects.create(symbol=self.old, scenario=self.scenario, date=self.start, alerts="A1")
        bt = self._backtest(self.scenario, close_positions_at_end=True)

        with patch("core.services.universe_resolver.UniverseResolver.resolve") as resolver_mock:
            with patch("core.services.backtesting.prep.prepare_backtest_data", return_value=self._fake_prep()):
                run_backtest_task(bt.id)

        resolver_mock.assert_not_called()
        bt.refresh_from_db()
        self.assertEqual(bt.status, Backtest.Status.DONE)
        self.assertEqual(bt.universe_snapshot, [self.old.ticker])
        self.assertNotIn("universe", bt.results["meta"])
        old_daily = bt.results["tickers"]["OLD"]["lines"][0]["daily"]
        self.assertTrue(old_daily)
        self.assertNotIn("universe_member", old_daily[0])
        self.assertNotIn("buy_blocked_by_universe", old_daily[0])
        self.assertNotIn("buy_blocked_reason", old_daily[0])

    def test_dynamic_backtest_missing_universe_does_not_call_provider_sync(self):
        self.scenario = self._scenario(dynamic=True)
        bt = self._backtest(self.scenario)

        with patch("core.services.dynamic_universe_auto_prepare.bootstrap_sp500_symbols_from_eodhd") as bootstrap_mock:
            with patch("core.services.dynamic_universe_auto_prepare.sync_sp500_historical_memberships_from_eodhd") as sync_mock:
                with patch("core.services.backtesting.prep.prepare_backtest_data") as prep_mock:
                    with self.assertRaisesMessage(Exception, DYNAMIC_UNIVERSE_USER_ERROR):
                        run_backtest_task(bt.id)

        bootstrap_mock.assert_not_called()
        sync_mock.assert_not_called()
        prep_mock.assert_not_called()
        bt.refresh_from_db()
        self.assertEqual(bt.status, Backtest.Status.FAILED)
        self.assertEqual(bt.error_message, DYNAMIC_UNIVERSE_USER_ERROR)

    def test_dynamic_backtest_ready_universe_continues_without_provider_sync(self):
        self.scenario = self._scenario(dynamic=True)
        self._validated_sp500_universe()
        self._market_data()
        Alert.objects.create(symbol=self.keep, scenario=self.scenario, date=self.start, alerts="A1")
        bt = self._backtest(self.scenario, close_positions_at_end=True)

        with patch("core.services.dynamic_universe_auto_prepare.bootstrap_sp500_symbols_from_eodhd") as bootstrap_mock:
            with patch("core.services.dynamic_universe_auto_prepare.sync_sp500_historical_memberships_from_eodhd") as sync_mock:
                with patch("core.services.backtesting.prep.prepare_backtest_data", return_value=self._fake_prep()):
                    run_backtest_task(bt.id)

        bootstrap_mock.assert_not_called()
        sync_mock.assert_not_called()
        bt.refresh_from_db()
        self.assertEqual(bt.status, Backtest.Status.DONE)
        self.assertEqual({row["ticker"] for row in bt.universe_snapshot}, {"KEEP", "NEW", "OLD"})

    def test_csi300_dynamic_backtest_uses_csv_universe_without_provider_sync(self):
        self.scenario = Scenario.objects.create(
            name="Dynamic CSI300",
            universe_mode=Scenario.UniverseMode.CSI300_HISTORICAL_DYNAMIC,
            active=True,
            nglobal=2,
        )
        self._validated_csi300_universe()
        self._market_data()
        Alert.objects.create(symbol=self.keep, scenario=self.scenario, date=self.start, alerts="A1")
        bt = self._backtest(self.scenario, close_positions_at_end=True)

        with patch("core.services.dynamic_universe_auto_prepare.bootstrap_sp500_symbols_from_eodhd") as bootstrap_mock:
            with patch("core.services.dynamic_universe_auto_prepare.sync_sp500_historical_memberships_from_eodhd") as sync_mock:
                with patch("core.services.backtesting.prep.prepare_backtest_data", return_value=self._fake_prep()):
                    run_backtest_task(bt.id)

        bootstrap_mock.assert_not_called()
        sync_mock.assert_not_called()
        bt.refresh_from_db()
        self.assertEqual(bt.status, Backtest.Status.DONE)
        self.assertEqual({row["ticker"] for row in bt.universe_snapshot}, {"KEEP", "NEW", "OLD"})
        self.assertEqual(bt.results["meta"]["universe"]["universe_code"], "CSI300")

    def test_dynamic_backtest_missing_coverage_fails_without_provider_sync(self):
        self.scenario = self._scenario(dynamic=True)
        UniverseDefinition.objects.create(code="SP500", name="S&P 500", active=True)
        bt = self._backtest(self.scenario)

        with patch("core.services.dynamic_universe_auto_prepare.bootstrap_sp500_symbols_from_eodhd") as bootstrap_mock:
            with patch("core.services.dynamic_universe_auto_prepare.sync_sp500_historical_memberships_from_eodhd") as sync_mock:
                with patch("core.services.backtesting.prep.prepare_backtest_data") as prep_mock:
                    with self.assertRaisesMessage(Exception, DYNAMIC_UNIVERSE_USER_ERROR):
                        run_backtest_task(bt.id)

        bootstrap_mock.assert_not_called()
        sync_mock.assert_not_called()
        prep_mock.assert_not_called()
        bt.refresh_from_db()
        self.assertEqual(bt.status, Backtest.Status.FAILED)

    def test_static_compute_metrics_without_symbols_still_blocks(self):
        self.scenario = Scenario.objects.create(
            name="Static empty",
            universe_mode=Scenario.UniverseMode.STATIC_TICKERS,
            active=True,
        )
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.COMPUTE_METRICS,
            status=ProcessingJob.Status.PENDING,
            scenario=self.scenario,
        )

        message = compute_metrics_job_task.run(
            job_id=job.id,
            scenario_id=self.scenario.id,
            symbol_ids=None,
            recompute_all=False,
        )

        job.refresh_from_db()
        self.assertEqual(job.status, ProcessingJob.Status.DONE)
        self.assertEqual(message, "No symbols linked to this scenario (nothing to compute).")

    def test_dynamic_compute_metrics_without_manual_symbols_uses_resolved_universe(self):
        self.scenario = self._scenario(dynamic=True)
        self._validated_sp500_universe()
        bt = self._backtest(self.scenario)
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.COMPUTE_METRICS,
            status=ProcessingJob.Status.PENDING,
            scenario=self.scenario,
            backtest=bt,
        )

        with patch("core.tasks._compute_metrics_for_scenario", return_value={"symbols": 3, "rows": 0, "full": True}) as compute_mock:
            message = compute_metrics_job_task.run(
                job_id=job.id,
                scenario_id=self.scenario.id,
                symbol_ids=None,
                recompute_all=True,
                backtest_id=bt.id,
            )

        compute_mock.assert_called_once()
        symbols_qs = compute_mock.call_args.kwargs["symbols_qs"]
        self.assertEqual({symbol.ticker for symbol in symbols_qs}, {"KEEP", "NEW", "OLD"})
        self.assertIn("dynamic_sp500=3", message)
        self.assertNotIn("No symbols linked", message)
        job.refresh_from_db()
        self.assertEqual(job.status, ProcessingJob.Status.DONE)

    def test_dynamic_compute_metrics_without_validated_universe_uses_friendly_error(self):
        self.scenario = self._scenario(dynamic=True)
        bt = self._backtest(self.scenario)
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.COMPUTE_METRICS,
            status=ProcessingJob.Status.PENDING,
            scenario=self.scenario,
            backtest=bt,
        )

        with patch("core.services.dynamic_universe_auto_prepare.bootstrap_sp500_symbols_from_eodhd") as bootstrap_mock:
            with patch("core.services.dynamic_universe_auto_prepare.sync_sp500_historical_memberships_from_eodhd") as sync_mock:
                with patch("core.tasks._compute_metrics_for_scenario") as compute_mock:
                    with self.assertRaisesMessage(ValueError, DYNAMIC_UNIVERSE_USER_ERROR):
                        compute_metrics_job_task.run(
                            job_id=job.id,
                            scenario_id=self.scenario.id,
                            symbol_ids=None,
                            recompute_all=True,
                            backtest_id=bt.id,
                        )

        bootstrap_mock.assert_not_called()
        sync_mock.assert_not_called()
        compute_mock.assert_not_called()
        job.refresh_from_db()
        self.assertEqual(job.status, ProcessingJob.Status.FAILED)
        self.assertEqual(job.error, DYNAMIC_UNIVERSE_USER_ERROR)

    def test_dynamic_compute_metrics_missing_coverage_does_not_auto_prepare(self):
        self.scenario = self._scenario(dynamic=True)
        UniverseDefinition.objects.create(code="SP500", name="S&P 500", active=True)
        bt = self._backtest(self.scenario)
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.COMPUTE_METRICS,
            status=ProcessingJob.Status.PENDING,
            scenario=self.scenario,
            backtest=bt,
        )

        with patch("core.services.dynamic_universe_auto_prepare.bootstrap_sp500_symbols_from_eodhd") as bootstrap_mock:
            with patch("core.services.dynamic_universe_auto_prepare.sync_sp500_historical_memberships_from_eodhd") as sync_mock:
                with patch("core.tasks._compute_metrics_for_scenario") as compute_mock:
                    with self.assertRaisesMessage(ValueError, DYNAMIC_UNIVERSE_USER_ERROR):
                        compute_metrics_job_task.run(
                            job_id=job.id,
                            scenario_id=self.scenario.id,
                            symbol_ids=None,
                            recompute_all=True,
                            backtest_id=bt.id,
                        )

        bootstrap_mock.assert_not_called()
        sync_mock.assert_not_called()
        compute_mock.assert_not_called()
        job.refresh_from_db()
        self.assertEqual(job.status, ProcessingJob.Status.FAILED)

    def test_dynamic_task_sets_superset_snapshot_and_metadata(self):
        self.scenario = self._scenario(dynamic=True)
        self._validated_sp500_universe()
        self._market_data()
        Alert.objects.create(symbol=self.keep, scenario=self.scenario, date=self.start, alerts="A1")
        bt = self._backtest(self.scenario, close_positions_at_end=True)

        def assert_dynamic_snapshot(backtest):
            backtest.refresh_from_db()
            self.assertEqual({row["ticker"] for row in backtest.universe_snapshot}, {"KEEP", "NEW", "OLD"})
            return self._fake_prep()

        with patch("core.services.backtesting.prep.prepare_backtest_data", side_effect=assert_dynamic_snapshot):
            run_backtest_task(bt.id)

        bt.refresh_from_db()
        self.assertEqual(bt.status, Backtest.Status.DONE)
        self.assertEqual({row["ticker"] for row in bt.universe_snapshot}, {"KEEP", "NEW", "OLD"})
        universe_meta = bt.results["meta"]["universe"]
        self.assertEqual(universe_meta["mode"], Scenario.UniverseMode.SP500_HISTORICAL_DYNAMIC)
        self.assertEqual(universe_meta["universe_code"], "SP500")
        self.assertEqual(universe_meta["superset_count"], 3)
        self.assertEqual(universe_meta["coverage_start"], self.start.isoformat())
        self.assertEqual(universe_meta["coverage_end"], self.end.isoformat())

    def test_prepare_backtest_data_dynamic_missing_ohlc_warns_without_hidden_fetch(self):
        self.scenario = self._scenario(dynamic=True)
        self._validated_sp500_universe()
        self._bars_metrics(self.keep)
        bt = self._backtest(self.scenario)
        resolved = UniverseResolver().resolve(self.scenario, self.start, self.end)
        bt.universe_snapshot = [
            {"ticker": symbol.ticker, "exchange": symbol.exchange, "sector": symbol.sector}
            for symbol in resolved.symbols
        ]
        bt.save(update_fields=["universe_snapshot"])

        with patch("core.tasks.fetch_daily_bars_task") as fetch_mock:
            with patch("core.tasks._compute_metrics_for_scenario") as compute_mock:
                report = prepare_backtest_data(bt)

        fetch_mock.assert_not_called()
        compute_mock.assert_called_once()
        self.assertTrue(report.did_compute_metrics)
        self.assertFalse(report.did_fetch_bars)
        self.assertTrue(any("actions n'ont pas de prix" in note for note in report.notes))
        self.assertTrue(any("seront ignorées" in note for note in report.notes))

    def test_prepare_backtest_data_static_mode_blocks_only_when_no_exploitable_ohlc(self):
        self.scenario = self._scenario(dynamic=False)
        bt = self._backtest(self.scenario)

        with patch("core.services.backtesting.prep.ensure_ohlc_ready_for_backtest") as readiness_mock:
            with patch("core.tasks.fetch_daily_bars_task") as fetch_mock:
                with patch("core.tasks._compute_metrics_for_scenario") as compute_mock:
                    with self.assertRaisesMessage(ValueError, "aucun prix exploitable"):
                        prepare_backtest_data(bt)

        readiness_mock.assert_not_called()
        fetch_mock.assert_not_called()
        compute_mock.assert_not_called()

    def test_prepare_backtest_data_static_mode_complete_ohlc_does_not_fetch(self):
        self.scenario = self._scenario(dynamic=False)
        self._bars_metrics(self.old)
        bt = self._backtest(self.scenario)

        with patch("core.services.backtesting.prep.ensure_ohlc_ready_for_backtest") as readiness_mock:
            with patch("core.tasks.fetch_daily_bars_task") as fetch_mock:
                with patch("core.tasks._compute_metrics_for_scenario") as compute_mock:
                    report = prepare_backtest_data(bt)

        readiness_mock.assert_not_called()
        fetch_mock.assert_not_called()
        compute_mock.assert_not_called()
        self.assertFalse(report.did_fetch_bars)
        self.assertFalse(report.did_compute_metrics)

    def test_static_missing_bar_coverage_uses_grouped_result(self):
        self.scenario = self._scenario(dynamic=False)
        self.scenario.symbols.add(self.new)
        self._bars_metrics(self.old)
        missing = _missing_bar_coverage_symbols(
            self.scenario.symbols.order_by("ticker"),
            self.start,
            self.end,
        )
        self.assertEqual(missing, ["NEW"])

    def test_metrics_depth_uses_effective_bounds_for_non_trading_dates(self):
        self.scenario = self._scenario(dynamic=False)
        self.scenario.symbols.add(self.new)
        requested_start = self.start - timedelta(days=1)
        requested_end = self.end + timedelta(days=1)
        for symbol in (self.old, self.new):
            for offset in range((self.end - self.start).days + 1):
                DailyMetric.objects.create(
                    symbol=symbol,
                    scenario=self.scenario,
                    date=self.start + timedelta(days=offset),
                    P=Decimal("10"),
                    ratio_P=Decimal("1"),
                )

        report = check_metrics_depth(
            scenario_id=self.scenario.id,
            symbol_ids=list(self.scenario.symbols.values_list("id", flat=True)),
            required_start=requested_start,
            required_end=requested_end,
        )

        self.assertEqual(report.covered_symbols, 2)
        self.assertEqual(report.missing_symbol_ids, [])
        self.assertEqual(report.effective_start, self.start)
        self.assertEqual(report.effective_end, self.end)
        self.assertFalse(report.needs_full_recompute())

    def test_metrics_depth_detects_true_missing_metrics(self):
        self.scenario = self._scenario(dynamic=False)
        self.scenario.symbols.add(self.new)
        for offset in range((self.end - self.start).days + 1):
            DailyMetric.objects.create(
                symbol=self.old,
                scenario=self.scenario,
                date=self.start + timedelta(days=offset),
                P=Decimal("10"),
                ratio_P=Decimal("1"),
            )

        report = check_metrics_depth(
            scenario_id=self.scenario.id,
            symbol_ids=list(self.scenario.symbols.values_list("id", flat=True)),
            required_start=self.start,
            required_end=self.end,
        )

        self.assertIn(self.new.id, report.no_metrics_at_all_symbol_ids)
        self.assertIn(self.new.id, report.missing_symbol_ids)
        self.assertTrue(report.needs_full_recompute())

    def test_static_ohlc_diagnostic_uses_effective_bounds_for_non_trading_dates(self):
        self.scenario = self._scenario(dynamic=False)
        self.scenario.symbols.add(self.new)
        for symbol in (self.old, self.new):
            self._bars_metrics(symbol, closes=["10", "11", "12", "13"])
        requested_start = self.start - timedelta(days=1)
        requested_end = self.end + timedelta(days=1)

        report = _static_ohlc_coverage_diagnostic(
            self.scenario.symbols.order_by("ticker"),
            requested_start,
            requested_end,
        )

        self.assertFalse(report.has_issues)
        self.assertEqual(report.effective_start, self.start)
        self.assertEqual(report.effective_end, self.end)
        self.assertEqual(set(report.ok), {"NEW", "OLD"})

    def test_static_ohlc_diagnostic_classifies_missing_ranges(self):
        self.scenario = self._scenario(dynamic=False)
        missing_end = Symbol.objects.create(ticker="END", exchange="NYSE", active=True)
        no_range = Symbol.objects.create(ticker="RANGE", exchange="NYSE", active=True)
        no_bars = Symbol.objects.create(ticker="EMPTY", exchange="NYSE", active=True)
        self.scenario.symbols.add(self.new, missing_end, no_range, no_bars)
        self._bars_metrics(self.old, closes=["10", "11", "12", "13"])
        self._bars_metrics(self.new, closes=["11", "12", "13", "14"])
        DailyBar.objects.filter(symbol=self.new, date__in=[self.start, self.start + timedelta(days=1)]).delete()
        self._bars_metrics(missing_end, closes=["12", "13", "14", "15"])
        DailyBar.objects.filter(symbol=missing_end, date__gte=self.end).delete()
        DailyBar.objects.create(
            symbol=no_range,
            date=self.end + timedelta(days=10),
            open=Decimal("10"),
            high=Decimal("10"),
            low=Decimal("10"),
            close=Decimal("10"),
            volume=1000,
        )

        report = _static_ohlc_coverage_diagnostic(
            self.scenario.symbols.order_by("ticker"),
            self.start,
            self.end,
        )

        self.assertIn("OLD", report.ok)
        self.assertIn("NEW", report.missing_start)
        self.assertIn("END", report.missing_end)
        self.assertIn("RANGE", report.no_bars_in_range)
        self.assertIn("EMPTY", report.no_bars_at_all)
        ranges = {(item.ticker, item.reason, item.start, item.end) for item in report.missing_ranges}
        self.assertIn(("NEW", "MISSING_START", self.start, self.start + timedelta(days=1)), ranges)
        self.assertIn(("END", "MISSING_END", self.end, self.end), ranges)
        self.assertIn(("RANGE", "NO_BARS_IN_RANGE", self.start, self.end), ranges)
        self.assertIn(("EMPTY", "NO_BARS_AT_ALL", self.start, self.end), ranges)

    def test_prepare_backtest_data_static_partial_metrics_warns_without_mass_recompute(self):
        self.scenario = self._scenario(dynamic=False)
        self.scenario.symbols.add(self.new)
        for symbol in (self.old, self.new):
            self._bars_metrics(symbol)
        DailyMetric.objects.filter(symbol=self.new, scenario=self.scenario, date__in=[self.start, self.start + timedelta(days=1)]).delete()
        bt = self._backtest(self.scenario)
        bt.universe_snapshot = [self.old.ticker, self.new.ticker]
        bt.save(update_fields=["universe_snapshot"])

        with patch("core.tasks._compute_metrics_for_scenario") as compute_mock:
            report = prepare_backtest_data(bt)

        compute_mock.assert_not_called()
        self.assertFalse(report.did_compute_metrics)
        self.assertTrue(any("couverture indicateurs partielle" in note for note in report.notes))
        self.assertTrue(any("Aucun recalcul massif" in note for note in report.notes))

    def test_prepare_backtest_data_static_partial_ohlc_warns_without_global_fetch(self):
        self.scenario = self._scenario(dynamic=False)
        self.scenario.symbols.add(self.new)
        self._bars_metrics(self.old)
        for offset in range((self.end - self.start).days + 1):
            DailyMetric.objects.create(
                symbol=self.new,
                scenario=self.scenario,
                date=self.start + timedelta(days=offset),
                P=Decimal("10"),
                ratio_P=Decimal("1"),
            )
        bt = self._backtest(self.scenario)
        bt.universe_snapshot = [self.old.ticker, self.new.ticker]
        bt.save(update_fields=["universe_snapshot"])

        with patch("core.tasks.fetch_daily_bars_task") as fetch_mock:
            with patch("core.tasks._compute_metrics_for_scenario") as compute_mock:
                report = prepare_backtest_data(bt)

        fetch_mock.assert_not_called()
        compute_mock.assert_not_called()
        self.assertFalse(report.did_fetch_bars)
        self.assertTrue(any("Attention : couverture prix partielle" in note for note in report.notes))
        message = "\\n".join(report.notes)
        self.assertIn("Le backtest est lancé sur les données disponibles", message)
        self.assertIn("n’ont aucun prix dans la période", message)
        self.assertIn("Trigger > Télécharger les prix des actions", message)

    def test_prepare_backtest_data_skips_metrics_recompute_when_ohlc_missing_in_range(self):
        self.scenario = self._scenario(dynamic=False)
        self.scenario.symbols.add(self.new)
        self._bars_metrics(self.old)
        DailyBar.objects.create(
            symbol=self.new,
            date=self.end + timedelta(days=10),
            open=Decimal("10"),
            high=Decimal("10"),
            low=Decimal("10"),
            close=Decimal("10"),
            volume=1000,
        )
        bt = self._backtest(self.scenario)
        bt.universe_snapshot = [self.old.ticker, self.new.ticker]
        bt.save(update_fields=["universe_snapshot"])

        with patch("core.tasks._compute_metrics_for_scenario") as compute_mock:
            report = prepare_backtest_data(bt)

        compute_mock.assert_not_called()
        self.assertFalse(report.did_compute_metrics)
        message = "\n".join(report.notes)
        self.assertIn("Certaines métriques ne sont pas recalculées", message)
        self.assertIn("Récupérez d’abord les prix manquants", message)
        self.assertIn("NEW", message)

    def test_prepare_backtest_data_skips_metrics_recompute_when_ohlc_missing_entirely(self):
        self.scenario = self._scenario(dynamic=False)
        self.scenario.symbols.add(self.new)
        self._bars_metrics(self.old)
        bt = self._backtest(self.scenario)
        bt.universe_snapshot = [self.old.ticker, self.new.ticker]
        bt.save(update_fields=["universe_snapshot"])

        with patch("core.tasks._compute_metrics_for_scenario") as compute_mock:
            report = prepare_backtest_data(bt)

        compute_mock.assert_not_called()
        self.assertFalse(report.did_compute_metrics)
        message = "\n".join(report.notes)
        self.assertIn("Certaines métriques ne sont pas recalculées", message)
        self.assertIn("Récupérez d’abord les prix manquants", message)
        self.assertIn("NEW", message)

    def test_prepare_backtest_data_recomputes_missing_metrics_when_ohlc_present(self):
        self.scenario = self._scenario(dynamic=False)
        self.scenario.symbols.add(self.new)
        self._bars_metrics(self.old)
        for offset in range((self.end - self.start).days + 1):
            close = Decimal("10")
            DailyBar.objects.create(
                symbol=self.new,
                date=self.start + timedelta(days=offset),
                open=close,
                high=close,
                low=close,
                close=close,
                volume=1000 + offset,
            )
        bt = self._backtest(self.scenario)
        bt.universe_snapshot = [self.old.ticker, self.new.ticker]
        bt.save(update_fields=["universe_snapshot"])

        with patch("core.tasks._compute_metrics_for_scenario") as compute_mock:
            report = prepare_backtest_data(bt)

        compute_mock.assert_called_once()
        kwargs = compute_mock.call_args.kwargs
        self.assertEqual(list(kwargs["symbols_qs"].values_list("ticker", flat=True)), ["NEW"])
        self.assertFalse(kwargs["recompute_all"])
        self.assertTrue(report.did_compute_metrics)

    def test_run_backtest_task_static_partial_ohlc_finishes_with_prep_warning(self):
        self.scenario = self._scenario(dynamic=False)
        self.scenario.symbols.add(self.new)
        self._bars_metrics(self.old)
        for offset in range((self.end - self.start).days + 1):
            DailyMetric.objects.create(
                symbol=self.new,
                scenario=self.scenario,
                date=self.start + timedelta(days=offset),
                P=Decimal("10"),
                ratio_P=Decimal("1"),
            )
        bt = self._backtest(self.scenario)
        bt.universe_snapshot = [self.old.ticker, self.new.ticker]
        bt.save(update_fields=["universe_snapshot"])

        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.RUN_BACKTEST,
            status=ProcessingJob.Status.RUNNING,
            backtest=bt,
            scenario=self.scenario,
            message="started",
        )

        with patch("core.tasks.fetch_daily_bars_task") as fetch_mock:
            msg = run_backtest_task(bt.id, job_id=job.id)

        fetch_mock.assert_not_called()
        self.assertEqual(msg, "ok")
        bt.refresh_from_db()
        self.assertEqual(bt.status, Backtest.Status.DONE)
        prep_notes = ((bt.results or {}).get("prep") or {}).get("notes") or []
        self.assertTrue(any("Attention : couverture prix partielle" in note for note in prep_notes))
        job.refresh_from_db()
        self.assertIn("[backtest timing] step=prepare_backtest_data", job.message)
        self.assertIn("[backtest timing] step=result_save", job.message)

    def test_dynamic_buy_gate_blocks_new_buy_after_exit(self):
        self.scenario = self._scenario(dynamic=True)
        self._validated_sp500_universe()
        self._market_data()
        Alert.objects.create(symbol=self.old, scenario=self.scenario, date=self.start + timedelta(days=2), alerts="A1")
        bt = self._backtest(self.scenario, close_positions_at_end=False)
        resolved = UniverseResolver().resolve(self.scenario, self.start, self.end)
        bt.universe_snapshot = [{"ticker": symbol.ticker, "exchange": symbol.exchange, "sector": symbol.sector} for symbol in resolved.symbols]
        bt.save(update_fields=["universe_snapshot"])

        results = run_backtest(bt, resolved_universe=resolved).results

        old_daily = results["tickers"]["OLD"]["lines"][0]["daily"]
        self.assertNotIn("BUY", {row.get("action") for row in old_daily})
        blocked_row = next(row for row in old_daily if row["date"] == (self.start + timedelta(days=2)).isoformat())
        self.assertIsNone(blocked_row["action"])
        self.assertEqual(blocked_row["shares"], 0)
        self.assertIs(blocked_row["universe_member"], False)
        self.assertIs(blocked_row["buy_blocked_by_universe"], True)
        self.assertEqual(blocked_row["buy_blocked_reason"], "not_active_in_universe")

    def test_dynamic_position_survives_exit_and_sell_after_exit_is_allowed(self):
        self.scenario = self._scenario(dynamic=True)
        self._validated_sp500_universe()
        self._market_data()
        Alert.objects.bulk_create([
            Alert(symbol=self.old, scenario=self.scenario, date=self.start, alerts="A1"),
            Alert(symbol=self.old, scenario=self.scenario, date=self.start + timedelta(days=2), alerts="B1"),
        ])
        bt = self._backtest(self.scenario, close_positions_at_end=False)
        resolved = UniverseResolver().resolve(self.scenario, self.start, self.end)
        bt.universe_snapshot = [{"ticker": symbol.ticker, "exchange": symbol.exchange, "sector": symbol.sector} for symbol in resolved.symbols]
        bt.save(update_fields=["universe_snapshot"])

        results = run_backtest(bt, resolved_universe=resolved).results

        old_daily = results["tickers"]["OLD"]["lines"][0]["daily"]
        self.assertEqual(old_daily[0]["action"], "BUY")
        self.assertGreater(old_daily[0]["shares"], 0)
        self.assertIs(old_daily[0]["universe_member"], True)
        self.assertIs(old_daily[0]["buy_blocked_by_universe"], False)
        self.assertIsNone(old_daily[0]["buy_blocked_reason"])
        self.assertIsNone(old_daily[1]["action"])
        self.assertGreater(old_daily[1]["shares"], 0)
        self.assertEqual(old_daily[2]["action"], "SELL")
        self.assertIs(old_daily[2]["universe_member"], False)
        self.assertIs(old_daily[2]["buy_blocked_by_universe"], False)
        self.assertIsNone(old_daily[2]["buy_blocked_reason"])
        self.assertFalse(old_daily[1]["forced_close"])

    def test_dynamic_position_stays_valued_after_exit_without_universe_block_flag(self):
        self.scenario = self._scenario(dynamic=True)
        self._validated_sp500_universe()
        self._market_data()
        Alert.objects.create(symbol=self.old, scenario=self.scenario, date=self.start, alerts="A1")
        bt = self._backtest(self.scenario, close_positions_at_end=False)
        resolved = UniverseResolver().resolve(self.scenario, self.start, self.end)
        bt.universe_snapshot = [{"ticker": symbol.ticker, "exchange": symbol.exchange, "sector": symbol.sector} for symbol in resolved.symbols]
        bt.save(update_fields=["universe_snapshot"])

        results = run_backtest(bt, resolved_universe=resolved).results

        old_daily = results["tickers"]["OLD"]["lines"][0]["daily"]
        post_exit_row = next(row for row in old_daily if row["date"] == (self.start + timedelta(days=2)).isoformat())
        self.assertIsNone(post_exit_row["action"])
        self.assertGreater(post_exit_row["shares"], 0)
        self.assertIs(post_exit_row["universe_member"], False)
        self.assertIs(post_exit_row["buy_blocked_by_universe"], False)
        self.assertIsNone(post_exit_row["buy_blocked_reason"])
        self.assertFalse(post_exit_row["forced_close"])

    def test_dynamic_no_signal_after_exit_is_not_marked_as_universe_blocked(self):
        self.scenario = self._scenario(dynamic=True)
        self._validated_sp500_universe()
        self._market_data()
        bt = self._backtest(self.scenario, close_positions_at_end=False)
        resolved = UniverseResolver().resolve(self.scenario, self.start, self.end)
        bt.universe_snapshot = [{"ticker": symbol.ticker, "exchange": symbol.exchange, "sector": symbol.sector} for symbol in resolved.symbols]
        bt.save(update_fields=["universe_snapshot"])

        results = run_backtest(bt, resolved_universe=resolved).results

        old_daily = results["tickers"]["OLD"]["lines"][0]["daily"]
        post_exit_row = next(row for row in old_daily if row["date"] == (self.start + timedelta(days=2)).isoformat())
        self.assertIsNone(post_exit_row["action"])
        self.assertEqual(post_exit_row["shares"], 0)
        self.assertIs(post_exit_row["universe_member"], False)
        self.assertIs(post_exit_row["buy_blocked_by_universe"], False)
        self.assertIsNone(post_exit_row["buy_blocked_reason"])

    def test_dynamic_forced_sell_final_is_unchanged_after_index_exit(self):
        self.scenario = self._scenario(dynamic=True)
        self._validated_sp500_universe()
        self._market_data()
        Alert.objects.create(symbol=self.old, scenario=self.scenario, date=self.start, alerts="A1")
        bt = self._backtest(self.scenario, close_positions_at_end=True)
        resolved = UniverseResolver().resolve(self.scenario, self.start, self.end)
        bt.universe_snapshot = [{"ticker": symbol.ticker, "exchange": symbol.exchange, "sector": symbol.sector} for symbol in resolved.symbols]
        bt.save(update_fields=["universe_snapshot"])

        results = run_backtest(bt, resolved_universe=resolved).results

        old_daily = results["tickers"]["OLD"]["lines"][0]["daily"]
        self.assertEqual(old_daily[0]["action"], "BUY")
        self.assertIn("FORCED_SELL", old_daily[-1]["action"])
        self.assertTrue(old_daily[-1]["forced_close"])

    def test_game_runtime_and_kpi_only_stay_out_of_dynamic_universe_integration(self):
        base = Path(__file__).resolve().parents[1]
        for relative in [
            "services/game_scenarios/runner.py",
            "services/game_scenarios/sync.py",
        ]:
            source = (base / relative).read_text()
            self.assertNotIn("UniverseResolver", source)
            self.assertNotIn("universe_resolver", source)
        self.assertEqual(run_backtest_kpi_only.__name__, "run_backtest_kpi_only")
