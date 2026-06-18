from __future__ import annotations

from io import StringIO
from unittest.mock import patch

from django.core.management import call_command
from django.test import TestCase

from core.models import (
    DailyBar,
    HistoricalMarketCap,
    ProcessingJob,
    Symbol,
    UniverseCoverageSnapshot,
    UniverseDefinition,
    UniverseMembership,
)


US_MARKET_ETFS = {"SPY", "QQQ", "DIA", "IWM"}
US_SECTOR_ETFS = {"XLK", "XLF", "XLE", "XLV", "XLY", "XLP", "XLI", "XLB", "XLU", "XLRE", "XLC"}
EUROPE_ETFS = {"VGK", "FEZ", "EZU", "EWU", "EWQ", "EWG", "EWI", "EWP", "EWN", "EWD", "EWL"}
REFERENCE_ETFS = US_MARKET_ETFS | US_SECTOR_ETFS | EUROPE_ETFS


class InitReferenceDataCommandTests(TestCase):
    def _call_command(self, *args) -> str:
        out = StringIO()
        call_command("init_reference_data", *args, stdout=out)
        return out.getvalue()

    def test_creates_minimal_sp500_definition_and_reference_symbols_on_empty_database(self):
        output = self._call_command()

        universe = UniverseDefinition.objects.get(code="SP500")
        self.assertEqual(universe.name, "S&P 500")
        self.assertEqual(universe.source, "reference_data")
        self.assertTrue(universe.active)
        self.assertEqual(universe.metadata["provider"], "eodhd")
        self.assertIn("SP500 UniverseDefinition created active=True", output)
        self.assertIn("US market ETFs: created=4", output)
        self.assertIn("US sector ETFs: created=11", output)
        self.assertIn("Europe ETFs: created=11", output)
        self.assertIn("memberships=0", output)

        symbols = Symbol.objects.filter(ticker__in=REFERENCE_ETFS, exchange="NYSE ARCA")
        self.assertEqual(symbols.count(), len(REFERENCE_ETFS))
        self.assertEqual(set(symbols.values_list("ticker", flat=True)), REFERENCE_ETFS)
        for symbol in symbols:
            self.assertEqual(symbol.instrument_type, "ETF")
            self.assertEqual(symbol.country, "US")
            self.assertEqual(symbol.currency, "USD")
            self.assertTrue(symbol.active)

        self.assertEqual(UniverseMembership.objects.count(), 0)
        self.assertEqual(UniverseCoverageSnapshot.objects.count(), 0)
        self.assertEqual(DailyBar.objects.count(), 0)
        self.assertEqual(HistoricalMarketCap.objects.count(), 0)
        self.assertEqual(ProcessingJob.objects.count(), 0)

    def test_is_idempotent(self):
        first_output = self._call_command()
        symbol_count_after_first = Symbol.objects.count()
        second_output = self._call_command()

        self.assertIn("created", first_output)
        self.assertIn("already exists active=True", second_output)
        self.assertIn("US market ETFs: created=0 existing=4", second_output)
        self.assertEqual(UniverseDefinition.objects.filter(code="SP500").count(), 1)
        self.assertEqual(Symbol.objects.count(), symbol_count_after_first)
        self.assertEqual(Symbol.objects.filter(ticker__in=REFERENCE_ETFS, exchange="NYSE ARCA").count(), len(REFERENCE_ETFS))
        self.assertEqual(UniverseMembership.objects.count(), 0)
        self.assertEqual(UniverseCoverageSnapshot.objects.count(), 0)

    def test_reactivates_existing_sp500_definition_without_duplicate(self):
        UniverseDefinition.objects.create(
            code="SP500",
            name="S&P 500",
            source="manual",
            active=False,
        )

        output = self._call_command()

        universe = UniverseDefinition.objects.get(code="SP500")
        self.assertTrue(universe.active)
        self.assertEqual(universe.source, "manual")
        self.assertIn("SP500 UniverseDefinition reactivated", output)
        self.assertEqual(UniverseDefinition.objects.filter(code="SP500").count(), 1)

    def test_existing_enriched_symbol_is_not_overwritten(self):
        Symbol.objects.create(
            ticker="SPY",
            exchange="NYSE ARCA",
            name="Custom SPY Name",
            instrument_type="Existing ETF",
            country="USA",
            currency="USD",
            sector="Existing Sector",
            active=True,
        )
        Symbol.objects.create(ticker="QQQ", exchange="NYSE ARCA", active=False)

        output = self._call_command()

        spy = Symbol.objects.get(ticker="SPY", exchange="NYSE ARCA")
        self.assertEqual(spy.name, "Custom SPY Name")
        self.assertEqual(spy.instrument_type, "Existing ETF")
        self.assertEqual(spy.country, "USA")
        self.assertEqual(spy.sector, "Existing Sector")

        qqq = Symbol.objects.get(ticker="QQQ", exchange="NYSE ARCA")
        self.assertTrue(qqq.active)
        self.assertEqual(qqq.name, "Invesco QQQ Trust")
        self.assertEqual(qqq.instrument_type, "ETF")
        self.assertIn("updated=1", output)

    def test_dry_run_does_not_create_reference_data(self):
        output = self._call_command("--dry-run")

        self.assertIn("would be created active=True", output)
        self.assertIn("mode=dry-run", output)
        self.assertIn("US market ETFs: created=0 existing=0 updated=0 would_create=4", output)
        self.assertFalse(UniverseDefinition.objects.filter(code="SP500").exists())
        self.assertEqual(Symbol.objects.count(), 0)
        self.assertEqual(UniverseMembership.objects.count(), 0)
        self.assertEqual(UniverseCoverageSnapshot.objects.count(), 0)

    @patch("core.services.symbol_enrichment.enrich_symbols_metadata")
    @patch("core.services.benchmark_etf_sync.sync_benchmark_etfs_for_symbols")
    def test_does_not_call_provider_or_sync_helpers(self, sync_mock, enrichment_mock):
        self._call_command()

        sync_mock.assert_not_called()
        enrichment_mock.assert_not_called()

    def test_does_not_create_heavy_data_or_jobs(self):
        self._call_command()

        self.assertEqual(UniverseMembership.objects.count(), 0)
        self.assertEqual(UniverseCoverageSnapshot.objects.count(), 0)
        self.assertEqual(DailyBar.objects.count(), 0)
        self.assertEqual(HistoricalMarketCap.objects.count(), 0)
        self.assertEqual(ProcessingJob.objects.count(), 0)
