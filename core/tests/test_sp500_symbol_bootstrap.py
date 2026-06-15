from __future__ import annotations

from datetime import date
from io import StringIO
from unittest.mock import patch

from django.core.management import call_command
from django.test import TestCase

from core.models import Symbol
from core.services.provider_eodhd import EODHDError
from core.services.sp500_symbol_bootstrap import bootstrap_sp500_symbols_from_eodhd


class FakeEODHDClient:
    def __init__(self, records=None, error: Exception | None = None):
        self.records = records or []
        self.error = error
        self.called = False

    def fetch_sp500_historical_components(self):
        self.called = True
        if self.error:
            raise self.error
        return self.records


def record(code, name=None, start="2020-01-01", end=None, active=1, delisted=0):
    return {
        "Code": code,
        "Name": name or f"{code} Corp",
        "StartDate": start,
        "EndDate": end,
        "IsActiveNow": active,
        "IsDelisted": delisted,
    }


class SP500SymbolBootstrapTests(TestCase):
    def _bootstrap(self, records, *, apply=False):
        return bootstrap_sp500_symbols_from_eodhd(
            coverage_start=date(2020, 1, 1),
            coverage_end=date(2020, 1, 3),
            dry_run=not apply,
            client=FakeEODHDClient(records),
        )

    def test_dry_run_does_not_create_symbols(self):
        result = self._bootstrap([record("AAPL"), record("MSFT")], apply=False)

        self.assertTrue(result.dry_run)
        self.assertEqual(result.provider_records, 2)
        self.assertEqual(result.records_retained, 2)
        self.assertEqual(result.to_create, 2)
        self.assertEqual(result.create_examples, ["AAPL", "MSFT"])
        self.assertEqual(Symbol.objects.count(), 0)

    def test_apply_creates_missing_symbols_with_us_defaults(self):
        result = self._bootstrap([record("AAPL", "Apple Inc"), record("MSFT", "Microsoft Corp")], apply=True)

        self.assertFalse(result.dry_run)
        self.assertEqual(result.created, 2)
        aapl = Symbol.objects.get(ticker="AAPL", exchange="US")
        self.assertEqual(aapl.name, "Apple Inc")
        self.assertEqual(aapl.country, "US")
        self.assertEqual(aapl.currency, "USD")
        self.assertEqual(aapl.instrument_type, "Common Stock")
        self.assertTrue(aapl.active)

    def test_apply_is_idempotent_and_does_not_modify_existing_symbol(self):
        existing = Symbol.objects.create(ticker="AAPL", exchange="NASDAQ", name="Existing Apple", active=True)

        first = self._bootstrap([record("AAPL", "Provider Apple"), record("MSFT")], apply=True)
        second = self._bootstrap([record("AAPL", "Provider Apple"), record("MSFT")], apply=True)

        self.assertEqual(first.created, 1)
        self.assertEqual(second.created, 0)
        self.assertEqual(Symbol.objects.filter(ticker="AAPL").count(), 1)
        existing.refresh_from_db()
        self.assertEqual(existing.name, "Existing Apple")
        self.assertEqual(Symbol.objects.filter(ticker="MSFT", exchange="US").count(), 1)

    def test_records_outside_period_are_ignored(self):
        result = self._bootstrap([
            record("OLD", start="2010-01-01", end="2019-12-31"),
            record("FUT", start="2020-01-04", end=None),
            record("AAPL", start="2020-01-01", end=None),
        ], apply=True)

        self.assertEqual(result.created, 1)
        self.assertEqual(result.skipped, 2)
        self.assertEqual(set(Symbol.objects.values_list("ticker", flat=True)), {"AAPL"})

    def test_missing_start_date_warns_and_can_create(self):
        result = self._bootstrap([record("OLD", start=None, end="2020-01-02", active=0)], apply=True)

        self.assertEqual(result.created, 1)
        self.assertIn("missing StartDate", " ".join(result.warnings))
        self.assertTrue(Symbol.objects.filter(ticker="OLD", exchange="US").exists())

    def test_old_suffix_is_skipped_unless_symbol_already_exists(self):
        result = self._bootstrap([record("APC_old"), record("AAPL")], apply=True)

        self.assertEqual(result.created, 1)
        self.assertEqual(result.skipped, 1)
        self.assertIn("historical _OLD provider suffix skipped", " ".join(result.warnings))
        self.assertFalse(Symbol.objects.filter(ticker="APC_OLD").exists())
        self.assertTrue(Symbol.objects.filter(ticker="AAPL", exchange="US").exists())

    def test_old_suffix_existing_symbol_is_not_duplicated(self):
        Symbol.objects.create(ticker="APC_OLD", exchange="US", active=True)

        result = self._bootstrap([record("APC_old")], apply=True)

        self.assertEqual(result.created, 0)
        self.assertEqual(result.existing, 1)
        self.assertEqual(Symbol.objects.filter(ticker="APC_OLD").count(), 1)

    def test_dash_ticker_creates_dot_variant(self):
        result = self._bootstrap([record("BRK-B", "Berkshire Hathaway")], apply=True)

        self.assertEqual(result.created, 1)
        self.assertTrue(Symbol.objects.filter(ticker="BRK.B", exchange="US").exists())
        self.assertFalse(Symbol.objects.filter(ticker="BRK-B", exchange="US").exists())

    def test_dash_ticker_matches_existing_dot_variant_without_duplicate(self):
        Symbol.objects.create(ticker="BRK.B", exchange="NYSE", active=True)

        result = self._bootstrap([record("BRK-B")], apply=True)

        self.assertEqual(result.created, 0)
        self.assertEqual(result.existing, 1)
        self.assertEqual(Symbol.objects.filter(ticker__in=["BRK.B", "BRK-B"]).count(), 1)

    def test_provider_error_is_raised_without_writes(self):
        with self.assertRaisesMessage(EODHDError, "no access"):
            bootstrap_sp500_symbols_from_eodhd(
                coverage_start=date(2020, 1, 1),
                coverage_end=date(2020, 1, 3),
                dry_run=True,
                client=FakeEODHDClient(error=EODHDError("no access")),
            )

        self.assertEqual(Symbol.objects.count(), 0)

    @patch("core.services.provider_eodhd.requests.get", side_effect=AssertionError("no network in tests"))
    def test_command_dry_run_uses_mocked_client_without_network(self, _mock_get):
        out = StringIO()
        with patch("core.services.sp500_symbol_bootstrap.EODHDClient", return_value=FakeEODHDClient([record("AAPL")])):
            call_command(
                "bootstrap_sp500_symbols_from_eodhd",
                "--coverage-start",
                "2020-01-01",
                "--coverage-end",
                "2020-01-03",
                stdout=out,
            )

        self.assertIn("mode=dry-run", out.getvalue())
        self.assertIn("to_create=1", out.getvalue())
        self.assertIn("examples_to_create=AAPL", out.getvalue())
        self.assertEqual(Symbol.objects.count(), 0)

    @patch("core.services.provider_eodhd.requests.get", side_effect=AssertionError("no network in tests"))
    def test_command_apply_uses_mocked_client_without_network(self, _mock_get):
        out = StringIO()
        with patch("core.services.sp500_symbol_bootstrap.EODHDClient", return_value=FakeEODHDClient([record("AAPL")])):
            call_command(
                "bootstrap_sp500_symbols_from_eodhd",
                "--coverage-start",
                "2020-01-01",
                "--coverage-end",
                "2020-01-03",
                "--apply",
                stdout=out,
            )

        self.assertIn("mode=apply", out.getvalue())
        self.assertIn("created=1", out.getvalue())
        self.assertTrue(Symbol.objects.filter(ticker="AAPL", exchange="US").exists())
