from __future__ import annotations

from datetime import date
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from django.core.management import call_command
from django.test import TestCase

from core.models import Symbol, UniverseDefinition, UniverseMembership
from core.services.csi300_eodhd_metadata import (
    enrich_csi300_symbols_from_eodhd_metadata,
    format_csi300_eodhd_metadata_summary,
    normalize_eodhd_general_for_symbol,
)
from core.services.provider_eodhd import EODHDError


class FakeEODHDMetadataClient:
    def __init__(self, payloads=None, errors=None):
        self.payloads = payloads or {}
        self.errors = errors or {}
        self.calls = []

    def fetch_symbol_general_metadata(self, provider_symbol):
        self.calls.append(provider_symbol)
        if provider_symbol in self.errors:
            raise self.errors[provider_symbol]
        return self.payloads.get(provider_symbol, {})


def general_payload(
    *,
    name="China Corp",
    country="China",
    currency="CNY",
    exchange="SHG",
    sector="Financial Services",
    industry="Banks - Regional",
):
    return {
        "name": name,
        "country": country,
        "currency": currency,
        "exchange": exchange,
        "sector": sector,
        "industry": industry,
    }


class CSI300EODHDMetadataTests(TestCase):
    def setUp(self):
        self.csi300 = UniverseDefinition.objects.create(code="CSI300", name="CSI 300", active=True, source="manual_csv")

    def _member(self, ticker, exchange, provider_symbol=None, *, name=""):
        symbol = Symbol.objects.create(ticker=ticker, exchange=exchange, name=name, active=True)
        UniverseMembership.objects.create(
            universe=self.csi300,
            symbol=symbol,
            ticker=ticker,
            exchange=exchange,
            provider_symbol=provider_symbol or f"{ticker}.{exchange}",
            valid_from=date(2020, 1, 1),
            source="manual_csv",
            source_payload={},
        )
        return symbol

    def test_payload_shanghai_with_sector_updates_symbol(self):
        symbol = self._member("600000", "SHG", "600000.SHG")
        client = FakeEODHDMetadataClient({"600000.SHG": general_payload(exchange="SHG")})

        report = enrich_csi300_symbols_from_eodhd_metadata(client=client, dry_run=False)

        symbol.refresh_from_db()
        self.assertEqual(symbol.name, "China Corp")
        self.assertEqual(symbol.country, "China")
        self.assertEqual(symbol.currency, "CNY")
        self.assertEqual(symbol.sector, "Financial Services")
        self.assertEqual(report.field_updates["sector"], 1)
        self.assertEqual(client.calls, ["600000.SHG"])

    def test_payload_shenzhen_with_sector_updates_symbol(self):
        symbol = self._member("000002", "SHE", "000002.SHE")
        client = FakeEODHDMetadataClient({"000002.SHE": general_payload(exchange="SHE", sector="Real Estate", industry="Real Estate - Development")})

        report = enrich_csi300_symbols_from_eodhd_metadata(client=client, dry_run=False)

        symbol.refresh_from_db()
        self.assertEqual(symbol.sector, "Real Estate")
        self.assertEqual(report.raw_sector_counts, {"Real Estate": 1})
        self.assertEqual(report.industries_present, 1)

    def test_absent_sector_is_reported_without_sector_write(self):
        symbol = self._member("600001", "SHG", "600001.SHG")
        client = FakeEODHDMetadataClient({"600001.SHG": general_payload(sector="", industry="")})

        report = enrich_csi300_symbols_from_eodhd_metadata(client=client, dry_run=False)

        symbol.refresh_from_db()
        self.assertEqual(symbol.sector, "")
        self.assertEqual(report.missing_sector, 1)
        self.assertNotIn("sector", report.field_updates)

    def test_generic_other_sector_is_reported_but_not_stored(self):
        symbol = self._member("600001", "SHG", "600001.SHG")
        client = FakeEODHDMetadataClient({"600001.SHG": general_payload(sector="Other", industry="Other")})

        report = enrich_csi300_symbols_from_eodhd_metadata(client=client, dry_run=False)

        symbol.refresh_from_db()
        self.assertEqual(symbol.sector, "")
        self.assertEqual(report.generic_sector, 1)
        self.assertEqual(report.raw_sector_counts, {"Other": 1})

    def test_industry_is_reported_but_not_stored_on_symbol(self):
        symbol = self._member("000001", "SHE", "000001.SHE")
        client = FakeEODHDMetadataClient({"000001.SHE": general_payload(industry="Banks - Regional")})

        report = enrich_csi300_symbols_from_eodhd_metadata(client=client, dry_run=False)

        symbol.refresh_from_db()
        self.assertFalse(hasattr(symbol, "industry"))
        self.assertEqual(report.industries_present, 1)

    def test_missing_name_country_currency_are_not_written(self):
        symbol = self._member("300750", "SHE", "300750.SHE")
        client = FakeEODHDMetadataClient({"300750.SHE": general_payload(name="", country="", currency="", sector="Consumer Cyclical")})

        report = enrich_csi300_symbols_from_eodhd_metadata(client=client, dry_run=False)

        symbol.refresh_from_db()
        self.assertEqual(symbol.name, "")
        self.assertEqual(symbol.country, "")
        self.assertEqual(symbol.currency, "")
        self.assertEqual(symbol.sector, "Consumer Cyclical")
        self.assertEqual(report.field_updates, {"sector": 1})

    def test_existing_values_are_not_overwritten(self):
        symbol = self._member("000001", "SHE", "000001.SHE", name="Local Name")
        symbol.country = "CN"
        symbol.currency = "CNY"
        symbol.sector = "Financials"
        symbol.save(update_fields=["country", "currency", "sector"])
        client = FakeEODHDMetadataClient({"000001.SHE": general_payload(name="Ping An Bank", sector="Financial Services")})

        report = enrich_csi300_symbols_from_eodhd_metadata(client=client, dry_run=False)

        symbol.refresh_from_db()
        self.assertEqual(symbol.name, "Local Name")
        self.assertEqual(symbol.sector, "Financials")
        self.assertEqual(report.updated, 0)
        self.assertEqual(report.unchanged, 1)

    def test_apply_is_idempotent(self):
        symbol = self._member("600000", "SHG", "600000.SHG")
        client = FakeEODHDMetadataClient({"600000.SHG": general_payload()})

        first = enrich_csi300_symbols_from_eodhd_metadata(client=client, dry_run=False)
        second = enrich_csi300_symbols_from_eodhd_metadata(client=client, dry_run=False)

        symbol.refresh_from_db()
        self.assertEqual(symbol.sector, "Financial Services")
        self.assertEqual(first.updated, 1)
        self.assertEqual(second.updated, 0)
        self.assertEqual(second.unchanged, 1)

    def test_dry_run_reports_without_writing(self):
        symbol = self._member("600000", "SHG", "600000.SHG")
        client = FakeEODHDMetadataClient({"600000.SHG": general_payload()})

        report = enrich_csi300_symbols_from_eodhd_metadata(client=client, dry_run=True)

        symbol.refresh_from_db()
        self.assertEqual(symbol.sector, "")
        self.assertEqual(report.updated, 1)
        self.assertTrue(report.dry_run)

    def test_provider_error_is_partial_and_sanitized(self):
        good = self._member("600000", "SHG", "600000.SHG")
        bad = self._member("000001", "SHE", "000001.SHE")
        client = FakeEODHDMetadataClient(
            {"600000.SHG": general_payload()},
            {"000001.SHE": EODHDError("boom api_token=secret-token")},
        )

        report = enrich_csi300_symbols_from_eodhd_metadata(client=client, dry_run=False)

        good.refresh_from_db()
        bad.refresh_from_db()
        self.assertEqual(good.sector, "Financial Services")
        self.assertEqual(bad.sector, "")
        self.assertEqual(report.updated, 1)
        self.assertEqual(report.errors, 1)
        error_detail = next(detail for detail in report.per_symbol if detail.get("error"))
        self.assertIn("api_token=***", error_detail["error"])
        self.assertNotIn("secret-token", error_detail["error"])

    def test_ticker_and_limit_restrict_provider_calls(self):
        self._member("600000", "SHG", "600000.SHG")
        self._member("000001", "SHE", "000001.SHE")
        client = FakeEODHDMetadataClient({
            "600000.SHG": general_payload(),
            "000001.SHE": general_payload(exchange="SHE"),
        })

        report = enrich_csi300_symbols_from_eodhd_metadata(tickers=["000001"], limit=1, client=client, dry_run=True)

        self.assertEqual(report.processed, 1)
        self.assertEqual(client.calls, ["000001.SHE"])

    def test_summary_is_stable(self):
        report = enrich_csi300_symbols_from_eodhd_metadata(client=FakeEODHDMetadataClient({}), dry_run=True)

        self.assertEqual(
            format_csi300_eodhd_metadata_summary(report),
            "EODHD CSI300 metadata (dry-run) — processed=0, fetched=0, updated=0, unchanged=0, skipped=0, errors=0, missing_sector=0, generic_sector=0, industries_present=0.",
        )

    @patch("core.services.provider_eodhd.requests.get", side_effect=AssertionError("no network in tests"))
    def test_command_eodhd_uses_mocked_client_without_network(self, _mock_get):
        self._member("600000", "SHG", "600000.SHG")
        fake = FakeEODHDMetadataClient({"600000.SHG": general_payload()})
        out = StringIO()

        with patch("core.services.csi300_eodhd_metadata.EODHDClient", return_value=fake):
            call_command("enrich_csi300_symbol_metadata", "--source", "eodhd", "--ticker", "600000", stdout=out)

        output = out.getvalue()
        self.assertIn("EODHD CSI300 metadata (dry-run)", output)
        self.assertIn("sectors_enriched=1", output)
        self.assertIn("raw_sector Financial Services symbols=1 decision=usable_raw", output)
        self.assertEqual(fake.calls, ["600000.SHG"])

    @patch("core.services.provider_eodhd.requests.get", side_effect=AssertionError("no network in tests"))
    def test_command_apply_persists_with_mocked_client_without_network(self, _mock_get):
        symbol = self._member("600000", "SHG", "600000.SHG")
        fake = FakeEODHDMetadataClient({"600000.SHG": general_payload()})

        with patch("core.services.csi300_eodhd_metadata.EODHDClient", return_value=fake):
            call_command("enrich_csi300_symbol_metadata", "--source", "eodhd", "--ticker", "600000", "--apply", stdout=StringIO())

        symbol.refresh_from_db()
        self.assertEqual(symbol.sector, "Financial Services")

    def test_sp500_is_not_modified_by_csi300_eodhd_command(self):
        self._member("600000", "SHG", "600000.SHG")
        sp500 = UniverseDefinition.objects.create(code="SP500", name="S&P 500", active=True)
        aapl = Symbol.objects.create(ticker="AAPL", exchange="US", active=True)
        UniverseMembership.objects.create(
            universe=sp500,
            symbol=aapl,
            ticker="AAPL",
            exchange="US",
            provider_symbol="AAPL.US",
            valid_from=date(2020, 1, 1),
            source="manual_csv",
        )
        fake = FakeEODHDMetadataClient({"600000.SHG": general_payload()})

        with patch("core.services.csi300_eodhd_metadata.EODHDClient", return_value=fake):
            call_command("enrich_csi300_symbol_metadata", "--source", "eodhd", "--apply", stdout=StringIO())

        aapl.refresh_from_db()
        self.assertEqual(aapl.name, "")
        self.assertEqual(aapl.sector, "")
        self.assertEqual(fake.calls, ["600000.SHG"])

    def test_backtest_engine_does_not_import_eodhd_metadata_enrichment(self):
        source = (Path(__file__).resolve().parents[1] / "services" / "backtesting" / "engine.py").read_text()

        self.assertNotIn("csi300_eodhd_metadata", source)
        self.assertNotIn("fetch_symbol_general_metadata", source)

    def test_normalizer_does_not_map_taxonomy_keywords(self):
        normalized = normalize_eodhd_general_for_symbol(general_payload(sector="Financial Services"))
        generic = normalize_eodhd_general_for_symbol(general_payload(sector="Other"))

        self.assertEqual(normalized["sector"], "Financial Services")
        self.assertEqual(normalized["raw_sector"], "Financial Services")
        self.assertEqual(generic["sector"], "")
        self.assertEqual(generic["raw_sector"], "Other")
