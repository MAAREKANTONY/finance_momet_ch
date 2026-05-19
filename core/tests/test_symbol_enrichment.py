from __future__ import annotations

from io import StringIO
from unittest.mock import Mock, patch

from django.core.management import call_command
from django.test import TestCase

from core.models import Symbol
from core.services.provider_twelvedata import TwelveDataClient, TwelveDataRateLimitError
from core.services.symbol_enrichment import enrich_symbols_metadata


class SymbolEnrichmentServiceTests(TestCase):
    def setUp(self):
        self.symbol = Symbol.objects.create(ticker="AAPL", exchange="", active=True)

    @patch("core.services.symbol_enrichment.TwelveDataClient.fetch_symbol_metadata")
    def test_enrich_missing_name(self, metadata_mock):
        metadata_mock.return_value = {"name": "Apple Inc."}

        totals = enrich_symbols_metadata([self.symbol])

        self.symbol.refresh_from_db()
        self.assertEqual(self.symbol.name, "Apple Inc.")
        self.assertEqual(totals["updated"], 1)
        self.assertEqual(totals["per_symbol"][0]["updated_fields"], ["name"])

    @patch("core.services.symbol_enrichment.TwelveDataClient.fetch_symbol_metadata")
    def test_enrich_missing_exchange(self, metadata_mock):
        metadata_mock.return_value = {"exchange": "NASDAQ"}

        enrich_symbols_metadata([self.symbol])

        self.symbol.refresh_from_db()
        self.assertEqual(self.symbol.exchange, "NASDAQ")

    @patch("core.services.symbol_enrichment.TwelveDataClient.fetch_symbol_metadata")
    def test_enrich_missing_country(self, metadata_mock):
        metadata_mock.return_value = {"country": "United States"}

        enrich_symbols_metadata([self.symbol])

        self.symbol.refresh_from_db()
        self.assertEqual(self.symbol.country, "United States")

    @patch("core.services.symbol_enrichment.TwelveDataClient.fetch_symbol_metadata")
    def test_enrich_missing_currency(self, metadata_mock):
        metadata_mock.return_value = {"currency": "USD"}

        enrich_symbols_metadata([self.symbol])

        self.symbol.refresh_from_db()
        self.assertEqual(self.symbol.currency, "USD")

    @patch("core.services.symbol_enrichment.TwelveDataClient.fetch_symbol_metadata")
    def test_enrich_missing_sector(self, metadata_mock):
        metadata_mock.return_value = {"sector": "Technology"}

        enrich_symbols_metadata([self.symbol])

        self.symbol.refresh_from_db()
        self.assertEqual(self.symbol.sector, "Technology")

    @patch("core.services.symbol_enrichment.TwelveDataClient.fetch_symbol_metadata")
    def test_enrich_missing_instrument_type(self, metadata_mock):
        metadata_mock.return_value = {"instrument_type": "Common Stock"}

        enrich_symbols_metadata([self.symbol])

        self.symbol.refresh_from_db()
        self.assertEqual(self.symbol.instrument_type, "Common Stock")

    @patch("core.services.symbol_enrichment.TwelveDataClient.fetch_symbol_metadata")
    def test_does_not_overwrite_populated_fields_by_default(self, metadata_mock):
        self.symbol.name = "Local Apple"
        self.symbol.exchange = "NYSE"
        self.symbol.save(update_fields=["name", "exchange"])
        metadata_mock.return_value = {
            "name": "Apple Inc.",
            "exchange": "NASDAQ",
            "country": "United States",
        }

        totals = enrich_symbols_metadata([self.symbol])

        self.symbol.refresh_from_db()
        self.assertEqual(self.symbol.name, "Local Apple")
        self.assertEqual(self.symbol.exchange, "NYSE")
        self.assertEqual(self.symbol.country, "United States")
        self.assertEqual(totals["per_symbol"][0]["updated_fields"], ["country"])

    @patch("core.services.symbol_enrichment.TwelveDataClient.fetch_symbol_metadata")
    def test_can_enrich_queryset_input(self, metadata_mock):
        metadata_mock.return_value = {"currency": "USD"}

        totals = enrich_symbols_metadata(Symbol.objects.filter(id=self.symbol.id))

        self.symbol.refresh_from_db()
        self.assertEqual(self.symbol.currency, "USD")
        self.assertEqual(totals["processed"], 1)

    @patch("core.services.symbol_enrichment.TwelveDataClient.fetch_symbol_metadata")
    def test_can_enrich_list_input(self, metadata_mock):
        metadata_mock.return_value = {"country": "United States"}

        totals = enrich_symbols_metadata([self.symbol])

        self.symbol.refresh_from_db()
        self.assertEqual(self.symbol.country, "United States")
        self.assertEqual(totals["processed"], 1)

    @patch("core.services.symbol_enrichment.TwelveDataClient.fetch_symbol_metadata")
    def test_dry_run_writes_nothing(self, metadata_mock):
        metadata_mock.return_value = {"name": "Apple Inc.", "currency": "USD"}

        totals = enrich_symbols_metadata([self.symbol], dry_run=True)

        self.symbol.refresh_from_db()
        self.assertEqual(self.symbol.name, "")
        self.assertEqual(self.symbol.currency, "")
        self.assertEqual(totals["updated"], 1)
        self.assertTrue(totals["per_symbol"][0]["dry_run"])

    @patch("core.services.symbol_enrichment.TwelveDataClient.fetch_symbol_metadata")
    def test_provider_error_on_one_symbol_does_not_abort_batch(self, metadata_mock):
        other = Symbol.objects.create(ticker="MSFT", exchange="NASDAQ", active=True)
        metadata_mock.side_effect = [RuntimeError("provider exploded"), {"name": "Microsoft Corp."}]

        totals = enrich_symbols_metadata([self.symbol, other])

        self.symbol.refresh_from_db()
        other.refresh_from_db()
        self.assertEqual(totals["errors"], 1)
        self.assertEqual(totals["updated"], 1)
        self.assertEqual(other.name, "Microsoft Corp.")

    @patch("core.services.symbol_enrichment.TwelveDataClient.fetch_symbol_metadata")
    def test_provider_returns_missing_field_safely(self, metadata_mock):
        metadata_mock.return_value = {"name": "Apple Inc."}

        totals = enrich_symbols_metadata([self.symbol])

        self.symbol.refresh_from_db()
        self.assertEqual(self.symbol.name, "Apple Inc.")
        self.assertEqual(self.symbol.sector, "")
        self.assertEqual(totals["unchanged"], 0)

    @patch("core.services.symbol_enrichment.TwelveDataClient.fetch_symbol_metadata")
    def test_provider_returns_industry_but_it_is_not_persisted(self, metadata_mock):
        metadata_mock.return_value = {
            "name": "Apple Inc.",
            "sector": "Technology",
            "industry": "Consumer Electronics",
        }

        enrich_symbols_metadata([self.symbol])

        self.symbol.refresh_from_db()
        self.assertEqual(self.symbol.name, "Apple Inc.")
        self.assertEqual(self.symbol.sector, "Technology")
        self.assertFalse(hasattr(self.symbol, "industry"))


class SymbolEnrichmentCommandTests(TestCase):
    def setUp(self):
        self.aapl = Symbol.objects.create(ticker="AAPL", exchange="", active=True)
        self.msft = Symbol.objects.create(ticker="MSFT", exchange="", active=True)
        self.inactive = Symbol.objects.create(ticker="ZZZ", exchange="", active=False)

    @patch("core.services.symbol_enrichment.TwelveDataClient.fetch_symbol_metadata")
    def test_command_enriches_explicit_tickers(self, metadata_mock):
        metadata_mock.side_effect = [
            {"name": "Apple Inc.", "exchange": "NASDAQ"},
            {"name": "Microsoft Corp.", "exchange": "NASDAQ"},
        ]
        out = StringIO()

        call_command("enrich_symbols_metadata", "--symbols=AAPL,MSFT", stdout=out)

        self.aapl.refresh_from_db()
        self.msft.refresh_from_db()
        self.inactive.refresh_from_db()
        self.assertEqual(self.aapl.name, "Apple Inc.")
        self.assertEqual(self.msft.name, "Microsoft Corp.")
        self.assertEqual(self.inactive.name, "")
        self.assertIn("summary processed=2 updated=2", out.getvalue())

    @patch("core.services.symbol_enrichment.TwelveDataClient.fetch_symbol_metadata")
    def test_command_enriches_active_symbols_by_default(self, metadata_mock):
        metadata_mock.side_effect = [
            {"name": "Apple Inc."},
            {"name": "Microsoft Corp."},
        ]
        out = StringIO()

        call_command("enrich_symbols_metadata", stdout=out)

        self.aapl.refresh_from_db()
        self.msft.refresh_from_db()
        self.inactive.refresh_from_db()
        self.assertEqual(self.aapl.name, "Apple Inc.")
        self.assertEqual(self.msft.name, "Microsoft Corp.")
        self.assertEqual(self.inactive.name, "")
        self.assertIn("summary processed=2 updated=2", out.getvalue())

    @patch("core.services.symbol_enrichment.TwelveDataClient.fetch_symbol_metadata")
    def test_command_dry_run_does_not_persist(self, metadata_mock):
        metadata_mock.return_value = {"name": "Apple Inc."}
        out = StringIO()

        call_command("enrich_symbols_metadata", "--symbols=AAPL", "--dry-run", stdout=out)

        self.aapl.refresh_from_db()
        self.assertEqual(self.aapl.name, "")
        self.assertIn("dry_run=1", out.getvalue())


class TwelveDataMetadataClientTests(TestCase):
    def test_profile_uses_retry_path(self):
        client = TwelveDataClient(api_key="demo")

        with patch.object(
            client,
            "_request_once",
            side_effect=[TwelveDataRateLimitError("minute limit"), {"name": "Apple Inc."}],
        ) as request_once:
            with patch("core.services.provider_twelvedata.time.sleep") as sleep_mock:
                payload = client.profile("AAPL", exchange="NASDAQ")

        self.assertEqual(payload["name"], "Apple Inc.")
        self.assertEqual(request_once.call_count, 2)
        sleep_mock.assert_called_once()

    def test_profile_uses_global_rate_limiter(self):
        client = TwelveDataClient(api_key="demo")
        limiter = Mock()
        response = Mock()
        response.status_code = 200
        response.raise_for_status.return_value = None
        response.json.return_value = {"name": "Apple Inc."}

        with patch("core.services.provider_twelvedata.get_twelvedata_rate_limiter", return_value=limiter):
            with patch("core.services.provider_twelvedata.requests.get", return_value=response) as requests_get:
                payload = client.profile("AAPL", exchange="NASDAQ")

        self.assertEqual(payload["name"], "Apple Inc.")
        limiter.wait_for_slot.assert_called_once()
        requests_get.assert_called_once()

    def test_fetch_symbol_metadata_falls_back_to_stocks_reference(self):
        client = TwelveDataClient(api_key="demo")

        with patch.object(client, "profile", return_value={}) as profile_mock:
            with patch.object(
                client,
                "stocks",
                return_value=[
                    {"symbol": "AAPL", "exchange": "NYSE", "name": "Wrong Exchange"},
                    {
                        "symbol": "AAPL",
                        "exchange": "NASDAQ",
                        "name": "Apple Inc.",
                        "country": "United States",
                        "currency": "USD",
                        "type": "Common Stock",
                    },
                ],
            ) as stocks_mock:
                payload = client.fetch_symbol_metadata("AAPL", exchange="NASDAQ")

        profile_mock.assert_called_once()
        stocks_mock.assert_called_once()
        self.assertEqual(payload["name"], "Apple Inc.")
        self.assertEqual(payload["exchange"], "NASDAQ")
        self.assertEqual(payload["instrument_type"], "Common Stock")
