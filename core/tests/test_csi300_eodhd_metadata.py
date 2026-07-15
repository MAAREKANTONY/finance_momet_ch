from __future__ import annotations

from datetime import date
from decimal import Decimal
from io import StringIO
from pathlib import Path
from unittest.mock import Mock, patch

from django.core.management import call_command
from django.db import connection
from django.test import TestCase

from core.models import (
    DailyBar,
    Symbol,
    UniverseCoverageSnapshot,
    UniverseCoverageStatus,
    UniverseDefinition,
    UniverseImportBatch,
    UniverseMembership,
)
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

    def _member(self, ticker, exchange, provider_symbol=None, *, name="", name_en=""):
        symbol = Symbol.objects.create(
            ticker=ticker,
            exchange=exchange,
            name=name,
            name_en=name_en,
            active=True,
        )
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

    def test_symbol_name_en_field_matches_local_name_capacity(self):
        name_field = Symbol._meta.get_field("name")
        name_en_field = Symbol._meta.get_field("name_en")

        self.assertEqual(name_en_field.max_length, name_field.max_length)
        self.assertTrue(name_en_field.blank)
        self.assertEqual(name_en_field.default, "")

    def test_postgresql_name_en_schema_matches_model(self):
        if connection.vendor != "postgresql":
            self.skipTest("PostgreSQL only")

        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT data_type, character_maximum_length
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'core_symbol'
                  AND column_name = 'name_en'
                """
            )
            row = cursor.fetchone()

        self.assertEqual(row, ("character varying", Symbol._meta.get_field("name_en").max_length))

    def test_payload_shanghai_with_sector_updates_symbol(self):
        symbol = self._member("600000", "SHG", "600000.SHG")
        client = FakeEODHDMetadataClient({"600000.SHG": general_payload(exchange="SHG")})

        report = enrich_csi300_symbols_from_eodhd_metadata(client=client, dry_run=False)

        symbol.refresh_from_db()
        self.assertEqual(symbol.name, "")
        self.assertEqual(symbol.name_en, "China Corp")
        self.assertEqual(symbol.country, "China")
        self.assertEqual(symbol.currency, "CNY")
        self.assertEqual(symbol.sector, "Financial Services")
        self.assertEqual(report.english_names_created, 1)
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
        self.assertEqual(symbol.name_en, "")
        self.assertEqual(symbol.country, "")
        self.assertEqual(symbol.currency, "")
        self.assertEqual(symbol.sector, "Consumer Cyclical")
        self.assertEqual(report.english_names_missing, 1)
        self.assertEqual(report.field_updates, {"sector": 1})

    def test_english_name_is_normalized_and_numeric_ticker_or_generic_values_are_rejected(self):
        normalized = normalize_eodhd_general_for_symbol(general_payload(name="  China   Railway\tGroup  "))
        self.assertEqual(normalized["name_en"], "China Railway Group")

        numeric = self._member("601006", "SHG", "601006.SHG", name="000780")
        ticker_only = self._member("600002", "SHG", "600002.SHG")
        generic = self._member("000003", "SHE", "000003.SHE")
        client = FakeEODHDMetadataClient(
            {
                "601006.SHG": general_payload(name="000780"),
                "600002.SHG": general_payload(name="600002"),
                "000003.SHE": general_payload(name="Unknown", exchange="SHE"),
            }
        )

        report = enrich_csi300_symbols_from_eodhd_metadata(client=client, dry_run=False)

        numeric.refresh_from_db()
        ticker_only.refresh_from_db()
        generic.refresh_from_db()
        self.assertEqual(numeric.name, "000780")
        self.assertEqual(numeric.name_en, "")
        self.assertEqual(ticker_only.name_en, "")
        self.assertEqual(generic.name_en, "")
        self.assertEqual(report.english_names_present, 3)
        self.assertEqual(report.english_names_useful, 0)
        self.assertEqual(report.english_names_rejected, 3)
        self.assertEqual(report.english_names_missing, 3)

    def test_existing_english_name_is_preserved_when_provider_name_is_missing(self):
        symbol = self._member(
            "600000",
            "SHG",
            "600000.SHG",
            name="浦发银行",
            name_en="Shanghai Pudong Development Bank",
        )
        client = FakeEODHDMetadataClient({"600000.SHG": general_payload(name="")})

        report = enrich_csi300_symbols_from_eodhd_metadata(client=client, dry_run=False)

        symbol.refresh_from_db()
        self.assertEqual(symbol.name_en, "Shanghai Pudong Development Bank")
        self.assertEqual(report.english_names_preserved, 1)
        self.assertEqual(report.english_names_missing, 0)

    def test_realistic_shanghai_and_shenzhen_names_fill_name_en_only(self):
        shanghai = self._member("601006", "SHG", "601006.SHG", name="000780")
        shenzhen = self._member("000001", "SHE", "000001.SHE", name="平安银行")
        client = FakeEODHDMetadataClient(
            {
                "601006.SHG": general_payload(name="Daqin Railway Co., Ltd."),
                "000001.SHE": general_payload(name="Ping An Bank Co., Ltd.", exchange="SHE"),
            }
        )

        report = enrich_csi300_symbols_from_eodhd_metadata(client=client, dry_run=False)

        shanghai.refresh_from_db()
        shenzhen.refresh_from_db()
        self.assertEqual(shanghai.name, "000780")
        self.assertEqual(shanghai.name_en, "Daqin Railway Co., Ltd.")
        self.assertEqual(shenzhen.name, "平安银行")
        self.assertEqual(shenzhen.name_en, "Ping An Bank Co., Ltd.")
        self.assertEqual(report.english_names_created, 2)

    def test_apply_changes_no_membership_snapshot_or_daily_bar(self):
        symbol = self._member("600000", "SHG", "600000.SHG", name="Local")
        membership = UniverseMembership.objects.get(universe=self.csi300, symbol=symbol)
        batch = UniverseImportBatch.objects.create(
            universe=self.csi300,
            provider="fixture",
            source_name="fixture",
            period_start=date(2020, 1, 1),
            period_end=date(2020, 1, 1),
            expected_member_count=1,
            imported_member_count=1,
            mapped_member_count=1,
            status=UniverseCoverageStatus.VALIDATED,
        )
        snapshot = UniverseCoverageSnapshot.objects.create(
            universe=self.csi300,
            import_batch=batch,
            coverage_date=date(2020, 1, 1),
            expected_member_count=1,
            actual_member_count=1,
            mapped_member_count=1,
            status=UniverseCoverageStatus.VALIDATED,
        )
        bar = DailyBar.objects.create(
            symbol=symbol,
            date=date(2020, 1, 1),
            open=Decimal("10"),
            high=Decimal("11"),
            low=Decimal("9"),
            close=Decimal("10.5"),
            volume=100,
        )
        membership_before = (
            membership.ticker,
            membership.exchange,
            membership.provider_symbol,
            membership.valid_from,
            membership.valid_to,
            membership.source,
        )
        snapshot_before = (
            snapshot.import_batch_id,
            snapshot.coverage_date,
            snapshot.expected_member_count,
            snapshot.actual_member_count,
            snapshot.mapped_member_count,
            snapshot.unmapped_member_count,
            snapshot.status,
        )
        bar_before = (bar.open, bar.high, bar.low, bar.close, bar.volume, bar.source)
        client = FakeEODHDMetadataClient({"600000.SHG": general_payload(name="English Name")})

        enrich_csi300_symbols_from_eodhd_metadata(client=client, dry_run=False)

        membership.refresh_from_db()
        snapshot.refresh_from_db()
        bar.refresh_from_db()
        symbol.refresh_from_db()
        self.assertEqual(symbol.name, "Local")
        self.assertEqual(symbol.name_en, "English Name")
        self.assertEqual(
            (
                membership.ticker,
                membership.exchange,
                membership.provider_symbol,
                membership.valid_from,
                membership.valid_to,
                membership.source,
            ),
            membership_before,
        )
        self.assertEqual(
            (
                snapshot.import_batch_id,
                snapshot.coverage_date,
                snapshot.expected_member_count,
                snapshot.actual_member_count,
                snapshot.mapped_member_count,
                snapshot.unmapped_member_count,
                snapshot.status,
            ),
            snapshot_before,
        )
        self.assertEqual((bar.open, bar.high, bar.low, bar.close, bar.volume, bar.source), bar_before)

    def test_existing_values_are_not_overwritten(self):
        symbol = self._member(
            "000001",
            "SHE",
            "000001.SHE",
            name="Local Name",
            name_en="Manual English Name",
        )
        symbol.country = "CN"
        symbol.currency = "CNY"
        symbol.sector = "Financials"
        symbol.save(update_fields=["country", "currency", "sector"])
        client = FakeEODHDMetadataClient({"000001.SHE": general_payload(name="Ping An Bank", sector="Financial Services")})

        report = enrich_csi300_symbols_from_eodhd_metadata(client=client, dry_run=False)

        symbol.refresh_from_db()
        self.assertEqual(symbol.name, "Local Name")
        self.assertEqual(symbol.name_en, "Manual English Name")
        self.assertEqual(symbol.sector, "Financials")
        self.assertEqual(report.english_names_preserved, 1)
        self.assertEqual(report.updated, 0)
        self.assertEqual(report.unchanged, 1)

    def test_apply_is_idempotent(self):
        symbol = self._member("600000", "SHG", "600000.SHG")
        client = FakeEODHDMetadataClient({"600000.SHG": general_payload()})

        first = enrich_csi300_symbols_from_eodhd_metadata(client=client, dry_run=False)
        second = enrich_csi300_symbols_from_eodhd_metadata(client=client, dry_run=False)

        symbol.refresh_from_db()
        self.assertEqual(symbol.sector, "Financial Services")
        self.assertEqual(symbol.name_en, "China Corp")
        self.assertEqual(first.updated, 1)
        self.assertEqual(first.english_names_created, 1)
        self.assertEqual(second.updated, 0)
        self.assertEqual(second.unchanged, 1)
        self.assertEqual(second.english_names_unchanged, 1)

    def test_dry_run_reports_without_writing(self):
        symbol = self._member("600000", "SHG", "600000.SHG")
        client = FakeEODHDMetadataClient({"600000.SHG": general_payload()})

        report = enrich_csi300_symbols_from_eodhd_metadata(client=client, dry_run=True)

        symbol.refresh_from_db()
        self.assertEqual(symbol.sector, "")
        self.assertEqual(symbol.name_en, "")
        self.assertEqual(report.updated, 1)
        self.assertEqual(report.english_names_present, 1)
        self.assertEqual(report.english_names_useful, 1)
        self.assertEqual(report.english_names_to_create, 1)
        self.assertEqual(report.english_names_created, 0)
        self.assertEqual(report.english_names_missing, 0)
        self.assertEqual(report.per_symbol[0]["english_name_candidate"], "China Corp")
        self.assertEqual(report.per_symbol[0]["english_name_status"], "to_create")
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
        self.assertEqual(good.name_en, "China Corp")
        self.assertEqual(bad.name_en, "")
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

    def test_progress_callback_is_called_after_each_symbol(self):
        self._member("600000", "SHG", "600000.SHG")
        self._member("000001", "SHE", "000001.SHE")
        callback = Mock()
        client = FakeEODHDMetadataClient({
            "600000.SHG": general_payload(),
            "000001.SHE": general_payload(exchange="SHE"),
        })

        report = enrich_csi300_symbols_from_eodhd_metadata(
            client=client,
            dry_run=True,
            progress_callback=callback,
        )

        self.assertEqual(report.processed, 2)
        self.assertEqual(callback.call_count, 2)
        self.assertEqual(callback.call_args.kwargs["processed"], 2)
        self.assertEqual(callback.call_args.kwargs["total"], 2)

    def test_service_without_progress_callback_remains_functional(self):
        self._member("600000", "SHG", "600000.SHG")
        client = FakeEODHDMetadataClient({"600000.SHG": general_payload()})

        report = enrich_csi300_symbols_from_eodhd_metadata(client=client, dry_run=True)

        self.assertEqual(report.processed, 1)
        self.assertEqual(client.calls, ["600000.SHG"])

    def test_summary_is_stable(self):
        report = enrich_csi300_symbols_from_eodhd_metadata(client=FakeEODHDMetadataClient({}), dry_run=True)

        self.assertEqual(
            format_csi300_eodhd_metadata_summary(report),
            "Métadonnées EODHD CSI300 (dry-run) — traités=0, récupérés=0, mis_à_jour=0, inchangés=0, ignorés=0, erreurs=0, secteurs_absents=0, secteurs_génériques=0, industries_présentes=0. Noms anglais: trouvés=0, utiles=0, à_créer=0, créés=0, inchangés=0, préservés=0, absents=0, rejetés=0.",
        )

    @patch("core.services.provider_eodhd.requests.get", side_effect=AssertionError("no network in tests"))
    def test_command_eodhd_uses_mocked_client_without_network(self, _mock_get):
        self._member("600000", "SHG", "600000.SHG")
        fake = FakeEODHDMetadataClient({"600000.SHG": general_payload()})
        out = StringIO()

        with patch("core.services.csi300_eodhd_metadata.EODHDClient", return_value=fake):
            call_command("enrich_csi300_symbol_metadata", "--source", "eodhd", "--ticker", "600000", stdout=out)

        output = out.getvalue()
        self.assertIn("Métadonnées EODHD CSI300 (dry-run)", output)
        self.assertIn("english_names_to_create=1", output)
        self.assertIn("name_en_candidate=China Corp", output)
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
        self.assertEqual(symbol.name, "")
        self.assertEqual(symbol.name_en, "China Corp")
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
        self.assertEqual(aapl.name_en, "")
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
