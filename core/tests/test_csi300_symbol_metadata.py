from __future__ import annotations

from datetime import date
from io import StringIO
from pathlib import Path
import tempfile
from unittest.mock import patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase

from core.models import Symbol, UniverseDefinition, UniverseMembership
from core.services.dynamic_universe_symbols import ensure_universe_membership_symbols
from core.services.universe_import import import_universe_memberships_from_csv


class CSI300SymbolMetadataEnrichmentTests(TestCase):
    def _csv_file(self, body: str, header: str | None = None) -> str:
        header = header or (
            "universe_code,symbol,exchange,mic,name,start_date,end_date,weight,"
            "provider_symbol,source,country,currency,sector,industry\n"
        )
        handle = tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False, encoding="utf-8")
        handle.write(header)
        handle.write(body)
        handle.flush()
        handle.close()
        return handle.name

    def _import_csi300(self, body: str, *, expected_member_count: int = 1) -> None:
        import_universe_memberships_from_csv(
            self._csv_file(body),
            universe_code="CSI300",
            universe_name="CSI 300",
            coverage_start=date(2020, 1, 1),
            coverage_end=date(2020, 1, 2),
            expected_member_count=expected_member_count,
            dry_run=False,
        )

    def test_creates_shanghai_symbol_metadata_and_provider_symbol(self):
        self._import_csi300(
            "CSI300,600519,SHG,XSHG,Kweichow Moutai,2020-01-01,,0.052,,"
            "manual_csv,CN,CNY,Consumer Staples,Distillers & Wineries\n"
        )

        report = ensure_universe_membership_symbols("CSI300", enrich_metadata=True)

        symbol = Symbol.objects.get(ticker="600519", exchange="SHG")
        membership = UniverseMembership.objects.get(ticker="600519", exchange="SHG")
        self.assertEqual(symbol.name, "Kweichow Moutai")
        self.assertEqual(symbol.country, "CN")
        self.assertEqual(symbol.currency, "CNY")
        self.assertEqual(symbol.sector, "Consumer Staples")
        self.assertFalse(hasattr(symbol, "industry"))
        self.assertEqual(membership.provider_symbol, "600519.SHG")
        self.assertEqual(report.created_symbols, 1)
        self.assertEqual(report.provider_symbols_created, 1)
        self.assertEqual(report.metadata_fields_updated["name"], 1)
        self.assertEqual(report.metadata_industries_available, 1)

    def test_creates_shenzhen_symbol_and_preserves_local_ticker_leading_zero(self):
        self._import_csi300(
            "CSI300,000001,SHE,XSHE,Ping An Bank,2020-01-01,,0.014,,"
            "manual_csv,CN,CNY,Financials,Banks\n"
        )

        ensure_universe_membership_symbols("CSI300", enrich_metadata=True)

        symbol = Symbol.objects.get(ticker="000001", exchange="SHE")
        membership = UniverseMembership.objects.get(ticker="000001", exchange="SHE")
        self.assertEqual(symbol.ticker, "000001")
        self.assertEqual(symbol.exchange, "SHE")
        self.assertEqual(symbol.name, "Ping An Bank")
        self.assertEqual(membership.provider_symbol, "000001.SHE")
        self.assertFalse(Symbol.objects.filter(ticker="1", exchange="SHE").exists())

    def test_enriches_existing_symbol_missing_metadata_without_replacing_ticker(self):
        existing = Symbol.objects.create(ticker="600519", exchange="SHG", active=True)
        self._import_csi300(
            "CSI300,600519,SHG,XSHG,Kweichow Moutai,2020-01-01,,0.052,600519.SHG,"
            "manual_csv,CN,CNY,Consumer Staples,Distillers & Wineries\n"
        )

        report = ensure_universe_membership_symbols("CSI300", enrich_metadata=True)

        existing.refresh_from_db()
        self.assertEqual(existing.ticker, "600519")
        self.assertEqual(existing.name, "Kweichow Moutai")
        self.assertEqual(existing.country, "CN")
        self.assertEqual(existing.currency, "CNY")
        self.assertEqual(existing.sector, "Consumer Staples")
        self.assertEqual(report.linked_existing_symbols + report.already_mapped, 1)
        self.assertEqual(report.metadata_symbols_updated, 1)

    def test_country_and_currency_can_be_inferred_from_supported_china_exchange(self):
        Symbol.objects.create(ticker="600000", exchange="XSHG", active=True)
        self._import_csi300(
            "CSI300,600000,XSHG,XSHG,SPD Bank,2020-01-01,,0.010,600000.SHG,"
            "manual_csv,,,,\n"
        )

        report = ensure_universe_membership_symbols("CSI300", enrich_metadata=True)

        symbol = Symbol.objects.get(ticker="600000", exchange="XSHG")
        self.assertEqual(symbol.country, "CN")
        self.assertEqual(symbol.currency, "CNY")
        self.assertEqual(symbol.sector, "")
        self.assertNotIn("sector", report.metadata_fields_updated)
        self.assertEqual(report.metadata_industries_available, 0)

    def test_existing_values_are_not_overwritten_by_empty_or_generic_source_values(self):
        existing = Symbol.objects.create(
            ticker="000001",
            exchange="SHE",
            name="Local Ping An",
            country="CN",
            currency="CNY",
            sector="Financials",
            active=True,
        )
        self._import_csi300(
            "CSI300,000001,SHE,XSHE,,2020-01-01,,0.014,000001.SHE,"
            "manual_csv,CN,CNY,UNKNOWN,\n"
        )

        report = ensure_universe_membership_symbols("CSI300", enrich_metadata=True)

        existing.refresh_from_db()
        self.assertEqual(existing.name, "Local Ping An")
        self.assertEqual(existing.sector, "Financials")
        self.assertEqual(report.metadata_symbols_updated, 0)
        self.assertEqual(report.metadata_symbols_unchanged, 1)

    def test_apply_is_idempotent(self):
        self._import_csi300(
            "CSI300,600519,SHG,XSHG,Kweichow Moutai,2020-01-01,,0.052,,"
            "manual_csv,CN,CNY,Consumer Staples,Distillers & Wineries\n"
        )

        first = ensure_universe_membership_symbols("CSI300", enrich_metadata=True)
        second = ensure_universe_membership_symbols("CSI300", enrich_metadata=True)

        self.assertEqual(first.created_symbols, 1)
        self.assertEqual(first.provider_symbols_created, 1)
        self.assertEqual(second.created_symbols, 0)
        self.assertEqual(second.provider_symbols_created, 0)
        self.assertEqual(second.metadata_symbols_updated, 0)
        self.assertEqual(Symbol.objects.filter(ticker="600519", exchange="SHG").count(), 1)

    def test_dry_run_reports_changes_without_writing(self):
        Symbol.objects.create(ticker="600519", exchange="SHG", active=True)
        self._import_csi300(
            "CSI300,600519,SHG,XSHG,Kweichow Moutai,2020-01-01,,0.052,,"
            "manual_csv,CN,CNY,Consumer Staples,Distillers & Wineries\n"
        )

        report = ensure_universe_membership_symbols("CSI300", enrich_metadata=True, dry_run=True)

        symbol = Symbol.objects.get(ticker="600519", exchange="SHG")
        membership = UniverseMembership.objects.get(ticker="600519", exchange="SHG")
        self.assertEqual(symbol.name, "")
        self.assertEqual(membership.provider_symbol, "")
        self.assertEqual(report.provider_symbols_created, 1)
        self.assertEqual(report.metadata_symbols_updated, 1)

    def test_command_dry_run_and_apply_emit_actionable_summary(self):
        self._import_csi300(
            "CSI300,600519,SHG,XSHG,Kweichow Moutai,2020-01-01,,0.052,,"
            "manual_csv,CN,CNY,Consumer Staples,Distillers & Wineries\n"
        )
        out = StringIO()

        call_command("enrich_csi300_symbol_metadata", stdout=out)

        dry_output = out.getvalue()
        self.assertIn("dry-run", dry_output)
        self.assertIn("tickers_analyzed=1", dry_output)
        self.assertIn("names_enriched=1", dry_output)
        self.assertEqual(Symbol.objects.count(), 0)

        out = StringIO()
        call_command("enrich_csi300_symbol_metadata", "--apply", stdout=out)

        apply_output = out.getvalue()
        self.assertIn("apply", apply_output)
        self.assertIn("provider_symbols_created=1", apply_output)
        self.assertIn("sector Consumer Staples symbols=1 source=UniverseMembership.source_payload", apply_output)
        self.assertEqual(Symbol.objects.filter(ticker="600519", exchange="SHG").count(), 1)

    def test_command_rejects_dry_run_and_apply_together(self):
        with self.assertRaisesMessage(CommandError, "--dry-run and --apply cannot be used together"):
            call_command("enrich_csi300_symbol_metadata", "--dry-run", "--apply")

    def test_provider_symbol_conflict_is_reported_without_overwrite(self):
        self._import_csi300(
            "CSI300,600519,SHG,XSHG,Kweichow Moutai,2020-01-01,2020-01-01,0.052,600519.SS,"
            "manual_csv,CN,CNY,Consumer Staples,Distillers & Wineries\n"
            "CSI300,600519,SHG,XSHG,Kweichow Moutai,2020-01-02,,0.052,600519.SHG,"
            "manual_csv,CN,CNY,Consumer Staples,Distillers & Wineries\n",
            expected_member_count=1,
        )

        report = ensure_universe_membership_symbols("CSI300", enrich_metadata=True)

        bad_membership = UniverseMembership.objects.get(ticker="600519", valid_to=date(2020, 1, 1))
        self.assertEqual(bad_membership.provider_symbol, "600519.SS")
        self.assertGreaterEqual(len(report.provider_symbol_conflicts), 1)
        self.assertIn("600519:SHG", report.provider_symbol_conflicts[0])

    def test_sp500_memberships_are_not_modified_by_csi300_command(self):
        sp500 = UniverseDefinition.objects.create(code="SP500", name="S&P 500", active=True, source="manual_csv")
        aapl = Symbol.objects.create(ticker="AAPL", exchange="US", active=True)
        UniverseMembership.objects.create(
            universe=sp500,
            symbol=aapl,
            ticker="AAPL",
            exchange="US",
            provider_symbol="",
            valid_from=date(2020, 1, 1),
            source="manual_csv",
            source_payload={"company_name": "Apple Inc.", "row": {"country": "US", "currency": "USD", "sector": "Technology"}},
        )
        self._import_csi300(
            "CSI300,600519,SHG,XSHG,Kweichow Moutai,2020-01-01,,0.052,,"
            "manual_csv,CN,CNY,Consumer Staples,Distillers & Wineries\n"
        )

        call_command("enrich_csi300_symbol_metadata", "--apply", stdout=StringIO())

        aapl.refresh_from_db()
        sp500_membership = UniverseMembership.objects.get(universe=sp500, ticker="AAPL")
        self.assertEqual(aapl.name, "")
        self.assertEqual(aapl.country, "")
        self.assertEqual(sp500_membership.provider_symbol, "")

    @patch("requests.get")
    def test_enrichment_does_not_call_provider(self, requests_get_mock):
        self._import_csi300(
            "CSI300,600519,SHG,XSHG,Kweichow Moutai,2020-01-01,,0.052,,"
            "manual_csv,CN,CNY,Consumer Staples,Distillers & Wineries\n"
        )

        ensure_universe_membership_symbols("CSI300", enrich_metadata=True)

        requests_get_mock.assert_not_called()

    def test_backtest_engine_is_not_coupled_to_metadata_command(self):
        source = (Path(__file__).resolve().parents[1] / "services" / "backtesting" / "engine.py").read_text()

        self.assertNotIn("enrich_csi300_symbol_metadata", source)
        self.assertNotIn("ensure_universe_membership_symbols", source)
