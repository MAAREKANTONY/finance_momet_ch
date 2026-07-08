from __future__ import annotations

from datetime import date
from io import StringIO
from pathlib import Path
import tempfile
from unittest.mock import patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase

from core.models import (
    Scenario,
    Symbol,
    UniverseCoverageSnapshot,
    UniverseCoverageStatus,
    UniverseDefinition,
    UniverseImportBatch,
    UniverseMembership,
)
from core.services.universe_import import UniverseImportError, import_universe_memberships_from_csv
from core.services.universe_resolver import UniverseCoverageError, UniverseResolver


CSV_HEADER = "universe_code,ticker,exchange,provider_symbol,valid_from,valid_to,company_name,source\n"


class DynamicUniverseImportTests(TestCase):
    def setUp(self):
        self.aapl = Symbol.objects.create(ticker="AAPL", exchange="NASDAQ", name="Apple Inc.", active=True)
        self.old = Symbol.objects.create(ticker="OLD", exchange="NYSE", name="Old Corp", active=True)
        self.new = Symbol.objects.create(ticker="NEW", exchange="NYSE", name="New Corp", active=True)
        self.moutai = Symbol.objects.create(ticker="600519", exchange="XSHG", name="Kweichow Moutai", active=True)
        self.pingan = Symbol.objects.create(ticker="000001", exchange="XSHE", name="Ping An Bank", active=True)
        self.pudong = Symbol.objects.create(ticker="600000", exchange="XSHG", name="SPD Bank", active=True)

    def _csv_file(self, body: str) -> str:
        handle = tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False, encoding="utf-8")
        handle.write(CSV_HEADER)
        handle.write(body)
        handle.flush()
        handle.close()
        return handle.name

    def _single_member_csv(self, provider_symbol: str = "AAPL.US", valid_to: str = "") -> str:
        return self._csv_file(
            f"SP500,AAPL,NASDAQ,{provider_symbol},2020-01-01,{valid_to},Apple Inc.,manual_csv\n"
        )

    def _resolve(self, start: date = date(2020, 1, 1), end: date = date(2020, 1, 3)):
        scenario = Scenario.objects.create(
            name="Dynamic",
            universe_mode=Scenario.UniverseMode.SP500_HISTORICAL_DYNAMIC,
            active=True,
        )
        return UniverseResolver().resolve(scenario, start_date=start, end_date=end)

    def test_dry_run_complete_does_not_modify_database(self):
        csv_path = self._single_member_csv()

        result = import_universe_memberships_from_csv(
            csv_path,
            coverage_start=date(2020, 1, 1),
            coverage_end=date(2020, 1, 3),
            expected_member_count=1,
            dry_run=True,
        )

        self.assertTrue(result.dry_run)
        self.assertEqual(result.rows_read, 1)
        self.assertEqual(result.status, UniverseCoverageStatus.VALIDATED)
        self.assertIn("would be created", " ".join(result.warnings))
        self.assertEqual(UniverseDefinition.objects.count(), 0)
        self.assertEqual(UniverseMembership.objects.count(), 0)
        self.assertEqual(UniverseImportBatch.objects.count(), 0)
        self.assertEqual(UniverseCoverageSnapshot.objects.count(), 0)

    def test_import_complete_validated_creates_memberships_batch_snapshots_and_resolves(self):
        csv_path = self._csv_file(
            "SP500,AAPL,NASDAQ,AAPL.US,2020-01-01,,Apple Inc.,manual_csv\n"
            "SP500,OLD,NYSE,OLD.US,2020-01-01,2020-01-02,Old Corp,manual_csv\n"
            "SP500,NEW,NYSE,NEW.US,2020-01-03,,New Corp,manual_csv\n"
        )

        result = import_universe_memberships_from_csv(
            csv_path,
            coverage_start=date(2020, 1, 1),
            coverage_end=date(2020, 1, 3),
            expected_member_count=2,
            dry_run=False,
            source_reference="fixture.csv",
        )

        self.assertFalse(result.dry_run)
        self.assertEqual(result.status, UniverseCoverageStatus.VALIDATED)
        self.assertEqual(result.memberships_created, 3)
        self.assertEqual(result.coverage_days, 3)
        universe = UniverseDefinition.objects.get(code="SP500")
        self.assertEqual(universe.name, "S&P 500")
        self.assertEqual(UniverseMembership.objects.filter(universe=universe).count(), 3)
        batch = UniverseImportBatch.objects.get(id=result.batch_id)
        self.assertEqual(batch.status, UniverseCoverageStatus.VALIDATED)
        self.assertEqual(batch.source_reference, "fixture.csv")
        self.assertEqual(
            set(UniverseCoverageSnapshot.objects.values_list("status", flat=True)),
            {UniverseCoverageStatus.VALIDATED},
        )

        resolved = self._resolve()
        self.assertEqual(set(resolved.tickers), {"AAPL", "OLD", "NEW"})
        self.assertIn("OLD", resolved.active_by_date[date(2020, 1, 1)])
        self.assertNotIn("OLD", resolved.active_by_date[date(2020, 1, 3)])
        self.assertNotIn("NEW", resolved.active_by_date[date(2020, 1, 2)])

    def test_import_with_unmapped_symbol_is_partial_and_resolver_blocks(self):
        csv_path = self._csv_file("SP500,MISSING,NYSE,MISSING.US,2020-01-01,,Missing Corp,manual_csv\n")

        result = import_universe_memberships_from_csv(
            csv_path,
            coverage_start=date(2020, 1, 1),
            coverage_end=date(2020, 1, 2),
            expected_member_count=1,
            dry_run=False,
        )

        self.assertEqual(result.status, UniverseCoverageStatus.PARTIAL)
        self.assertEqual(result.unmapped_member_count, 1)
        self.assertEqual(UniverseMembership.objects.get().symbol_id, None)
        self.assertEqual(
            set(UniverseCoverageSnapshot.objects.values_list("status", flat=True)),
            {UniverseCoverageStatus.PARTIAL},
        )
        with self.assertRaises(UniverseCoverageError):
            self._resolve(end=date(2020, 1, 2))

    def test_invalid_csv_date_raises_without_silent_validation(self):
        csv_path = self._csv_file("SP500,AAPL,NASDAQ,AAPL.US,not-a-date,,Apple Inc.,manual_csv\n")

        with self.assertRaisesRegex(UniverseImportError, "valid_from must be YYYY-MM-DD"):
            import_universe_memberships_from_csv(
                csv_path,
                coverage_start=date(2020, 1, 1),
                coverage_end=date(2020, 1, 1),
                expected_member_count=1,
                dry_run=False,
            )

        self.assertEqual(UniverseImportBatch.objects.count(), 0)
        self.assertEqual(UniverseCoverageSnapshot.objects.count(), 0)

    def test_expected_member_count_mismatch_marks_partial_and_resolver_blocks(self):
        csv_path = self._single_member_csv()

        result = import_universe_memberships_from_csv(
            csv_path,
            coverage_start=date(2020, 1, 1),
            coverage_end=date(2020, 1, 2),
            expected_member_count=2,
            dry_run=False,
        )

        self.assertEqual(result.status, UniverseCoverageStatus.PARTIAL)
        self.assertEqual(
            set(UniverseCoverageSnapshot.objects.values_list("status", flat=True)),
            {UniverseCoverageStatus.PARTIAL},
        )
        with self.assertRaises(UniverseCoverageError):
            self._resolve(end=date(2020, 1, 2))

    def test_partial_csv_does_not_validate_from_existing_memberships(self):
        full_csv = self._csv_file(
            "SP500,AAPL,NASDAQ,AAPL.US,2020-01-01,,Apple Inc.,manual_csv\n"
            "SP500,OLD,NYSE,OLD.US,2020-01-01,,Old Corp,manual_csv\n"
            "SP500,NEW,NYSE,NEW.US,2020-01-01,,New Corp,manual_csv\n"
        )
        partial_csv = self._single_member_csv(provider_symbol="AAPL2.US")

        full = import_universe_memberships_from_csv(
            full_csv,
            coverage_start=date(2020, 1, 1),
            coverage_end=date(2020, 1, 1),
            expected_member_count=3,
            dry_run=False,
        )
        partial = import_universe_memberships_from_csv(
            partial_csv,
            coverage_start=date(2020, 1, 1),
            coverage_end=date(2020, 1, 1),
            expected_member_count=3,
            dry_run=False,
        )

        self.assertEqual(full.status, UniverseCoverageStatus.VALIDATED)
        self.assertEqual(partial.rows_read, 1)
        self.assertEqual(partial.imported_member_count, 1)
        self.assertEqual(partial.status, UniverseCoverageStatus.PARTIAL)
        snapshot = UniverseCoverageSnapshot.objects.get(coverage_date=date(2020, 1, 1))
        self.assertEqual(snapshot.actual_member_count, 1)
        self.assertEqual(snapshot.status, UniverseCoverageStatus.PARTIAL)

    def test_import_upserts_existing_membership_without_duplicates(self):
        first_csv = self._single_member_csv(provider_symbol="AAPL.US")
        second_csv = self._single_member_csv(provider_symbol="AAPL2.US", valid_to="2020-01-01")

        first = import_universe_memberships_from_csv(
            first_csv,
            coverage_start=date(2020, 1, 1),
            coverage_end=date(2020, 1, 1),
            expected_member_count=1,
            dry_run=False,
        )
        second = import_universe_memberships_from_csv(
            second_csv,
            coverage_start=date(2020, 1, 1),
            coverage_end=date(2020, 1, 1),
            expected_member_count=1,
            dry_run=False,
        )

        self.assertEqual(first.memberships_created, 1)
        self.assertEqual(second.memberships_created, 0)
        self.assertEqual(second.memberships_updated, 1)
        self.assertEqual(UniverseMembership.objects.count(), 1)
        membership = UniverseMembership.objects.get()
        self.assertEqual(membership.provider_symbol, "AAPL2.US")
        self.assertEqual(membership.valid_to, date(2020, 1, 1))

    def test_command_dry_run_does_not_modify_database(self):
        csv_path = self._single_member_csv()
        out = StringIO()

        call_command(
            "import_sp500_memberships",
            "--file",
            csv_path,
            "--coverage-start",
            "2020-01-01",
            "--coverage-end",
            "2020-01-01",
            "--expected-member-count",
            "1",
            stdout=out,
        )

        self.assertIn("mode=dry-run", out.getvalue())
        self.assertEqual(UniverseMembership.objects.count(), 0)
        self.assertEqual(UniverseImportBatch.objects.count(), 0)

    def test_command_apply_persists_import(self):
        csv_path = self._single_member_csv()
        out = StringIO()

        call_command(
            "import_sp500_memberships",
            "--file",
            csv_path,
            "--coverage-start",
            "2020-01-01",
            "--coverage-end",
            "2020-01-01",
            "--expected-member-count",
            "1",
            "--apply",
            stdout=out,
        )

        self.assertIn("mode=apply", out.getvalue())
        self.assertIn("status=VALIDATED", out.getvalue())
        self.assertEqual(UniverseMembership.objects.count(), 1)
        self.assertEqual(UniverseImportBatch.objects.count(), 1)
        self.assertEqual(UniverseCoverageSnapshot.objects.count(), 1)

    def test_command_invalid_csv_returns_command_error(self):
        csv_path = self._csv_file("SP500,AAPL,NASDAQ,AAPL.US,bad-date,,Apple Inc.,manual_csv\n")

        with self.assertRaises(CommandError):
            call_command(
                "import_sp500_memberships",
                "--file",
                csv_path,
                "--coverage-start",
                "2020-01-01",
                "--coverage-end",
                "2020-01-01",
                "--apply",
            )


    def _raw_csv_file(self, header: str, body: str) -> str:
        handle = tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False, encoding="utf-8")
        handle.write(header)
        handle.write(body)
        handle.flush()
        handle.close()
        return handle.name

    def _modern_csi300_csv(self) -> str:
        return self._raw_csv_file(
            "universe_code,symbol,exchange,mic,name,start_date,end_date,weight,provider_symbol,source,country,currency,sector,industry\n",
            "CSI300,600519,XSHG,XSHG,Kweichow Moutai,2020-01-01,,0.052,,manual_csv,CN,CNY,Consumer Defensive,Beverages\n"
            "CSI300,000001,XSHE,XSHE,Ping An Bank,2020-01-01,2023-06-30,0.014,,manual_csv,CN,CNY,Financials,Banks\n",
        )

    def test_generic_csv_accepts_csi300_modern_format_in_dry_run(self):
        csv_path = self._modern_csi300_csv()

        result = import_universe_memberships_from_csv(
            csv_path,
            universe_code="CSI300",
            universe_name="CSI 300",
            expected_member_count=1,
            dry_run=True,
        )

        self.assertTrue(result.dry_run)
        self.assertEqual(result.universe_code, "CSI300")
        self.assertEqual(result.universe_name, "CSI 300")
        self.assertEqual(result.rows_read, 2)
        self.assertEqual(result.rows_valid, 2)
        self.assertEqual(result.rows_rejected, 0)
        self.assertEqual(result.distinct_tickers, 2)
        self.assertEqual(result.exchanges, ["XSHE", "XSHG"])
        self.assertEqual(result.valid_from_min, date(2020, 1, 1))
        self.assertEqual(result.valid_to_max, date(2023, 6, 30))
        self.assertEqual(result.open_memberships, 1)
        self.assertEqual(UniverseDefinition.objects.count(), 0)
        self.assertEqual(UniverseMembership.objects.count(), 0)
        self.assertEqual(UniverseImportBatch.objects.count(), 0)

    def test_generic_csv_apply_creates_csi300_universe_and_memberships(self):
        csv_path = self._modern_csi300_csv()

        result = import_universe_memberships_from_csv(
            csv_path,
            universe_code="CSI300",
            universe_name="CSI 300",
            coverage_start=date(2020, 1, 1),
            coverage_end=date(2020, 1, 2),
            expected_member_count=2,
            dry_run=False,
        )

        self.assertFalse(result.dry_run)
        self.assertEqual(result.status, UniverseCoverageStatus.VALIDATED)
        universe = UniverseDefinition.objects.get(code="CSI300")
        self.assertEqual(universe.name, "CSI 300")
        self.assertEqual(UniverseMembership.objects.filter(universe=universe).count(), 2)
        moutai = UniverseMembership.objects.get(universe=universe, ticker="600519")
        self.assertEqual(moutai.exchange, "XSHG")
        self.assertEqual(moutai.symbol, self.moutai)
        self.assertEqual(moutai.provider_symbol, "")
        self.assertEqual(moutai.source_payload["company_name"], "Kweichow Moutai")
        self.assertEqual(moutai.source_payload["mic"], "XSHG")
        self.assertEqual(moutai.source_payload["extras"]["weight"], "0.052")
        self.assertEqual(moutai.source_payload["extras"]["sector"], "Consumer Defensive")
        pingan = UniverseMembership.objects.get(universe=universe, ticker="000001")
        self.assertEqual(pingan.exchange, "XSHE")
        self.assertEqual(pingan.valid_to, date(2023, 6, 30))

    def test_generic_csv_preserves_numeric_symbol_leading_zeroes(self):
        csv_path = self._modern_csi300_csv()

        import_universe_memberships_from_csv(
            csv_path,
            universe_code="CSI300",
            universe_name="CSI 300",
            coverage_start=date(2020, 1, 1),
            coverage_end=date(2020, 1, 1),
            expected_member_count=2,
            dry_run=False,
        )

        self.assertTrue(UniverseMembership.objects.filter(ticker="000001", exchange="XSHE").exists())
        self.assertFalse(UniverseMembership.objects.filter(ticker="1", exchange="XSHE").exists())

    def test_generic_csv_uses_mic_as_exchange_fallback(self):
        csv_path = self._raw_csv_file(
            "universe_code,symbol,mic,name,start_date,end_date,weight\n",
            "SSE50,600000,XSHG,SPD Bank,2020-01-01,,0.021\n",
        )

        result = import_universe_memberships_from_csv(
            csv_path,
            universe_code="SSE50",
            universe_name="SSE 50",
            coverage_start=date(2020, 1, 1),
            coverage_end=date(2020, 1, 1),
            expected_member_count=1,
            dry_run=False,
        )

        self.assertEqual(result.status, UniverseCoverageStatus.VALIDATED)
        membership = UniverseMembership.objects.get(ticker="600000")
        self.assertEqual(membership.exchange, "XSHG")
        self.assertEqual(membership.symbol, self.pudong)
        self.assertEqual(membership.source_payload["extras"]["weight"], "0.021")

    def test_generic_csv_rejects_universe_code_mismatch(self):
        csv_path = self._raw_csv_file(
            "universe_code,symbol,start_date\n",
            "SSE180,600000,2020-01-01\n",
        )

        with self.assertRaisesRegex(UniverseImportError, "does not match requested universe_code=CSI300"):
            import_universe_memberships_from_csv(csv_path, universe_code="CSI300", dry_run=True)

    def test_generic_csv_rejects_end_date_before_start_date(self):
        csv_path = self._raw_csv_file(
            "universe_code,symbol,start_date,end_date\n",
            "CSI300,600519,2020-01-02,2020-01-01\n",
        )

        with self.assertRaisesRegex(UniverseImportError, "valid_to must be greater than or equal to valid_from"):
            import_universe_memberships_from_csv(csv_path, universe_code="CSI300", dry_run=True)

    def test_generic_csv_apply_is_idempotent(self):
        csv_path = self._modern_csi300_csv()

        first = import_universe_memberships_from_csv(
            csv_path,
            universe_code="CSI300",
            universe_name="CSI 300",
            coverage_start=date(2020, 1, 1),
            coverage_end=date(2020, 1, 1),
            expected_member_count=2,
            dry_run=False,
        )
        second = import_universe_memberships_from_csv(
            csv_path,
            universe_code="CSI300",
            universe_name="CSI 300",
            coverage_start=date(2020, 1, 1),
            coverage_end=date(2020, 1, 1),
            expected_member_count=2,
            dry_run=False,
        )

        self.assertEqual(first.memberships_created, 2)
        self.assertEqual(second.memberships_created, 0)
        self.assertEqual(UniverseMembership.objects.count(), 2)
        self.assertEqual(UniverseImportBatch.objects.count(), 2)
        self.assertEqual(UniverseCoverageSnapshot.objects.count(), 1)

    def test_generic_command_dry_run_and_apply(self):
        csv_path = self._modern_csi300_csv()
        out = StringIO()

        call_command(
            "import_universe_memberships",
            "--csv",
            csv_path,
            "--universe-code",
            "CSI300",
            "--universe-name",
            "CSI 300",
            "--coverage-start",
            "2020-01-01",
            "--coverage-end",
            "2020-01-01",
            "--expected-member-count",
            "2",
            stdout=out,
        )
        self.assertIn("mode=dry-run", out.getvalue())
        self.assertEqual(UniverseMembership.objects.count(), 0)

        out = StringIO()
        call_command(
            "import_universe_memberships",
            "--csv",
            csv_path,
            "--universe-code",
            "CSI300",
            "--universe-name",
            "CSI 300",
            "--coverage-start",
            "2020-01-01",
            "--coverage-end",
            "2020-01-01",
            "--expected-member-count",
            "2",
            "--apply",
            stdout=out,
        )
        self.assertIn("mode=apply", out.getvalue())
        self.assertIn("universe=CSI300", out.getvalue())
        self.assertEqual(UniverseMembership.objects.count(), 2)

    def test_generic_command_rejects_dry_run_and_apply_together(self):
        csv_path = self._modern_csi300_csv()

        with self.assertRaisesMessage(CommandError, "--dry-run and --apply cannot be used together"):
            call_command(
                "import_universe_memberships",
                "--csv",
                csv_path,
                "--universe-code",
                "CSI300",
                "--dry-run",
                "--apply",
            )

        self.assertEqual(UniverseDefinition.objects.count(), 0)
        self.assertEqual(UniverseMembership.objects.count(), 0)
        self.assertEqual(UniverseImportBatch.objects.count(), 0)

    @patch("requests.get")
    def test_generic_csv_import_does_not_call_provider(self, requests_get_mock):
        csv_path = self._modern_csi300_csv()

        import_universe_memberships_from_csv(
            csv_path,
            universe_code="CSI300",
            universe_name="CSI 300",
            coverage_start=date(2020, 1, 1),
            coverage_end=date(2020, 1, 1),
            expected_member_count=2,
            dry_run=False,
        )

        requests_get_mock.assert_not_called()

    def test_backtest_game_and_provider_files_do_not_import_dynamic_universe_import(self):
        base = Path(__file__).resolve().parents[1]
        paths = [
            base / "services" / "backtesting" / "engine.py",
            base / "services" / "backtesting" / "prep.py",
            base / "services" / "backtesting" / "diagnostic.py",
            base / "services" / "game_scenarios" / "runner.py",
            base / "services" / "game_scenarios" / "sync.py",
            base / "services" / "provider_eodhd.py",
            base / "services" / "provider_twelvedata.py",
        ]
        for path in paths:
            source = path.read_text()
            self.assertNotIn("universe_import", source)
            self.assertNotIn("import_universe_memberships_from_csv", source)
