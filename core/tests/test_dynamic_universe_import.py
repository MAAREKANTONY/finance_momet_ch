from __future__ import annotations

from datetime import date
from decimal import Decimal
from io import StringIO
from pathlib import Path
import tempfile
from unittest.mock import patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase

from core.models import (
    DailyBar,
    Scenario,
    Symbol,
    UniverseCoverageSnapshot,
    UniverseCoverageStatus,
    UniverseDefinition,
    UniverseImportBatch,
    UniverseMembership,
)
from core.services.dynamic_universe_symbols import ensure_universe_membership_symbols
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
        self.keep = Symbol.objects.create(ticker="KEEP", exchange="NYSE", name="Keep Corp", active=True)

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

    def _exact_initial_csv(self) -> str:
        return self._csv_file(
            "SP500,AAPL,NASDAQ,AAPL.US,2020-01-01,,Apple Inc.,manual_csv\n"
            "SP500,OLD,NYSE,OLD.US,2020-01-01,,Old Corp,manual_csv\n"
            "SP500,KEEP,NYSE,KEEP.US,2020-01-01,,Keep Corp,manual_csv\n"
        )

    def _exact_target_csv(self) -> str:
        return self._csv_file(
            "SP500,AAPL,NASDAQ,AAPL2.US,2020-01-01,2020-01-02,Apple Inc.,manual_csv\n"
            "SP500,NEW,NYSE,NEW.US,2020-01-01,,New Corp,manual_csv\n"
            "SP500,KEEP,NYSE,KEEP.US,2020-01-01,,Keep Corp,manual_csv\n"
        )

    def _seed_exact_initial_state(self):
        return import_universe_memberships_from_csv(
            self._exact_initial_csv(),
            universe_name="S&P 500 initial",
            coverage_start=date(2019, 12, 31),
            coverage_end=date(2020, 1, 3),
            expected_member_count=1,
            dry_run=False,
        )

    def _apply_exact_target(self, csv_path: str | None = None):
        return import_universe_memberships_from_csv(
            csv_path or self._exact_target_csv(),
            universe_name="S&P 500 exact",
            coverage_start=date(2020, 1, 1),
            coverage_end=date(2020, 1, 2),
            expected_member_count=3,
            dry_run=False,
            replace_existing=True,
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

    def test_historical_mode_without_flag_never_deletes_memberships_or_old_snapshots(self):
        self._seed_exact_initial_state()

        result = import_universe_memberships_from_csv(
            self._exact_target_csv(),
            universe_name="S&P 500 exact",
            coverage_start=date(2020, 1, 1),
            coverage_end=date(2020, 1, 2),
            expected_member_count=3,
            dry_run=False,
        )

        universe = UniverseDefinition.objects.get(code="SP500")
        self.assertFalse(result.replace_existing)
        self.assertEqual(result.memberships_deleted, 0)
        self.assertEqual(result.expected_final_memberships, 4)
        self.assertTrue(universe.memberships.filter(ticker="OLD").exists())
        self.assertTrue(universe.coverage_snapshots.filter(coverage_date=date(2019, 12, 31)).exists())
        self.assertTrue(universe.coverage_snapshots.filter(coverage_date=date(2020, 1, 3)).exists())

    def test_exact_dry_run_reports_full_plan_without_writes(self):
        initial = self._seed_exact_initial_state()
        universe = UniverseDefinition.objects.get(code="SP500")
        memberships_before = list(universe.memberships.order_by("id").values())
        snapshots_before = list(universe.coverage_snapshots.order_by("coverage_date").values())
        batches_before = UniverseImportBatch.objects.count()

        result = import_universe_memberships_from_csv(
            self._exact_target_csv(),
            universe_name="S&P 500 exact",
            coverage_start=date(2020, 1, 1),
            coverage_end=date(2020, 1, 2),
            expected_member_count=3,
            dry_run=True,
            replace_existing=True,
        )

        self.assertTrue(result.dry_run)
        self.assertTrue(result.replace_existing)
        self.assertEqual(result.rows_read, 3)
        self.assertEqual(result.rows_valid, 3)
        self.assertEqual(result.distinct_tickers, 3)
        self.assertEqual(result.active_members, 3)
        self.assertEqual(result.mapped_member_count, 3)
        self.assertEqual(result.unmapped_member_count, 0)
        self.assertEqual(result.memberships_to_create, 1)
        self.assertEqual(result.memberships_to_update, 1)
        self.assertEqual(result.memberships_unchanged, 1)
        self.assertEqual(result.memberships_to_delete, 1)
        self.assertEqual(result.memberships_created, 0)
        self.assertEqual(result.memberships_deleted, 0)
        self.assertEqual(result.snapshots_to_delete, 4)
        self.assertEqual(result.snapshots_to_rebuild, 2)
        self.assertEqual(result.expected_final_memberships, 3)
        self.assertEqual(result.conflicts, 0)
        self.assertEqual(result.errors, [])
        self.assertEqual(result.batch_id, None)
        self.assertEqual(initial.batch_id, UniverseImportBatch.objects.get().id)
        self.assertEqual(UniverseImportBatch.objects.count(), batches_before)
        self.assertEqual(list(universe.memberships.order_by("id").values()), memberships_before)
        self.assertEqual(list(universe.coverage_snapshots.order_by("coverage_date").values()), snapshots_before)
        universe.refresh_from_db()
        self.assertEqual(universe.name, "S&P 500 initial")

    def test_exact_apply_makes_memberships_and_snapshots_equal_to_target(self):
        self._seed_exact_initial_state()

        result = self._apply_exact_target()

        universe = UniverseDefinition.objects.get(code="SP500")
        keys = set(universe.memberships.values_list("ticker", "exchange", "valid_from"))
        self.assertEqual(
            keys,
            {
                ("AAPL", "NASDAQ", date(2020, 1, 1)),
                ("NEW", "NYSE", date(2020, 1, 1)),
                ("KEEP", "NYSE", date(2020, 1, 1)),
            },
        )
        self.assertEqual(result.memberships_created, 1)
        self.assertEqual(result.memberships_updated, 1)
        self.assertEqual(result.memberships_unchanged, 1)
        self.assertEqual(result.memberships_deleted, 1)
        self.assertEqual(result.expected_final_memberships, 3)
        aapl = universe.memberships.get(ticker="AAPL")
        self.assertEqual(aapl.provider_symbol, "AAPL2.US")
        self.assertEqual(aapl.valid_to, date(2020, 1, 2))
        snapshots = list(universe.coverage_snapshots.order_by("coverage_date"))
        self.assertEqual([item.coverage_date for item in snapshots], [date(2020, 1, 1), date(2020, 1, 2)])
        self.assertEqual({item.import_batch_id for item in snapshots}, {result.batch_id})
        self.assertEqual({item.actual_member_count for item in snapshots}, {3})
        self.assertEqual(result.snapshots_deleted, 4)
        self.assertEqual(result.snapshots_created, 2)

    def test_exact_apply_preserves_other_universe_symbols_and_daily_bars(self):
        self._seed_exact_initial_state()
        other = UniverseDefinition.objects.create(code="OTHER", name="Other", source="fixture")
        other_membership = UniverseMembership.objects.create(
            universe=other,
            symbol=self.pudong,
            ticker=self.pudong.ticker,
            exchange=self.pudong.exchange,
            valid_from=date(2018, 1, 1),
            source="fixture",
        )
        other_batch = UniverseImportBatch.objects.create(
            universe=other,
            period_start=date(2018, 1, 1),
            period_end=date(2018, 1, 1),
            expected_member_count=1,
            imported_member_count=1,
            mapped_member_count=1,
            status=UniverseCoverageStatus.VALIDATED,
        )
        other_snapshot = UniverseCoverageSnapshot.objects.create(
            universe=other,
            import_batch=other_batch,
            coverage_date=date(2018, 1, 1),
            expected_member_count=1,
            actual_member_count=1,
            mapped_member_count=1,
            status=UniverseCoverageStatus.VALIDATED,
        )
        bar = DailyBar.objects.create(
            symbol=self.aapl,
            date=date(2020, 1, 1),
            open=Decimal("10"),
            high=Decimal("11"),
            low=Decimal("9"),
            close=Decimal("10.5"),
            volume=100,
        )
        symbols_before = list(Symbol.objects.order_by("id").values())
        bars_before = list(DailyBar.objects.order_by("id").values())
        other_membership_before = UniverseMembership.objects.filter(id=other_membership.id).values().get()
        other_snapshot_before = UniverseCoverageSnapshot.objects.filter(id=other_snapshot.id).values().get()

        self._apply_exact_target()

        self.assertEqual(list(Symbol.objects.order_by("id").values()), symbols_before)
        self.assertEqual(list(DailyBar.objects.order_by("id").values()), bars_before)
        self.assertTrue(DailyBar.objects.filter(id=bar.id).exists())
        self.assertEqual(UniverseMembership.objects.filter(id=other_membership.id).values().get(), other_membership_before)
        self.assertEqual(UniverseCoverageSnapshot.objects.filter(id=other_snapshot.id).values().get(), other_snapshot_before)

    def test_exact_apply_rolls_back_everything_when_upsert_fails_after_deletion(self):
        self._seed_exact_initial_state()
        universe = UniverseDefinition.objects.get(code="SP500")
        universe_before = UniverseDefinition.objects.filter(id=universe.id).values().get()
        memberships_before = list(universe.memberships.order_by("id").values())
        snapshots_before = list(universe.coverage_snapshots.order_by("coverage_date").values())
        batches_before = list(UniverseImportBatch.objects.order_by("id").values())

        with patch("core.services.universe_import._upsert_memberships", side_effect=RuntimeError("forced failure")):
            with self.assertRaisesRegex(RuntimeError, "forced failure"):
                self._apply_exact_target()

        self.assertEqual(UniverseDefinition.objects.filter(id=universe.id).values().get(), universe_before)
        self.assertEqual(list(universe.memberships.order_by("id").values()), memberships_before)
        self.assertEqual(list(universe.coverage_snapshots.order_by("coverage_date").values()), snapshots_before)
        self.assertEqual(list(UniverseImportBatch.objects.order_by("id").values()), batches_before)

    def test_exact_apply_is_business_idempotent_with_a_new_audit_batch(self):
        self._seed_exact_initial_state()
        first = self._apply_exact_target()
        universe = UniverseDefinition.objects.get(code="SP500")
        memberships_after_first = list(
            universe.memberships.order_by("ticker").values(
                "ticker", "exchange", "symbol_id", "provider_symbol", "valid_from", "valid_to", "source", "source_payload"
            )
        )
        snapshots_after_first = list(
            universe.coverage_snapshots.order_by("coverage_date").values(
                "coverage_date",
                "expected_member_count",
                "actual_member_count",
                "mapped_member_count",
                "unmapped_member_count",
                "status",
                "metadata",
            )
        )
        batch_count = UniverseImportBatch.objects.count()

        second = self._apply_exact_target()

        self.assertEqual(second.memberships_created, 0)
        self.assertEqual(second.memberships_updated, 0)
        self.assertEqual(second.memberships_deleted, 0)
        self.assertEqual(second.memberships_unchanged, 3)
        self.assertNotEqual(second.batch_id, first.batch_id)
        self.assertEqual(UniverseImportBatch.objects.count(), batch_count + 1)
        self.assertEqual(
            list(
                universe.memberships.order_by("ticker").values(
                    "ticker", "exchange", "symbol_id", "provider_symbol", "valid_from", "valid_to", "source", "source_payload"
                )
            ),
            memberships_after_first,
        )
        self.assertEqual(
            list(
                universe.coverage_snapshots.order_by("coverage_date").values(
                    "coverage_date",
                    "expected_member_count",
                    "actual_member_count",
                    "mapped_member_count",
                    "unmapped_member_count",
                    "status",
                    "metadata",
                )
            ),
            snapshots_after_first,
        )

    def test_exact_apply_refuses_empty_target_without_deleting(self):
        self._seed_exact_initial_state()
        empty_csv = self._csv_file("")
        counts_before = (
            UniverseMembership.objects.count(),
            UniverseCoverageSnapshot.objects.count(),
            UniverseImportBatch.objects.count(),
        )

        with self.assertRaisesRegex(UniverseImportError, "no membership rows"):
            import_universe_memberships_from_csv(
                empty_csv,
                coverage_start=date(2020, 1, 1),
                coverage_end=date(2020, 1, 2),
                expected_member_count=1,
                dry_run=False,
                replace_existing=True,
            )

        self.assertEqual(
            (
                UniverseMembership.objects.count(),
                UniverseCoverageSnapshot.objects.count(),
                UniverseImportBatch.objects.count(),
            ),
            counts_before,
        )

    def test_exact_apply_rejects_csv_error_without_writes(self):
        self._seed_exact_initial_state()
        invalid_csv = self._csv_file(
            "SP500,AAPL,NASDAQ,AAPL.US,not-a-date,,Apple Inc.,manual_csv\n"
        )
        memberships_before = list(UniverseMembership.objects.order_by("id").values())
        snapshots_before = list(UniverseCoverageSnapshot.objects.order_by("id").values())
        batches_before = UniverseImportBatch.objects.count()

        with self.assertRaisesRegex(UniverseImportError, "valid_from must be YYYY-MM-DD"):
            import_universe_memberships_from_csv(invalid_csv, dry_run=False, replace_existing=True)

        self.assertEqual(list(UniverseMembership.objects.order_by("id").values()), memberships_before)
        self.assertEqual(list(UniverseCoverageSnapshot.objects.order_by("id").values()), snapshots_before)
        self.assertEqual(UniverseImportBatch.objects.count(), batches_before)

    def test_exact_apply_rejects_active_unmapped_members_without_deleting(self):
        self._seed_exact_initial_state()
        unmapped_csv = self._csv_file(
            "SP500,MISSING,NYSE,MISSING.US,2020-01-01,,Missing Corp,manual_csv\n"
        )
        memberships_before = list(UniverseMembership.objects.order_by("id").values())
        snapshots_before = list(UniverseCoverageSnapshot.objects.order_by("id").values())

        with self.assertRaisesRegex(UniverseImportError, "active unmapped or ambiguous"):
            import_universe_memberships_from_csv(
                unmapped_csv,
                coverage_start=date(2020, 1, 1),
                coverage_end=date(2020, 1, 2),
                expected_member_count=1,
                dry_run=False,
                replace_existing=True,
            )

        self.assertEqual(list(UniverseMembership.objects.order_by("id").values()), memberships_before)
        self.assertEqual(list(UniverseCoverageSnapshot.objects.order_by("id").values()), snapshots_before)

    def test_exact_dry_run_reports_duplicate_business_key_conflict(self):
        self._seed_exact_initial_state()
        duplicate_csv = self._csv_file(
            "SP500,AAPL,NASDAQ,AAPL.US,2020-01-01,,Apple Inc.,manual_csv\n"
            "SP500,AAPL,NASDAQ,AAPL2.US,2020-01-01,,Apple Inc.,manual_csv\n"
        )
        memberships_before = list(UniverseMembership.objects.order_by("id").values())

        result = import_universe_memberships_from_csv(
            duplicate_csv,
            coverage_start=date(2020, 1, 1),
            coverage_end=date(2020, 1, 1),
            expected_member_count=1,
            dry_run=True,
            replace_existing=True,
        )

        self.assertEqual(result.conflicts, 1)
        self.assertEqual(result.status, UniverseCoverageStatus.FAILED)
        self.assertIn("duplicate membership business key", " ".join(result.errors))
        self.assertEqual(list(UniverseMembership.objects.order_by("id").values()), memberships_before)

    def test_generic_cli_supports_exact_dry_run_and_apply_with_counters(self):
        self._seed_exact_initial_state()
        csv_path = self._exact_target_csv()
        out = StringIO()

        call_command(
            "import_universe_memberships",
            "--csv",
            csv_path,
            "--universe-code",
            "SP500",
            "--coverage-start",
            "2020-01-01",
            "--coverage-end",
            "2020-01-02",
            "--expected-member-count",
            "3",
            "--dry-run",
            "--replace-existing",
            stdout=out,
        )
        dry_output = out.getvalue()
        self.assertIn("mode=dry-run", dry_output)
        self.assertIn("replace_existing=true", dry_output)
        self.assertIn("to_create=1", dry_output)
        self.assertIn("to_update=1", dry_output)
        self.assertIn("unchanged=1", dry_output)
        self.assertIn("to_delete=1", dry_output)
        self.assertIn("snapshots_to_delete=4", dry_output)
        self.assertEqual(UniverseMembership.objects.count(), 3)

        out = StringIO()
        call_command(
            "import_universe_memberships",
            "--csv",
            csv_path,
            "--universe-code",
            "SP500",
            "--coverage-start",
            "2020-01-01",
            "--coverage-end",
            "2020-01-02",
            "--expected-member-count",
            "3",
            "--apply",
            "--replace-existing",
            stdout=out,
        )
        apply_output = out.getvalue()
        self.assertIn("mode=apply", apply_output)
        self.assertIn("deleted=1", apply_output)
        self.assertIn("snapshots_created=2", apply_output)
        self.assertEqual(UniverseMembership.objects.count(), 3)

    def test_exact_apply_simulates_csi300_2023_cutover(self):
        old_csv = self._csv_file(
            "CSI300,OLD,NYSE,OLD.US,2010-01-01,2012-12-31,Old Corp,manual_csv\n"
            "CSI300,600519,XSHG,600519.SHG,2019-01-01,,Kweichow Moutai,manual_csv\n"
        )
        import_universe_memberships_from_csv(
            old_csv,
            universe_code="CSI300",
            universe_name="CSI 300",
            coverage_start=date(2010, 1, 1),
            coverage_end=date(2010, 1, 2),
            expected_member_count=1,
            dry_run=False,
        )
        target_csv = self._csv_file(
            "CSI300,600519,XSHG,600519.SHG,2023-01-03,,Kweichow Moutai,manual_csv\n"
            "CSI300,000001,XSHE,000001.SHE,2023-01-03,,Ping An Bank,manual_csv\n"
            "CSI300,600000,XSHG,600000.SHG,2023-01-04,,SPD Bank,manual_csv\n"
        )

        result = import_universe_memberships_from_csv(
            target_csv,
            universe_code="CSI300",
            universe_name="CSI 300",
            coverage_start=date(2023, 1, 3),
            coverage_end=date(2023, 1, 4),
            expected_member_count=2,
            dry_run=False,
            replace_existing=True,
        )

        universe = UniverseDefinition.objects.get(code="CSI300")
        self.assertEqual(result.memberships_deleted, 2)
        self.assertEqual(result.memberships_created, 3)
        self.assertEqual(result.expected_final_memberships, 3)
        self.assertEqual(universe.memberships.count(), 3)
        self.assertFalse(universe.memberships.filter(valid_from__lt=date(2023, 1, 3)).exists())
        self.assertEqual(
            set(universe.coverage_snapshots.values_list("coverage_date", flat=True)),
            {date(2023, 1, 3), date(2023, 1, 4)},
        )

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

    def test_symbol_mapping_creates_csi300_symbols_and_refreshes_coverage(self):
        csv_path = self._raw_csv_file(
            "universe_code,symbol,exchange,mic,name,start_date,end_date,weight,provider_symbol,source,country,currency,sector,industry\n",
            "CSI300,600519,SHG,XSHG,Kweichow Moutai,2020-01-01,,0.052,600519.SHG,manual_csv,CN,CNY,Consumer Staples,Distillers & Wineries\n"
            "CSI300,000001,SHE,XSHE,Ping An Bank,2020-01-01,,0.014,000001.SHE,manual_csv,CN,CNY,Financials,Banks\n",
        )
        imported = import_universe_memberships_from_csv(
            csv_path,
            universe_code="CSI300",
            universe_name="CSI 300",
            coverage_start=date(2020, 1, 1),
            coverage_end=date(2020, 1, 2),
            expected_member_count=2,
            dry_run=False,
        )
        self.assertEqual(imported.status, UniverseCoverageStatus.PARTIAL)

        report = ensure_universe_membership_symbols("CSI300")

        self.assertEqual(report.memberships_total, 2)
        self.assertEqual(report.created_symbols, 2)
        self.assertEqual(report.still_unmapped, 0)
        self.assertTrue(Symbol.objects.filter(ticker="000001", exchange="SHE", country="CN", currency="CNY").exists())
        self.assertFalse(Symbol.objects.filter(ticker="1", exchange="SHE").exists())
        membership = UniverseMembership.objects.get(ticker="000001", exchange="SHE")
        self.assertIsNotNone(membership.symbol_id)
        self.assertEqual(membership.provider_symbol, "000001.SHE")
        batch = UniverseImportBatch.objects.get(id=imported.batch_id)
        self.assertEqual(batch.status, UniverseCoverageStatus.VALIDATED)
        self.assertEqual(batch.mapped_member_count, 2)
        self.assertEqual(batch.unmapped_member_count, 0)
        self.assertEqual(set(UniverseCoverageSnapshot.objects.values_list("status", flat=True)), {UniverseCoverageStatus.VALIDATED})
        self.assertEqual(set(UniverseCoverageSnapshot.objects.values_list("mapped_member_count", flat=True)), {2})

    def test_symbol_mapping_links_existing_symbol_without_duplicate(self):
        csv_path = self._raw_csv_file(
            "universe_code,symbol,exchange,start_date,provider_symbol\n",
            "CSI300,000001,SHE,2020-01-01,000001.SHE\n",
        )
        import_universe_memberships_from_csv(
            csv_path,
            universe_code="CSI300",
            universe_name="CSI 300",
            coverage_start=date(2020, 1, 1),
            coverage_end=date(2020, 1, 1),
            expected_member_count=1,
            dry_run=False,
        )
        existing = Symbol.objects.create(ticker="000001", exchange="SHE", name="Ping An Bank", active=True)

        report = ensure_universe_membership_symbols("CSI300")

        self.assertEqual(report.linked_existing_symbols, 1)
        self.assertEqual(report.created_symbols, 0)
        self.assertEqual(Symbol.objects.filter(ticker="000001", exchange="SHE").count(), 1)
        self.assertEqual(UniverseMembership.objects.get(ticker="000001").symbol, existing)

    def test_symbol_mapping_warns_on_unsupported_csi300_exchange(self):
        csv_path = self._raw_csv_file(
            "universe_code,symbol,exchange,start_date,provider_symbol\n",
            "CSI300,123456,HK,2020-01-01,123456.HK\n",
        )
        import_universe_memberships_from_csv(
            csv_path,
            universe_code="CSI300",
            universe_name="CSI 300",
            coverage_start=date(2020, 1, 1),
            coverage_end=date(2020, 1, 1),
            expected_member_count=1,
            dry_run=False,
        )

        report = ensure_universe_membership_symbols("CSI300")

        self.assertEqual(report.created_symbols, 0)
        self.assertEqual(report.still_unmapped, 1)
        self.assertEqual(report.unsupported_exchanges, ["123456:HK"])
        self.assertFalse(Symbol.objects.filter(ticker="123456", exchange="HK").exists())

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
