from __future__ import annotations

from datetime import date
from io import StringIO
from unittest.mock import patch

from django.core.management import call_command
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
from core.services.provider_eodhd import EODHDError
from core.services.universe_eodhd_sync import sync_sp500_historical_memberships_from_eodhd
from core.services.universe_resolver import UniverseCoverageError, UniverseResolver


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


class DynamicUniverseEODHDSyncTests(TestCase):
    def setUp(self):
        self.aapl = Symbol.objects.create(ticker="AAPL", exchange="NASDAQ", name="Apple Inc.", active=True)
        self.old = Symbol.objects.create(ticker="OLD", exchange="NYSE", name="Old Corp", active=True)
        self.new = Symbol.objects.create(ticker="NEW", exchange="NYSE", name="New Corp", active=True)

    def _records(self):
        return [
            record("AAPL", "Apple Inc", "2020-01-01", None),
            record("OLD", "Old Corp", "2020-01-01", "2020-01-02", active=0),
            record("NEW", "New Corp", "2020-01-03", None),
        ]

    def _sync(self, records=None, *, apply=False, expected=2, start=date(2020, 1, 1), end=date(2020, 1, 3)):
        return sync_sp500_historical_memberships_from_eodhd(
            coverage_start=start,
            coverage_end=end,
            expected_member_count=expected,
            dry_run=not apply,
            client=FakeEODHDClient(records or self._records()),
        )

    def _resolve(self, start=date(2020, 1, 1), end=date(2020, 1, 3)):
        scenario = Scenario.objects.create(
            name="Dynamic",
            universe_mode=Scenario.UniverseMode.SP500_HISTORICAL_DYNAMIC,
            active=True,
        )
        return UniverseResolver().resolve(scenario, start_date=start, end_date=end)

    def test_dry_run_fetches_provider_but_does_not_persist(self):
        client = FakeEODHDClient(self._records())

        result = sync_sp500_historical_memberships_from_eodhd(
            coverage_start=date(2020, 1, 1),
            coverage_end=date(2020, 1, 3),
            expected_member_count=2,
            dry_run=True,
            client=client,
        )

        self.assertTrue(client.called)
        self.assertTrue(result.dry_run)
        self.assertEqual(result.provider_records, 3)
        self.assertEqual(result.records_retained, 3)
        self.assertEqual(result.status, UniverseCoverageStatus.VALIDATED)
        self.assertIn("would be created", " ".join(result.warnings))
        self.assertEqual(UniverseDefinition.objects.count(), 0)
        self.assertEqual(UniverseMembership.objects.count(), 0)
        self.assertEqual(UniverseImportBatch.objects.count(), 0)
        self.assertEqual(UniverseCoverageSnapshot.objects.count(), 0)

    def test_apply_creates_memberships_batch_snapshots_and_resolves(self):
        result = self._sync(apply=True)

        self.assertFalse(result.dry_run)
        self.assertEqual(result.status, UniverseCoverageStatus.VALIDATED)
        self.assertEqual(result.memberships_created, 3)
        self.assertEqual(result.coverage_days, 3)
        universe = UniverseDefinition.objects.get(code="SP500")
        self.assertEqual(universe.source, "eodhd_fundamentals")
        self.assertEqual(UniverseMembership.objects.filter(universe=universe).count(), 3)
        batch = UniverseImportBatch.objects.get(id=result.batch_id)
        self.assertEqual(batch.provider, "eodhd")
        self.assertEqual(batch.source_name, "eodhd_fundamentals")
        self.assertEqual(batch.status, UniverseCoverageStatus.VALIDATED)
        self.assertEqual(
            set(UniverseCoverageSnapshot.objects.values_list("status", flat=True)),
            {UniverseCoverageStatus.VALIDATED},
        )

        resolved = self._resolve()
        self.assertEqual(set(resolved.tickers), {"AAPL", "OLD", "NEW"})
        self.assertIn("OLD", resolved.active_by_date[date(2020, 1, 1)])
        self.assertNotIn("OLD", resolved.active_by_date[date(2020, 1, 3)])
        self.assertIn("NEW", resolved.active_by_date[date(2020, 1, 3)])

    def test_apply_is_idempotent_for_memberships(self):
        first = self._sync(apply=True)
        second = self._sync(apply=True)

        self.assertEqual(first.memberships_created, 3)
        self.assertEqual(second.memberships_created, 0)
        self.assertEqual(second.memberships_updated, 0)
        self.assertEqual(UniverseMembership.objects.count(), 3)
        self.assertEqual(UniverseImportBatch.objects.count(), 2)
        self.assertEqual(UniverseCoverageSnapshot.objects.count(), 3)

    def test_mapping_incomplete_marks_partial_and_resolver_blocks(self):
        result = self._sync(records=[record("AAPL"), record("MISSING")], apply=True, expected=2)

        self.assertEqual(result.status, UniverseCoverageStatus.PARTIAL)
        self.assertEqual(result.unmapped_member_count, 1)
        self.assertIn("unmapped symbol MISSING", " ".join(result.warnings))
        self.assertEqual(
            set(UniverseCoverageSnapshot.objects.values_list("status", flat=True)),
            {UniverseCoverageStatus.PARTIAL},
        )
        with self.assertRaises(UniverseCoverageError):
            self._resolve()

    def test_missing_start_date_uses_coverage_start_with_warning(self):
        result = self._sync(records=[record("OLD", start=None, end="2020-01-02", active=0)], apply=True, expected=1, end=date(2020, 1, 2))

        self.assertEqual(result.status, UniverseCoverageStatus.VALIDATED)
        self.assertIn("missing StartDate", " ".join(result.warnings))
        membership = UniverseMembership.objects.get()
        self.assertEqual(membership.valid_from, date(2020, 1, 1))
        self.assertEqual(membership.valid_to, date(2020, 1, 2))
        self.assertTrue(membership.source_payload["assumed_valid_from_coverage_start"])

    def test_future_end_date_is_capped_to_coverage_end_with_warning(self):
        result = self._sync(records=[record("AAPL", start="2020-01-01", end="2020-01-10")], apply=True, expected=1)

        self.assertEqual(result.status, UniverseCoverageStatus.VALIDATED)
        self.assertIn("capped to 2020-01-03", " ".join(result.warnings))
        membership = UniverseMembership.objects.get()
        self.assertEqual(membership.valid_to, date(2020, 1, 3))

    def test_special_dash_ticker_can_map_to_local_dot_ticker(self):
        Symbol.objects.create(ticker="BRK.B", exchange="NYSE", active=True)

        result = self._sync(records=[record("BRK-B")], apply=True, expected=1)

        self.assertEqual(result.status, UniverseCoverageStatus.VALIDATED)
        membership = UniverseMembership.objects.get(ticker="BRK.B")
        self.assertEqual(membership.symbol.ticker, "BRK.B")
        self.assertEqual(membership.provider_symbol, "BRK-B.US")

    def test_ambiguous_local_ticker_marks_partial(self):
        Symbol.objects.create(ticker="DUP", exchange="NYSE", active=True)
        Symbol.objects.create(ticker="DUP", exchange="NASDAQ", active=True)

        result = self._sync(records=[record("DUP")], apply=True, expected=1)

        self.assertEqual(result.status, UniverseCoverageStatus.PARTIAL)
        self.assertIn("ambiguous symbol DUP", " ".join(result.warnings))

    def test_provider_error_is_raised_without_writes(self):
        with self.assertRaisesMessage(EODHDError, "no access"):
            sync_sp500_historical_memberships_from_eodhd(
                coverage_start=date(2020, 1, 1),
                coverage_end=date(2020, 1, 3),
                expected_member_count=1,
                dry_run=True,
                client=FakeEODHDClient(error=EODHDError("no access")),
            )

        self.assertEqual(UniverseMembership.objects.count(), 0)
        self.assertEqual(UniverseImportBatch.objects.count(), 0)

    @patch("core.services.provider_eodhd.requests.get", side_effect=AssertionError("no network in tests"))
    def test_command_dry_run_uses_mocked_client_without_network(self, _mock_get):
        out = StringIO()
        with patch("core.services.universe_eodhd_sync.EODHDClient", return_value=FakeEODHDClient(self._records())):
            call_command(
                "sync_sp500_historical_memberships",
                "--coverage-start",
                "2020-01-01",
                "--coverage-end",
                "2020-01-03",
                "--expected-member-count",
                "2",
                stdout=out,
            )

        self.assertIn("mode=dry-run", out.getvalue())
        self.assertIn("provider_records=3", out.getvalue())
        self.assertIn("status=VALIDATED", out.getvalue())
        self.assertEqual(UniverseMembership.objects.count(), 0)

    @patch("core.services.provider_eodhd.requests.get", side_effect=AssertionError("no network in tests"))
    def test_command_apply_persists_with_mocked_client_without_network(self, _mock_get):
        out = StringIO()
        with patch("core.services.universe_eodhd_sync.EODHDClient", return_value=FakeEODHDClient(self._records())):
            call_command(
                "sync_sp500_historical_memberships",
                "--coverage-start",
                "2020-01-01",
                "--coverage-end",
                "2020-01-03",
                "--expected-member-count",
                "2",
                "--apply",
                stdout=out,
            )

        self.assertIn("mode=apply", out.getvalue())
        self.assertIn("status=VALIDATED", out.getvalue())
        self.assertEqual(UniverseMembership.objects.count(), 3)
        self.assertEqual(UniverseImportBatch.objects.count(), 1)
