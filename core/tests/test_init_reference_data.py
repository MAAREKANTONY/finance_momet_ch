from __future__ import annotations

from io import StringIO

from django.core.management import call_command
from django.test import TestCase

from core.models import (
    DailyBar,
    ProcessingJob,
    UniverseCoverageSnapshot,
    UniverseDefinition,
    UniverseMembership,
)


class InitReferenceDataCommandTests(TestCase):
    def _call_command(self, *args) -> str:
        out = StringIO()
        call_command("init_reference_data", *args, stdout=out)
        return out.getvalue()

    def test_creates_minimal_sp500_definition_on_empty_database(self):
        output = self._call_command()

        universe = UniverseDefinition.objects.get(code="SP500")
        self.assertEqual(universe.name, "S&P 500")
        self.assertEqual(universe.source, "reference_data")
        self.assertTrue(universe.active)
        self.assertEqual(universe.metadata["provider"], "eodhd")
        self.assertIn("SP500 UniverseDefinition created active=True", output)
        self.assertIn("memberships=0", output)
        self.assertEqual(UniverseMembership.objects.count(), 0)
        self.assertEqual(UniverseCoverageSnapshot.objects.count(), 0)

    def test_is_idempotent(self):
        first_output = self._call_command()
        second_output = self._call_command()

        self.assertIn("created", first_output)
        self.assertIn("already exists active=True", second_output)
        self.assertEqual(UniverseDefinition.objects.filter(code="SP500").count(), 1)
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

    def test_dry_run_does_not_create_reference_data(self):
        output = self._call_command("--dry-run")

        self.assertIn("would be created active=True", output)
        self.assertIn("mode=dry-run", output)
        self.assertFalse(UniverseDefinition.objects.filter(code="SP500").exists())
        self.assertEqual(UniverseMembership.objects.count(), 0)
        self.assertEqual(UniverseCoverageSnapshot.objects.count(), 0)

    def test_does_not_create_heavy_data_or_jobs(self):
        self._call_command()

        self.assertEqual(UniverseMembership.objects.count(), 0)
        self.assertEqual(UniverseCoverageSnapshot.objects.count(), 0)
        self.assertEqual(DailyBar.objects.count(), 0)
        self.assertEqual(ProcessingJob.objects.count(), 0)
