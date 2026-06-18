from __future__ import annotations

from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Count

from core.models import UniverseCoverageSnapshot, UniverseDefinition, UniverseMembership
from core.services.universe_resolver import SP500_UNIVERSE_CODE


SP500_DEFAULTS = {
    "name": "S&P 500",
    "description": "Historical S&P 500 universe definition. Memberships are imported separately.",
    "source": "reference_data",
    "active": True,
    "metadata": {"provider": "eodhd", "scope": "dynamic_universe_v1"},
}


class Command(BaseCommand):
    help = "Initialize minimal idempotent reference data for a fresh StockAlert database."

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show the reference data changes that would be applied without writing to the database.",
        )

    def handle(self, *args, **options):
        dry_run = bool(options.get("dry_run"))

        with transaction.atomic():
            universe = UniverseDefinition.objects.filter(code=SP500_UNIVERSE_CODE).first()
            if universe is None:
                if dry_run:
                    self.stdout.write("SP500 UniverseDefinition would be created active=True")
                    universe_id = None
                else:
                    universe = UniverseDefinition.objects.create(
                        code=SP500_UNIVERSE_CODE,
                        **SP500_DEFAULTS,
                    )
                    universe_id = universe.id
                    self.stdout.write(self.style.SUCCESS("SP500 UniverseDefinition created active=True"))
            elif not universe.active:
                universe_id = universe.id
                if dry_run:
                    self.stdout.write("SP500 UniverseDefinition would be reactivated")
                else:
                    universe.active = True
                    universe.save(update_fields=["active", "updated_at"])
                    self.stdout.write(self.style.SUCCESS("SP500 UniverseDefinition reactivated"))
            else:
                universe_id = universe.id
                self.stdout.write("SP500 UniverseDefinition already exists active=True")

            if dry_run:
                transaction.set_rollback(True)

        persisted_universe = UniverseDefinition.objects.filter(code=SP500_UNIVERSE_CODE).first()
        membership_count = (
            UniverseMembership.objects.filter(universe=persisted_universe).count()
            if persisted_universe
            else 0
        )
        coverage_counts = {}
        if persisted_universe:
            coverage_counts = {
                row["status"]: row["count"]
                for row in UniverseCoverageSnapshot.objects.filter(universe=persisted_universe)
                .values("status")
                .annotate(count=Count("id"))
                .order_by("status")
            }

        self.stdout.write(
            "summary "
            f"mode={'dry-run' if dry_run else 'apply'} "
            f"universe={SP500_UNIVERSE_CODE} "
            f"universe_id={universe_id or ''} "
            f"memberships={membership_count} "
            f"coverage={coverage_counts or '{}'}"
        )
        self.stdout.write(
            "note init_reference_data creates only minimal reference definitions; "
            "it does not import S&P500 memberships, validate coverage, fetch OHLC, or call providers."
        )
