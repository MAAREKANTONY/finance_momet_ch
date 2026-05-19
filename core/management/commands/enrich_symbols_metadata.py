from __future__ import annotations

from django.core.management.base import BaseCommand

from core.models import Symbol
from core.services.symbol_enrichment import enrich_symbols_metadata


class Command(BaseCommand):
    help = "Enrich local Symbol metadata from Twelve Data reference/profile endpoints."

    def add_arguments(self, parser):
        parser.add_argument(
            "--symbols",
            default="",
            help="Comma-separated tickers to enrich. Omit to enrich all active symbols.",
        )
        parser.add_argument(
            "--exchange",
            default="",
            help="Optional exchange filter when selecting explicit tickers.",
        )
        parser.add_argument(
            "--overwrite-populated",
            action="store_true",
            help="Allow non-empty local fields to be refreshed when provider data differs. Exchange changes stay conservative.",
        )
        parser.add_argument(
            "--include-inactive",
            action="store_true",
            help="Include inactive symbols when no explicit ticker filter is provided.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Report intended updates without persisting Symbol changes.",
        )

    def handle(self, *args, **options):
        symbols_arg = str(options.get("symbols") or "").strip()
        exchange = str(options.get("exchange") or "").strip()
        dry_run = bool(options.get("dry_run"))
        only_missing = not bool(options.get("overwrite_populated"))
        include_inactive = bool(options.get("include_inactive"))

        qs = Symbol.objects.all().order_by("ticker", "exchange")
        if not include_inactive:
            qs = qs.filter(active=True)
        if symbols_arg:
            tickers = [item.strip().upper() for item in symbols_arg.split(",") if item.strip()]
            qs = qs.filter(ticker__in=tickers)
        if exchange:
            qs = qs.filter(exchange__iexact=exchange)

        totals = enrich_symbols_metadata(
            qs,
            only_missing=only_missing,
            dry_run=dry_run,
            provider="twelvedata",
        )

        for detail in totals["per_symbol"]:
            symbol_label = detail["symbol"]
            if detail["error"]:
                level = self.style.WARNING if detail.get("skipped") else self.style.ERROR
                self.stdout.write(level(f"{symbol_label}: {detail['error']}"))
            elif detail["updated_fields"]:
                updated_fields = ",".join(detail["updated_fields"])
                prefix = "dry_run update" if dry_run else "updated"
                self.stdout.write(f"{symbol_label}: {prefix} fields={updated_fields}")
            else:
                self.stdout.write(f"{symbol_label}: unchanged")

        self.stdout.write(
            "summary "
            f"processed={totals['processed']} "
            f"updated={totals['updated']} "
            f"unchanged={totals['unchanged']} "
            f"skipped={totals['skipped']} "
            f"errors={totals['errors']} "
            f"dry_run={int(dry_run)}"
        )
