from __future__ import annotations

from datetime import date

from django.core.management.base import BaseCommand

from core.models import Symbol
from core.services.market_cap_sync import sync_market_caps_for_symbols


class Command(BaseCommand):
    help = "Sync historical market capitalization from EODHD into the local cache."

    def add_arguments(self, parser):
        parser.add_argument("--symbols", default="", help="Comma-separated tickers. Omit to sync all active symbols.")
        parser.add_argument("--from", dest="from_date", required=True, help="Start date, YYYY-MM-DD.")
        parser.add_argument("--to", dest="to_date", required=True, help="End date, YYYY-MM-DD.")
        parser.add_argument("--provider", default="eodhd", help="Provider label for stored rows.")
        parser.add_argument("--exchange", default="", help="Optional exchange filter for ticker disambiguation.")
        parser.add_argument("--dry-run", action="store_true", help="Fetch and report without writing rows.")

    def handle(self, *args, **options):
        provider = str(options["provider"] or "eodhd")
        from_date = date.fromisoformat(options["from_date"])
        to_date = date.fromisoformat(options["to_date"])
        symbols_arg = str(options.get("symbols") or "").strip()
        exchange = str(options.get("exchange") or "").strip()
        dry_run = bool(options.get("dry_run"))

        qs = Symbol.objects.filter(active=True).order_by("ticker", "exchange")
        if symbols_arg:
            tickers = [s.strip().upper() for s in symbols_arg.split(",") if s.strip()]
            qs = qs.filter(ticker__in=tickers)
        if exchange:
            qs = qs.filter(exchange__iexact=exchange)

        totals = sync_market_caps_for_symbols(
            qs,
            from_date,
            to_date,
            provider=provider,
            dry_run=dry_run,
        )
        for detail in totals["per_symbol"]:
            symbol_label = detail["symbol"]
            if detail["skipped"]:
                self.stdout.write(self.style.WARNING(f"{symbol_label}: skipped {detail['error']}"))
            elif detail["error"]:
                self.stdout.write(self.style.ERROR(f"{symbol_label}: error {detail['error']}"))
            elif dry_run:
                self.stdout.write(f"{symbol_label}: fetched={detail['fetched']} dry_run=1")
            else:
                self.stdout.write(
                    f"{symbol_label}: fetched={detail['fetched']} "
                    f"inserted={detail['inserted']} updated={detail['updated']} existing={detail['existing']}"
                )

        self.stdout.write(
            "summary "
            f"fetched={totals['fetched']} "
            f"inserted={totals['inserted']} "
            f"updated={totals['updated']} "
            f"existing={totals['existing']} "
            f"skipped={totals['skipped']} "
            f"errors={totals['errors']}"
        )
