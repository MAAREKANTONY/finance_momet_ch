from __future__ import annotations

from django.core.management.base import BaseCommand

from core.models import Symbol
from core.services.benchmark_etf_sync import format_benchmark_sync_summary, sync_benchmark_etfs_for_symbols


class Command(BaseCommand):
    help = "Ensure and optionally sync benchmark ETF symbols required by current trend-filter mappings."

    def add_arguments(self, parser):
        parser.add_argument(
            "--symbols",
            default="",
            help="Comma-separated source tickers used to resolve required benchmark ETFs. Omit to use all active symbols.",
        )
        parser.add_argument("--dry-run", action="store_true", help="Report required benchmark ETF work without creating symbols or fetching OHLC.")
        parser.add_argument("--skip-ohlc", action="store_true", help="Ensure/enrich benchmark ETF symbols without fetching DailyBar OHLC.")
        parser.add_argument("--skip-enrichment", action="store_true", help="Ensure benchmark ETF symbols without running metadata enrichment.")

    def handle(self, *args, **options):
        symbols_arg = str(options.get("symbols") or "").strip()
        dry_run = bool(options.get("dry_run"))
        skip_ohlc = bool(options.get("skip_ohlc"))
        skip_enrichment = bool(options.get("skip_enrichment"))

        qs = Symbol.objects.filter(active=True).order_by("ticker", "exchange")
        if symbols_arg:
            tickers = [item.strip().upper() for item in symbols_arg.split(",") if item.strip()]
            qs = qs.filter(ticker__in=tickers)

        totals = sync_benchmark_etfs_for_symbols(
            qs,
            dry_run=dry_run,
            skip_ohlc=skip_ohlc,
            skip_enrichment=skip_enrichment,
        )

        for detail in totals["per_symbol"]:
            self.stdout.write(f"{detail['ticker']}: {detail['status']}")

        enrichment = totals.get("enrichment") or {}
        ohlc = totals.get("ohlc") or {}
        summary = format_benchmark_sync_summary(totals)
        summary += f" enrichment_updated={enrichment.get('updated', 0)}"
        if not ohlc:
            summary += " ohlc_symbols=0 ohlc_bars=0"
        self.stdout.write(summary)
