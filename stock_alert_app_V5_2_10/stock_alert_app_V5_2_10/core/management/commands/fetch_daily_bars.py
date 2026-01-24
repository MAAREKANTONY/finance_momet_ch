from django.core.management.base import BaseCommand
from core.tasks import fetch_daily_bars_task
class Command(BaseCommand):
    help = "Fetch latest daily OHLC bars for active symbols."
    def handle(self, *args, **options):
        self.stdout.write(str(fetch_daily_bars_task()))
