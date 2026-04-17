from django.core.management.base import BaseCommand
from core.tasks import send_daily_alerts_task
class Command(BaseCommand):
    help = "Send today's alert email."
    def handle(self, *args, **options):
        self.stdout.write(str(send_daily_alerts_task()))
