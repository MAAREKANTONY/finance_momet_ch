from django.core.management.base import BaseCommand
from core.tasks import compute_metrics_task


class Command(BaseCommand):
    help = "Compute metrics + alerts."

    def add_arguments(self, parser):
        parser.add_argument(
            "--recompute-all",
            action="store_true",
            help="Force full recompute for all scenarios (ignore incremental mode).",
        )

    def handle(self, *args, **options):
        recompute_all = bool(options.get("recompute_all"))
        self.stdout.write(str(compute_metrics_task(recompute_all=recompute_all)))
