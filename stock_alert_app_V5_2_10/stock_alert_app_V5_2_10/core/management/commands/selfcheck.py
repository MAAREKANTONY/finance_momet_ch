from django.core.management.base import BaseCommand
import compileall
import sys
from pathlib import Path

class Command(BaseCommand):
    help = "Basic sanity checks: compile all python files."

    def handle(self, *args, **options):
        base = Path(__file__).resolve().parents[3]
        ok = compileall.compile_dir(str(base), quiet=1)
        if not ok:
            self.stderr.write(self.style.ERROR("Compileall found errors."))
            sys.exit(1)
        self.stdout.write(self.style.SUCCESS("Selfcheck OK (python files compile)."))
