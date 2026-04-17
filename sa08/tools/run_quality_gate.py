from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def run_step(name: str, cmd: list[str]) -> int:
    print(f"\n=== {name} ===")
    print("$", " ".join(cmd))
    completed = subprocess.run(cmd, cwd=ROOT)
    return completed.returncode


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the StockAlert quality gate.")
    parser.add_argument("--allow-architecture-violations", action="store_true", help="Do not fail when architecture audit finds violations.")
    parser.add_argument("--skip-tests", action="store_true")
    args = parser.parse_args()

    python = sys.executable
    steps: list[tuple[str, list[str]]] = [
        ("Compile Python files", [python, "-m", "compileall", "core", "stockalert", "tools"]),
        (
            "Architecture audit",
            [python, "tools/architecture_audit.py", "--format", "text"]
            + ([] if args.allow_architecture_violations else ["--fail-on-violations"]),
        ),
    ]
    if not args.skip_tests:
        steps.extend(
            [
                ("Architecture audit unit tests", [python, "-m", "unittest", "core.tests.test_architecture_audit"]),
                ("Job launch tests", [python, "manage.py", "test", "core.tests.test_job_launch", "--verbosity", "2"]),
                (
                    "Recovery robustness tests",
                    [python, "manage.py", "test", "core.tests.test_jobs_robustness", "--verbosity", "2"],
                ),
                (
                    "Export regression tests",
                    [python, "manage.py", "test", "core.tests.test_exports_regressions", "--verbosity", "2"],
                ),
            ]
        )

    for name, cmd in steps:
        code = run_step(name, cmd)
        if code != 0:
            return code
    print("\nQuality gate completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
