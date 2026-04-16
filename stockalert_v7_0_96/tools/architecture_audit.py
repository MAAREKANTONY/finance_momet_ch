from __future__ import annotations

import argparse
import ast
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Violation:
    rule_id: str
    severity: str
    file: str
    line: int
    message: str


@dataclass(frozen=True)
class Rule:
    rule_id: str
    severity: str
    description: str


RULES: dict[str, Rule] = {
    "views_direct_delay": Rule(
        rule_id="views_direct_delay",
        severity="high",
        description="Direct Celery dispatch in views should be routed through a helper or explicitly reviewed.",
    ),
    "views_processingjob_create": Rule(
        rule_id="views_processingjob_create",
        severity="high",
        description="ProcessingJob creation in views bypasses centralized lifecycle control.",
    ),
    "exports_direct_ws_append": Rule(
        rule_id="exports_direct_ws_append",
        severity="medium",
        description="Excel worksheet appends should go through a serializer/helper to avoid list/dict/set regressions.",
    ),
}


class AuditVisitor(ast.NodeVisitor):
    def __init__(self, file_path: Path, project_root: Path = ROOT):
        self.file_path = file_path
        self.project_root = project_root
        self.violations: list[Violation] = []

    def visit_Call(self, node: ast.Call) -> None:
        if self.file_path.name == "views.py":
            if _is_direct_celery_dispatch(node):
                self.violations.append(
                    Violation(
                        rule_id="views_direct_delay",
                        severity=RULES["views_direct_delay"].severity,
                        file=str(self.file_path.relative_to(self.project_root)),
                        line=node.lineno,
                        message="Direct Celery dispatch found in core/views.py",
                    )
                )
            if _is_processingjob_create(node):
                self.violations.append(
                    Violation(
                        rule_id="views_processingjob_create",
                        severity=RULES["views_processingjob_create"].severity,
                        file=str(self.file_path.relative_to(self.project_root)),
                        line=node.lineno,
                        message="ProcessingJob.objects.create(...) found in core/views.py",
                    )
                )

        if self.file_path.name in {"views.py", "exports.py"} and _is_ws_append(node):
            self.violations.append(
                Violation(
                    rule_id="exports_direct_ws_append",
                    severity=RULES["exports_direct_ws_append"].severity,
                    file=str(self.file_path.relative_to(self.project_root)),
                    line=node.lineno,
                    message="Direct ws.append(...) found; consider append_excel_row()/serializer helper.",
                )
            )

        self.generic_visit(node)


def _is_direct_celery_dispatch(node: ast.Call) -> bool:
    return isinstance(node.func, ast.Attribute) and node.func.attr in {"delay", "apply_async"}


def _is_ws_append(node: ast.Call) -> bool:
    return (
        isinstance(node.func, ast.Attribute)
        and node.func.attr == "append"
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id in {"ws", "worksheet"}
    )


def _is_processingjob_create(node: ast.Call) -> bool:
    func = node.func
    return (
        isinstance(func, ast.Attribute)
        and func.attr == "create"
        and isinstance(func.value, ast.Attribute)
        and func.value.attr == "objects"
        and isinstance(func.value.value, ast.Name)
        and func.value.value.id == "ProcessingJob"
    )


def iter_python_files(root: Path) -> Iterable[Path]:
    for path in sorted((root / "core").rglob("*.py")):
        if "__pycache__" not in path.parts:
            yield path


def run_audit(root: Path) -> dict[str, object]:
    violations: list[Violation] = []
    for file_path in iter_python_files(root):
        try:
            tree = ast.parse(file_path.read_text(encoding="utf-8"), filename=str(file_path))
        except SyntaxError as exc:
            violations.append(
                Violation(
                    rule_id="syntax_error",
                    severity="critical",
                    file=str(file_path.relative_to(root)),
                    line=exc.lineno or 0,
                    message=str(exc),
                )
            )
            continue

        visitor = AuditVisitor(file_path, root)
        visitor.visit(tree)
        violations.extend(visitor.violations)

    summary: dict[str, int] = {}
    for violation in violations:
        summary[violation.rule_id] = summary.get(violation.rule_id, 0) + 1

    return {
        "root": str(root),
        "rules": {k: asdict(v) for k, v in RULES.items()},
        "violations": [asdict(v) for v in violations],
        "summary": summary,
        "violation_count": len(violations),
    }


def render_text(report: dict[str, object]) -> str:
    lines = ["StockAlert architecture audit", ""]
    violations = report["violations"]
    if not violations:
        lines.append("No violations found.")
        return "\n".join(lines)

    lines.append(f"Violations: {report['violation_count']}")
    lines.append("")
    for item in violations:
        lines.append(
            f"- [{item['severity']}] {item['rule_id']} {item['file']}:{item['line']} — {item['message']}"
        )
    lines.append("")
    lines.append("Summary:")
    for rule_id, count in sorted(report["summary"].items()):
        lines.append(f"  - {rule_id}: {count}")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run lightweight architecture checks on StockAlert.")
    parser.add_argument("--root", default=str(ROOT), help="Project root")
    parser.add_argument("--format", choices=["text", "json"], default="text")
    parser.add_argument("--output", help="Optional output file path")
    parser.add_argument("--fail-on-violations", action="store_true", help="Return non-zero when violations are found")
    args = parser.parse_args()

    report = run_audit(Path(args.root).resolve())
    rendered = render_text(report) if args.format == "text" else json.dumps(report, indent=2, ensure_ascii=False)

    if args.output:
        Path(args.output).write_text(rendered, encoding="utf-8")
    else:
        print(rendered)

    if args.fail_on_violations and int(report["violation_count"]):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
