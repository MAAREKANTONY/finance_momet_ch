from __future__ import annotations

import ast
import tempfile
from pathlib import Path
from unittest import TestCase

from tools.architecture_audit import AuditVisitor, ROOT, _is_direct_celery_dispatch, _is_processingjob_create, _is_ws_append, run_audit


class ArchitectureAuditHelpersTests(TestCase):
    def test_helper_detection_functions(self):
        call_delay = ast.parse("task.delay(x=1)").body[0].value
        call_create = ast.parse("ProcessingJob.objects.create(status='PENDING')").body[0].value
        call_append = ast.parse("ws.append(['a'])").body[0].value
        self.assertTrue(_is_direct_celery_dispatch(call_delay))
        self.assertTrue(_is_processingjob_create(call_create))
        self.assertTrue(_is_ws_append(call_append))

    def test_visitor_flags_direct_delay_in_views(self):
        file_path = ROOT / "core" / "views.py"
        visitor = AuditVisitor(file_path)
        tree = ast.parse("task.delay()\nProcessingJob.objects.create()\nws.append([1])")
        visitor.visit(tree)
        rule_ids = [v.rule_id for v in visitor.violations]
        self.assertIn("views_direct_delay", rule_ids)
        self.assertIn("views_processingjob_create", rule_ids)
        self.assertIn("exports_direct_ws_append", rule_ids)

    def test_run_audit_reports_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "core").mkdir()
            (root / "core" / "views.py").write_text("task.delay()\n", encoding="utf-8")
            report = run_audit(root)
            self.assertEqual(report["violation_count"], 1)
            self.assertEqual(report["summary"]["views_direct_delay"], 1)


class ArchitectureAuditProjectBaselineTests(TestCase):
    def test_views_direct_delay_is_zero_in_current_project(self):
        report = run_audit(ROOT)
        self.assertEqual(report["summary"].get("views_direct_delay", 0), 0)


    def test_total_violations_is_zero_in_current_project(self):
        report = run_audit(ROOT)
        self.assertEqual(report["violation_count"], 0)
