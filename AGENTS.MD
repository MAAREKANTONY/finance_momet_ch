# AGENTS.md — StockAlert

## Safety
- No regression.
- Minimal diffs only.
- Do not touch Celery, Redis, UI, KPI unless explicitly requested.

## Process
- Read failing tests first.
- Explain root cause before patching.
- Patch only files in scope.
- Run validation after every change.

## Validation
- Main command:
  sudo docker compose exec web python tools/run_quality_gate.py

## Output format
Always return:
1. root cause
2. files changed
3. commands run
4. result
5. remaining failures

## KPI correction rule
- Any KPI formula change must include:
  - production code update
  - regression/golden test update
  - user-facing documentation update
  - label/help text clarification where needed
- No KPI business correction is considered complete without aligned UI/help wording.
