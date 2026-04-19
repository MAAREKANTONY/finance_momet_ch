# Project rules — StockAlert

## Safety
- No regression.
- No broad refactor without explicit request.
- Do not touch Celery config, Redis config, UI, KPI unless explicitly requested.
- Prefer minimal fixes.

## Validation
- Run:
  sudo docker compose exec web python tools/run_quality_gate.py
- Never claim success without test output.

## Scope discipline
- Read the failing test first.
- Explain root cause before patching.
- Modify only the files in scope.

## Deliverables
- Provide:
  - changed files
  - summary of root cause
  - commands run
  - exact test output
