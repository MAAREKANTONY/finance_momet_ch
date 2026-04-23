# CODEX CONTEXT — StockAlert buy/sell redesign

## Project state
- Celery stable
- Redis stable
- Jobs layer frozen
- No business parallelism
- All tests currently passing
- Quality gate OK
- Golden tests already exist
- KPI formulas have already been centralized in a previous phase
- Portfolio BT/BMJ calculations corrected
- UI/help documentation aligned

## Current problem
The current trading model for backtests and games is based on:
- buy signals combined with AND/OR
- sell signals combined with AND/OR
- day-by-day stateless trigger logic

This is now considered:
- too complex
- hard to explain
- not robust enough
- not realistic enough from a trading/business standpoint

## Target model
We want a stateful buy/sell model based on signal pairs.

Each signal is defined as:
- activation event Si+
- opposite/invalidation event Si-

Rules:
- signals may activate on different dates
- once activated, a signal remains latched in memory
- disappearance of Si+ does NOT cancel it
- only appearance of Si- cancels it
- buy occurs when all required signals are latched at the same time
- sell occurs when any signal is invalidated while in position
- after sell, full reset
- before buy, invalidation is selective: only the impacted signal becomes unlatched
- no same-day re-entry after sell

## Warmup
There is a configurable warmup period X days:
- all signal logic and latches are computed during warmup
- no trade is allowed during warmup
- the first tradable day inherits warmup-built state

## Scope
The new logic must apply identically to:
- backtests
- games

## Constraints
- No regression
- Minimal diffs only
- Do not touch Celery, Redis, jobs layer, UI, KPI unless explicitly required
- No massive refactor
- Tests mandatory before broad implementation
- Same state machine logic must be shared by backtests and games
- Existing metrics/KPI semantics must not be broken accidentally

## Process
- Analyze first
- Explain current behavior and root cause / gap first
- Then propose implementation steps
- Then write tests first
- Then implement incrementally
- Run validation after each step

## Validation command
sudo docker compose exec web python tools/run_quality_gate.py

## Expected output format
Always return:
1. root cause
2. files changed
3. commands run
4. result
5. remaining failures
