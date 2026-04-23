MISSION

We are redesigning the buy/sell trigger model for backtests and games.

IMPORTANT
Do not start coding immediately.
First analyze the current implementation and map it against the target specification below.

CONSTRAINTS
- No regression
- Minimal diffs only
- Do not touch Celery, Redis, jobs layer, UI, KPI unless explicitly required
- Same trading logic for backtests and games
- No massive refactor
- Tests first
- Explain root cause / architecture gap before patching
- Validate after each step

TARGET MODEL

We no longer want:
- buy signals combined with daily AND/OR
- sell signals combined with daily AND/OR
- stateless day-by-day logic

We want a stateful signal-latch model with memory.

Signals are defined in pairs:
- S1+ / S1-
- S2+ / S2-
- ...
where:
- Si+ activates / latches signal i
- Si- invalidates / unlatches signal i

SIGNAL RULES
- A signal remains latched after Si+ appears
- It is NOT invalidated by disappearance of Si+
- It is invalidated ONLY by appearance of Si-
- Multiple signals can activate on different dates
- Buy occurs when ALL signals are latched at the same time
- Sell occurs when ANY latched signal is invalidated while in position
- After sell, full reset of the setup state
- Before buy, invalidation is selective: only that signal becomes unlatched
- No same-day re-entry after sell

WARMUP
- There is a configurable warmup period of X days
- During warmup, signals and latch states are computed normally
- No trades are allowed during warmup
- The first tradable day inherits the full signal state from warmup
- Therefore a buy can happen on the first tradable day if all signals are already latched

SPEC DECISIONS TO APPLY
1. Per-signal latch memory
2. No global reset before buy
3. Selective unlatch on opposite signal before buy
4. Buy when all signals are simultaneously latched
5. Immediate sell on first invalidation in position
6. Full reset after sell
7. Same state machine for backtests and games
8. Same-day activation and invalidation of same signal must be handled deterministically; prefer conservative behavior

WHAT I WANT FROM YOU NOW

PHASE 1 — ANALYSIS ONLY
1. Inspect the current code paths for:
   - signal computation
   - buy trigger logic
   - sell trigger logic
   - warmup handling
   - backtest engine
   - game engine
2. Identify exactly where current logic is:
   - stateless
   - AND/OR based
   - duplicated
   - tied to sell signals
3. Produce a migration map:
   - reusable logic to keep
   - files/helpers to introduce or adapt
   - safest implementation order
4. Propose the minimal-diff implementation plan

DO NOT CODE YET.

OUTPUT FORMAT
1. current behavior summary
2. gap versus target spec
3. proposed design in code structure terms
4. files likely impacted
5. proposed step-by-step implementation plan
6. risks / regression points
7. tests to write first

VALIDATION COMMAND
sudo docker compose exec web python tools/run_quality_gate.py
