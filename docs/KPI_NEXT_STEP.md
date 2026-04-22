# KPI next step

Current status:
- jobs layer frozen
- tests green
- KPI consolidation phase 1 completed

Done:
- shared per-line KPI assembly centralized in engine
- tradable/ratio read-only projection centralized for views/debug

Next phase:
- KPI correctness validation
- start with golden tests only
- no code behavior change before business expectations are frozen

First business correction implemented:
- portfolio BT now uses total capital as denominator
- portfolio BMJ is derived from the corrected portfolio BT
- portfolio NB_DAYS remains the count of portfolio daily rows where invested > 0
- line BT/BMJ/BMD remain unchanged in this iteration

Business note:
- historical portfolio BT/BMJ values computed before this correction are not directly comparable
  to corrected values because the previous denominator was invested_end instead of total capital

First target formulas:
- line BT
- line BMJ
- line BMD
- RATIO_IN_POSITION
- RATIO_NOT_IN_POSITION
- portfolio BT
- portfolio BMJ
- portfolio NB_DAYS
