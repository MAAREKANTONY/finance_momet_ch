# CONTEXT - GM SIMPLIFICATION AUDIT

## Project

StockAlert

Django quantitative trading/backtesting platform.

Recent completed features:

1. Independent BUY/SELL slope thresholds
2. Protection anti-chute (Recent High Drawdown)
3. Market conditions per signal line

All features are merged.

---

## GM Historical Model

Historically the platform provided:

* GM_CURRENT
* GM_market
* GM_sector

These were implemented as global trend filters.

Their purpose was:

* authorize BUY entries
* block BUY entries

They never generated SELL signals.

They were not ticker signals.

---

## User Request

Users wanted to express rules such as:

* SPa AND GM_market positive
* SPVa AND GM_sector positive
* Af AND GM_CURRENT positive

without transforming GM into ticker signals.

---

## Product Decision

GM is NOT a ticker signal.

GM must never be added to:

* SPa
* SPVa
* Af
* Bf
* RHD_OK
* other signal choices

GM remains a market condition.

GM must never generate SELL.

---

## Implemented Feature

Market conditions were added directly to signal lines.

Each line now supports:

* GM actuel
* GM marché
* GM secteur

Values:

* Ignorer
* Positif
* Négatif

Operator:

* ET
* OU

Validation rule:

A line containing market conditions must contain at least one BUY ticker signal.

Market conditions alone cannot become a hidden BUY trigger.

---

## Current Situation

The UI now exposes two GM systems:

### A

Line-level market conditions

Used inside signal lines.

### B

Global trend filters

Legacy section still present in scenarios and games.

---

## Product Concern

The UI appears duplicated.

Users now see:

1. Conditions de marché
2. Filtres de tendance

Both use GM concepts.

This creates confusion.

---

## Audit Goal

Determine whether global trend filters are still necessary.

Do not assume they must be preserved.

Prove their usefulness.

Evaluate:

* functional overlap
* remaining use cases
* existing dependency in scenarios/games
* migration complexity
* deprecation feasibility
* removal feasibility

---

## Important Philosophy

The project intentionally avoids:

* dead code
* duplicate business models
* long-term legacy accumulation

If global GM filters no longer provide meaningful value compared to line-level market conditions, removal should be considered.

---

## Out of Scope

Do not modify:

* Study cleanup
* migration drift cleanup
* Protection anti-chute
* SELL threshold feature
* GM calculations
* benchmark logic

This audit only concerns the coexistence of:

* global GM filters
* line-level GM conditions
