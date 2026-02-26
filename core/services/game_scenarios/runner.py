from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from django.db import transaction
from django.utils import timezone

from core.models import Backtest, DailyBar, GameScenario, Scenario, Symbol
from core.services.backtesting.engine import run_backtest_kpi_only
from core.services.metrics_depth import check_metrics_depth


def _sync_engine_scenario(game: GameScenario) -> Scenario:
    """Create or update the internal Scenario used to persist metrics/alerts."""
    sc = game.engine_scenario
    if sc is None:
        sc = Scenario(
            name=f"[GAME] {game.name}",
            description=f"Auto-generated scenario for GameScenario #{game.id}",
            active=False,
            is_default=False,
        )

    # Copy scenario parameters (NO silent math change)
    for f in [
        "a",
        "b",
        "c",
        "d",
        "e",
        "vc",
        "fl",
        "n1",
        "n2",
        "n3",
        "n4",
        "n5",
        "k2j",
        "cr",
        "m_v",
    ]:
        setattr(sc, f, getattr(game, f))

    # Keep name updated for readability
    sc.name = f"[GAME] {game.name}"
    sc.description = f"Auto-generated scenario for GameScenario #{game.id}"
    sc.active = False
    sc.is_default = False
    sc.save()

    if game.engine_scenario_id != sc.id:
        game.engine_scenario = sc
        game.save(update_fields=["engine_scenario", "updated_at"])
    return sc


def run_game_scenario_now(game_id: int, *, force_fetch: bool = False, force_recompute: bool = False) -> dict:
    """Run a game scenario end-to-end.

    Steps:
    - Ensure engine scenario exists (for metrics/alerts persistence)
    - Ensure latest bars exist (fetch if needed)
    - Compute metrics+alerts (incremental unless forced)
    - Run KPI-only backtest and store today's snapshot
    """
    game = GameScenario.objects.get(id=game_id)

    if not game.active:
        game.last_run_at = timezone.now()
        game.last_run_status = "skipped"
        game.last_run_message = "GameScenario inactive"
        game.save(update_fields=["last_run_at", "last_run_status", "last_run_message"])
        return {"status": "skipped"}

    game.last_run_at = timezone.now()
    game.last_run_status = "running"
    game.last_run_message = ""
    game.save(update_fields=["last_run_at", "last_run_status", "last_run_message"])

    scenario = _sync_engine_scenario(game)

    symbols = Symbol.objects.filter(active=True).order_by("ticker")

    # 1) Fetch bars (if necessary)
    from core.tasks import _fetch_daily_bars_for_symbols, _compute_metrics_for_scenario

    # Outputsize heuristic: study window + buffer
    outputsize = min(5000, int(game.study_days or 1000) + 400)
    _fetch_daily_bars_for_symbols(symbol_qs=symbols, outputsize=outputsize, force_full=bool(force_fetch), job=None)

    # 2) Determine date window
    end_d = DailyBar.objects.order_by("-date").values_list("date", flat=True).first() or date.today()
    # generous calendar buffer to collect enough market days
    start_d = end_d - timedelta(days=int(game.study_days or 1000) * 3 + 45)

    # 3) Metrics depth check => auto full recompute when study_days increased
    symbol_ids = list(symbols.values_list("id", flat=True))
    depth = check_metrics_depth(
        scenario_id=scenario.id,
        symbol_ids=symbol_ids,
        required_start=start_d,
        required_end=end_d,
    )
    auto_force = depth.needs_full_recompute()
    do_full = bool(force_recompute) or bool(auto_force)

    # 4) Compute metrics/alerts
    _compute_metrics_for_scenario(symbols_qs=symbols, scenario=scenario, recompute_all=bool(do_full), job=None)

    # 5) Build an in-memory Backtest config for KPI-only computation
    tickers = list(symbols.values_list("ticker", flat=True))
    bt = Backtest(
        scenario=scenario,
        start_date=start_d,
        end_date=end_d,
        capital_total=game.capital_total,
        capital_per_ticker=game.capital_per_ticker,
        ratio_threshold=0,
        include_all_tickers=True,
        signal_lines=game.signal_lines or [{"buy": "A1", "sell": "B1"}],
        close_positions_at_end=game.close_positions_at_end,
        universe_snapshot=tickers,
        settings=game.settings or {},
    )

    out = run_backtest_kpi_only(bt, max_days=int(game.study_days or 1000))

    # 6) Build today's snapshot
    rows = []
    # NOTE: BMD returned by the engine is a *ratio* (0.01 == 1%).
    # UX choice: in the Game UI, the user enters the threshold as a *percent* value
    # (e.g. 0.3 means 0.3%). We therefore convert it to a ratio for the comparison.
    thr_pct = game.tradability_threshold  # percent value (0.3 == 0.3%)
    thr_ratio = None
    try:
        thr_ratio = Decimal(str(thr_pct)) / Decimal("100")
    except Exception:
        thr_ratio = None
    for ticker, tentry in out.items():
        best = tentry.get("best_bmd")  # ratio (string/Decimal/None)
        ok = False
        if best is not None and thr_ratio is not None:
            try:
                bmd_ratio = Decimal(str(best))
                ok = (bmd_ratio >= thr_ratio)
            except Exception:
                ok = False
        # Keep raw ratio in snapshot; display layer formats as %.
        rows.append({"ticker": ticker, "bmd": best, "ok": bool(ok)})

    snapshot = {"date": str(end_d), "rows": rows, "threshold_pct": str(thr_pct)}

    with transaction.atomic():
        game = GameScenario.objects.select_for_update().get(id=game_id)
        game.today_results = snapshot
        game.last_run_at = timezone.now()
        game.last_run_status = "ok"
        msg = f"Computed {len(rows)} tickers for {end_d}"
        if do_full:
            if force_recompute:
                msg += " (forced full recompute)"
            elif auto_force:
                msg += f" (auto full recompute: depth insufficient on {len(depth.missing_symbol_ids)}/{depth.total_symbols})"
        game.last_run_message = msg
        game.save(update_fields=["today_results", "last_run_at", "last_run_status", "last_run_message"])

    return {"status": "ok", "date": str(end_d), "count": len(rows)}