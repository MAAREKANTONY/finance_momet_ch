from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from django.db import transaction
from django.utils import timezone

from core.models import Backtest, DailyBar, GameScenario, Scenario, Symbol, ProcessingJob
from core.services.backtesting.engine import run_backtest_kpi_only
from core.services.metrics_depth import check_metrics_depth


def _compute_avg_slope_for_ticker(*, symbol_id: int, end_d: date, npente: int) -> str | None:
    """Return average daily slope over the last `npente` market days for one ticker.

    slope(t) = (P(t) - P(t-1)) / P(t-1)
    Stored as raw ratio (0.001 = 0.1%).
    """
    try:
        window = max(int(npente or 0), 1) + 1
    except Exception:
        window = 101
    bars = list(
        DailyBar.objects.filter(symbol_id=symbol_id, date__lte=end_d)
        .order_by("-date")
        .values_list("close", flat=True)[:window]
    )
    if len(bars) < 2:
        return None
    closes = list(reversed([Decimal(str(v)) for v in bars if v is not None]))
    slopes = []
    for prev_p, cur_p in zip(closes, closes[1:]):
        if prev_p == 0:
            continue
        slopes.append((cur_p - prev_p) / prev_p)
        if len(slopes) >= max(int(npente or 0), 1):
            break
    if not slopes:
        return None
    avg = sum(slopes, Decimal("0")) / Decimal(len(slopes))
    return str(avg)


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
        # Kf3 (floating line 3)
        "n5f3",
        "crf3",
        "nampL3",
        "baseL3",
        "periodeL3",
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


class GameJobCancelled(Exception):
    pass


class GameJobKilled(Exception):
    pass


def _job_checkpoint(job: ProcessingJob | None) -> None:
    """Cooperative cancellation + heartbeat for Game runs."""
    if job is None:
        return
    try:
        job.refresh_from_db(fields=["cancel_requested", "kill_requested", "status"])
        if job.kill_requested:
            raise GameJobKilled("kill requested")
        if job.cancel_requested:
            raise GameJobCancelled("cancel requested")
        ProcessingJob.objects.filter(id=job.id).update(heartbeat_at=timezone.now())
    except (GameJobCancelled, GameJobKilled):
        raise
    except Exception:
        # If the checkpoint failed for any other reason, do not block the run.
        return


def run_game_scenario_now(
    game_id: int,
    *,
    force_fetch: bool = False,
    force_recompute: bool = False,
    skip_metrics: bool = False,
    job: ProcessingJob | None = None,
) -> dict:
    """Run a game scenario end-to-end.

    Steps:
    - Ensure engine scenario exists (for metrics/alerts persistence)
    - Ensure latest bars exist (fetch if needed)
    - Compute metrics+alerts (incremental unless forced)
    - Run KPI-only backtest and store today's snapshot
    """
    game = GameScenario.objects.get(id=game_id)

    _job_checkpoint(job)

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
    symbol_map = dict(symbols.values_list("ticker", "id"))

    _job_checkpoint(job)

    # 1) Fetch bars (if necessary)
    from core.tasks import _fetch_daily_bars_for_symbols, _compute_metrics_for_scenario

    # Outputsize heuristic: study window + buffer
    outputsize = min(5000, int(game.study_days or 1000) + 400)
    _fetch_daily_bars_for_symbols(symbol_qs=symbols, outputsize=outputsize, force_full=bool(force_fetch), job=job)

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

    _job_checkpoint(job)

    # 4) Compute metrics/alerts
    # Optimization: allow callers (e.g. daily refresh job) to refresh metrics separately
    # and skip this step when depth is sufficient.
    if (not skip_metrics) or bool(do_full):
        _compute_metrics_for_scenario(symbols_qs=symbols, scenario=scenario, recompute_all=bool(do_full), job=job)

    # 5) Build an in-memory Backtest config for KPI-only computation
    tickers = list(symbols.values_list("ticker", flat=True))
    bt = Backtest(
        scenario=scenario,
        start_date=start_d,
        end_date=end_d,
        capital_total=game.capital_total,
        capital_per_ticker=game.capital_per_ticker,
        capital_mode=getattr(game, "capital_mode", "REINVEST"),
        ratio_threshold=0,
        include_all_tickers=True,
        signal_lines=game.signal_lines or [{"buy": "A1", "sell": "B1"}],
        close_positions_at_end=game.close_positions_at_end,
        universe_snapshot=tickers,
        settings=game.settings or {},
    )

    out = run_backtest_kpi_only(bt, max_days=int(game.study_days or 1000))

    _job_checkpoint(job)

    # 6) Build today's snapshot
    rows = []
    # NOTE: BMD returned by the engine is a *ratio* (0.01 == 1%).
    # UX choice: in the Game UI, the BMD threshold is entered as a *percent* value
    # (e.g. 0.3 means 0.3%). We therefore convert it to a ratio for the comparison.
    thr_pct = game.tradability_threshold
    try:
        thr_ratio = Decimal(str(thr_pct)) / Decimal("100")
    except Exception:
        thr_ratio = None
    try:
        slope_threshold = Decimal(str(game.slope_threshold))
    except Exception:
        slope_threshold = None
    try:
        presence_threshold_pct = Decimal(str(game.presence_threshold_pct))
    except Exception:
        presence_threshold_pct = None
    npente = int(game.npente or 100)

    for ticker, tentry in out.items():
        best = tentry.get("best_bmd")  # ratio (string/Decimal/None)

        # Extract the KPI counters from the line that produced the best BMD.
        best_final: dict = {}
        try:
            best_dec = None if best is None else Decimal(str(best))
        except Exception:
            best_dec = None
        if best_dec is not None:
            try:
                for line in (tentry.get("lines") or []):
                    fin = (line or {}).get("final") or {}
                    bmd_val = fin.get("BMD")
                    if bmd_val is None:
                        continue
                    try:
                        if Decimal(str(bmd_val)) == best_dec:
                            best_final = fin
                            break
                    except Exception:
                        continue
            except Exception:
                best_final = {}

        td = int(best_final.get("TRADABLE_DAYS") or 0) if isinstance(best_final, dict) else 0
        ip = int(best_final.get("TRADABLE_DAYS_IN_POSITION_CLOSED") or 0) if isinstance(best_final, dict) else 0
        ratio_ip = None
        ratio_ip_dec = None
        if td > 0:
            try:
                ratio_ip_dec = (Decimal(ip) / Decimal(td)) * Decimal("100")
                ratio_ip = str(ratio_ip_dec)
            except Exception:
                ratio_ip = None
                ratio_ip_dec = None

        symbol_id = symbol_map.get(ticker)
        avg_slope = _compute_avg_slope_for_ticker(symbol_id=symbol_id, end_d=end_d, npente=npente) if symbol_id else None
        try:
            avg_slope_dec = None if avg_slope is None else Decimal(str(avg_slope))
        except Exception:
            avg_slope_dec = None

        ok = False
        if best is not None and thr_ratio is not None and slope_threshold is not None and presence_threshold_pct is not None:
            try:
                bmd_ratio = Decimal(str(best))
                ok = (
                    (bmd_ratio >= thr_ratio)
                    and (avg_slope_dec is not None and avg_slope_dec >= slope_threshold)
                    and (ratio_ip_dec is not None and ratio_ip_dec >= presence_threshold_pct)
                )
            except Exception:
                ok = False

        rows.append(
            {
                "ticker": ticker,
                "bmd": best,
                "avg_slope": avg_slope,
                "ok": bool(ok),
                "TRADABLE_DAYS": td,
                "TRADABLE_DAYS_IN_POSITION_CLOSED": ip,
                "RATIO_IN_POSITION": ratio_ip,
            }
        )

    snapshot = {
        "date": str(end_d),
        "rows": rows,
        "threshold_pct": str(thr_pct),
        "npente": npente,
        "slope_threshold": str(game.slope_threshold),
        "presence_threshold_pct": str(game.presence_threshold_pct),
    }

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