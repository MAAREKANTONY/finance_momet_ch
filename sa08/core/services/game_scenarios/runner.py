from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from django.db import transaction
from django.utils import timezone

from core.job_tracking import JobCancelled, JobCheckpointPulse, JobKilled
from core.models import Backtest, DailyBar, DailyMetric, GameScenario, Scenario, Symbol, ProcessingJob
from core.services.backtesting.engine import run_backtest_kpi_only
from core.services.metrics_depth import check_metrics_depth


def _compute_avg_slope_for_ticker(*, scenario_id: int, symbol_id: int, end_d: date) -> str | None:
    dm = (
        DailyMetric.objects.filter(
            scenario_id=scenario_id,
            symbol_id=symbol_id,
            date__lte=end_d,
            sum_slope__isnull=False,
        )
        .order_by("-date")
        .only("sum_slope")
        .first()
    )
    if dm is None or dm.sum_slope is None:
        return None
    return str(dm.sum_slope)


def _sync_engine_scenario(game: GameScenario) -> Scenario:
    sc = game.engine_scenario
    if sc is None:
        sc = Scenario(
            name=f"[GAME] {game.name}",
            description=f"Auto-generated scenario for GameScenario #{game.id}",
            active=False,
            is_default=False,
        )

    for f in [
        "a", "b", "c", "d", "e",
        "n1", "n2",
        "npente", "slope_threshold", "npente_basse", "slope_threshold_basse", "nglobal",
    ]:
        setattr(sc, f, getattr(game, f))

    sc.name = f"[GAME] {game.name}"
    sc.description = f"Auto-generated scenario for GameScenario #{game.id}"
    sc.active = False
    sc.is_default = False
    sc.save()

    if game.engine_scenario_id != sc.id:
        game.engine_scenario = sc
        game.save(update_fields=["engine_scenario", "updated_at"])
    return sc


GameJobCancelled = JobCancelled
GameJobKilled = JobKilled


def run_game_scenario_now(
    game_id: int,
    *,
    force_fetch: bool = False,
    force_recompute: bool = False,
    skip_metrics: bool = False,
    job: ProcessingJob | None = None,
    task_request=None,
) -> dict:
    game = GameScenario.objects.get(id=game_id)
    phase_pulse = JobCheckpointPulse(job, every_n=1, every_seconds=15, task_request=task_request, base_label=f"run_game:{game_id}")
    phase_pulse.hit(checkpoint="start", force=True)

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
    phase_pulse.hit(checkpoint="scenario_synced", force=True)

    symbols = Symbol.objects.filter(active=True).order_by("ticker")
    symbol_map = dict(symbols.values_list("ticker", "id"))

    from core.tasks import _fetch_daily_bars_for_symbols, _compute_metrics_for_scenario

    warmup_days = int(getattr(game, "warmup_days", 0) or 0)
    outputsize = min(5000, int(game.study_days or 1000) + warmup_days + 400)
    _fetch_daily_bars_for_symbols(
        symbol_qs=symbols,
        outputsize=outputsize,
        force_full=bool(force_fetch),
        job=job,
        task_request=task_request,
    )
    phase_pulse.hit(checkpoint="bars_ready", force=True)

    end_d = DailyBar.objects.order_by("-date").values_list("date", flat=True).first() or date.today()
    start_d = end_d - timedelta(days=int(game.study_days or 1000) * 3 + 45)
    required_start_d = start_d - timedelta(days=warmup_days) if warmup_days > 0 else start_d

    symbol_ids = list(symbols.values_list("id", flat=True))
    depth = check_metrics_depth(
        scenario_id=scenario.id,
        symbol_ids=symbol_ids,
        required_start=required_start_d,
        required_end=end_d,
    )
    auto_force = depth.needs_full_recompute()
    do_full = bool(force_recompute) or bool(auto_force)
    phase_pulse.hit(checkpoint="depth_checked", force=True)

    if (not skip_metrics) or bool(do_full):
        _compute_metrics_for_scenario(
            symbols_qs=symbols,
            scenario=scenario,
            recompute_all=bool(do_full),
            job=job,
            task_request=task_request,
        )
    phase_pulse.hit(checkpoint="metrics_ready", force=True)

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
        signal_lines=game.signal_lines or [{"buy": "A1", "sell": "B1", "buy_logic": "AND", "sell_logic": "OR"}],
        warmup_days=getattr(game, "warmup_days", 0),
        close_positions_at_end=game.close_positions_at_end,
        universe_snapshot=tickers,
        settings=game.settings or {},
    )

    out = run_backtest_kpi_only(bt, max_days=int(game.study_days or 1000))
    phase_pulse.hit(checkpoint="kpi_computed", force=True)

    rows = []
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

    row_pulse = JobCheckpointPulse(job, every_n=100, every_seconds=10, task_request=task_request, base_label=f"run_game:{game_id}:today_results")
    total_out = len(out)
    for idx, (ticker, tentry) in enumerate(out.items(), start=1):
        row_pulse.hit(checkpoint=f"ticker {idx}/{total_out} {ticker}")
        best = tentry.get("best_bmd")

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
        avg_slope = _compute_avg_slope_for_ticker(scenario_id=scenario.id, symbol_id=symbol_id, end_d=end_d) if symbol_id else None
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

    phase_pulse.hit(checkpoint="done", force=True)
    return {"status": "ok", "date": str(end_d), "count": len(rows)}
