from celery import shared_task
from decimal import Decimal
from datetime import datetime, date
from django.conf import settings
from django.core.mail import EmailMultiAlternatives

import hashlib
from datetime import timedelta
from django.db.models import Max

from .models import Symbol, Scenario, DailyBar, DailyMetric, Alert, EmailRecipient, EmailSettings
from .models import Backtest
from .models import ProcessingJob
from .services.provider_twelvedata import TwelveDataClient
from .services.calculations import compute_for_symbol_scenario

from django.utils import timezone

def parse_date(s: str) -> date:
    return datetime.fromisoformat(s.replace("Z","")).date()

def desired_outputsize_years(years: int) -> int:
    # Roughly 252 trading days / year. Add buffer.
    if years <= 0:
        return 260
    return min(5000, years * 260)


def _fetch_daily_bars_for_symbols(*, symbol_qs, outputsize: int) -> dict:
    """Fetch/update daily bars for a queryset of Symbol.

    Returns basic stats: {"symbols":..., "bars":...}.
    """
    client = TwelveDataClient()
    symbols = list(symbol_qs)
    bars_written = 0

    for sym in symbols:
        exchange = sym.exchange or getattr(settings, "DEFAULT_EXCHANGE", "")
        try:
            values = client.time_series_daily(sym.ticker, exchange=exchange, outputsize=outputsize)
        except Exception as e:
            print(f"[fetch] error {sym}: {e}")
            continue

        if not values:
            continue

        values_sorted = sorted(values, key=lambda v: v.get("datetime"))
        for v in values_sorted:
            try:
                d = parse_date(v["datetime"])
                o = Decimal(v["open"]); h = Decimal(v["high"]); l = Decimal(v["low"]); c = Decimal(v["close"])
            except Exception:
                continue

            DailyBar.objects.update_or_create(
                symbol=sym,
                date=d,
                defaults={"open": o, "high": h, "low": l, "close": c, "source": "twelvedata"},
            )
            bars_written += 1

        last_bar = DailyBar.objects.filter(symbol=sym).order_by("-date").first()
        prev_bar = DailyBar.objects.filter(symbol=sym, date__lt=last_bar.date).order_by("-date").first() if last_bar else None
        if last_bar and prev_bar and prev_bar.close:
            change_amount = last_bar.close - prev_bar.close
            change_pct = (change_amount / prev_bar.close) * Decimal("100") if prev_bar.close != 0 else None
            DailyBar.objects.filter(id=last_bar.id).update(change_amount=change_amount, change_pct=change_pct)

    return {"symbols": len(symbols), "bars": bars_written}


def _compute_metrics_for_scenario(*, symbols_qs, scenario: Scenario, recompute_all: bool = False) -> dict:
    """Compute DailyMetric + Alert for a given scenario and subset of symbols."""
    symbols = list(symbols_qs)

    def scenario_hash(s: Scenario) -> str:
        payload = "|".join([
            str(s.a), str(s.b), str(s.c), str(s.d), str(s.e), str(getattr(s,'vc',None)), str(getattr(s,'fl',None)),
            str(s.n1), str(s.n2), str(s.n3), str(s.n4),
        ])
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    cur_hash = scenario_hash(scenario)
    needs_full = recompute_all or (scenario.last_computed_config_hash and scenario.last_computed_config_hash != cur_hash)

    if needs_full:
        print(f"[compute] full recompute scenario={scenario.id} {scenario.name}")
        Alert.objects.filter(scenario=scenario, symbol__in=symbols).delete()
        DailyMetric.objects.filter(scenario=scenario, symbol__in=symbols).delete()
        scenario.last_full_recompute_at = timezone.now()
        scenario.save(update_fields=["last_full_recompute_at"])

    n1 = int(scenario.n1 or 0)
    n2 = int(scenario.n2 or 0)
    n3 = int(scenario.n3 or 0)
    n4 = int(scenario.n4 or 0)
    lookback_trading = max((n1 + n2 + 5), (n3 + n4 + 5), (n1 + 5), 20)
    buffer_days = lookback_trading * 2 + 10

    computed_rows = 0
    for sym in symbols:
        try:
            if not needs_full:
                last_date = DailyMetric.objects.filter(symbol=sym, scenario=scenario).aggregate(m=Max("date"))["m"]
                if last_date:
                    start = last_date - timedelta(days=buffer_days)
                    Alert.objects.filter(symbol=sym, scenario=scenario, date__gte=start).delete()
                    DailyMetric.objects.filter(symbol=sym, scenario=scenario, date__gte=start).delete()
                else:
                    start = None
            else:
                start = None

            bars_qs = DailyBar.objects.filter(symbol=sym).order_by("date")
            if start:
                bars_qs = bars_qs.filter(date__gte=start)

            for d in bars_qs.values_list("date", flat=True):
                compute_for_symbol_scenario(sym, scenario, d)
                computed_rows += 1
        except Exception as e:
            print(f"[compute] error {sym} {scenario}: {e}")
            continue

    Scenario.objects.filter(id=scenario.id).update(last_computed_config_hash=cur_hash)
    return {"symbols": len(symbols), "rows": computed_rows, "full": bool(needs_full)}


@shared_task
def fetch_daily_bars_task():
    symbols = Symbol.objects.filter(active=True).all()
    max_years = Scenario.objects.filter(active=True).order_by("-history_years").values_list("history_years", flat=True).first() or 2
    outputsize = desired_outputsize_years(int(max_years))

    stats = _fetch_daily_bars_for_symbols(symbol_qs=symbols, outputsize=outputsize)
    return f"ok outputsize={outputsize} symbols={stats.get('symbols')} bars={stats.get('bars')}"

@shared_task
def compute_metrics_task(recompute_all: bool = False):
    """Compute metrics and alerts.

    Default behavior is **incremental** (recompute the recent window + new days).
    If scenario variables changed since last compute, we do a **full recompute** for that scenario.
    If recompute_all=True, force full recompute for all scenarios.
    """
    symbols = Symbol.objects.filter(active=True).all()
    scenarios = Scenario.objects.filter(active=True).all()

    for scenario in scenarios:
        _compute_metrics_for_scenario(symbols_qs=symbols, scenario=scenario, recompute_all=recompute_all)

    return "ok"


@shared_task
def _noop():
    return "ok"


@shared_task(bind=True)
def fetch_daily_bars_job_task(self, *, symbol_ids=None, scenario_id=None, backtest_id=None, user_id=None, job_id=None):
    """Tracked job wrapper around DailyBar fetching.

    If symbol_ids is None -> fetch for all active symbols.
    If scenario_id provided -> outputsize based on scenario.history_years.
    """
    job = None
    if job_id:
        job = ProcessingJob.objects.filter(id=job_id).first()
        if job:
            job.status = ProcessingJob.Status.RUNNING
            job.task_id = getattr(self.request, "id", "") or ""
            job.started_at = timezone.now()
            job.save(update_fields=["status", "task_id", "started_at"])
    if job is None:
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.FETCH_BARS,
            status=ProcessingJob.Status.RUNNING,
            task_id=getattr(self.request, "id", "") or "",
            backtest_id=backtest_id,
            scenario_id=scenario_id,
            created_by_id=user_id,
            started_at=timezone.now(),
        )
    try:
        if symbol_ids:
            symbol_qs = Symbol.objects.filter(id__in=list(symbol_ids))
        else:
            symbol_qs = Symbol.objects.filter(active=True)

        years = None
        if scenario_id:
            years = Scenario.objects.filter(id=scenario_id).values_list("history_years", flat=True).first()
        if years is None:
            years = Scenario.objects.filter(active=True).order_by("-history_years").values_list("history_years", flat=True).first() or 2
        outputsize = desired_outputsize_years(int(years))

        stats = _fetch_daily_bars_for_symbols(symbol_qs=symbol_qs, outputsize=outputsize)
        job.status = ProcessingJob.Status.DONE
        job.message = f"Fetched bars: symbols={stats.get('symbols')} bars={stats.get('bars')} outputsize={outputsize}"
        job.finished_at = timezone.now()
        job.save(update_fields=["status", "message", "finished_at"])
        return job.message
    except Exception as e:
        job.status = ProcessingJob.Status.FAILED
        job.error = str(e)
        job.finished_at = timezone.now()
        job.save(update_fields=["status", "error", "finished_at"])
        raise


@shared_task(bind=True)
def compute_metrics_job_task(self, *, scenario_id, symbol_ids=None, recompute_all=False, backtest_id=None, user_id=None, job_id=None):
    """Tracked job wrapper around metrics computation."""
    job = None
    if job_id:
        job = ProcessingJob.objects.filter(id=job_id).first()
        if job:
            job.status = ProcessingJob.Status.RUNNING
            job.task_id = getattr(self.request, "id", "") or ""
            job.started_at = timezone.now()
            job.save(update_fields=["status", "task_id", "started_at"])
    if job is None:
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.COMPUTE_METRICS,
            status=ProcessingJob.Status.RUNNING,
            task_id=getattr(self.request, "id", "") or "",
            backtest_id=backtest_id,
            scenario_id=scenario_id,
            created_by_id=user_id,
            started_at=timezone.now(),
        )
    try:
        scenario = Scenario.objects.get(id=scenario_id)
        if symbol_ids:
            symbols_qs = Symbol.objects.filter(id__in=list(symbol_ids))
        else:
            symbols_qs = Symbol.objects.filter(active=True)

        stats = _compute_metrics_for_scenario(symbols_qs=symbols_qs, scenario=scenario, recompute_all=bool(recompute_all))
        job.status = ProcessingJob.Status.DONE
        job.message = f"Computed metrics: symbols={stats.get('symbols')} rows={stats.get('rows')} full={stats.get('full')}"
        job.finished_at = timezone.now()
        job.save(update_fields=["status", "message", "finished_at"])
        return job.message
    except Exception as e:
        job.status = ProcessingJob.Status.FAILED
        job.error = str(e)
        job.finished_at = timezone.now()
        job.save(update_fields=["status", "error", "finished_at"])
        raise


@shared_task(bind=True)
def compute_metrics_all_job_task(self, recompute_all: bool = False, user_id=None):
    """Tracked job wrapper for a full compute across all active scenarios."""
    job = ProcessingJob.objects.create(
        job_type=ProcessingJob.JobType.COMPUTE_METRICS,
        status=ProcessingJob.Status.RUNNING,
        task_id=getattr(self.request, "id", "") or "",
        created_by_id=user_id,
        started_at=timezone.now(),
        message=f"compute_all recompute_all={bool(recompute_all)}",
    )
    try:
        msg = compute_metrics_task(bool(recompute_all))
        job.status = ProcessingJob.Status.DONE
        job.message = f"{job.message} -> {msg}"
        job.finished_at = timezone.now()
        job.save(update_fields=["status", "message", "finished_at"])
        return msg
    except Exception as e:
        job.status = ProcessingJob.Status.FAILED
        job.error = str(e)
        job.finished_at = timezone.now()
        job.save(update_fields=["status", "error", "finished_at"])
        raise

@shared_task
def send_daily_alerts_task():
    """
    Sends a single recap email for the most recent alert date.
    Includes RATIO_P and AMP_H trend indicators per (symbol, scenario).
    """
    last = Alert.objects.order_by("-date").first()
    if not last:
        return "no-alerts"

    alert_date = last.date
    alerts = Alert.objects.filter(date=alert_date).select_related("symbol", "scenario").order_by("scenario__name", "symbol__ticker")
    if not alerts.exists():
        return "no-alerts-today"

    recipients = list(EmailRecipient.objects.filter(active=True).values_list("email", flat=True))
    if not recipients:
        return "no-recipients"

    def fmt_pct(x):
        """Formats values that are already stored as percentages (0-100)."""
        if x is None:
            return "—"
        try:
            return f"{Decimal(x):.2f}%"
        except Exception:
            return "—"

    def fmt_num(x):
        if x is None:
            return "—"
        try:
            return f"{Decimal(x):.6f}"
        except Exception:
            return "—"

    rows = []
    for a in alerts:
        m = DailyMetric.objects.filter(symbol=a.symbol, scenario=a.scenario, date=alert_date).first()
        ratio_p = fmt_pct(getattr(m, "ratio_P", None) if m else None)
        amp_h = fmt_pct(getattr(m, "amp_h", None) if m else None)
        rows.append(
            f"<tr>"
            f"<td>{a.date}</td>"
            f"<td>{a.symbol.ticker}</td>"
            f"<td>{a.scenario.name}</td>"
            f"<td>{a.alerts}</td>"
            f"<td>{ratio_p}</td>"
            f"<td>{amp_h}</td>"
            f"</tr>"
        )

    html = f"""
    <h3>Alertes bourse - {alert_date}</h3>
    <p>Un seul email récapitulatif pour tous les scénarios.</p>
    <table border="1" cellpadding="6" cellspacing="0">
      <thead>
        <tr>
          <th>Date</th><th>Action</th><th>Scénario</th><th>Alertes</th><th>RATIO_P</th><th>AMP_H</th>
        </tr>
      </thead>
      <tbody>
        {''.join(rows)}
      </tbody>
    </table>
    """

    msg = EmailMultiAlternatives(
        subject=f"Stock Alerts - {alert_date}",
        body=f"Alertes bourse - {alert_date}",
        from_email=getattr(settings, "DEFAULT_FROM_EMAIL", None),
        to=recipients,
    )
    msg.attach_alternative(html, "text/html")
    msg.send()
    return "sent"
@shared_task
def check_and_send_scheduled_alerts_task():
    """Runs every minute. If configured time matches and not sent today, send daily alerts."""
    from django.utils import timezone as dj_tz
    email_cfg = EmailSettings.get_solo()

    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(email_cfg.timezone or "Asia/Jerusalem")
    except Exception:
        tz = dj_tz.get_current_timezone()

    now = dj_tz.now().astimezone(tz)
    today = now.date()

    if email_cfg.last_sent_date == today:
        return "already_sent"

    if now.hour == int(email_cfg.send_hour) and now.minute == int(email_cfg.send_minute):
        send_daily_alerts_task.delay()
        email_cfg.last_sent_date = today
        email_cfg.save(update_fields=["last_sent_date", "updated_at"])
        return "scheduled_sent"

    return "not_due"

@shared_task
def run_backtest_task(backtest_id: int):
    # Lazy imports to avoid circular imports
    from .services.backtesting.prep import prepare_backtest_data
    from .services.backtesting.engine import run_backtest as engine_run_backtest
    """Run a backtest end-to-end (Feature 3).

    Steps:
    1) Mark Backtest as RUNNING
    2) Ensure prerequisite data exists (fetch bars / compute metrics) if missing
    3) Run the backtest engine (minimal implementation)
    4) Persist results JSON and mark DONE (or FAILED)
    """
    bt = Backtest.objects.filter(id=backtest_id).first()
    if not bt:
        return f"backtest {backtest_id} not found"

    Backtest.objects.filter(id=bt.id).update(status=Backtest.Status.RUNNING, error_message="")
    try:
        prep_report = prepare_backtest_data(bt)
        engine_result = engine_run_backtest(bt)
        results = engine_result.results
        results["prep"] = {
            "did_fetch_bars": prep_report.did_fetch_bars,
            "did_compute_metrics": prep_report.did_compute_metrics,
            "notes": prep_report.notes,
        }
        # Persist results JSON + portfolio tables (Feature 8)
        from django.db import transaction
        from .models import BacktestPortfolioDaily, BacktestPortfolioKPI

        portfolio = results.get("portfolio") or {}
        port_daily = portfolio.get("daily") or []
        port_kpi = portfolio.get("kpi") or {}

        with transaction.atomic():
            Backtest.objects.filter(id=bt.id).update(status=Backtest.Status.DONE, results=results, error_message="")

            # Replace portfolio daily rows
            BacktestPortfolioDaily.objects.filter(backtest_id=bt.id).delete()
            daily_objs = []
            for r in port_daily:
                try:
                    daily_objs.append(
                        BacktestPortfolioDaily(
                            backtest_id=bt.id,
                            date=r.get("date"),
                            global_cash=r.get("global_cash") or 0,
                            cash_allocated=r.get("cash_allocated") or 0,
                            positions_value=r.get("positions_value") or 0,
                            equity=r.get("equity") or 0,
                            invested=r.get("invested") or 0,
                            drawdown=r.get("drawdown") or 0,
                        )
                    )
                except Exception:
                    continue
            if daily_objs:
                BacktestPortfolioDaily.objects.bulk_create(daily_objs, batch_size=1000)

            # Upsert KPI row
            BacktestPortfolioKPI.objects.update_or_create(
                backtest_id=bt.id,
                defaults={
                    "capital_total": port_kpi.get("capital_total") or 0,
                    "invested_end": port_kpi.get("invested_end") or 0,
                    "equity_end": port_kpi.get("equity_end") or 0,
                    "bt_return": port_kpi.get("BT"),
                    "bmj_return": port_kpi.get("BMJ"),
                    "nb_days": port_kpi.get("NB_DAYS") or 0,
                    "max_drawdown": port_kpi.get("max_drawdown") or 0,
                },
            )
        return "ok"
    except Exception as e:
        Backtest.objects.filter(id=bt.id).update(status=Backtest.Status.FAILED, error_message=str(e))
        raise


@shared_task(bind=True)
def run_backtest_job_task(self, backtest_id: int, user_id=None, job_id=None):
    """Tracked job wrapper around run_backtest_task."""
    job = None
    if job_id:
        job = ProcessingJob.objects.filter(id=job_id).first()
        if job:
            job.status = ProcessingJob.Status.RUNNING
            job.task_id = getattr(self.request, "id", "") or ""
            job.started_at = timezone.now()
            job.save(update_fields=["status", "task_id", "started_at"])
    if job is None:
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.RUN_BACKTEST,
            status=ProcessingJob.Status.RUNNING,
            task_id=getattr(self.request, "id", "") or "",
            backtest_id=backtest_id,
            created_by_id=user_id,
            started_at=timezone.now(),
        )
    try:
        msg = run_backtest_task(backtest_id)
        job.status = ProcessingJob.Status.DONE
        job.message = str(msg)
        job.finished_at = timezone.now()
        job.save(update_fields=["status", "message", "finished_at"])
        return msg
    except Exception as e:
        job.status = ProcessingJob.Status.FAILED
        job.error = str(e)
        job.finished_at = timezone.now()
        job.save(update_fields=["status", "error", "finished_at"])
        raise
