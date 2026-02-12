from celery import shared_task
from decimal import Decimal
from datetime import datetime, date
from django.conf import settings
from django.core.mail import EmailMultiAlternatives

import hashlib
from datetime import timedelta
from django.db.models import Max, Q

from .models import Symbol, Scenario, DailyBar, DailyMetric, Alert, EmailRecipient, EmailSettings, AlertDefinition
from .models import Backtest
from .models import ProcessingJob
from .services.provider_twelvedata import TwelveDataClient
from .services.calculations import compute_for_symbol_scenario
from .services.calculations_fast import compute_full_for_symbol_scenario

from django.utils import timezone

def parse_date(s: str) -> date:
    return datetime.fromisoformat(s.replace("Z","")).date()

def desired_outputsize_years(years: int) -> int:
    # Roughly 252 trading days / year. Add buffer.
    if years <= 0:
        return 260
    return min(5000, years * 260)



def _parse_int_or_none(x):
    """Parse TwelveData numeric strings to int; return None if empty/invalid."""
    if x is None:
        return None
    if isinstance(x, int):
        return x
    s = str(x).strip()
    if s == "" or s.lower() == "null":
        return None
    try:
        return int(Decimal(s))
    except Exception:
        return None

def _fetch_daily_bars_for_symbols(*, symbol_qs, outputsize: int, force_full: bool = False) -> dict:
    """Fetch/update daily bars for a queryset of Symbol.

    Returns basic stats: {"symbols":..., "bars":...}.
    """
    client = TwelveDataClient()
    symbols = list(symbol_qs)
    bars_written = 0

    today = timezone.now().date()

    for sym in symbols:
        exchange = sym.exchange or getattr(settings, "DEFAULT_EXCHANGE", "")
        try:
            # Delta fetch by default: if we already have bars, only request dates after the last stored bar.
            # This avoids re-downloading years of history each day.
            start_date = None
            if not force_full:
                last_date = DailyBar.objects.filter(symbol=sym).aggregate(Max("date")).get("date__max")
                if last_date:
                    start = last_date + timedelta(days=1)
                    if start <= today:
                        start_date = start.isoformat()
                    else:
                        # Already up to date.
                        continue

            values = client.time_series_daily(
                sym.ticker,
                exchange=exchange,
                outputsize=outputsize,
                start_date=start_date,
            )
        except Exception as e:
            print(f"[fetch] error {sym}: {e}")
            continue

        if not values:
            continue

        values_sorted = sorted(values, key=lambda v: v.get("datetime"))

        # Build new bars in memory and insert in bulk.
        new_bars = []
        for v in values_sorted:
            try:
                d = parse_date(v["datetime"])
                o = Decimal(v["open"]); h = Decimal(v["high"]); l = Decimal(v["low"]); c = Decimal(v["close"])
                vol = _parse_int_or_none(v.get("volume"))
            except Exception:
                continue

            if force_full:
                # Legacy behavior: upsert all rows (slower but keeps ability to refresh history).
                DailyBar.objects.update_or_create(
                    symbol=sym,
                    date=d,
                    defaults={"open": o, "high": h, "low": l, "close": c, "volume": vol, "source": "twelvedata"},
                )
                bars_written += 1
            else:
                # Delta mode: insert only new rows.
                new_bars.append(
                    DailyBar(
                        symbol=sym,
                        date=d,
                        open=o,
                        high=h,
                        low=l,
                        close=c,
                        volume=vol,
                        source="twelvedata",
                    )
                )

        if not force_full and new_bars:
            DailyBar.objects.bulk_create(new_bars, ignore_conflicts=True, batch_size=2000)
            bars_written += len(new_bars)

        # Update change_* for the latest bar (cheap and keeps UI consistent).
        last_two = list(DailyBar.objects.filter(symbol=sym).order_by("-date")[:2])
        if len(last_two) >= 2 and last_two[1].close:
            last_bar, prev_bar = last_two[0], last_two[1]
            change_amount = last_bar.close - prev_bar.close
            change_pct = (change_amount / prev_bar.close) * Decimal("100") if prev_bar.close != 0 else None
            DailyBar.objects.filter(id=last_bar.id).update(change_amount=change_amount, change_pct=change_pct)

    return {"symbols": len(symbols), "bars": bars_written, "force_full": bool(force_full)}


def _compute_metrics_for_scenario(*, symbols_qs, scenario: Scenario, recompute_all: bool = False) -> dict:
    """Compute DailyMetric + Alert for a given scenario and subset of symbols."""
    symbols = list(symbols_qs)

    def scenario_hash(s: Scenario) -> str:
        payload = "|".join([
            str(s.a), str(s.b), str(s.c), str(s.d), str(s.e), str(getattr(s,'vc',None)), str(getattr(s,'fl',None)),
            str(s.n1), str(s.n2), str(s.n3), str(s.n4),
            # K2f parameters (additive but must participate in the hash to keep metrics consistent)
            str(getattr(s, 'n5', None)), str(getattr(s, 'k2j', None)), str(getattr(s, 'cr', None)),
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
    n5 = int(getattr(scenario, 'n5', 0) or 0)
    k2j = int(getattr(scenario, 'k2j', 0) or 0)
    # K2f requires enough history to compute N5 variations + K2J smoothing.
    lookback_trading = max((n1 + n2 + 5), (n3 + n4 + 5), (n1 + 5), (n5 + k2j + 5), 20)
    buffer_days = lookback_trading * 2 + 10

    computed_rows = 0
    for sym in symbols:
        try:
            if needs_full:
                # Fast full recompute: compute in-memory and bulk_create.
                bars = DailyBar.objects.filter(symbol=sym).order_by("date").only("date", "open", "high", "low", "close")
                m_written, a_written = compute_full_for_symbol_scenario(symbol=sym, scenario=scenario, bars=bars)
                computed_rows += m_written
                continue

            # Incremental recompute (legacy behavior): delete a recent window and recompute per day.
            last_date = DailyMetric.objects.filter(symbol=sym, scenario=scenario).aggregate(m=Max("date"))["m"]
            if last_date:
                start = last_date - timedelta(days=buffer_days)
                Alert.objects.filter(symbol=sym, scenario=scenario, date__gte=start).delete()
                DailyMetric.objects.filter(symbol=sym, scenario=scenario, date__gte=start).delete()
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
def fetch_daily_bars_job_task(self, *, symbol_ids=None, scenario_id=None, force_full: bool = False, backtest_id=None, user_id=None, job_id=None):
    """Tracked job wrapper around DailyBar fetching.

    If symbol_ids is None -> fetch for all active symbols.
    If scenario_id provided -> outputsize based on scenario.history_years.
    force_full=True keeps legacy upsert behavior to refresh historical rows.
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

        stats = _fetch_daily_bars_for_symbols(symbol_qs=symbol_qs, outputsize=outputsize, force_full=bool(force_full))
        job.status = ProcessingJob.Status.DONE
        job.message = (
            f"Fetched bars: symbols={stats.get('symbols')} bars={stats.get('bars')} "
            f"outputsize={outputsize} force_full={bool(force_full)}"
        )
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
        # Scoping rules (no regression for legacy flows):
        # - If explicit symbol_ids are provided (e.g., from a Backtest universe snapshot), compute only those.
        # - Otherwise, when computing a single scenario from UI, compute only the symbols attached to that scenario.
        #   Fallback to all active symbols only if the scenario has no explicit universe.
        if symbol_ids:
            symbols_qs = Symbol.objects.filter(id__in=list(symbol_ids))
            scope_note = f"explicit_ids={len(list(symbol_ids))}"
        else:
            # When no explicit symbol_ids are provided, we interpret this job as "compute this scenario".
            # To avoid surprise recomputes across the whole universe, we scope strictly to the scenario symbols.
            scenario_symbols_qs = scenario.symbols.filter(active=True)
            if not scenario_symbols_qs.exists():
                job.status = ProcessingJob.Status.DONE
                job.message = "No symbols linked to this scenario (nothing to compute)."
                job.finished_at = timezone.now()
                job.save(update_fields=["status", "message", "finished_at"])
                return job.message
            symbols_qs = scenario_symbols_qs
            scope_note = "scenario_symbols"

        stats = _compute_metrics_for_scenario(symbols_qs=symbols_qs, scenario=scenario, recompute_all=bool(recompute_all))
        job.status = ProcessingJob.Status.DONE
        job.message = (
            f"Computed metrics: scope={scope_note} scenario={scenario.id} "
            f"symbols={stats.get('symbols')} rows={stats.get('rows')} full={stats.get('full')}"
        )
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


def _send_alert_definition_email(defn: AlertDefinition, alert_date):
    """Send one email for a specific AlertDefinition.

    NO-REGRESSION: this reads from the existing `Alert` table; it does not
    change how alerts are computed.
    """
    recipients = list(defn.recipients.filter(active=True).values_list("email", flat=True))
    if not recipients:
        return "no-recipients"

    qs = Alert.objects.filter(date=alert_date).select_related("symbol", "scenario")
    # Scenario filter (empty => all)
    scenario_ids = list(defn.scenarios.values_list("id", flat=True))
    if scenario_ids:
        qs = qs.filter(scenario_id__in=scenario_ids)

    codes = defn.get_codes_list()
    if codes:
        q = Q()
        for c in codes:
            q |= Q(alerts__icontains=c)
        qs = qs.filter(q)

    alerts = list(qs.order_by("scenario__name", "symbol__ticker"))
    if not alerts:
        return "no-alerts"

    def fmt_pct(x):
        if x is None:
            return "—"
        try:
            return f"{Decimal(x):.2f}%"
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
    <h3>Stock Alerts - {alert_date}</h3>
    <p><strong>{defn.name}</strong></p>
    <p class=\"muted\">{defn.description or ''}</p>
    <table border=\"1\" cellpadding=\"6\" cellspacing=\"0\">
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
        subject=f"Stock Alerts - {defn.name} - {alert_date}",
        body=f"Stock Alerts - {defn.name} - {alert_date}",
        from_email=getattr(settings, "DEFAULT_FROM_EMAIL", None),
        to=recipients,
    )
    msg.attach_alternative(html, "text/html")
    msg.send()
    return "sent"

@shared_task
def send_alert_definition_now_task(definition_id: int):
    """Send one configured alert definition immediately.

    This is used by the UI action 'Envoyer'.
    NO-REGRESSION: reads from existing Alert rows and does not change computation.
    """
    last = Alert.objects.order_by("-date").first()
    if not last:
        return "no-alert-data"
    defn = AlertDefinition.objects.filter(id=definition_id).prefetch_related("scenarios", "recipients").first()
    if not defn:
        return "not-found"
    return _send_alert_definition_email(defn, last.date)


@shared_task
def check_and_send_scheduled_alerts_task():
    """Runs every minute.

    Legacy behavior (NO-REGRESSION):
    - Uses `EmailSettings` to send ONE recap email for all scenarios.

    Additive behavior:
    - Also evaluates user-defined `AlertDefinition` rules (scenarios + lines + recipients + schedule).
    """
    from django.utils import timezone as dj_tz
    email_cfg = EmailSettings.get_solo()

    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(email_cfg.timezone or "Asia/Jerusalem")
    except Exception:
        tz = dj_tz.get_current_timezone()

    now = dj_tz.now().astimezone(tz)
    today = now.date()

    results = []

    # --- Legacy global email (unchanged) ---
    if email_cfg.last_sent_date != today:
        if now.hour == int(email_cfg.send_hour) and now.minute == int(email_cfg.send_minute):
            send_daily_alerts_task.delay()
            email_cfg.last_sent_date = today
            email_cfg.save(update_fields=["last_sent_date", "updated_at"])
            results.append("global_sent")
        else:
            results.append("global_not_due")
    else:
        results.append("global_already_sent")

    # --- Additive: user-defined alert definitions ---
    last = Alert.objects.order_by("-date").first()
    if not last:
        results.append("no_alert_data")
        return ";".join(results)

    alert_date = last.date
    defs = AlertDefinition.objects.filter(is_active=True).prefetch_related("scenarios", "recipients")
    for d in defs:
        try:
            import zoneinfo
            dtz = zoneinfo.ZoneInfo(d.timezone or "Asia/Jerusalem")
        except Exception:
            dtz = tz

        dnow = dj_tz.now().astimezone(dtz)
        dtoday = dnow.date()

        if d.last_sent_date == dtoday:
            continue

        if dnow.hour == int(d.send_hour) and dnow.minute == int(d.send_minute):
            _send_alert_definition_email(d, alert_date)
            d.last_sent_date = dtoday
            d.save(update_fields=["last_sent_date", "updated_at"])
            results.append(f"def_sent#{d.id}")

    return ";".join(results)

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

        # --- Optional (NO-REGRESSION) Parquet storage ---
        # Writes daily series to Parquet *in addition* to existing JSON results.
        # Enabled only when ENABLE_PARQUET_STORAGE=1.
        try:
            from .services.backtesting.parquet_storage import write_backtest_parquet_files

            write_backtest_parquet_files(bt, results)
        except Exception:
            # Never fail a backtest because of optional storage.
            pass

        results["prep"] = {
            "did_fetch_bars": prep_report.did_fetch_bars,
            "did_compute_metrics": prep_report.did_compute_metrics,
            "notes": prep_report.notes,
        }

        # --- Automatic results offload (NO-REGRESSION) ---
        # PostgreSQL JSONB has a hard limit (~256MB) for object elements.
        # Large universes (e.g., S&P 500) can exceed this when storing the full
        # per-ticker daily series inside Backtest.results. When the payload is
        # too large, we offload only the heavy `daily` arrays to files on disk
        # and keep lightweight pointers in JSON.
        try:
            from .services.backtesting.results_offload import offload_daily_series_if_needed

            results = offload_daily_series_if_needed(bt, results)
        except Exception:
            # Never fail a backtest because of optional offload logic.
            pass
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