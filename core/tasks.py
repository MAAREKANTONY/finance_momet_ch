from celery import shared_task
from decimal import Decimal
from datetime import datetime, date
from django.conf import settings
from django.core.mail import EmailMultiAlternatives
import traceback

import hashlib
from datetime import timedelta
from django.db.models import Max

from .models import Symbol, Scenario, DailyBar, DailyMetric, Alert, EmailRecipient, EmailSettings
from .services.provider_twelvedata import TwelveDataClient
from .services.calculations import compute_for_symbol_scenario
from .services.joblog import log_info, log_error

def parse_date(s: str) -> date:
    return datetime.fromisoformat(s.replace("Z","")).date()

def desired_outputsize_years(years: int) -> int:
    # Roughly 252 trading days / year. Add buffer.
    if years <= 0:
        return 260
    return min(5000, years * 260)

@shared_task
def fetch_daily_bars_task():
    log_info(\"fetch_daily_bars\", \"START\")
    client = TwelveDataClient()
    symbols = Symbol.objects.filter(active=True).all()
    max_years = Scenario.objects.filter(active=True).order_by("-history_years").values_list("history_years", flat=True).first() or 2
    outputsize = desired_outputsize_years(int(max_years))

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

            DailyBar.objects.update_or_create(symbol=sym, date=d, defaults={"open": o, "high": h, "low": l, "close": c, "source": "twelvedata"})

        last_bar = DailyBar.objects.filter(symbol=sym).order_by("-date").first()
        prev_bar = DailyBar.objects.filter(symbol=sym, date__lt=last_bar.date).order_by("-date").first() if last_bar else None
        if last_bar and prev_bar and prev_bar.close:
            change_amount = last_bar.close - prev_bar.close
            change_pct = (change_amount / prev_bar.close) * Decimal("100") if prev_bar.close != 0 else None
            DailyBar.objects.filter(id=last_bar.id).update(change_amount=change_amount, change_pct=change_pct)

    log_info("fetch_daily_bars", f"SUCCESS outputsize={outputsize}")
    return f"ok outputsize={outputsize}"

@shared_task
def compute_metrics_task(recompute_all: bool = False):
    log_info("compute_metrics", f"START recompute_all={recompute_all}")
    """Compute metrics and alerts.

    Default behavior is **incremental** (recompute the recent window + new days).
    If scenario variables changed since last compute, we do a **full recompute** for that scenario.
    If recompute_all=True, force full recompute for all scenarios.
    """
        scenarios = Scenario.objects.filter(active=True).all()

    def scenario_hash(s: Scenario) -> str:
        payload = "|".join([
            str(s.a), str(s.b), str(s.c), str(s.d), str(s.e),
            str(s.n1), str(s.n2), str(s.n3), str(s.n4),
        ])
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    for scenario in scenarios:
        # auto-attach unassigned symbols to default scenario
        if getattr(scenario, 'is_default', False):
            from .models import SymbolScenario
            unassigned = Symbol.objects.filter(active=True).exclude(id__in=SymbolScenario.objects.values('symbol_id'))
            for sym_u in unassigned:
                SymbolScenario.objects.get_or_create(symbol=sym_u, scenario=scenario)
        symbols = scenario.symbols.filter(active=True).all()  # after any auto-attach
        cur_hash = scenario_hash(scenario)
        needs_full = recompute_all or (scenario.last_computed_config_hash and scenario.last_computed_config_hash != cur_hash)

        if needs_full:
            print(f"[compute] full recompute scenario={scenario.id} {scenario.name}")
            Alert.objects.filter(scenario=scenario).delete()
            DailyMetric.objects.filter(scenario=scenario).delete()
            from django.utils import timezone
            scenario.last_full_recompute_at = timezone.now()
            scenario.save(update_fields=["last_full_recompute_at"])  # hash saved at end

        # incremental window size (trading days) -> convert to calendar days
        n1 = int(scenario.n1 or 0)
        n2 = int(scenario.n2 or 0)
        n3 = int(scenario.n3 or 0)
        n4 = int(scenario.n4 or 0)
        lookback_trading = max((n1 + n2 + 5), (n3 + n4 + 5), (n1 + 5), 20)
        buffer_days = lookback_trading * 2 + 10  # weekends/holidays buffer

        for sym in symbols:
            try:
                if not needs_full:
                    last_date = DailyMetric.objects.filter(symbol=sym, scenario=scenario).aggregate(m=Max("date"))["m"]
                    if last_date:
                        start = last_date - timedelta(days=buffer_days)
                        # delete recent window to recompute consistently with rolling windows
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
            except Exception as e:
                log_error("compute_metrics", f"error {sym} {scenario}: {e}", traceback.format_exc(), scenario=scenario, symbol=sym)
                continue

        # Mark scenario as computed with current hash
        Scenario.objects.filter(id=scenario.id).update(last_computed_config_hash=cur_hash)

    log_info("compute_metrics", "SUCCESS")
    log_info("send_daily_alerts", "SUCCESS")
    return "ok"

@shared_task
def send_daily_alerts_task():
    log_info("send_daily_alerts", "START")
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
    try:
        msg.send()
    except Exception as e:
        log_error("send_daily_alerts", f"ERROR: {e}", traceback.format_exc())
        raise
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
