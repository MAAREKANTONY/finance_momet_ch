import csv
from io import BytesIO
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse, HttpResponse
from django.shortcuts import render, redirect, get_object_or_404
from django.db.models import Max
from django.views.decorators.http import require_POST

from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from openpyxl.styles import Font, Alignment
import zipfile as pyzip

from .models import Alert, Scenario, Symbol, EmailRecipient, DailyBar, DailyMetric, EmailSettings
from .forms import ScenarioForm, EmailRecipientForm, SymbolManualForm, EmailSettingsForm
from .services.provider_twelvedata import TwelveDataClient


@login_required
def dashboard(request):
    last = Alert.objects.aggregate(last_date=Max("date"))["last_date"]
    scenarios = Scenario.objects.filter(active=True).count()
    symbols = Symbol.objects.filter(active=True).count()
    alerts_count = Alert.objects.filter(date=last).count() if last else 0
    return render(request, "dashboard.html", {"last_date": last, "scenarios": scenarios, "symbols": symbols, "alerts_count": alerts_count})



@login_required
def alerts_table(request):
    date_str = (request.GET.get("date") or "").strip()
    scenario_id = (request.GET.get("scenario") or "").strip()
    ticker = (request.GET.get("ticker") or "").strip()
    alert_codes = request.GET.getlist("alert")

    qs = Alert.objects.select_related("symbol", "scenario").all().order_by("-date", "scenario__name", "symbol__ticker")
    if date_str:
        qs = qs.filter(date=date_str)
    if scenario_id:
        qs = qs.filter(scenario_id=scenario_id)
    if ticker:
        qs = qs.filter(symbol__ticker=ticker)
    if alert_codes:
        from django.db.models import Q
        q = Q()
        for code in alert_codes:
            if code:
                q |= Q(alerts__icontains=code)
        qs = qs.filter(q)

    scenarios = Scenario.objects.all().order_by("name")
    symbols = Symbol.objects.all().order_by("ticker")
    all_alert_codes = ["A1","B1","C1","D1","E1","F1","G1","H1"]

    return render(request, "alerts.html", {
        "alerts": qs[:2000],
        "scenarios": scenarios,
        "symbols": symbols,
        "selected_date": date_str,
        "selected_scenario": int(scenario_id) if scenario_id else "",
        "selected_ticker": ticker,
        "selected_alerts": alert_codes,
        "all_alert_codes": all_alert_codes,
    })


@login_required
def alerts_export_csv(request):
    date_str = request.GET.get("date") or ""
    scenario_id = request.GET.get("scenario") or ""
    qs = Alert.objects.select_related("symbol", "scenario").all().order_by("date", "scenario__name", "symbol__ticker")
    if date_str:
        qs = qs.filter(date=date_str)
    if scenario_id:
        qs = qs.filter(scenario_id=scenario_id)

    response = HttpResponse(content_type="text/csv; charset=utf-8")
    response["Content-Disposition"] = 'attachment; filename="alerts.csv"'
    writer = csv.writer(response)
    writer.writerow(["date", "ticker", "exchange", "scenario", "alerts"])
    for a in qs:
        writer.writerow([a.date.isoformat(), a.symbol.ticker, a.symbol.exchange, a.scenario.name, a.alerts])
    return response


def _autosize(ws):
    for col in ws.columns:
        max_len = 0
        col_letter = get_column_letter(col[0].column)
        for cell in col:
            v = "" if cell.value is None else str(cell.value)
            max_len = max(max_len, len(v))
        ws.column_dimensions[col_letter].width = min(70, max(10, max_len + 2))


def _header(ws, row=1):
    for cell in ws[row]:
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center")


def _build_scenario_workbook(scenario: Scenario, symbols_qs, date_from: str = "", date_to: str = "") -> Workbook:
    wb = Workbook()
    first = True

    alerts_qs = Alert.objects.filter(scenario=scenario, symbol__in=symbols_qs).select_related("symbol").order_by("symbol__ticker", "date")
    if date_from:
        alerts_qs = alerts_qs.filter(date__gte=date_from)
    if date_to:
        alerts_qs = alerts_qs.filter(date__lte=date_to)
    alerts_map = {(a.symbol_id, a.date): a.alerts for a in alerts_qs}

    for sym in symbols_qs.order_by("ticker", "exchange"):
        bars = DailyBar.objects.filter(symbol=sym).order_by("date")
        if date_from:
            bars = bars.filter(date__gte=date_from)
        if date_to:
            bars = bars.filter(date__lte=date_to)

        metrics = DailyMetric.objects.filter(symbol=sym, scenario=scenario).order_by("date")
        if date_from:
            metrics = metrics.filter(date__gte=date_from)
        if date_to:
            metrics = metrics.filter(date__lte=date_to)
        metrics_by_date = {m.date: m for m in metrics}

        title = sym.ticker[:28]
        ws = wb.active if first else wb.create_sheet(title=title)
        ws.title = title
        first = False

        ws.append([f"Scenario: {scenario.name}"])
        ws.append([f"Description: {scenario.description}"])
        ws.append([f"Vars: a={scenario.a} b={scenario.b} c={scenario.c} d={scenario.d} e={scenario.e} | N1={scenario.n1} N2={scenario.n2} N3={scenario.n3} | history_years={scenario.history_years}"])
        ws.append([f"Ticker: {sym.ticker}  Exchange: {sym.exchange}  Name: {sym.name}"])
        ws.append([])

        header = [
            "date",
            "open","high","low","close","change_amount","change_pct",
            "V","slope_P","sum_pos_P","nb_pos_P","ratio_P","amp_h",
            "P","M","M1","X","X1","T","Q","S","K1","K2","K3","K4",
            "alerts",
        ]
        ws.append(header)
        _header(ws, ws.max_row)

        def f(x):
            return float(x) if x is not None else None

        for b in bars:
            m = metrics_by_date.get(b.date)
            ws.append([
                b.date.isoformat(),
                f(b.open), f(b.high), f(b.low), f(b.close), f(b.change_amount), f(b.change_pct),
                f(m.V) if m else None,
                f(m.slope_P) if m else None,
                f(m.sum_pos_P) if m else None,
                (m.nb_pos_P if m and m.nb_pos_P is not None else None),
                f(m.ratio_P) if m else None,
                f(m.amp_h) if m else None,
                f(m.P) if m else None,
                f(m.M) if m else None,
                f(m.M1) if m else None,
                f(m.X) if m else None,
                f(m.X1) if m else None,
                f(m.T) if m else None,
                f(m.Q) if m else None,
                f(m.S) if m else None,
                f(m.K1) if m else None,
                f(m.K2) if m else None,
                f(m.K3) if m else None,
                f(m.K4) if m else None,
                alerts_map.get((sym.id, b.date), ""),
            ])

        _autosize(ws)

    return wb


@login_required
def data_export_scenario_xlsx(request, scenario_id: int):
    scenario = get_object_or_404(Scenario, pk=scenario_id)
    ticker = (request.GET.get("ticker") or "").strip()
    exchange = (request.GET.get("exchange") or "").strip()
    date_from = (request.GET.get("from") or "").strip()
    date_to = (request.GET.get("to") or "").strip()

    symbols_qs = Symbol.objects.filter(active=True)
    if ticker:
        symbols_qs = symbols_qs.filter(ticker=ticker)
    if exchange:
        symbols_qs = symbols_qs.filter(exchange=exchange)

    wb = _build_scenario_workbook(scenario, symbols_qs, date_from=date_from, date_to=date_to)

    bio = BytesIO()
    wb.save(bio)
    bio.seek(0)
    safe_name = "".join([c if c.isalnum() or c in ("-","_") else "_" for c in scenario.name])[:50] or "scenario"
    resp = HttpResponse(bio.getvalue(), content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    resp["Content-Disposition"] = f'attachment; filename="{safe_name}.xlsx"'
    return resp


@login_required
def data_export_all_scenarios_zip(request):
    ticker = (request.GET.get("ticker") or "").strip()
    exchange = (request.GET.get("exchange") or "").strip()
    date_from = (request.GET.get("from") or "").strip()
    date_to = (request.GET.get("to") or "").strip()

    symbols_qs = Symbol.objects.filter(active=True)
    if ticker:
        symbols_qs = symbols_qs.filter(ticker=ticker)
    if exchange:
        symbols_qs = symbols_qs.filter(exchange=exchange)

    scenarios = Scenario.objects.filter(active=True).order_by("name")

    zip_bytes = BytesIO()
    with pyzip.ZipFile(zip_bytes, "w", compression=pyzip.ZIP_DEFLATED) as zf:
        for scenario in scenarios:
            wb = _build_scenario_workbook(scenario, symbols_qs, date_from=date_from, date_to=date_to)
            wb_io = BytesIO()
            wb.save(wb_io)
            wb_io.seek(0)
            safe_name = "".join([c if c.isalnum() or c in ("-","_") else "_" for c in scenario.name])[:50] or "scenario"
            zf.writestr(f"{safe_name}.xlsx", wb_io.getvalue())

    zip_bytes.seek(0)
    resp = HttpResponse(zip_bytes.getvalue(), content_type="application/zip")
    resp["Content-Disposition"] = 'attachment; filename="scenarios_exports.zip"'
    return resp


@login_required
def data_export_xlsx(request):
    # Legacy combined export retained (3 sheets)
    ticker = (request.GET.get("ticker") or "").strip()
    exchange = (request.GET.get("exchange") or "").strip()
    scenario_id = (request.GET.get("scenario") or "").strip()
    date_from = (request.GET.get("from") or "").strip()
    date_to = (request.GET.get("to") or "").strip()

    symbols_qs = Symbol.objects.all()
    if ticker:
        symbols_qs = symbols_qs.filter(ticker=ticker)
    if exchange:
        symbols_qs = symbols_qs.filter(exchange=exchange)

    symbol_ids = list(symbols_qs.values_list("id", flat=True))
    bars = DailyBar.objects.filter(symbol_id__in=symbol_ids).select_related("symbol").order_by("symbol__ticker", "date")
    if date_from:
        bars = bars.filter(date__gte=date_from)
    if date_to:
        bars = bars.filter(date__lte=date_to)

    metrics = DailyMetric.objects.filter(symbol_id__in=symbol_ids).select_related("symbol", "scenario").order_by("symbol__ticker", "scenario__name", "date")
    if scenario_id:
        metrics = metrics.filter(scenario_id=scenario_id)
    if date_from:
        metrics = metrics.filter(date__gte=date_from)
    if date_to:
        metrics = metrics.filter(date__lte=date_to)

    alerts = Alert.objects.filter(symbol_id__in=symbol_ids).select_related("symbol", "scenario").order_by("symbol__ticker", "scenario__name", "date")
    if scenario_id:
        alerts = alerts.filter(scenario_id=scenario_id)
    if date_from:
        alerts = alerts.filter(date__gte=date_from)
    if date_to:
        alerts = alerts.filter(date__lte=date_to)

    wb = Workbook()
    wb.remove(wb.active)

    ws1 = wb.create_sheet("DailyBars")
    header1 = ["ticker", "exchange", "date", "open", "high", "low", "close", "change_amount", "change_pct", "source"]
    ws1.append(header1); _header(ws1, 1)
    for b in bars:
        ws1.append([b.symbol.ticker, b.symbol.exchange, b.date.isoformat(), float(b.open), float(b.high), float(b.low), float(b.close),
                    float(b.change_amount) if b.change_amount is not None else None,
                    float(b.change_pct) if b.change_pct is not None else None, b.source])
    _autosize(ws1)

    ws2 = wb.create_sheet("DailyMetrics")
    header2 = ["ticker","exchange","scenario","date","P","M","M1","X","X1","T","Q","S","K1","K2","K3","K4"]
    ws2.append(header2); _header(ws2, 1)
    def f(x): return float(x) if x is not None else None
    for m in metrics:
        ws2.append([m.symbol.ticker, m.symbol.exchange, m.scenario.name, m.date.isoformat(), f(m.P), f(m.M), f(m.M1), f(m.X), f(m.X1), f(m.T), f(m.Q), f(m.S), f(m.K1), f(m.K2), f(m.K3), f(m.K4)])
    _autosize(ws2)

    ws3 = wb.create_sheet("Alerts")
    header3 = ["date","ticker","exchange","scenario","alerts"]
    ws3.append(header3); _header(ws3, 1)
    for a in alerts:
        ws3.append([a.date.isoformat(), a.symbol.ticker, a.symbol.exchange, a.scenario.name, a.alerts])
    _autosize(ws3)

    bio = BytesIO(); wb.save(bio); bio.seek(0)
    resp = HttpResponse(bio.getvalue(), content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    resp["Content-Disposition"] = 'attachment; filename="stock_data_export.xlsx"'
    return resp


@login_required
def symbols_page(request):
    symbols = Symbol.objects.all().order_by("-active", "ticker")[:2000]
    manual_form = SymbolManualForm()
    return render(request, "symbols.html", {"symbols": symbols, "manual_form": manual_form})


@login_required
@require_POST
def symbol_add(request):
    ticker = (request.POST.get("ticker") or "").strip()
    exchange = (request.POST.get("exchange") or "").strip()
    name = (request.POST.get("name") or "").strip()
    instrument_type = (request.POST.get("instrument_type") or "").strip()
    country = (request.POST.get("country") or "").strip()
    currency = (request.POST.get("currency") or "").strip()

    if ticker:
        obj, created = Symbol.objects.get_or_create(
            ticker=ticker, exchange=exchange,
            defaults={"name": name, "instrument_type": instrument_type, "country": country, "currency": currency, "active": True}
        )
        if not created:
            Symbol.objects.filter(id=obj.id).update(
                name=name or obj.name,
                instrument_type=instrument_type or obj.instrument_type,
                country=country or obj.country,
                currency=currency or obj.currency,
                active=True,
            )
        messages.success(request, f"Ajouté: {ticker} {('('+exchange+')') if exchange else ''}")
        return redirect("symbols_page")

    form = SymbolManualForm(request.POST)
    if form.is_valid():
        sym = form.save()
        messages.success(request, f"Ajouté: {sym}")
    else:
        messages.error(request, "Erreur: symbole invalide.")
    return redirect("symbols_page")


@login_required
@require_POST
def symbol_toggle_active(request, pk: int):
    sym = get_object_or_404(Symbol, pk=pk)
    sym.active = not sym.active
    sym.save(update_fields=["active"])
    return redirect("symbols_page")


@login_required
@require_POST
def symbol_delete(request, pk: int):
    sym = get_object_or_404(Symbol, pk=pk)
    sym.delete()
    return redirect("symbols_page")


@login_required
def scenarios_page(request):
    scenarios = Scenario.objects.all().order_by("-active", "name")
    return render(request, "scenarios.html", {"scenarios": scenarios})


@login_required
def scenario_create(request):
    if request.method == "POST":
        form = ScenarioForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, "Scénario créé.")
            return redirect("scenarios_page")
    else:
        form = ScenarioForm()
    return render(request, "scenario_form.html", {"form": form, "mode": "create"})


@login_required
def scenario_edit(request, pk: int):
    scenario = get_object_or_404(Scenario, pk=pk)
    if request.method == "POST":
        form = ScenarioForm(request.POST, instance=scenario)
        if form.is_valid():
            form.save()
            messages.success(request, "Scénario mis à jour.")
            return redirect("scenarios_page")
    else:
        form = ScenarioForm(instance=scenario)
    return render(request, "scenario_form.html", {"form": form, "mode": "edit", "scenario": scenario})


@login_required
@require_POST
def scenario_delete(request, pk: int):
    scenario = get_object_or_404(Scenario, pk=pk)
    scenario.delete()
    messages.success(request, "Scénario supprimé.")
    return redirect("scenarios_page")



@login_required
def email_settings_page(request):
    recipients = EmailRecipient.objects.all().order_by("-active", "email")
    settings_obj = EmailSettings.get_solo()

    if request.method == "POST" and request.POST.get("_action") == "add_recipient":
        form = EmailRecipientForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, "Destinataire ajouté.")
            return redirect("email_settings")
        messages.error(request, "Email invalide.")
        settings_form = EmailSettingsForm(instance=settings_obj)

    elif request.method == "POST" and request.POST.get("_action") == "save_settings":
        settings_form = EmailSettingsForm(request.POST, instance=settings_obj)
        if settings_form.is_valid():
            settings_form.save()
            messages.success(request, "Paramètres email mis à jour.")
            return redirect("email_settings")
        messages.error(request, "Paramètres invalides.")
        form = EmailRecipientForm()

    else:
        form = EmailRecipientForm()
        settings_form = EmailSettingsForm(instance=settings_obj)

    return render(
        request,
        "email_settings.html",
        {"recipients": recipients, "form": form, "settings_form": settings_form, "settings_obj": settings_obj},
    )




@login_required
@require_POST
def run_compute_now(request):
    try:
        from core.tasks import compute_metrics_task
        compute_metrics_task.delay()
        messages.success(request, "Calculs lancés (en background via Celery).")
    except Exception as e:
        messages.error(request, f"Erreur lancement calculs: {e}")
    return redirect("email_settings")


@login_required
@require_POST
def run_recompute_all_now(request):
    """Force a full recompute for all scenarios.

    Useful when you suspect old rows were computed with previous formulas,
    or after a big backfill of historical daily bars.
    """
    try:
        from core.tasks import compute_metrics_task
        compute_metrics_task.delay(True)
        messages.success(request, "Recompute complet lancé (tous scénarios, via Celery).")
    except Exception as e:
        messages.error(request, f"Erreur recompute complet: {e}")
    return redirect("email_settings")


@login_required
@require_POST
def send_mail_now(request):
    try:
        from core.tasks import send_daily_alerts_task
        send_daily_alerts_task.delay()
        messages.success(request, "Envoi email demandé (en background via Celery).")
    except Exception as e:
        messages.error(request, f"Erreur envoi email: {e}")
    return redirect("email_settings")

@login_required
@require_POST
def email_recipient_toggle(request, pk: int):
    r = get_object_or_404(EmailRecipient, pk=pk)
    r.active = not r.active
    r.save(update_fields=["active"])
    return redirect("email_settings")


@login_required
@require_POST
def email_recipient_delete(request, pk: int):
    r = get_object_or_404(EmailRecipient, pk=pk)
    r.delete()
    return redirect("email_settings")


@login_required
def api_symbol_search(request):
    q = (request.GET.get("q") or "").strip()
    if len(q) < 1:
        return JsonResponse({"data": []})
    client = TwelveDataClient()
    try:
        items = client.symbol_search(q, limit=12)
    except Exception as e:
        return JsonResponse({"error": str(e), "data": []}, status=400)
    out = []
    for it in items:
        out.append(
            {
                "symbol": it.get("symbol") or it.get("ticker") or "",
                "exchange": it.get("exchange") or "",
                "name": it.get("instrument_name") or it.get("name") or "",
                "instrument_type": it.get("instrument_type") or "",
                "country": it.get("country") or "",
                "currency": it.get("currency") or "",
            }
        )
    return JsonResponse({"data": out})
