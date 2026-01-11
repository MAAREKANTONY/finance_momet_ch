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

from .models import (
    Alert, Scenario, Symbol, SymbolScenario, EmailRecipient, DailyBar, DailyMetric, EmailSettings,
    JobLog,
    Strategy, StrategyRule, BacktestRun, BacktestCapitalOverride, BacktestResult, BacktestTrade,
    BacktestDailyStat,
)
from .forms import (
    ScenarioForm,
    EmailRecipientForm,
    SymbolManualForm,
    EmailSettingsForm,
    SymbolScenariosForm,
    BacktestRunForm,
    SymbolImportForm,
)
from .services.provider_twelvedata import TwelveDataClient
from .tasks import run_backtest_task


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

    symbols_qs = scenario.symbols.filter(active=True)
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

    symbols_qs = scenario.symbols.filter(active=True)
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
        # auto-assign default scenario
        default_s = Scenario.objects.filter(is_default=True, active=True).first()
        if default_s:
            SymbolScenario.objects.get_or_create(symbol=obj, scenario=default_s)
        messages.success(request, f"Ajouté: {ticker} {('('+exchange+')') if exchange else ''}")
        return redirect("symbols_page")

    form = SymbolManualForm(request.POST)
    if form.is_valid():
        sym = form.save()
        # auto-assign default scenario
        default_s = Scenario.objects.filter(is_default=True, active=True).first()
        if default_s:
            SymbolScenario.objects.get_or_create(symbol=sym, scenario=default_s)
        messages.success(request, f"Ajouté: {sym}")
    else:
        messages.error(request, "Erreur: symbole invalide.")
    return redirect("symbols_page")


@login_required
def symbols_import(request):
    """
    Bulk import symbols from CSV/Excel.

    Expected columns (3):
    - ticker code (e.g. MSFT)
    - ticker market (e.g. NASDAQ)
    - scenario list (e.g. Scenario1, Scenario2)
    """
    from io import BytesIO, TextIOWrapper
    import csv

    form = SymbolImportForm(request.POST or None, request.FILES or None)
    if request.method == "POST" and form.is_valid():
        f = form.cleaned_data["file"]
        filename = (getattr(f, "name", "") or "").lower()

        rows = []
        try:
            if filename.endswith(".csv"):
                wrapper = TextIOWrapper(f.file, encoding="utf-8", errors="ignore")
                reader = csv.reader(wrapper)
                for r in reader:
                    if not r or all(not (c or "").strip() for c in r):
                        continue
                    rows.append(r)
            elif filename.endswith(".xlsx") or filename.endswith(".xlsm") or filename.endswith(".xltx") or filename.endswith(".xltm"):
                from openpyxl import load_workbook
                wb = load_workbook(filename=BytesIO(f.read()), data_only=True)
                ws = wb.active
                for r in ws.iter_rows(values_only=True):
                    if not r or all(v is None or str(v).strip() == "" for v in r):
                        continue
                    rows.append([str(v) if v is not None else "" for v in r])
            else:
                messages.error(request, "Format non supporté. Utilise un CSV ou un Excel .xlsx")
                return redirect("symbols_import")
        except Exception as e:
            JobLog.log("IMPORT_SYMBOLS", "ERROR", f"Erreur de lecture du fichier: {e}")
            messages.error(request, f"Erreur de lecture du fichier: {e}")
            return redirect("symbols_import")

        # Detect header (optional)
        def norm(x): return (x or "").strip().lower()
        if rows:
            h = [norm(c) for c in rows[0]]
            if "ticker" in " ".join(h) or "market" in " ".join(h) or "scenario" in " ".join(h):
                rows = rows[1:]

        ok_count = 0
        not_found = 0
        scenario_ignored = 0
        errors = 0

        for i, r in enumerate(rows, start=1):
            try:
                code = (r[0] if len(r) > 0 else "").strip().upper()
                market = (r[1] if len(r) > 1 else "").strip().upper()
                scen_list = (r[2] if len(r) > 2 else "").strip()

                if not code:
                    continue

                # Validate symbol with Twelve
                matches = symbol_search(code)
                match = None
                for m in matches:
                    if (m.get("symbol") or "").upper() == code:
                        if not market or (m.get("exchange") or "").upper() == market:
                            match = m
                            break
                if match is None:
                    not_found += 1
                    JobLog.log("IMPORT_SYMBOLS", "WARN", f"Ligne {i}: ticker introuvable: {code} ({market})")
                    continue

                sym, _created = Symbol.objects.get_or_create(
                    ticker=code,
                    defaults={
                        "exchange": match.get("exchange") or market,
                        "name": match.get("instrument_name") or code,
                        "currency": match.get("currency") or "",
                        "active": True,
                    },
                )
                # Update basic fields if empty
                if not sym.exchange and (match.get("exchange") or market):
                    sym.exchange = match.get("exchange") or market
                    sym.save(update_fields=["exchange"])

                # Associate scenarios
                scenarios_ok = []
                if scen_list:
                    for sname in [x.strip() for x in scen_list.split(",") if x.strip()]:
                        sc = Scenario.objects.filter(name__iexact=sname).first()
                        if not sc:
                            scenario_ignored += 1
                            JobLog.log("IMPORT_SYMBOLS", "WARN", f"Ligne {i}: scénario introuvable ignoré: {sname}")
                            continue
                        scenarios_ok.append(sc)

                # If no scenario specified, attach to default scenario (existing rule)
                if not scenarios_ok:
                    default_sc = Scenario.objects.filter(is_default=True).first()
                    if default_sc:
                        scenarios_ok = [default_sc]

                for sc in scenarios_ok:
                    SymbolScenario.objects.get_or_create(symbol=sym, scenario=sc)

                ok_count += 1
            except Exception as e:
                errors += 1
                JobLog.log("IMPORT_SYMBOLS", "ERROR", f"Ligne {i}: {code} ({market}) erreur: {e}")

        JobLog.log(
            "IMPORT_SYMBOLS",
            "INFO",
            f"Import terminé. OK={ok_count} / introuvables={not_found} / scénarios ignorés={scenario_ignored} / erreurs={errors}",
        )
        messages.success(request, f"Import terminé. OK={ok_count}, introuvables={not_found}, erreurs={errors}. Voir l'onglet Logs.")
        return redirect("symbols_page")

    return render(request, "symbols_import.html", {"form": form})


@login_required
def backtest_capital_overrides_page(request):
    """Page UI pour définir les capitaux initiaux par action (overrides) pour un scénario."""
    scenario_id = request.GET.get("scenario")
    scenario = None
    if scenario_id:
        try:
            scenario = Scenario.objects.get(pk=int(scenario_id))
        except Exception:
            scenario = None

    if scenario is None:
        scenario = Scenario.objects.filter(is_default=True).first() or Scenario.objects.order_by("name").first()

    rows = []
    if scenario:
        symbols = list(scenario.symbols.filter(active=True).order_by("ticker"))
        overrides = {
            o.symbol_id: float(o.initial_capital)
            for o in BacktestCapitalOverride.objects.filter(scenario=scenario)
        }
        for sym in symbols:
            rows.append({"sym": sym, "value": overrides.get(sym.id)})

    scenarios = Scenario.objects.order_by("-is_default", "name").all()
    return render(
        request,
        "backtest_capital_overrides.html",
        {"scenario": scenario, "scenarios": scenarios, "rows": rows},
    )


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


@login_required
def symbol_scenarios_edit(request, pk: int):
    sym = get_object_or_404(Symbol, pk=pk)
    if request.method == "POST":
        form = SymbolScenariosForm(request.POST)
        if form.is_valid():
            selected = list(form.cleaned_data["scenarios"])
            # Replace associations
            SymbolScenario.objects.filter(symbol=sym).exclude(scenario__in=selected).delete()
            for sc in selected:
                SymbolScenario.objects.get_or_create(symbol=sym, scenario=sc)
            messages.success(request, "Associations ticker ↔ scénarios mises à jour.")
            return redirect("symbols_page")
    else:
        form = SymbolScenariosForm(initial={"scenarios": sym.scenarios.all()})
    return render(request, "symbol_scenarios.html", {"symbol": sym, "form": form})


@login_required
@require_POST
def run_fetch_now(request):
    from .tasks import fetch_daily_bars_task
    fetch_daily_bars_task.delay()
    messages.success(request, "Collecte TwelveData lancée (asynchrone).")
    return redirect("email_settings")


@login_required
def logs_page(request):
    level = (request.GET.get("level") or "").strip().upper()
    job = (request.GET.get("job") or "").strip()
    qs = JobLog.objects.all()
    if level:
        qs = qs.filter(level=level)
    if job:
        qs = qs.filter(job__icontains=job)
    logs = qs[:500]
    return render(request, "logs.html", {"logs": logs, "level": level, "job": job})


@login_required
def backtesting_page(request):
    scenarios = Scenario.objects.order_by("-is_default", "name").all()

    # Ensure default strategies exist (3 lines)
    # Note: the DB might already contain one strategy (ex: Ligne 1) from older versions.
    # We therefore create the missing ones individually.
    defaults = [
        ("Ligne 1: Buy A1 / Sell B1", "A1", "B1"),
        ("Ligne 2: Buy E1 / Sell F1", "E1", "F1"),
        ("Ligne 3: Buy G1 / Sell H1", "G1", "H1"),
    ]
    for name, buy_sig, sell_sig in defaults:
        s, created = Strategy.objects.get_or_create(name=name, defaults={"description": "Auto-created"})
        # Ensure exactly the two rules exist
        StrategyRule.objects.get_or_create(
            strategy=s,
            action=StrategyRule.ACTION_BUY,
            signal_type=StrategyRule.SIGNAL_ALERT,
            signal_value=buy_sig,
            defaults={"sizing": "ALL_IN", "active": True},
        )
        StrategyRule.objects.get_or_create(
            strategy=s,
            action=StrategyRule.ACTION_SELL,
            signal_type=StrategyRule.SIGNAL_ALERT,
            signal_value=sell_sig,
            defaults={"sizing": "ALL_IN", "active": True},
        )

    strategies = Strategy.objects.order_by("name").all()
    runs = BacktestRun.objects.select_related("scenario", "strategy").order_by("-created_at")[:50]

    default_sc = Scenario.objects.filter(is_default=True).first() or Scenario.objects.order_by("name").first()
    default_strategy = strategies.first()

    form = BacktestRunForm(
        initial={
            "scenario": default_sc,
            "strategy": default_strategy,
            "capital_total": 0,
            "capital_per_symbol": 1000,
            "min_ratio_p": 0,
        }
    )

    return render(
        request,
        "backtesting.html",
        {
            "scenarios": scenarios,
            "strategies": strategies,
            "runs": runs,
            "form": form,
        },
    )


@login_required
@require_POST
def backtesting_save_capitals(request):
    scenario_id = request.POST.get("scenario_id")
    scenario = get_object_or_404(Scenario, pk=scenario_id)
    # Update per-symbol overrides. Input names: cap_<symbol_id>
    symbols = list(scenario.symbols.filter(active=True).only("id"))
    for sym in symbols:
        key = f"cap_{sym.id}"
        if key not in request.POST:
            continue
        raw = (request.POST.get(key) or "").strip()
        if raw == "":
            # empty means remove override
            BacktestCapitalOverride.objects.filter(scenario=scenario, symbol_id=sym.id).delete()
            continue
        try:
            val = float(raw)
        except Exception:
            continue
        if val <= 0:
            continue
        BacktestCapitalOverride.objects.update_or_create(
            scenario=scenario,
            symbol_id=sym.id,
            defaults={"initial_capital": val},
        )
    messages.success(request, "Capitaux de backtest enregistrés.")
    return redirect(f"/backtesting/?scenario={scenario.id}")


@login_required
@require_POST
def backtesting_run(request):
    form = BacktestRunForm(request.POST)
    if not form.is_valid():
        messages.error(request, "Paramètres de backtest invalides.")
        return redirect("backtesting_page")

    run: BacktestRun = form.save(commit=False)
    run.status = "CREATED"
    run.save()
    run_backtest_task.delay(run.id)
    messages.success(request, f"Backtest lancé (run #{run.id}).")
    return redirect("backtest_run_detail", run_id=run.id)


@login_required
def backtest_run_detail(request, run_id: int):
    run = get_object_or_404(BacktestRun, id=run_id, user=request.user)

    trades = list(
        BacktestTrade.objects.filter(run=run)
        .select_related("symbol")
        .order_by("symbol__ticker", "entry_date")
    )

    per_ticker_qs = (
        BacktestDailyStat.objects.filter(run=run)
        .select_related("symbol")
        .order_by("symbol__ticker", "date")
    )

    # Agrégation globale par date
    by_date: dict = {}
    for s in per_ticker_qs:
        by_date.setdefault(s.date, []).append(s)

    global_chart_labels: list[str] = []
    global_sgn_values: list[float] = []
    global_bt_values: list[float] = []
    global_bmj_values: list[float] = []

    for d in sorted(by_date.keys()):
        stats = by_date[d]
        if not stats:
            continue
        global_chart_labels.append(d.isoformat())
        global_sgn_values.append(float(sum(x.s_g_n for x in stats) / len(stats)))
        global_bt_values.append(float(sum(x.bt for x in stats)))
        global_bmj_values.append(float(sum(x.bmj for x in stats) / len(stats)))

    # Séries par ticker pour afficher graphs + table
    per_ticker_data = {}
    for s in per_ticker_qs:
        t = s.symbol.ticker
        dct = per_ticker_data.setdefault(
            t,
            {
                "labels": [],
                "sgn": [],
                "bt": [],
                "bmj": [],
                "trade_n": [],
                "last_g": [],
                "ratio_p": [],
            },
        )
        dct["labels"].append(s.date.isoformat())
        dct["sgn"].append(float(s.s_g_n))
        dct["bt"].append(float(s.bt))
        dct["bmj"].append(float(s.bmj))
        dct["trade_n"].append(int(s.trade_n))
        dct["last_g"].append(float(s.last_trade_g))
        dct["ratio_p"].append(float(s.ratio_p) if s.ratio_p is not None else None)

    return render(
        request,
        "backtest_run_detail.html",
        {
            "run": run,
            "trades": trades,
            "global_chart_labels": global_chart_labels,
            "global_sgn_values": global_sgn_values,
            "global_bt_values": global_bt_values,
            "global_bmj_values": global_bmj_values,
            "per_ticker_data": per_ticker_data,
        },
    )
def backtest_run_rerun(request, run_id: int):
    """Relance un backtest existant (efface les résultats précédents)."""
    run = get_object_or_404(BacktestRun, id=run_id)

    if request.method != "POST":
        # sécurité: ne relancer qu'en POST
        return redirect("backtest_run_detail", run_id=run.id)

    # Nettoyage des résultats précédents
    BacktestTrade.objects.filter(run=run).delete()
    # Purge previous daily stats/results before recompute
    BacktestDailyStat.objects.filter(run=run).delete()
    BacktestResult.objects.filter(run=run).delete()

    # BacktestRun.status is stored as a string (CREATED/RUNNING/DONE/FAILED)
    run.status = "CREATED"
    run.error_message = ""
    run.started_at = None
    run.finished_at = None
    # NOTE: BacktestRun doesn't have an `updated_at` field.
    run.save(update_fields=["status", "error_message", "started_at", "finished_at"])

    JobLog.info("backtest", f"Relance backtest run_id={run.id}", extra={"run_id": run.id})
    run_backtest_task.delay(run.id)

    messages.success(request, "Backtest relancé (traitement en arrière-plan).")
    return redirect("backtest_run_detail", run_id=run.id)
def backtest_run_export_xlsx(request, run_id: int):
    run = get_object_or_404(BacktestRun.objects.select_related("scenario", "strategy"), pk=run_id)
    from .services.excel_backtest import export_backtest_run_xlsx

    data = export_backtest_run_xlsx(run)
    filename = f"backtest_run_{run.id}.xlsx"
    resp = HttpResponse(data, content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    resp["Content-Disposition"] = f'attachment; filename="{filename}"'
    return resp


@login_required
def backtests_archive(request):
    """Archive/search over backtest runs."""
    q = (request.GET.get("q") or "").strip()
    scenario_id = (request.GET.get("scenario") or "").strip()
    status = (request.GET.get("status") or "").strip().upper()

    qs = BacktestRun.objects.select_related("scenario", "strategy").order_by("-created_at")
    if q:
        from django.db.models import Q
        qs = qs.filter(Q(name__icontains=q) | Q(description__icontains=q))
    if scenario_id:
        qs = qs.filter(scenario_id=scenario_id)
    if status:
        qs = qs.filter(status=status)

    runs = qs[:200]
    scenarios = Scenario.objects.order_by("-is_default", "name").all()
    statuses = ["CREATED", "RUNNING", "DONE", "ERROR"]
    return render(
        request,
        "backtests_archive.html",
        {"runs": runs, "q": q, "scenario_id": scenario_id, "status": status, "scenarios": scenarios, "statuses": statuses},
    )


@login_required
def backtest_symbol_detail(request, run_id: int, symbol_id: int):
    import json
    run = get_object_or_404(BacktestRun.objects.select_related("scenario", "strategy"), pk=run_id)
    symbol = get_object_or_404(Symbol, pk=symbol_id)
    stats = list(
        BacktestDailyStat.objects.filter(run=run, symbol=symbol).order_by("date")
    )
    # Build series
    dates = [s.date.isoformat() for s in stats]
    series_sgn = [float(s.S_G_N) if s.S_G_N is not None else None for s in stats]
    series_bt = [float(s.BT) if s.BT is not None else None for s in stats]
    series_bmj = [float(s.BMJ) if s.BMJ is not None else None for s in stats]

    # trades for symbol
    trades = list(BacktestTrade.objects.filter(run=run, symbol=symbol).order_by("buy_exec_date"))

    return render(
        request,
        "backtest_symbol_detail.html",
        {
            "run": run,
            "symbol": symbol,
            "stats": stats,
            "trades": trades,
            "chart": json.dumps({"dates": dates, "sgn": series_sgn, "bt": series_bt, "bmj": series_bmj}),
        },
    )