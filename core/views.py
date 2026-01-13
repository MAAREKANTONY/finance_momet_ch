import csv
from io import BytesIO
from typing import Iterable
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse, HttpResponse
from django.shortcuts import render, redirect, get_object_or_404
from django.db.models import Max
from django.views.decorators.http import require_POST

from openpyxl import Workbook, load_workbook
from openpyxl.utils import get_column_letter
from openpyxl.styles import Font, Alignment
import zipfile as pyzip
import json

from .models import Alert, Scenario, Symbol, EmailRecipient, DailyBar, DailyMetric, EmailSettings, JobLog, Backtest
from .forms import ScenarioForm, EmailRecipientForm, SymbolManualForm, EmailSettingsForm, SymbolScenariosForm, SymbolImportForm, BacktestForm, BACKTEST_SIGNAL_CHOICES
from .services.provider_twelvedata import TwelveDataClient

try:
    # Celery is optional in dev; we keep the import defensive so the web container can boot.
    from .tasks import fetch_daily_bars_task
except Exception:  # pragma: no cover
    fetch_daily_bars_task = None


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

    default_scenario = Scenario.objects.filter(is_default=True, active=True).first()

    if ticker:
        obj, created = Symbol.objects.get_or_create(
            ticker=ticker,
            exchange=exchange,
            defaults={
                "name": name,
                "instrument_type": instrument_type,
                "country": country,
                "currency": currency,
                "active": True,
            },
        )
        if not created:
            Symbol.objects.filter(id=obj.id).update(
                name=name or obj.name,
                instrument_type=instrument_type or obj.instrument_type,
                country=country or obj.country,
                currency=currency or obj.currency,
                active=True,
            )

        # Toujours associer au scénario par défaut si présent
        if default_scenario:
            obj.scenarios.add(default_scenario)

        messages.success(request, f"Ajouté: {ticker} {('('+exchange+')') if exchange else ''}")
        return redirect("symbols_page")

    form = SymbolManualForm(request.POST)
    if form.is_valid():
        sym = form.save()
        chosen = list(form.cleaned_data.get("scenarios") or [])
        if default_scenario and default_scenario not in chosen:
            chosen.append(default_scenario)
        if chosen:
            sym.scenarios.set(chosen)
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
def symbol_scenarios_edit(request, pk: int):
    """Manage scenario links from the ticker side."""

    symbol = get_object_or_404(Symbol, pk=pk)
    default_scenario = Scenario.objects.filter(is_default=True, active=True).first()

    if request.method == "POST":
        form = SymbolScenariosForm(request.POST)
        if form.is_valid():
            selected = list(form.cleaned_data["scenarios"])
            # Always keep default scenario linked if it exists
            if default_scenario and default_scenario not in selected:
                selected.append(default_scenario)
            symbol.scenarios.set(selected)
            messages.success(request, "Scénarios mis à jour.")
            JobLog.objects.create(job="symbol_scenarios", level="INFO", message=f"Updated scenarios for {symbol}")
            return redirect("symbols_page")
        messages.error(request, "Formulaire invalide.")
    else:
        initial = symbol.scenarios.filter(active=True)
        form = SymbolScenariosForm(initial={"scenarios": initial})

    return render(
        request,
        "symbol_scenarios.html",
        {"symbol": symbol, "form": form, "default_scenario": default_scenario},
    )


def _iter_symbol_rows_from_csv(file_obj) -> Iterable[dict]:
    """Yield dict rows from a CSV (auto-detect delimiter)."""

    content = file_obj.read()
    if isinstance(content, bytes):
        # try utf-8 first, fallback to latin-1
        try:
            text = content.decode("utf-8")
        except UnicodeDecodeError:
            text = content.decode("latin-1")
    else:
        text = str(content)

    sample = text[:4096]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
    except Exception:
        dialect = csv.excel
    reader = csv.DictReader(text.splitlines(), dialect=dialect)
    for row in reader:
        yield {k.strip() if isinstance(k, str) else k: (v.strip() if isinstance(v, str) else v) for k, v in row.items()}


def _iter_symbol_rows_from_xlsx(file_obj) -> Iterable[dict]:
    wb = load_workbook(filename=file_obj, read_only=True, data_only=True)
    ws = wb.active
    rows = ws.iter_rows(values_only=True)
    headers = next(rows, None)
    if not headers:
        return
    headers = [str(h).strip() if h is not None else "" for h in headers]
    for values in rows:
        d = {}
        for i, h in enumerate(headers):
            if not h:
                continue
            v = values[i] if i < len(values) else None
            if isinstance(v, str):
                v = v.strip()
            d[h] = v
        yield d


@login_required
def symbols_import(request):
    """Bulk import tickers from CSV/XLSX.

    Expected columns (case-insensitive):
      - ticker code (MSFT)
      - ticker market (NASDAQ)
      - scenario list (scenario1, scenario2)
    """

    if request.method == "POST":
        form = SymbolImportForm(request.POST, request.FILES)
        if form.is_valid():
            f = form.cleaned_data["file"]
            filename = (getattr(f, "name", "") or "").lower()
            default_scenario = Scenario.objects.filter(is_default=True, active=True).first()

            created = updated = skipped = 0
            missing_scenarios = 0
            errors: list[str] = []

            try:
                if filename.endswith(".xlsx") or filename.endswith(".xlsm") or filename.endswith(".xltx"):
                    row_iter = _iter_symbol_rows_from_xlsx(f)
                else:
                    row_iter = _iter_symbol_rows_from_csv(f)
            except Exception as e:
                messages.error(request, f"Impossible de lire le fichier: {e}")
                JobLog.objects.create(job="import_symbols", level="ERROR", message="Import failed", traceback=str(e))
                return redirect("symbols_page")

            def _get(row: dict, *keys: str) -> str:
                for k in keys:
                    for rk, rv in row.items():
                        if str(rk).strip().lower() == k:
                            return "" if rv is None else str(rv).strip()
                return ""

            for idx, row in enumerate(row_iter, start=2):
                ticker = _get(row, "ticker code", "ticker", "code", "ticker_code")
                market = _get(row, "ticker market", "market", "exchange", "ticker_market")
                scen_list = _get(row, "scenario list", "scenarios", "scenario", "scenario_list")

                if not ticker:
                    skipped += 1
                    continue

                try:
                    sym, was_created = Symbol.objects.get_or_create(
                        ticker=ticker,
                        exchange=market,
                        defaults={"active": True},
                    )
                    if was_created:
                        created += 1
                    else:
                        updated += 1
                        if not sym.active:
                            sym.active = True
                            sym.save(update_fields=["active"])

                    selected_scenarios: list[Scenario] = []
                    if default_scenario:
                        selected_scenarios.append(default_scenario)

                    if scen_list:
                        for name in [s.strip() for s in scen_list.split(",") if s.strip()]:
                            scen = Scenario.objects.filter(name__iexact=name).first()
                            if scen and scen.active:
                                if scen not in selected_scenarios:
                                    selected_scenarios.append(scen)
                            else:
                                missing_scenarios += 1

                    if selected_scenarios:
                        sym.scenarios.add(*selected_scenarios)
                except Exception as e:
                    skipped += 1
                    msg = f"Ligne {idx}: erreur pour ticker={ticker} market={market}: {e}"
                    errors.append(msg)

            summary = (
                f"Import tickers terminé. created={created}, updated={updated}, skipped={skipped}, "
                f"scenario_not_found={missing_scenarios}."
            )
            details = "\n".join(errors[:80])
            JobLog.objects.create(
                job="import_symbols",
                level="ERROR" if errors else "INFO",
                message=summary + ("\n" + details if details else ""),
            )
            messages.success(request, summary)
            return redirect("symbols_page")
    else:
        form = SymbolImportForm()

    return render(request, "symbols_import.html", {"form": form})


@login_required
def scenarios_page(request):
    scenarios = Scenario.objects.all().order_by("-active", "name")
    return render(request, "scenarios.html", {"scenarios": scenarios})


@login_required
def scenario_create(request):
    has_other_default = Scenario.objects.filter(is_default=True).exists()
    if request.method == "POST":
        form = ScenarioForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, "Scénario créé.")
            return redirect("scenarios_page")
    else:
        form = ScenarioForm()
    return render(request, "scenario_form.html", {"form": form, "mode": "create", "has_other_default": has_other_default})


@login_required
def scenario_edit(request, pk: int):
    scenario = get_object_or_404(Scenario, pk=pk)
    has_other_default = Scenario.objects.filter(is_default=True).exclude(pk=scenario.pk).exists()
    if request.method == "POST":
        form = ScenarioForm(request.POST, instance=scenario)
        if form.is_valid():
            form.save()
            messages.success(request, "Scénario mis à jour.")
            return redirect("scenarios_page")
    else:
        form = ScenarioForm(instance=scenario)
    return render(request, "scenario_form.html", {"form": form, "mode": "edit", "scenario": scenario, "has_other_default": has_other_default})


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
def fetch_bars_now(request):
    """Manual trigger for fetching daily bars (market data)."""
    try:
        fetch_daily_bars_task.delay()
        JobLog.objects.create(level="INFO", job="fetch_bars_now", message="Fetch des daily bars demandé (Celery).")
        messages.success(request, "Collecte demandée (en background via Celery).")
    except Exception as e:
        JobLog.objects.create(level="ERROR", job="fetch_bars_now", message=str(e))
        messages.error(request, f"Erreur collecte: {e}")
    return redirect("email_settings")




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
def fetch_bars_now(request):
    """Fetch daily bars immediately (useful for manual refresh)."""
    try:
        fetch_daily_bars_task.delay()
        JobLog.objects.create(level="INFO", job="fetch_bars", message="Fetch daily bars demandé (Celery).")
        messages.success(request, "Collecte demandée (en background via Celery).")
    except Exception as e:
        JobLog.objects.create(level="ERROR", job="fetch_bars", message="Erreur lancement fetch", traceback=str(e))
        messages.error(request, f"Erreur lancement collecte: {e}")
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

from django.core.paginator import Paginator


@login_required
def logs_page(request):
    """Log viewer to help debug async/background jobs."""
    level = (request.GET.get("level") or "").upper().strip()
    job = (request.GET.get("job") or "").strip()

    qs = JobLog.objects.all().order_by("-created_at")
    if level in {"INFO", "WARNING", "ERROR"}:
        qs = qs.filter(level=level)
    if job:
        qs = qs.filter(job__icontains=job)

    paginator = Paginator(qs, 50)
    page = paginator.get_page(request.GET.get("page"))
    return render(request, "logs.html", {"page": page, "level": level, "job": job})



@login_required
def backtests_page(request):
    """Archive page: list saved backtests with a simple search."""
    qs = Backtest.objects.select_related("scenario").all()
    q = (request.GET.get("q") or "").strip()
    scenario_id = (request.GET.get("scenario") or "").strip()

    if q:
        qs = qs.filter(Q(name__icontains=q) | Q(description__icontains=q))
    if scenario_id:
        qs = qs.filter(scenario_id=scenario_id)

    scenarios = Scenario.objects.all().order_by("name")
    return render(
        request,
        "backtests_list.html",
        {"backtests": qs[:200], "q": q, "scenarios": scenarios, "scenario_id": scenario_id},
    )


@login_required
def backtest_create(request):
    """Create a Backtest configuration (no computation in Feature 1)."""
    if request.method == "POST":
        form = BacktestForm(request.POST)
        if form.is_valid():
            bt = form.save(commit=False)
            bt.created_by = request.user if request.user.is_authenticated else None
            # Snapshot current scenario universe for reproducibility
            symbols = (
                bt.scenario.symbols.all()
                .order_by("ticker", "exchange")
                .values_list("ticker", "exchange")
            )
            bt.universe_snapshot = [{"ticker": t, "exchange": e} for t, e in symbols]
            bt.save()
            messages.success(request, "Backtest enregistré (configuration).")
            return redirect("backtest_detail", pk=bt.pk)
    else:
        form = BacktestForm()
    return render(request, "backtest_create.html", {"form": form, "signal_choices_json": json.dumps(BACKTEST_SIGNAL_CHOICES)})


@login_required
def backtest_detail(request, pk: int):
    bt = get_object_or_404(Backtest.objects.select_related("scenario"), pk=pk)
    return render(request, "backtest_detail.html", {"bt": bt})


@login_required
def backtest_run(request, pk: int):
    """Launch a backtest run asynchronously."""
    bt = get_object_or_404(Backtest, pk=pk)
    if request.method != "POST":
        return redirect("backtest_detail", pk=pk)

    Backtest.objects.filter(id=bt.id).update(status=Backtest.Status.PENDING, error_message="")
    run_backtest_task.delay(bt.id)
    messages.success(request, "Backtest lancé (traitement en arrière-plan).")
    return redirect("backtest_detail", pk=pk)
