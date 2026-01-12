from decimal import Decimal, InvalidOperation
from core.models import DailyBar, DailyMetric, Alert

def D(x) -> Decimal:
    if x is None:
        return None
    if isinstance(x, Decimal):
        return x
    try:
        return Decimal(str(x))
    except (InvalidOperation, ValueError):
        return None

def compute_for_symbol_scenario(symbol, scenario, trading_date):
    bar = DailyBar.objects.filter(symbol=symbol, date=trading_date).first()
    if not bar:
        return None, None

    a = D(scenario.a); b = D(scenario.b); c = D(scenario.c); d = D(scenario.d); e = D(scenario.e)
    denom = (a + b + c + d)
    if denom == 0:
        return None, None

    F = D(bar.close); H = D(bar.high); L = D(bar.low); O = D(bar.open)
    P = (a*F + b*H + c*L + d*O) / denom

    prior_metrics = DailyMetric.objects.filter(symbol=symbol, scenario=scenario, date__lt=trading_date, P__isnull=False).order_by("-date")
    n1 = int(scenario.n1); n2 = int(scenario.n2)

    prior_P_for_M = list(prior_metrics.values_list("P", flat=True)[:n1])
    if len(prior_P_for_M) < n1:
        metric, _ = DailyMetric.objects.update_or_create(symbol=symbol, scenario=scenario, date=trading_date, defaults={"P": P})
        return metric, None

    M = max(prior_P_for_M)
    X = min(prior_P_for_M)

    prior_M = list(DailyMetric.objects.filter(symbol=symbol, scenario=scenario, date__lt=trading_date, M__isnull=False).order_by("-date").values_list("M", flat=True)[:n2])
    prior_X = list(DailyMetric.objects.filter(symbol=symbol, scenario=scenario, date__lt=trading_date, X__isnull=False).order_by("-date").values_list("X", flat=True)[:n2])

    if len(prior_M) < n2 or len(prior_X) < n2:
        metric, _ = DailyMetric.objects.update_or_create(symbol=symbol, scenario=scenario, date=trading_date, defaults={"P": P, "M": M, "X": X})
        return metric, None

    M1 = sum(prior_M) / Decimal(len(prior_M))
    X1 = sum(prior_X) / Decimal(len(prior_X))

    if e == 0:
        metric, _ = DailyMetric.objects.update_or_create(symbol=symbol, scenario=scenario, date=trading_date, defaults={"P": P, "M": M, "M1": M1, "X": X, "X1": X1})
        return metric, None

    T = (M1 - X1) / e
    Q = M1 - T
    S = M1 + T

    K1 = P - M1
    K2 = P - X1
    K3 = P - Q
    K4 = P - S

    # --- Trend indicators (V, slope_P, sum_pos_P, nb_pos_P, ratio_P, amp_h) ---
    V = None
    prev_bar = DailyBar.objects.filter(symbol=symbol, date__lt=trading_date).order_by("-date").first()
    if prev_bar and D(prev_bar.close) and D(prev_bar.close) != 0:
        # Percent change (already *100)
        V = (D(bar.close) - D(prev_bar.close)) * D(100) / D(prev_bar.close)

    n3 = int(getattr(scenario, "n3", 0) or 0)
    n4 = int(getattr(scenario, "n4", 0) or 0)

    slope_P = None
    if n3 and n3 > 0:
        # Need N3 days of V (requires N3+1 bars including the close of the day before the window)
        bars_desc = list(DailyBar.objects.filter(symbol=symbol, date__lte=trading_date).order_by("-date")[: (n3 + 1)])
        if len(bars_desc) >= (n3 + 1):
            vs = []
            for i in range(n3):
                c2 = D(bars_desc[i].close)
                c1 = D(bars_desc[i+1].close)
                if c1 and c1 != 0:
                    # Percent change (already *100)
                    vs.append((c2 - c1) * D(100) / c1)
            if len(vs) == n3:
                slope_P = sum(vs) / D(n3)

    sum_pos_P = None
    nb_pos_P = None
    ratio_P = None
    amp_h = None
    if n4 and n4 > 0 and n3 and n3 > 0:
        # Ensure current day slope is included even if metrics are computed out-of-order
        recent_metrics = list(DailyMetric.objects.filter(symbol=symbol, scenario=scenario, date__lte=trading_date, slope_P__isnull=False).order_by("-date")[:n4])
        # If current day isn't saved yet, we will include slope_P computed above
        if slope_P is not None and (not recent_metrics or recent_metrics[0].date != trading_date):
            # fabricate an in-memory record for current day
            class _Tmp: pass
            tmp=_Tmp(); tmp.slope_P=slope_P; tmp.date=trading_date
            recent_metrics = [tmp] + recent_metrics
            recent_metrics = recent_metrics[:n4]

        if len(recent_metrics) == n4:
            positives = [D(m.slope_P) for m in recent_metrics if D(m.slope_P) is not None and D(m.slope_P) > 0]
            nb_pos_P = len(positives)
            sum_pos_P = sum(positives) if positives else D(0)
            # Already a percentage
            ratio_P = D(nb_pos_P) * D(100) / D(n4)
            if nb_pos_P > 0:
                # Already a percentage
                amp_h = (sum_pos_P * D(100)) / (D(nb_pos_P) * D(n3))

    metric, _ = DailyMetric.objects.update_or_create(
        symbol=symbol, scenario=scenario, date=trading_date,
        defaults={"P": P, "M": M, "M1": M1, "X": X, "X1": X1, "T": T, "Q": Q, "S": S, "K1": K1, "K2": K2, "K3": K3, "K4": K4, "V": V, "slope_P": slope_P, "sum_pos_P": sum_pos_P, "nb_pos_P": nb_pos_P, "ratio_P": ratio_P, "amp_h": amp_h}
    )

    prev_metric = DailyMetric.objects.filter(symbol=symbol, scenario=scenario, date__lt=trading_date, K1__isnull=False, K2__isnull=False, K3__isnull=False, K4__isnull=False).order_by("-date").first()
    if not prev_metric:
        return metric, None

    alerts = []
    def cross(prev, cur, pos_code, neg_code):
        if prev is None or cur is None:
            return
        if prev < 0 and cur > 0:
            alerts.append(pos_code)
        elif prev > 0 and cur < 0:
            alerts.append(neg_code)

    cross(prev_metric.K1, metric.K1, "A1", "B1")
    cross(prev_metric.K2, metric.K2, "C1", "D1")
    cross(prev_metric.K3, metric.K3, "E1", "F1")
    cross(prev_metric.K4, metric.K4, "G1", "H1")

    if alerts:
        alert_obj, _ = Alert.objects.update_or_create(symbol=symbol, scenario=scenario, date=trading_date, defaults={"alerts": ",".join(alerts)})
        return metric, alert_obj

    Alert.objects.filter(symbol=symbol, scenario=scenario, date=trading_date).delete()
    return metric, None
