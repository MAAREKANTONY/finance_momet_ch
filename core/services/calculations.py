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

    # SUM_SLOPE on study price P
    npente = int(getattr(scenario, "npente", 100) or 100)
    sum_slope = None
    if npente and npente > 0:
        prior_Ps_desc = list(prior_metrics.values_list("P", flat=True)[:max(npente, n2)])
        prior_Ps = list(reversed([D(x) for x in prior_Ps_desc if D(x) is not None]))
        p_series = prior_Ps + [D(P)]
        if len(p_series) >= 2:
            vals = []
            for i in range(1, len(p_series)):
                p0 = D(p_series[i - 1])
                p1 = D(p_series[i])
                if p0 in (None, 0) or p1 is None:
                    continue
                vals.append((p1 - p0) / p0)
            if vals:
                sum_slope = sum(vals[-npente:])

    Kf = None
    if n2 and n2 > 0:
        prior_Ps_desc = list(prior_metrics.values_list("P", flat=True)[:n2])
        prior_Ps = list(reversed([D(x) for x in prior_Ps_desc if D(x) is not None]))
        p_series = prior_Ps + [D(P)]
        if len(p_series) >= (n2 + 1):
            vals_n2 = []
            for i in range(1, len(p_series[-(n2 + 1):])):
                p0 = D(p_series[-(n2 + 1):][i - 1])
                p1 = D(p_series[-(n2 + 1):][i])
                if p0 in (None, 0) or p1 is None:
                    vals_n2 = []
                    break
                vals_n2.append((p1 - p0) / p0)
            if len(vals_n2) == n2:
                p_sum = sum(vals_n2)
                Kf = M1 - (T * p_sum)

    metric, _ = DailyMetric.objects.update_or_create(
        symbol=symbol,
        scenario=scenario,
        date=trading_date,
        defaults={
            "P": P,
            "M": M,
            "M1": M1,
            "X": X,
            "X1": X1,
            "T": T,
            "Q": Q,
            "S": S,
            "K1": K1,
            "K1f": None,
            "K2f": None,
            "K2f_pre": None,
            "Kf2bis": Kf,
            "Kf3": None,
            "V_pre": None,
            "V_line": None,
            "K2": K2,
            "K3": K3,
            "K4": K4,
            "V": None,
            "slope_P": None,
            "sum_slope": sum_slope,
            "sum_pos_P": None,
            "nb_pos_P": None,
            "ratio_P": None,
            "amp_h": None,
        },
    )

    prev_metric = DailyMetric.objects.filter(symbol=symbol, scenario=scenario, date__lt=trading_date).order_by("-date").first()
    if not prev_metric:
        return metric, None

    # Signals are defined as strict crossings around 0 for the indicator series.
    # For indicators defined as K = P - Line, this is equivalent to P crossing that Line.
    alerts = []

    def cross0(prev_x, cur_x, pos_code, neg_code):
        prev_x = D(prev_x)
        cur_x = D(cur_x)
        if prev_x is None or cur_x is None:
            return
        if (prev_x < 0) and (cur_x > 0):
            alerts.append(pos_code)
        elif (prev_x > 0) and (cur_x < 0):
            alerts.append(neg_code)

    # A1/B1 : K1 crosses 0  (K1 = P - M1)
    cross0(prev_metric.K1, metric.K1, "A1", "B1")


    # C1/D1 : K2 crosses 0  (K2 = P - X1)
    cross0(prev_metric.K2, metric.K2, "C1", "D1")

    # E1/F1 : K3 crosses 0  (K3 = P - Q)
    cross0(prev_metric.K3, metric.K3, "E1", "F1")

    # G1/H1 : K4 crosses 0  (K4 = P - S)
    cross0(prev_metric.K4, metric.K4, "G1", "H1")

    # Kf alerts (Af/Bf) based on P crossing the Kf price line
    try:
        prev_p = D(getattr(prev_metric, "P", None))
        cur_p = D(getattr(metric, "P", None))
        prev_kf = D(getattr(prev_metric, "Kf2bis", None))
        cur_kf = D(getattr(metric, "Kf2bis", None))

        cross_up = (
            prev_p is not None and cur_p is not None and prev_kf is not None and cur_kf is not None
            and (prev_p < prev_kf) and (cur_p > cur_kf)
        )
        cross_down = (
            prev_p is not None and cur_p is not None and prev_kf is not None and cur_kf is not None
            and (prev_p > prev_kf) and (cur_p < cur_kf)
        )
        if cross_up:
            alerts.append("Af")
        if cross_down:
            alerts.append("Bf")
    except Exception:
        pass

    # SUM_SLOPE alerts (SPa/SPv) based on crossing the configured slope threshold
    try:
        prev_sum_slope = D(getattr(prev_metric, "sum_slope", None))
        cur_sum_slope = D(getattr(metric, "sum_slope", None))
        slope_threshold = D(getattr(scenario, "slope_threshold", None))
        if prev_sum_slope is not None and cur_sum_slope is not None and slope_threshold is not None:
            if (prev_sum_slope < slope_threshold) and (cur_sum_slope > slope_threshold):
                alerts.append("SPa")
            elif (prev_sum_slope > slope_threshold) and (cur_sum_slope < slope_threshold):
                alerts.append("SPv")
    except Exception:
        pass


    if alerts:
        alert_obj, _ = Alert.objects.update_or_create(symbol=symbol, scenario=scenario, date=trading_date, defaults={"alerts": ",".join(alerts)})
        return metric, alert_obj

    Alert.objects.filter(symbol=symbol, scenario=scenario, date=trading_date).delete()
    return metric, None