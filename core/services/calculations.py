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
    # K2f parameters (all configurable on Scenario; additive, does not change legacy formulas)
    n5 = int(getattr(scenario, "n5", 100) or 100)
    k2j = int(getattr(scenario, "k2j", 10) or 10)
    cr = D(getattr(scenario, "cr", D("10")))
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

    # --- K2f floating line (V5.2.32) ---
    # Definitions (using P as "prix d'étude"):
    # (1) daily variation v = (P - P(-1)) / P(-1)   (ratio, not percent)
    # (2) pente1 = sum_{N5 days} v * 100
    # (3) pente_deg = pente1 / 90
    # (4) uses existing scenario.e
    # (5) CR (scenario.cr, default 10)
    # (6) FC = pente_deg * e * CR
    # (7) K2f_pre = K1 - FC
    # (8) K2f = moving average of K2f_pre over K2J days
    # (9) pente2 = sum_{N5/2 days} v * 100
    # (10) diff = pente2 - pente1
    # Alerts:
    # - A2f: K1 crosses K2f from below to above AND diff > 0
    # - B2f: K1 crosses K2f from above to below OR diff < 0

    K2f_pre = None
    K2f = None
    k2f_diff = None

    try:
        if n5 and n5 > 1 and e not in (None, 0) and cr is not None and K1 is not None:
            # Build P series: last N5 previous P values (ascending) + today's P
            prior_Ps_desc = list(prior_metrics.values_list("P", flat=True)[:n5])
            prior_Ps = list(reversed([D(x) for x in prior_Ps_desc if D(x) is not None]))
            P_series = prior_Ps + [D(P)]
            # Need N5 variations => N5+1 P values
            if len(P_series) >= (n5 + 1):
                # keep only the last N5+1 values
                P_series = P_series[-(n5 + 1) :]
                vars_ = []
                ok = True
                for i in range(1, len(P_series)):
                    p0 = D(P_series[i - 1])
                    p1 = D(P_series[i])
                    if p0 in (None, 0) or p1 is None:
                        ok = False
                        break
                    vars_.append((p1 - p0) / p0)
                if ok and len(vars_) == n5:
                    pente1 = sum(vars_) * D(100)
                    half = max(1, int(n5 / 2))
                    pente2 = sum(vars_[-half:]) * D(100)
                    k2f_diff = pente2 - pente1

                    pente_deg = pente1 / D(90)
                    FC = pente_deg * e * cr
                    K2f_pre = K1 - FC

                    if k2j and k2j > 0:
                        prior_pre_desc = list(
                            DailyMetric.objects.filter(
                                symbol=symbol,
                                scenario=scenario,
                                date__lt=trading_date,
                                K2f_pre__isnull=False,
                            )
                            .order_by("-date")
                            .values_list("K2f_pre", flat=True)[: max(0, k2j - 1)]
                        )
                        prior_pre = list(reversed([D(x) for x in prior_pre_desc if D(x) is not None]))
                        pre_series = prior_pre + [D(K2f_pre)]
                        if len(pre_series) >= k2j:
                            pre_series = pre_series[-k2j:]
                            K2f = sum(pre_series) / D(len(pre_series))
    except Exception:
        # K2f is additive; never break the legacy computations
        K2f_pre = None
        K2f = None
        k2f_diff = None

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

    # --- K1f correction (uses ratio_p in [0..1], ratio_P is stored as percent [0..100]) ---
    vc = D(getattr(scenario, 'vc', None))
    if vc is None:
        vc = D('0.5')
    fl = D(getattr(scenario, 'fl', None))
    if fl is None:
        fl = D('0.5')
    ratio_p = (D(ratio_P) / D(100)) if ratio_P is not None else None
    E = (M1 - X1) if (M1 is not None and X1 is not None) else None
    C = D(0)
    if ratio_p is not None and E is not None and vc is not None:
        C = (vc - ratio_p) * fl * E
    K1f = (K1 + C) if K1 is not None else None

    # --- K2f floating line (V5.2.32) ---
    # All computations are additive (new fields) and do not alter existing K1/K2/K3/K4 logic.
    #
    # Definitions (from user spec):
    # 1) daily_variation = (P - P(-1)) / P(-1)  (ratio, not percent)
    # 2) slope1 = sum_{last N5 days}(daily_variation) * 100
    # 3) slope_deg = slope1 / 90
    # 4) use scenario.e (existing parameter)
    # 5) CR correction index (default 10)
    # 6) FC = slope_deg * e * CR
    # 7) K2f_pre = K1 - FC
    # 8) K2f = moving average over last K2J days of K2f_pre
    # 9) slope2 = sum_{last N5/2 days}(daily_variation) * 100
    # 10) diff = slope2 - slope1
    # 11) Buy (A2f): K1 crosses K2f bottom-up AND diff > 0
    # 12) Sell (B2f): K1 crosses K2f top-down OR diff < 0

    K2f_pre = None
    K2f = None
    diff_slope = None

    try:
        n5_eff = max(1, int(n5 or 1))
        n5_half = max(1, n5_eff // 2)
        k2j_eff = max(1, int(k2j or 1))
    except Exception:
        n5_eff = 100
        n5_half = 50
        k2j_eff = 10

    if cr is None:
        cr = D("10")

    # Need N5 prior P values to compute N5 daily variations when adding today's P.
    prior_P_for_var = list(prior_metrics.values_list("P", flat=True)[:n5_eff])
    if len(prior_P_for_var) >= n5_eff and K1 is not None and e not in (None, 0):
        # Build chronological series: oldest -> newest, then append today's P
        P_series = list(reversed([D(x) for x in prior_P_for_var])) + [P]

        # daily variations for the last N5 days
        variations = []
        ok = True
        for i in range(1, len(P_series)):
            p_prev = D(P_series[i - 1])
            p_cur = D(P_series[i])
            if p_prev is None or p_prev == 0 or p_cur is None:
                ok = False
                break
            variations.append((p_cur - p_prev) / p_prev)

        if ok and len(variations) >= n5_eff:
            variations = variations[-n5_eff:]
            slope1 = sum(variations) * D(100)
            # slope2 uses the last N5/2 variations
            slope2 = sum(variations[-n5_half:]) * D(100) if len(variations) >= n5_half else None
            if slope2 is not None:
                diff_slope = slope2 - slope1

            slope_deg = slope1 / D(90) if D(90) != 0 else None
            if slope_deg is not None and cr is not None:
                FC = slope_deg * e * cr
                K2f_pre = K1 - FC

                # Rolling mean over last K2J pre-line values (including today)
                prior_pre = list(
                    DailyMetric.objects.filter(
                        symbol=symbol,
                        scenario=scenario,
                        date__lt=trading_date,
                        K2f_pre__isnull=False,
                    )
                    .order_by("-date")
                    .values_list("K2f_pre", flat=True)[: max(0, k2j_eff - 1)]
                )
                pre_series = list(reversed([D(x) for x in prior_pre])) + [K2f_pre]
                if len(pre_series) >= k2j_eff:
                    pre_series = pre_series[-k2j_eff:]
                    K2f = sum(pre_series) / D(len(pre_series))

    # --- end K2f ---

    # Persist all indicators, including K1f (needed for A1f/B1f alerts + exports)
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
            "K1f": K1f,
            "K2f": K2f,
            "K2f_pre": K2f_pre,
            "K2": K2,
            "K3": K3,
            "K4": K4,
            "V": V,
            "slope_P": slope_P,
            "sum_pos_P": sum_pos_P,
            "nb_pos_P": nb_pos_P,
            "ratio_P": ratio_P,
            "amp_h": amp_h,
        },
    )

    prev_metric = DailyMetric.objects.filter(
        symbol=symbol,
        scenario=scenario,
        date__lt=trading_date,
        K1__isnull=False,
        K2__isnull=False,
        K3__isnull=False,
        K4__isnull=False,
    ).order_by("-date").first()
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
    cross(prev_metric.K1f, metric.K1f, "A1f", "B1f")
    cross(prev_metric.K2, metric.K2, "C1", "D1")
    cross(prev_metric.K3, metric.K3, "E1", "F1")
    cross(prev_metric.K4, metric.K4, "G1", "H1")

    # K2f alerts (A2f/B2f) based on K1 crossing the K2f floating line
    try:
        prev_k1 = D(prev_metric.K1)
        prev_k2f = D(getattr(prev_metric, "K2f", None))
        cur_k1 = D(metric.K1)
        cur_k2f = D(getattr(metric, "K2f", None))

        cross_up = (
            prev_k1 is not None and prev_k2f is not None and cur_k1 is not None and cur_k2f is not None and
            (prev_k1 < prev_k2f) and (cur_k1 > cur_k2f)
        )
        cross_down = (
            prev_k1 is not None and prev_k2f is not None and cur_k1 is not None and cur_k2f is not None and
            (prev_k1 > prev_k2f) and (cur_k1 < cur_k2f)
        )

        if cross_up and (diff_slope is not None) and (D(diff_slope) > 0):
            alerts.append("A2f")
        if cross_down or ((diff_slope is not None) and (D(diff_slope) < 0)):
            alerts.append("B2f")
    except Exception:
        pass

    if alerts:
        alert_obj, _ = Alert.objects.update_or_create(symbol=symbol, scenario=scenario, date=trading_date, defaults={"alerts": ",".join(alerts)})
        return metric, alert_obj

    Alert.objects.filter(symbol=symbol, scenario=scenario, date=trading_date).delete()
    return metric, None