"""Fast bulk computations for DailyMetric/Alert.

This module is **additive**.

Goal: keep the exact same business logic / formulas as
`core.services.calculations.compute_for_symbol_scenario`, but compute in-memory
for a whole symbol and write results with bulk_create. This removes millions of
small DB queries/updates during a full recompute (e.g., S&P500).

IMPORTANT:
- This is used only in full-recompute code paths where we delete existing
  DailyMetric/Alert rows for (symbol, scenario) before writing new ones.
- For incremental (diff) recompute, the legacy per-day function remains used.
"""

from __future__ import annotations

from collections import deque
from decimal import Decimal, InvalidOperation
from typing import Iterable, List, Tuple

from core.models import DailyMetric, Alert


def D(x) -> Decimal | None:
    if x is None:
        return None
    if isinstance(x, Decimal):
        return x
    try:
        return Decimal(str(x))
    except (InvalidOperation, ValueError):
        return None


def compute_full_for_symbol_scenario(
    *,
    symbol,
    scenario,
    bars: Iterable,
    batch_size: int = 5000,
) -> Tuple[int, int]:
    """Compute full DailyMetric + Alert for one symbol/scenario.

    bars must be iterable of objects/dicts providing:
      date, open, high, low, close
    ordered ascending by date.

    Returns (metrics_written, alerts_written).
    """

    # --- scenario constants ---
    a = D(scenario.a)
    b = D(scenario.b)
    c = D(scenario.c)
    d = D(scenario.d)
    e = D(scenario.e)

    denom = (a + b + c + d) if (a is not None and b is not None and c is not None and d is not None) else None
    if denom in (None, 0):
        return 0, 0

    n1 = int(getattr(scenario, "n1", 0) or 0)
    n2 = int(getattr(scenario, "n2", 0) or 0)
    n3 = int(getattr(scenario, "n3", 0) or 0)
    n4 = int(getattr(scenario, "n4", 0) or 0)

    # --- K2f floating line parameters (additive) ---
    n5 = int(getattr(scenario, "n5", 100) or 100)
    k2j = int(getattr(scenario, "k2j", 10) or 10)
    cr = D(getattr(scenario, "cr", D("10")))

    # --- Kf3 floating line parameters (V7.x, additive) ---
    n5f3 = int(getattr(scenario, "n5f3", 100) or 100)
    crf3 = D(getattr(scenario, "crf3", D("10")))
    nampL3 = int(getattr(scenario, "nampL3", 100) or 100)
    baseL3 = D(getattr(scenario, "baseL3", D("0.02")))
    periodeL3 = int(getattr(scenario, "periodeL3", 100) or 100)

    # --- SUM_SLOPE / SPa-SPv ---
    npente = int(getattr(scenario, "npente", 100) or 100)
    slope_threshold = D(getattr(scenario, "slope_threshold", D("0.1")))

    # --- V line parameters (V5.2.37) ---
    m_v = int(getattr(scenario, "m_v", 20) or 20)
    m1_v = max(1, int(m_v / 2))

    vc = D(getattr(scenario, "vc", None))
    if vc is None:
        vc = D("0.5")
    fl = D(getattr(scenario, "fl", None))
    if fl is None:
        fl = D("0.5")

    # Rolling windows matching the legacy logic
    prior_P = deque(maxlen=n1 if n1 > 0 else 1)
    prior_M = deque(maxlen=n2 if n2 > 0 else 1)
    prior_X = deque(maxlen=n2 if n2 > 0 else 1)

    closes_for_slope = deque(maxlen=(n3 + 1) if n3 and n3 > 0 else 1)
    sum_slope_window = deque(maxlen=(npente + 1) if npente and npente > 0 else 1)
    slopes_for_ratio = deque(maxlen=n4 if n4 and n4 > 0 else 1)

    # K2f rolling windows
    var_for_k2f = deque(maxlen=n5 if n5 and n5 > 0 else 1)  # daily_variation ratios (not %)
    pre_for_k2f = deque(maxlen=k2j if k2j and k2j > 0 else 1)  # K2f_pre values
    n5_half = max(1, (n5 // 2)) if n5 and n5 > 0 else 1
    p_for_k2f = deque(maxlen=(n5 + n5_half - 1) if n5 and n5 > 0 else 1)  # store P for Mf1/Xf1 windows

    # Kf3 rolling windows
    n5f3_half = max(1, (n5f3 // 2)) if n5f3 and n5f3 > 0 else 1
    p_for_kf3 = deque(maxlen=(n5f3 + n5f3_half) if n5f3 and n5f3 > 0 else 1)  # newest -> oldest
    deltas_for_kf3 = deque(maxlen=5000)  # signed deltas (ratio)
    abs_for_kf3 = deque(maxlen=5000)  # abs(deltas)

    # V line rolling windows
    highs_for_v = deque(maxlen=m_v if m_v and m_v > 0 else 1)  # daily highs
    vpre_for_v = deque(maxlen=m1_v if m1_v and m1_v > 0 else 1)  # V_pre values

    prev_close = None
    prev_P = None
    prev_high = None
    prev_vline = None
    prev_k = None  # (P,M1,X1,Q,S,K1,K1f,K2f,Kf2bis,Kf3) for alert crossing
    prev_sum_slope = None
    prev_kf3 = None

    metrics: List[DailyMetric] = []
    alerts: List[Alert] = []

    def _bar_get(bar, key):
        # support dicts or model instances
        if isinstance(bar, dict):
            return bar.get(key)
        return getattr(bar, key)

    for bar in bars:
        trading_date = _bar_get(bar, "date")
        F = D(_bar_get(bar, "close"))
        H = D(_bar_get(bar, "high"))
        L = D(_bar_get(bar, "low"))
        O = D(_bar_get(bar, "open"))

        if F is None or H is None or L is None or O is None:
            # Keep alignment: still record nothing for this day
            continue

        P = (a * F + b * H + c * L + d * O) / denom

        # Keep P history for K2f Mf1/Xf1 computation (newest first)
        p_for_k2f.appendleft(P)

        # Keep P history for Kf3 (newest first)
        p_for_kf3.appendleft(P)

        # --- K2f daily variation based on P (study price) ---
        daily_var = None
        if prev_P is not None and prev_P != 0:
            daily_var = (P - prev_P) / prev_P
        prev_P = P
        if daily_var is not None and n5 and n5 > 0:
            var_for_k2f.appendleft(daily_var)

        # Kf3 deltas history (signed + abs)
        if daily_var is not None:
            deltas_for_kf3.appendleft(daily_var)
            abs_for_kf3.appendleft(abs(daily_var))

        # Defaults
        M = X = M1 = X1 = T = Q = S = None
        K1 = K1f = K2f_pre = K2f = Kf2bis = K2 = K3 = K4 = None
        Kf3 = None
        diff_slope = None
        V = slope_P = sum_slope = sum_pos_P = nb_pos_P = ratio_P = amp_h = None

        # --- V line (V5.2.37) ---
        V_pre_line = None
        V_line_line = None
        try:
            if m_v and m_v > 1:
                highs_for_v.appendleft(H)
                if len(highs_for_v) >= m_v:
                    V_pre_line = max(highs_for_v)
                    vpre_for_v.appendleft(V_pre_line)
                    if len(vpre_for_v) >= m1_v:
                        V_line_line = sum(list(vpre_for_v)[:m1_v]) / D(m1_v)
        except Exception:
            V_pre_line = None
            V_line_line = None

        # Need n1 prior P values to compute M/X
        if n1 > 0 and len(prior_P) >= n1:
            M = max(prior_P)
            X = min(prior_P)

            # prior_M / prior_X accumulate once M/X are available
            if n2 > 0:
                prior_M.appendleft(M)
                prior_X.appendleft(X)

                if len(prior_M) >= n2 and len(prior_X) >= n2:
                    M1 = sum(prior_M) / Decimal(len(prior_M))
                    X1 = sum(prior_X) / Decimal(len(prior_X))

        # V: % change *100 (matches legacy)
        if prev_close is not None and prev_close != 0:
            V = (F - prev_close) * D(100) / prev_close
        prev_close = F

        # slope_P: average of last n3 daily % changes (computed from closes window)
        if n3 and n3 > 0:
            closes_for_slope.appendleft(F)
            if len(closes_for_slope) >= (n3 + 1):
                # Build vs for window (latest->older), exactly like legacy loop
                vs = []
                ok = True
                for i in range(n3):
                    c2 = closes_for_slope[i]
                    c1 = closes_for_slope[i + 1]
                    if c1 is None or c1 == 0:
                        ok = False
                        break
                    vs.append((c2 - c1) * D(100) / c1)
                if ok and len(vs) == n3:
                    slope_P = sum(vs) / D(n3)

        # sum_slope: SUM((P(t)-P(t-1))/P(t-1)) over Npente days
        if npente and npente > 0 and P is not None:
            sum_slope_window.appendleft(P)
            if len(sum_slope_window) >= 2:
                vals = []
                max_i = min(npente, len(sum_slope_window) - 1)
                for i in range(max_i):
                    p1 = D(sum_slope_window[i])
                    p0 = D(sum_slope_window[i + 1])
                    if p0 not in (None, 0) and p1 is not None:
                        vals.append((p1 - p0) / p0)
                if vals:
                    sum_slope = sum(vals)

        # ratio_P, amp_h depend on last n4 slopes (including current day if any)
        if n4 and n4 > 0 and n3 and n3 > 0 and slope_P is not None:
            slopes_for_ratio.appendleft(slope_P)
            if len(slopes_for_ratio) >= n4:
                positives = [D(x) for x in slopes_for_ratio if D(x) is not None and D(x) > 0]
                nb_pos_P = len(positives)
                sum_pos_P = sum(positives) if positives else D(0)
                ratio_P = D(nb_pos_P) * D(100) / D(n4)
                if nb_pos_P > 0:
                    amp_h = (sum_pos_P * D(100)) / (D(nb_pos_P) * D(n3))

        # Core indicators need M1/X1 and e
        if M1 is not None and X1 is not None and e not in (None, 0):
            T = (M1 - X1) / e
            Q = M1 - T
            S = M1 + T

            K1 = P - M1
            K2 = P - X1
            K3 = P - Q
            K4 = P - S

            # K1f correction
            ratio_p = (D(ratio_P) / D(100)) if ratio_P is not None else None
            E = (M1 - X1)
            Ccorr = D(0)
            if ratio_p is not None and E is not None and vc is not None:
                Ccorr = (vc - ratio_p) * fl * E
            K1f = (K1 + Ccorr) if K1 is not None else None

            # --- K2f floating line ---
            # Requires: e != 0, N5 daily variations of P, and enough P history to compute Mf1/Xf1.
            if cr is None:
                cr = D("10")
            if n5 and n5 > 0 and len(var_for_k2f) >= n5 and e not in (None, 0) and cr is not None and len(p_for_k2f) >= (n5 + n5_half - 1):
                v_list = list(var_for_k2f)
                v_list = v_list[:n5]
                slope1 = sum(v_list) * D(100)
                slope2 = sum(v_list[:n5_half]) * D(100) if len(v_list) >= n5_half else None
                if slope2 is not None:
                    diff_slope = slope2 - slope1

                slope_deg = slope1 / D(90) if D(90) != 0 else None
                if slope_deg is not None:
                    # Mf1/Xf1: average over last n5_half days of rolling max/min of P over N5 days
                    p_list = list(p_for_k2f)  # newest -> oldest
                    max_list = []
                    min_list = []
                    for i in range(n5_half):
                        window = p_list[i : i + n5]
                        max_list.append(max(window))
                        min_list.append(min(window))
                    Mf1 = sum(max_list) / D(len(max_list))
                    Xf1 = sum(min_list) / D(len(min_list))
                    Ef = Mf1 - Xf1

                    # Kf2bis(t) = Mf1(t) - Ef(t) * p(t), with p = SUM(delta_j over N2 days)
                    if n2 and n2 > 0 and len(v_list) >= n2:
                        p_sum = sum(v_list[:n2])
                        Kf2bis = Mf1 - (Ef * p_sum)

                    # FC(t) = slope_deg(t) × CR × Ef(t) / e
                    FC = slope_deg * cr * (Ef / e)

                    # K2f_pre(t) = Mf1(t) - FC(t)
                    K2f_pre = Mf1 - FC

                    if k2j and k2j > 0:
                        pre_for_k2f.appendleft(K2f_pre)
                        if len(pre_for_k2f) >= k2j:
                            K2f = sum(list(pre_for_k2f)[:k2j]) / D(k2j)

            # --- Kf3 floating line (V7.x) ---
            try:
                if crf3 is None:
                    crf3 = D("10")
                n5f3_eff = max(1, int(n5f3 or 1))
                n5f3_half_eff = max(1, int(n5f3_eff // 2))
                namp_eff = max(1, int(nampL3 or 1))
                periode_nom = max(1, int(periodeL3 or 1))

                # amp from last namp_eff abs deltas (newest first)
                abs_list = list(abs_for_kf3)
                abs_slice = abs_list[:namp_eff] if len(abs_list) >= namp_eff else abs_list
                amp = (sum(abs_slice) / D(len(abs_slice))) if abs_slice else None

                if amp is None or amp <= 0 or baseL3 is None or baseL3 <= 0:
                    periode_dyn = D(periode_nom)
                else:
                    k = amp / baseL3
                    periode_dyn = (D(periode_nom) / k) if k not in (None, 0) else D(periode_nom)

                try:
                    periode_int = int(Decimal(periode_dyn).to_integral_value(rounding="ROUND_HALF_UP"))
                except Exception:
                    try:
                        periode_int = int(round(float(periode_dyn)))
                    except Exception:
                        periode_int = periode_nom
                periode_int = max(1, min(5000, int(periode_int)))

                # slope_deg mean of signed deltas over periode_int (newest first)
                d_list = list(deltas_for_kf3)
                d_slice = d_list[:periode_int] if len(d_list) >= periode_int else d_list
                slope_deg_f3 = (sum(d_slice) / D(len(d_slice))) if d_slice else None

                # Mf1/Xf1 windows using newest-first P list
                if slope_deg_f3 is not None and len(p_for_kf3) >= (n5f3_eff + n5f3_half_eff) and e not in (None, 0):
                    p_list = list(p_for_kf3)
                    max_list = []
                    min_list = []
                    for i in range(n5f3_half_eff):
                        window = p_list[i : i + n5f3_eff]
                        if len(window) != n5f3_eff:
                            break
                        max_list.append(max(window))
                        min_list.append(min(window))
                    if len(max_list) == n5f3_half_eff and len(min_list) == n5f3_half_eff:
                        Mf1_f3 = sum(max_list) / D(len(max_list))
                        Xf1_f3 = sum(min_list) / D(len(min_list))
                        Ef_f3 = Mf1_f3 - Xf1_f3
                        FC_f3 = slope_deg_f3 * crf3 * (Ef_f3 / e)
                        Kf3 = Mf1_f3 - FC_f3
            except Exception:
                Kf3 = None

        # Persist metric
        metrics.append(
            DailyMetric(
                symbol=symbol,
                scenario=scenario,
                date=trading_date,
                P=P,
                M=M,
                M1=M1,
                X=X,
                X1=X1,
                T=T,
                Q=Q,
                S=S,
                K1=K1,
                K1f=K1f,
                K2f=K2f,
                K2f_pre=K2f_pre,
                Kf2bis=Kf2bis,
                Kf3=Kf3,
                V_pre=V_pre_line,
                V_line=V_line_line,
                K2=K2,
                K3=K3,
                K4=K4,
                V=V,
                slope_P=slope_P,
                sum_slope=sum_slope,
                sum_pos_P=sum_pos_P,
                nb_pos_P=nb_pos_P,
                ratio_P=ratio_P,
                amp_h=amp_h,
            )
        )

        # Alerts (crossing)
        current_alerts = []

        # Signals are defined as strict crossings around 0 for the indicator series.
        # For indicators defined as K = P - Line, this is equivalent to P crossing that Line.
        if prev_k is not None and P is not None:
            prev_P, prev_M1, prev_X1, prev_Q, prev_S, prev_K1, prev_K1f, prev_K2f, prev_Kf2bis, prev_Kf3 = prev_k

            def cross0(prev_x, cur_x, pos_code, neg_code):
                prev_x = D(prev_x)
                cur_x = D(cur_x)
                if prev_x is None or cur_x is None:
                    return
                if (prev_x < 0) and (cur_x > 0):
                    current_alerts.append(pos_code)
                elif (prev_x > 0) and (cur_x < 0):
                    current_alerts.append(neg_code)

            # A1/B1 : K1 crosses 0
            cross0(prev_K1, K1, "A1", "B1")

            # A1f/B1f : K1f crosses 0
            cross0(prev_K1f, K1f, "A1f", "B1f")

            # C1/D1 : K2 crosses 0
            cross0(prev_P - prev_X1 if (prev_P is not None and prev_X1 is not None) else None, K2, "C1", "D1")

            # E1/F1 : K3 crosses 0
            cross0(prev_P - prev_Q if (prev_P is not None and prev_Q is not None) else None, K3, "E1", "F1")

            # G1/H1 : K4 crosses 0
            cross0(prev_P - prev_S if (prev_P is not None and prev_S is not None) else None, K4, "G1", "H1")

            # A2f/B2f : P crosses the K2f price line, without diff_slope condition
            try:
                prev_p = D(prev_P)
                cur_p = D(P)
                prev_k2f = D(prev_K2f)
                cur_k2f = D(K2f)

                cross_up = (
                    prev_p is not None and cur_p is not None and prev_k2f is not None and cur_k2f is not None
                    and (prev_p < prev_k2f) and (cur_p > cur_k2f)
                )
                cross_down = (
                    prev_p is not None and cur_p is not None and prev_k2f is not None and cur_k2f is not None
                    and (prev_p > prev_k2f) and (cur_p < cur_k2f)
                )
                if cross_up:
                    current_alerts.append("A2f")
                if cross_down:
                    current_alerts.append("B2f")
            except Exception:
                pass

            # A2bis/B2bis : P crosses the Kf2bis price line
            try:
                prev_p = D(prev_P)
                cur_p = D(P)
                prev_kf2bis = D(prev_Kf2bis)
                cur_kf2bis = D(Kf2bis)
                cross_up = (
                    prev_p is not None and cur_p is not None and prev_kf2bis is not None and cur_kf2bis is not None
                    and (prev_p < prev_kf2bis) and (cur_p > cur_kf2bis)
                )
                cross_down = (
                    prev_p is not None and cur_p is not None and prev_kf2bis is not None and cur_kf2bis is not None
                    and (prev_p > prev_kf2bis) and (cur_p < cur_kf2bis)
                )
                if cross_up:
                    current_alerts.append("A2bis")
                if cross_down:
                    current_alerts.append("B2bis")
            except Exception:
                pass

            # AF3/BF3 : P crosses the Kf3 price line
            try:
                prev_p = D(prev_P)
                cur_p = D(P)
                prev_kf3 = D(prev_Kf3)
                cur_kf3 = D(Kf3)
                cross_up = (
                    prev_p is not None and cur_p is not None and prev_kf3 is not None and cur_kf3 is not None
                    and (prev_p < prev_kf3) and (cur_p > cur_kf3)
                )
                cross_down = (
                    prev_p is not None and cur_p is not None and prev_kf3 is not None and cur_kf3 is not None
                    and (prev_p > prev_kf3) and (cur_p < cur_kf3)
                )
                if cross_up:
                    current_alerts.append("AF3")
                if cross_down:
                    current_alerts.append("BF3")
            except Exception:
                pass

        # SUM_SLOPE alerts (SPa/SPv)
        try:
            if prev_sum_slope is not None and sum_slope is not None and slope_threshold is not None:
                if (prev_sum_slope < slope_threshold) and (sum_slope > slope_threshold):
                    current_alerts.append("SPa")
                elif (prev_sum_slope > slope_threshold) and (sum_slope < slope_threshold):
                    current_alerts.append("SPv")
        except Exception:
            pass

        # V line alerts (I1/J1): High crosses V_line
        try:
            if prev_high is not None and prev_vline is not None and H is not None and V_line_line is not None:
                if (prev_high < prev_vline) and (H > V_line_line):
                    current_alerts.append("I1")
                elif (prev_high > prev_vline) and (H < V_line_line):
                    current_alerts.append("J1")
        except Exception:
            pass

        # Persist alerts for the day (if any)
        if current_alerts:
            alerts.append(
                Alert(
                    symbol=symbol,
                    scenario=scenario,
                    date=trading_date,
                    alerts=",".join(current_alerts),
                )
            )

        # Keep what we need for next-day crossings.
        if P is not None and M1 is not None and X1 is not None and Q is not None and S is not None:
            prev_k = (P, M1, X1, Q, S, K1, K1f, K2f, Kf2bis, Kf3)
        prev_sum_slope = sum_slope

        # Update V line previous values
        if H is not None:
            prev_high = H
        if V_line_line is not None:
            prev_vline = V_line_line

        # Update prior windows at end of day (matches legacy: prior metrics are strictly < trading_date)
        if n1 and n1 > 0:
            prior_P.appendleft(P)

    # Bulk write
    if metrics:
        DailyMetric.objects.bulk_create(metrics, batch_size=batch_size)
    if alerts:
        Alert.objects.bulk_create(alerts, batch_size=batch_size)

    return len(metrics), len(alerts)
