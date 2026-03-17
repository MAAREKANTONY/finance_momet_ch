"""Fast bulk computations for DailyMetric/Alert.

Optimized full recompute path aligned with the cleaned indicator set:
P, M/X, M1/X1, T, Q/S, K1..K4, Kf, SUM_SLOPE and alerts.
"""

from __future__ import annotations

from collections import deque
from decimal import Decimal, InvalidOperation
from typing import Iterable, List, Tuple

from core.models import Alert, DailyMetric


def D(x) -> Decimal | None:
    if x is None:
        return None
    if isinstance(x, Decimal):
        return x
    try:
        return Decimal(str(x))
    except (InvalidOperation, ValueError):
        return None


def compute_full_for_symbol_scenario(*, symbol, scenario, bars: Iterable, batch_size: int = 5000) -> Tuple[int, int]:
    a = D(scenario.a)
    b = D(scenario.b)
    c = D(scenario.c)
    d = D(scenario.d)
    e = D(scenario.e)
    denom = (a + b + c + d) if None not in (a, b, c, d) else None
    if denom in (None, 0):
        return 0, 0

    n1 = int(getattr(scenario, "n1", 0) or 0)
    n2 = int(getattr(scenario, "n2", 0) or 0)
    npente = int(getattr(scenario, "npente", 100) or 100)
    slope_threshold = D(getattr(scenario, "slope_threshold", D("0.1")))

    prior_P = deque(maxlen=max(1, n1))
    prior_M = deque(maxlen=max(1, n2))
    prior_X = deque(maxlen=max(1, n2))
    p_window = deque(maxlen=max(1, max(n2, npente) + 1))

    prev_alert_tuple = None  # (P, Q, S, K1, K2, K3, K4, Kf)
    prev_sum_slope = None

    metrics: List[DailyMetric] = []
    alerts: List[Alert] = []

    def _bar_get(bar, key):
        return bar.get(key) if isinstance(bar, dict) else getattr(bar, key)

    for bar in bars:
        trading_date = _bar_get(bar, "date")
        F = D(_bar_get(bar, "close"))
        H = D(_bar_get(bar, "high"))
        L = D(_bar_get(bar, "low"))
        O = D(_bar_get(bar, "open"))
        if None in (F, H, L, O):
            continue

        P = (a * F + b * H + c * L + d * O) / denom
        p_window.appendleft(P)

        M = X = M1 = X1 = T = Q = S = None
        K1 = K2 = K3 = K4 = Kf = None
        sum_slope = None

        if n1 > 0 and len(prior_P) >= n1:
            M = max(prior_P)
            X = min(prior_P)
            if n2 > 0:
                prior_M.appendleft(M)
                prior_X.appendleft(X)
                if len(prior_M) >= n2 and len(prior_X) >= n2:
                    M1 = sum(prior_M) / Decimal(len(prior_M))
                    X1 = sum(prior_X) / Decimal(len(prior_X))

        if len(p_window) >= 2:
            vals = []
            max_i = min(max(n2, npente), len(p_window) - 1)
            for i in range(max_i):
                p1 = D(p_window[i])
                p0 = D(p_window[i + 1])
                if p0 not in (None, 0) and p1 is not None:
                    vals.append((p1 - p0) / p0)
            if vals and npente > 0:
                sum_slope = sum(vals[:npente])

        if M1 is not None and X1 is not None and e not in (None, 0):
            T = (M1 - X1) / e
            Q = M1 - T
            S = M1 + T
            K1 = P - M1
            K2 = P - X1
            K3 = P - Q
            K4 = P - S

            if n2 > 0 and len(p_window) >= (n2 + 1):
                vals_n2 = []
                for i in range(n2):
                    p1 = D(p_window[i])
                    p0 = D(p_window[i + 1])
                    if p0 in (None, 0) or p1 is None:
                        vals_n2 = []
                        break
                    vals_n2.append((p1 - p0) / p0)
                if len(vals_n2) == n2:
                    Kf = M1 - (T * sum(vals_n2))

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
                K1f=None,
                K2f=None,
                K2f_pre=None,
                Kf2bis=Kf,
                Kf3=None,
                V_pre=None,
                V_line=None,
                K2=K2,
                K3=K3,
                K4=K4,
                V=None,
                slope_P=None,
                sum_slope=sum_slope,
                sum_pos_P=None,
                nb_pos_P=None,
                ratio_P=None,
                amp_h=None,
            )
        )

        day_alerts = []
        if prev_alert_tuple is not None:
            prev_P, prev_Q, prev_S, prev_K1, prev_K2, prev_K3, prev_K4, prev_Kf = prev_alert_tuple

            def cross0(prev_x, cur_x, pos_code, neg_code):
                prev_x = D(prev_x)
                cur_x = D(cur_x)
                if prev_x is None or cur_x is None:
                    return
                if prev_x < 0 and cur_x > 0:
                    day_alerts.append(pos_code)
                elif prev_x > 0 and cur_x < 0:
                    day_alerts.append(neg_code)

            cross0(prev_K1, K1, "A1", "B1")
            cross0(prev_K2, K2, "C1", "D1")
            cross0(prev_K3, K3, "E1", "F1")
            cross0(prev_K4, K4, "G1", "H1")

            prev_p = D(prev_P)
            cur_p = D(P)
            prev_kf = D(prev_Kf)
            cur_kf = D(Kf)
            if None not in (prev_p, cur_p, prev_kf, cur_kf):
                if prev_p < prev_kf and cur_p > cur_kf:
                    day_alerts.append("Af")
                elif prev_p > prev_kf and cur_p < cur_kf:
                    day_alerts.append("Bf")

        if prev_sum_slope is not None and sum_slope is not None and slope_threshold is not None:
            if prev_sum_slope < slope_threshold and sum_slope > slope_threshold:
                day_alerts.append("SPa")
            elif prev_sum_slope > slope_threshold and sum_slope < slope_threshold:
                day_alerts.append("SPv")

        if day_alerts:
            alerts.append(Alert(symbol=symbol, scenario=scenario, date=trading_date, alerts=",".join(day_alerts)))

        if None not in (P, Q, S):
            prev_alert_tuple = (P, Q, S, K1, K2, K3, K4, Kf)
        prev_sum_slope = sum_slope
        if n1 > 0:
            prior_P.appendleft(P)

    if metrics:
        DailyMetric.objects.bulk_create(metrics, batch_size=batch_size)
    if alerts:
        Alert.objects.bulk_create(alerts, batch_size=batch_size)
    return len(metrics), len(alerts)
