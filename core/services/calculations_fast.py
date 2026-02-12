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
    slopes_for_ratio = deque(maxlen=n4 if n4 and n4 > 0 else 1)

    # K2f rolling windows
    var_for_k2f = deque(maxlen=n5 if n5 and n5 > 0 else 1)  # daily_variation ratios (not %)
    pre_for_k2f = deque(maxlen=k2j if k2j and k2j > 0 else 1)  # K2f_pre values

    prev_close = None
    prev_P = None
    prev_k = None  # (K1,K2f,K1f,K2,K3,K4) for alert crossing

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

        # --- K2f daily variation based on P (study price) ---
        daily_var = None
        if prev_P is not None and prev_P != 0:
            daily_var = (P - prev_P) / prev_P
        prev_P = P
        if daily_var is not None and n5 and n5 > 0:
            var_for_k2f.appendleft(daily_var)

        # Defaults
        M = X = M1 = X1 = T = Q = S = None
        K1 = K1f = K2f_pre = K2f = K2 = K3 = K4 = None
        diff_slope = None
        V = slope_P = sum_pos_P = nb_pos_P = ratio_P = amp_h = None

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
            # Requires: K1 available, e != 0, and N5 daily variations of P.
            if cr is None:
                cr = D("10")
            if n5 and n5 > 0 and len(var_for_k2f) >= n5 and K1 is not None and e not in (None, 0) and cr is not None:
                v_list = list(var_for_k2f)
                v_list = v_list[:n5]
                slope1 = sum(v_list) * D(100)
                n5_half = max(1, n5 // 2)
                slope2 = sum(v_list[:n5_half]) * D(100) if len(v_list) >= n5_half else None
                if slope2 is not None:
                    diff_slope = slope2 - slope1

                slope_deg = slope1 / D(90) if D(90) != 0 else None
                if slope_deg is not None:
                    FC = slope_deg * e * cr
                    K2f_pre = K1 - FC

                    if k2j and k2j > 0:
                        pre_for_k2f.appendleft(K2f_pre)
                        if len(pre_for_k2f) >= k2j:
                            K2f = sum(list(pre_for_k2f)[:k2j]) / D(k2j)

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
                K2=K2,
                K3=K3,
                K4=K4,
                V=V,
                slope_P=slope_P,
                sum_pos_P=sum_pos_P,
                nb_pos_P=nb_pos_P,
                ratio_P=ratio_P,
                amp_h=amp_h,
            )
        )

        # Alerts (crossing) only when previous had full K values
        if prev_k is not None and K1 is not None and K2 is not None and K3 is not None and K4 is not None:
            prev_K1, prev_K2f, prev_K1f, prev_K2, prev_K3, prev_K4 = prev_k
            current_alerts = []

            def cross(prev, cur, pos_code, neg_code):
                if prev is None or cur is None:
                    return
                if prev < 0 and cur > 0:
                    current_alerts.append(pos_code)
                elif prev > 0 and cur < 0:
                    current_alerts.append(neg_code)

            cross(prev_K1, K1, "A1", "B1")
            cross(prev_K1f, K1f, "A1f", "B1f")
            cross(prev_K2, K2, "C1", "D1")
            cross(prev_K3, K3, "E1", "F1")
            cross(prev_K4, K4, "G1", "H1")

            # K2f alerts (A2f/B2f): K1 crosses K2f with slope filter / fast-sell rule
            try:
                if prev_K2f is not None and K2f is not None:
                    cross_up = (prev_K1 < prev_K2f) and (K1 > K2f)
                    cross_down = (prev_K1 > prev_K2f) and (K1 < K2f)
                else:
                    cross_up = False
                    cross_down = False

                if cross_up and (diff_slope is not None) and (D(diff_slope) > 0):
                    current_alerts.append("A2f")
                if cross_down or ((diff_slope is not None) and (D(diff_slope) < 0)):
                    current_alerts.append("B2f")
            except Exception:
                pass

            if current_alerts:
                alerts.append(
                    Alert(
                        symbol=symbol,
                        scenario=scenario,
                        date=trading_date,
                        alerts=",".join(current_alerts),
                    )
                )

        if K1 is not None and K2 is not None and K3 is not None and K4 is not None:
            prev_k = (K1, K2f, K1f, K2, K3, K4)

        # Update prior windows at end of day (matches legacy: prior metrics are strictly < trading_date)
        if n1 and n1 > 0:
            prior_P.appendleft(P)

    # Bulk write
    if metrics:
        DailyMetric.objects.bulk_create(metrics, batch_size=batch_size)
    if alerts:
        Alert.objects.bulk_create(alerts, batch_size=batch_size)

    return len(metrics), len(alerts)
