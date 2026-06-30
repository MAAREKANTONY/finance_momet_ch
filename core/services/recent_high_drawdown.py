from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal, InvalidOperation
from typing import Any

RHD_OK_MODE_CLASSIC = "classic"
RHD_OK_MODE_REBOUND_CONFIRMED = "rebound_confirmed"


def to_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def normalize_recent_high_drawdown_params(obj: Any) -> tuple[int | None, Decimal | None]:
    lookback = getattr(obj, "recent_high_drawdown_lookback_days", None)
    max_drop = getattr(obj, "recent_high_drawdown_max_drop_pct", None)
    try:
        lookback_value = int(lookback) if lookback not in (None, "") else None
    except (TypeError, ValueError):
        lookback_value = None
    max_drop_value = to_decimal(max_drop)
    if lookback_value is None or lookback_value <= 0 or max_drop_value is None:
        return None, None
    return lookback_value, max_drop_value


def normalize_rhd_ok_reactivation_params(obj: Any) -> dict[str, Any]:
    mode = str(getattr(obj, "rhd_ok_reactivation_mode", RHD_OK_MODE_CLASSIC) or RHD_OK_MODE_CLASSIC).strip()
    if mode not in {RHD_OK_MODE_CLASSIC, RHD_OK_MODE_REBOUND_CONFIRMED}:
        mode = RHD_OK_MODE_CLASSIC
    rebound_threshold = to_decimal(getattr(obj, "rhd_ok_rebound_threshold", Decimal("0.08")))
    if rebound_threshold is None or rebound_threshold < 0:
        rebound_threshold = Decimal("0.08")
    reentry_max_drawdown = to_decimal(getattr(obj, "rhd_ok_reentry_max_drawdown", Decimal("0.40")))
    if reentry_max_drawdown is None or reentry_max_drawdown < 0:
        reentry_max_drawdown = Decimal("0.40")
    try:
        confirmation_days = int(getattr(obj, "rhd_ok_confirmation_days", 2) or 2)
    except (TypeError, ValueError):
        confirmation_days = 2
    if confirmation_days <= 0:
        confirmation_days = 1
    return {
        "mode": mode,
        "rebound_threshold": rebound_threshold,
        "confirmation_days": confirmation_days,
        "reentry_max_drawdown": reentry_max_drawdown,
    }


def recent_high_drawdown_enabled(obj: Any) -> bool:
    lookback, max_drop = normalize_recent_high_drawdown_params(obj)
    return lookback is not None and max_drop is not None


def compute_recent_high_drawdown_condition(
    *,
    previous_prices: Iterable[Any],
    current_price: Any,
    lookback_days: int | None,
    max_drop_pct: Decimal | None,
) -> dict[str, Any]:
    return _compute_recent_high_drawdown_condition(
        previous_prices=previous_prices,
        current_price=current_price,
        lookback_days=lookback_days,
        max_drop_pct=max_drop_pct,
        require_full_lookback=True,
    )


def _compute_recent_high_drawdown_condition(
    *,
    previous_prices: Iterable[Any],
    current_price: Any,
    lookback_days: int | None,
    max_drop_pct: Decimal | None,
    require_full_lookback: bool,
) -> dict[str, Any]:
    current = to_decimal(current_price)
    if lookback_days is None or lookback_days <= 0 or max_drop_pct is None:
        return {
            "enabled": False,
            "passed": False,
            "sufficient_history": False,
            "recent_high": None,
            "threshold_price": None,
        }

    previous = [to_decimal(value) for value in previous_prices]
    previous = [value for value in previous if value is not None]
    min_history = lookback_days if require_full_lookback else 1
    if current is None or len(previous) < min_history:
        return {
            "enabled": True,
            "passed": False,
            "sufficient_history": False,
            "recent_high": None,
            "threshold_price": None,
        }

    window = previous[-lookback_days:]
    recent_high = max(window)
    threshold_price = recent_high * (Decimal("1") + max_drop_pct)
    return {
        "enabled": True,
        "passed": current >= threshold_price,
        "sufficient_history": True,
        "recent_high": recent_high,
        "threshold_price": threshold_price,
    }


class RecentHighDrawdownAlertState:
    def __init__(
        self,
        *,
        lookback_days: int | None,
        max_drop_pct: Decimal | None,
        mode: str = RHD_OK_MODE_CLASSIC,
        rebound_threshold: Decimal | None = None,
        confirmation_days: int = 2,
        reentry_max_drawdown: Decimal | None = None,
    ) -> None:
        self.lookback_days = lookback_days
        self.max_drop_pct = max_drop_pct
        self.mode = mode if mode == RHD_OK_MODE_REBOUND_CONFIRMED else RHD_OK_MODE_CLASSIC
        self.rebound_threshold = rebound_threshold if rebound_threshold is not None else Decimal("0.08")
        self.confirmation_days = max(1, int(confirmation_days or 1))
        self.reentry_max_drawdown = reentry_max_drawdown if reentry_max_drawdown is not None else Decimal("0.40")
        self.prices: list[Decimal | None] = []
        self.prev_rhd_passed = False
        self.state = "OK"
        self.fail_reference_high: Decimal | None = None
        self.low_since_rhd_fail: Decimal | None = None
        self.confirmation_count = 0
        self.reset_index: int | None = None

    def process(self, current_price: Any) -> list[str]:
        if self.mode == RHD_OK_MODE_REBOUND_CONFIRMED:
            alerts = self._process_rebound_confirmed(current_price)
        else:
            alerts = self._process_classic(current_price)
        self.prices.append(to_decimal(current_price))
        return alerts

    def _process_classic(self, current_price: Any) -> list[str]:
        current_rhd = compute_recent_high_drawdown_condition(
            previous_prices=self.prices,
            current_price=current_price,
            lookback_days=self.lookback_days,
            max_drop_pct=self.max_drop_pct,
        )
        alerts: list[str] = []
        if current_rhd["enabled"]:
            if (not self.prev_rhd_passed) and current_rhd["passed"]:
                alerts.append("RHD_OK")
            elif self.prev_rhd_passed and (not current_rhd["passed"]):
                alerts.append("RHD_FAIL")
            self.prev_rhd_passed = bool(current_rhd["passed"])
        return alerts

    def _process_rebound_confirmed(self, current_price: Any) -> list[str]:
        current = to_decimal(current_price)
        if self.state == "FAILED":
            return self._process_failed(current)

        previous = self.prices
        require_full = True
        if self.reset_index is not None:
            previous = self.prices[self.reset_index :]
            require_full = False
        current_rhd = _compute_recent_high_drawdown_condition(
            previous_prices=previous,
            current_price=current,
            lookback_days=self.lookback_days,
            max_drop_pct=self.max_drop_pct,
            require_full_lookback=require_full,
        )
        alerts: list[str] = []
        if not current_rhd["enabled"]:
            return alerts
        if (not self.prev_rhd_passed) and current_rhd["passed"]:
            alerts.append("RHD_OK")
        elif self.prev_rhd_passed and (not current_rhd["passed"]):
            alerts.append("RHD_FAIL")
            self.state = "FAILED"
            self.fail_reference_high = current_rhd.get("recent_high")
            self.low_since_rhd_fail = current
            self.confirmation_count = 0
            self.prev_rhd_passed = False
            return alerts
        self.prev_rhd_passed = bool(current_rhd["passed"])
        return alerts

    def _process_failed(self, current: Decimal | None) -> list[str]:
        if current is None or current <= 0 or self.fail_reference_high in (None, 0):
            self.confirmation_count = 0
            return []
        if self.low_since_rhd_fail is None or current < self.low_since_rhd_fail:
            self.low_since_rhd_fail = current
        if self.low_since_rhd_fail in (None, 0):
            self.confirmation_count = 0
            return []

        rebound = (current / self.low_since_rhd_fail) - Decimal("1")
        drawdown = Decimal("1") - (current / self.fail_reference_high)
        condition_ok = rebound >= self.rebound_threshold and drawdown <= self.reentry_max_drawdown
        if condition_ok:
            self.confirmation_count += 1
        else:
            self.confirmation_count = 0
        if self.confirmation_count < self.confirmation_days:
            return []

        self.state = "OK"
        self.prev_rhd_passed = True
        self.fail_reference_high = None
        self.low_since_rhd_fail = None
        self.confirmation_count = 0
        self.reset_index = len(self.prices)
        return ["RHD_OK"]


def compute_recent_high_drawdown_alerts_for_series(
    prices: Iterable[Any],
    *,
    lookback_days: int | None,
    max_drop_pct: Decimal | None,
    mode: str = RHD_OK_MODE_CLASSIC,
    rebound_threshold: Decimal | None = None,
    confirmation_days: int = 2,
    reentry_max_drawdown: Decimal | None = None,
) -> list[list[str]]:
    state = RecentHighDrawdownAlertState(
        lookback_days=lookback_days,
        max_drop_pct=max_drop_pct,
        mode=mode,
        rebound_threshold=rebound_threshold,
        confirmation_days=confirmation_days,
        reentry_max_drawdown=reentry_max_drawdown,
    )
    return [state.process(price) for price in prices]
