from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

from core.utils.numbers import format_decimal_plain


COULOIR_SIGNAL_CODE = "COULOIR"
COULOIR_TRADING_MODEL = "COULOIR_STATEFUL"

DEFAULT_INITIAL_LOW_LOOKBACK_DAYS = 240
DEFAULT_BUY_REBOUND_THRESHOLD = Decimal("0.10")
DEFAULT_SELL_DRAWDOWN_THRESHOLD = Decimal("0.10")
DEFAULT_BUY_CONFIRMATION_DAYS = 2
DEFAULT_SELL_CONFIRMATION_DAYS = 1


def to_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _positive_int(value: Any, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return parsed if parsed >= 1 else default


def _non_negative_decimal(value: Any, default: Decimal) -> Decimal:
    parsed = to_decimal(value)
    if parsed is None or parsed < 0:
        return default
    return parsed


def is_couloir_line(line: dict[str, Any] | None) -> bool:
    if not isinstance(line, dict):
        return False
    trading_model = str(line.get("trading_model") or "").strip().upper()
    mode = str(line.get("mode") or "").strip().lower()
    buy_codes = line.get("buy") or line.get("buy_conditions") or []
    if isinstance(buy_codes, str):
        normalized_buy = {item.strip().upper() for item in buy_codes.split(",") if item.strip()}
    else:
        try:
            normalized_buy = {str(item).strip().upper() for item in buy_codes if str(item).strip()}
        except TypeError:
            normalized_buy = set()
    return (
        trading_model == COULOIR_TRADING_MODEL
        or mode == "couloir"
        or COULOIR_SIGNAL_CODE in normalized_buy
    )


def normalize_couloir_line_config(line: dict[str, Any] | None) -> dict[str, Any]:
    line = line if isinstance(line, dict) else {}
    return {
        "couloir_initial_low_lookback_days": _positive_int(
            line.get("couloir_initial_low_lookback_days"),
            DEFAULT_INITIAL_LOW_LOOKBACK_DAYS,
        ),
        "couloir_buy_rebound_threshold": format_decimal_plain(
            _non_negative_decimal(
                line.get("couloir_buy_rebound_threshold"),
                DEFAULT_BUY_REBOUND_THRESHOLD,
            )
        ),
        "couloir_sell_drawdown_threshold": format_decimal_plain(
            _non_negative_decimal(
                line.get("couloir_sell_drawdown_threshold"),
                DEFAULT_SELL_DRAWDOWN_THRESHOLD,
            )
        ),
        "couloir_buy_confirmation_days": _positive_int(
            line.get("couloir_buy_confirmation_days"),
            DEFAULT_BUY_CONFIRMATION_DAYS,
        ),
        "couloir_sell_confirmation_days": _positive_int(
            line.get("couloir_sell_confirmation_days"),
            DEFAULT_SELL_CONFIRMATION_DAYS,
        ),
    }


@dataclass(frozen=True)
class CouloirConfig:
    initial_low_lookback_days: int
    buy_rebound_threshold: Decimal
    sell_drawdown_threshold: Decimal
    buy_confirmation_days: int
    sell_confirmation_days: int

    @classmethod
    def from_line(cls, line: dict[str, Any] | None) -> "CouloirConfig":
        normalized = normalize_couloir_line_config(line)
        return cls(
            initial_low_lookback_days=int(normalized["couloir_initial_low_lookback_days"]),
            buy_rebound_threshold=to_decimal(normalized["couloir_buy_rebound_threshold"]) or DEFAULT_BUY_REBOUND_THRESHOLD,
            sell_drawdown_threshold=to_decimal(normalized["couloir_sell_drawdown_threshold"]) or DEFAULT_SELL_DRAWDOWN_THRESHOLD,
            buy_confirmation_days=int(normalized["couloir_buy_confirmation_days"]),
            sell_confirmation_days=int(normalized["couloir_sell_confirmation_days"]),
        )


class CouloirState:
    OUT = "OUT"
    IN = "IN"

    def __init__(self, config: CouloirConfig) -> None:
        self.config = config
        self.position_state = self.OUT
        self.low_ref: Decimal | None = None
        self.high_ref: Decimal | None = None
        self.out_price_count = 0
        self.first_cycle = True
        self.buy_confirmation_count = 0
        self.sell_confirmation_count = 0
        self.buy_armed = False
        self._last_buy_eval_date: date | None = None
        self._last_buy_candidate = False
        self._last_sell_eval_date: date | None = None
        self._last_sell_candidate = False

    def observe_warmup_price(self, price: Any) -> None:
        if self.position_state != self.OUT:
            return
        current = self._valid_price(price)
        if current is None:
            return
        self._update_out_low(current)
        self.buy_confirmation_count = 0
        self.buy_armed = False

    def evaluate_buy_candidate(self, as_of: date, price: Any) -> bool:
        if self._last_buy_eval_date == as_of:
            return self._last_buy_candidate
        self._last_buy_eval_date = as_of
        self._last_buy_candidate = self._evaluate_buy_candidate(price)
        return self._last_buy_candidate

    def evaluate_sell_candidate(self, as_of: date, price: Any) -> bool:
        if self._last_sell_eval_date == as_of:
            return self._last_sell_candidate
        self._last_sell_eval_date = as_of
        self._last_sell_candidate = self._evaluate_sell_candidate(price)
        return self._last_sell_candidate

    def on_buy_executed(self, price: Any) -> None:
        current = self._valid_price(price)
        if current is None:
            return
        self.position_state = self.IN
        self.high_ref = current
        self.low_ref = None
        self.out_price_count = 0
        self.buy_confirmation_count = 0
        self.sell_confirmation_count = 0
        self.buy_armed = False
        self._last_sell_eval_date = None
        self._last_sell_candidate = False

    def on_sell_executed(self, price: Any) -> None:
        current = self._valid_price(price)
        self.position_state = self.OUT
        self.high_ref = None
        self.sell_confirmation_count = 0
        self.buy_confirmation_count = 0
        self.buy_armed = False
        self._last_buy_eval_date = None
        self._last_buy_candidate = False
        self.first_cycle = False
        if current is None:
            self.low_ref = None
            self.out_price_count = 0
            return
        self.low_ref = current
        self.out_price_count = 1

    def _evaluate_buy_candidate(self, price: Any) -> bool:
        if self.position_state != self.OUT:
            return False
        current = self._valid_price(price)
        if current is None:
            self.buy_confirmation_count = 0
            return False
        self._update_out_low(current)
        if not self._has_sufficient_out_history():
            self.buy_confirmation_count = 0
            self.buy_armed = False
            return False
        if self.low_ref is None or self.low_ref <= 0:
            self.buy_confirmation_count = 0
            return False
        threshold_price = self.low_ref * (Decimal("1") + self.config.buy_rebound_threshold)
        condition = current >= threshold_price
        if not condition:
            self.buy_confirmation_count = 0
            self.buy_armed = True
            return False
        if not self.buy_armed:
            # Once the OUT low history is usable, the rebound threshold is a
            # level condition: do not wait forever for a fresh crossing if it
            # already holds on the first eligible day.
            self.buy_armed = True
        self.buy_confirmation_count += 1
        return self.buy_confirmation_count >= self.config.buy_confirmation_days

    def _evaluate_sell_candidate(self, price: Any) -> bool:
        if self.position_state != self.IN:
            return False
        current = self._valid_price(price)
        if current is None:
            self.sell_confirmation_count = 0
            return False
        if self.high_ref is None or current > self.high_ref:
            self.high_ref = current
        if self.high_ref is None or self.high_ref <= 0:
            self.sell_confirmation_count = 0
            return False
        threshold_price = self.high_ref * (Decimal("1") - self.config.sell_drawdown_threshold)
        condition = current <= threshold_price
        if not condition:
            self.sell_confirmation_count = 0
            return False
        self.sell_confirmation_count += 1
        return self.sell_confirmation_count >= self.config.sell_confirmation_days

    def _update_out_low(self, current: Decimal) -> None:
        self.out_price_count += 1
        if self.low_ref is None or current < self.low_ref:
            self.low_ref = current

    def _has_sufficient_out_history(self) -> bool:
        if not self.first_cycle:
            return self.low_ref is not None
        return self.out_price_count >= self.config.initial_low_lookback_days

    @staticmethod
    def _valid_price(price: Any) -> Decimal | None:
        current = to_decimal(price)
        if current is None or current <= 0:
            return None
        return current
