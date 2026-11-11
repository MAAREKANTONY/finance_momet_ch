"""Flow-adjusted portfolio protection, independent of the existing KPI drawdown."""
from decimal import Decimal, InvalidOperation
from fractions import Fraction


PROTECTION_KEY = "portfolio_protection"
VALUE_MODE = "portfolio_value"
PNL_MODE = "cycle_pnl"


def _protection_mode(config):
    mode = config.get("mode") or VALUE_MODE
    if mode not in (VALUE_MODE, PNL_MODE):
        raise ValueError("Mode de protection du portefeuille invalide.")
    return mode


def _initial_loss_amount(config):
    try:
        amount = Decimal(str(config.get("initial_loss_amount")))
    except (InvalidOperation, ValueError):
        amount = Decimal("NaN")
    if not amount.is_finite() or amount <= 0:
        raise ValueError("La perte initiale maximale L doit être un montant strictement positif dans la devise du backtest.")
    return amount


def protection_threshold(settings):
    config = (settings or {}).get(PROTECTION_KEY) or {}
    if not isinstance(config, dict):
        raise ValueError("Configuration de protection du portefeuille invalide.")
    if not config.get("enabled", False):
        return None
    try:
        threshold = Decimal(str(config.get("threshold_pct")))
    except (InvalidOperation, ValueError):
        threshold = Decimal("NaN")
    if not threshold.is_finite() or not 0 < threshold < 100:
        raise ValueError("Le seuil de protection doit être strictement compris entre 0 et 100 %.")
    if _protection_mode(config) == PNL_MODE:
        _initial_loss_amount(config)
    return threshold


class PortfolioProtection:
    mode = VALUE_MODE

    def __init__(self, threshold):
        self.threshold = threshold
        self.units = None
        self._index = None
        self._peak = None
        self.pending = False
        self.current = None
        self.cycles = []

    def observe(self, day, equity):
        if self.current is None:
            if equity <= 0:
                return
            self.units = Fraction(equity) / 100
            self._index = self._peak = Fraction(100)
            self.current = {
                "cycle": len(self.cycles) + 1, "start_date": str(day),
                "start_equity": str(equity), "external_funding": "0",
                "trigger_date": None, "trigger_drawdown": None,
            }
        else:
            self._index = Fraction(equity) / self.units
            self._peak = max(self._peak, self._index)
        self.current.update(end_equity=str(equity), peak_index=str(self.peak), index=str(self.index))

    def fund(self, day, equity_before, amount):
        # Allocations occur together before BUYs, at the same marks as observe().
        if self.current is None:
            self.observe(day, equity_before + amount)
            return
        # Rational units keep contributions exactly neutral, including recurring
        # decimal quotients. Only the displayed index is converted to Decimal.
        self.units += Fraction(amount) / self._index
        self.current["external_funding"] = str(Decimal(self.current["external_funding"]) + amount)
        self.current["end_equity"] = str(equity_before + amount)

    @staticmethod
    def _decimal(value):
        return None if value is None else Decimal(value.numerator) / Decimal(value.denominator)

    @property
    def index(self):
        return self._decimal(self._index)

    @property
    def peak(self):
        return self._decimal(self._peak)

    @property
    def drawdown(self):
        return self._decimal((self._peak - self._index) / self._peak) if self._peak else Decimal("0")

    def check(self, day, equity):
        self.observe(day, equity)
        # Compare without rounding the percentage displayed to the user.
        if self.current and not self.pending and (self._peak - self._index) * 100 > self._peak * Fraction(self.threshold):
            self.pending = True
            self.current.update(trigger_date=str(day), trigger_drawdown=str(self.drawdown))

    def summary(self, day, reason):
        return {
            **self.current, "end_date": str(day), "close_reason": reason,
            "pnl": str(Decimal(self.current["end_equity"]) - Decimal(self.current["start_equity"])
                       - Decimal(self.current["external_funding"])),
        }

    def finish(self, day):
        self.cycles.append(self.summary(day, "PORTFOLIO_DRAWDOWN"))
        self.pending = False
        self.current = None
        self.units = self._index = self._peak = None

    def daily_fields(self):
        return {
            "protection_cycle": self.current["cycle"] if self.current else None,
            "protection_index": self.current["index"] if self.current else None,
            "protection_peak": self.current["peak_index"] if self.current else None,
            "protection_drawdown": str(self.drawdown),
            "protection_pending": self.pending,
        }


class CyclePnlProtection:
    """Track realized + unrealized cycle profit, net of external financing.

    Peak and boundary are observed by check(), before orders. observe() records
    end-of-day accounts without introducing a second trading decision.
    """

    mode = PNL_MODE

    def __init__(self, threshold, initial_loss_amount):
        self.threshold = threshold
        self.initial_loss_amount = initial_loss_amount
        self.pending = False
        self.current = None
        self.cycles = []
        self.cycle_pnl = Decimal("0")
        self.peak_pnl = Decimal("0")
        self.decision = {}

    @property
    def bound(self):
        if self.peak_pnl > 0:
            return self.peak_pnl * (Decimal("1") - self.threshold / Decimal("100"))
        return -self.initial_loss_amount

    def observe(self, day, equity):
        if self.current is None:
            # Keep the engine's existing financing/insolvency buy guard.
            if equity <= 0:
                return
            self.cycle_pnl = self.peak_pnl = Decimal("0")
            self.current = {
                "cycle": len(self.cycles) + 1, "start_date": str(day),
                "start_equity": str(equity), "external_funding": "0",
                "trigger_date": None, "trigger_pnl": None,
                "trigger_peak_pnl": None, "trigger_bound": None, "trigger_rule": None,
            }
        self.cycle_pnl = equity - Decimal(self.current["start_equity"]) - Decimal(self.current["external_funding"])
        self.current.update(end_equity=str(equity), pnl_peak=str(self.peak_pnl), pnl_bound=str(self.bound))

    def fund(self, day, equity_before, amount):
        if self.current is not None:
            self.current["external_funding"] = str(Decimal(self.current["external_funding"]) + amount)
        self.observe(day, equity_before + amount)

    def check(self, day, equity):
        self.observe(day, equity)
        self.decision = {}
        if self.current is None:
            return
        self.peak_pnl = max(self.peak_pnl, self.cycle_pnl)
        self.current.update(pnl_peak=str(self.peak_pnl), pnl_bound=str(self.bound))
        self.decision = {"pnl": str(self.cycle_pnl), "peak": str(self.peak_pnl), "bound": str(self.bound)}
        if not self.pending and self.cycle_pnl < self.bound:
            self.pending = True
            self.current.update(
                trigger_date=str(day), trigger_pnl=str(self.cycle_pnl),
                trigger_peak_pnl=str(self.peak_pnl), trigger_bound=str(self.bound),
                trigger_rule="profit_retracement" if self.peak_pnl > 0 else "initial_loss",
            )

    def summary(self, day, reason):
        return {**self.current, "end_date": str(day), "close_reason": reason, "pnl": str(self.cycle_pnl)}

    def finish(self, day):
        self.cycles.append(self.summary(day, "PORTFOLIO_DRAWDOWN"))
        self.pending = False
        self.current = None
        self.cycle_pnl = self.peak_pnl = Decimal("0")
        self.decision = {}

    def daily_fields(self):
        return {
            "protection_cycle": self.current["cycle"] if self.current else None,
            "protection_cycle_pnl": str(self.cycle_pnl) if self.current else None,
            "protection_pnl_peak": str(self.peak_pnl) if self.current else None,
            "protection_pnl_bound": str(self.bound) if self.current else None,
            "protection_decision_pnl": self.decision.get("pnl"),
            "protection_decision_peak": self.decision.get("peak"),
            "protection_decision_bound": self.decision.get("bound"),
            "protection_pending": self.pending,
        }


def build_portfolio_protection(settings):
    threshold = protection_threshold(settings)
    if threshold is None:
        return None
    config = settings[PROTECTION_KEY]
    if _protection_mode(config) == PNL_MODE:
        return CyclePnlProtection(threshold, _initial_loss_amount(config))
    return PortfolioProtection(threshold)
