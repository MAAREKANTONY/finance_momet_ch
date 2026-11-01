"""Flow-adjusted portfolio protection, independent of the existing KPI drawdown."""
from decimal import Decimal, InvalidOperation
from fractions import Fraction


PROTECTION_KEY = "portfolio_protection"


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
    return threshold


class PortfolioProtection:
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
