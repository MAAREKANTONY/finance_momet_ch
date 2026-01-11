from django.db import models
from django.core.validators import MinValueValidator
from django.db.models import Q

import json
class Symbol(models.Model):
    ticker = models.CharField(max_length=64)
    exchange = models.CharField(max_length=64, blank=True, default="")
    name = models.CharField(max_length=200, blank=True, default="")
    instrument_type = models.CharField(max_length=64, blank=True, default="")
    country = models.CharField(max_length=64, blank=True, default="")
    currency = models.CharField(max_length=16, blank=True, default="")
    active = models.BooleanField(default=True)

    class Meta:
        unique_together = ("ticker", "exchange")
        indexes = [models.Index(fields=["ticker", "exchange", "active"])]

    def __str__(self):
        return f"{self.ticker}{(':'+self.exchange) if self.exchange else ''}"

class Scenario(models.Model):
    name = models.CharField(max_length=120)
    description = models.TextField(blank=True, default="")

    is_default = models.BooleanField(default=False)

    a = models.DecimalField(max_digits=18, decimal_places=6, default=1)
    b = models.DecimalField(max_digits=18, decimal_places=6, default=1)
    c = models.DecimalField(max_digits=18, decimal_places=6, default=1)
    d = models.DecimalField(max_digits=18, decimal_places=6, default=1)
    e = models.DecimalField(max_digits=18, decimal_places=6, default=1, validators=[MinValueValidator(0.0001)])

    n1 = models.PositiveIntegerField(default=5)
    n2 = models.PositiveIntegerField(default=3)
    n3 = models.PositiveIntegerField(default=0)
    n4 = models.PositiveIntegerField(default=0)

    history_years = models.PositiveIntegerField(default=2)

    # Backtesting
    backtest_default_capital = models.DecimalField(max_digits=18, decimal_places=2, default=10000)

    # Backtesting
    backtest_default_capital = models.DecimalField(max_digits=18, decimal_places=2, default=10000)

    # Symbols associated to this scenario
    symbols = models.ManyToManyField('Symbol', through='SymbolScenario', related_name='scenarios', blank=True)

    active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    # Used to decide between incremental compute and full recompute when config changes.
    last_computed_config_hash = models.CharField(max_length=64, blank=True, default="")
    last_full_recompute_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        constraints = [
            models.CheckConstraint(check=Q(e__gt=0), name='scenario_e_gt_0'),
            models.UniqueConstraint(fields=['is_default'], condition=Q(is_default=True), name='scenario_single_default')
        ]

    def __str__(self):
        return self.name

class DailyBar(models.Model):
    symbol = models.ForeignKey(Symbol, on_delete=models.CASCADE)
    date = models.DateField()
    open = models.DecimalField(max_digits=18, decimal_places=6)
    high = models.DecimalField(max_digits=18, decimal_places=6)
    low = models.DecimalField(max_digits=18, decimal_places=6)
    close = models.DecimalField(max_digits=18, decimal_places=6, default=1, validators=[MinValueValidator(0.0001)])
    change_amount = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    change_pct = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    source = models.CharField(max_length=64, default="twelvedata")
    ingested_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ("symbol", "date")
        indexes = [models.Index(fields=["symbol", "date"])]

class DailyMetric(models.Model):
    symbol = models.ForeignKey(Symbol, on_delete=models.CASCADE)
    scenario = models.ForeignKey(Scenario, on_delete=models.CASCADE)
    date = models.DateField()

    P = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    M = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    M1 = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    X = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    X1 = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    T = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    Q = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    S = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    K1 = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    K2 = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    K3 = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    K4 = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)


    V = models.DecimalField(max_digits=18, decimal_places=12, null=True, blank=True)  # daily close variation ratio
    slope_P = models.DecimalField(max_digits=18, decimal_places=12, null=True, blank=True)  # avg of V over last N3 days
    sum_pos_P = models.DecimalField(max_digits=18, decimal_places=12, null=True, blank=True)  # sum of positive slope_P over last N4 days
    nb_pos_P = models.PositiveIntegerField(null=True, blank=True)
    ratio_P = models.DecimalField(max_digits=18, decimal_places=12, null=True, blank=True)  # nb_pos_P / N4
    amp_h = models.DecimalField(max_digits=18, decimal_places=12, null=True, blank=True)  # sum_pos_P/(nb_pos_P*N3)
    computed_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ("symbol", "scenario", "date")
        indexes = [models.Index(fields=["symbol", "scenario", "date"])]



class SymbolScenario(models.Model):
    symbol = models.ForeignKey(Symbol, on_delete=models.CASCADE)
    scenario = models.ForeignKey(Scenario, on_delete=models.CASCADE)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ("symbol", "scenario")
        indexes = [models.Index(fields=["scenario", "symbol"])]

    def __str__(self):
        return f"{self.symbol} ↔ {self.scenario}"

class JobLog(models.Model):
    """Generic application log visible in the UI.

    NOTE: we keep this deliberately simple (no dependency on Python logging handlers)
    to ensure logs are persisted and readable from the Django app.
    """

    LEVEL_DEBUG = "DEBUG"
    LEVEL_INFO = "INFO"
    LEVEL_WARNING = "WARNING"
    LEVEL_ERROR = "ERROR"

    LEVEL_CHOICES = [
        (LEVEL_DEBUG, "Debug"),
        (LEVEL_INFO, "Info"),
        (LEVEL_WARNING, "Warning"),
        (LEVEL_ERROR, "Error"),
    ]

    created_at = models.DateTimeField(auto_now_add=True)
    level = models.CharField(max_length=10, choices=LEVEL_CHOICES, default=LEVEL_INFO)
    job = models.CharField(max_length=100)
    message = models.TextField()
    traceback = models.TextField(blank=True)

    scenario = models.ForeignKey(Scenario, on_delete=models.SET_NULL, null=True, blank=True)
    symbol = models.ForeignKey(Symbol, on_delete=models.SET_NULL, null=True, blank=True)

    def __str__(self):
        return f"[{self.level}] {self.job}: {self.message[:60]}"

    @classmethod
    def log(
        cls,
        job: str,
        message: str,
        level: str = LEVEL_INFO,
        *,
        scenario=None,
        symbol=None,
        traceback: str = "",
        extra: dict | None = None,
    ):
        """Create a log entry.

        Some callers pass an 'extra' dict (similar to Python logging). We store it by
        appending it to the message (we don't add a DB column to keep migrations stable).
        """
        if extra:
            try:
                message = f"{message} | extra={json.dumps(extra, ensure_ascii=False)}"
            except Exception:
                message = f"{message} | extra={extra}"

        return cls.objects.create(
            job=job,
            level=level,
            message=message,
            traceback=traceback or "",
            scenario=scenario,
            symbol=symbol,
        )

    @classmethod
    def debug(cls, job: str, message: str, **kwargs):
        return cls.log(job, message, level=cls.LEVEL_DEBUG, **kwargs)

    @classmethod
    def info(cls, job: str, message: str, **kwargs):
        return cls.log(job, message, level=cls.LEVEL_INFO, **kwargs)

    @classmethod
    def warning(cls, job: str, message: str, **kwargs):
        return cls.log(job, message, level=cls.LEVEL_WARNING, **kwargs)

    @classmethod
    def error(cls, job: str, message: str, **kwargs):
        return cls.log(job, message, level=cls.LEVEL_ERROR, **kwargs)


class Alert(models.Model):
    symbol = models.ForeignKey(Symbol, on_delete=models.CASCADE)
    scenario = models.ForeignKey(Scenario, on_delete=models.CASCADE)
    date = models.DateField()
    alerts = models.CharField(max_length=255, blank=True, default="")
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ("symbol", "scenario", "date")
        indexes = [models.Index(fields=["date", "scenario"])]

class EmailRecipient(models.Model):
    email = models.EmailField(unique=True)
    active = models.BooleanField(default=True)



class EmailSettings(models.Model):
    """Single-row settings (we keep it simple: id=1)."""
    send_hour = models.PositiveIntegerField(default=18)   # 0-23
    send_minute = models.PositiveIntegerField(default=0)  # 0-59
    timezone = models.CharField(max_length=64, default="Asia/Jerusalem")
    last_sent_date = models.DateField(null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True)

    @classmethod
    def get_solo(cls):
        obj, _ = cls.objects.get_or_create(id=1)
        return obj


# ---------------------------
# Backtesting
# ---------------------------

class Strategy(models.Model):
    """Backtesting strategy defined as a set of rules.

    V1 ships with a single built-in strategy: buy on A1, sell on B1.
    Execution is always at next trading day's OPEN.
    """

    EXECUTION_NEXT_OPEN = "NEXT_OPEN"
    EXECUTION_CHOICES = [(EXECUTION_NEXT_OPEN, "Open J+1")]

    name = models.CharField(max_length=120, unique=True)
    description = models.TextField(blank=True, default="")
    execution = models.CharField(max_length=20, choices=EXECUTION_CHOICES, default=EXECUTION_NEXT_OPEN)
    active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.name


class StrategyRule(models.Model):
    """A rule transforms a signal into an action."""

    SIGNAL_ALERT = "ALERT"
    SIGNAL_CHOICES = [(SIGNAL_ALERT, "Alert")]

    ACTION_BUY = "BUY"
    ACTION_SELL = "SELL"
    ACTION_CHOICES = [(ACTION_BUY, "Buy"), (ACTION_SELL, "Sell")]

    SIZING_ALL_IN = "ALL_IN"
    SIZING_ALL_OUT = "ALL_OUT"
    SIZING_CHOICES = [(SIZING_ALL_IN, "All-in"), (SIZING_ALL_OUT, "All-out")]

    strategy = models.ForeignKey(Strategy, on_delete=models.CASCADE, related_name="rules")
    signal_type = models.CharField(max_length=20, choices=SIGNAL_CHOICES, default=SIGNAL_ALERT)
    signal_value = models.CharField(max_length=20)  # e.g. A1, B1
    action = models.CharField(max_length=10, choices=ACTION_CHOICES)
    sizing = models.CharField(max_length=20, choices=SIZING_CHOICES)
    active = models.BooleanField(default=True)

    class Meta:
        unique_together = ("strategy", "signal_type", "signal_value")


class BacktestCapitalOverride(models.Model):
    """Override initial capital per (scenario, symbol)."""
    scenario = models.ForeignKey(Scenario, on_delete=models.CASCADE)
    symbol = models.ForeignKey(Symbol, on_delete=models.CASCADE)
    initial_capital = models.DecimalField(max_digits=18, decimal_places=2, validators=[MinValueValidator(0)])
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ("scenario", "symbol")
        indexes = [models.Index(fields=["scenario", "symbol"])]


class BacktestRun(models.Model):
    STATUS_CREATED = "CREATED"
    STATUS_RUNNING = "RUNNING"
    STATUS_SUCCESS = "SUCCESS"
    STATUS_FAILED = "FAILED"

    scenario = models.ForeignKey(Scenario, on_delete=models.CASCADE)
    strategy = models.ForeignKey(Strategy, on_delete=models.PROTECT)

    # Archive / identification
    name = models.CharField(max_length=160, blank=True, default="")
    description = models.TextField(blank=True, default="")

    # Backtest settings (per run, not per scenario)
    # CP: total portfolio capital. If 0 => infinite.
    capital_total = models.DecimalField(max_digits=18, decimal_places=2, default=0, validators=[MinValueValidator(0)])
    # CT: default capital allocated to a symbol "wallet" at start (or before first trade).
    capital_per_symbol = models.DecimalField(max_digits=18, decimal_places=2, default=1000, validators=[MinValueValidator(0)])
    # X: minimum ratio_p (%) required to open a new position.
    min_ratio_p = models.DecimalField(max_digits=18, decimal_places=6, default=0, validators=[MinValueValidator(0)])

    created_at = models.DateTimeField(auto_now_add=True)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    status = models.CharField(max_length=20, default="CREATED")  # CREATED/RUNNING/DONE/ERROR
    error_message = models.TextField(blank=True, default="")

    def __str__(self):
        label = self.name.strip() or f"Run #{self.id}"
        return f"{label} - {self.scenario} - {self.strategy}"
class BacktestResult(models.Model):
    run = models.ForeignKey(BacktestRun, on_delete=models.CASCADE, related_name="results")
    symbol = models.ForeignKey(Symbol, on_delete=models.CASCADE)
    initial_capital = models.DecimalField(max_digits=18, decimal_places=2)
    final_capital = models.DecimalField(max_digits=18, decimal_places=2)
    return_pct = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    trades_count = models.PositiveIntegerField(default=0)
    last_close = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)

    class Meta:
        unique_together = ("run", "symbol")
        indexes = [models.Index(fields=["run", "symbol"])]


class BacktestTrade(models.Model):
    run = models.ForeignKey(BacktestRun, on_delete=models.CASCADE, related_name="trades")
    symbol = models.ForeignKey(Symbol, on_delete=models.CASCADE)

    buy_signal_date = models.DateField(null=True, blank=True)
    buy_exec_date = models.DateField(null=True, blank=True)
    buy_price = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)

    sell_signal_date = models.DateField(null=True, blank=True)
    sell_exec_date = models.DateField(null=True, blank=True)
    sell_price = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)

    shares = models.DecimalField(max_digits=24, decimal_places=12, null=True, blank=True)
    pnl_amount = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    pnl_pct = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        indexes = [models.Index(fields=["run", "symbol", "buy_exec_date"])]




class BacktestDailyStat(models.Model):
    """Daily time series for a backtest run (per symbol)."""
    run = models.ForeignKey(BacktestRun, on_delete=models.CASCADE, related_name="daily_stats")
    symbol = models.ForeignKey(Symbol, on_delete=models.CASCADE)
    date = models.DateField()
    # Ratio_P (en %) pour ce ticker à cette date (peut être NULL si métrique absente)
    ratio_p = models.DecimalField(max_digits=10, decimal_places=4, null=True, blank=True)


    # Number of completed round trips (buy+sell) up to this date
    N = models.PositiveIntegerField(default=0)

    # Gain % for the last completed trade on this date (if any), expressed in %
    G = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)

    # Cumulative average gain: sum(G_i)/N, expressed in %
    S_G_N = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)

    # Total benefit = S_G_N * N (still in %)
    BT = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)

    # Tradable days count (ratio_p >= X, no open position) up to this date
    tradable_days = models.PositiveIntegerField(default=0)

    # Benefit mean per tradable day = BT / tradable_days (in %)
    BMJ = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)

    class Meta:
        unique_together = ("run", "symbol", "date")
        indexes = [models.Index(fields=["run", "symbol", "date"])]
