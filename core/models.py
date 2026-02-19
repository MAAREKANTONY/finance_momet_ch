from __future__ import annotations

from decimal import Decimal
from django.core.validators import MinValueValidator
from django.db import models, transaction
from django.db.models import Q
from django.conf import settings as django_settings


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

    def __str__(self) -> str:
        return f"{self.ticker}{(':'+self.exchange) if self.exchange else ''}"


class Scenario(models.Model):
    name = models.CharField(max_length=120)
    description = models.TextField(blank=True, default="")

    # A single default scenario can exist (enforced by DB constraint + save() logic)
    is_default = models.BooleanField(default=False)

    # If True, this scenario was created as an internal clone for a Study.
    # It should generally be hidden from the main Scenarios list in the UI.
    is_study_clone = models.BooleanField(default=False)

    a = models.DecimalField(max_digits=18, decimal_places=6, default=1)
    b = models.DecimalField(max_digits=18, decimal_places=6, default=1)
    c = models.DecimalField(max_digits=18, decimal_places=6, default=1)
    d = models.DecimalField(max_digits=18, decimal_places=6, default=1)
    # e is used as a divisor -> forbid 0
    e = models.DecimalField(
        max_digits=18,
        decimal_places=6,
        default=1,
        validators=[MinValueValidator(0.0001)],
    )

    # VC: correction target for K1f (default 0.5)
    vc = models.DecimalField(max_digits=6, decimal_places=4, default=Decimal('0.5'))

    # FL: smoothing factor applied to K1f correction (default 0.5)
    fl = models.DecimalField(max_digits=6, decimal_places=4, default=Decimal('0.5'))

    n1 = models.PositiveIntegerField(default=5)
    n2 = models.PositiveIntegerField(default=3)
    n3 = models.PositiveIntegerField(default=0)
    n4 = models.PositiveIntegerField(default=0)

    # --- K2f floating line parameters (V5.2.32) ---
    # N5: window (in days) used for the cumulative daily variation sum.
    # Default: 100 days.
    n5 = models.PositiveIntegerField(
        default=100,
        help_text="K2f: fenêtre N5 (jours) pour la somme des variations journalières.",
    )

    # K2J: smoothing window (in days) used for the moving average of the pre-line.
    # Default: 10 days.
    k2j = models.PositiveIntegerField(
        default=10,
        help_text="K2f: fenêtre K2J (jours) de lissage (moyenne mobile) de la pré-ligne K2f.",
    )

    # CR: correction index (dimensionless).
    # Default: 10.
    cr = models.DecimalField(
        max_digits=10,
        decimal_places=4,
        default=Decimal('10'),
        help_text="K2f: indice de correction CR (défaut 10).",
    )

    # --- V line parameters (V5.2.37) ---
    # M_V: window (in days) used for the rolling max of daily highs.
    # Default: 20 days.
    # M1_V is derived and equals M_V/2.
    m_v = models.PositiveIntegerField(
        default=20,
        help_text="V: fenêtre M (jours) pour le max glissant des plus hauts (défaut 20). M1 = M/2.",
    )

    history_years = models.PositiveIntegerField(default=2)

    # Symbols associated to this scenario
    symbols = models.ManyToManyField(
        Symbol,
        through="SymbolScenario",
        related_name="scenarios",
        blank=True,
    )

    active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    # Used to decide between incremental compute and full recompute when config changes.
    last_computed_config_hash = models.CharField(max_length=64, blank=True, default="")
    last_full_recompute_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        constraints = [
            models.CheckConstraint(check=Q(e__gt=0), name="scenario_e_gt_0"),
            # Only one row can have is_default=True
            models.UniqueConstraint(
                fields=["is_default"],
                condition=Q(is_default=True),
                name="scenario_single_default",
            ),
        ]

    def save(self, *args, **kwargs):
        # Ensure that setting a scenario to default clears previous default.
        with transaction.atomic():
            super().save(*args, **kwargs)
            if self.is_default:
                Scenario.objects.exclude(pk=self.pk).filter(is_default=True).update(is_default=False)

    def __str__(self) -> str:
        return self.name



class Universe(models.Model):
    """Reusable group of symbols (tickers) to quickly populate Scenarios and Studies.

    Applying a Universe to a Scenario/Study *copies* its symbols into the target. The Universe itself
    is not modified when a user later adds/removes symbols from the Scenario/Study.
    """

    name = models.CharField(max_length=120)
    active = models.BooleanField(default=True)

    # Reuse the existing Symbol model used by Scenario.symbols
    symbols = models.ManyToManyField(
        Symbol,
        related_name="universes",
        blank=True,
    )

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["name"]
        indexes = [models.Index(fields=["active"], name="core_univer_active_idx")]

    def __str__(self) -> str:
        return self.name


class Study(models.Model):
    """User-friendly, unified configuration container.

    Sprint 1 scope:
    - A Study owns a cloned Scenario (is_study_clone=True)
    - The user edits everything from a single page (Study + Scenario parameters + tickers)

    Future sprints may attach AlertDefinition/Backtest clones.
    """

    name = models.CharField(max_length=120)
    description = models.TextField(blank=True, default="")

    scenario = models.ForeignKey(
        "Scenario",
        on_delete=models.PROTECT,
        related_name="studies",
    )

    # Sprint 2: a Study can also own a dedicated AlertDefinition and Backtest configuration.
    # These objects are *cloned* and live independently of their origin.
    alert_definition = models.ForeignKey(
        "AlertDefinition",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="studies",
    )

    backtest = models.ForeignKey(
        "Backtest",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="studies",
    )

    origin_scenario = models.ForeignKey(
        "Scenario",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="studies_origin",
        help_text="Scénario source utilisé lors de la création (trace uniquement).",
    )
    origin_universe = models.ForeignKey(
        "Universe",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="studies_origin",
        help_text="Universe source utilisé lors de la création (trace uniquement).",
    )

    created_by = models.ForeignKey(
        django_settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="studies",
    )

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at"]
        indexes = [models.Index(fields=["created_by", "created_at"]) ]

    def __str__(self) -> str:
        return self.name


class SymbolScenario(models.Model):
    symbol = models.ForeignKey(Symbol, on_delete=models.CASCADE)
    scenario = models.ForeignKey(Scenario, on_delete=models.CASCADE)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ("symbol", "scenario")
        indexes = [models.Index(fields=["scenario", "symbol"])]

    def __str__(self) -> str:
        return f"{self.symbol} ↔ {self.scenario}"




class Backtest(models.Model):
    """A saved backtest configuration and (optionally) its computed results.

    Feature 1 scope:
    - Store settings (scenario, dates, capital params, threshold, selected signal lines)
    - Store an immutable snapshot of the tickers universe at creation time
    - Store run status and opaque JSON results (engine will populate later)
    """

    class Status(models.TextChoices):
        PENDING = "PENDING", "Pending"
        RUNNING = "RUNNING", "Running"
        DONE = "DONE", "Done"
        FAILED = "FAILED", "Failed"

    name = models.CharField(max_length=120)
    description = models.TextField(blank=True, default="")

    scenario = models.ForeignKey(
        "Scenario",
        on_delete=models.PROTECT,
        related_name="backtests",
    )

    start_date = models.DateField(null=True, blank=True)
    end_date = models.DateField(null=True, blank=True)

    # CP: total capital. 0 means infinite (no global constraint).
    capital_total = models.DecimalField(max_digits=18, decimal_places=2, default=0)

    # CT: capital allocated per ticker / per position (first activation).
    capital_per_ticker = models.DecimalField(max_digits=18, decimal_places=2, default=0)

    # X: ratio_p threshold expressed in percent (e.g. 5.00 means 5%).
    ratio_threshold = models.DecimalField(max_digits=6, decimal_places=2, default=0)

    # If enabled, ignore the ratio_p eligibility condition (ratio_p >= X)
    # and allow BUY/SELL for all symbols in the scenario universe.
    include_all_tickers = models.BooleanField(
        default=False,
        help_text="Backtest sur toutes les actions du scénario (ignore ratio_p >= X).",
    )

    # Selected lines/rules for (buy_signal, sell_signal).
    # Stored as a list of objects, e.g. [{"buy":"A1","sell":"B1"}, ...]
    signal_lines = models.JSONField(default=list, blank=True)

    # Default behaviour chosen: close open positions on the last available day.
    close_positions_at_end = models.BooleanField(default=True)

    # Extensibility bucket (data source, slippage, fees, etc.)
    settings = models.JSONField(default=dict, blank=True)

    # Snapshot of the tickers at creation time (to keep runs reproducible even if scenario changes).
    universe_snapshot = models.JSONField(default=list, blank=True)

    # Opaque results bucket (engine will populate later).
    results = models.JSONField(default=dict, blank=True)

    status = models.CharField(max_length=10, choices=Status.choices, default=Status.PENDING)
    error_message = models.TextField(blank=True, default="")

    created_by = models.ForeignKey(
        django_settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="backtests",
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["scenario", "created_at"]),
            models.Index(fields=["status", "created_at"]),
        ]

    def __str__(self) -> str:
        return f"{self.name} ({self.scenario.name})"


# ---------------------------
# Feature 8 – Synthèse portefeuille globale
# ---------------------------


class BacktestPortfolioDaily(models.Model):
    """Daily portfolio aggregation for a Backtest.

    Stored to support UI + exports without recomputing.
    All ratios are stored as *ratios* (0.01 == 1%).
    """

    backtest = models.ForeignKey(
        Backtest,
        on_delete=models.CASCADE,
        related_name="portfolio_daily",
    )
    date = models.DateField()

    # Remaining global cash (only meaningful when CP is limited; otherwise 0).
    global_cash = models.DecimalField(max_digits=20, decimal_places=6, default=0)
    # Sum of cash across allocated (ticker,line) strategies.
    cash_allocated = models.DecimalField(max_digits=20, decimal_places=6, default=0)
    # Market value of all open positions.
    positions_value = models.DecimalField(max_digits=20, decimal_places=6, default=0)
    # Total equity = global_cash + cash_allocated + positions_value.
    equity = models.DecimalField(max_digits=20, decimal_places=6, default=0)

    # Total invested capital for the portfolio at this date.
    invested = models.DecimalField(max_digits=20, decimal_places=6, default=0)

    # Drawdown relative to peak equity (ratio, e.g. -0.12 == -12%).
    drawdown = models.DecimalField(max_digits=20, decimal_places=12, default=0)

    class Meta:
        unique_together = ("backtest", "date")
        indexes = [models.Index(fields=["backtest", "date"])]
        ordering = ["date"]

    def __str__(self) -> str:
        return f"Backtest#{self.backtest_id} {self.date} equity={self.equity}"


class BacktestPortfolioKPI(models.Model):
    """Aggregated portfolio KPIs for a Backtest."""

    backtest = models.OneToOneField(
        Backtest,
        on_delete=models.CASCADE,
        related_name="portfolio_kpi",
    )

    capital_total = models.DecimalField(max_digits=20, decimal_places=6, default=0)
    invested_end = models.DecimalField(max_digits=20, decimal_places=6, default=0)
    equity_end = models.DecimalField(max_digits=20, decimal_places=6, default=0)

    # Total return ratio: (equity_end - invested_end) / invested_end
    bt_return = models.DecimalField(max_digits=20, decimal_places=12, null=True, blank=True)
    # Mean return per day (ratio). Simple average based on invested days.
    bmj_return = models.DecimalField(max_digits=20, decimal_places=12, null=True, blank=True)

    nb_days = models.PositiveIntegerField(default=0)
    max_drawdown = models.DecimalField(max_digits=20, decimal_places=12, default=0)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self) -> str:
        return f"Backtest#{self.backtest_id} KPI"


class DailyBar(models.Model):
    symbol = models.ForeignKey(Symbol, on_delete=models.CASCADE)
    date = models.DateField()
    open = models.DecimalField(max_digits=18, decimal_places=6)
    high = models.DecimalField(max_digits=18, decimal_places=6)
    low = models.DecimalField(max_digits=18, decimal_places=6)
    close = models.DecimalField(
        max_digits=18,
        decimal_places=6,
        validators=[MinValueValidator(0.0001)],
    )
    volume = models.BigIntegerField(null=True, blank=True)
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
    K1f = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    # K2f: floating line derived from K1 (see README / Scenario parameters)
    K2f = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    # K2f_pre: pre-line before moving average (step 7 in spec). Stored to enable exact
    # rolling mean computation in incremental mode without recomputing history.
    K2f_pre = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    K2 = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    K3 = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    K4 = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)

    # --- V line (V5.2.37) ---
    # V_pre: rolling max of daily highs over M days (step 1)
    # V_line: rolling mean of V_pre over M1=M/2 days (step 2)
    V_pre = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    V_line = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)

    V = models.DecimalField(max_digits=18, decimal_places=12, null=True, blank=True)  # daily close variation ratio
    slope_P = models.DecimalField(max_digits=18, decimal_places=12, null=True, blank=True)  # avg of V over last N3 days
    sum_pos_P = models.DecimalField(max_digits=18, decimal_places=12, null=True, blank=True)  # sum positive slope_P
    nb_pos_P = models.PositiveIntegerField(null=True, blank=True)
    ratio_P = models.DecimalField(max_digits=18, decimal_places=12, null=True, blank=True)  # nb_pos_P / N4
    amp_h = models.DecimalField(max_digits=18, decimal_places=12, null=True, blank=True)
    computed_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ("symbol", "scenario", "date")
        indexes = [models.Index(fields=["symbol", "scenario", "date"])]


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

    def __str__(self):
        return self.email


class EmailSettings(models.Model):
    """Single-row settings (we keep it simple: id=1)."""

    send_hour = models.PositiveIntegerField(default=18)  # 0-23
    send_minute = models.PositiveIntegerField(default=0)  # 0-59
    timezone = models.CharField(max_length=64, default="Asia/Jerusalem")
    last_sent_date = models.DateField(null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True)

    @classmethod
    def get_solo(cls):
        obj, _ = cls.objects.get_or_create(id=1)
        return obj


class AlertDefinition(models.Model):
    """User-defined alert configuration (CRUD).

    IMPORTANT (NO-REGRESSION):
    - The alert engine still computes and stores detected alerts in `core.Alert`.
    - `AlertDefinition` only defines *how to filter* those alerts and *when/where* to send emails.
    """

    name = models.CharField(max_length=120, unique=True)
    description = models.TextField(blank=True, default="")

    scenarios = models.ManyToManyField("Scenario", related_name="alert_definitions", blank=True)

    # Comma-separated list of alert codes (e.g. "A1,B1,A2f")
    alert_codes = models.CharField(max_length=300, blank=True, default="")

    # Recipients (reuse existing EmailRecipient table)
    recipients = models.ManyToManyField("EmailRecipient", related_name="alert_definitions", blank=True)

    send_hour = models.PositiveIntegerField(default=18)  # 0-23
    send_minute = models.PositiveIntegerField(default=0)  # 0-59
    timezone = models.CharField(max_length=64, default="Asia/Jerusalem")

    is_active = models.BooleanField(default=True)
    last_sent_date = models.DateField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["name"]

    def __str__(self) -> str:
        return self.name

    def get_codes_list(self) -> list[str]:
        """Normalized list of alert codes."""
        codes = []
        for c in (self.alert_codes or "").split(","):
            c = c.strip()
            if c:
                codes.append(c)
        # de-dup while preserving order
        seen = set()
        out = []
        for c in codes:
            if c not in seen:
                seen.add(c)
                out.append(c)
        return out


# ---------------------------
# Processing Jobs (background tasks tracking)
# ---------------------------


class ProcessingJob(models.Model):
    """Track long-running background jobs (Celery tasks)."""

    class JobType(models.TextChoices):
        FETCH_BARS = "FETCH_BARS", "Fetch Daily Bars"
        COMPUTE_METRICS = "COMPUTE_METRICS", "Compute Metrics"
        RUN_BACKTEST = "RUN_BACKTEST", "Run Backtest"

    class Status(models.TextChoices):
        PENDING = "PENDING", "Pending"
        RUNNING = "RUNNING", "Running"
        DONE = "DONE", "Done"
        FAILED = "FAILED", "Failed"
        CANCELLED = "CANCELLED", "Cancelled"
        KILLED = "KILLED", "Killed"

    job_type = models.CharField(max_length=32, choices=JobType.choices)
    status = models.CharField(max_length=16, choices=Status.choices, default=Status.PENDING)

    # Celery task id (if applicable)
    task_id = models.CharField(max_length=64, blank=True, default="")

    # Context
    backtest = models.ForeignKey(
        "Backtest",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="jobs",
    )
    scenario = models.ForeignKey(
        "Scenario",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="jobs",
    )
    created_by = models.ForeignKey(
        django_settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="processing_jobs",
    )

    message = models.TextField(blank=True, default="")
    error = models.TextField(blank=True, default="")

    # Manual stop controls (additive, backward compatible)
    cancel_requested = models.BooleanField(default=False)
    kill_requested = models.BooleanField(default=False)

    # Heartbeat for detecting stuck/zombie jobs
    heartbeat_at = models.DateTimeField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["status", "created_at"]),
            models.Index(fields=["job_type", "created_at"]),
            models.Index(fields=["backtest", "created_at"]),
            models.Index(fields=["scenario", "created_at"]),
            models.Index(fields=["status", "heartbeat_at"]),
        ]

    def __str__(self) -> str:
        return f"{self.job_type}#{self.id} {self.status}"


class JobLog(models.Model):
    LEVEL_INFO = "INFO"
    LEVEL_ERROR = "ERROR"

    created_at = models.DateTimeField(auto_now_add=True)
    level = models.CharField(max_length=10, default=LEVEL_INFO)
    job = models.CharField(max_length=80)
    message = models.TextField(blank=True, default="")
    traceback = models.TextField(blank=True, default="")
    scenario = models.ForeignKey(Scenario, null=True, blank=True, on_delete=models.SET_NULL)
    symbol = models.ForeignKey(Symbol, null=True, blank=True, on_delete=models.SET_NULL)

    class Meta:
        ordering = ["-created_at"]
        indexes = [models.Index(fields=["created_at", "level", "job"])]

    @classmethod
    def info(cls, job: str, message: str, *, scenario: Scenario | None = None, symbol: Symbol | None = None):
        return cls.objects.create(level=cls.LEVEL_INFO, job=job, message=message, scenario=scenario, symbol=symbol)

    @classmethod
    def error(
        cls,
        job: str,
        message: str,
        *,
        traceback: str = "",
        scenario: Scenario | None = None,
        symbol: Symbol | None = None,
    ):
        return cls.objects.create(
            level=cls.LEVEL_ERROR,
            job=job,
            message=message,
            traceback=traceback,
            scenario=scenario,
            symbol=symbol,
        )
