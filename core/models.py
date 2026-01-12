from __future__ import annotations

from django.core.validators import MinValueValidator
from django.db import models, transaction
from django.db.models import Q


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

    n1 = models.PositiveIntegerField(default=5)
    n2 = models.PositiveIntegerField(default=3)
    n3 = models.PositiveIntegerField(default=0)
    n4 = models.PositiveIntegerField(default=0)

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


class SymbolScenario(models.Model):
    symbol = models.ForeignKey(Symbol, on_delete=models.CASCADE)
    scenario = models.ForeignKey(Scenario, on_delete=models.CASCADE)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ("symbol", "scenario")
        indexes = [models.Index(fields=["scenario", "symbol"])]

    def __str__(self) -> str:
        return f"{self.symbol} ↔ {self.scenario}"


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
