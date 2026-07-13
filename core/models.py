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
    sector = models.CharField(max_length=120, blank=True, default="")
    active = models.BooleanField(default=True)

    class Meta:
        unique_together = ("ticker", "exchange")
        indexes = [models.Index(fields=["ticker", "exchange", "active"])]

    def __str__(self) -> str:
        return f"{self.ticker}{(':'+self.exchange) if self.exchange else ''}"


class Scenario(models.Model):
    class RhdOkReactivationMode(models.TextChoices):
        CLASSIC = "classic", "Classique"
        REBOUND_CONFIRMED = "rebound_confirmed", "Rebond confirmé"

    class UniverseMode(models.TextChoices):
        STATIC_TICKERS = "STATIC_TICKERS", "Sélection statique de tickers"
        SP500_HISTORICAL_DYNAMIC = "SP500_HISTORICAL_DYNAMIC", "S&P500 historique dynamique"
        CSI300_HISTORICAL_DYNAMIC = "CSI300_HISTORICAL_DYNAMIC", "CSI 300 historique dynamique — via CSV"

    name = models.CharField(max_length=120)
    description = models.TextField(blank=True, default="")
    description = models.TextField(blank=True, default="")

    # A single default scenario can exist (enforced by DB constraint + save() logic)
    is_default = models.BooleanField(default=False)

    # If True, this scenario was created as an internal clone for a Study.
    # It should generally be hidden from the main Scenarios list in the UI.
    is_study_clone = models.BooleanField(default=False)

    universe_mode = models.CharField(
        max_length=32,
        choices=UniverseMode.choices,
        default=UniverseMode.STATIC_TICKERS,
        help_text="Mode d'univers du scénario. Phase 1 stocke uniquement ce choix.",
    )

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
    n2 = models.PositiveIntegerField(default=3, help_text="Fenêtre N2 (jours), utilisée aussi pour la ligne flottante 2 bis.")
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

    # --- Kf3 floating line parameters (V7.x) ---
    # Equivalent of N5/CR but for floating line 3.
    n5f3 = models.PositiveIntegerField(
        default=100,
        help_text="Kf3: fenêtre N5f3 (jours) pour max/min flottants (défaut 100).",
    )
    crf3 = models.DecimalField(
        max_digits=10,
        decimal_places=4,
        default=Decimal('10'),
        help_text="Kf3: indice de correction CRf3 (défaut 10).",
    )

    # Dynamic slope window (L3):
    # amp(t) = mean over NampL3 of abs((P-P-1)/P-1)
    # k(t) = amp/base
    # periode(t) = periodeL3 / k
    nampL3 = models.PositiveIntegerField(
        default=100,
        help_text="Kf3: NampL3 (jours) pour la moyenne des variations absolues (défaut 100).",
    )
    baseL3 = models.DecimalField(
        max_digits=12,
        decimal_places=6,
        default=Decimal('0.02'),
        validators=[MinValueValidator(0.000001)],
        help_text="Kf3: base (défaut 0.02).",
    )
    periodeL3 = models.PositiveIntegerField(
        default=100,
        help_text="Kf3: période nominale (défaut 100).",
    )

    npente = models.PositiveIntegerField(
        default=100,
        help_text="Nombre de jours utilisés pour calculer SUM((P(t)-P(t-1))/P(t-1)) pour SPa/SPv.",
    )
    slope_threshold = models.DecimalField(
        max_digits=18,
        decimal_places=8,
        default=Decimal("0.1"),
        help_text="Seuil de déclenchement achat utilisé par SPa/SPVa (ratio brut, ex: 0.1 = 10%).",
    )
    slope_sell_threshold = models.DecimalField(
        max_digits=18,
        decimal_places=8,
        null=True,
        blank=True,
        help_text="Seuil de déclenchement vente utilisé par SPv/SPVv. Si vide, le seuil d'achat est réutilisé.",
    )
    npente_basse = models.PositiveIntegerField(
        default=20,
        help_text="Nombre de jours utilisés pour calculer SUM_SLOPE_BASSE et SLOPE_VRAI_BASSE.",
    )
    slope_threshold_basse = models.DecimalField(
        max_digits=18,
        decimal_places=8,
        default=Decimal("0.02"),
        help_text="Seuil de déclenchement achat — pente basse, utilisé par SPa_basse/SPVa_basse (ratio brut, ex: 0.02 = 2%).",
    )
    slope_sell_threshold_basse = models.DecimalField(
        max_digits=18,
        decimal_places=8,
        null=True,
        blank=True,
        help_text="Seuil de déclenchement vente — pente basse, utilisé par SPv_basse/SPVv_basse. Si vide, le seuil d'achat est réutilisé.",
    )
    recent_high_drawdown_lookback_days = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="Protection anti-chute : nombre de jours de cotation précédents utilisés pour calculer le plus haut récent. Le jour courant est exclu.",
    )
    recent_high_drawdown_max_drop_pct = models.DecimalField(
        max_digits=18,
        decimal_places=8,
        null=True,
        blank=True,
        help_text="Protection anti-chute : pourcentage maximal de baisse autorisé par rapport au plus haut récent (ratio brut, ex: -0.10 = -10%).",
    )
    rhd_ok_reactivation_mode = models.CharField(
        max_length=32,
        choices=RhdOkReactivationMode.choices,
        default=RhdOkReactivationMode.CLASSIC,
        help_text="Mode de réactivation de RHD_OK après RHD_FAIL.",
    )
    rhd_ok_rebound_threshold = models.DecimalField(
        max_digits=18,
        decimal_places=8,
        default=Decimal("0.08"),
        validators=[MinValueValidator(Decimal("0"))],
        help_text="Rebond minimum depuis le point bas après RHD_FAIL (ratio brut, ex: 0.08 = 8%).",
    )
    rhd_ok_confirmation_days = models.PositiveSmallIntegerField(
        default=2,
        validators=[MinValueValidator(1)],
        help_text="Nombre de jours de cotation consécutifs requis pour confirmer le rebond RHD_OK.",
    )
    rhd_ok_reentry_max_drawdown = models.DecimalField(
        max_digits=18,
        decimal_places=8,
        default=Decimal("0.40"),
        validators=[MinValueValidator(Decimal("0"))],
        help_text="Drawdown maximum autorisé à la réentrée RHD_OK en mode rebond confirmé.",
    )
    nglobal = models.PositiveIntegerField(
        default=20,
        help_text="Nombre de jours utilisés pour la courbe globale moyenne des rendements.",
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
    updated_at = models.DateTimeField(auto_now=True)

    # Used to decide between incremental compute and full recompute when config changes.
    last_computed_config_hash = models.CharField(max_length=64, blank=True, default="")
    last_full_recompute_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        constraints = [
            models.CheckConstraint(condition=Q(e__gt=0), name="scenario_e_gt_0"),
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

    description = models.TextField(blank=True, default="")
    updated_at = models.DateTimeField(auto_now=True)

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


class UniverseDefinition(models.Model):
    code = models.CharField(max_length=32, unique=True)
    name = models.CharField(max_length=120)
    description = models.TextField(blank=True, default="")
    source = models.CharField(max_length=64, blank=True, default="")
    active = models.BooleanField(default=True)
    metadata = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["code"]

    def __str__(self) -> str:
        return f"{self.code} - {self.name}"


class UniverseMembership(models.Model):
    universe = models.ForeignKey(
        UniverseDefinition,
        on_delete=models.CASCADE,
        related_name="memberships",
    )
    symbol = models.ForeignKey(
        Symbol,
        on_delete=models.PROTECT,
        null=True,
        blank=True,
        related_name="universe_memberships",
    )
    ticker = models.CharField(max_length=64)
    exchange = models.CharField(max_length=64, blank=True, default="")
    provider_symbol = models.CharField(max_length=128, blank=True, default="")
    valid_from = models.DateField()
    valid_to = models.DateField(null=True, blank=True)
    source = models.CharField(max_length=64, blank=True, default="")
    source_payload = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["universe__code", "ticker", "valid_from"]
        constraints = [
            models.UniqueConstraint(
                fields=["universe", "ticker", "exchange", "valid_from"],
                name="uniq_universe_membership_start",
            ),
            models.CheckConstraint(
                condition=Q(valid_to__isnull=True) | Q(valid_to__gte=models.F("valid_from")),
                name="membership_valid_to_gte_from",
            ),
        ]
        indexes = [
            models.Index(fields=["universe", "valid_from", "valid_to"], name="core_um_range_idx"),
            models.Index(fields=["universe", "ticker"], name="core_um_ticker_idx"),
            models.Index(fields=["symbol"], name="core_um_symbol_idx"),
        ]

    def __str__(self) -> str:
        suffix = f":{self.exchange}" if self.exchange else ""
        end = self.valid_to.isoformat() if self.valid_to else "open"
        return f"{self.universe.code} {self.ticker}{suffix} {self.valid_from.isoformat()}..{end}"


class UniverseCoverageStatus(models.TextChoices):
    IMPORTED = "IMPORTED", "Imported"
    VALIDATED = "VALIDATED", "Validated"
    PARTIAL = "PARTIAL", "Partial"
    FAILED = "FAILED", "Failed"
    STALE = "STALE", "Stale"


class UniverseImportBatch(models.Model):
    universe = models.ForeignKey(
        UniverseDefinition,
        on_delete=models.CASCADE,
        related_name="import_batches",
    )
    provider = models.CharField(max_length=64, blank=True, default="")
    source_name = models.CharField(max_length=120, blank=True, default="")
    source_reference = models.CharField(max_length=255, blank=True, default="")
    period_start = models.DateField()
    period_end = models.DateField()
    expected_member_count = models.PositiveIntegerField(default=500)
    imported_member_count = models.PositiveIntegerField(default=0)
    mapped_member_count = models.PositiveIntegerField(default=0)
    unmapped_member_count = models.PositiveIntegerField(default=0)
    status = models.CharField(
        max_length=16,
        choices=UniverseCoverageStatus.choices,
        default=UniverseCoverageStatus.IMPORTED,
    )
    validated_at = models.DateTimeField(null=True, blank=True)
    metadata = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["universe__code", "period_start", "period_end", "id"]
        constraints = [
            models.CheckConstraint(
                condition=Q(period_end__gte=models.F("period_start")),
                name="uib_period_end_gte_start",
            ),
        ]
        indexes = [
            models.Index(fields=["universe", "period_start", "period_end"], name="core_uib_period_idx"),
            models.Index(fields=["universe", "status"], name="core_uib_status_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.universe.code} {self.period_start.isoformat()}..{self.period_end.isoformat()} {self.status}"


class UniverseCoverageSnapshot(models.Model):
    universe = models.ForeignKey(
        UniverseDefinition,
        on_delete=models.CASCADE,
        related_name="coverage_snapshots",
    )
    import_batch = models.ForeignKey(
        UniverseImportBatch,
        on_delete=models.PROTECT,
        related_name="coverage_snapshots",
    )
    coverage_date = models.DateField()
    expected_member_count = models.PositiveIntegerField(default=500)
    actual_member_count = models.PositiveIntegerField(default=0)
    mapped_member_count = models.PositiveIntegerField(default=0)
    unmapped_member_count = models.PositiveIntegerField(default=0)
    status = models.CharField(
        max_length=16,
        choices=UniverseCoverageStatus.choices,
        default=UniverseCoverageStatus.IMPORTED,
    )
    metadata = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["universe__code", "coverage_date"]
        constraints = [
            models.UniqueConstraint(
                fields=["universe", "coverage_date"],
                name="uniq_universe_coverage_date",
            ),
        ]
        indexes = [
            models.Index(fields=["universe", "coverage_date"], name="core_ucs_date_idx"),
            models.Index(fields=["universe", "status"], name="core_ucs_status_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.universe.code} {self.coverage_date.isoformat()} {self.status}"


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


class GameScenario(models.Model):
    """"Scénario de Jeu".

    Merge entity combining Scenario params + Backtest rules + recipients.

    Runs daily on *all* symbols, computing only the per-ticker BMD KPI for the
    last `study_days` market days ending today.

    Stores only the current day's snapshot in `today_results` (no history).
    """

    name = models.CharField(max_length=120)
    description = models.TextField(blank=True, default="")

    study_days = models.PositiveIntegerField(default=1000)

    active = models.BooleanField(default=True)

    tradability_threshold = models.DecimalField(
        max_digits=12,
        decimal_places=6,
        validators=[MinValueValidator(Decimal("0"))],
        default=Decimal("0"),
        help_text="Seuil de tradabilité (BMD >= seuil => OK).",
    )
    npente = models.PositiveIntegerField(
        default=100,
        help_text="Nombre de jours utilisés pour calculer la moyenne des pentes.",
    )
    slope_threshold = models.DecimalField(
        max_digits=18,
        decimal_places=8,
        default=Decimal("0.1"),
        help_text="Seuil de déclenchement achat utilisé à la fois pour la tradabilité du Game, SPa et SPVa (ratio brut, ex: 0.1 = 10%).",
    )
    slope_sell_threshold = models.DecimalField(
        max_digits=18,
        decimal_places=8,
        null=True,
        blank=True,
        help_text="Seuil de déclenchement vente utilisé par SPv/SPVv. Si vide, le seuil d'achat est réutilisé. N'affecte pas la tradabilité du Game.",
    )
    npente_basse = models.PositiveIntegerField(
        default=20,
        help_text="Nombre de jours utilisés pour calculer SUM_SLOPE_BASSE et SLOPE_VRAI_BASSE.",
    )
    slope_threshold_basse = models.DecimalField(
        max_digits=18,
        decimal_places=8,
        default=Decimal("0.02"),
        help_text="Seuil de déclenchement achat — pente basse, utilisé par SPa_basse/SPVa_basse (ratio brut, ex: 0.02 = 2%).",
    )
    slope_sell_threshold_basse = models.DecimalField(
        max_digits=18,
        decimal_places=8,
        null=True,
        blank=True,
        help_text="Seuil de déclenchement vente — pente basse, utilisé par SPv_basse/SPVv_basse. Si vide, le seuil d'achat est réutilisé.",
    )
    recent_high_drawdown_lookback_days = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="Protection anti-chute : nombre de jours de cotation précédents utilisés pour calculer le plus haut récent. Le jour courant est exclu.",
    )
    recent_high_drawdown_max_drop_pct = models.DecimalField(
        max_digits=18,
        decimal_places=8,
        null=True,
        blank=True,
        help_text="Protection anti-chute : pourcentage maximal de baisse autorisé par rapport au plus haut récent (ratio brut, ex: -0.10 = -10%). N'affecte la tradabilité du Game que via les alertes si utilisé dans les signaux.",
    )
    rhd_ok_reactivation_mode = models.CharField(
        max_length=32,
        choices=Scenario.RhdOkReactivationMode.choices,
        default=Scenario.RhdOkReactivationMode.CLASSIC,
        help_text="Mode de réactivation de RHD_OK après RHD_FAIL.",
    )
    rhd_ok_rebound_threshold = models.DecimalField(
        max_digits=18,
        decimal_places=8,
        default=Decimal("0.08"),
        validators=[MinValueValidator(Decimal("0"))],
        help_text="Rebond minimum depuis le point bas après RHD_FAIL (ratio brut, ex: 0.08 = 8%).",
    )
    rhd_ok_confirmation_days = models.PositiveSmallIntegerField(
        default=2,
        validators=[MinValueValidator(1)],
        help_text="Nombre de jours de cotation consécutifs requis pour confirmer le rebond RHD_OK.",
    )
    rhd_ok_reentry_max_drawdown = models.DecimalField(
        max_digits=18,
        decimal_places=8,
        default=Decimal("0.40"),
        validators=[MinValueValidator(Decimal("0"))],
        help_text="Drawdown maximum autorisé à la réentrée RHD_OK en mode rebond confirmé.",
    )
    nglobal = models.PositiveIntegerField(
        default=20,
        help_text="Nombre de jours utilisés pour la courbe globale moyenne des rendements.",
    )
    presence_threshold_pct = models.DecimalField(
        max_digits=8,
        decimal_places=4,
        validators=[MinValueValidator(Decimal("0"))],
        default=Decimal("30"),
        help_text="Seuil minimal de temps de présence en position (%).",
    )

    email_recipients = models.TextField(blank=True, default="")

    # --- Scenario fields (same defaults/constraints) ---
    a = models.DecimalField(max_digits=18, decimal_places=6, default=1)
    b = models.DecimalField(max_digits=18, decimal_places=6, default=1)
    c = models.DecimalField(max_digits=18, decimal_places=6, default=1)
    d = models.DecimalField(max_digits=18, decimal_places=6, default=1)
    e = models.DecimalField(
        max_digits=18,
        decimal_places=6,
        default=1,
        validators=[MinValueValidator(0.0001)],
    )
    vc = models.DecimalField(max_digits=6, decimal_places=4, default=Decimal("0.5"))
    fl = models.DecimalField(max_digits=6, decimal_places=4, default=Decimal("0.5"))

    n1 = models.PositiveIntegerField(default=5)
    n2 = models.PositiveIntegerField(default=3, help_text="Fenêtre N2 (jours), utilisée aussi pour la ligne flottante 2 bis.")
    n3 = models.PositiveIntegerField(default=0)
    n4 = models.PositiveIntegerField(default=0)
    n5 = models.PositiveIntegerField(default=100)
    k2j = models.PositiveIntegerField(default=10)
    cr = models.DecimalField(max_digits=10, decimal_places=4, default=Decimal("10"))
    m_v = models.PositiveIntegerField(default=20)

    # --- Kf3 parameters (same defaults as Scenario) ---
    n5f3 = models.PositiveIntegerField(default=100)
    crf3 = models.DecimalField(max_digits=10, decimal_places=4, default=Decimal("10"))
    nampL3 = models.PositiveIntegerField(default=100)
    baseL3 = models.DecimalField(
        max_digits=12,
        decimal_places=6,
        default=Decimal("0.02"),
        validators=[MinValueValidator(Decimal("0.000001"))],
    )
    periodeL3 = models.PositiveIntegerField(default=100)

    # Internal scenario used to persist DailyMetric/Alert.
    engine_scenario = models.OneToOneField(
        "Scenario",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="game_scenario",
    )

    # --- Backtest fields (same defaults) ---
    class CapitalMode(models.TextChoices):
        REINVEST = "REINVEST", "Reinvest (capital évolutif)"
        FIXED = "FIXED", "Fixed (capital initial constant)"

    capital_total = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    capital_per_ticker = models.DecimalField(max_digits=18, decimal_places=2, default=0)

    # Controls whether CT evolves with realized gains/losses per ticker (legacy behaviour)
    # or stays fixed at the initial CT at each new BUY.
    capital_mode = models.CharField(
        max_length=12,
        choices=CapitalMode.choices,
        default=CapitalMode.REINVEST,
    )
    signal_lines = models.JSONField(default=list, blank=True)
    warmup_days = models.PositiveIntegerField(default=0, help_text="Nombre de jours calendaires de warmup avant le début réel du Game.")
    close_positions_at_end = models.BooleanField(default=True)
    settings = models.JSONField(default=dict, blank=True)

    last_run_at = models.DateTimeField(null=True, blank=True)
    last_run_status = models.CharField(max_length=20, blank=True, default="")
    last_run_message = models.TextField(blank=True, default="")
    today_results = models.JSONField(default=dict, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at"]
        indexes = [models.Index(fields=["active", "last_run_at"], name="core_gamesce_active_idx")]

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

    class CapitalMode(models.TextChoices):
        REINVEST = "REINVEST", "Reinvest (capital évolutif)"
        FIXED = "FIXED", "Fixed (capital initial constant)"

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

    # Controls whether CT evolves with realized gains/losses per ticker (legacy behaviour)
    # or stays fixed at the initial CT at each new BUY.
    capital_mode = models.CharField(
        max_length=12,
        choices=CapitalMode.choices,
        default=CapitalMode.REINVEST,
    )

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
    warmup_days = models.PositiveIntegerField(default=0, help_text="Nombre de jours calendaires de warmup avant le début réel du backtest.")

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


class RunConfigurationSnapshot(models.Model):
    class Kind(models.TextChoices):
        BACKTEST = "BACKTEST", "Backtest"
        GAME = "GAME", "Game"

    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    kind = models.CharField(max_length=16, choices=Kind.choices, db_index=True)
    label = models.CharField(max_length=255)
    config_hash = models.CharField(max_length=64, db_index=True)
    scenario_snapshot = models.JSONField(default=dict, blank=True)
    run_snapshot = models.JSONField(default=dict, blank=True)
    source_scenario = models.ForeignKey(
        Scenario,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="configuration_snapshots",
    )
    source_backtest = models.ForeignKey(
        Backtest,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="configuration_snapshots",
    )
    source_game_scenario = models.ForeignKey(
        GameScenario,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="configuration_snapshots",
    )

    class Meta:
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["kind", "created_at"]),
            models.Index(fields=["config_hash"]),
            models.Index(fields=["source_backtest"]),
            models.Index(fields=["source_game_scenario"]),
        ]

    def __str__(self) -> str:
        return f"{self.kind} {self.label}"


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

    # Total portfolio BT ratio: (equity_end - invested_end) / invested_end
    bt_return = models.DecimalField(max_digits=20, decimal_places=12, null=True, blank=True)
    # Mean portfolio return per invested day: bt_return / nb_days.
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

    # Kf3: floating line 3 (price line)
    Kf3 = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    # Kf2bis: floating line 2 bis (price line)
    Kf2bis = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
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
    sum_slope = models.DecimalField(max_digits=18, decimal_places=12, null=True, blank=True)  # sum of daily study-price slopes over Npente days
    slope_vrai = models.DecimalField(max_digits=18, decimal_places=12, null=True, blank=True)  # (P(t)-P(t-Npente))/P(t-Npente)
    sum_slope_basse = models.DecimalField(max_digits=18, decimal_places=12, null=True, blank=True)  # sum of daily study-price slopes over Npente_basse days
    slope_vrai_basse = models.DecimalField(max_digits=18, decimal_places=12, null=True, blank=True)  # (P(t)-P(t-Npente_basse))/P(t-Npente_basse)
    sum_pos_P = models.DecimalField(max_digits=18, decimal_places=12, null=True, blank=True)  # sum positive slope_P
    nb_pos_P = models.PositiveIntegerField(null=True, blank=True)
    ratio_P = models.DecimalField(max_digits=18, decimal_places=12, null=True, blank=True)  # nb_pos_P / N4
    amp_h = models.DecimalField(max_digits=18, decimal_places=12, null=True, blank=True)
    computed_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ("symbol", "scenario", "date")
        indexes = [models.Index(fields=["symbol", "scenario", "date"])]


class HistoricalMarketCap(models.Model):
    symbol = models.ForeignKey(
        Symbol,
        on_delete=models.CASCADE,
        related_name="historical_market_caps",
    )
    date = models.DateField()
    market_cap = models.DecimalField(max_digits=24, decimal_places=2)
    currency = models.CharField(max_length=8, blank=True, default="")
    provider = models.CharField(max_length=32, default="eodhd")
    provider_symbol = models.CharField(max_length=64, blank=True, default="")
    source_payload = models.JSONField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["provider", "symbol", "date"],
                name="historical_market_cap_unique_provider_symbol_date",
            ),
        ]
        indexes = [
            models.Index(
                fields=["provider", "symbol", "date"],
                name="core_hmcap_prov_sym_dt_idx",
            ),
        ]


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
        SYNC_MARKET_CAPS = "SYNC_MARKET_CAPS", "Sync Market Caps"
        ENRICH_METADATA = "ENRICH_METADATA", "Enrichissement des métadonnées"
        RUN_BACKTEST = "RUN_BACKTEST", "Run Backtest"
        RUN_GAME = "RUN_GAME", "Run Game Scenario"
        SEND_EMAILS = "SEND_EMAILS", "Send Emails"
        EXPORT_ALERTS_CSV = "EXPORT_ALERTS_CSV", "Export Alerts CSV"
        EXPORT_SCENARIO_XLSX = "EXPORT_SCENARIO_XLSX", "Export Scenario XLSX"
        EXPORT_ALL_SCENARIOS_ZIP = "EXPORT_ALL_SCENARIOS_ZIP", "Export All Scenarios ZIP"
        EXPORT_DATA_XLSX = "EXPORT_DATA_XLSX", "Export Data XLSX"
        EXPORT_BACKTEST_DEBUG_CSV = "EXPORT_BACKTEST_DEBUG_CSV", "Export Backtest Debug CSV"
        EXPORT_BACKTEST_DEBUG_XLSX = "EXPORT_BACKTEST_DEBUG_XLSX", "Export Backtest Debug XLSX"
        EXPORT_BACKTEST_XLSX = "EXPORT_BACKTEST_XLSX", "Export Backtest XLSX"
        EXPORT_BACKTEST_XLSX_COMPACT = "EXPORT_BACKTEST_XLSX_COMPACT", "Export Backtest XLSX Compact"
        EXPORT_GAME_SCENARIO_XLSX = "EXPORT_GAME_SCENARIO_XLSX", "Export Game Scenario XLSX"
        EXPORT_BACKTEST_DETAILS_ZIP = "EXPORT_BACKTEST_DETAILS_ZIP", "Export Backtest Details ZIP"

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
    game_scenario = models.ForeignKey(
        "GameScenario",
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

    # Optional artifact produced by the job (e.g. async exports)
    output_file = models.CharField(max_length=512, blank=True, default="")
    output_name = models.CharField(max_length=255, blank=True, default="")

    # Manual stop controls (additive, backward compatible)
    cancel_requested = models.BooleanField(default=False)
    kill_requested = models.BooleanField(default=False)

    # Heartbeat + visibility metadata for detecting stuck/zombie jobs
    heartbeat_at = models.DateTimeField(null=True, blank=True)
    last_checkpoint = models.CharField(max_length=255, blank=True, default="")
    worker_hostname = models.CharField(max_length=255, blank=True, default="")

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
            models.Index(fields=["game_scenario", "created_at"]),
            models.Index(fields=["status", "heartbeat_at"]),
            models.Index(fields=["status", "worker_hostname"]),
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
