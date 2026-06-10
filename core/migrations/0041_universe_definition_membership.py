from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0040_scenario_universe_mode"),
    ]

    operations = [
        migrations.CreateModel(
            name="UniverseDefinition",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("code", models.CharField(max_length=32, unique=True)),
                ("name", models.CharField(max_length=120)),
                ("description", models.TextField(blank=True, default="")),
                ("source", models.CharField(blank=True, default="", max_length=64)),
                ("active", models.BooleanField(default=True)),
                ("metadata", models.JSONField(blank=True, default=dict)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "ordering": ["code"],
            },
        ),
        migrations.CreateModel(
            name="UniverseMembership",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("ticker", models.CharField(max_length=64)),
                ("exchange", models.CharField(blank=True, default="", max_length=64)),
                ("provider_symbol", models.CharField(blank=True, default="", max_length=128)),
                ("valid_from", models.DateField()),
                ("valid_to", models.DateField(blank=True, null=True)),
                ("source", models.CharField(blank=True, default="", max_length=64)),
                ("source_payload", models.JSONField(blank=True, default=dict)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "symbol",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="universe_memberships",
                        to="core.symbol",
                    ),
                ),
                (
                    "universe",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="memberships",
                        to="core.universedefinition",
                    ),
                ),
            ],
            options={
                "ordering": ["universe__code", "ticker", "valid_from"],
            },
        ),
        migrations.AddConstraint(
            model_name="universemembership",
            constraint=models.UniqueConstraint(
                fields=("universe", "ticker", "exchange", "valid_from"),
                name="uniq_universe_membership_start",
            ),
        ),
        migrations.AddConstraint(
            model_name="universemembership",
            constraint=models.CheckConstraint(
                condition=models.Q(("valid_to__isnull", True), ("valid_to__gte", models.F("valid_from")), _connector="OR"),
                name="membership_valid_to_gte_from",
            ),
        ),
        migrations.AddIndex(
            model_name="universemembership",
            index=models.Index(fields=["universe", "valid_from", "valid_to"], name="core_um_range_idx"),
        ),
        migrations.AddIndex(
            model_name="universemembership",
            index=models.Index(fields=["universe", "ticker"], name="core_um_ticker_idx"),
        ),
        migrations.AddIndex(
            model_name="universemembership",
            index=models.Index(fields=["symbol"], name="core_um_symbol_idx"),
        ),
    ]
