from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0016_processingjob_cancel_and_heartbeat"),
    ]

    operations = [
        migrations.AddField(
            model_name="scenario",
            name="is_study_clone",
            field=models.BooleanField(default=False),
        ),
        migrations.CreateModel(
            name="Universe",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("name", models.CharField(max_length=120)),
                ("description", models.TextField(blank=True, default="")),
                (
                    "is_public",
                    models.BooleanField(
                        default=False,
                        help_text="Si activé, visible par tous les utilisateurs (sinon uniquement le créateur).",
                    ),
                ),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "created_by",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="universes",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "ordering": ["name"],
            },
        ),
        migrations.CreateModel(
            name="UniverseSymbol",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                (
                    "symbol",
                    models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to="core.symbol"),
                ),
                (
                    "universe",
                    models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to="core.universe"),
                ),
            ],
            options={
                "unique_together": {("universe", "symbol")},
            },
        ),
        migrations.AddField(
            model_name="universe",
            name="symbols",
            field=models.ManyToManyField(blank=True, related_name="universes", through="core.UniverseSymbol", to="core.symbol"),
        ),
        migrations.CreateModel(
            name="Study",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("name", models.CharField(max_length=120)),
                ("description", models.TextField(blank=True, default="")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "created_by",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="studies",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
                (
                    "origin_scenario",
                    models.ForeignKey(
                        blank=True,
                        help_text="Scénario source utilisé lors de la création (trace uniquement).",
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="studies_origin",
                        to="core.scenario",
                    ),
                ),
                (
                    "origin_universe",
                    models.ForeignKey(
                        blank=True,
                        help_text="Universe source utilisé lors de la création (trace uniquement).",
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="studies_origin",
                        to="core.universe",
                    ),
                ),
                (
                    "scenario",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="studies",
                        to="core.scenario",
                    ),
                ),
            ],
            options={
                "ordering": ["-created_at"],
            },
        ),
        migrations.AddIndex(
            model_name="universe",
            index=models.Index(fields=["name", "is_public"], name="core_universe_name_is_public_idx"),
        ),
        migrations.AddIndex(
            model_name="universesymbol",
            index=models.Index(fields=["universe", "symbol"], name="core_universesymbol_universe_symbol_idx"),
        ),
        migrations.AddIndex(
            model_name="study",
            index=models.Index(fields=["created_by", "created_at"], name="core_study_created_by_created_at_idx"),
        ),
    ]
