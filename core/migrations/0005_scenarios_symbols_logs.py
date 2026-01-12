from django.db import migrations, models
import django.db.models.deletion
from django.db.models import Q
from django.core.validators import MinValueValidator


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0004_scenario_compute_hash"),
    ]

    operations = [
        migrations.AddField(
            model_name="scenario",
            name="is_default",
            field=models.BooleanField(default=False),
        ),
        migrations.CreateModel(
            name="SymbolScenario",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("scenario", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to="core.scenario")),
                ("symbol", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to="core.symbol")),
            ],
            options={
                "unique_together": {("symbol", "scenario")},
                "indexes": [models.Index(fields=["scenario", "symbol"], name="sym_scen_idx")],
            },
        ),
        migrations.AddField(
            model_name="scenario",
            name="symbols",
            field=models.ManyToManyField(blank=True, related_name="scenarios", through="core.SymbolScenario", to="core.symbol"),
        ),
        migrations.CreateModel(
            name="JobLog",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("level", models.CharField(default="INFO", max_length=10)),
                ("job", models.CharField(max_length=80)),
                ("message", models.TextField(blank=True, default="")),
                ("traceback", models.TextField(blank=True, default="")),
                ("scenario", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to="core.scenario")),
                ("symbol", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to="core.symbol")),
            ],
            options={
                "ordering": ["-created_at"],
                "indexes": [models.Index(fields=["created_at", "level", "job"], name="joblog_idx")],
            },
        ),
        migrations.AddConstraint(
            model_name="scenario",
            constraint=models.CheckConstraint(check=Q(e__gt=0), name="scenario_e_gt_0"),
        ),
        migrations.AddConstraint(
            model_name="scenario",
            constraint=models.UniqueConstraint(fields=(), condition=Q(is_default=True), name="scenario_single_default"),
        ),
    ]
