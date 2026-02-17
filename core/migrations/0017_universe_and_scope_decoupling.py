from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0016_processingjob_cancel_and_heartbeat"),
    ]

    operations = [
        migrations.CreateModel(
            name="Universe",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("name", models.CharField(max_length=120, unique=True)),
                ("description", models.TextField(blank=True, default="")),
                ("active", models.BooleanField(default=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "ordering": ["name"],
            },
        ),
        migrations.AddIndex(
            model_name="universe",
            index=models.Index(fields=["active", "name"], name="core_univer_active_1c6b75_idx"),
        ),
        migrations.AddField(
            model_name="universe",
            name="symbols",
            field=models.ManyToManyField(blank=True, related_name="universes", to="core.symbol"),
        ),
        migrations.AddField(
            model_name="backtest",
            name="universe",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="backtests",
                to="core.universe",
            ),
        ),
        migrations.AddField(
            model_name="alertdefinition",
            name="universes",
            field=models.ManyToManyField(blank=True, related_name="alert_definitions", to="core.universe"),
        ),
    ]
