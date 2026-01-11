from django.db import migrations, models
import django.db.models.deletion
from django.core.validators import MinValueValidator


def seed_default_strategy(apps, schema_editor):
    Strategy = apps.get_model("core", "Strategy")
    StrategyRule = apps.get_model("core", "StrategyRule")
    strategy, _ = Strategy.objects.get_or_create(
        name="Alerts A1/B1 (Open J+1)",
        defaults={
            "description": "Acheter sur A1, vendre sur B1. Execution au prochain OPEN.",
            "execution": "NEXT_OPEN",
            "active": True,
        },
    )
    StrategyRule.objects.get_or_create(
        strategy=strategy,
        signal_type="ALERT",
        signal_value="A1",
        defaults={"action": "BUY", "sizing": "ALL_IN"},
    )
    StrategyRule.objects.get_or_create(
        strategy=strategy,
        signal_type="ALERT",
        signal_value="B1",
        defaults={"action": "SELL", "sizing": "ALL_OUT"},
    )


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0006_scenario_backtest_default_capital"),
    ]

    operations = [
        migrations.CreateModel(
            name="Strategy",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("name", models.CharField(max_length=120, unique=True)),
                ("description", models.TextField(blank=True, default="")),
                ("execution", models.CharField(choices=[("NEXT_OPEN", "Open J+1")], default="NEXT_OPEN", max_length=20)),
                ("active", models.BooleanField(default=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
            ],
        ),
        migrations.CreateModel(
            name="StrategyRule",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("signal_type", models.CharField(choices=[("ALERT", "Alert")], default="ALERT", max_length=20)),
                ("signal_value", models.CharField(max_length=20)),
                ("action", models.CharField(choices=[("BUY", "Buy"), ("SELL", "Sell")], max_length=10)),
                ("sizing", models.CharField(choices=[("ALL_IN", "All-in"), ("ALL_OUT", "All-out")], max_length=20)),
                ("strategy", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="rules", to="core.strategy")),
            ],
            options={
                "unique_together": {("strategy", "signal_type", "signal_value")},
            },
        ),
        migrations.CreateModel(
            name="BacktestRun",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("started_at", models.DateTimeField(blank=True, null=True)),
                ("finished_at", models.DateTimeField(blank=True, null=True)),
                ("status", models.CharField(default="CREATED", max_length=20)),
                ("error_message", models.TextField(blank=True, default="")),
                ("scenario", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to="core.scenario")),
                ("strategy", models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, to="core.strategy")),
            ],
        ),
        migrations.CreateModel(
            name="BacktestCapitalOverride",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("initial_capital", models.DecimalField(decimal_places=2, max_digits=18, validators=[MinValueValidator(0)])),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("scenario", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to="core.scenario")),
                ("symbol", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to="core.symbol")),
            ],
            options={
                "unique_together": {("scenario", "symbol")},
            },
        ),
        migrations.AddIndex(
            model_name="backtestcapitaloverride",
            index=models.Index(fields=["scenario", "symbol"], name="core_backte_scenario_4d4c7d_idx"),
        ),
        migrations.CreateModel(
            name="BacktestResult",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("initial_capital", models.DecimalField(decimal_places=2, max_digits=18)),
                ("final_capital", models.DecimalField(decimal_places=2, max_digits=18)),
                ("return_pct", models.DecimalField(blank=True, decimal_places=6, max_digits=18, null=True)),
                ("trades_count", models.PositiveIntegerField(default=0)),
                ("last_close", models.DecimalField(blank=True, decimal_places=6, max_digits=18, null=True)),
                ("run", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="results", to="core.backtestrun")),
                ("symbol", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to="core.symbol")),
            ],
            options={
                "unique_together": {("run", "symbol")},
            },
        ),
        migrations.AddIndex(
            model_name="backtestresult",
            index=models.Index(fields=["run", "symbol"], name="core_backte_run_sym_87d8cb_idx"),
        ),
        migrations.CreateModel(
            name="BacktestTrade",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("buy_signal_date", models.DateField(blank=True, null=True)),
                ("buy_exec_date", models.DateField(blank=True, null=True)),
                ("buy_price", models.DecimalField(blank=True, decimal_places=6, max_digits=18, null=True)),
                ("sell_signal_date", models.DateField(blank=True, null=True)),
                ("sell_exec_date", models.DateField(blank=True, null=True)),
                ("sell_price", models.DecimalField(blank=True, decimal_places=6, max_digits=18, null=True)),
                ("shares", models.DecimalField(blank=True, decimal_places=12, max_digits=24, null=True)),
                ("pnl_amount", models.DecimalField(blank=True, decimal_places=6, max_digits=18, null=True)),
                ("pnl_pct", models.DecimalField(blank=True, decimal_places=6, max_digits=18, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("run", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="trades", to="core.backtestrun")),
                ("symbol", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to="core.symbol")),
            ],
        ),
        migrations.AddIndex(
            model_name="backtesttrade",
            index=models.Index(fields=["run", "symbol", "buy_exec_date"], name="core_backte_run_sym_f0a880_idx"),
        ),
        migrations.RunPython(seed_default_strategy, migrations.RunPython.noop),
    ]
