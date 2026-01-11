from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0008_strategy_rule_active_seed_strategies"),
    ]

    operations = [
        migrations.CreateModel(
            name="BacktestDailyStat",
            fields=[
                (
                    "id",
                    models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID"),
                ),
                ("date", models.DateField()),
                ("ratio_p", models.DecimalField(blank=True, decimal_places=4, max_digits=12, null=True)),
                ("n", models.IntegerField(default=0)),
                ("g", models.DecimalField(blank=True, decimal_places=4, max_digits=12, null=True)),
                ("s_g_n", models.DecimalField(blank=True, decimal_places=4, max_digits=12, null=True)),
                ("bt", models.DecimalField(blank=True, decimal_places=4, max_digits=12, null=True)),
                ("bmj", models.DecimalField(blank=True, decimal_places=6, max_digits=18, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                (
                    "run",
                    models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="daily_stats", to="core.backtestrun"),
                ),
                (
                    "symbol",
                    models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to="core.symbol"),
                ),
            ],
            options={
                "ordering": ["date"],
            },
        ),
        migrations.AddConstraint(
            model_name="backtestdailystat",
            constraint=models.UniqueConstraint(fields=("run", "symbol", "date"), name="uniq_backtest_daily_run_symbol_date"),
        ),
        migrations.AddIndex(
            model_name="backtestdailystat",
            index=models.Index(fields=["run", "date"], name="bt_daily_run_date_idx"),
        ),
        migrations.AddIndex(
            model_name="backtestdailystat",
            index=models.Index(fields=["symbol", "date"], name="bt_daily_symbol_date_idx"),
        ),
    ]
