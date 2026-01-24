# Generated manually for V5_0_0 (Feature 8: Portfolio synthesis)
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0006_backtests"),
    ]

    operations = [
        migrations.CreateModel(
            name="BacktestPortfolioDaily",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("date", models.DateField()),
                ("global_cash", models.DecimalField(decimal_places=6, default=0, max_digits=20)),
                ("cash_allocated", models.DecimalField(decimal_places=6, default=0, max_digits=20)),
                ("positions_value", models.DecimalField(decimal_places=6, default=0, max_digits=20)),
                ("equity", models.DecimalField(decimal_places=6, default=0, max_digits=20)),
                ("invested", models.DecimalField(decimal_places=6, default=0, max_digits=20)),
                ("drawdown", models.DecimalField(decimal_places=12, default=0, max_digits=20)),
                (
                    "backtest",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="portfolio_daily",
                        to="core.backtest",
                    ),
                ),
            ],
            options={
                "ordering": ["date"],
                "unique_together": {("backtest", "date")},
            },
        ),
        migrations.CreateModel(
            name="BacktestPortfolioKPI",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("capital_total", models.DecimalField(decimal_places=6, default=0, max_digits=20)),
                ("invested_end", models.DecimalField(decimal_places=6, default=0, max_digits=20)),
                ("equity_end", models.DecimalField(decimal_places=6, default=0, max_digits=20)),
                ("bt_return", models.DecimalField(blank=True, decimal_places=12, max_digits=20, null=True)),
                ("bmj_return", models.DecimalField(blank=True, decimal_places=12, max_digits=20, null=True)),
                ("nb_days", models.PositiveIntegerField(default=0)),
                ("max_drawdown", models.DecimalField(decimal_places=12, default=0, max_digits=20)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "backtest",
                    models.OneToOneField(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="portfolio_kpi",
                        to="core.backtest",
                    ),
                ),
            ],
        ),
        migrations.AddIndex(
            model_name="backtestportfoliodaily",
            index=models.Index(fields=["backtest", "date"], name="core_btpf_backte_5c2c9d_idx"),
        ),
    ]
