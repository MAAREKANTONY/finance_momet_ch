from django.db import migrations, models
from django.db.models import Q


def seed_strategies(apps, schema_editor):
    Strategy = apps.get_model("core", "Strategy")
    StrategyRule = apps.get_model("core", "StrategyRule")

    def ensure(name: str, buy_code: str, sell_code: str, ordering_base: int = 0):
        strategy, _ = Strategy.objects.get_or_create(
            name=name,
            defaults={"description": f"Auto-seeded strategy: BUY on {buy_code}, SELL on {sell_code}."},
        )
        # BUY rule
        StrategyRule.objects.update_or_create(
            strategy=strategy,
            ordering=ordering_base + 1,
            defaults={
                "action": "BUY",
                "signal_type": "ALERT",
                "signal_value": buy_code,
                "sizing": "ALL_IN",
                "active": True,
            },
        )
        # SELL rule
        StrategyRule.objects.update_or_create(
            strategy=strategy,
            ordering=ordering_base + 2,
            defaults={
                "action": "SELL",
                "signal_type": "ALERT",
                "signal_value": sell_code,
                "sizing": "ALL_OUT",
                "active": True,
            },
        )

    ensure("Line 1 (A1/B1)", "A1", "B1")
    ensure("Line 2 (E1/F1)", "E1", "F1")
    ensure("Line 3 (G1/H1)", "G1", "H1")


def unseed_strategies(apps, schema_editor):
    Strategy = apps.get_model("core", "Strategy")
    Strategy.objects.filter(name__in=["Line 1 (A1/B1)", "Line 2 (E1/F1)", "Line 3 (G1/H1)"]).delete()


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0007_backtesting_models"),
    ]

    operations = [
        migrations.AddField(
            model_name="strategyrule",
            name="active",
            field=models.BooleanField(default=True),
        ),
        migrations.RunPython(seed_strategies, unseed_strategies),
    ]
