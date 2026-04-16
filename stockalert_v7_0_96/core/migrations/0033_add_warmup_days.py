from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0032_add_nglobal"),
    ]

    operations = [
        migrations.AddField(
            model_name="backtest",
            name="warmup_days",
            field=models.PositiveIntegerField(default=0, help_text="Nombre de jours calendaires de warmup avant le début réel du backtest."),
        ),
        migrations.AddField(
            model_name="gamescenario",
            name="warmup_days",
            field=models.PositiveIntegerField(default=0, help_text="Nombre de jours calendaires de warmup avant le début réel du Game."),
        ),
    ]
