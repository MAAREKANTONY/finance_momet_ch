from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0031_add_low_slope_metrics_and_signals"),
    ]

    operations = [
        migrations.AddField(
            model_name="scenario",
            name="nglobal",
            field=models.PositiveIntegerField(default=20, help_text="Nombre de jours utilisés pour la courbe globale moyenne des rendements."),
        ),
        migrations.AddField(
            model_name="gamescenario",
            name="nglobal",
            field=models.PositiveIntegerField(default=20, help_text="Nombre de jours utilisés pour la courbe globale moyenne des rendements."),
        ),
    ]
