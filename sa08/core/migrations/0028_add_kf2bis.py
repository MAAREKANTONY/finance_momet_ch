from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0027_scenario_npente_sum_slope_and_sp_signals"),
    ]

    operations = [
        migrations.AddField(
            model_name="dailymetric",
            name="Kf2bis",
            field=models.DecimalField(blank=True, decimal_places=6, max_digits=18, null=True),
        ),
    ]
