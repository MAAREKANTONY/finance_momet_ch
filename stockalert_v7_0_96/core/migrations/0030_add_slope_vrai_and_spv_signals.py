from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0029_cleanup_signal_codes_kf"),
    ]

    operations = [
        migrations.AddField(
            model_name="dailymetric",
            name="slope_vrai",
            field=models.DecimalField(max_digits=18, decimal_places=12, null=True, blank=True),
        ),
    ]
