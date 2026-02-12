from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0014_alert_definition"),
    ]

    operations = [
        migrations.AddField(
            model_name="scenario",
            name="m_v",
            field=models.PositiveIntegerField(
                default=20,
                help_text="V: fenêtre M (jours) pour le max glissant des plus hauts (défaut 20). M1 = M/2.",
            ),
        ),
        migrations.AddField(
            model_name="dailymetric",
            name="V_pre",
            field=models.DecimalField(blank=True, decimal_places=6, max_digits=18, null=True),
        ),
        migrations.AddField(
            model_name="dailymetric",
            name="V_line",
            field=models.DecimalField(blank=True, decimal_places=6, max_digits=18, null=True),
        ),
    ]
