from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0018_add_study"),
    ]

    operations = [
        migrations.AddField(
            model_name="study",
            name="alert_definition",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="studies",
                to="core.alertdefinition",
            ),
        ),
        migrations.AddField(
            model_name="study",
            name="backtest",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="studies",
                to="core.backtest",
            ),
        ),
    ]
