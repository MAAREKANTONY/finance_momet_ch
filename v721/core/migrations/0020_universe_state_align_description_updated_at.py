from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0019_study_alert_backtest"),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            database_operations=[],
            state_operations=[
                migrations.AddField(
                    model_name="universe",
                    name="description",
                    field=models.TextField(blank=True, default=""),
                ),
                migrations.AddField(
                    model_name="universe",
                    name="updated_at",
                    field=models.DateTimeField(auto_now=True),
                ),
            ],
        ),
    ]
