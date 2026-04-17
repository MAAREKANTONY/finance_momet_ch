from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0015_add_v_line_and_scenario_mv"),
    ]

    operations = [
        migrations.AddField(
            model_name="processingjob",
            name="cancel_requested",
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name="processingjob",
            name="kill_requested",
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name="processingjob",
            name="heartbeat_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddIndex(
            model_name="processingjob",
            index=models.Index(fields=["status", "heartbeat_at"], name="core_proces_status_heartb_9e2c9a_idx"),
        ),
    ]
