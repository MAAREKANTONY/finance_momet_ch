from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0035_processingjob_visibility_fields"),
    ]

    operations = [
        migrations.AddField(
            model_name="processingjob",
            name="game_scenario",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=models.SET_NULL,
                related_name="jobs",
                to="core.gamescenario",
            ),
        ),
        migrations.AddIndex(
            model_name="processingjob",
            index=models.Index(fields=["game_scenario", "created_at"], name="core_proces_game_sc_b24a83_idx"),
        ),
    ]
