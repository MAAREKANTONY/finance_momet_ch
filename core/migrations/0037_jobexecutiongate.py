from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0036_processingjob_game_scenario'),
    ]

    operations = [
        migrations.CreateModel(
            name='JobExecutionGate',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('updated_at', models.DateTimeField(auto_now=True)),
            ],
        ),
    ]
