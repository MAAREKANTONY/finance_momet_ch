from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0002_emailsettings'),
    ]

    operations = [
        migrations.AddField(
            model_name='scenario',
            name='n4',
            field=models.PositiveIntegerField(default=0),
        ),
        migrations.AddField(
            model_name='dailymetric',
            name='V',
            field=models.DecimalField(blank=True, decimal_places=12, max_digits=18, null=True),
        ),
        migrations.AddField(
            model_name='dailymetric',
            name='slope_P',
            field=models.DecimalField(blank=True, decimal_places=12, max_digits=18, null=True),
        ),
        migrations.AddField(
            model_name='dailymetric',
            name='sum_pos_P',
            field=models.DecimalField(blank=True, decimal_places=12, max_digits=18, null=True),
        ),
        migrations.AddField(
            model_name='dailymetric',
            name='nb_pos_P',
            field=models.PositiveIntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='dailymetric',
            name='ratio_P',
            field=models.DecimalField(blank=True, decimal_places=12, max_digits=18, null=True),
        ),
        migrations.AddField(
            model_name='dailymetric',
            name='amp_h',
            field=models.DecimalField(blank=True, decimal_places=12, max_digits=18, null=True),
        ),
    ]
