from __future__ import annotations

from decimal import Decimal
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0010_add_vc_and_k1f"),
    ]

    operations = [
        migrations.AddField(
            model_name="scenario",
            name="fl",
            field=models.DecimalField(default=Decimal("0.5"), max_digits=6, decimal_places=4),
        ),
    ]
