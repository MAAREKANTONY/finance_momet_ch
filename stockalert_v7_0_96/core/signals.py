
from django.db.models.signals import pre_save
from django.dispatch import receiver
from .models import Scenario

@receiver(pre_save, sender=Scenario)
def ensure_single_default(sender, instance, **kwargs):
    if instance.is_default:
        Scenario.objects.exclude(pk=instance.pk).filter(is_default=True).update(is_default=False)
