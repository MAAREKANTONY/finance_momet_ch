from django.conf import settings


def app_version(request):
    """Expose APP_VERSION to all templates."""
    return {"APP_VERSION": getattr(settings, "APP_VERSION", "")}
