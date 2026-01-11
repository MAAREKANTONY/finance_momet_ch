from django import template

register = template.Library()


@register.filter
def get_item(mapping, key):
    """Allow dict access in templates: {{ mydict|get_item:mykey }}"""
    try:
        return mapping.get(key)
    except Exception:
        return None
