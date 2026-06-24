from decimal import Decimal


def format_decimal_plain(value):
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    dec = Decimal(text)
    rendered = format(dec, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    if rendered in ("", "-0"):
        return "0"
    return rendered
