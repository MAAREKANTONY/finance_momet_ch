from django.db import migrations
import json


def _map_code(code: str) -> str | None:
    c = (code or "").strip()
    upper = c.upper()
    if upper in {"A2F", "A2BIS", "AF3"}:
        return "Af"
    if upper in {"B2F", "B2BIS", "BF3"}:
        return "Bf"
    if upper in {"A1F", "B1F", "I1", "J1"}:
        return None
    mapping = {
        "A1": "A1", "B1": "B1", "C1": "C1", "D1": "D1",
        "E1": "E1", "F1": "F1", "G1": "G1", "H1": "H1",
        "SPA": "SPa", "SPV": "SPv", "AF": "Af", "BF": "Bf",
    }
    return mapping.get(upper, c or None)


def forwards(apps, schema_editor):
    AlertDefinition = apps.get_model("core", "AlertDefinition")
    Backtest = apps.get_model("core", "Backtest")
    GameScenario = apps.get_model("core", "GameScenario")
    Alert = apps.get_model("core", "Alert")

    for obj in AlertDefinition.objects.all():
        codes = []
        seen = set()
        for raw in (obj.alert_codes or "").split(","):
            mapped = _map_code(raw)
            if mapped and mapped not in seen:
                seen.add(mapped)
                codes.append(mapped)
        obj.alert_codes = ",".join(codes)
        obj.save(update_fields=["alert_codes"])

    def normalize_lines(lines):
        out = []
        for line in (lines or []):
            if not isinstance(line, dict):
                continue
            buy = _map_code(line.get("buy"))
            sell = _map_code(line.get("sell"))
            if not buy and not sell:
                continue
            new_line = dict(line)
            if buy:
                new_line["buy"] = buy
            else:
                new_line.pop("buy", None)
            if sell:
                new_line["sell"] = sell
            else:
                new_line.pop("sell", None)
            out.append(new_line)
        return out

    for model in (Backtest, GameScenario):
        for obj in model.objects.all():
            obj.signal_lines = normalize_lines(obj.signal_lines)
            obj.save(update_fields=["signal_lines"])

    for obj in Alert.objects.all():
        codes = []
        seen = set()
        for raw in (obj.alerts or "").split(","):
            mapped = _map_code(raw)
            if mapped and mapped not in seen:
                seen.add(mapped)
                codes.append(mapped)
        obj.alerts = ",".join(codes)
        obj.save(update_fields=["alerts"])


class Migration(migrations.Migration):
    dependencies = [("core", "0028_add_kf2bis")]
    operations = [migrations.RunPython(forwards, migrations.RunPython.noop)]
