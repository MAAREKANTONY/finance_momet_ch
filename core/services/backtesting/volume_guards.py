import os
from typing import Dict, List, Tuple, Any

def volume_guards_enabled() -> bool:
    return (os.getenv("ENABLE_VOLUME_GUARDS", "0").strip() == "1")

def excel_full_tickers_threshold() -> int:
    try:
        return int(os.getenv("EXCEL_FULL_TICKERS_THRESHOLD", "150").strip())
    except Exception:
        return 150

def excel_top_n() -> int:
    try:
        return int(os.getenv("EXCEL_TOP_N", "50").strip())
    except Exception:
        return 50

def should_limit_excel(num_tickers: int) -> bool:
    if not volume_guards_enabled():
        return False
    return num_tickers > excel_full_tickers_threshold()

def select_top_tickers_by_metric(tickers_map: Dict[str, Any], top_n: int) -> List[str]:
    """Select Top N tickers by best final BT among all lines.

    Purely technical heuristic used only when volume guards are enabled.
    """
    scores: List[Tuple[float, str]] = []
    for ticker, tentry in (tickers_map or {}).items():
        best = None
        for line in (tentry or {}).get("lines") or []:
            fin = (line or {}).get("final") or {}
            bt = fin.get("BT")
            try:
                bt_f = float(bt)
            except Exception:
                continue
            if best is None or bt_f > best:
                best = bt_f
        if best is None:
            best = float("-inf")
        scores.append((best, ticker))
    # sort desc score, then ticker asc for determinism
    scores.sort(key=lambda x: (-x[0], x[1]))
    selected = [t for _, t in scores[: max(0, int(top_n))]]
    return selected
