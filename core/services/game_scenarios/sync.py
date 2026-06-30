from __future__ import annotations

from core.models import GameScenario, Scenario


GAME_RUNTIME_SCENARIO_FIELDS = (
    "a", "b", "c", "d", "e", "vc", "fl",
    "n1", "n2", "n3", "n4",
    "n5", "k2j", "cr",
    "n5f3", "crf3", "nampL3", "baseL3", "periodeL3",
    "npente", "slope_threshold", "slope_sell_threshold",
    "npente_basse", "slope_threshold_basse", "slope_sell_threshold_basse",
    "recent_high_drawdown_lookback_days", "recent_high_drawdown_max_drop_pct",
    "rhd_ok_reactivation_mode", "rhd_ok_rebound_threshold", "rhd_ok_confirmation_days", "rhd_ok_reentry_max_drawdown",
    "nglobal", "m_v",
)


def sync_game_engine_scenario(game: GameScenario) -> Scenario:
    """Create or update the internal Scenario used by a GameScenario.

    This is the single authoritative mapping from GameScenario indicator
    parameters into the runtime Scenario object reused by tasks and the runner.
    """
    sc = game.engine_scenario
    if sc is None:
        sc = Scenario(
            name=f"[GAME] {game.name}",
            description=f"Auto-generated scenario for GameScenario #{game.id}",
            active=False,
            is_default=False,
        )

    for field_name in GAME_RUNTIME_SCENARIO_FIELDS:
        setattr(sc, field_name, getattr(game, field_name))

    sc.name = f"[GAME] {game.name}"
    sc.description = f"Auto-generated scenario for GameScenario #{game.id}"
    sc.active = False
    sc.is_default = False
    sc.save()

    if game.engine_scenario_id != sc.id:
        game.engine_scenario = sc
        game.save(update_fields=["engine_scenario", "updated_at"])
    return sc
