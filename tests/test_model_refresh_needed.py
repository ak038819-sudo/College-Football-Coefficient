"""Code pushes refresh only when the live feed has newer FBS results."""
from model_refresh_needed import needs_refresh


def _game(status="completed", home=52, away=28, classification="fbs"):
    return {"id": 401856699, "start_date": "2026-09-26T19:30:00Z", "status": status,
            "home": {"points": home, "classification": classification},
            "away": {"points": away, "classification": classification}}


def test_new_or_changed_final_triggers_refresh():
    raw = {"game_id": "401856699", "home_score": "", "away_score": ""}
    assert needs_refresh({"games": [_game()]}, [], 2026)
    assert needs_refresh({"games": [_game()]}, [raw], 2026)
    assert needs_refresh({"games": [_game()]}, [{**raw, "home_score": "50", "away_score": "28"}], 2026)
    assert not needs_refresh({"games": [_game()]}, [{**raw, "home_score": "52", "away_score": "28"}], 2026)


def test_scheduled_and_non_fbs_games_do_not_trigger_refresh():
    assert not needs_refresh({"games": [_game(status="scheduled")]}, [], 2026)
    assert not needs_refresh({"games": [_game(classification="fcs")]}, [], 2026)
    assert not needs_refresh({"games": [_game(home=None)]}, [], 2026)
    assert not needs_refresh({"games": [_game()]}, [], 2025)
