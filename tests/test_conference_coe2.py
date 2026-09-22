"""Tests for build_conference_coe2.py's external-game classification logic."""
from build_conference_coe2 import is_external_game


def test_regular_season_same_conference_is_not_external():
    assert is_external_game("SEC", "SEC", "regular") is False


def test_regular_season_different_conference_is_external():
    assert is_external_game("SEC", "Big Ten", "regular") is True


def test_postseason_always_external_even_same_conference():
    """
    The spec is explicit: ConferenceGameSet = Nonconference + Postseason.
    A bowl game between two same-conference teams (rare, but has
    happened historically) must still count as external -- postseason
    status alone qualifies it, regardless of the opponent's conference.
    """
    assert is_external_game("SEC", "SEC", "bowl") is True
    assert is_external_game("SEC", "SEC", "cfp") is True


def test_opponent_with_no_conference_is_external():
    """An Independent opponent (no conference_real) always counts as external."""
    assert is_external_game("SEC", None, "regular") is True
