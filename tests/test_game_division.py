"""
Which games this project rates, which it only shows, and which it drops.

`division=fbs` does not mean "both teams are FBS" -- CFBD returns every game an
FBS team played. These pin the three-way split, and above all that the set of
RATED games is exactly what it was before non-FBS games were kept at all: a
game moving into games_<year>.csv would move published Elo and CoE.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from fetch_cfbd_games import (  # noqa: E402
    BOTH_FBS,
    FBS_VS_OTHER,
    NEITHER_FBS,
    game_division,
    is_fbs_game,
)


def labelled(home, away, **extra):
    g = {"homeClassification": home, "awayClassification": away}
    return {k: v for k, v in {**g, **extra}.items() if v is not None}


@pytest.mark.parametrize("home, away, expected", [
    ("fbs", "fbs", BOTH_FBS),
    ("FBS", "Fbs", BOTH_FBS),          # the source's casing is not load-bearing
    ("fbs", "fcs", FBS_VS_OTHER),
    ("fcs", "fbs", FBS_VS_OTHER),      # the FCS side can be either home or away
    ("fbs", "ii", FBS_VS_OTHER),
    ("fcs", "fcs", NEITHER_FBS),
    ("ii", "iii", NEITHER_FBS),
])
def test_both_sides_labelled_is_decided_by_the_labels(home, away, expected):
    assert game_division(labelled(home, away)) == expected


@pytest.mark.parametrize("home, away", [("fbs", None), (None, "fbs"), ("fcs", None), (None, "fcs")])
def test_a_half_labelled_game_is_shown_but_never_rated(home, away):
    # This case used to be dropped outright. It must not become a rated game:
    # that would add games to Elo and CoE that have never been in them.
    assert game_division(labelled(home, away)) == FBS_VS_OTHER


@pytest.mark.parametrize("notes, expected", [
    ("", BOTH_FBS),
    ("Rose Bowl", BOTH_FBS),
    ("FCS Championship", FBS_VS_OTHER),
    ("NCAA Division I Championship Subdivision", FBS_VS_OTHER),
    ("Division II semifinal", FBS_VS_OTHER),
    ("Division III final", FBS_VS_OTHER),
])
def test_with_no_classification_the_notes_are_the_only_evidence(notes, expected):
    # Most of the pre-2000 archive carries no classification at all. Absence of
    # evidence is read as FBS, which is what this project assumed before, so
    # historical ratings stay where they are.
    assert game_division({"notes": notes}) == expected


def test_the_rated_set_is_unchanged_by_the_three_way_split():
    """is_fbs_game is the old gate; it must still admit exactly BOTH_FBS."""
    payloads = [
        labelled("fbs", "fbs"), labelled("fbs", "fcs"), labelled("fcs", "fcs"),
        labelled("fbs", None), labelled(None, "fcs"),
        {"notes": "Rose Bowl"}, {"notes": "FCS Championship"}, {},
    ]
    for g in payloads:
        assert is_fbs_game(g) is (game_division(g) == BOTH_FBS), g
    # And the historical gate's own answer, restated as literals rather than
    # recomputed from the code under test.
    assert [is_fbs_game(g) for g in payloads] == [
        True, False, False, False, False, True, False, True]
