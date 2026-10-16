"""
Where a game that has not finished goes.

There are three answers and all three used to be two: a not-yet-played
FBS-vs-FCS game was dropped on the floor, which made every count of a team's
remaining games too low. It now has a file of its own. The thing that must NOT
change is schedule_<year>.csv, which predict_upcoming.py reads and which has
no rating for an FCS opponent to price a game with.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from fetch_cfbd_games import (  # noqa: E402
    BOTH_FBS,
    FBS_VS_OTHER,
    FCS_SCHEDULE_FIELDS,
    NEITHER_FBS,
    SCHEDULE_FIELDS,
    fcs_schedule_row,
    unfinished_row,
)

GAME = {
    "id": 401677192, "week": 3, "startDate": "2026-09-12T16:00:00.000Z",
    "startTimeTBD": False, "seasonType": "regular",
    "homeTeam": "Alabama", "awayTeam": "Mercer",
    "homeConference": "SEC", "awayConference": "Southern",
    "homeClassification": "fbs", "awayClassification": "fcs",
    "neutralSite": False, "notes": "", "venue": "Bryant-Denny Stadium", "venueId": 900,
}


def test_an_fbs_fixture_still_goes_to_the_schedule_file():
    bucket, row = unfinished_row(GAME, 2026, None, BOTH_FBS)
    assert bucket == "schedule"
    # Exactly the old fields: predict_upcoming.py and load_schedule.py read
    # this file and a widened row would reach both.
    assert sorted(row) == sorted(SCHEDULE_FIELDS)


def test_a_non_fbs_fixture_goes_to_its_own_file_rather_than_nowhere():
    bucket, row = unfinished_row(GAME, 2026, None, FBS_VS_OTHER)
    assert bucket == "fcs_schedule"
    assert sorted(row) == sorted(FCS_SCHEDULE_FIELDS)


def test_a_game_with_no_fbs_side_is_still_dropped():
    assert unfinished_row(GAME, 2026, None, NEITHER_FBS) == (None, None)


def test_the_row_carries_both_classifications_so_the_loader_can_tell_the_sides_apart():
    # This is the ONLY thing in the payload that knows which side was FBS that
    # season, and the loader refuses a row without it. See load_fcs_games.py.
    row = fcs_schedule_row(GAME, 2026, None)
    assert (row["home_classification"], row["away_classification"]) == ("fbs", "fcs")
    assert (row["home_conference"], row["away_conference"]) == ("SEC", "Southern")


def test_the_row_carries_no_score_fields():
    # It has not been played. A 0 here would be a tie in somebody's record.
    row = fcs_schedule_row(GAME, 2026, None)
    assert not [k for k in row if "score" in k or "points" in k]


def test_the_identifying_fields_survive_the_copy_from_the_schedule_row():
    row = fcs_schedule_row(GAME, 2026, None)
    assert row["game_id"] == 401677192
    assert row["season_year"] == 2026
    assert row["week"] == "3"
    assert row["kickoff_utc"] == "2026-09-12T16:00:00.000Z"
    assert row["home_team"] == "Alabama"
    assert row["away_team"] == "Mercer"
    assert row["game_phase"] == "regular"


def test_a_missing_classification_is_an_empty_string_not_a_none():
    # csv.DictWriter writes None as an empty field anyway, but the loader reads
    # these with .strip(), so a None here would be a crash on load.
    row = fcs_schedule_row({**GAME, "homeClassification": None,
                            "awayConference": None}, 2026, None)
    assert row["home_classification"] == ""
    assert row["away_conference"] == ""


def test_the_two_schedule_files_agree_on_the_fields_they_share():
    # fcs_schedule is the schedule row plus four columns. If they drift, a
    # reader of one file cannot be reused for the other.
    assert FCS_SCHEDULE_FIELDS[:len(SCHEDULE_FIELDS)] == SCHEDULE_FIELDS
