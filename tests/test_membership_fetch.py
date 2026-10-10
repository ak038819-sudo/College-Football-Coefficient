"""The membership fetch must not drop a team it cannot name.

CFBD's /teams `school` is not this project's team name, and the fetcher used
to skip any school a bare `WHERE team_name = ?` missed. Six real FBS programs
were silently absent from every season it wrote, which is how 2026 shipped
with no conference for Miami (FL), Appalachian State, San Jose State, FIU,
ULM and FAU.
"""
import csv
import json
import sqlite3
from pathlib import Path

import pytest

from fetch_cfbd_team_memberships import load_year

ROOT = Path(__file__).resolve().parent.parent

# The six programs CFBD names differently from this project, with the string
# CFBD actually sends. patch_known_membership_gaps.py patches these same six
# for 2014-2025, which is the historical shadow of this same bug.
RENAMED = [
    ("Miami", "Miami (FL)"),
    ("San José State", "San Jose State"),
    ("Florida Atlantic", "FAU"),
    ("Florida International", "FIU"),
    ("UL Monroe", "ULM"),
    ("App State", "Appalachian State"),
]


def _db():
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        "CREATE TABLE teams(team_id INTEGER PRIMARY KEY, team_name TEXT);"
        "CREATE TABLE team_aliases(alias TEXT, team_name TEXT);"
        "CREATE TABLE team_membership_by_season("
        "  team_id INTEGER, season_year INTEGER, conference_real TEXT,"
        "  is_fbs INTEGER, PRIMARY KEY (team_id, season_year));"
    )
    names = [name for _, name in RENAMED] + ["Alabama"]
    conn.executemany(
        "INSERT INTO teams VALUES (?, ?)",
        [(9000 + i, name) for i, name in enumerate(names)],
    )
    return conn


def _written(cur, year):
    return {
        row[0]: row[1]
        for row in cur.execute(
            "SELECT t.team_name, m.conference_real FROM team_membership_by_season m "
            "JOIN teams t ON t.team_id = m.team_id WHERE m.season_year = ?",
            (year,),
        )
    }


@pytest.mark.parametrize("school,team_name", RENAMED)
def test_a_school_cfbd_names_differently_still_gets_its_row(school, team_name):
    conn = _db()
    cur = conn.cursor()
    inserted, unresolved, no_conference = load_year(
        cur, 2026, [{"school": school, "conference": "Sun Belt",
                     "classification": "fbs"}]
    )
    assert (inserted, unresolved, no_conference) == (1, [], [])
    assert _written(cur, 2026) == {team_name: "Sun Belt"}


def test_an_unnameable_fbs_school_is_reported_rather_than_skipped():
    conn = _db()
    cur = conn.cursor()
    inserted, unresolved, no_conference = load_year(
        cur, 2026, [{"school": "Alabama", "conference": "SEC",
                     "classification": "fbs"},
                    {"school": "Invented Tech", "conference": "SEC",
                     "classification": "fbs"}]
    )
    assert inserted == 1
    assert unresolved == ["Invented Tech"]
    assert no_conference == []
    # The nameable school still landed; one bad row does not lose the season.
    assert _written(cur, 2026) == {"Alabama": "SEC"}


def test_an_fbs_school_with_no_conference_is_listed_not_an_error():
    conn = _db()
    cur = conn.cursor()
    inserted, unresolved, no_conference = load_year(
        cur, 2026, [{"school": "Miami", "conference": None,
                     "classification": "fbs"}]
    )
    # A program reclassifying from FCS genuinely has no FBS conference yet.
    # No row is the correct outcome; being told about it is the point.
    assert (inserted, unresolved) == (0, [])
    assert no_conference == ["Miami -> Miami (FL)"]
    assert _written(cur, 2026) == {}


def test_a_school_that_is_neither_nameable_nor_conferenced_is_still_listed():
    conn = _db()
    cur = conn.cursor()
    _, unresolved, no_conference = load_year(
        cur, 2026, [{"school": "Invented Tech", "conference": "",
                     "classification": "fbs"}]
    )
    # Reported as a conference gap rather than vanishing: resolution happens
    # before the conference check for exactly this case.
    assert unresolved == []
    assert no_conference == ["Invented Tech"]


def test_non_fbs_schools_are_skipped_without_being_reported():
    conn = _db()
    cur = conn.cursor()
    inserted, unresolved, no_conference = load_year(
        cur, 2026, [{"school": "Some FCS School", "conference": "Big Sky",
                     "classification": "fcs"}]
    )
    assert (inserted, unresolved, no_conference) == (0, [], [])


def test_the_2026_snapshot_covers_every_reviewed_fbs_program():
    """The committed snapshot is what CI loads, so guard it directly.

    Only the programs this project knows are not FBS in 2026 may be absent:
    Idaho dropped to FCS after 2017, and Sacramento State and North Dakota
    State are reclassifying upward and have no FBS conference yet.
    """
    reviewed = {
        team["canonical_name"]
        for team in json.loads(
            (ROOT / "data/reference/team_identities.json").read_text()
        )["teams"]
    }
    snapshot = {
        row["team_name"]
        for row in csv.DictReader(
            (ROOT / "data/raw/membership_2026.csv").open(newline="")
        )
    }
    not_fbs_in_2026 = {"Idaho", "North Dakota State", "Sacramento State"}

    assert snapshot <= reviewed, sorted(snapshot - reviewed)
    assert reviewed - snapshot == not_fbs_in_2026, sorted(reviewed - snapshot - not_fbs_in_2026)


def test_the_2026_snapshot_names_one_conference_per_team():
    rows = list(csv.DictReader(
        (ROOT / "data/raw/membership_2026.csv").open(newline="")))
    assert len(rows) == len({row["team_name"] for row in rows})
    for row in rows:
        assert row["conference_real"].strip(), row
        assert row["is_fbs"] == "1", row
