"""Player and coach page exports.

Drives the real exporter against a real database and then loads the files it
emits, because the thing that matters is whether a browser can read them, not
whether Python could write them.
"""
import json
import sqlite3
import subprocess

import pytest

import person_identity as identity
from export_people_pages import (PLAYER_SHARDS, build_coaches, build_name_index,
                                 build_players, build_rosters, export, shard_key)

TEAMS = {1: "Clemson", 2: "Oregon"}


@pytest.fixture
def conn(repo_root):
    c = sqlite3.connect(":memory:")
    c.execute("PRAGMA foreign_keys = ON")
    c.executescript((repo_root / "sql" / "schema.sql").read_text(encoding="utf-8"))
    c.executescript((repo_root / "sql" / "person_tables.sql").read_text(encoding="utf-8"))
    for team_id, name in TEAMS.items():
        c.execute("INSERT INTO teams (team_id, team_name) VALUES (?, ?)", (team_id, name))
    return c


def _player(conn, name, seasons, **profile):
    pid = identity.create_player(conn, name, **profile)
    for season_year, team_id, *rest in seasons:
        conn.execute("INSERT INTO player_team_seasons (player_id, season_year, team_id, position, "
                     "jersey, class_year, source) VALUES (?, ?, ?, ?, ?, ?, 'cfbd')",
                     (pid, season_year, team_id, rest[0] if rest else None,
                      rest[1] if len(rest) > 1 else None, rest[2] if len(rest) > 2 else None))
    return pid


def _coach(conn, name, tenures):
    cur = conn.execute("INSERT INTO coaches (display_name, created_at, updated_at) "
                       "VALUES (?, '', '')", (name,))
    cid = int(cur.lastrowid)
    for season_year, team_id, hire_date in tenures:
        conn.execute("INSERT INTO coach_tenures (coach_id, team_id, season_year, hire_date, source) "
                     "VALUES (?, ?, ?, ?, 'cfbd')", (cid, team_id, season_year, hire_date))
    return cid


# --- shard keys -------------------------------------------------------------

@pytest.mark.parametrize("name,expected", [
    ("Cade Klubnik", "c"), ("D.J. Uiagalelei", "d"), ("'Amari Wilson", "a"),
    ("4th Stringer", "0"), ("", "_"), ("!!!", "_"),
])
def test_the_shard_key_is_a_safe_filename_for_any_name(name, expected):
    assert shard_key(name) == expected


def test_dotted_initials_shard_with_their_undotted_spelling():
    """Both spellings must land in the same shard, or search for one would never
    load the shard holding the other."""
    assert shard_key("D.J. Uiagalelei") == shard_key("DJ Uiagalelei")


# --- payloads ---------------------------------------------------------------

def test_a_missing_field_is_absent_rather_than_null(conn):
    """A page omits what it does not know. A null would have to be rendered as
    something, and 'N/A' everywhere is what the guide rules out."""
    _player(conn, "Sparse Person", [(2026, 1)])
    player = next(iter(build_players(conn).values()))
    assert "hometown" not in player and "height" not in player and "headshot_path" not in player
    assert player["display_name"] == "Sparse Person"
    assert "position" not in player["seasons"][0]


def test_a_person_with_no_season_is_not_exported(conn):
    """The roster that justified them is gone, so the page has nothing true to
    show. Unresolved people must not become empty pages."""
    identity.create_player(conn, "No Seasons")
    _player(conn, "Has A Season", [(2026, 1)])
    players = build_players(conn)
    assert [p["display_name"] for p in players.values()] == ["Has A Season"]


def test_a_transfer_carries_both_team_seasons_in_order(conn):
    pid = _player(conn, "Transfer Guy", [(2026, 2, "RB"), (2025, 1, "RB")])
    player = build_players(conn)[pid]
    assert [(s["season_year"], s["team_id"]) for s in player["seasons"]] == [(2025, 1), (2026, 2)]


def test_a_coach_carries_every_tenure_with_its_own_hire_date(conn):
    cid = _coach(conn, "Al Golden", [(2010, 1, "2005-12-08"), (2011, 2, "2010-12-12")])
    coach = build_coaches(conn)[cid]
    assert [(t["season_year"], t["team_id"], t["hire_date"]) for t in coach["tenures"]] == [
        (2010, 1, "2005-12-08"), (2011, 2, "2010-12-12")]


def test_a_name_only_identity_note_reaches_the_page(conn):
    """The weakness the database records has to be visible to a reader, not only
    to someone running SQL."""
    cid = _coach(conn, "Al Kincaid", [(1985, 1, None), (1990, 2, None)])
    identity.record_unresolved(conn, "coach", "cfbd", "coaches", "Al Kincaid",
                               "name-only identity with a gap after 1985", 1985)
    assert build_coaches(conn)[cid]["identity_note"] == \
        "name-only identity with a gap after 1985"


def test_rosters_group_by_season_and_team(conn):
    a = _player(conn, "Clemson Guy", [(2026, 1, "QB", 7, "JR")])
    b = _player(conn, "Oregon Guy", [(2026, 2)])
    c = _player(conn, "Older Guy", [(2025, 1)])
    rosters = build_rosters(build_players(conn))
    assert list(rosters[2026]["teams"]) == ["1", "2"]
    assert rosters[2026]["teams"]["1"] == [[a, "Clemson Guy", 7, "QB", "JR", None, None]]
    assert rosters[2026]["teams"]["2"] == [[b, "Oregon Guy", None, None, None, None, None]]
    assert [r[0] for r in rosters[2025]["teams"]["1"]] == [c]


def test_a_roster_row_carries_what_the_table_displays_not_only_an_id(conn):
    """Ids alone would make one team's roster depend on every player-detail shard
    in the country. The fields are positional, so their ORDER is the contract."""
    pid = _player(conn, "Cade Klubnik", [(2026, 1, "QB", 2, "JR")])
    roster = build_rosters(build_players(conn))[2026]
    assert roster["fields"] == ["player_id", "name", "jersey", "position", "class_year",
                               "height", "weight"]
    assert dict(zip(roster["fields"], roster["teams"]["1"][0])) == {
        "player_id": pid, "name": "Cade Klubnik", "jersey": 2, "position": "QB",
        "class_year": "JR", "height": None, "weight": None}


def test_a_roster_prefers_the_season_height_and_weight_over_the_person_record(conn):
    """A player's listed weight changes from one season to the next, so the row
    for 2026 must show the 2026 figure. The person-level value is the fallback
    for a season whose row never carried one."""
    pid = identity.create_player(conn, "Grown Guy", height=70, weight=180)
    conn.execute("INSERT INTO player_team_seasons (player_id, season_year, team_id, height, "
                 "weight, source) VALUES (?, 2025, 1, NULL, NULL, 'cfbd')", (pid,))
    conn.execute("INSERT INTO player_team_seasons (player_id, season_year, team_id, height, "
                 "weight, source) VALUES (?, 2026, 1, 73, 215, 'cfbd')", (pid,))
    rosters = build_rosters(build_players(conn))
    assert rosters[2026]["teams"]["1"][0][5:] == [73, 215]
    assert rosters[2025]["teams"]["1"][0][5:] == [70, 180]


def test_a_roster_is_ordered_by_jersey_with_the_unnumbered_last(conn):
    """The order a roster is published in. A player with no number sorts last,
    not first, which is where an empty string would have put them."""
    _player(conn, "Zeta Ninety", [(2026, 1, "QB", 90)])
    _player(conn, "Alpha None", [(2026, 1, "QB", None)])
    _player(conn, "Beta Nine", [(2026, 1, "QB", 9)])
    rows = build_rosters(build_players(conn))[2026]["teams"]["1"]
    assert [r[1] for r in rows] == ["Beta Nine", "Zeta Ninety", "Alpha None"]


def test_the_name_index_badges_each_entity_type(conn):
    """Search must be able to label a result without a second lookup."""
    pid = _player(conn, "Cade Klubnik", [(2026, 1, "QB")])
    cid = _coach(conn, "Cade Coach", [(2026, 1, None)])
    rows = build_name_index(build_players(conn), build_coaches(conn))["c"]
    assert ["p", pid, "Cade Klubnik", 1, "QB"] in rows
    assert ["c", cid, "Cade Coach", 1, "head coach"] in rows


# --- the emitted files ------------------------------------------------------

def test_every_emitted_file_parses_and_round_trips_through_a_js_runtime(conn, tmp_path):
    """The real check: a browser has to be able to read these. Driving the files
    through node proves the script wrapper and the JSON are both valid, which
    inspecting the Python payload would not."""
    _player(conn, "Cade Klubnik", [(2026, 1, "QB", 2, "JR")], first_name="Cade",
            last_name="Klubnik", hometown="Austin")
    _player(conn, "Transfer Guy", [(2025, 1, "RB"), (2026, 2, "RB")])
    _coach(conn, "Al Golden", [(2010, 1, "2005-12-08"), (2011, 2, "2010-12-12")])
    manifest = export(conn, tmp_path)

    assert manifest["counts"] == {"players": 2, "coaches": 1, "player_seasons": 3,
                                  "coach_seasons": 2, "seasons": [2025, 2026]}
    # Every manifest path must name a file that was actually written.
    for path in ([manifest["coaches"]] + list(manifest["players"].values())
                 + list(manifest["index"].values()) + list(manifest["rosters"].values())):
        name = path.split("/")[-1].split("?")[0]
        assert (tmp_path / name).exists(), f"{path} names a file that was not written"

    script = (
        "global.window = {};"
        "const fs = require('fs');"
        f"for (const f of fs.readdirSync({json.dumps(str(tmp_path))})) "
        f"  eval(fs.readFileSync(require('path').join({json.dumps(str(tmp_path))}, f), 'utf8'));"
        "const P = window.__CFB_PEOPLE_PLAYERS__, C = window.__CFB_PEOPLE_COACHES__,"
        "      I = window.__CFB_PEOPLE_INDEX__, R = window.__CFB_PEOPLE_ROSTERS__;"
        "const players = Object.values(P).reduce((n, s) => n + Object.keys(s).length, 0);"
        "console.log(JSON.stringify({shards: Object.keys(P).length, players,"
        "  coaches: Object.keys(C).length,"
        "  indexed: Object.values(I).reduce((n, s) => n + s.length, 0),"
        "  seasons: Object.keys(R).sort()}));")
    out = subprocess.run(["node", "-e", script], capture_output=True, text=True, check=True)
    assert json.loads(out.stdout) == {"shards": PLAYER_SHARDS, "players": 2, "coaches": 1,
                                      "indexed": 3, "seasons": ["2025", "2026"]}


def test_the_content_version_changes_when_the_data_does(conn, tmp_path):
    """The version is hashed into the manifest so a cached page can never be
    paired with a stale data file -- the same reason team_pages.js is hashed."""
    _player(conn, "First Person", [(2026, 1)])
    before = export(conn, tmp_path)["coaches"]
    _coach(conn, "A Coach", [(2026, 1, None)])
    assert export(conn, tmp_path)["coaches"] != before
