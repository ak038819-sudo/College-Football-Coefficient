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
from export_people_pages import (PLAYER_SHARDS, RECRUIT_SHARDS, TOP_SIGNEES, build_coach_recruits,
                                 build_coaches, build_name_index,
                                 build_players, build_rosters, build_team_coaches, export,
                                 shard_key)

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


def test_an_unusually_long_career_notes_only_the_player_who_earned_it(conn):
    """The caveat belongs to a player_id, not to a name. Two players share a name
    here, and only the one whose id spans too many seasons may carry the note --
    the whole point of the identity work is that a name is not a person."""
    long_id = _player(conn, "John Smith", [(y, 1) for y in range(2015, 2024)])
    short_id = _player(conn, "John Smith", [(2026, 1)])
    players = build_players(conn)
    assert players[long_id]["identity_note"] == "source id spanning 9 seasons at 1 school"
    assert "identity_note" not in players[short_id]


def test_a_negative_player_id_is_sharded_the_way_the_page_asks_for_it(conn, tmp_path):
    """29,162 of the archive's CFBD athlete ids are negative. The page computes
    its shard with a floored modulo, so the exporter must use the same one or
    those people are written to a file nothing ever loads."""
    pid = -1044360
    identity.create_player(conn, "Placeholder Id", player_id=pid)
    conn.execute("INSERT INTO player_team_seasons (player_id, season_year, team_id, source) "
                 "VALUES (?, 2020, 1, 'cfbd')", (pid,))
    manifest = export(conn, tmp_path)
    expected = pid % PLAYER_SHARDS          # floored: 56, not -56 and not abs()
    written = json.loads(subprocess.run(
        ["node", "-e", "global.window=global;"
         f"require({str(tmp_path / f'player_{expected}.js')!r});"
         "process.stdout.write(JSON.stringify(Object.keys("
         f"window.__CFB_PEOPLE_PLAYERS__[{expected}])))"],
        capture_output=True, text=True, check=True).stdout)
    assert written == [str(pid)]
    assert str(expected) in manifest["players"]


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


def test_a_person_is_indexed_under_every_word_of_their_name(conn):
    """How anyone actually searches. Sharding on the full name alone put Cade
    Klubnik in `c` only, so typing "Klubnik" loaded shard `k` and found nobody."""
    pid = _player(conn, "Cade Klubnik", [(2026, 1, "QB")])
    index = build_name_index(build_players(conn), {})
    assert [r[1] for r in index["c"]] == [pid]
    assert [r[1] for r in index["k"]] == [pid]
    assert sorted(index) == ["c", "k"]


def test_a_repeated_initial_does_not_list_a_person_twice_in_one_shard(conn):
    pid = _player(conn, "Cade Carter", [(2026, 1, "QB")])
    index = build_name_index(build_players(conn), {})
    assert [r[1] for r in index["c"]] == [pid]
    assert sorted(index) == ["c"]


def test_the_name_index_badges_each_entity_type(conn):
    """Search must be able to label a result without a second lookup."""
    pid = _player(conn, "Cade Klubnik", [(2026, 1, "QB")])
    cid = _coach(conn, "Cade Coach", [(2026, 1, None)])
    rows = build_name_index(build_players(conn), build_coaches(conn))["c"]
    assert ["p", pid, "Cade Klubnik", 1, "QB"] in rows
    assert ["c", cid, "Cade Coach", 1, "head coach"] in rows


def test_a_team_coach_index_names_one_coach_per_team_and_season(conn):
    """A team page needs one name, not 800 KB of careers."""
    a = _coach(conn, "Dan Lanning", [(2025, 2, None), (2026, 2, None)])
    b = _coach(conn, "Dabo Swinney", [(2026, 1, None)])
    index = build_team_coaches(build_coaches(conn))
    assert index[2026] == {"2": [a, "Dan Lanning"], "1": [b, "Dabo Swinney"]}
    assert index[2025] == {"2": [a, "Dan Lanning"]}


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
                                  "coach_seasons": 2, "seasons": [2025, 2026],
                                  "stat_seasons": [], "coach_recruit_points": 0,
                                  "coach_recruits_unlocated": 0,
                                  "coach_recruit_classes": 0}
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
        # Entries are one per word of a name, so the distinct people behind them
        # is the number that must match what was exported.
        "  people: new Set(Object.values(I).flat().map(r => r[0] + r[1])).size,"
        "  seasons: Object.keys(R).sort()}));")
    out = subprocess.run(["node", "-e", script], capture_output=True, text=True, check=True)
    read = json.loads(out.stdout)
    assert read == {"shards": PLAYER_SHARDS, "players": 2, "coaches": 1,
                    "indexed": 6, "seasons": ["2025", "2026"], "people": 3}, read


def test_the_content_version_changes_when_the_data_does(conn, tmp_path):
    """The version is hashed into the manifest so a cached page can never be
    paired with a stale data file -- the same reason team_pages.js is hashed."""
    _player(conn, "First Person", [(2026, 1)])
    before = export(conn, tmp_path)["coaches"]
    _coach(conn, "A Coach", [(2026, 1, None)])
    assert export(conn, tmp_path)["coaches"] != before


def test_a_partial_season_caveat_rides_with_that_season_not_the_player(conn):
    """It is true of one season, so it belongs beside that season's statistics.
    Hung on the player, a page would caveat a 2018 total because 2022 was
    split -- and a reader would stop believing the flag."""
    pid = _player(conn, "Split Person", [(2021, 1, "RB"), (2022, 1, "RB")])
    for year, yards in ((2021, 900), (2022, 41)):
        conn.execute("INSERT INTO player_season_stats (player_id, season_year, team_id, category, "
                     "stat_type, stat, source) VALUES (?, ?, 1, 'rushing', 'YDS', ?, 'cfbd')",
                     (pid, year, yards))
    conn.execute("INSERT INTO player_season_stat_caveats (player_id, season_year, reason, source) "
                 "VALUES (?, 2022, 'two athlete ids at Clemson in 2022', 'cfbd')", (pid,))

    player = build_players(conn)[pid]
    assert player["stats"]["2022"]["_partial"] == "two athlete ids at Clemson in 2022"
    assert "_partial" not in player["stats"]["2021"]


def test_a_caveat_for_a_season_with_no_statistics_is_not_invented(conn):
    """A caveat is a note ON a total. With no total to annotate there is nothing
    for a page to mark, and writing an empty season in would make the page
    render a heading for statistics it does not have."""
    pid = _player(conn, "No Stats", [(2022, 1, "RB")])
    conn.execute("INSERT INTO player_season_stat_caveats (player_id, season_year, reason, source) "
                 "VALUES (?, 2022, 'two athlete ids', 'cfbd')", (pid,))

    player = build_players(conn)[pid]
    assert "2022" not in player.get("stats", {})


# ---- the recruiting class a season carries -----------------------------------

def _class(conn, team_id, season, rank, points=100.0, source=None):
    conn.executescript((__import__('pathlib').Path(__file__).resolve().parents[1]
                        / 'sql' / 'recruiting_tables.sql').read_text(encoding='utf-8'))
    conn.execute("INSERT OR REPLACE INTO recruiting_classes "
                 "(source_team, season_year, team_id, class_rank, points) "
                 "VALUES (?, ?, ?, ?, ?)",
                 (source or TEAMS.get(team_id, str(team_id)), season, team_id, rank, points))


def test_a_season_carries_the_class_the_program_signed(conn):
    cid = _coach(conn, "Dabo Swinney", [(2020, 1, None), (2021, 1, None)])
    _class(conn, 1, 2020, 3, 290.5)
    _class(conn, 1, 2021, 7, 270.0)
    tenures = build_coaches(conn)[cid]["tenures"]
    assert [(t["season_year"], t["class_rank"]) for t in tenures] == [(2020, 3), (2021, 7)]
    assert tenures[0]["class_points"] == 290.5


def test_a_first_season_at_a_school_is_marked_as_inherited(conn):
    """A class is signed in February. Saban's first LSU class ranked 21st and
    his next four ranked 2nd, 21st, 3rd and 4th -- crediting him with the 21st
    would be crediting him with three weeks of work."""
    cid = _coach(conn, "Nick Saban", [(2007, 1, None), (2008, 1, None)])
    _class(conn, 1, 2007, 12)
    _class(conn, 1, 2008, 3)
    tenures = build_coaches(conn)[cid]["tenures"]
    assert tenures[0]["class_inherited"] is True
    assert "class_inherited" not in tenures[1]


def test_a_coach_who_returns_years_later_inherits_again(conn):
    """Mack Brown left North Carolina in 1997 and came back in 2019. Asking
    for the minimum season would call only 1988 a first year."""
    cid = _coach(conn, "Mack Brown", [(1996, 1, None), (1997, 1, None), (2019, 1, None), (2020, 1, None)])
    _class(conn, 1, 1996, 20)
    _class(conn, 1, 1997, 22)
    _class(conn, 1, 2019, 40)
    _class(conn, 1, 2020, 15)
    marked = [t["season_year"] for t in build_coaches(conn)[cid]["tenures"]
              if t.get("class_inherited")]
    assert marked == [1996, 2019]


def test_the_season_after_another_coach_is_inherited(conn):
    """The predecessor signed it, so the successor's first season is marked
    even though the program's seasons are continuous."""
    _coach(conn, "The Predecessor", [(2018, 1, None), (2019, 1, None)])
    successor = _coach(conn, "The Successor", [(2020, 1, None), (2021, 1, None)])
    for season, rank in ((2018, 10), (2019, 11), (2020, 12), (2021, 13)):
        _class(conn, 1, season, rank)
    tenures = build_coaches(conn)[successor]["tenures"]
    assert tenures[0].get("class_inherited") is True
    assert tenures[1].get("class_inherited") is None


def test_a_season_with_no_class_carries_no_key_at_all(conn):
    """Classes start in 2000, and most of this archive predates them. A key
    with a null in it would render as a cell rather than as nothing."""
    cid = _coach(conn, "Bear Bryant", [(1975, 1, None)])
    _class(conn, 1, 2020, 1)
    tenure = build_coaches(conn)[cid]["tenures"][0]
    assert "class_rank" not in tenure and "class_points" not in tenure


def test_an_unranked_class_still_carries_its_points(conn):
    """CFBD ranks only part of the field in the early seasons."""
    cid = _coach(conn, "Someone", [(2003, 1, None)])
    _class(conn, 1, 2003, None, 41.2)
    tenure = build_coaches(conn)[cid]["tenures"][0]
    assert "class_rank" not in tenure and tenure["class_points"] == 41.2


def test_a_class_belonging_to_another_team_is_not_attached(conn):
    cid = _coach(conn, "Someone", [(2020, 1, None)])
    _class(conn, 2, 2020, 1)
    assert "class_rank" not in build_coaches(conn)[cid]["tenures"][0]


def test_the_export_still_works_without_the_recruiting_tables(conn):
    """A database built before this data existed must still export."""
    cid = _coach(conn, "Someone", [(2020, 1, None)])
    assert build_coaches(conn)[cid]["tenures"][0]["season_year"] == 2020


# ---- where the signees came from ---------------------------------------------

def _recruit(conn, recruit_id, team_id, season, lat, lon, city="Mobile", state="AL",
             name="A Recruit", position=None, stars=None, rating=None, national_rank=None,
             athlete_id=None):
    # Deliberately no recruiting_classes row: a class with recruits but no
    # ranking is a real state of this archive (FIU 2022) and the map must be
    # right in it, so the fixture does not hand the builder one for free.
    conn.executescript((__import__('pathlib').Path(__file__).resolve().parents[1]
                        / 'sql' / 'recruiting_tables.sql').read_text(encoding='utf-8'))
    conn.execute(
        "INSERT OR REPLACE INTO recruits (recruit_id, season_year, name, committed_to, "
        "committed_team_id, city, state_province, hometown_latitude, hometown_longitude, "
        "position, stars, rating, national_rank, athlete_id) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (recruit_id, season, name, TEAMS.get(team_id, str(team_id)), team_id,
         city, state, lat, lon, position, stars, rating, national_rank, athlete_id))


def _only_class(out, cid):
    """The single class in a coach's export.

    The builder keeps one entry per class he signed, so a test about where a
    class came from asserts on that class rather than on a career total the
    page no longer has.
    """
    classes = out[cid]["classes"]
    assert len(classes) == 1, f"expected one class, got {[c['year'] for c in classes]}"
    return classes[0]


def test_a_coachs_map_holds_the_towns_his_signees_came_from(conn):
    cid = _coach(conn, "Dabo Swinney", [(2019, 1, None), (2020, 1, None)])
    _recruit(conn, 1, 1, 2020, 30.6954, -88.0399)
    coaches = build_coaches(conn)
    out = _only_class(build_coach_recruits(conn, coaches), cid)
    assert out == {"year": 2020, "team": 1, "points": [[30.7, -88.04, 1, "Mobile, AL"]],
                   "unlocated": 0,
                   "top": [["A Recruit", None, None, None, None, "Mobile, AL", None]]}


def test_signees_from_one_town_are_one_point_carrying_the_count(conn):
    """Eleven circles drawn on top of each other do not show eleven players."""
    cid = _coach(conn, "Dabo Swinney", [(2019, 1, None), (2020, 1, None)])
    for n, (lat, lon) in enumerate([(30.7012, -88.0399), (30.6991, -88.0402),
                                    (30.7034, -88.0398)]):
        _recruit(conn, n + 1, 1, 2020, lat, lon)
    assert _only_class(build_coach_recruits(conn, build_coaches(conn)), cid)["points"] == \
        [[30.7, -88.04, 3, "Mobile, AL"]]


def test_two_towns_a_kilometre_apart_stay_two_points(conn):
    """Rounding to two places merges a city; it must not merge its neighbour."""
    cid = _coach(conn, "Dabo Swinney", [(2019, 1, None), (2020, 1, None)])
    _recruit(conn, 1, 1, 2020, 30.69, -88.04, "Mobile", "AL")
    _recruit(conn, 2, 1, 2020, 30.72, -88.04, "Prichard", "AL")
    assert len(_only_class(build_coach_recruits(conn, build_coaches(conn)), cid)["points"]) == 2


def test_an_inherited_class_is_not_on_the_map(conn):
    """The map is a claim about where a coach goes for players. His first
    class was signed three weeks after he arrived, by someone else."""
    cid = _coach(conn, "Nick Saban", [(2007, 1, None), (2008, 1, None)])
    _recruit(conn, 1, 1, 2007, 30.69, -88.04, "Mobile", "AL")
    _recruit(conn, 2, 1, 2008, 33.52, -86.80, "Birmingham", "AL")
    assert _only_class(build_coach_recruits(conn, build_coaches(conn)), cid) \
        ["points"] == [[33.52, -86.8, 1, "Birmingham, AL"]]


def test_each_class_is_kept_apart_rather_than_summed(conn):
    """The career view is these classes merged in the browser. Summing them
    here would publish a total the page could not take back apart, and "where
    did he go for players in 2020" is the question the panel is for."""
    cid = _coach(conn, "Dabo Swinney", [(2019, 1, None), (2020, 1, None), (2021, 1, None)])
    _recruit(conn, 1, 1, 2020, 30.69, -88.04, "Mobile", "AL")
    _recruit(conn, 2, 1, 2021, 30.69, -88.04, "Mobile", "AL")
    _recruit(conn, 3, 1, 2021, 33.52, -86.80, "Birmingham", "AL")
    classes = build_coach_recruits(conn, build_coaches(conn))[cid]["classes"]
    assert [c["year"] for c in classes] == [2020, 2021], "oldest first"
    assert classes[0]["points"] == [[30.69, -88.04, 1, "Mobile, AL"]]
    # Tied counts order by name, so the bytes do not depend on SQLite row order.
    assert classes[1]["points"] == [[33.52, -86.8, 1, "Birmingham, AL"],
                                    [30.69, -88.04, 1, "Mobile, AL"]]


def test_a_class_carries_the_school_it_was_signed_for(conn):
    """A career spans schools, and a class drawn without one cannot say which
    program the pipeline fed."""
    cid = _coach(conn, "Nick Saban", [(2003, 1, None), (2004, 1, None),
                                      (2007, 2, None), (2008, 2, None)])
    _recruit(conn, 1, 1, 2004, 30.69, -88.04)
    _recruit(conn, 2, 2, 2008, 33.52, -86.80)
    assert [(c["year"], c["team"]) for c in
            build_coach_recruits(conn, build_coaches(conn))[cid]["classes"]] == \
        [(2004, 1), (2008, 2)]


def test_a_coach_whose_only_class_was_inherited_has_no_map(conn):
    cid = _coach(conn, "A Caretaker", [(2020, 1, None)])
    _recruit(conn, 1, 1, 2020, 30.69, -88.04)
    assert cid not in build_coach_recruits(conn, build_coaches(conn))


def test_another_teams_signees_are_not_on_this_coachs_map(conn):
    cid = _coach(conn, "Dabo Swinney", [(2019, 1, None), (2020, 1, None)])
    _recruit(conn, 1, 2, 2020, 44.05, -123.09, "Eugene", "OR")
    assert cid not in build_coach_recruits(conn, build_coaches(conn))


def test_a_signee_without_coordinates_is_counted_rather_than_placed(conn):
    """A recruit the feed gave no hometown for cannot be drawn anywhere --
    putting him at 0,0 would put him in the Gulf of Guinea -- but dropping him
    would publish a class total smaller than the class. 440 signees across the
    archive are in this state, 11 of them Lance Leipold's."""
    cid = _coach(conn, "Dabo Swinney", [(2019, 1, None), (2020, 1, None)])
    _recruit(conn, 1, 1, 2020, None, None)
    _recruit(conn, 2, 1, 2020, 30.69, -88.04)
    got = _only_class(build_coach_recruits(conn, build_coaches(conn)), cid)
    assert got["points"] == [[30.69, -88.04, 1, "Mobile, AL"]]
    assert got["unlocated"] == 1
    assert len(got["top"]) == 2, "a signee with no hometown is still a signee"


def test_a_coach_whose_every_signee_lacks_a_hometown_is_still_counted(conn):
    cid = _coach(conn, "Dabo Swinney", [(2019, 1, None), (2020, 1, None)])
    _recruit(conn, 1, 1, 2020, None, None)
    got = _only_class(build_coach_recruits(conn, build_coaches(conn)), cid)
    assert (got["points"], got["unlocated"]) == ([], 1)


def test_an_unplaceable_signee_of_an_inherited_class_is_not_counted_either(conn):
    cid = _coach(conn, "A Caretaker", [(2020, 1, None)])
    _recruit(conn, 1, 1, 2020, None, None)
    assert cid not in build_coach_recruits(conn, build_coaches(conn))


def test_a_first_season_is_inherited_even_with_no_ranking_row(conn):
    """class_inherited is only set where the archive also has a class RANK.
    FIU's 2022 class has 17 located recruits and no ranking row, so reading
    that flag put Butch Davis's last class on Mike MacIntyre's map."""
    _coach(conn, "Butch Davis", [(2020, 1, None), (2021, 1, None)])
    successor = _coach(conn, "Mike MacIntyre", [(2022, 1, None), (2023, 1, None)])
    _recruit(conn, 1, 1, 2022, 30.69, -88.04, "Mobile", "AL")
    _recruit(conn, 2, 1, 2023, 33.52, -86.80, "Birmingham", "AL")
    tenures = build_coaches(conn)[successor]["tenures"]
    assert "class_inherited" not in tenures[0], "the flag is absent without a ranked class"
    assert _only_class(build_coach_recruits(conn, build_coaches(conn)), successor) \
        ["points"] == [[33.52, -86.8, 1, "Birmingham, AL"]]


def test_two_towns_that_round_together_do_not_borrow_each_others_names(conn):
    """The feed has a "Hampton, SC" at Hampton, Virginia's coordinates. With
    one label cache for the whole archive, whichever row SQLite returned first
    named that point for every coach who ever signed anyone there."""
    first = _coach(conn, "First Coach", [(2019, 1, None), (2020, 1, None)])
    second = _coach(conn, "Second Coach", [(2019, 2, None), (2020, 2, None)])
    _recruit(conn, 1, 1, 2020, 37.05, -76.29, "Hampton", "SC")
    _recruit(conn, 2, 2, 2020, 37.05, -76.29, "Hampton", "VA")
    out = build_coach_recruits(conn, build_coaches(conn))
    assert _only_class(out, first)["points"][0][3] == "Hampton, SC"
    assert _only_class(out, second)["points"][0][3] == "Hampton, VA"


def test_the_busiest_town_comes_first(conn):
    cid = _coach(conn, "Dabo Swinney", [(2019, 1, None), (2020, 1, None)])
    _recruit(conn, 1, 1, 2020, 33.52, -86.80, "Birmingham", "AL")
    for n, _ in enumerate(range(2)):
        _recruit(conn, n + 2, 1, 2020, 30.69, -88.04, "Mobile", "AL")
    assert [p[3] for p in _only_class(build_coach_recruits(conn, build_coaches(conn)),
                                      cid)["points"]] == ["Mobile, AL", "Birmingham, AL"]


def test_towns_sending_the_same_number_are_ordered_by_name(conn):
    """Otherwise the export's byte content depends on SQLite's row order and
    every rebuild rewrites the file with a new hash."""
    cid = _coach(conn, "Dabo Swinney", [(2019, 1, None), (2020, 1, None)])
    _recruit(conn, 1, 1, 2020, 44.05, -123.09, "Eugene", "OR")
    _recruit(conn, 2, 1, 2020, 30.69, -88.04, "Mobile", "AL")
    assert [p[3] for p in _only_class(build_coach_recruits(conn, build_coaches(conn)),
                                      cid)["points"]] == ["Eugene, OR", "Mobile, AL"]


def test_a_hometown_with_no_name_is_still_a_point(conn):
    cid = _coach(conn, "Dabo Swinney", [(2019, 1, None), (2020, 1, None)])
    _recruit(conn, 1, 1, 2020, 30.69, -88.04, None, None)
    assert _only_class(build_coach_recruits(conn, build_coaches(conn)), cid)["points"] == \
        [[30.69, -88.04, 1, ""]]


# ---- the signees a class landed ---------------------------------------------

def test_a_class_names_its_signees_best_first(conn):
    cid = _coach(conn, "Nick Saban", [(2016, 1, None), (2017, 1, None)])
    _recruit(conn, 1, 1, 2017, 37.0, -121.8, "Antioch", "CA", "Najee Harris",
             "RB", 5, 0.9984, 2)
    _recruit(conn, 2, 1, 2017, 27.5, -82.6, "Bradenton", "FL", "Dylan Moses",
             "OLB", 5, 0.9921, 13)
    assert _only_class(build_coach_recruits(conn, build_coaches(conn)), cid)["top"] == [
        ["Najee Harris", "RB", 5, 0.9984, 2, "Antioch, CA", None],
        ["Dylan Moses", "OLB", 5, 0.9921, 13, "Bradenton, FL", None]]


def test_a_signee_the_feed_never_ranked_sorts_last_rather_than_first(conn):
    """national_rank is NULL on most pre-2010 rows and on the tail of every
    class since. Treating a missing rank as rank zero would hand a page's "top
    signees" to the players nobody rated."""
    cid = _coach(conn, "Dabo Swinney", [(2019, 1, None), (2020, 1, None)])
    _recruit(conn, 1, 1, 2020, 30.69, -88.04, name="Unranked", stars=3, rating=0.81)
    _recruit(conn, 2, 1, 2020, 33.52, -86.80, name="Ranked 300", stars=4,
             rating=0.89, national_rank=300)
    assert [s[0] for s in _only_class(build_coach_recruits(conn, build_coaches(conn)),
                                      cid)["top"]] == ["Ranked 300", "Unranked"]


def test_unranked_signees_fall_back_to_rating_and_then_to_stars(conn):
    """Named so the alphabet CONTRADICTS the right answer. The first version of
    this test called them Lower/Higher/Unrated, which happen to come out in the
    right order by name alone, so it passed against a key that never looked at
    the rating at all."""
    cid = _coach(conn, "Dabo Swinney", [(2019, 1, None), (2020, 1, None)])
    _recruit(conn, 1, 1, 2020, 30.69, -88.04, name="Aaron Worst", stars=4, rating=0.80)
    _recruit(conn, 2, 1, 2020, 30.69, -88.04, name="Zeke Best", stars=3, rating=0.88)
    _recruit(conn, 3, 1, 2020, 30.69, -88.04, name="Adam Unrated", stars=5)
    assert [s[0] for s in _only_class(build_coach_recruits(conn, build_coaches(conn)),
                                      cid)["top"]] == \
        ["Zeke Best", "Aaron Worst", "Adam Unrated"]


def test_stars_break_a_tie_on_rating(conn):
    cid = _coach(conn, "Dabo Swinney", [(2019, 1, None), (2020, 1, None)])
    _recruit(conn, 1, 1, 2020, 30.69, -88.04, name="Aaron Three", stars=3, rating=0.85)
    _recruit(conn, 2, 1, 2020, 30.69, -88.04, name="Zeke Five", stars=5, rating=0.85)
    assert [s[0] for s in _only_class(build_coach_recruits(conn, build_coaches(conn)),
                                      cid)["top"]] == ["Zeke Five", "Aaron Three"]


def test_the_cut_to_twelve_keeps_the_best_rated_not_the_alphabet(conn):
    """The export keeps only TOP_SIGNEES of a class, so whatever the key does
    not order, the cut decides and the browser can never get back: a class with
    more unranked signees than the export keeps would drop its best-rated
    players for being late in the alphabet. Twenty classes in the archive have
    more than twelve unranked signees."""
    cid = _coach(conn, "Dabo Swinney", [(2019, 1, None), (2020, 1, None)])
    # The best-rated signee is also the last alphabetically, so an alphabetical
    # cut loses him and a rating-ordered one keeps him first.
    for n in range(20):
        _recruit(conn, n + 1, 1, 2020, 30.69, -88.04,
                 name=f"{chr(ord('a') + n)} Recruit", stars=3, rating=0.50 + n / 100)
    top = [s[0] for s in _only_class(build_coach_recruits(conn, build_coaches(conn)),
                                     cid)["top"]]
    assert len(top) == TOP_SIGNEES
    assert top[0] == "t Recruit", "the best-rated signee must survive the cut"
    assert "a Recruit" not in top, "the worst-rated must not survive it for being first"


def test_the_export_order_is_the_order_the_page_sorts_by(conn):
    """The page re-sorts what it is given; if the two keys disagree, the cut
    above and the list a reader sees disagree about what the top of a class is.
    This pins the export's key to the three the shell's recruitTopSignees uses,
    in that order: rank present, rank, then rating, then stars."""
    shell = (__import__('pathlib').Path(__file__).resolve().parents[1]
             / 'ui' / 'dashboard_shell.html').read_text(encoding='utf-8')
    body = shell[shell.index('function recruitTopSignees('):]
    body = body[:body.index('\n}')]
    assert "(a.rank == null) - (b.rank == null)" in body
    assert "a.rank - b.rank" in body
    assert "(b.rating || 0) - (a.rating || 0)" in body
    assert "(b.stars || 0) - (a.stars || 0)" in body


def test_a_class_names_at_most_the_signees_the_page_can_show(conn):
    """Twelve a class, so a career top ten merged from them is exact: a
    career's tenth-best signee cannot be worse than tenth in his own class."""
    cid = _coach(conn, "Dabo Swinney", [(2019, 1, None), (2020, 1, None)])
    for n in range(40):
        _recruit(conn, n + 1, 1, 2020, 30.69, -88.04, name=f"Guy {n:02d}",
                 national_rank=n + 1)
    top = _only_class(build_coach_recruits(conn, build_coaches(conn)), cid)["top"]
    assert len(top) == TOP_SIGNEES
    assert [s[0] for s in top] == [f"Guy {n:02d}" for n in range(TOP_SIGNEES)]


def test_a_signee_is_linked_to_a_career_page_only_where_this_site_has_him(conn):
    """A link built from the recruiting feed's athlete id alone lands a reader
    on "player not found" for every signee who never made a roster here, and
    only about half the rows since 2010 carry an id at all."""
    cid = _coach(conn, "Dabo Swinney", [(2019, 1, None), (2020, 1, None)])
    _recruit(conn, 1, 1, 2020, 30.69, -88.04, name="Made It", athlete_id=4241457)
    _recruit(conn, 2, 1, 2020, 33.52, -86.80, name="Never Here", athlete_id=999999)
    _recruit(conn, 3, 1, 2020, 44.05, -123.09, name="No Id At All")
    out = build_coach_recruits(conn, build_coaches(conn), {4241457: {}})
    assert {s[0]: s[6] for s in _only_class(out, cid)["top"]} == \
        {"Made It": 4241457, "Never Here": None, "No Id At All": None}


def test_no_player_payloads_means_no_links_rather_than_broken_ones(conn):
    cid = _coach(conn, "Dabo Swinney", [(2019, 1, None), (2020, 1, None)])
    _recruit(conn, 1, 1, 2020, 30.69, -88.04, name="Made It", athlete_id=4241457)
    assert _only_class(build_coach_recruits(conn, build_coaches(conn)), cid)["top"][0][6] is None


def test_there_is_no_map_data_without_the_recruiting_tables(conn):
    """A database built before this data existed must still export."""
    _coach(conn, "Someone", [(2020, 1, None)])
    assert build_coach_recruits(conn, build_coaches(conn)) == {}


def test_the_export_writes_a_shard_per_coach_id(conn, tmp_path):
    cid = _coach(conn, "Dabo Swinney", [(2019, 1, None), (2020, 1, None)])
    _recruit(conn, 1, 1, 2020, 30.69, -88.04)
    manifest = export(conn, tmp_path)
    assert manifest["recruit_shards"] == RECRUIT_SHARDS
    assert len(manifest["coach_recruits"]) == RECRUIT_SHARDS
    assert manifest["counts"]["coach_recruit_points"] == 1
    assert manifest["counts"]["coach_recruits_unlocated"] == 0
    shard = tmp_path / f"coach_recruits_{cid % RECRUIT_SHARDS}.js"
    assert str(cid) in shard.read_text(encoding="utf-8")
    assert manifest["coach_recruits"][str(cid % RECRUIT_SHARDS)].startswith(
        f"data/people/coach_recruits_{cid % RECRUIT_SHARDS}.js?v=")


def test_every_declared_shard_file_exists_even_when_empty(conn, tmp_path):
    """The page asks for its own shard before it knows whether that shard has
    anything in it, and a 404 in the console is a bug report."""
    _coach(conn, "Someone", [(2020, 1, None)])
    manifest = export(conn, tmp_path)
    for shard, src in manifest["coach_recruits"].items():
        assert (tmp_path / src.split("?")[0].rsplit("/", 1)[1]).exists(), shard
