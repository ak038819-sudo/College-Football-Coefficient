"""Player and coach identity: the rules that stop the people graph from rotting.

These drive the real resolver and the real loaders against a real SQLite database
built from the project's own schema files. The point of every test here is a
behaviour that would be invisible in a passing build and expensive to discover
later: a transfer silently becoming two players, two same-name players merging
into one page, a re-run doubling a roster.
"""
import json
import sqlite3
from pathlib import Path

import pytest

import person_identity as identity
from load_coaches import coach_key, load_coaches
from load_rosters import load_roster

TEAMS = {1: "Clemson", 2: "Oregon", 3: "Ohio State"}


@pytest.fixture
def conn(repo_root):
    """A fresh database built the way db/league.db is: core schema, then people."""
    c = sqlite3.connect(":memory:")
    # Before any statement opens a transaction: SQLite silently ignores this
    # pragma inside one, which would leave the foreign keys below unenforced and
    # make a constraint test pass for the wrong reason.
    c.execute("PRAGMA foreign_keys = ON")
    c.executescript((repo_root / "sql" / "schema.sql").read_text(encoding="utf-8"))
    c.executescript((repo_root / "sql" / "person_tables.sql").read_text(encoding="utf-8"))
    for team_id, name in TEAMS.items():
        c.execute("INSERT INTO teams (team_id, team_name) VALUES (?, ?)", (team_id, name))
    c.execute("INSERT INTO team_aliases (alias, team_name) VALUES ('Ohio St', 'Ohio State')")
    return c


def _roster(tmp_path, year, rows):
    path = tmp_path / f"{year}.json"
    path.write_text(json.dumps([{"season_year": year, **row} for row in rows]), encoding="utf-8")
    return path


# --- schema -----------------------------------------------------------------

def test_person_tables_apply_to_a_fresh_schema_with_live_foreign_keys(conn):
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    assert {"players", "player_external_ids", "player_team_seasons", "coaches",
            "coach_external_ids", "coach_tenures", "person_unresolved"} <= tables
    player_id = identity.create_player(conn, "Test Player")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO player_team_seasons (player_id, season_year, team_id, source) "
                     "VALUES (?, 2026, 999, 'cfbd')", (player_id,))


def test_applying_the_person_schema_twice_keeps_existing_people(conn):
    identity.create_player(conn, "Keep Me")
    identity.apply_schema(conn)
    assert conn.execute("SELECT COUNT(*) FROM players").fetchone()[0] == 1


# --- name normalization -----------------------------------------------------

@pytest.mark.parametrize("variant", ["Jerome Gaillard Jr.", "Jerome Gaillard JR", "Jerome Gaillard"])
def test_suffix_and_punctuation_variants_share_one_lookup_key(variant):
    assert identity.normalize_name(variant) == "jerome gaillard"


@pytest.mark.parametrize("variant", ["D.J. Uiagalelei", "DJ Uiagalelei", "D J Uiagalelei"])
def test_dotted_and_undotted_initials_share_one_lookup_key(variant):
    """Stripping the periods from 'D.J.' leaves two tokens where 'DJ' leaves one.
    Without collapsing the run, a box-score line spelled one way would never
    match the roster row spelled the other."""
    assert identity.normalize_name(variant) == "dj uiagalelei"


def test_a_lone_middle_initial_is_not_glued_to_the_next_name():
    """Only runs of two or more collapse; gluing a single initial to a real name
    would invent a token no source ever wrote."""
    assert identity.normalize_name("John F Kennedy") == "john f kennedy"
    assert identity.normalize_name("John F. Kennedy") == "john f kennedy"


def test_an_id_less_box_score_line_matches_the_roster_spelled_the_other_way(conn):
    """The whole point of folding the key: resolution across spellings."""
    player_id, _ = identity.resolve_player(conn, "cfbd", "D.J. Uiagalelei", external_id="900",
                                           season_year=2025, team_id=1, position="QB")
    conn.execute("INSERT INTO player_team_seasons (player_id, season_year, team_id, position, source) "
                 "VALUES (?, 2025, 1, 'QB', 'cfbd')", (player_id,))
    matched, reason = identity.resolve_player(conn, "cfbd", "DJ Uiagalelei", season_year=2025,
                                              team_id=1, position="QB", context="boxscore",
                                              create=False)
    assert (matched, reason) == (player_id, "context")


def test_normalization_is_a_lookup_key_not_an_identity(conn):
    """Two same-name players must not merge just because their keys match."""
    a = identity.create_player(conn, "Mike Williams")
    identity.link_external_id(conn, a, "cfbd", "111", "id")
    b = identity.create_player(conn, "Mike Williams")
    identity.link_external_id(conn, b, "cfbd", "222", "id")
    assert a != b


# --- identity resolution ----------------------------------------------------

def test_one_athlete_id_across_seasons_is_one_player(conn):
    first, _ = identity.resolve_player(conn, "cfbd", "Cade Klubnik", external_id="4431", season_year=2025,
                                       team_id=1, position="QB")
    again, reason = identity.resolve_player(conn, "cfbd", "Cade Klubnik", external_id="4431",
                                            season_year=2026, team_id=1, position="QB")
    assert again == first and reason == "external id"
    assert conn.execute("SELECT COUNT(*) FROM players").fetchone()[0] == 1


def test_a_renamed_player_keeps_one_id_and_gains_an_alias(conn):
    first, _ = identity.resolve_player(conn, "cfbd", "D.J. Uiagalelei", external_id="900",
                                       season_year=2025, team_id=1)
    again, _ = identity.resolve_player(conn, "cfbd", "DJ Uiagalelei", external_id="900",
                                       season_year=2026, team_id=1)
    assert again == first
    # Both spellings fold to ONE key, so a feed using either reaches this person.
    aliases = {r[0] for r in conn.execute("SELECT alias FROM player_name_aliases WHERE player_id = ?",
                                          (first,))}
    assert aliases == {"dj uiagalelei"}


def test_a_box_score_line_can_link_but_never_create_a_person(conn):
    """An archived line with no athlete id must not mint people out of names."""
    player_id, _ = identity.resolve_player(conn, "cfbd", "Cade Klubnik", external_id="4431",
                                           season_year=2025, team_id=1, position="QB")
    conn.execute("INSERT INTO player_team_seasons (player_id, season_year, team_id, position, source) "
                 "VALUES (?, 2025, 1, 'QB', 'cfbd')", (player_id,))
    linked, reason = identity.resolve_player(conn, "cfbd", "Cade Klubnik", season_year=2025,
                                             team_id=1, position="QB", context="boxscore", create=False)
    assert (linked, reason) == (player_id, "context")

    unknown, reason = identity.resolve_player(conn, "cfbd", "Nobody At All", season_year=2025,
                                              team_id=1, position="QB", context="boxscore", create=False)
    assert unknown is None and reason == "no match"
    assert conn.execute("SELECT COUNT(*) FROM players").fetchone()[0] == 1
    assert conn.execute("SELECT reason FROM person_unresolved WHERE display_name = 'Nobody At All'"
                        ).fetchone()[0] == "no match"


def test_two_same_name_players_on_one_roster_are_a_collision_not_a_match(conn):
    for athlete_id in ("1", "2"):
        player_id, _ = identity.resolve_player(conn, "cfbd", "Mike Williams", external_id=athlete_id,
                                               season_year=2025, team_id=1, position="WR")
        conn.execute("INSERT INTO player_team_seasons (player_id, season_year, team_id, position, source) "
                     "VALUES (?, 2025, 1, 'WR', 'cfbd')", (player_id,))
    matched, reason = identity.resolve_player(conn, "cfbd", "Mike Williams", season_year=2025,
                                              team_id=1, position="WR", context="boxscore", create=False)
    assert matched is None and reason == "name collision"


def test_contextual_matching_will_not_cross_teams(conn):
    """Same name, same season, different school: a different person, so no match."""
    player_id, _ = identity.resolve_player(conn, "cfbd", "Mike Williams", external_id="1",
                                           season_year=2025, team_id=1, position="WR")
    conn.execute("INSERT INTO player_team_seasons (player_id, season_year, team_id, position, source) "
                 "VALUES (?, 2025, 1, 'WR', 'cfbd')", (player_id,))
    matched, reason = identity.resolve_player(conn, "cfbd", "Mike Williams", season_year=2025,
                                              team_id=2, position="WR", create=False)
    assert matched is None and reason == "no match"


def test_a_person_created_without_a_source_id_is_flagged_for_review(conn):
    player_id, reason = identity.resolve_player(conn, "manual", "No Id Here", season_year=2025, team_id=1)
    assert player_id is not None and reason == "created without source id"
    assert conn.execute("SELECT COUNT(*) FROM person_unresolved WHERE reason = 'created without source id'"
                        ).fetchone()[0] == 1


def test_team_resolution_goes_through_the_project_aliases(conn):
    assert identity.resolve_team_id(conn, "Ohio St") == 3
    assert identity.resolve_team_id(conn, "Ohio State") == 3
    assert identity.resolve_team_id(conn, "Some Division II School") is None


# --- roster ingestion -------------------------------------------------------

def test_rerunning_one_roster_season_is_idempotent(conn, tmp_path):
    rows = [{"athlete_id": "1", "name": "Cade Klubnik", "team": "Clemson", "position": "QB",
             "jersey": 2, "class_year": 3, "height": 73, "weight": 210},
            {"athlete_id": "2", "name": "Peter Woods", "team": "Clemson", "position": "DL"}]
    path = _roster(tmp_path, 2026, rows)
    def snapshot():
        return (conn.execute("SELECT player_id, display_name FROM players ORDER BY player_id").fetchall(),
                conn.execute("SELECT * FROM player_team_seasons ORDER BY player_id").fetchall(),
                conn.execute("SELECT * FROM player_external_ids ORDER BY player_id").fetchall())

    first = load_roster(conn, path)
    after_first = snapshot()
    second = load_roster(conn, path)
    # The second run resolves the same people instead of creating them, so the
    # created/matched split moves while the stored rows must not.
    assert snapshot() == after_first
    assert (first["loaded"], first["created"]) == (2, 2)
    assert (second["loaded"], second["created"], second["matched"]) == (2, 0, 2)
    assert conn.execute("SELECT COUNT(*) FROM players").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM player_team_seasons").fetchone()[0] == 2
    assert conn.execute("SELECT class_year FROM player_team_seasons WHERE jersey = 2"
                        ).fetchone()[0] == "JR"


def test_rerunning_a_roster_with_no_athlete_ids_does_not_duplicate_people(conn, tmp_path):
    """A row with no athlete id can only be recognised by its existing season
    row, so deleting the season before resolving would hide the only evidence
    the match has -- and every run would create another person, orphaning the
    last. Three runs, because the second alone can look fine by accident."""
    path = _roster(tmp_path, 2026, [{"name": "No Id Player", "team": "Clemson", "position": "QB"},
                                    {"name": "Also No Id", "team": "Oregon", "position": "RB"}])
    first = load_roster(conn, path)
    assert (first["loaded"], first["created"]) == (2, 2)
    for _ in range(2):
        again = load_roster(conn, path)
        assert (again["loaded"], again["created"], again["matched"]) == (2, 0, 2)
    assert conn.execute("SELECT COUNT(*) FROM players").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM player_team_seasons").fetchone()[0] == 2
    # No person left behind without the season row that justifies them.
    assert conn.execute("SELECT COUNT(*) FROM players p WHERE NOT EXISTS ("
                        "SELECT 1 FROM player_team_seasons s WHERE s.player_id = p.player_id)"
                        ).fetchone()[0] == 0


def test_an_id_less_roster_row_still_leaves_the_season_when_it_drops_off(conn, tmp_path):
    """Resolving before the delete must not cost the wholesale replacement."""
    load_roster(conn, _roster(tmp_path, 2026, [
        {"name": "Stays On", "team": "Clemson"}, {"name": "Drops Off", "team": "Clemson"}]))
    load_roster(conn, _roster(tmp_path, 2026, [{"name": "Stays On", "team": "Clemson"}]))
    assert [r[0] for r in conn.execute(
        "SELECT p.display_name FROM player_team_seasons s JOIN players p USING (player_id) "
        "WHERE s.season_year = 2026")] == ["Stays On"]


def test_a_transfer_is_two_team_seasons_for_one_player(conn, tmp_path):
    load_roster(conn, _roster(tmp_path, 2025, [
        {"athlete_id": "77", "name": "Transfer Guy", "team": "Clemson", "position": "RB"}]))
    load_roster(conn, _roster(tmp_path, 2026, [
        {"athlete_id": "77", "name": "Transfer Guy", "team": "Oregon", "position": "RB"}]))
    assert conn.execute("SELECT COUNT(*) FROM players").fetchone()[0] == 1
    assert conn.execute("SELECT season_year, team_id FROM player_team_seasons ORDER BY season_year"
                        ).fetchall() == [(2025, 1), (2026, 2)]
    assert conn.execute("SELECT latest_season FROM players").fetchone()[0] == 2026


def test_a_player_who_leaves_the_roster_leaves_that_season(conn, tmp_path):
    load_roster(conn, _roster(tmp_path, 2026, [
        {"athlete_id": "1", "name": "Stays On", "team": "Clemson"},
        {"athlete_id": "2", "name": "Transfers Out", "team": "Clemson"}]))
    load_roster(conn, _roster(tmp_path, 2026, [
        {"athlete_id": "1", "name": "Stays On", "team": "Clemson"}]))
    assert conn.execute("SELECT COUNT(*) FROM player_team_seasons WHERE season_year = 2026"
                        ).fetchone()[0] == 1
    # The person is kept: they exist, and may appear in another season's roster.
    assert conn.execute("SELECT COUNT(*) FROM players").fetchone()[0] == 2


def test_a_school_outside_this_database_is_recorded_not_invented(conn, tmp_path):
    stats = load_roster(conn, _roster(tmp_path, 2026, [
        {"athlete_id": "1", "name": "Real Guy", "team": "Clemson"},
        {"athlete_id": "2", "name": "Fcs Guy", "team": "Mercer"}]))
    assert stats["unknown_team"] == 1 and stats["loaded"] == 1
    assert conn.execute("SELECT COUNT(*) FROM teams WHERE team_name = 'Mercer'").fetchone()[0] == 0
    assert conn.execute("SELECT source_team FROM person_unresolved WHERE reason = 'unknown team'"
                        ).fetchone()[0] == "Mercer"


def test_missing_bio_fields_stay_null_rather_than_becoming_zero(conn, tmp_path):
    load_roster(conn, _roster(tmp_path, 2026, [
        {"athlete_id": "1", "name": "Sparse Row", "team": "Clemson"}]))
    row = conn.execute("SELECT s.height, s.weight, p.hometown, s.jersey "
                       "FROM player_team_seasons s JOIN players p USING (player_id)").fetchone()
    assert row == (None, None, None, None)


def test_a_roster_row_with_no_name_is_skipped(conn, tmp_path):
    stats = load_roster(conn, _roster(tmp_path, 2026, [
        {"athlete_id": "1", "name": "", "team": "Clemson"},
        {"athlete_id": "2", "name": "Real Guy", "team": "Clemson"}]))
    assert stats["loaded"] == 1
    assert conn.execute("SELECT display_name FROM players").fetchall() == [("Real Guy",)]


# --- coach ingestion --------------------------------------------------------

def _coaches(tmp_path, records):
    path = tmp_path / "coaches.json"
    path.write_text(json.dumps(records), encoding="utf-8")
    return path


def test_rerunning_the_coaching_snapshot_creates_no_duplicate_coaches(conn, tmp_path):
    path = _coaches(tmp_path, [{"name": "Dabo Swinney", "hire_date": "2008-10-13", "seasons": [
        {"year": 2024, "school": "Clemson", "games": 13, "wins": 10, "losses": 4, "ties": 0,
         "postseason_rank": 16},
        {"year": 2025, "school": "Clemson", "games": 12, "wins": 9, "losses": 3, "ties": 0}]}])
    first = load_coaches(conn, path)
    second = load_coaches(conn, path)
    assert first == second == {"coaches": 1, "tenures": 2, "unknown_team": 0,
                               "collisions": 0, "skipped": 0}
    assert conn.execute("SELECT COUNT(*) FROM coaches").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM coach_tenures").fetchone()[0] == 2
    assert conn.execute("SELECT latest_season FROM coaches").fetchone()[0] == 2025


def test_a_coach_at_two_schools_is_one_coach_with_a_chronological_tenure(conn, tmp_path):
    load_coaches(conn, _coaches(tmp_path, [{"name": "Moving Coach", "hire_date": "2015-01-01",
                                            "seasons": [{"year": 2016, "school": "Oregon"},
                                                        {"year": 2017, "school": "Ohio St"}]}]))
    assert conn.execute("SELECT COUNT(*) FROM coaches").fetchone()[0] == 1
    assert conn.execute("SELECT season_year, team_id FROM coach_tenures ORDER BY season_year"
                        ).fetchall() == [(2016, 2), (2017, 3)]


def test_two_coaches_sharing_a_name_and_hire_date_stay_two_people(conn, tmp_path):
    stats = load_coaches(conn, _coaches(tmp_path, [
        {"name": "Bobby Johnson", "hire_date": None, "seasons": [{"year": 1990, "school": "Oregon"}]},
        {"name": "Bobby Johnson", "hire_date": None, "seasons": [{"year": 2005, "school": "Clemson"}]}]))
    assert stats["coaches"] == 2 and stats["collisions"] == 1
    assert conn.execute("SELECT COUNT(DISTINCT coach_id) FROM coach_tenures").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM person_unresolved WHERE entity = 'coach'"
                        ).fetchone()[0] == 1


def test_a_corrected_season_record_replaces_the_old_one(conn, tmp_path):
    load_coaches(conn, _coaches(tmp_path, [{"name": "Dabo Swinney", "hire_date": "2008-10-13",
                                            "seasons": [{"year": 2025, "school": "Clemson", "wins": 8}]}]))
    load_coaches(conn, _coaches(tmp_path, [{"name": "Dabo Swinney", "hire_date": "2008-10-13",
                                            "seasons": [{"year": 2025, "school": "Clemson", "wins": 9}]}]))
    assert conn.execute("SELECT wins FROM coach_tenures").fetchone()[0] == 9
    assert conn.execute("SELECT COUNT(*) FROM coaches").fetchone()[0] == 1


def test_every_loaded_tenure_is_head_coach_and_keeps_the_source_school_string(conn, tmp_path):
    load_coaches(conn, _coaches(tmp_path, [{"name": "Some Coach", "hire_date": None,
                                            "seasons": [{"year": 2020, "school": "Ohio St"}]}]))
    assert conn.execute("SELECT role, source_team FROM coach_tenures").fetchone() == ("head coach", "Ohio St")


def test_a_coach_season_at_an_unknown_school_is_recorded_not_dropped_silently(conn, tmp_path):
    stats = load_coaches(conn, _coaches(tmp_path, [
        {"name": "Fcs Coach", "hire_date": None, "seasons": [{"year": 2020, "school": "Mercer"},
                                                             {"year": 2021, "school": "Clemson"}]}]))
    assert stats["unknown_team"] == 1 and stats["tenures"] == 1
    assert conn.execute("SELECT season_year FROM person_unresolved WHERE entity = 'coach' "
                        "AND reason = 'unknown team'").fetchone()[0] == 2020


def test_the_derived_coach_key_ignores_name_punctuation_but_not_the_hire_date():
    assert coach_key({"name": "Dabo Swinney", "hire_date": "2008-10-13"}) == \
        coach_key({"name": "Dabo  Swinney Jr.", "hire_date": "2008-10-13"})
    assert coach_key({"name": "Dabo Swinney", "hire_date": "2008-10-13"}) != \
        coach_key({"name": "Dabo Swinney", "hire_date": None})


# --- fetchers ---------------------------------------------------------------
# The HTTP calls themselves are not tested; what matters is that a bad or empty
# response can never overwrite a committed snapshot, and that the shapes the
# loaders above read are the shapes the fetchers actually write.

def test_roster_rows_reduce_both_cfbd_name_styles_and_drop_nameless_rows():
    import fetch_cfbd_rosters as fr
    rows = fr.roster_rows([
        {"id": 4431, "firstName": "Cade", "lastName": "Klubnik", "team": "Clemson",
         "jersey": "2", "position": "QB", "year": 3, "height": 73, "weight": 210,
         "homeCity": "Austin", "homeState": "TX"},
        {"id": 99, "first_name": "Snake", "last_name": "Case", "school": "Oregon"},
        {"id": 100, "team": "Clemson"}], 2026)
    assert [r["name"] for r in rows] == ["Cade Klubnik", "Snake Case"]
    assert rows[0] == {"athlete_id": "4431", "first_name": "Cade", "last_name": "Klubnik",
                       "name": "Cade Klubnik", "team": "Clemson", "season_year": 2026,
                       "jersey": 2, "position": "QB", "class_year": 3, "height": 73,
                       "weight": 210, "hometown": "Austin", "home_state": "TX"}
    assert rows[1]["team"] == "Oregon" and rows[1]["jersey"] is None


def test_a_failed_or_empty_roster_fetch_keeps_the_committed_snapshot(tmp_path):
    import fetch_cfbd_rosters as fr
    import requests
    (tmp_path / "2026.json").write_text("committed", encoding="utf-8")
    original = requests.get
    try:
        requests.get = lambda *a, **k: (_ for _ in ()).throw(requests.ConnectionError("offline"))
        assert fr.write_season(2026, {}, tmp_path) is None
        requests.get = lambda *a, **k: type("R", (), {"raise_for_status": lambda s: None,
                                                      "json": lambda s: []})()
        assert fr.write_season(2026, {}, tmp_path) is None
    finally:
        requests.get = original
    assert (tmp_path / "2026.json").read_text(encoding="utf-8") == "committed"


def test_a_roster_season_before_cfbd_coverage_is_refused_not_written_empty(tmp_path):
    import fetch_cfbd_rosters as fr
    assert fr.write_season(fr.FIRST_SEASON - 1, {}, tmp_path) is None
    assert not list(tmp_path.glob("*.json"))


def test_coach_records_keep_head_coaching_seasons_and_drop_recordless_coaches():
    import fetch_cfbd_coaches as fc
    records = fc.coach_records([
        {"firstName": "Dabo", "lastName": "Swinney", "hireDate": "2008-10-13",
         "seasons": [{"year": 2025, "school": "Clemson", "wins": 9, "losses": 3,
                      "postseasonRank": 16}, {"school": "Clemson"}]},
        {"firstName": "No", "lastName": "Seasons", "seasons": []},
        {"seasons": [{"year": 2025, "school": "Oregon"}]}])
    assert len(records) == 1
    assert records[0]["name"] == "Dabo Swinney" and records[0]["hire_date"] == "2008-10-13"
    assert records[0]["seasons"] == [{"year": 2025, "school": "Clemson", "games": None, "wins": 9,
                                      "losses": 3, "ties": None, "preseason_rank": None,
                                      "postseason_rank": 16}]


def test_disjoint_careers_under_one_name_are_not_merged_by_the_fetcher():
    """Grouping on name and hire date alone blends two coaches into one record,
    and the loader -- seeing a single record -- never gets to flag the
    collision, so the blend is permanent. CFBD omits hire dates for older
    seasons, which is exactly when this bites."""
    import fetch_cfbd_coaches as fc
    merged = fc.merge_records([
        {"name": "Bobby Johnson", "hire_date": None, "seasons": [{"year": 1990, "school": "Oregon"}]},
        {"name": "Bobby Johnson", "hire_date": None, "seasons": [{"year": 2005, "school": "Clemson"}]}])
    assert len(merged) == 2
    assert [[s["year"] for s in r["seasons"]] for r in merged] == [[1990], [2005]]


def test_slices_that_share_a_season_are_folded_into_one_career():
    """A slice overlapping two clusters joins them: one person, not three."""
    import fetch_cfbd_coaches as fc
    merged = fc.merge_records([
        {"name": "B Coach", "hire_date": None, "seasons": [{"year": 2000, "school": "Oregon"}]},
        {"name": "B Coach", "hire_date": None, "seasons": [{"year": 2002, "school": "Clemson"}]},
        {"name": "B Coach", "hire_date": None, "seasons": [{"year": 2000, "school": "Oregon"},
                                                           {"year": 2002, "school": "Clemson"}]}])
    assert len(merged) == 1
    assert [s["year"] for s in merged[0]["seasons"]] == [2000, 2002]


def test_the_fetcher_and_loader_together_keep_two_same_name_coaches_apart(tmp_path):
    """End to end, because each half looked correct on its own: the fetcher
    merged the pair, so the loader's collision branch never ran."""
    import json as _json
    import fetch_cfbd_coaches as fc
    merged = fc.merge_records([
        {"name": "Bobby Johnson", "hire_date": None, "seasons": [{"year": 1990, "school": "Oregon"}]},
        {"name": "Bobby Johnson", "hire_date": None, "seasons": [{"year": 2005, "school": "Clemson"}]}])
    path = tmp_path / "coaches.json"
    path.write_text(_json.dumps(merged), encoding="utf-8")

    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    repo = Path(__file__).resolve().parent.parent
    conn.executescript((repo / "sql" / "schema.sql").read_text(encoding="utf-8"))
    conn.executescript((repo / "sql" / "person_tables.sql").read_text(encoding="utf-8"))
    for team_id, name in TEAMS.items():
        conn.execute("INSERT INTO teams (team_id, team_name) VALUES (?, ?)", (team_id, name))

    stats = load_coaches(conn, path)
    assert stats["coaches"] == 2 and stats["collisions"] == 1
    assert conn.execute("SELECT COUNT(DISTINCT coach_id) FROM coach_tenures").fetchone()[0] == 2


def test_year_slices_merge_into_one_career_per_coach():
    import fetch_cfbd_coaches as fc
    merged = fc.merge_records([
        {"name": "A Coach", "first_name": "A", "last_name": "Coach", "hire_date": "2010-01-01",
         "seasons": [{"year": 2011, "school": "Oregon"}]},
        {"name": "A Coach", "first_name": "A", "last_name": "Coach", "hire_date": "2010-01-01",
         "seasons": [{"year": 2011, "school": "Oregon"}, {"year": 2012, "school": "Oregon"}]},
        {"name": "A Coach", "first_name": "A", "last_name": "Coach", "hire_date": None,
         "seasons": [{"year": 1995, "school": "Clemson"}]}])
    assert len(merged) == 2
    career = next(r for r in merged if r["hire_date"] == "2010-01-01")
    assert [s["year"] for s in career["seasons"]] == [2011, 2012]


def test_a_failed_year_slice_abandons_the_whole_coach_fetch(tmp_path):
    """A partial pull would erase careers from the snapshot, so it is never written."""
    import fetch_cfbd_coaches as fc
    import requests
    out = tmp_path / "coaches.json"
    out.write_text("committed", encoding="utf-8")
    calls = []

    def flaky(*a, **k):
        calls.append(k.get("params", {}).get("year"))
        if len(calls) > 1:
            raise requests.ConnectionError("offline")
        return type("R", (), {"raise_for_status": lambda s: None, "json": lambda s: [
            {"firstName": "A", "lastName": "Coach",
             "seasons": [{"year": 2024, "school": "Clemson"}]}]})()

    original = requests.get
    requests.get = flaky
    try:
        assert fc.write_coaches(2024, 2026, {}, out) is None
    finally:
        requests.get = original
    assert out.read_text(encoding="utf-8") == "committed"


# --- against the real database ----------------------------------------------

def _real_db_copy(db_path):
    """In-memory copy, so these never read from or write to the real league.db."""
    src = sqlite3.connect(str(db_path))
    mem = sqlite3.connect(":memory:")
    src.backup(mem)
    src.close()
    return mem


def test_the_person_tables_apply_to_the_real_database_without_touching_it(db_path, repo_root):
    """Adding people to an already-built league.db must not disturb what is there."""
    conn = _real_db_copy(db_path)
    before = {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
              for t in ("teams", "games", "team_membership_by_season")}
    identity.apply_schema(conn)
    identity.apply_schema(conn)  # a rebuild applies it again
    after = {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in before}
    assert after == before
    assert conn.execute("SELECT COUNT(*) FROM players").fetchone()[0] == 0


def test_real_team_names_and_aliases_resolve_for_roster_ingestion(db_path, tmp_path):
    """Roster ingestion against the real team table: real schools land on real
    team_ids through the project's own aliases, and a school outside this
    FBS-only database is counted rather than created."""
    conn = _real_db_copy(db_path)
    names = [r[0] for r in conn.execute("SELECT team_name FROM teams ORDER BY team_name LIMIT 5")]
    alias = conn.execute("SELECT alias FROM team_aliases LIMIT 1").fetchone()[0]
    rows = [{"season_year": 2026, "athlete_id": str(i), "name": f"Player {i}", "team": team,
             "position": "QB"} for i, team in enumerate(names + [alias], start=1)]
    rows.append({"season_year": 2026, "athlete_id": "999", "name": "Fcs Player",
                 "team": "Not A Real School At All"})
    path = tmp_path / "2026.json"
    path.write_text(json.dumps(rows), encoding="utf-8")

    stats = load_roster(conn, path)
    assert stats["loaded"] == len(names) + 1 and stats["unknown_team"] == 1
    assert conn.execute(
        "SELECT COUNT(*) FROM player_team_seasons s JOIN teams t USING (team_id) "
        "WHERE s.season_year = 2026").fetchone()[0] == len(names) + 1
    # Still idempotent with the real alias table in play.
    assert load_roster(conn, path)["loaded"] == stats["loaded"]
    assert conn.execute("SELECT COUNT(*) FROM players").fetchone()[0] == len(names) + 1
