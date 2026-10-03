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


# --- coach ingestion -------------------------------------------------------
# Shaped like the LIVE feed, which a full 1980-2026 pull settled: one record per
# SEASON, and hire_date belonging to the job rather than the person. Al Golden
# arrives as ten records, five under each of his two hire dates.


def _coaches(tmp_path, records):
    path = tmp_path / "coaches.json"
    path.write_text(json.dumps(records), encoding="utf-8")
    return path


def _season(year, school, hire_date=None, **extra):
    return {"year": year, "school": school, "hire_date": hire_date, **extra}


def test_one_season_per_record_is_assembled_into_one_career(tmp_path):
    """The shape the feed really sends. Grouping on name and hire date would make
    this coach two people; grouping on overlapping seasons would make them five."""
    import fetch_cfbd_coaches as fc
    slices = ([{"name": "Al Golden", "first_name": "Al", "last_name": "Golden",
                "hire_date": "2005-12-08", "seasons": [{"year": y, "school": "Oregon"}]}
               for y in range(2006, 2011)]
              + [{"name": "Al Golden", "first_name": "Al", "last_name": "Golden",
                  "hire_date": "2010-12-12", "seasons": [{"year": y, "school": "Clemson"}]}
                 for y in range(2011, 2016)])
    merged = fc.merge_records(slices)
    assert len(merged) == 1
    assert [s["year"] for s in merged[0]["seasons"]] == list(range(2006, 2016))
    # The hire date rides the season, which is where it is true.
    by_year = {s["year"]: s["hire_date"] for s in merged[0]["seasons"]}
    assert by_year[2006] == "2005-12-08" and by_year[2011] == "2010-12-12"


def test_the_coach_key_is_the_name_and_not_the_hire_date():
    """The feed offers no person-level signal beyond the name, so the key cannot
    include the hire date -- that would split a coach at every job change."""
    assert coach_key({"name": "Al Golden", "hire_date": "2005-12-08"}) == \
        coach_key({"name": "Al  Golden", "hire_date": "2010-12-12"})
    assert coach_key({"name": "A.J. Golden"}) == coach_key({"name": "AJ Golden"})


def test_the_coach_key_keeps_a_generational_suffix():
    """Mike Sanford Sr. (UNLV 2005-2009) and Mike Sanford Jr. (Western Kentucky
    and Colorado 2017-2022) are a father and son who both held FBS head-coaching
    jobs. Folding the suffix put both careers under one coach_id -- a false
    statement on a page that no later correction could detect."""
    assert coach_key({"name": "Mike Sanford Jr."}) != coach_key({"name": "Mike Sanford Sr."})


def test_two_coaches_differing_only_by_a_suffix_are_flagged(conn, tmp_path):
    """The inverse risk of keeping the suffix: a source omitting it on some rows
    splits one person. Recorded, so that failure is visible too."""
    stats = load_coaches(conn, _coaches(tmp_path, [
        {"name": "Mike Sanford Sr.", "hire_date": None,
         "seasons": [_season(y, "Clemson", "2005-01-01") for y in range(2005, 2010)]},
        {"name": "Mike Sanford Jr.", "hire_date": None,
         "seasons": [_season(y, "Oregon", "2017-01-01") for y in range(2017, 2020)]}]))
    assert stats["coaches"] == 2
    assert conn.execute(
        "SELECT display_name FROM person_unresolved WHERE reason = "
        "'names differing only by a generational suffix'").fetchone()[0] == \
        "Mike Sanford Jr. / Mike Sanford Sr."


def test_a_database_keyed_by_the_previous_loader_is_migrated_not_duplicated(conn, tmp_path):
    """The first version keyed on name|hire-date. Without migration the next run
    inserts a second coach per name, moves the tenures to it, and orphans the
    old row under a supposedly immutable id."""
    cur = conn.execute("INSERT INTO coaches (display_name, created_at, updated_at) "
                       "VALUES ('Dabo Swinney', '', '')")
    legacy_id = int(cur.lastrowid)
    conn.execute("INSERT INTO coach_external_ids (coach_id, source, external_id, confidence, "
                 "is_primary) VALUES (?, 'cfbd', 'cfbd:dabo swinney|2008-10-13', 'derived key', 1)",
                 (legacy_id,))
    conn.execute("INSERT INTO coach_tenures (coach_id, team_id, season_year, source) "
                 "VALUES (?, 1, 2024, 'cfbd')", (legacy_id,))

    load_coaches(conn, _coaches(tmp_path, [{
        "name": "Dabo Swinney", "hire_date": None,
        "seasons": [_season(2024, "Clemson", "2008-10-13"),
                    _season(2025, "Clemson", "2008-10-13")]}]))

    assert conn.execute("SELECT COUNT(*) FROM coaches").fetchone()[0] == 1
    # The id survives the migration rather than being reissued.
    assert conn.execute("SELECT coach_id FROM coaches").fetchone()[0] == legacy_id
    assert conn.execute("SELECT external_id FROM coach_external_ids").fetchall() == \
        [("cfbd:dabo swinney",)]
    assert conn.execute("SELECT COUNT(*) FROM coaches WHERE coach_id NOT IN "
                        "(SELECT coach_id FROM coach_tenures)").fetchone()[0] == 0


def test_a_legacy_row_folds_into_a_coach_that_already_holds_the_current_key(conn, tmp_path):
    """The half-migrated database: one run of the new loader created a coach under
    `cfbd:<name>`, and a legacy `cfbd:<name>|<hire date>` row is still there.

    The current key can only belong to one coach, so the legacy row has to fold
    into the coach that already holds it. Taking the legacy id as the keeper
    instead leaves it behind with the old key, and the next load quietly keeps
    two Dabo Swinneys -- which is the duplication this migration exists to stop.
    """
    cur = conn.execute("INSERT INTO coaches (display_name, created_at, updated_at) "
                       "VALUES ('Dabo Swinney', '', '')")
    legacy_id = int(cur.lastrowid)
    conn.execute("INSERT INTO coach_external_ids (coach_id, source, external_id, confidence, "
                 "is_primary) VALUES (?, 'cfbd', 'cfbd:dabo swinney|2008-10-13', 'derived key', 1)",
                 (legacy_id,))
    conn.execute("INSERT INTO coach_tenures (coach_id, team_id, season_year, source) "
                 "VALUES (?, 1, 2023, 'cfbd')", (legacy_id,))
    cur = conn.execute("INSERT INTO coaches (display_name, created_at, updated_at) "
                       "VALUES ('Dabo Swinney', '', '')")
    current_id = int(cur.lastrowid)
    conn.execute("INSERT INTO coach_external_ids (coach_id, source, external_id, confidence, "
                 "is_primary) VALUES (?, 'cfbd', 'cfbd:dabo swinney', 'name only', 1)",
                 (current_id,))
    assert legacy_id < current_id, "the legacy id must be the lower one for this to bite"

    load_coaches(conn, _coaches(tmp_path, [{
        "name": "Dabo Swinney", "hire_date": None,
        "seasons": [_season(2024, "Clemson", "2008-10-13")]}]))

    assert conn.execute("SELECT coach_id FROM coaches").fetchall() == [(current_id,)]
    assert conn.execute("SELECT external_id FROM coach_external_ids").fetchall() == \
        [("cfbd:dabo swinney",)]
    assert conn.execute("SELECT COUNT(*) FROM coach_tenures WHERE coach_id = ?",
                        (legacy_id,)).fetchone()[0] == 0


def test_legacy_rows_split_across_hire_dates_collapse_onto_one_coach(conn, tmp_path):
    """The old key gave a coach one row per job. Those extra rows are the split
    this release undoes, so they go rather than linger as orphans."""
    ids = []
    for hire in ("2005-12-08", "2010-12-12"):
        cur = conn.execute("INSERT INTO coaches (display_name, created_at, updated_at) "
                           "VALUES ('Al Golden', '', '')")
        ids.append(int(cur.lastrowid))
        conn.execute("INSERT INTO coach_external_ids (coach_id, source, external_id, confidence, "
                     "is_primary) VALUES (?, 'cfbd', ?, 'derived key', 1)",
                     (ids[-1], f"cfbd:al golden|{hire}"))
    load_coaches(conn, _coaches(tmp_path, [{
        "name": "Al Golden", "hire_date": None,
        "seasons": [_season(2010, "Clemson", "2005-12-08"),
                    _season(2011, "Oregon", "2010-12-12")]}]))
    assert conn.execute("SELECT coach_id FROM coaches").fetchall() == [(min(ids),)]
    assert conn.execute("SELECT COUNT(*) FROM coach_tenures").fetchone()[0] == 2


@pytest.mark.parametrize("value,expected", [
    (1, "FR"), (4, "SR"), (5, "GR"), ("3", "JR"),
    # CFBD overloads the roster's `year` field: on its stub rows it holds the
    # SEASON, not a class. 1,625 of the 2026 rows carried 2026 there, which
    # displayed as a class year of "2026" until this returned None.
    (2026, None), ("2026", None), (0, None), (9, None),
    # A real class some sources spell out is kept as given.
    ("Freshman", "Freshman"), ("RS-FR", "RS-FR"),
    (None, None), ("", None), (True, None),
])
def test_a_number_that_is_not_a_class_does_not_become_a_class_year(value, expected):
    assert identity.class_year_label(value) == expected


def test_a_snapshot_with_the_hire_date_at_record_level_still_loads(conn, tmp_path):
    """The snapshot committed before the hire date moved onto the season carries
    it at record level; reading both keeps those snapshots loadable."""
    load_coaches(conn, _coaches(tmp_path, [{
        "name": "Older Snapshot", "hire_date": "2008-10-13",
        "seasons": [{"year": 2024, "school": "Clemson"}, {"year": 2025, "school": "Clemson"}]}]))
    assert conn.execute("SELECT DISTINCT hire_date FROM coach_tenures").fetchall() == [("2008-10-13",)]


def test_a_career_at_two_schools_is_one_coach_with_both_tenures(conn, tmp_path):
    load_coaches(conn, _coaches(tmp_path, [{
        "name": "Al Golden", "hire_date": None,
        "seasons": [_season(2010, "Oregon", "2005-12-08"),
                    _season(2011, "Ohio St", "2010-12-12")]}]))
    assert conn.execute("SELECT COUNT(*) FROM coaches").fetchone()[0] == 1
    assert conn.execute("SELECT season_year, team_id, hire_date FROM coach_tenures "
                        "ORDER BY season_year").fetchall() == [
        (2010, 2, "2005-12-08"), (2011, 3, "2010-12-12")]


def test_rerunning_the_coaching_snapshot_creates_no_duplicate_coaches(conn, tmp_path):
    path = _coaches(tmp_path, [{"name": "Dabo Swinney", "hire_date": None, "seasons": [
        _season(2024, "Clemson", "2008-10-13", games=13, wins=10, losses=4, ties=0,
                postseason_rank=16),
        _season(2025, "Clemson", "2008-10-13", games=12, wins=9, losses=3, ties=0)]}])
    first = load_coaches(conn, path)
    second = load_coaches(conn, path)
    assert first == second == {"coaches": 1, "tenures": 2, "unknown_team": 0,
                               "ambiguous": 0, "skipped": 0}
    assert conn.execute("SELECT COUNT(*) FROM coaches").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM coach_tenures").fetchone()[0] == 2
    assert conn.execute("SELECT latest_season FROM coaches").fetchone()[0] == 2025


def test_many_records_for_one_coach_are_counted_as_one_coach(conn, tmp_path):
    """The feed's real shape: one record per SEASON, so several records are
    routinely the same person. Counting records reported 5,714 coaches for the
    826 the table actually held."""
    records = [{"name": "Kirk Ferentz", "hire_date": "1998-12-02",
                "seasons": [_season(y, "Clemson", "1998-12-02")]}
               for y in range(2020, 2026)]
    stats = load_coaches(conn, _coaches(tmp_path, records))
    assert stats["coaches"] == 1
    assert stats["tenures"] == 6
    assert conn.execute("SELECT COUNT(*) FROM coaches").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM coach_tenures").fetchone()[0] == 6
    # One unbroken job, so nothing to flag even though it arrived as six records.
    assert stats["ambiguous"] == 0


def test_the_identity_is_recorded_as_name_only_rather_than_dressed_up(conn, tmp_path):
    load_coaches(conn, _coaches(tmp_path, [
        {"name": "Some Coach", "hire_date": None, "seasons": [_season(2020, "Clemson")]}]))
    assert conn.execute("SELECT source, external_id, confidence FROM coach_external_ids"
                        ).fetchone() == ("cfbd", "cfbd:some coach", "name only")


def test_a_career_spanning_two_hire_dates_is_flagged_for_review(conn, tmp_path):
    """One coach with two jobs, or two people sharing a name. The feed cannot say,
    so it is recorded rather than asserted either way."""
    stats = load_coaches(conn, _coaches(tmp_path, [{
        "name": "Al Golden", "hire_date": None,
        "seasons": [_season(2010, "Oregon", "2005-12-08"),
                    _season(2011, "Ohio St", "2010-12-12")]}]))
    assert stats["ambiguous"] == 1
    assert conn.execute("SELECT reason FROM person_unresolved WHERE entity = 'coach'"
                        ).fetchone()[0] == "name-only identity spanning 2 hire dates"


def test_a_career_with_a_gap_is_flagged_for_review(conn, tmp_path):
    """Al Kincaid: Wyoming 1981-85 then Arkansas State 1990-91, no hire date at all."""
    stats = load_coaches(conn, _coaches(tmp_path, [{
        "name": "Al Kincaid", "hire_date": None,
        "seasons": [_season(y, "Oregon") for y in range(1981, 1986)]
                   + [_season(y, "Clemson") for y in (1990, 1991)]}]))
    assert stats["ambiguous"] == 1
    assert conn.execute("SELECT reason FROM person_unresolved WHERE entity = 'coach'"
                        ).fetchone()[0] == "name-only identity with a gap after 1985"
    # Flagged, but still one coach with the whole career available.
    assert conn.execute("SELECT COUNT(*) FROM coaches").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM coach_tenures").fetchone()[0] == 7


def test_an_unbroken_single_job_career_is_not_flagged(conn, tmp_path):
    stats = load_coaches(conn, _coaches(tmp_path, [{
        "name": "Dabo Swinney", "hire_date": None,
        "seasons": [_season(y, "Clemson", "2008-10-13") for y in range(2020, 2026)]}]))
    assert stats["ambiguous"] == 0
    assert conn.execute("SELECT COUNT(*) FROM person_unresolved").fetchone()[0] == 0


def test_a_corrected_season_record_replaces_the_old_one(conn, tmp_path):
    load_coaches(conn, _coaches(tmp_path, [{"name": "Dabo Swinney", "hire_date": None,
                                            "seasons": [_season(2025, "Clemson", wins=8)]}]))
    load_coaches(conn, _coaches(tmp_path, [{"name": "Dabo Swinney", "hire_date": None,
                                            "seasons": [_season(2025, "Clemson", wins=9)]}]))
    assert conn.execute("SELECT wins FROM coach_tenures").fetchone()[0] == 9
    assert conn.execute("SELECT COUNT(*) FROM coaches").fetchone()[0] == 1


def test_every_loaded_tenure_is_head_coach_and_keeps_the_source_school_string(conn, tmp_path):
    load_coaches(conn, _coaches(tmp_path, [{"name": "Some Coach", "hire_date": None,
                                            "seasons": [_season(2020, "Ohio St")]}]))
    assert conn.execute("SELECT role, source_team FROM coach_tenures").fetchone() == ("head coach", "Ohio St")


def test_a_coach_season_at_an_unknown_school_is_recorded_not_dropped_silently(conn, tmp_path):
    stats = load_coaches(conn, _coaches(tmp_path, [{
        "name": "Fcs Coach", "hire_date": None,
        "seasons": [_season(2020, "Mercer"), _season(2021, "Clemson")]}]))
    assert stats["unknown_team"] == 1 and stats["tenures"] == 1
    assert conn.execute("SELECT season_year FROM person_unresolved WHERE entity = 'coach' "
                        "AND reason = 'unknown team'").fetchone()[0] == 2020


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






def test_two_same_name_coaches_are_merged_and_the_ambiguity_is_recorded(tmp_path):
    """End to end, and deliberately NOT the conservative split.

    This feed gives one season per record and a hire date per job, so a name is
    its only person-level signal. Splitting on a gap or a second hire date would
    shatter real careers -- Al Golden coached ten seasons under two hire dates.
    So the name groups the career, and the cases a name cannot vouch for are
    recorded for review instead of being settled by guesswork either way.
    """
    import fetch_cfbd_coaches as fc
    merged = fc.merge_records([
        {"name": "Bobby Johnson", "hire_date": None, "seasons": [{"year": 1990, "school": "Oregon"}]},
        {"name": "Bobby Johnson", "hire_date": None, "seasons": [{"year": 2005, "school": "Clemson"}]}])
    assert len(merged) == 1
    path = tmp_path / "coaches.json"
    path.write_text(json.dumps(merged), encoding="utf-8")

    c = sqlite3.connect(":memory:")
    c.execute("PRAGMA foreign_keys = ON")
    repo = Path(__file__).resolve().parent.parent
    c.executescript((repo / "sql" / "schema.sql").read_text(encoding="utf-8"))
    c.executescript((repo / "sql" / "person_tables.sql").read_text(encoding="utf-8"))
    for team_id, name in TEAMS.items():
        c.execute("INSERT INTO teams (team_id, team_name) VALUES (?, ?)", (team_id, name))

    stats = load_coaches(c, path)
    assert stats["coaches"] == 1 and stats["ambiguous"] == 1
    assert c.execute("SELECT reason FROM person_unresolved WHERE entity = 'coach'"
                     ).fetchone()[0] == "name-only identity with a gap after 1990"


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
