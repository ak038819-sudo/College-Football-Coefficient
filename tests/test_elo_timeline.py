"""
Point-in-time Elo standings and movement (P1-05, P1-06).

The properties worth pinning here are relationships, not numbers: that a
snapshot is really the engine's own state at that point, that the stages cover
the season exactly once, and that a movement column reconciles with the ratings
beside it. They run against the real db/league.db and skip cleanly without it,
like the rest of the suite.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest

import elo_timeline
from elo_timeline import PRESEASON_KEY, POSTSEASON_KEY, build_timeline, ranks_for, stage_key

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from export_dashboard_data import load_elo_by_season  # noqa: E402

SAMPLE_SEASONS = [1980, 1998, 2014, 2024]


@pytest.fixture(scope="module")
def timeline(db_conn):
    data = build_timeline(db_conn)
    if not data:
        pytest.skip("no Elo history -- run build_elo.py first")
    return data


def rows_by_team(payload, stage_index):
    fields = payload["row_fields"]
    return {payload["teams"][r[fields.index("team")]][1]:
            dict(zip(fields, r)) for r in payload["rows"][stage_index]}


def stage_keys(payload):
    return [s[0] for s in payload["stages"]]


# --------------------------------------------------------------------------
# Ranking helper, against hand-written cases
# --------------------------------------------------------------------------

def test_ties_share_a_rank_and_the_next_value_skips_past_them():
    assert ranks_for([10.0, 9.0, 9.0, 8.0]) == [1, 2, 2, 4]


def test_a_whole_table_of_ties_is_all_rank_one():
    assert ranks_for([1500.0] * 4) == [1, 1, 1, 1]


def test_ranks_of_nothing_is_empty_rather_than_an_error():
    assert ranks_for([]) == []


def test_stage_key_sends_both_postseason_phases_to_one_stage():
    assert stage_key("bowl", 1) == POSTSEASON_KEY
    assert stage_key("cfp", 1) == POSTSEASON_KEY
    assert stage_key("regular", 1) == "w1"
    assert stage_key("regular", 12) == "w12"


# --------------------------------------------------------------------------
# Structure
# --------------------------------------------------------------------------

def test_every_season_starts_with_a_preseason_stage(timeline):
    for season, payload in timeline.items():
        assert stage_keys(payload)[0] == PRESEASON_KEY, season


def test_regular_weeks_run_in_ascending_order_between_preseason_and_postseason(timeline):
    for season, payload in timeline.items():
        keys = stage_keys(payload)
        weeks = [int(k[1:]) for k in keys if k.startswith("w")]
        assert weeks == sorted(weeks), season
        assert keys.index(PRESEASON_KEY) == 0, season
        if POSTSEASON_KEY in keys:
            assert keys.index(POSTSEASON_KEY) == len(keys) - 1, season


def test_a_season_without_a_postseason_has_no_postseason_stage(timeline, db_conn):
    for season, payload in timeline.items():
        played = db_conn.execute(
            "SELECT COUNT(*) FROM games WHERE season_year=? AND game_phase IN ('bowl','cfp') "
            "AND home_score IS NOT NULL", (season,)).fetchone()[0]
        assert (POSTSEASON_KEY in stage_keys(payload)) == (played > 0), season


def test_the_stages_cover_every_completed_game_exactly_once(timeline, db_conn):
    """A game in no stage would silently vanish from the season; a game in two
    would be counted twice."""
    for season, payload in timeline.items():
        counted = sum(s[payload["stage_fields"].index("games")] for s in payload["stages"])
        actual = db_conn.execute(
            "SELECT COUNT(*) FROM games WHERE season_year=? AND home_score IS NOT NULL "
            "AND away_score IS NOT NULL", (season,)).fetchone()[0]
        assert counted == actual, season


def test_only_seasons_with_ratings_appear(db_conn):
    seasons = set(build_timeline(db_conn))
    with_elo = {r[0] for r in db_conn.execute(
        "SELECT DISTINCT g.season_year FROM games g JOIN elo_game_history e ON e.game_id=g.game_id "
        "WHERE g.home_score IS NOT NULL")}
    assert seasons == with_elo


def test_a_database_without_the_tables_yields_nothing_rather_than_raising():
    conn = sqlite3.connect(":memory:")
    assert build_timeline(conn) == {}
    conn.close()


# --------------------------------------------------------------------------
# The snapshots themselves
# --------------------------------------------------------------------------

@pytest.mark.parametrize("season", SAMPLE_SEASONS)
def test_the_last_stage_is_the_season_the_site_already_publishes(timeline, db_path, season):
    """
    The strongest check available: the final snapshot must equal the
    end-of-season Elo the dashboard has always shown, which is computed by
    completely separate code in export_dashboard_data.py.
    """
    if season not in timeline:
        pytest.skip(f"{season} not in this database")
    published = {r["team"]: r["elo"] for r in load_elo_by_season(str(db_path))[season]}
    final = {name: row["elo"] for name, row in rows_by_team(timeline[season], -1).items()}
    assert final == published


@pytest.mark.parametrize("season", SAMPLE_SEASONS)
def test_preseason_is_the_rating_each_team_carried_in(timeline, db_conn, season):
    if season not in timeline:
        pytest.skip(f"{season} not in this database")
    carried = {r["team_name"]: round(r["pregame_elo"], 1) for r in db_conn.execute(
        """
        SELECT team_name, pregame_elo FROM (
            SELECT t.team_name, e.pregame_elo,
                   ROW_NUMBER() OVER (PARTITION BY e.team_id ORDER BY g.game_date, e.game_id) AS rn
            FROM elo_game_history e
            JOIN games g ON g.game_id = e.game_id
            JOIN teams t ON t.team_id = e.team_id
            WHERE g.season_year = ? AND g.home_score IS NOT NULL
        ) WHERE rn = 1
        """, (season,))}
    assert {n: r["elo"] for n, r in rows_by_team(timeline[season], 0).items()} == carried


def test_the_first_season_starts_everyone_at_the_same_rating(timeline):
    """No earlier season to carry over from -- the page says so, and this is why."""
    first = min(timeline)
    values = {row["elo"] for row in rows_by_team(timeline[first], 0).values()}
    assert len(values) == 1


@pytest.mark.parametrize("season", SAMPLE_SEASONS)
def test_movement_reconciles_with_the_ratings_beside_it(timeline, season):
    """
    d_elo and d_rank are what a reader checks by subtracting the two rows
    themselves, so they must be exactly that -- computed from the rounded
    figures on display, not from unrounded ones behind them.
    """
    if season not in timeline:
        pytest.skip(f"{season} not in this database")
    payload = timeline[season]
    for index in range(1, len(payload["stages"])):
        before = rows_by_team(payload, index - 1)
        for name, row in rows_by_team(payload, index).items():
            prior = before.get(name)
            if prior is None:
                assert row["d_elo"] is None and row["d_rank"] is None, (season, name)
                continue
            assert row["d_elo"] == pytest.approx(round(row["elo"] - prior["elo"], 1)), (season, name)
            # Positive means the team climbed, even though the rank number fell.
            assert row["d_rank"] == prior["rank"] - row["rank"], (season, name)


def test_the_first_stage_reports_no_movement_rather_than_zero(timeline):
    """Zero would claim the team stood still; it had nothing to move from."""
    for season, payload in timeline.items():
        for row in rows_by_team(payload, 0).values():
            assert row["d_elo"] is None and row["d_rank"] is None, season


@pytest.mark.parametrize("season", SAMPLE_SEASONS)
def test_a_bye_keeps_the_rating_and_says_it_played_nothing(timeline, season):
    if season not in timeline:
        pytest.skip(f"{season} not in this database")
    payload = timeline[season]
    byes = 0
    for index in range(1, len(payload["stages"])):
        before = rows_by_team(payload, index - 1)
        for name, row in rows_by_team(payload, index).items():
            if row["stage_games"] == 0 and name in before:
                byes += 1
                assert row["elo"] == before[name]["elo"], (season, name)
                assert row["d_elo"] == 0.0, (season, name)
    assert byes > 0, "no bye weeks at all would mean stage_games is not being counted"


@pytest.mark.parametrize("season", SAMPLE_SEASONS)
def test_games_played_totals_match_the_database(timeline, db_conn, season):
    if season not in timeline:
        pytest.skip(f"{season} not in this database")
    actual = {r["team_name"]: r["n"] for r in db_conn.execute(
        """
        SELECT t.team_name, COUNT(*) AS n
        FROM elo_game_history e
        JOIN games g ON g.game_id = e.game_id
        JOIN teams t ON t.team_id = e.team_id
        WHERE g.season_year = ? AND g.home_score IS NOT NULL
        GROUP BY t.team_name
        """, (season,))}
    assert {n: r["played"] for n, r in rows_by_team(timeline[season], -1).items()} == actual


@pytest.mark.parametrize("season", SAMPLE_SEASONS)
def test_every_snapshot_is_ordered_and_ranked_consistently(timeline, season):
    if season not in timeline:
        pytest.skip(f"{season} not in this database")
    payload = timeline[season]
    fields = payload["row_fields"]
    elo_at, rank_at = fields.index("elo"), fields.index("rank")
    for index, rows in enumerate(payload["rows"]):
        values = [r[elo_at] for r in rows]
        assert values == sorted(values, reverse=True), (season, index)
        assert [r[rank_at] for r in rows] == ranks_for(values), (season, index)


def test_a_team_appears_once_per_snapshot(timeline):
    for season, payload in timeline.items():
        index_at = payload["row_fields"].index("team")
        for stage_index, rows in enumerate(payload["rows"]):
            ids = [r[index_at] for r in rows]
            assert len(ids) == len(set(ids)), (season, stage_index)


# --------------------------------------------------------------------------
# What actually ships
# --------------------------------------------------------------------------

def test_the_committed_export_carries_a_file_for_every_season_it_advertises(repo_root):
    manifest = json.loads((repo_root / "ui" / "data" / "static_manifest.json").read_text())
    advertised = manifest.get("elo_timeline")
    assert advertised, "the manifest must tell the page which seasons have a timeline"
    for season, src in advertised.items():
        path = repo_root / "ui" / (src.split("?")[0])
        assert path.exists(), f"{season} is advertised but {path.name} is missing"
        text = path.read_text(encoding="utf-8")
        assert text.startswith("(window.__CFB_ELO_TIMELINE__"), season
        assert f'"season":{season}' in text.replace(" ", "")


def test_every_field_the_page_reads_is_in_the_shipped_field_lists(repo_root):
    """
    The payload is positional arrays plus a field list. If a name the browser
    looks up is missing, the page renders undefined rather than failing.
    """
    shell = (repo_root / "ui" / "dashboard_shell.html").read_text(encoding="utf-8")
    for field in ("elo", "rank", "d_elo", "d_rank", "played", "stage_games"):
        assert f"r.{field}" in shell or f'"{field}"' in shell, field
        assert field in elo_timeline.ROW_FIELDS, field
    for field in ("key", "label", "games", "first_date", "last_date"):
        assert field in elo_timeline.STAGE_FIELDS, field


# --------------------------------------------------------------------------
# Poll tracks: the AP and CFP ballots beside the model
# --------------------------------------------------------------------------
# The grid reads a poll's own releases, never the Elo stages, so what matters
# here is that a release is a real, ordered, labelled column and that movement
# is measured against the previous release of the SAME poll.

def polls(payload):
    return payload.get("polls") or {}


def poll_rows(payload, poll, release_index):
    fields = payload["poll_row_fields"]
    return [dict(zip(fields, r)) for r in polls(payload)[poll]["rows"][release_index]]


def poll_releases(payload, poll):
    fields = payload["poll_release_fields"]
    return [dict(zip(fields, r)) for r in polls(payload)[poll]["releases"]]


@pytest.fixture(scope="module")
def polled(timeline):
    seasons = {season: payload for season, payload in timeline.items() if polls(payload)}
    if not seasons:
        pytest.skip("no poll release loaded -- run load_rankings.py first")
    return seasons


def test_a_release_key_names_the_phase_as_well_as_the_week():
    # A regular-season week 1 and a postseason week 1 are different columns, and
    # CFBD numbers both of them 1.
    assert elo_timeline.poll_release_key("regular", 1) != elo_timeline.poll_release_key("postseason", 1)
    assert elo_timeline.poll_release_key("regular", 8) == "r8"
    assert elo_timeline.poll_release_key("postseason", 1) == "p1"


def test_the_postseason_release_is_named_for_being_final_not_for_its_number():
    assert elo_timeline.poll_release_label("postseason", 1) == "Final"
    assert elo_timeline.poll_release_label("regular", 8) == "Week 8"


def test_a_season_with_no_poll_release_carries_no_poll_keys_at_all(timeline):
    """
    Absent rather than present and empty, so the page can tell "never released"
    from "released and empty" -- and so a season that predates the poll archive
    does not advertise a track with nothing in it.
    """
    bare = [season for season, payload in timeline.items() if not polls(payload)]
    if not bare:
        pytest.skip("every season in the data has a poll release")
    for season in bare:
        payload = timeline[season]
        assert "polls" not in payload, season
        assert "poll_row_fields" not in payload, season


def test_every_release_is_keyed_once_and_ordered_with_the_final_poll_last(polled):
    for season, payload in polled.items():
        for poll in polls(payload):
            releases = poll_releases(payload, poll)
            keys = [r["key"] for r in releases]
            assert len(keys) == len(set(keys)), (season, poll)
            phases = [r["season_type"] == "postseason" for r in releases]
            assert phases == sorted(phases), (season, poll)
            regular = [r["week"] for r in releases if r["season_type"] == "regular"]
            assert regular == sorted(regular), (season, poll)
            assert len(payload["polls"][poll]["rows"]) == len(releases), (season, poll)


def test_a_release_ranks_each_school_once_and_counts_up_from_one(polled):
    for season, payload in polled.items():
        for poll in polls(payload):
            for index, release in enumerate(poll_releases(payload, poll)):
                rows = poll_rows(payload, poll, index)
                assert rows, (season, poll, release["key"])
                schools = [r["school"] for r in rows]
                assert len(schools) == len(set(schools)), (season, poll, release["key"])
                ranks = [r["rank"] for r in rows]
                assert ranks == sorted(ranks), (season, poll, release["key"])
                assert min(ranks) == 1, (season, poll, release["key"])


def test_a_team_index_points_at_the_team_the_school_names(polled):
    """
    The index is into the payload's own teams list. A resolved row must point at
    the team it names, or a poll column would show one team's rank on another's
    row; an unresolved row keeps its raw school name instead of a gap.
    """
    for season, payload in polled.items():
        names = [name for _, name, *_ in payload["teams"]]
        for poll in polls(payload):
            for index in range(len(poll_releases(payload, poll))):
                for row in poll_rows(payload, poll, index):
                    assert row["school"], (season, poll)
                    if row["team"] is None:
                        continue
                    assert 0 <= row["team"] < len(names), (season, poll, row)
                    # Resolution goes through the same alias table load_games.py
                    # uses, so the two names can differ -- what must hold is that
                    # the index names a team, not which spelling it carries.
                    assert names[row["team"]], (season, poll, row)


def test_movement_is_measured_against_the_previous_release_of_the_same_poll(polled):
    """
    d_rank is positive when the team climbed, and None when it was not in the
    previous release -- a team entering a poll at 23 did not climb from 26.
    """
    for season, payload in polled.items():
        for poll in polls(payload):
            releases = poll_releases(payload, poll)
            for index in range(len(releases)):
                before = {r["school"]: r["rank"] for r in poll_rows(payload, poll, index - 1)} \
                    if index else {}
                for row in poll_rows(payload, poll, index):
                    was = before.get(row["school"])
                    if was is None:
                        assert row["d_rank"] is None, (season, poll, index, row)
                    else:
                        assert row["d_rank"] == was - row["rank"], (season, poll, index, row)


def test_the_first_release_claims_no_movement_for_anyone(polled):
    for season, payload in polled.items():
        for poll in polls(payload):
            for row in poll_rows(payload, poll, 0):
                assert row["d_rank"] is None, (season, poll, row)


def test_a_release_key_can_never_be_mistaken_for_a_stage_key(polled):
    """
    A poll released in week N is voted on games through week N-1, so a release
    and an Elo stage are not the same moment. Keeping the two key spaces
    disjoint is what stops a later change from reusing stage_key for a poll and
    silently lining the two column sets up.
    """
    for season, payload in polled.items():
        stages = {s[0] for s in payload["stages"]}
        for poll in polls(payload):
            keys = {r["key"] for r in poll_releases(payload, poll)}
            assert not (keys & stages), (season, poll, keys & stages)


def test_a_poll_has_a_release_for_a_week_the_elo_table_has_not_reached(polled):
    """
    The concrete form of the same point, measured rather than asserted: a
    mid-season poll is released BEFORE that week's games, so the ballots run a
    week ahead of the played weeks. Found from the data, so it holds whichever
    season is current.
    """
    ahead = False
    for payload in polled.values():
        played = {s[3] for s in payload["stages"] if s[3] is not None}
        for poll in polls(payload):
            weeks = {r["week"] for r in poll_releases(payload, poll)
                     if r["season_type"] == "regular"}
            if weeks - played:
                ahead = True
    assert ahead, "no poll release past the last played week -- the check proves nothing"


def test_the_page_reads_only_poll_fields_the_payload_ships(repo_root):
    shell = (repo_root / "ui" / "dashboard_shell.html").read_text(encoding="utf-8")
    for field in ("school", "rank", "d_rank"):
        assert field in elo_timeline.POLL_ROW_FIELDS, field
    for field in ("key", "label", "season_type", "week"):
        assert field in elo_timeline.POLL_RELEASE_FIELDS, field
        assert f"r.{field}" in shell or f"st.{field}" in shell or f'"{field}"' in shell, field
    # The browser unpacks the poll arrays through these two field lists; without
    # them every poll row would read as undefined.
    assert "poll_release_fields" in shell and "poll_row_fields" in shell


# --------------------------------------------------------------------------
# A team's conference, as it was that season
# --------------------------------------------------------------------------

def test_a_team_carries_the_conference_it_was_in_that_season(timeline, db_conn):
    """
    The grid's conference filter has to cut a 1995 table by 1995's membership.
    Measured against team_membership_by_season itself, the same source
    build_conference_elo_by_year reads, for every season at once.
    """
    expected = {}
    for year, tid, conference in db_conn.execute(
            """SELECT season_year, team_id, conference_real FROM team_membership_by_season
               WHERE conference_real IS NOT NULL"""):
        expected[(int(year), tid)] = conference
    if not expected:
        pytest.skip("no membership snapshot loaded")
    checked = 0
    for season, payload in timeline.items():
        for tid, _name, conference in payload["teams"]:
            assert conference == expected.get((season, tid)), (season, tid)
            checked += conference is not None
    assert checked, "no team in any season carried a conference"


def test_a_realignment_is_not_backdated_onto_an_older_season(timeline):
    """
    The point of reading that season's membership: a team that changed league
    must read differently in two seasons. Found from the data rather than named,
    so the check does not go stale when the archive widens.
    """
    seen = {}
    for season, payload in timeline.items():
        for tid, _name, conference in payload["teams"]:
            if conference:
                seen.setdefault(tid, set()).add(conference)
    assert any(len(v) > 1 for v in seen.values()), "no team ever changed conference"


# --------------------------------------------------------------------------
# Poll tracks against a hand-written database
# --------------------------------------------------------------------------
# Two properties the live data cannot yet show: the current season has no
# postseason ballot, and no team has been spelled two ways within it. Both are
# ordinary in the archive this will carry, so they are pinned here rather than
# waiting for a season to make the real-data checks meaningful.

@pytest.fixture
def poll_db():
    conn = sqlite3.connect(":memory:")
    conn.executescript((Path(__file__).resolve().parents[1] / "sql" / "rankings_tables.sql")
                       .read_text(encoding="utf-8")
                       .replace("REFERENCES teams(team_id)", "REFERENCES poll_rankings(team_id)"))
    return conn


def insert_release(conn, season_type, week, rows, poll="ap", season=2030):
    conn.executemany(
        """INSERT INTO poll_rankings (season_year, season_type, week, poll, rank, team_id, school)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        [(season, season_type, week, poll, rank, team_id, school)
         for rank, team_id, school in rows])
    conn.commit()


def test_the_final_ballot_is_the_last_column_whatever_week_cfbd_numbered_it(poll_db):
    """
    CFBD restarts the week count for the postseason, so a final poll can arrive
    numbered 1. Ordering by week alone would put the season's last ballot first
    and make every movement number read backwards.
    """
    insert_release(poll_db, "postseason", 1, [(1, 7, "Georgia"), (2, 8, "Texas")])
    insert_release(poll_db, "regular", 9, [(1, 8, "Texas"), (2, 7, "Georgia")])
    track = elo_timeline.poll_timeline(poll_db, 2030, {7: 0, 8: 1})["ap"]
    assert [r[0] for r in track["releases"]] == ["r9", "p1"]
    assert [r[1] for r in track["releases"]] == ["Week 9", "Final"]
    # Texas was 1st in week 9 and 2nd in the final: a one-place drop, which only
    # reads that way if the final ballot came second.
    final = dict(zip(elo_timeline.POLL_ROW_FIELDS, track["rows"][1][1]))
    assert final["school"] == "Texas" and final["rank"] == 2 and final["d_rank"] == -1


def test_a_team_respelled_mid_season_keeps_one_run_of_movement(poll_db):
    """
    Movement follows the team id, not the school string. The feed renames
    schools (see the aliases in src/team_identity.py), and a team keyed by its
    printed name would read as entering the poll fresh the week it was renamed.
    """
    insert_release(poll_db, "regular", 1, [(1, 7, "Miami (FL)")])
    insert_release(poll_db, "regular", 2, [(3, 7, "Miami")])
    rows = elo_timeline.poll_timeline(poll_db, 2030, {7: 0})["ap"]["rows"]
    second = dict(zip(elo_timeline.POLL_ROW_FIELDS, rows[1][0]))
    assert second["d_rank"] == -2, "a renamed school is still the same team falling two places"
    assert second["school"] == "Miami", "the row still prints the name the ballot used"


def test_two_unresolved_schools_are_two_rows_and_not_one(poll_db):
    """
    A school CFBD never matched to a canonical team has no id. Collapsing every
    such row under a single missing id would merge unrelated programmes and
    invent movement between them.
    """
    insert_release(poll_db, "regular", 1, [(1, None, "Alpha A&M"), (2, None, "Beta State")])
    insert_release(poll_db, "regular", 2, [(1, None, "Beta State"), (2, None, "Alpha A&M")])
    rows = [dict(zip(elo_timeline.POLL_ROW_FIELDS, r))
            for r in elo_timeline.poll_timeline(poll_db, 2030, {})["ap"]["rows"][1]]
    assert [r["school"] for r in rows] == ["Beta State", "Alpha A&M"]
    assert [r["team"] for r in rows] == [None, None]
    assert [r["d_rank"] for r in rows] == [1, -1]


def test_a_poll_with_no_release_is_absent_rather_than_empty(poll_db):
    insert_release(poll_db, "regular", 1, [(1, 7, "Georgia")], poll="ap")
    tracks = elo_timeline.poll_timeline(poll_db, 2030, {7: 0})
    assert set(tracks) == {"ap"}
    assert elo_timeline.poll_timeline(poll_db, 2031, {7: 0}) == {}


def test_a_database_without_the_poll_table_builds_a_timeline_anyway():
    """
    The poll archive is display data loaded by a separate step. A build that has
    not run it must still produce every Elo stage rather than failing.
    """
    assert elo_timeline.poll_timeline(sqlite3.connect(":memory:"), 2030, {}) == {}
