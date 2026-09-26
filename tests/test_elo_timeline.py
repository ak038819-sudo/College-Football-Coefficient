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
