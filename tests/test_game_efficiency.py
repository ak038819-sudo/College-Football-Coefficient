"""
Per-game efficiency on its way to a game page.

CFBD's `/stats/game/advanced` has been in this database since 2001 and was read
by exactly one consumer, `build_adjusted_stats.py`. The game page showed none of
it: its Matchup table carried Elo, Game Merits and the xSRDiff layer, and the
"More" panel announced "Box score & advanced stats -- available for this season"
without ever printing a number. These cover the export side of closing that.

The browser side is tests/game_efficiency_group.test.cjs, which drives the
shell's own helper.
"""
import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

import export_static_data as ex  # noqa: E402


def db(advanced_rows=(), *, with_advanced_table=True) -> sqlite3.Connection:
    """A two-team game, optionally with per-game advanced rows."""
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
      CREATE TABLE games (game_id INTEGER, season_year INTEGER, home_team_id INTEGER,
        away_team_id INTEGER, home_score INTEGER, away_score INTEGER);
      CREATE TABLE hybrid_game_ratings (game_id INTEGER, team_id INTEGER, elo_z REAL,
        coe_z REAL, hybrid_rating REAL, hybrid_expectation REAL, result_type TEXT);
      CREATE TABLE elo_game_history (game_id INTEGER, team_id INTEGER);
      INSERT INTO games VALUES (7, 2026, 1, 2, 31, 10);
      INSERT INTO hybrid_game_ratings VALUES (7, 1, 0.5, 0.5, 1600, 0.6, 'W'),
                                            (7, 2, 0.1, 0.1, 1500, 0.4, 'L');
    """)
    if with_advanced_table:
        conn.executescript("""
          CREATE TABLE game_team_advanced (game_id INTEGER, team_id INTEGER,
            off_plays INTEGER, off_ppa REAL, off_explosiveness REAL,
            def_plays INTEGER, def_ppa REAL, def_explosiveness REAL);
        """)
        conn.executemany(
            "INSERT INTO game_team_advanced (game_id, team_id, off_plays, off_ppa, "
            "off_explosiveness) VALUES (?, ?, ?, ?, ?)", advanced_rows)
    return conn


def row_of(conn) -> dict:
    payload = ex.build_game_details(conn)[2026]
    return dict(zip(payload["fields"], payload["games"][0]))


def test_each_side_carries_its_own_offence():
    # Home team 1, away team 2. The home columns must hold team 1's offence and
    # the away columns team 2's -- swapping them is the one mistake here that
    # would still produce a full, plausible-looking table.
    row = row_of(db([(7, 1, 64, 0.412, 1.771), (7, 2, 51, -0.108, 1.204)]))
    assert (row["home_plays"], row["home_ppa"], row["home_explosiveness"]) == (64, 0.412, 1.771)
    assert (row["away_plays"], row["away_ppa"], row["away_explosiveness"]) == (51, -0.108, 1.204)


def test_the_six_fields_are_appended_last_and_move_nothing():
    fields = ex.DETAIL_FIELDS
    assert fields[-6:] == ["home_plays", "home_ppa", "home_explosiveness",
                           "away_plays", "away_ppa", "away_explosiveness"]
    # Pinned to literals, not to len(fields) or to an index computed from the
    # list itself: an assertion against the thing it is checking is vacuous.
    assert fields.index("game_id") == 0
    assert fields.index("home_result") == 5
    assert fields.index("home_success_rate") == 11
    assert fields.index("home_line_scores") == 20
    assert len(fields) == 28


def test_a_missing_side_is_null_and_never_zero():
    # Only the home team has a row. A zero here would say the away offence ran
    # no plays and gained nothing, which is a measurement, not an absence.
    row = row_of(db([(7, 1, 64, 0.412, 1.771)]))
    assert row["home_plays"] == 64
    assert row["away_plays"] is None
    assert row["away_ppa"] is None
    assert row["away_explosiveness"] is None


def test_a_real_zero_survives():
    row = row_of(db([(7, 1, 0, 0.0, 0.0), (7, 2, 51, -0.108, 1.204)]))
    assert row["home_plays"] == 0
    assert row["home_ppa"] == 0.0
    assert row["home_explosiveness"] == 0.0


def test_a_database_without_the_table_exports_nulls():
    # 1980-2000 seasons, and any fresh bootstrap that skips the advanced load.
    row = row_of(db(with_advanced_table=False))
    assert row["home_ppa"] is None and row["away_ppa"] is None
    assert row["home_result"] == "W", "the rest of the row is unaffected"


def test_the_averages_keep_three_decimals_and_plays_stays_an_integer():
    row = row_of(db([(7, 1, 64, 0.4123456, 1.7714999), (7, 2, 51.0, -0.1084, 1.2)]))
    assert row["home_ppa"] == 0.412
    assert row["home_explosiveness"] == 1.771
    assert row["away_ppa"] == -0.108
    assert isinstance(row["away_plays"], int) and row["away_plays"] == 51


def test_defensive_columns_are_the_opponents_offensive_columns():
    """Why only the offence is exported.

    Over all 22,500 games of the 2001-2026 raw archive a team's `def_*` columns
    equal its opponent's `off_*` columns exactly, so exporting both would ship
    the same four numbers twice. This pins the claim to the committed data
    rather than to a comment, for the season most likely to be refetched.
    """
    import csv
    from collections import defaultdict
    path = REPO / "data" / "raw" / "game_advanced_2024.csv"
    if not path.exists():                                  # pragma: no cover
        import pytest
        pytest.skip("the 2024 per-game advanced file is not in this checkout")
    by_game = defaultdict(list)
    with path.open(encoding="utf-8") as handle:
        for raw in csv.DictReader(handle):
            by_game[raw["game_id"]].append(raw)
    pairs = [rows for rows in by_game.values() if len(rows) == 2]
    assert len(pairs) > 1_000, "expected a full season of two-sided rows"
    for a, b in pairs:
        for side in ("plays", "ppa", "success_rate", "explosiveness"):
            assert a["def_" + side] == b["off_" + side], (a["game_id"], side)
            assert b["def_" + side] == a["off_" + side], (a["game_id"], side)
