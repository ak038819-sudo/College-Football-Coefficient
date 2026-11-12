"""src/build_bowling_merits.py: the research model's per-game awards.

Two kinds of test here. The synthetic ones drive the builder end to end against
a database built in the test, so they run everywhere. The archive ones read
db/league.db and skip without it; CI builds that database, so they are a real
signal there -- see the lesson that a test which skips locally can fail in CI.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

import build_bowling_merits as bm  # noqa: E402

DB_PATH = REPO_ROOT / "db" / "league.db"

# Four clubs, a round robin and a bit, so every award has neighbours to read an
# opponent's quality from. One tie and one overtime game, because those are the
# two outcomes that do not award like a plain win or loss.
SYNTHETIC = [
    # game_id, season, date, home, away, home_score, away_score, went_ot
    (1, 2024, "2024-09-07", 10, 20, 31, 17, 0),
    (2, 2024, "2024-09-14", 30, 40, 24, 21, 1),
    (3, 2024, "2024-09-21", 10, 30, 14, 28, 0),
    (4, 2024, "2024-09-28", 20, 40, 35, 35, 0),   # tie: never scored
    (5, 2024, "2024-10-05", 10, 40, 42, 10, 0),
    (6, 2024, "2024-10-12", 20, 30, 13, 27, 0),
    (7, 2024, "2024-10-19", 30, 10, 21, 20, 0),
    (8, 2024, "2024-10-26", 40, 20, 17, 24, 0),
]


def make_db(path: Path, games=SYNTHETIC, membership=True) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE games (
            game_id INTEGER PRIMARY KEY, season_year INTEGER, game_date TEXT,
            home_team_id INTEGER, away_team_id INTEGER,
            home_score INTEGER, away_score INTEGER, went_ot INTEGER);
        CREATE TABLE team_membership_by_season (
            team_id INTEGER, season_year INTEGER, is_fbs INTEGER);
        """
    )
    conn.executemany(
        "INSERT INTO games (game_id, season_year, game_date, home_team_id, "
        "away_team_id, home_score, away_score, went_ot) VALUES (?,?,?,?,?,?,?,?)",
        games)
    if membership:
        teams = {g[3] for g in games} | {g[4] for g in games}
        conn.executemany(
            "INSERT INTO team_membership_by_season VALUES (?,?,1)",
            [(t, 2024) for t in teams])
    conn.commit()
    conn.close()


@pytest.fixture()
def built(tmp_path):
    path = tmp_path / "synthetic.db"
    make_db(path)
    assert bm.main(["--db", str(path)]) == 0
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    yield conn
    conn.close()


def rows(conn):
    return conn.execute(
        f"SELECT game_id, team_id, season_year, merits, opponent_quality, mature "
        f"FROM {bm.TABLE} ORDER BY game_id, team_id").fetchall()


def test_main_writes_two_awards_for_every_scored_game(built):
    got = rows(built)
    # Seven scored games, both sides of each. The tie is game 4.
    assert len(got) == 14
    assert {r[0] for r in got} == {1, 2, 3, 5, 6, 7, 8}


def test_a_tie_is_not_scored_at_all(built):
    # Absent, not zero: a Bowling Merits 0.0 is not an award any outcome gives,
    # so a tie stored as 0 would read as a result rather than as no result.
    assert built.execute(
        f"SELECT COUNT(*) FROM {bm.TABLE} WHERE game_id = 4").fetchone()[0] == 0


def test_a_regulation_loss_earns_a_negative_award(built):
    # Game 1: 10 beat 20 in regulation. This is the difference from production
    # Merits that the column's rendering depends on.
    winner = built.execute(
        f"SELECT merits FROM {bm.TABLE} WHERE game_id = 1 AND team_id = 10").fetchone()[0]
    loser = built.execute(
        f"SELECT merits FROM {bm.TABLE} WHERE game_id = 1 AND team_id = 20").fetchone()[0]
    assert winner >= 1.5, winner
    assert winner <= 4.0, winner
    assert loser < 0, loser
    assert loser >= -0.5, loser


def test_an_overtime_loss_earns_exactly_one_half(built):
    # Game 2: 30 beat 40 in overtime.
    assert built.execute(
        f"SELECT merits FROM {bm.TABLE} WHERE game_id = 2 AND team_id = 40").fetchone()[0] == 0.5


def test_awards_match_the_research_engine_exactly(built):
    """The builder must not be a second implementation of the award."""
    path = str(REPO_ROOT / "analysis" / "bowling_merits")
    sys.path.insert(0, path)
    try:
        import engine
    finally:
        sys.path.remove(path)
    games = [dict(id=str(g[0]), season=g[1], date=g[2], home=str(g[3]), away=str(g[4]),
                  hs=g[5], as_=g[6], ot=bool(g[7])) for g in SYNTHETIC if g[5] != g[6]]
    expected = {(int(r["id"]), int(r["team"])): round(r["merits"], 6)
                for r in engine.calculate(games, engine.Rules(1.5, 0.5, 3))}
    assert {(r[0], r[1]): r[3] for r in rows(built)} == expected
    assert expected  # the comparison is only worth making against real awards


def test_rebuilding_replaces_rather_than_accumulates(tmp_path):
    path = tmp_path / "twice.db"
    make_db(path)
    assert bm.main(["--db", str(path)]) == 0
    conn = sqlite3.connect(path)
    first = rows(conn)
    conn.close()
    # Drop a game and rebuild: the award for it must be gone, not left behind.
    conn = sqlite3.connect(path)
    conn.execute("DELETE FROM games WHERE game_id = 8")
    conn.commit()
    conn.close()
    assert bm.main(["--db", str(path)]) == 0
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    second = rows(conn)
    conn.close()
    assert len(second) == len(first) - 2
    assert 8 not in {r[0] for r in second}


def test_membership_is_not_required(tmp_path):
    """The builder must score a season the membership snapshots never covered.

    The research adapter joins team_membership_by_season, whose snapshots only
    run 2014-2025. Repeating that join here scored nothing before 2014 and --
    the reason this test exists -- nothing in 2026 either.
    """
    path = tmp_path / "nomembership.db"
    make_db(path, membership=False)
    assert bm.main(["--db", str(path)]) == 0
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        assert len(rows(conn)) == 14
    finally:
        conn.close()


def test_main_reports_a_missing_database_instead_of_raising(tmp_path):
    assert bm.main(["--db", str(tmp_path / "absent.db")]) == 1


def test_load_eligible_games_needs_the_games_table(tmp_path):
    path = tmp_path / "empty.db"
    sqlite3.connect(path).close()
    conn = sqlite3.connect(path)
    try:
        with pytest.raises(RuntimeError, match="games"):
            bm.load_eligible_games(conn)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Against the real archive. CI builds db/league.db; a bare checkout has none.
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def archive():
    if not DB_PATH.is_file():
        pytest.skip("db/league.db not built")
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    yield conn
    conn.close()


def test_the_membership_join_would_drop_nothing_it_should_keep(archive):
    """`games` is already the rated FBS-vs-FBS set, which is why the builder
    can skip the membership join. If a game ever appears whose teams are not
    both flagged FBS for its season, the shortcut has stopped being equivalent
    and the comment in GAMES_SQL has stopped being true."""
    covered = archive.execute(
        "SELECT MIN(season_year), MAX(season_year) FROM team_membership_by_season").fetchone()
    if covered == (None, None):
        pytest.skip("no membership snapshots loaded")
    low, high = covered
    unflagged = archive.execute(
        """
        SELECT COUNT(*) FROM games g
        WHERE g.season_year BETWEEN ? AND ?
          AND g.home_score IS NOT NULL AND g.away_score IS NOT NULL
          AND (NOT EXISTS (SELECT 1 FROM team_membership_by_season m
                           WHERE m.team_id = g.home_team_id
                             AND m.season_year = g.season_year AND m.is_fbs = 1)
            OR NOT EXISTS (SELECT 1 FROM team_membership_by_season m
                           WHERE m.team_id = g.away_team_id
                             AND m.season_year = g.season_year AND m.is_fbs = 1))
        """, (low, high)).fetchone()[0]
    assert unflagged == 0


def test_the_archive_scores_every_season_including_the_current_one(archive):
    have = {r[0] for r in archive.execute(
        f"SELECT DISTINCT season_year FROM {bm.TABLE}")} if archive.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (bm.TABLE,)).fetchone() else set()
    if not have:
        pytest.skip("bowling_merits_game_awards not built")
    seasons = {r[0] for r in archive.execute(
        "SELECT DISTINCT season_year FROM games "
        "WHERE home_score IS NOT NULL AND away_score IS NOT NULL "
        "AND home_score <> away_score")}
    # Every season with a decided game has awards -- no silent coverage hole,
    # which is exactly what the membership join introduced.
    assert seasons - have == set()


def test_every_award_is_inside_the_formula_s_range(archive):
    if not archive.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                           (bm.TABLE,)).fetchone():
        pytest.skip("bowling_merits_game_awards not built")
    low, high = archive.execute(f"SELECT MIN(merits), MAX(merits) FROM {bm.TABLE}").fetchone()
    # floor 1.5 .. 4.0 for a win, 0.5 flat for an overtime loss, and a
    # regulation loss between -0.5 and 0.
    assert low >= -0.5, low
    assert high <= 4.0, high
    assert archive.execute(
        f"SELECT COUNT(*) FROM {bm.TABLE} WHERE merits = 0").fetchone()[0] == 0
