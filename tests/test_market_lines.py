"""
The betting market on its way to a game page.

`game_lines` has held 40,490 rows since 2013 -- eleven providers over fourteen
seasons -- and `src/measure_vs_market.py` was its only reader, for the one
aggregate behind the Track Record page. No game page had ever shown a line.

The browser side is tests/game_market_panel.test.cjs.
"""
import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

import export_static_data as ex  # noqa: E402
from measure_vs_market import PROVIDER_PREFERENCE  # noqa: E402


def db(rows=(), *, with_table=True) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    if not with_table:
        return conn
    conn.executescript("""
      CREATE TABLE game_lines (game_id INTEGER, provider TEXT, season_year INTEGER,
        market_home_margin REAL, over_under REAL,
        snapshot_home_score INTEGER, snapshot_away_score INTEGER);
    """)
    conn.executemany("INSERT INTO game_lines VALUES (?,?,?,?,?,?,?)", rows)
    return conn


def games_of(conn, season=2024):
    payload = ex.build_market_lines(conn).get(season)
    if not payload:
        return []
    return [dict(zip(payload["fields"], row)) for row in payload["games"]]


def test_one_line_per_game_chosen_by_the_projects_own_preference():
    # Five books on one game must not become five rows, and the book that
    # speaks for it is the one the track record already uses -- otherwise a
    # game page would quote a number the page measuring the model disagrees
    # with.
    rows = [(7, "Bovada", 2024, -3.0, 54.5, 24, 17),
            (7, "consensus", 2024, -2.5, 55.0, 24, 17),
            (7, "DraftKings", 2024, -3.5, 54.0, 24, 17)]
    out = games_of(db(rows))
    assert len(out) == 1
    assert out[0]["provider"] == "consensus"
    assert PROVIDER_PREFERENCE[0] == "consensus", "this test tracks that order, not its own copy"
    assert out[0]["market_home_margin"] == -2.5


def test_the_total_comes_from_the_same_capture_as_the_spread():
    # Taking the spread from one book and the total from another would publish
    # a line no book ever offered.
    # The book that is NOT chosen is inserted first on purpose: reading
    # lines[0] instead of the chosen row passes when the preferred book
    # happens to come back first, which is how this would slip through.
    rows = [(7, "Bovada", 2024, -3.0, 61.5, 24, 17),
            (7, "consensus", 2024, -2.5, 55.0, 24, 17)]
    out = games_of(db(rows))
    assert (out[0]["provider"], out[0]["market_home_margin"], out[0]["over_under"]) \
        == ("consensus", -2.5, 55.0)


def test_an_unknown_book_is_usable_and_the_choice_stays_deterministic():
    rows = [(7, "Zed Sportsbook", 2024, -1.0, None, 24, 17),
            (7, "Acme Books", 2024, -4.0, None, 24, 17)]
    first = games_of(db(rows))
    # Reversed insertion order must not change the answer.
    second = games_of(db(list(reversed(rows))))
    assert first == second
    assert first[0]["provider"] == "Acme Books", "unknown books sort by name, not by row order"


def test_a_game_nobody_priced_is_not_in_the_payload():
    # A NULL spread is a game the market never called, stored deliberately
    # rather than as a 0 -- see sql/line_tables.sql. A pick'em is a real line
    # at 0 and must survive.
    rows = [(7, "consensus", 2024, None, 54.5, 24, 17),
            (8, "consensus", 2024, 0.0, 51.0, 20, 20)]
    out = games_of(db(rows))
    assert [row["game_id"] for row in out] == [8]
    assert out[0]["market_home_margin"] == 0.0


def test_a_capture_taken_before_the_game_ended_is_not_a_closing_line():
    # CFBD serves the current number for an unplayed game and the closing one
    # afterwards, in the same column. The snapshot's own record of the result
    # is the only thing that tells them apart.
    rows = [(7, "consensus", 2024, -2.5, 54.5, 24, 17),
            (8, "consensus", 2024, -7.0, 48.0, None, None)]
    out = {row["game_id"]: row["closing"] for row in games_of(db(rows))}
    assert out == {7: 1, 8: 0}


def test_a_missing_total_is_null_and_never_zero():
    out = games_of(db([(7, "consensus", 2024, -2.5, None, 24, 17)]))
    assert out[0]["over_under"] is None, "a total of 0 would be a real line nobody set"


def test_seasons_are_separate_payloads_and_only_priced_ones_exist():
    rows = [(7, "consensus", 2013, -2.5, 54.5, 24, 17),
            (8, "consensus", 2024, -7.0, 48.0, 31, 10)]
    payloads = ex.build_market_lines(db(rows))
    assert sorted(payloads) == [2013, 2024]
    assert payloads[2013]["fields"] == ex.MARKET_FIELDS
    assert len(payloads[2013]["games"]) == 1


def test_a_database_with_no_lines_table_exports_nothing():
    # Every season before 2013, and any bootstrap that never loaded them.
    assert ex.build_market_lines(db(with_table=False)) == {}


def test_only_the_seasons_that_have_lines_are_written(tmp_path):
    rows = [(7, "consensus", 2024, -2.5, 54.5, 24, 17)]
    files = ex.write_market_lines(tmp_path, ex.build_market_lines(db(rows)))
    assert list(files) == ["2024"], "a season with no lines must cost a reader no request"
    written = (tmp_path / "lines" / "2024.js").read_text(encoding="utf-8")
    assert written.startswith("(window.__CFB_LINES__=window.__CFB_LINES__||{})[2024]=")
    assert files["2024"].startswith("data/lines/2024.js?v=")
    # Nothing at all means no directory full of orphans either.
    assert ex.write_market_lines(tmp_path, {}) == {}
    assert not (tmp_path / "lines").exists()


def test_the_field_order_is_pinned():
    assert ex.MARKET_FIELDS == ["game_id", "market_home_margin", "over_under",
                                "provider", "closing"]
