"""Physical venue identity is distinct from home designation and ratings."""
import sqlite3

from build_stadiums import ensure_schema, resolve_games, seed_stadiums
from export_static_data import stadium_map


def stadium_db(tmp_path):
    conn = sqlite3.connect(tmp_path / "stadiums.db")
    conn.executescript("""
        CREATE TABLE teams (team_id INTEGER PRIMARY KEY, team_name TEXT NOT NULL);
        CREATE TABLE team_aliases (alias TEXT PRIMARY KEY, team_name TEXT NOT NULL);
        INSERT INTO teams VALUES (1,'Alpha'),(2,'Bravo');
        CREATE TABLE games (game_id INTEGER PRIMARY KEY, season_year INTEGER,
            home_team_id INTEGER, neutral_site INTEGER, game_phase TEXT, game_phase_check TEXT);
        CREATE TABLE scheduled_games (game_id INTEGER PRIMARY KEY, season_year INTEGER,
            home_team_id INTEGER, neutral_site INTEGER, game_phase TEXT, notes TEXT, venue TEXT);
    """)
    ensure_schema(conn)
    seed = tmp_path / "seed.tsv"
    seed.write_text("team_name\tstadium_name\nAlpha\tAlpha Field\nBravo\tMemorial Stadium\n")
    assert seed_stadiums(conn, seed)["stadiums_created"] == 2
    return conn, seed


def test_seed_is_idempotent_and_rename_preserves_physical_id(tmp_path):
    conn, seed = stadium_db(tmp_path)
    original = conn.execute("SELECT stadium_id FROM stadiums WHERE stadium_name='Alpha Field'").fetchone()[0]
    assert seed_stadiums(conn, seed) == {"teams_seeded": 2, "stadiums_created": 0, "stadiums_total": 2}
    seed.write_text("team_name\tstadium_name\nAlpha\tNew Sponsor Field\nBravo\tMemorial Stadium\n")
    assert seed_stadiums(conn, seed)["stadiums_created"] == 0
    assert conn.execute("SELECT stadium_id FROM stadiums WHERE stadium_name='New Sponsor Field'").fetchone()[0] == original
    assert conn.execute("SELECT stadium_id FROM stadium_aliases WHERE alias_key='alphafield'").fetchone()[0] == original
    assert conn.execute("SELECT COUNT(*) FROM team_stadiums").fetchone()[0] == 2
    conn.close()


def test_resolution_precedence_neutral_history_and_unknown(tmp_path):
    conn, _ = stadium_db(tmp_path)
    alpha, bravo = (conn.execute("SELECT stadium_id FROM stadiums WHERE stadium_key=?", (f"home-{t}",)).fetchone()[0]
                    for t in ("alpha", "bravo"))
    conn.executemany("INSERT INTO games VALUES (?,?,?,?,?,?,NULL,NULL)", [
        (1, 2026, 1, 0, "regular", None),     # unambiguous home inference
        (2, 2026, 1, 1, "regular", None),     # neutral cannot infer
        (3, 2025, 1, 0, "regular", None),     # no 2025 relationship
        (4, 2026, 1, 0, "regular", None),     # source wins over override/home
        (5, 2026, 1, 1, "regular", None),     # explicit neutral venue
        (6, 2026, 1, 0, "bowl", None),        # postseason cannot infer
        (7, 2026, 1, 0, "regular", "Classic"),  # suspect designation
        (8, 2026, 1, 0, "regular", None),     # unknown source cannot infer
        (9, 2026, 1, 0, "regular", None),     # curated override
    ])
    conn.execute("UPDATE games SET venue_text='Memorial Stadium' WHERE game_id IN (4,5)")
    conn.execute("UPDATE games SET venue_text='Unknown Arena' WHERE game_id=8")
    conn.executemany("INSERT INTO game_venue_overrides VALUES (?,?,?)", [(4, alpha, 'check precedence'),
                                                                          (9, bravo, 'manual verification')])
    conn.executemany("INSERT INTO scheduled_games VALUES (?,?,?,?,?,?,?,NULL)", [
        (10, 2026, 1, 0, "regular", None, None),
        (11, 2026, 1, 1, "regular", None, None),
        (12, 2026, 1, 1, "regular", None, "Alpha Field"),
    ])
    first = resolve_games(conn)
    assert dict(conn.execute("SELECT game_id, stadium_id FROM games")) == {
        1: alpha, 2: None, 3: None, 4: bravo, 5: bravo, 6: None, 7: None, 8: None, 9: bravo}
    assert dict(conn.execute("SELECT game_id, stadium_id FROM scheduled_games")) == {
        10: alpha, 11: None, 12: alpha}
    assert (first["games_backfilled"], first["scheduled_backfilled"], first["explicitly_neutral"]) == (4, 2, 4)
    assert (resolve_games(conn)["games_backfilled"], resolve_games(conn)["scheduled_backfilled"]) == (0, 0)
    conn.close()


def test_ambiguous_name_requires_override_and_historical_relationship_is_season_bound(tmp_path):
    conn, _ = stadium_db(tmp_path)
    conn.execute("INSERT INTO stadiums(stadium_key,stadium_name) VALUES ('old-alpha','Memorial Stadium')")
    old = conn.execute("SELECT stadium_id FROM stadiums WHERE stadium_key='old-alpha'").fetchone()[0]
    conn.execute("INSERT INTO team_stadiums VALUES (1,?,2025,2025,1,'verified season')", (old,))
    conn.executemany("INSERT INTO games (game_id,season_year,home_team_id,neutral_site,game_phase,game_phase_check,venue_text) VALUES (?,?,?,?,?,?,?)", [
        (1, 2025, 1, 0, "regular", None, None),
        (2, 2026, 1, 1, "regular", None, "Memorial Stadium"),
    ])
    result = resolve_games(conn)
    assert conn.execute("SELECT stadium_id FROM games WHERE game_id=1").fetchone()[0] == old
    assert conn.execute("SELECT stadium_id FROM games WHERE game_id=2").fetchone()[0] is None
    assert result["ambiguous"][0]["candidate_ids"] == sorted([old, conn.execute(
        "SELECT stadium_id FROM stadiums WHERE stadium_key='home-bravo'").fetchone()[0]])
    conn.execute("INSERT INTO game_venue_overrides VALUES (2,?,'verified site')", (old,))
    assert resolve_games(conn)["games_unresolved"] == 0
    conn.close()


def test_export_only_uses_resolved_id_and_2026_seed_covers_canonical_teams(tmp_path, backup_db_path):
    conn = sqlite3.connect(tmp_path / "seed.db")
    conn.executescript("CREATE TABLE teams(team_id INTEGER PRIMARY KEY, team_name TEXT);"
                       "CREATE TABLE team_aliases(alias TEXT, team_name TEXT);")
    source = sqlite3.connect(backup_db_path)
    conn.executemany("INSERT INTO teams VALUES (?,?)", source.execute("SELECT team_id,team_name FROM teams"))
    conn.executemany("INSERT INTO team_aliases VALUES (?,?)", source.execute("SELECT alias,team_name FROM team_aliases"))
    source.close()
    ensure_schema(conn)
    from build_stadiums import SEED
    result = seed_stadiums(conn, SEED)
    assert result["teams_seeded"] == 138
    assert result["stadiums_created"] == 138
    assert conn.execute("SELECT COUNT(*) FROM team_stadiums WHERE start_season=2026").fetchone()[0] == 138
    conn.execute("CREATE TABLE games(game_id INTEGER PRIMARY KEY, stadium_id INTEGER)")
    sid = conn.execute("SELECT stadium_id FROM stadiums LIMIT 1").fetchone()[0]
    conn.executemany("INSERT INTO games VALUES (?,?)", [(1, sid), (2, None)])
    assert stadium_map(conn, "games") == {1: (sid, conn.execute(
        "SELECT stadium_name FROM stadiums WHERE stadium_id=?", (sid,)).fetchone()[0]), 2: (None, None)}
    conn.close()
