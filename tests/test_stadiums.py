"""Physical venue identity is distinct from home designation and ratings."""
import sqlite3
import json

from build_stadiums import ensure_schema, import_venue_catalog, resolve_games, seed_stadiums
from export_static_data import FIELDS, build_stadium_payload, stadium_map


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
    conn.executemany("INSERT INTO games (game_id,season_year,home_team_id,neutral_site,game_phase,game_phase_check) VALUES (?,?,?,?,?,?)", [
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
    conn.executemany("INSERT INTO scheduled_games (game_id,season_year,home_team_id,neutral_site,game_phase,notes,venue) VALUES (?,?,?,?,?,?,?)", [
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
    # The production bootstrap replaces the backup's dead UMass duplicate
    # with the canonical Massachusetts team before the stadium seed runs.
    conn.execute("UPDATE teams SET team_name='Massachusetts' WHERE team_name='UMass'")
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


def test_cfbd_venue_id_resolves_same_name_neutral_site_before_name_or_override(tmp_path):
    conn, _ = stadium_db(tmp_path)
    conn.execute("INSERT INTO stadiums(stadium_key,stadium_name) VALUES ('other','Memorial Stadium')")
    conn.execute("INSERT INTO games (game_id,season_year,home_team_id,neutral_site,game_phase,source_venue_id,venue_text) "
                 "VALUES (31,2026,1,1,'regular',987,'Memorial Stadium')")
    catalog = tmp_path / "venues.json"
    catalog.write_text(json.dumps([{"id": 987, "name": "Memorial Stadium", "city": "Somewhere",
                                    "state": "MN", "latitude": 44.1, "longitude": -92.4, "capacity": 30000}]))
    result = import_venue_catalog(conn, catalog)
    assert result["catalog_created"] == 1 and result["catalog_ambiguous"] == 1
    sid = conn.execute("SELECT stadium_id FROM stadiums WHERE cfbd_venue_id=987").fetchone()[0]
    assert conn.execute("SELECT city, capacity FROM stadiums WHERE stadium_id=?", (sid,)).fetchone() == ("Somewhere", 30000)
    conn.execute("INSERT INTO game_venue_overrides VALUES (31,1,'lower priority')")
    assert resolve_games(conn)["ambiguous"] == []
    assert conn.execute("SELECT stadium_id,neutral_site FROM games WHERE game_id=31").fetchone() == (sid, 1)
    assert import_venue_catalog(conn, catalog)["catalog_created"] == 0
    conn.close()


def test_existing_stadium_table_migrates_to_unique_cfbd_ids(tmp_path):
    conn = sqlite3.connect(tmp_path / "legacy.db")
    conn.executescript("""CREATE TABLE teams(team_id INTEGER PRIMARY KEY,team_name TEXT);
        CREATE TABLE stadiums(stadium_id INTEGER PRIMARY KEY,stadium_key TEXT UNIQUE,
            stadium_name TEXT,city TEXT,state TEXT,latitude REAL,longitude REAL,
            capacity INTEGER,surface TEXT,indoor INTEGER,opened_year INTEGER);
        INSERT INTO teams VALUES(1,'Alpha');
        INSERT INTO stadiums(stadium_key,stadium_name) VALUES('old','Old Field');""")
    ensure_schema(conn)
    ensure_schema(conn)
    conn.execute("UPDATE stadiums SET cfbd_venue_id=123 WHERE stadium_key='old'")
    conn.execute("INSERT INTO stadiums(stadium_key,stadium_name) VALUES('new','New Field')")
    try:
        conn.execute("UPDATE stadiums SET cfbd_venue_id=123 WHERE stadium_key='new'")
    except sqlite3.IntegrityError:
        pass
    else:
        raise AssertionError("CFBD venue IDs must remain unique after migration")
    conn.close()


def test_catalog_does_not_merge_same_name_with_conflicting_location(tmp_path):
    conn, _ = stadium_db(tmp_path)
    conn.execute("UPDATE stadiums SET city='One City',state='UT' WHERE stadium_key='home-alpha'")
    conn.execute("INSERT INTO games(game_id,season_year,home_team_id,neutral_site,game_phase,source_venue_id) "
                 "VALUES (42,2026,2,1,'regular',456)")
    catalog = tmp_path / 'venues.json'
    catalog.write_text(json.dumps([{'id': 456, 'name': 'Alpha Field', 'city': 'Other City', 'state': 'MN'}]))
    assert import_venue_catalog(conn, catalog)['catalog_created'] == 1
    assert conn.execute("SELECT cfbd_venue_id FROM stadiums WHERE stadium_key='home-alpha'").fetchone()[0] is None
    assert conn.execute("SELECT stadium_key FROM stadiums WHERE cfbd_venue_id=456").fetchone()[0] == 'cfbd-456'
    conn.close()


def test_stadium_export_keeps_games_at_physical_site_including_neutral(tmp_path):
    conn, _ = stadium_db(tmp_path)
    sid = conn.execute("SELECT stadium_id FROM stadiums WHERE stadium_key='home-alpha'").fetchone()[0]
    conn.execute("UPDATE stadiums SET latitude=44.0, longitude=-92.0, capacity=10000 WHERE stadium_id=?", (sid,))
    def row(gid, completed, neutral, date):
        fields = {"game_id": gid, "completed": completed, "neutral": neutral, "date": date,
                  "home_id": 2, "away_id": 1, "home_score": 21 if completed else None,
                  "away_score": 17 if completed else None, "stadium_id": sid}
        return [fields.get(f) for f in FIELDS]
    payload = build_stadium_payload(conn, {2026: {"games": [row(3, 1, 1, "2026-09-12"),
                                                          row(4, 0, 0, "2026-10-03")]}})
    alpha = next(s for s in payload["stadiums"] if s["id"] == sid)
    assert alpha["lat"] == 44.0 and alpha["capacity"] == 10000
    assert alpha["completed_count"] == 1 and alpha["recent"][0]["neutral"] is True
    assert alpha["upcoming"][0]["id"] == 4
    conn.close()


def test_stadium_hosts_follow_latest_exported_season_and_allow_shared_homes(tmp_path):
    conn, _ = stadium_db(tmp_path)
    alpha = conn.execute("SELECT stadium_id FROM stadiums WHERE stadium_key='home-alpha'").fetchone()[0]
    bravo = conn.execute("SELECT stadium_id FROM stadiums WHERE stadium_key='home-bravo'").fetchone()[0]
    conn.execute("UPDATE team_stadiums SET end_season=2026 WHERE team_id=2 AND stadium_id=?", (bravo,))
    conn.execute("INSERT INTO team_stadiums VALUES (2,?,2027,NULL,1,'shared site')", (alpha,))
    in_2026 = build_stadium_payload(conn, {2026: {"games": []}})["stadiums"]
    in_2027 = build_stadium_payload(conn, {2027: {"games": []}})["stadiums"]
    assert {t['name'] for s in in_2026 for t in s['teams'] if s['id'] == bravo} == {'Bravo'}
    assert {t['name'] for s in in_2027 for t in s['teams'] if s['id'] == alpha} == {'Alpha', 'Bravo'}
    assert all(s['id'] != bravo for s in in_2027)
    conn.close()
