"""NBA database: connection, migration and idempotent loaders.

Every loader is safe to run twice. A refresh re-reads the whole archive each
time, so "load it again and nothing changes" is not a nicety here, it is the
normal path.

A missing value is stored as NULL. A score is never defaulted to zero — which
in this archive matters more than usual, because the source itself writes a
zero for a game nobody has played. `games.parse_row` is where that is caught.
"""

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

SCHEMA_VERSION = 1
DEFAULT_PATH = Path('db/nba.db')
SCHEMA_SQL = Path(__file__).with_name('schema.sql')

# Columns added after schema version 1, by table. Empty today and kept
# because `CREATE TABLE IF NOT EXISTS` does NOTHING to a table that already
# exists: the schema file cannot add a column to a database someone already
# has, and the symptom is an OperationalError on a column the schema file
# plainly declares, which reads as a typo rather than a missing migration.
ADDED_COLUMNS = {}

GAME_COLUMNS = (
    'game_key', 'provider_id', 'season', 'season_label', 'season_type',
    'postseason', 'game_date', 'tipoff_utc', 'tipoff_tbd', 'neutral_site',
    'completed', 'home_team_key', 'away_team_key', 'home_team_id',
    'away_team_id', 'home_points', 'away_points', 'venue_id', 'venue',
    'venue_city', 'venue_state', 'attendance', 'notes', 'rateable',
    'exclusions', 'snapshot_id',
)
# The key identifies the row; re-running an ingest must not rewrite it.
GAME_IMMUTABLE = ('game_key', 'provider_id', 'season', 'season_label')

TEAM_SEASON_COLUMNS = (
    'team_key', 'season', 'abbreviation', 'location', 'nickname', 'name',
    'conference', 'division', 'logo', 'logo_dark', 'snapshot_id',
)


def connect(path=None):
    """Open the NBA database, creating and migrating it if needed.

    The path is resolved inside the function, never as a def-time default, so
    a test that points this at a temporary file actually redirects it. A
    def-time default has silently defeated a patched path twice in this
    repository.
    """
    target = Path(path) if path is not None else DEFAULT_PATH
    if target != Path(':memory:'):
        target.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(target))
    connection.row_factory = sqlite3.Row
    connection.execute('PRAGMA foreign_keys = ON')
    migrate(connection)
    return connection


def add_missing_columns(connection):
    """Bring an existing table up to the current column list."""
    added = []
    for table, columns in ADDED_COLUMNS.items():
        present = {row['name'] for row in
                   connection.execute(f'PRAGMA table_info({table})')}
        if not present:
            continue
        for name, kind in columns:
            if name in present:
                continue
            connection.execute(f'ALTER TABLE {table} ADD COLUMN {name} {kind}')
            added.append(f'{table}.{name}')
    return added


def migrate(connection):
    """Apply the schema. Idempotent. A future version is refused, not written."""
    added = add_missing_columns(connection)
    connection.executescript(SCHEMA_SQL.read_text(encoding='utf-8'))
    if added:
        connection.commit()
    row = connection.execute('SELECT version FROM schema_version').fetchone()
    if row is None:
        connection.execute('INSERT INTO schema_version (version) VALUES (?)',
                           (SCHEMA_VERSION,))
    elif row['version'] > SCHEMA_VERSION:
        raise RuntimeError(f'Database is at schema version {row["version"]}, '
                           f'this code expects {SCHEMA_VERSION}')
    elif row['version'] < SCHEMA_VERSION:
        connection.execute('UPDATE schema_version SET version = ?',
                           (SCHEMA_VERSION,))
    connection.commit()
    return SCHEMA_VERSION


def record_snapshot(connection, kind, retrieved_at, *, season=None,
                    source=None, source_revision=None, rows_seen=0,
                    notes=None):
    cursor = connection.execute(
        'INSERT INTO snapshot (kind, season, retrieved_at, source,'
        ' source_revision, rows_seen, notes) VALUES (?, ?, ?, ?, ?, ?, ?)',
        (kind, season, retrieved_at, source, source_revision, rows_seen,
         notes))
    connection.commit()
    return cursor.lastrowid


def upsert_team_seasons(connection, rows, *, conferences=None,
                        snapshot_id=None):
    """Store a season's name for each franchise, and the franchise itself.

    `conferences` maps a team id to its conference for that season, read from
    the standings feed. Division is deliberately absent: the feed groups by
    conference only, and writing a division table down in code would be
    inventing a fact this source does not carry.
    """
    conferences = conferences or {}
    for row in rows:
        conference = conferences.get(row['team_id'])
        connection.execute(
            'INSERT INTO team (team_key, team_id, abbreviation, location,'
            ' nickname, name, conference, logo, logo_dark, first_season,'
            ' last_season, snapshot_id)'
            ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
            ' ON CONFLICT(team_key) DO UPDATE SET'
            '   first_season = min(team.first_season, excluded.first_season),'
            '   last_season = max(team.last_season, excluded.last_season),'
            '   snapshot_id = excluded.snapshot_id',
            (row['team_key'], row['team_id'], row['abbreviation'],
             row['location'], row['nickname'],
             row['name'] or f'Team {row["team_id"]}', conference,
             row['logo'], row['logo_dark'], row['season'], row['season'],
             snapshot_id))
        connection.execute(
            'INSERT INTO team_season (' + ', '.join(TEAM_SEASON_COLUMNS) + ')'
            ' VALUES (' + ', '.join('?' * len(TEAM_SEASON_COLUMNS)) + ')'
            ' ON CONFLICT(team_key, season) DO UPDATE SET'
            '   abbreviation = excluded.abbreviation,'
            '   location = excluded.location, nickname = excluded.nickname,'
            '   name = excluded.name, conference = excluded.conference,'
            '   division = excluded.division, logo = excluded.logo,'
            '   logo_dark = excluded.logo_dark,'
            '   snapshot_id = excluded.snapshot_id',
            (row['team_key'], row['season'], row['abbreviation'],
             row['location'], row['nickname'], row['name'], conference, None,
             row['logo'], row['logo_dark'], snapshot_id))
    connection.commit()
    return len(rows)


def refresh_team_identity(connection):
    """Set each franchise's display name and span from what it has played.

    A franchise is called what it is called NOW — team 25 is the Oklahoma
    City Thunder, even though eighteen of its twenty-five season rows say
    Seattle. The per-season names stay in `team_season`, so nothing is lost;
    what this prevents is a standings page from 2026 listing the
    SuperSonics because that row happened to be written last.
    """
    connection.execute(
        'UPDATE team SET'
        ' first_season = (SELECT min(ts.season) FROM team_season ts'
        '   WHERE ts.team_key = team.team_key),'
        ' last_season = (SELECT max(ts.season) FROM team_season ts'
        '   WHERE ts.team_key = team.team_key)'
        ' WHERE EXISTS (SELECT 1 FROM team_season ts'
        '   WHERE ts.team_key = team.team_key)')
    connection.execute(
        'UPDATE team SET'
        ' abbreviation = (SELECT ts.abbreviation FROM team_season ts'
        '   WHERE ts.team_key = team.team_key ORDER BY ts.season DESC'
        '   LIMIT 1),'
        ' location = (SELECT ts.location FROM team_season ts'
        '   WHERE ts.team_key = team.team_key ORDER BY ts.season DESC'
        '   LIMIT 1),'
        ' nickname = (SELECT ts.nickname FROM team_season ts'
        '   WHERE ts.team_key = team.team_key ORDER BY ts.season DESC'
        '   LIMIT 1),'
        ' name = COALESCE((SELECT ts.name FROM team_season ts'
        '   WHERE ts.team_key = team.team_key AND ts.name IS NOT NULL'
        '   ORDER BY ts.season DESC LIMIT 1), team.name),'
        ' conference = COALESCE((SELECT ts.conference FROM team_season ts'
        '   WHERE ts.team_key = team.team_key AND ts.conference IS NOT NULL'
        '   ORDER BY ts.season DESC LIMIT 1), team.conference),'
        ' logo = COALESCE((SELECT ts.logo FROM team_season ts'
        '   WHERE ts.team_key = team.team_key AND ts.logo IS NOT NULL'
        '   ORDER BY ts.season DESC LIMIT 1), team.logo),'
        ' logo_dark = COALESCE((SELECT ts.logo_dark FROM team_season ts'
        '   WHERE ts.team_key = team.team_key AND ts.logo_dark IS NOT NULL'
        '   ORDER BY ts.season DESC LIMIT 1), team.logo_dark)'
        ' WHERE EXISTS (SELECT 1 FROM team_season ts'
        '   WHERE ts.team_key = team.team_key)')
    connection.commit()


def upsert_games(connection, games, *, snapshot_id=None):
    """Store games. Re-running updates scores and leaves keys alone.

    The column list comes from `GAME_COLUMNS` rather than being typed out
    three times — once in the INSERT, once in the ON CONFLICT clause and once
    in the parameter tuple. Those three drift apart, and a row whose value
    tuple is one short of its column list is not a syntax error, it is a game
    stored with its fields shifted by one.
    """
    columns = ', '.join(GAME_COLUMNS)
    placeholders = ', '.join('?' * len(GAME_COLUMNS))
    updates = ', '.join(f'{name} = excluded.{name}' for name in GAME_COLUMNS
                        if name not in GAME_IMMUTABLE)
    statement = (f'INSERT INTO game ({columns}) VALUES ({placeholders})'
                 f' ON CONFLICT(game_key) DO UPDATE SET {updates}')
    for game in games:
        row = dict(game, exclusions=json.dumps(game['exclusions']),
                   snapshot_id=snapshot_id)
        # A missing key is raised, not defaulted to NULL. `games.parse_row`
        # emits every one of these, so a row short of one came from
        # somewhere else, and storing it with blanks is how a half-imported
        # game stops looking half-imported.
        missing = [name for name in GAME_COLUMNS if name not in row]
        if missing:
            raise KeyError(f'{row.get("provider_id", "a game row")} is '
                           f'missing {", ".join(missing)}')
        connection.execute(statement,
                           tuple(row[name] for name in GAME_COLUMNS))
    connection.commit()
    return len(games)


def rateable_games(connection, *, seasons=None):
    """Every rateable game in tip-off order, ready for a replay."""
    query = ('SELECT game_key, season, season_type, game_date, tipoff_utc,'
             ' neutral_site, home_team_key, away_team_key, home_points,'
             ' away_points FROM game WHERE rateable = 1')
    params = []
    if seasons:
        query += ' AND season IN (' + ','.join('?' * len(seasons)) + ')'
        params.extend(seasons)
    query += " ORDER BY game_date, COALESCE(tipoff_utc, ''), provider_id"
    return [dict(row) for row in connection.execute(query, params)]


def season_coverage(connection):
    """Per-season counts read back out of the database."""
    return [dict(row) for row in connection.execute(
        'SELECT season, season_label AS label, count(*) AS games,'
        ' sum(completed) AS played, sum(rateable) AS rateable,'
        ' count(DISTINCT home_team_key) AS home_teams'
        ' FROM game GROUP BY season, season_label ORDER BY season')]


def record_rating_model(connection, name, config, *, fitted_on=None,
                        held_out=None, holdout_brier=None, holdout_games=None,
                        created_at=None, snapshot_id=None):
    """Register a model configuration and return its id. Re-running replaces."""
    stamp = created_at or datetime.now(timezone.utc).isoformat()
    connection.execute(
        'INSERT INTO rating_model (name, config, fitted_on, held_out,'
        ' holdout_brier, holdout_games, created_at, snapshot_id)'
        ' VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
        ' ON CONFLICT(name) DO UPDATE SET config = excluded.config,'
        '   fitted_on = excluded.fitted_on, held_out = excluded.held_out,'
        '   holdout_brier = excluded.holdout_brier,'
        '   holdout_games = excluded.holdout_games,'
        '   created_at = excluded.created_at,'
        '   snapshot_id = excluded.snapshot_id',
        (name, json.dumps(config, sort_keys=True), fitted_on, held_out,
         holdout_brier, holdout_games, stamp, snapshot_id))
    connection.commit()
    row = connection.execute('SELECT model_id FROM rating_model WHERE name = ?',
                             (name,)).fetchone()
    return row['model_id']
