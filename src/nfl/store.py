"""NFL database: connection, migration and idempotent loaders.

Every loader is safe to run twice. A refresh re-reads the whole archive each
time, so "load it again and nothing changes" is not a nicety here, it is the
normal path.

A missing value is stored as NULL. A score is never defaulted to zero: a zero
is a measurement and an absence is not.
"""

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .identity import alignment, entity_key, franchise_code, franchise_key

SCHEMA_VERSION = 3
DEFAULT_PATH = Path('db/nfl.db')
SCHEMA_SQL = Path(__file__).with_name('schema.sql')

# Columns added after schema version 1, by table.
#
# Schema 3 adds TABLES rather than columns (`player`, `player_season`,
# `player_stat_season`, `player_stat`, `draft_pick`), and `CREATE TABLE IF NOT
# EXISTS` does create a table that is absent, so there is nothing for this
# table to carry on its behalf. It is only an existing table gaining a column
# that the schema file cannot do by itself.
#
# `CREATE TABLE IF NOT EXISTS` does NOTHING to a table that already exists, so
# running the new schema file against an old database leaves its `game` table
# at the 26 columns it was created with while every INSERT in this module now
# names 41. The symptom is an sqlite3.OperationalError on a column the schema
# file plainly declares, which reads as a typo rather than as a missing
# migration. Adding them explicitly is what makes `migrate()` a migration.
ADDED_COLUMNS = {
    'game': (
        ('home_coach', 'TEXT'),
        ('away_coach', 'TEXT'),
        ('spread_line', 'REAL'),
        ('total_line', 'REAL'),
        ('home_moneyline', 'INTEGER'),
        ('away_moneyline', 'INTEGER'),
        ('home_qb_id', 'TEXT'),
        ('away_qb_id', 'TEXT'),
        ('home_qb_name', 'TEXT'),
        ('away_qb_name', 'TEXT'),
        ('referee', 'TEXT'),
        ('home_rest', 'INTEGER'),
        ('away_rest', 'INTEGER'),
        ('temp', 'INTEGER'),
        ('wind', 'INTEGER'),
    ),
}

# The columns the widened `game` row carries, in the order every INSERT and
# the schema file list them. Written once because three places had drifted
# apart by one column before this list existed.
GAME_COLUMNS = (
    'game_key', 'provider_id', 'season', 'season_label', 'season_type',
    'postseason', 'week', 'game_date', 'kickoff_utc', 'kickoff_tbd',
    'neutral_site', 'home_code', 'away_code', 'home_team_key',
    'away_team_key', 'home_points', 'away_points', 'overtime',
    'divisional_game', 'roof', 'surface', 'stadium_id', 'stadium',
    'home_coach', 'away_coach', 'spread_line', 'total_line',
    'home_moneyline', 'away_moneyline', 'home_qb_id', 'away_qb_id',
    'home_qb_name', 'away_qb_name', 'referee', 'home_rest', 'away_rest',
    'temp', 'wind', 'rateable', 'exclusions', 'snapshot_id',
)
# The key identifies the row; re-running an ingest must not rewrite it.
GAME_IMMUTABLE = ('game_key', 'provider_id', 'season', 'season_label')


def connect(path=None):
    """Open the NFL database, creating and migrating it if needed.

    The path is resolved inside the function, never as a def-time default, so
    a test that points this at a temporary file actually redirects it.
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
    """Bring an existing table up to the current column list.

    Returns what it added, so a caller can say so rather than migrating
    silently.
    """
    added = []
    for table, columns in ADDED_COLUMNS.items():
        present = {row['name'] for row in
                   connection.execute(f'PRAGMA table_info({table})')}
        if not present:
            # The table does not exist yet; the schema file will create it
            # with every column already in place.
            continue
        for name, kind in columns:
            if name in present:
                continue
            connection.execute(
                f'ALTER TABLE {table} ADD COLUMN {name} {kind}')
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


def _text(value):
    return value if isinstance(value, str) and value else None


def _location(row):
    """The city, with the source's one bad row repaired rather than shown.

    teams.csv gives `location` as the city for every row but the Rams' recent
    ones, where it repeats the full name ("Los Angeles Rams"). Dropping a
    trailing nickname fixes that without touching any correct row, and
    leaves the field NULL rather than guessing when nothing is left.
    """
    location = _text(row.get('location'))
    nickname = _text(row.get('nickname'))
    if location and nickname and location != nickname \
            and location.endswith(' ' + nickname):
        location = location[:-(len(nickname) + 1)].strip()
    return location or None


def refresh_team_spans(connection):
    """Set each franchise's first and last season from the games it played.

    The source's team file starts in 2002 and the archive starts in 1999, so
    a span taken from the team file alone is three seasons short and says so
    nowhere. The games are the evidence.
    """
    connection.execute(
        "UPDATE team SET"
        " first_season = (SELECT min(g.season) FROM game g"
        "   WHERE g.home_team_key = team.team_key"
        "      OR g.away_team_key = team.team_key),"
        " last_season = (SELECT max(g.season) FROM game g"
        "   WHERE g.home_team_key = team.team_key"
        "      OR g.away_team_key = team.team_key)"
        " WHERE EXISTS (SELECT 1 FROM game g"
        "   WHERE g.home_team_key = team.team_key"
        "      OR g.away_team_key = team.team_key)")
    connection.commit()


def upsert_teams(connection, rows, *, snapshot_id=None):
    """Store franchises and their per-season codes and names.

    `rows` are the source's team-season records: dicts with `season`, `team`
    and the name fields. A row for `STL` in 2015 and one for `LA` in 2016 both
    resolve to the Rams' single franchise row; the code rows keep the
    distinction.
    """
    franchises = {}
    for row in rows:
        code = (row.get('team') or '').strip()
        season = int(row['season'])
        key = franchise_key(code)
        conference, division = alignment(code, season)
        name = _text(row.get('full')) or code
        current = franchises.setdefault(key, dict(
            key=key, code=franchise_code(code), name=name,
            location=_location(row),
            nickname=_text(row.get('nickname')),
            conference=conference, division=division,
            first_season=season, last_season=season))
        current['first_season'] = min(current['first_season'], season)
        # The franchise's display name is its most recent one: the Rams are
        # the Los Angeles Rams today even though most of their rows say St.
        # Louis. Per-season names stay in team_code.
        if season >= current['last_season']:
            current.update(last_season=season, name=name,
                           location=_location(row),
                           nickname=_text(row.get('nickname')),
                           conference=conference or current['conference'],
                           division=division or current['division'])
    for team in franchises.values():
        connection.execute(
            'INSERT INTO team (team_key, code, name, location, nickname,'
            ' conference, division, first_season, last_season, snapshot_id)'
            ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
            ' ON CONFLICT(team_key) DO UPDATE SET'
            '   name = excluded.name, location = excluded.location,'
            '   nickname = excluded.nickname,'
            '   conference = excluded.conference,'
            '   division = excluded.division,'
            '   first_season = min(team.first_season, excluded.first_season),'
            '   last_season = max(team.last_season, excluded.last_season),'
            '   snapshot_id = excluded.snapshot_id',
            (team['key'], team['code'], team['name'], team['location'],
             team['nickname'], team['conference'], team['division'],
             team['first_season'], team['last_season'], snapshot_id))
    for row in rows:
        code = (row.get('team') or '').strip()
        season = int(row['season'])
        conference, division = alignment(code, season)
        connection.execute(
            'INSERT INTO team_code (code, season, team_key, name, location,'
            ' nickname, conference, division, snapshot_id)'
            ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'
            ' ON CONFLICT(code, season) DO UPDATE SET'
            '   team_key = excluded.team_key, name = excluded.name,'
            '   location = excluded.location, nickname = excluded.nickname,'
            '   conference = excluded.conference,'
            '   division = excluded.division,'
            '   snapshot_id = excluded.snapshot_id',
            (code, season, franchise_key(code), _text(row.get('full')),
             _location(row), _text(row.get('nickname')),
             conference, division, snapshot_id))
    connection.commit()
    return len(franchises)


def teams_from_games(games):
    """Franchise stubs for codes the team file does not cover.

    The source's team file starts in 2002; the archive starts in 1999. Rather
    than leave three seasons of games pointing at absent teams, or invent
    names for them, a stub carries the code and the seasons it was seen in,
    and the real name arrives with the first team-file row that mentions it.
    """
    seen = {}
    for game in games:
        for code in (game['home_code'], game['away_code']):
            key = franchise_key(code)
            row = seen.setdefault(key, dict(
                team=franchise_code(code), season=game['season'],
                full=None, location=None, nickname=None))
            row['season'] = max(row['season'], game['season'])
    return list(seen.values())


def upsert_team_stubs(connection, games, *, snapshot_id=None):
    """Insert franchises mentioned only by the archive, leaving names alone."""
    stored = 0
    for stub in teams_from_games(games):
        code = stub['team']
        key = franchise_key(code)
        conference, division = alignment(code, stub['season'])
        cursor = connection.execute(
            'INSERT INTO team (team_key, code, name, conference, division,'
            ' first_season, last_season, snapshot_id)'
            ' VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
            ' ON CONFLICT(team_key) DO NOTHING',
            (key, code, code, conference, division, stub['season'],
             stub['season'], snapshot_id))
        stored += cursor.rowcount if cursor.rowcount > 0 else 0
    connection.commit()
    return stored


def upsert_games(connection, games, *, snapshot_id=None):
    """Store games. Re-running updates scores and leaves keys alone.

    The column list comes from `GAME_COLUMNS` rather than being typed out
    three times — once in the INSERT, once in the ON CONFLICT clause and once
    in the parameter tuple. Those three had to be edited in step, and a row
    whose value tuple is one short of its column list is not a syntax error,
    it is a game stored with its fields shifted by one.
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
        # emits every one of these, so a row that is short of one came from
        # somewhere else and silently storing it with blanks is how a
        # half-imported game stops looking half-imported.
        missing = [name for name in GAME_COLUMNS if name not in row]
        if missing:
            raise KeyError(f'{row.get("provider_id", "a game row")} is '
                           f'missing {", ".join(missing)}')
        connection.execute(statement,
                           tuple(row[name] for name in GAME_COLUMNS))
    connection.commit()
    return len(games)


def rateable_games(connection, *, seasons=None):
    """Every rateable game in kickoff order, ready for a replay."""
    query = ('SELECT game_key, season, game_date, kickoff_utc, neutral_site,'
             ' home_team_key, away_team_key, home_points, away_points'
             ' FROM game WHERE rateable = 1')
    params = []
    if seasons:
        query += ' AND season IN (' + ','.join('?' * len(seasons)) + ')'
        params.extend(seasons)
    query += " ORDER BY game_date, COALESCE(kickoff_utc, ''), provider_id"
    return [dict(row) for row in connection.execute(query, params)]


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


def upsert_team_ratings(connection, model_id, rows, *, updated_at=None):
    stamp = updated_at or datetime.now(timezone.utc).isoformat()
    for row in rows:
        connection.execute(
            'INSERT INTO team_rating (team_key, season, model_id, rating,'
            ' games_counted, updated_at) VALUES (?, ?, ?, ?, ?, ?)'
            ' ON CONFLICT(team_key, season, model_id) DO UPDATE SET'
            '   rating = excluded.rating,'
            '   games_counted = excluded.games_counted,'
            '   updated_at = excluded.updated_at',
            (row['team_key'], row['season'], model_id, row['rating'],
             row['games_counted'], stamp))
    connection.commit()
    return len(rows)


def upsert_rating_games(connection, model_id, rows):
    for row in rows:
        connection.execute(
            'INSERT INTO rating_game (game_key, model_id, home_pregame,'
            ' away_pregame, home_expected, home_postgame, away_postgame,'
            ' rating_change, multiplier, counted)'
            ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
            ' ON CONFLICT(game_key, model_id) DO UPDATE SET'
            '   home_pregame = excluded.home_pregame,'
            '   away_pregame = excluded.away_pregame,'
            '   home_expected = excluded.home_expected,'
            '   home_postgame = excluded.home_postgame,'
            '   away_postgame = excluded.away_postgame,'
            '   rating_change = excluded.rating_change,'
            '   multiplier = excluded.multiplier, counted = excluded.counted',
            (row['game_key'], model_id, row['home_pregame'],
             row['away_pregame'], row['home_expected'], row['home_postgame'],
             row['away_postgame'], row['rating_change'], row['multiplier'],
             row['counted']))
    connection.commit()
    return len(rows)
