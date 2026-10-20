"""Basketball database: connection, migration and idempotent loaders.

Every loader is safe to run twice. Repeated ingestion of the same snapshot must
leave the same rows, because a refresh re-reads windows it has already seen and
a resumed run re-reads the window it died in.

Missing provider values are stored as NULL. A score, a minute count and a
tip-off time are never defaulted to zero, because a zero is a measurement and
an absence is not.
"""

import json
import sqlite3
from pathlib import Path

from .coverage import classify_game
from .games import game_day
from .identity import entity_key, season_label

SCHEMA_VERSION = 1
DEFAULT_PATH = Path('db/ncaambb.db')
SCHEMA_SQL = Path(__file__).with_name('schema.sql')


def connect(path=None):
    """Open the basketball database, creating and migrating it if needed.

    The path is resolved inside the function, never as a def-time default, so a
    test that points this at a temporary file actually redirects it.
    """
    target = Path(path) if path is not None else DEFAULT_PATH
    if target != Path(':memory:'):
        target.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(target))
    connection.row_factory = sqlite3.Row
    connection.execute('PRAGMA foreign_keys = ON')
    migrate(connection)
    return connection


def migrate(connection):
    """Apply the schema. Idempotent: every statement is CREATE IF NOT EXISTS."""
    connection.executescript(SCHEMA_SQL.read_text(encoding='utf-8'))
    row = connection.execute('SELECT version FROM schema_version').fetchone()
    if row is None:
        connection.execute('INSERT INTO schema_version (version) VALUES (?)',
                           (SCHEMA_VERSION,))
    elif row['version'] != SCHEMA_VERSION:
        raise RuntimeError(f'Database is at schema version {row["version"]}, '
                           f'this code expects {SCHEMA_VERSION}')
    connection.commit()
    return SCHEMA_VERSION


def record_snapshot(connection, kind, retrieved_at, *, season=None,
                    source_revision=None, requests=0, retries=0, rows_seen=0,
                    notes=None):
    """Register an ingestion run and return its id."""
    cursor = connection.execute(
        'INSERT INTO snapshot (kind, season, retrieved_at, source_revision,'
        ' requests, retries, rows_seen, notes) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        (kind, season, retrieved_at, source_revision, requests, retries,
         rows_seen, notes))
    connection.commit()
    return cursor.lastrowid


def _text(value):
    """Keep a missing value missing rather than turning it into a string."""
    return value if isinstance(value, str) and value else None


def upsert_teams(connection, teams, *, snapshot_id=None):
    """Store team identities. Re-running updates names and leaves keys alone."""
    stored = 0
    for team in teams:
        key = entity_key('team', team['id'])
        connection.execute(
            'INSERT INTO team (team_key, provider_id, school, mascot,'
            ' abbreviation, display_name, short_name, primary_color,'
            ' secondary_color, current_venue, current_city, current_state,'
            ' first_seen_snapshot, last_seen_snapshot)'
            ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
            ' ON CONFLICT(team_key) DO UPDATE SET'
            '   school = excluded.school, mascot = excluded.mascot,'
            '   abbreviation = excluded.abbreviation,'
            '   display_name = excluded.display_name,'
            '   short_name = excluded.short_name,'
            '   primary_color = excluded.primary_color,'
            '   secondary_color = excluded.secondary_color,'
            '   current_venue = excluded.current_venue,'
            '   current_city = excluded.current_city,'
            '   current_state = excluded.current_state,'
            '   last_seen_snapshot = excluded.last_seen_snapshot',
            (key, team['id'], _text(team.get('school')) or str(team['id']),
             _text(team.get('mascot')), _text(team.get('abbreviation')),
             _text(team.get('displayName')), _text(team.get('shortDisplayName')),
             _text(team.get('primaryColor')), _text(team.get('secondaryColor')),
             _text(team.get('currentVenue')), _text(team.get('currentCity')),
             _text(team.get('currentState')), snapshot_id, snapshot_id))
        stored += 1
    connection.commit()
    return stored


def upsert_team_seasons(connection, season, teams, *, division_one=True,
                        snapshot_id=None):
    """Record which teams belong to a season, and whether they are Division I."""
    label = season_label(season)
    for team in teams:
        key = entity_key('team', team['id'])
        connection.execute(
            'INSERT INTO team_season (team_key, season, season_label,'
            ' conference_id, conference, division_one, snapshot_id)'
            ' VALUES (?, ?, ?, ?, ?, ?, ?)'
            ' ON CONFLICT(team_key, season) DO UPDATE SET'
            '   conference_id = excluded.conference_id,'
            '   conference = excluded.conference,'
            '   division_one = excluded.division_one,'
            '   snapshot_id = excluded.snapshot_id',
            (key, season, label, team.get('conferenceId'),
             _text(team.get('conference')), 1 if division_one else 0,
             snapshot_id))
    connection.commit()
    return len(teams)


def placeholder_teams(games, known_ids):
    """Teams that appear only as an opponent, so games can reference them.

    A non-Division I opponent is never in `/teams`, but its games are retained,
    and a game row must still point at a real team. The placeholder carries the
    provider's own name for the side and nothing invented.
    """
    missing = {}
    for game in games:
        for side, name in (('homeTeamId', 'homeTeam'), ('awayTeamId', 'awayTeam')):
            identifier = game.get(side)
            if type(identifier) is not int or identifier in known_ids:
                continue
            missing.setdefault(identifier, dict(id=identifier,
                                                school=_text(game.get(name))
                                                or f'Team {identifier}'))
    return list(missing.values())


def upsert_games(connection, games, division_one_ids, *, snapshot_id=None):
    """Store games with their rating eligibility and the reasons against it."""
    members = set(division_one_ids)
    for game in games:
        verdict = classify_game(game, members)
        day = game_day(game)
        if day is None:
            raise ValueError(f'Game {game.get("id")} has no usable start date')
        season = game['season']
        connection.execute(
            'INSERT INTO game (game_key, provider_id, season, season_label,'
            ' season_type, tournament, status, start_date, start_date_utc,'
            ' start_time_tbd, neutral_site, conference_game, home_team_key,'
            ' away_team_key, home_points, away_points, home_period_points,'
            ' away_period_points, venue_id, venue, city, state, attendance,'
            ' rateable, division_one, exclusions, snapshot_id)'
            ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,'
            ' ?, ?, ?, ?, ?, ?, ?, ?)'
            ' ON CONFLICT(game_key) DO UPDATE SET'
            '   status = excluded.status, start_date = excluded.start_date,'
            '   start_date_utc = excluded.start_date_utc,'
            '   start_time_tbd = excluded.start_time_tbd,'
            '   neutral_site = excluded.neutral_site,'
            '   conference_game = excluded.conference_game,'
            '   home_points = excluded.home_points,'
            '   away_points = excluded.away_points,'
            '   home_period_points = excluded.home_period_points,'
            '   away_period_points = excluded.away_period_points,'
            '   venue_id = excluded.venue_id, venue = excluded.venue,'
            '   city = excluded.city, state = excluded.state,'
            '   attendance = excluded.attendance,'
            '   rateable = excluded.rateable,'
            '   division_one = excluded.division_one,'
            '   exclusions = excluded.exclusions,'
            '   snapshot_id = excluded.snapshot_id',
            (entity_key('game', game['id']), game['id'], season,
             season_label(season), game['seasonType'],
             _text(game.get('tournament')), game['status'],
             game['startDate'], day.isoformat(),
             1 if game.get('startTimeTbd') is True else 0,
             1 if game.get('neutralSite') is True else 0,
             None if game.get('conferenceGame') is None
             else (1 if game['conferenceGame'] else 0),
             entity_key('team', game['homeTeamId']),
             entity_key('team', game['awayTeamId']),
             game.get('homePoints'), game.get('awayPoints'),
             json.dumps(game['homePeriodPoints'])
             if isinstance(game.get('homePeriodPoints'), list) else None,
             json.dumps(game['awayPeriodPoints'])
             if isinstance(game.get('awayPeriodPoints'), list) else None,
             game.get('venueId'), _text(game.get('venue')),
             _text(game.get('city')), _text(game.get('state')),
             game.get('attendance'),
             1 if verdict['rateable'] else 0,
             1 if verdict['division_one'] else 0,
             json.dumps(verdict['exclusions']), snapshot_id))
    connection.commit()
    return len(games)


def load_season(connection, season, games, division_one_teams, *,
                snapshot_id=None):
    """Store one season end to end: teams, membership, then games.

    Teams come first because a game references both sides, and opponents the
    provider does not list for the season are stored as placeholders so their
    games can be kept rather than dropped.
    """
    known = {team['id'] for team in division_one_teams}
    upsert_teams(connection, division_one_teams, snapshot_id=snapshot_id)
    upsert_team_seasons(connection, season, division_one_teams,
                        division_one=True, snapshot_id=snapshot_id)
    others = placeholder_teams(games, known)
    upsert_teams(connection, others, snapshot_id=snapshot_id)
    upsert_team_seasons(connection, season, others, division_one=False,
                        snapshot_id=snapshot_id)
    stored = upsert_games(connection, games, known, snapshot_id=snapshot_id)
    return dict(season=season, games=stored, division_one_teams=len(known),
                other_teams=len(others))


def _number(value):
    """A measurement, or None. A bool is not a measurement and neither is a
    missing value: coercing either one writes a number nobody recorded."""
    if value is None or isinstance(value, bool):
        return None
    return value if isinstance(value, (int, float)) else None


def _hometown(player):
    town = player.get('hometown')
    if not isinstance(town, dict):
        return (None, None, None)
    return (_text(town.get('city')), _text(town.get('state')),
            _text(town.get('country')))


def upsert_players(connection, players, *, snapshot_id=None):
    """Store player identities, keyed by the provider's athlete id.

    Two people who share a name stay two rows. A name is never an identity:
    `provider_id` is, and a row with no usable id is refused rather than
    guessed at.
    """
    stored = 0
    for player in players:
        key = entity_key('player', player['id'])
        city, state, country = _hometown(player)
        connection.execute(
            'INSERT INTO player (player_key, provider_id, name, first_name,'
            ' last_name, date_of_birth, hometown, hometown_state,'
            ' hometown_country, snapshot_id)'
            ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
            ' ON CONFLICT(player_key) DO UPDATE SET'
            '   name = COALESCE(excluded.name, player.name),'
            '   first_name = COALESCE(excluded.first_name, player.first_name),'
            '   last_name = COALESCE(excluded.last_name, player.last_name),'
            '   date_of_birth = COALESCE(excluded.date_of_birth,'
            '                            player.date_of_birth),'
            '   hometown = COALESCE(excluded.hometown, player.hometown),'
            '   hometown_state = COALESCE(excluded.hometown_state,'
            '                             player.hometown_state),'
            '   hometown_country = COALESCE(excluded.hometown_country,'
            '                               player.hometown_country),'
            '   snapshot_id = excluded.snapshot_id',
            (key, player['id'], _text(player.get('name')),
             _text(player.get('firstName')), _text(player.get('lastName')),
             _text(player.get('dateOfBirth')), city, state, country,
             snapshot_id))
        stored += 1
    connection.commit()
    return stored


def upsert_player_team_seasons(connection, season, pairs, *, snapshot_id=None):
    """Store roster membership. A transfer is two rows, never two players."""
    label = season_label(season)
    for player, team_id in pairs:
        connection.execute(
            'INSERT INTO player_team_season (player_key, team_key, season,'
            ' season_label, jersey, position, height, weight, snapshot_id)'
            ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'
            ' ON CONFLICT(player_key, team_key, season) DO UPDATE SET'
            '   jersey = excluded.jersey, position = excluded.position,'
            '   height = excluded.height, weight = excluded.weight,'
            '   snapshot_id = excluded.snapshot_id',
            (entity_key('player', player['id']), entity_key('team', team_id),
             season, label, _text(player.get('jersey')),
             _text(player.get('position')), player.get('height'),
             player.get('weight'), snapshot_id))
    connection.commit()
    return len(pairs)


def _known_games(connection):
    return {row['provider_id'] for row in
            connection.execute('SELECT provider_id FROM game')}


def upsert_box_scores(connection, boxes, *, snapshot_id=None):
    """Store team and player box score lines for games already loaded.

    A line for a game this database does not hold is counted and reported, not
    dropped in silence: it means the schedule load did not cover that game.

    `team_game_stat.points` is the sum of the box score's own player points,
    which is a box-score measurement and not the authoritative final score in
    `game`. Where the two disagree the count is returned so a caller can show
    it, because that disagreement is exactly what the pilot found in the lineup
    feed and it must stay visible.
    """
    skipped, players_stored, mismatches = 0, 0, 0
    known = _known_games(connection)
    for box in boxes:
        game_id, team_id = box.get('gameId'), box.get('teamId')
        if game_id not in known:
            skipped += 1
            continue
        game_key = entity_key('game', game_id)
        team_key = entity_key('team', team_id)
        lines = box.get('players') or []
        scored = [_number(row.get('points')) for row in lines]
        measured = [value for value in scored if value is not None]
        # No player line carries a points value, so nobody measured this team's
        # score. Summing an empty list would write 0, which reads downstream as
        # a team that was held scoreless.
        points = sum(measured) if measured else None
        connection.execute(
            'INSERT INTO team_game_stat (game_key, team_key, is_home, points,'
            ' game_minutes, pace, possessions, snapshot_id)'
            ' VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
            ' ON CONFLICT(game_key, team_key) DO UPDATE SET'
            '   is_home = excluded.is_home, points = excluded.points,'
            '   game_minutes = excluded.game_minutes, pace = excluded.pace,'
            '   possessions = excluded.possessions,'
            '   snapshot_id = excluded.snapshot_id',
            (game_key, team_key, 1 if box.get('isHome') is True else 0,
             points, box.get('gameMinutes'), box.get('gamePace'),
             box.get('possessions'), snapshot_id))

        stored_final = connection.execute(
            'SELECT home_team_key, home_points, away_points FROM game'
            ' WHERE game_key = ?', (game_key,)).fetchone()
        if stored_final is not None and points is not None:
            side = ('home_points' if stored_final['home_team_key'] == team_key
                    else 'away_points')
            if stored_final[side] is not None and stored_final[side] != points:
                mismatches += 1

        for row in lines:
            athlete = row.get('athleteId')
            if type(athlete) is not int:
                continue
            minutes = row.get('minutes')
            connection.execute(
                'INSERT INTO player_game_stat (game_key, player_key, team_key,'
                ' starter, minutes, points, snapshot_id)'
                ' VALUES (?, ?, ?, ?, ?, ?, ?)'
                ' ON CONFLICT(game_key, player_key) DO UPDATE SET'
                '   team_key = excluded.team_key, starter = excluded.starter,'
                '   minutes = excluded.minutes, points = excluded.points,'
                '   snapshot_id = excluded.snapshot_id',
                (game_key, entity_key('player', athlete), team_key,
                 None if row.get('starter') is None
                 else (1 if row['starter'] else 0),
                 _number(minutes), _number(row.get('points')), snapshot_id))
            players_stored += 1
    connection.commit()
    return dict(team_lines=len(boxes) - skipped, player_lines=players_stored,
                skipped_unknown_games=skipped, points_mismatches=mismatches)
