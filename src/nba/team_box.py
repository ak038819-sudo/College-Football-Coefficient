"""Load the NBA team box scores: the counts the team statistics divide.

`python -m src.nba.team_box` fetches a season's `team_box` feed, keeps the
two rows a game, stores them and DELETES the file before fetching the next
season — the stream-and-discard pattern `src/nba/plays.py` and
`src/nba/people.py` already use. A season is 2,626 rows and 128 KB of
Parquet, so 26 of them are small; the discard is for consistency with the
rest of the pipeline rather than for space.

It runs AFTER `src/nba/ingest.py`, because a box score for a game the
schedule does not carry is dropped rather than invented, and BEFORE
`src/nba/export.py`, which decides from this table whether there are team
statistics to publish at all.

FOUR TRAPS, each measured on the archive before it was written down.

1. THE FEED CALLS THE NBA CUP FINAL A REGULAR-SEASON GAME. The same trap
   `src/nba/people.py` documents: the schedule says `cup_final` and the rest
   of the archive keeps it out of won-lost records, so a stored row takes
   the SCHEDULE's season type, for every type rather than only for this one.
   Left as the feed has it, the two finalists each carry 83 regular-season
   games into a season rate.

2. THE ALL-STAR GAME IS ALSO FILED AS A REGULAR-SEASON GAME. 2025-26 has
   four such games under `season_type` 2 — World, Team Stars, Team Stripes —
   and they pass any filter built on season type. What marks them is that
   their sides are not franchises, so `identity.team_key` refuses them and
   the count is reported rather than lost.

3. `total_turnovers` MEANS TWO DIFFERENT THINGS IN TWO ERAS, and the wrong
   one looks right. The identity `total = turnovers + team_turnovers` holds
   on 62,028 of the 62,051 rows that carry a total. But in every season from
   2001-02 to 2011-12, `team_turnovers` is a COPY of `turnovers` — equal on
   2,520 of 2,520 rows in 2002 and on all of them in each of the eleven
   seasons — so the total is the team's turnovers counted TWICE: 28.9 a game
   against a true 14.4. Taken at face value it inflates every possession
   estimate in those seasons by about 12%, and because PxP+ is measured
   against the league it still centres on 100 and still looks right.
   2017-18 is the opposite hole: `total_turnovers` is NULL on 2,620 of its
   2,622 rows, the only season in the archive missing it.

   So `turnover_basis` is decided per SEASON from the season's own rows and
   stored beside the count that was chosen. No single row shows which era it
   belongs to, which is why this cannot be a per-row rule.

4. A COUNT CAN SIMPLY BE MISSING. Game 401810169 (Chicago at Orlando,
   2025-12-01) carries no `total_turnovers` for Chicago. It is stored as
   NULL and the game is unmeasurable for both sides, because the possession
   denominator is shared. Nothing here defaults a count to zero: a zero is a
   measurement.

WHAT IS NOT HERE: not one rate, index or grade. The table holds counts and
`src/nba/team_metrics.py` is the only place that divides them.
"""

import argparse
from datetime import datetime, timezone

from . import identity, source as source_module, store as store_module

DATASET = 'team_box'

# Feed column -> stored column. `total_turnovers` is listed before
# `turnovers` deliberately: the first is the one the arithmetic reads, and
# reading this tuple should not leave the impression that they are
# interchangeable.
COUNTS = (
    ('team_score', 'points'),
    ('field_goals_attempted', 'field_goals_attempted'),
    ('free_throws_attempted', 'free_throws_attempted'),
    ('offensive_rebounds', 'offensive_rebounds'),
    ('total_turnovers', 'total_turnovers'),
    ('turnovers', 'turnovers'),
    ('team_turnovers', 'team_turnovers'),
    ('field_goals_made', 'field_goals_made'),
    ('free_throws_made', 'free_throws_made'),
    ('three_point_field_goals_made', 'threes_made'),
    ('three_point_field_goals_attempted', 'threes_attempted'),
    ('defensive_rebounds', 'defensive_rebounds'),
    ('total_rebounds', 'total_rebounds'),
    ('assists', 'assists'),
    ('steals', 'steals'),
    ('blocks', 'blocks'),
    ('fouls', 'fouls'),
)

COLUMNS = (
    'game_key', 'team_key', 'opponent_key', 'season', 'season_type',
    'game_date', 'home_away',
) + tuple(stored for _, stored in COUNTS) + (
    'possession_turnovers', 'turnover_basis', 'reconciled')

# A season is read as DUPLICATING its turnover count when more than this
# share of its rows set `team_turnovers` equal to `turnovers`. The measured
# shares are 1.000 for every season 2002-2012 and at most 0.0004 for every
# season after, so the threshold sits between two populations that are not
# close to it, rather than being tuned to land between them.
DUPLICATE_SHARE = 0.5

# Likewise for availability: a season missing its total on more than this
# share of rows is read as not carrying one. Measured: 0.999 for 2017-18 and
# at most 0.0004 for every other season.
MISSING_SHARE = 0.5

TOTAL_BASIS = 'total'
TEAM_ONLY_BASIS = 'team-only'


def _int(value):
    if value is None or value == '' or isinstance(value, bool):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _str(value):
    if value is None:
        return None
    text = ' '.join(str(value).split())
    return text or None


def _franchise(team_id):
    """A franchise's key, or None for an All-Star side or an unknown id."""
    if team_id is None:
        return None
    try:
        return identity.team_key(team_id)
    except ValueError:
        return None


def known_games(connection):
    """`{game key: (season type, home key, away key, home pts, away pts)}`.

    The schedule's own view of every game, which is what decides a stored
    row's season type and whether its points reconcile. See trap 1.
    """
    return {row['game_key']: (row['season_type'], row['home_team_key'],
                              row['away_team_key'], row['home_points'],
                              row['away_points'])
            for row in connection.execute(
                'SELECT game_key, season_type, home_team_key, away_team_key,'
                '       home_points, away_points FROM game')}


def parse(rows, season):
    """`team_box` rows as stored rows, with everything refused counted.

    Returns `(parsed, counts)`. The season type and the reconciliation are
    NOT set here — they need the schedule, which `replace_season` has.
    """
    counts = dict(rows=len(rows), no_game=0, exhibition=0, duplicate=0)
    found = {}
    for row in rows:
        provider_game = _str(row.get('game_id'))
        if not provider_game:
            counts['no_game'] += 1
            continue
        team_key = _franchise(row.get('team_id'))
        if team_key is None:
            # TRAP 2: an All-Star side, not a franchise. Counted, not lost.
            counts['exhibition'] += 1
            continue
        game_key = identity.game_key(provider_game)
        pair = (game_key, team_key)
        if pair in found:
            counts['duplicate'] += 1
            continue
        parsed = dict(
            game_key=game_key,
            team_key=team_key,
            opponent_key=_franchise(row.get('opponent_team_id')),
            season=int(season),
            season_type=None,
            game_date=_str(row.get('game_date')),
            home_away=_str(row.get('team_home_away')),
            reconciled=None,
        )
        for feed_name, stored in COUNTS:
            # TRAP 4: a missing count stays missing.
            parsed[stored] = _int(row.get(feed_name))
        found[pair] = parsed
    return list(found.values()), counts


def turnover_basis(rows):
    """Which turnover count one season's possession estimates may read.

    `'total'` where `total_turnovers` is present and means what it says, and
    `'team-only'` where it is absent or is the team's own count doubled —
    see trap 3. Decided over the season rather than per row, because the
    duplication is a property of the feed's era and an individual row with
    one turnover and one team turnover is indistinguishable from a doubled
    one.

    An empty season yields `'total'`: there is nothing to distrust, and a
    season with no rows stores nothing anyway.
    """
    if not rows:
        return TOTAL_BASIS
    missing = sum(1 for row in rows if row.get('total_turnovers') is None)
    if missing > MISSING_SHARE * len(rows):
        return TEAM_ONLY_BASIS
    duplicated = sum(1 for row in rows
                     if row.get('team_turnovers') is not None
                     and row.get('team_turnovers') == row.get('turnovers'))
    if duplicated > DUPLICATE_SHARE * len(rows):
        return TEAM_ONLY_BASIS
    return TOTAL_BASIS


def possession_turnovers(row, basis):
    """The turnover count this row's possession estimate reads, or None.

    None rather than a fallback where the chosen count is missing: a game
    short of it is unmeasurable, and the one measured 2025-26 case is
    reported as such rather than silently measured off the narrower column.
    """
    if basis == TEAM_ONLY_BASIS:
        return row.get('turnovers')
    return row.get('total_turnovers')


def reconciled(row, schedule):
    """Whether this row's points equal the final the schedule carries.

    None where either side has no number to compare — an unplayed game has
    no final, and nothing disagreed. Stated, never corrected: see the
    schema note.
    """
    _, home_key, away_key, home_points, away_points = schedule
    points = row.get('points')
    if points is None:
        return None
    if row['team_key'] == home_key:
        final = home_points
    elif row['team_key'] == away_key:
        final = away_points
    else:
        # The feed puts this club in a game the schedule says it did not
        # play. Not a reconciliation failure, a contradiction: say so.
        return 0
    if final is None:
        return None
    return int(points == final)


def replace_season(connection, season, rows, *, snapshot_id=None):
    """Store one season's team box scores, replacing whatever was there.

    DELETE then INSERT, because the source revises these files: a row that
    has stopped existing must stop existing here. The season type is taken
    from the schedule for every row (trap 1), and a row for a game the
    schedule does not carry is dropped and counted.
    """
    schedule = known_games(connection)
    basis = turnover_basis(rows)
    kept, unknown = [], 0
    for row in rows:
        entry = schedule.get(row['game_key'])
        if entry is None:
            unknown += 1
            continue
        stored = dict(row)
        # TRAP 1: the schedule's classification wins.
        stored['season_type'] = entry[0]
        # TRAP 3: which turnover count this season's estimates may read.
        stored['turnover_basis'] = basis
        stored['possession_turnovers'] = possession_turnovers(stored, basis)
        stored['reconciled'] = reconciled(stored, entry)
        kept.append(stored)
    connection.execute('DELETE FROM team_game_box WHERE season = ?',
                       (int(season),))
    if kept:
        names = ', '.join(COLUMNS + ('snapshot_id',))
        marks = ', '.join('?' * (len(COLUMNS) + 1))
        connection.executemany(
            f'INSERT INTO team_game_box ({names}) VALUES ({marks})',
            [tuple(row[column] for column in COLUMNS) + (snapshot_id,)
             for row in kept])
    connection.commit()
    paired = _paired_games(connection, season)
    return dict(stored=len(kept), unknown_games=unknown, paired=paired,
                turnover_basis=basis,
                unreconciled=sum(1 for row in kept
                                 if row['reconciled'] == 0))


def _paired_games(connection, season):
    """Games of this season with both team rows stored."""
    return connection.execute(
        'SELECT COUNT(*) FROM (SELECT game_key FROM team_game_box'
        '  WHERE season = ? GROUP BY game_key HAVING COUNT(*) = 2)',
        (int(season),)).fetchone()[0]


def season_rows(connection, season, *, season_types=None):
    """The stored box rows of one season, ready for `team_metrics`.

    Ordered by date then game then team, so a replay built from them is
    reproducible from the archive rather than from the query plan.
    """
    query = ('SELECT * FROM team_game_box WHERE season = ?')
    params = [int(season)]
    if season_types:
        query += ' AND season_type IN (' + ','.join(
            '?' * len(season_types)) + ')'
        params.extend(season_types)
    query += ' ORDER BY game_date, game_key, team_key'
    return [dict(row) for row in connection.execute(query, params)]


def has_team_box(connection):
    """Whether this database carries the team box layer at all.

    An export built before this loader has run must say the layer is absent
    rather than publish a board with every PxP+ missing, which a reader
    cannot tell from a league where nobody scored.
    """
    return connection.execute(
        'SELECT 1 FROM team_game_box LIMIT 1').fetchone() is not None


def coverage(connection):
    """How far the layer reaches, for the docs and the export summary."""
    return dict(
        rows=connection.execute(
            'SELECT COUNT(*) FROM team_game_box').fetchone()[0],
        games=connection.execute(
            'SELECT COUNT(DISTINCT game_key) FROM team_game_box'
        ).fetchone()[0],
        seasons=connection.execute(
            'SELECT COUNT(DISTINCT season) FROM team_game_box'
        ).fetchone()[0],
        unreconciled=connection.execute(
            'SELECT COUNT(*) FROM team_game_box WHERE reconciled = 0'
        ).fetchone()[0])


def ingest_season(connection, season, *, raw_root=None, reuse_cache=False,
                  opener=None, sleep=None, now=None, rows=None):
    """One season: fetch, parse, store, and delete what was fetched."""
    stamp = (now or datetime.now(timezone.utc)).isoformat()
    path = None
    try:
        if rows is None:
            path, origin = source_module.load_asset(
                DATASET, season, root=raw_root, reuse_cache=reuse_cache,
                opener=opener, sleep=sleep)
            try:
                rows = source_module.read_parquet(path)
            except Exception:
                path.unlink(missing_ok=True)
                raise
        else:
            origin = 'given'
        parsed, counts = parse(rows, season)
        snapshot_id = store_module.record_snapshot(
            connection, 'team_box', stamp, season=season, source=origin,
            source_revision=source_module.source_revision(),
            rows_seen=counts['rows'],
            notes=source_module.asset_url(DATASET, season))
        stored = replace_season(connection, int(season), parsed,
                                snapshot_id=snapshot_id)
    finally:
        if path is not None:
            path.unlink(missing_ok=True)
    return dict(season=int(season), **counts, **stored)


def ingest(connection, seasons, *, raw_root=None, reuse_cache=False,
           opener=None, sleep=None, now=None):
    """Every season named, oldest first. A failure stops the run.

    The same rule the schedule ingest follows, and for the same reason:
    carrying on would write a database quietly short of a year, and every
    league rate taken from it afterwards would be wrong without looking
    wrong.
    """
    return [ingest_season(connection, season, raw_root=raw_root,
                          reuse_cache=reuse_cache, opener=opener, sleep=sleep,
                          now=now) for season in seasons]


def main(argv=None):
    parser = argparse.ArgumentParser(
        description='Load the NBA team box scores')
    parser.add_argument('--db', default=None)
    parser.add_argument('--raw-root', default=None)
    parser.add_argument('--seasons', default='',
                        help='e.g. 2026 or 2024-2027; default is all of them')
    parser.add_argument('--reuse-cache', action='store_true')
    args = parser.parse_args(argv)
    from .ingest import parse_seasons
    seasons = parse_seasons(args.seasons)
    connection = store_module.connect(args.db)
    results = ingest(connection, seasons, raw_root=args.raw_root,
                     reuse_cache=args.reuse_cache)
    stored = sum(row['stored'] for row in results)
    paired = sum(row['paired'] for row in results)
    exhibition = sum(row['exhibition'] for row in results)
    unknown = sum(row['unknown_games'] for row in results)
    unreconciled = sum(row['unreconciled'] for row in results)
    print(f'{stored} team box rows across {len(results)} seasons, '
          f'{paired} games with both sides')
    print(f'  dropped: {exhibition} exhibition rows, {unknown} for games the '
          f'schedule does not have')
    print(f'  {unreconciled} rows whose points disagree with the final the '
          f'schedule carries')
    narrow = [row['season'] for row in results
              if row['turnover_basis'] == TEAM_ONLY_BASIS]
    if narrow:
        print(f'  {len(narrow)} seasons read the narrower turnover count '
              f'because the feed\'s total is absent or doubled: '
              f'{", ".join(str(season) for season in narrow)}')
    for row in results:
        print(f'  {row["season"]:>6}  rows {row["stored"]:>5}  '
              f'games {row["paired"]:>5}  '
              f'turnovers {row["turnover_basis"]:>9}  '
              f'[exhibition {row["exhibition"]}, '
              f'unknown game {row["unknown_games"]}, '
              f'unreconciled {row["unreconciled"]}]')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
