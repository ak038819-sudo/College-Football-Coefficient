"""Snap counts and the weekly injury report: how much a player was on the
field, and whether he was fit to be.

Two more release files of `nflverse/nflverse-data`, both small, both free,
both reduced to one row per player per game or per player per week.

| what              | asset                                  | seasons   |
| ----------------- | -------------------------------------- | --------- |
| snap counts       | `snap_counts/snap_counts_<season>.csv` | 2013-2026 |
| the injury report | `injuries/injuries_<season>.csv`       | 2009-2026 |

**SNAP COUNTS BEGIN IN 2013, NOT 2012.** The release has a
`snap_counts_2012.csv` and it holds a header and nothing else. Published and
empty is not the same as absent, and neither is a failure: `parse_snaps` on
an empty file returns no rows and says so, and only a season that should have
data and has none is a problem a person needs to see.

**THE SNAP FILE HAS NO GSIS ID.** Its person id is Pro-Football-Reference's
(`BrowSp00`), so a snap row reaches a player only through `player.pfr_id`,
which the player file gives for 91.3% of players and which resolves 99.93% of
snap rows (333,129 of 333,375). The other 246, across 29 people, are dropped:
a snap count with nobody to attribute it to is a number with no page to show
it on.

**THE INJURY REPORT IS NOT A GAME.** It is filed before one, and one of the
weeks here never produced a game at all: Buffalo and Cincinnati both filed for
week 17 of 2022, the game was abandoned after Damar Hamlin's cardiac arrest,
and those 17 rows are the only 17 of 92,890 that reach no game in the
archive. So a report is keyed on its week and `game_key` is allowed to be
NULL, rather than the report being thrown away for the game's absence.

**AND THE SOURCE REPEATS A PLAYER WHEN A CLUB UPDATES ITS REPORT.** Two
players in week 15 of 2024 appear twice, Questionable on the Saturday and Out
on the Sunday morning. The LATEST `date_modified` wins: the last thing the
club said before kickoff is what the report said.
"""

import argparse
import sys

from .identity import people_franchise_key
from .store import connect, record_snapshot

# The source's game-type codes, in this archive's words. `game.season_type`
# uses the long form, so the short one cannot be stored as it arrives.
SEASON_TYPES = {
    'REG': 'regular',
    'WC': 'wildcard',
    'DIV': 'divisional',
    'CON': 'conference',
    'SB': 'superbowl',
}

SNAP_COLUMNS = (
    'player_key', 'game_key', 'team_key', 'position', 'offense_snaps',
    'offense_pct', 'defense_snaps', 'defense_pct', 'st_snaps', 'st_pct',
)
INJURY_COLUMNS = (
    'season', 'season_type', 'week', 'team_key', 'player_key', 'game_key',
    'position', 'report_primary', 'report_secondary', 'report_status',
    'practice_primary', 'practice_secondary', 'practice_status',
    'reported_at',
)

# The injury file carries a handful of values that are whitespace and nothing
# else — 215 practice lines are a newline and four spaces. `_str` already
# strips, so they arrive here as None; this names the case so the next reader
# does not go looking for a missing status.
BLANK_PRACTICE = '\n    '


def _int(value):
    text = (value or '').strip() if isinstance(value, str) else value
    if text in (None, ''):
        return None
    try:
        number = float(text)
    except (TypeError, ValueError):
        return None
    return int(number) if number == int(number) else None


def _real(value):
    text = (value or '').strip() if isinstance(value, str) else value
    if text in (None, ''):
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _str(value):
    text = (value or '').strip() if isinstance(value, str) else value
    return text or None


def pfr_index(connection):
    """Pro-Football-Reference id -> player key, for the snap join."""
    return {row['pfr_id']: row['player_key'] for row in
            connection.execute('SELECT pfr_id, player_key FROM player '
                               'WHERE pfr_id IS NOT NULL')}


def gsis_index(connection):
    return {row['gsis_id']: row['player_key'] for row in
            connection.execute('SELECT gsis_id, player_key FROM player')}


def game_index(connection):
    """The two ways these files name a game.

    `by_provider` is the snap file's `game_id` (`2024_01_ARI_BUF`), which is
    this archive's `game.provider_id`, not its key. `by_week` is how the
    injury file names one: a season, a round, a week and ONE of the teams.
    """
    by_provider, by_week = {}, {}
    for row in connection.execute(
            'SELECT game_key, provider_id, season, season_type, week, '
            'home_team_key, away_team_key FROM game'):
        by_provider[row['provider_id']] = row['game_key']
        for side in ('home_team_key', 'away_team_key'):
            by_week[(row['season'], row['season_type'], row['week'],
                     row[side])] = row['game_key']
    return by_provider, by_week


def parse_snaps(rows, *, players, games):
    """Snap rows, resolved. Unresolvable people and games are counted.

    `players` is a pfr id index and `games` a provider-id index, both as the
    functions above build them.
    """
    out, skipped = [], dict(no_player=0, no_game=0, duplicate=0)
    seen = set()
    for row in rows:
        player = players.get(_str(row.get('pfr_player_id')))
        if player is None:
            skipped['no_player'] += 1
            continue
        game = games.get(_str(row.get('game_id')))
        if game is None:
            skipped['no_game'] += 1
            continue
        if (player, game) in seen:
            # One player, one game, one row. A repeat would otherwise be an
            # INSERT conflict rather than a fact about the data.
            skipped['duplicate'] += 1
            continue
        seen.add((player, game))
        code = _str(row.get('team'))
        out.append(dict(
            player_key=player,
            game_key=game,
            team_key=people_franchise_key(code) if code else None,
            position=_str(row.get('position')),
            offense_snaps=_int(row.get('offense_snaps')),
            offense_pct=_real(row.get('offense_pct')),
            defense_snaps=_int(row.get('defense_snaps')),
            defense_pct=_real(row.get('defense_pct')),
            st_snaps=_int(row.get('st_snaps')),
            st_pct=_real(row.get('st_pct')),
        ))
    return out, skipped


def parse_injuries(rows, *, players, games):
    """Injury-report rows, one per player per week, latest report wins.

    `games` is the by-week index; a row that reaches no game keeps a NULL
    `game_key` rather than being dropped.
    """
    out, skipped = {}, dict(no_player=0, no_week=0, superseded=0)
    for row in rows:
        player = players.get(_str(row.get('gsis_id')))
        if player is None:
            skipped['no_player'] += 1
            continue
        season = _int(row.get('season'))
        week = _int(row.get('week'))
        season_type = SEASON_TYPES.get(_str(row.get('game_type')))
        code = _str(row.get('team'))
        if season is None or week is None or season_type is None or not code:
            skipped['no_week'] += 1
            continue
        team = people_franchise_key(code)
        key = (season, season_type, week, team, player)
        reported = _str(row.get('date_modified'))
        if key in out:
            # Both rows are real; the later one is the report as it stood.
            # Comparing ISO-8601 strings is comparing the instants.
            skipped['superseded'] += 1
            if (out[key]['reported_at'] or '') >= (reported or ''):
                continue
        out[key] = dict(
            season=season, season_type=season_type, week=week,
            team_key=team, player_key=player,
            game_key=games.get((season, season_type, week, team)),
            position=_str(row.get('position')),
            report_primary=_str(row.get('report_primary_injury')),
            report_secondary=_str(row.get('report_secondary_injury')),
            report_status=_str(row.get('report_status')),
            practice_primary=_str(row.get('practice_primary_injury')),
            practice_secondary=_str(row.get('practice_secondary_injury')),
            practice_status=_str(row.get('practice_status')),
            reported_at=reported,
        )
    return list(out.values()), skipped


def _replace(connection, table, columns, where, params, rows, *,
             snapshot_id=None):
    """Delete a season's rows and insert the ones just parsed.

    DELETE then INSERT rather than an upsert: these files are rewritten in
    place by the source, and a player dropped from a report should disappear
    from it here too, which an upsert cannot do.
    """
    connection.execute(f'DELETE FROM {table} WHERE {where}', params)
    names = columns + ('snapshot_id',)
    marks = ', '.join('?' for _ in names)
    connection.executemany(
        f'INSERT INTO {table} ({", ".join(names)}) VALUES ({marks})',
        [tuple(row[name] for name in columns) + (snapshot_id,)
         for row in rows])
    connection.commit()
    return len(rows)


def replace_snaps(connection, season, rows, *, snapshot_id=None):
    return _replace(
        connection, 'player_snap', SNAP_COLUMNS,
        'game_key IN (SELECT game_key FROM game WHERE season = ?)',
        (season,), rows, snapshot_id=snapshot_id)


def replace_injuries(connection, season, rows, *, snapshot_id=None):
    return _replace(connection, 'injury_report', INJURY_COLUMNS,
                    'season = ?', (season,), rows, snapshot_id=snapshot_id)


def ingest(connection, bundle, *, snapshot_id=None):
    """Load a `{season: rows}` bundle of snaps and injuries."""
    players_pfr = pfr_index(connection)
    players_gsis = gsis_index(connection)
    by_provider, by_week = game_index(connection)
    counts = dict(snaps=0, injuries=0)
    skipped = dict(snaps={}, injuries={})
    for season in sorted(bundle.get('snaps', {})):
        rows, drops = parse_snaps(bundle['snaps'][season],
                                  players=players_pfr, games=by_provider)
        counts['snaps'] += replace_snaps(connection, season, rows,
                                         snapshot_id=snapshot_id)
        skipped['snaps'][season] = drops
    for season in sorted(bundle.get('injuries', {})):
        rows, drops = parse_injuries(bundle['injuries'][season],
                                     players=players_gsis, games=by_week)
        counts['injuries'] += replace_injuries(connection, season, rows,
                                               snapshot_id=snapshot_id)
        skipped['injuries'][season] = drops
    return dict(counts=counts, skipped=skipped)


def coverage(connection):
    """What is loaded, by season, for the gate and the console."""
    snaps = {row['season']: row['rows'] for row in connection.execute(
        'SELECT g.season AS season, COUNT(*) AS rows FROM player_snap s '
        'JOIN game g ON g.game_key = s.game_key GROUP BY g.season')}
    injuries = {row['season']: row['rows'] for row in connection.execute(
        'SELECT season, COUNT(*) AS rows FROM injury_report '
        'GROUP BY season')}
    unplaced = connection.execute(
        'SELECT COUNT(*) AS rows FROM injury_report '
        'WHERE game_key IS NULL').fetchone()['rows']
    return dict(snaps=snaps, injuries=injuries, injuries_without_game=unplaced)


def main(argv=None):
    from . import people_source

    parser = argparse.ArgumentParser(
        description='Load snap counts and injury reports into the NFL database')
    parser.add_argument('--database', default=None)
    parser.add_argument('--raw-root', default=None)
    parser.add_argument('--seasons', default=None,
                        help='comma-separated seasons; every season by default')
    parser.add_argument('--reuse-cache', action='store_true')
    args = parser.parse_args(argv)
    seasons = ([int(part) for part in args.seasons.split(',') if part.strip()]
               if args.seasons else None)
    bundle, origins = people_source.load_usage(
        seasons=seasons, root=args.raw_root, reuse_cache=args.reuse_cache)
    connection = connect(args.database)
    snapshot_id = record_snapshot(
        connection, source='nflverse', origin='usage',
        detail=f'{len(bundle["snaps"])} snap seasons, '
               f'{len(bundle["injuries"])} injury seasons')
    result = ingest(connection, bundle, snapshot_id=snapshot_id)
    print(f'{result["counts"]["snaps"]} snap rows, '
          f'{result["counts"]["injuries"]} injury rows')
    absent = sorted(name for name, origin in origins.items()
                    if origin == 'absent')
    if absent:
        print(f'  not published yet: {", ".join(absent)}')
    for kind in ('snaps', 'injuries'):
        dropped = {season: drops for season, drops
                   in result['skipped'][kind].items() if any(drops.values())}
        if dropped:
            print(f'  {kind} dropped: {dropped}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
