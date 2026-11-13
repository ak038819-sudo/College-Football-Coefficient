"""ESPN Analytics Net Points, and the crosswalk that lets us use them.

A SECOND ESPN FEED, IN A THIRD PROVIDER'S IDS. `src/nba/source.py` reads
sportsdataverse's ESPN box scores, keyed by ESPN's own ids, which are what
every key in this archive is built from. This module reads ESPN's net
points -- a possession-level offensive and defensive grade a box score does
not carry -- from llimllib/nba_data, whose every table is keyed by **NBA
ids**: `0022500001` for a game, `1629006` for a player, `1610612745` for a
franchise. So nothing can be stored until all three are resolved, and a
resolution that cannot be verified is quarantined rather than guessed.

What is measured, 2023-24 through 2026-27 (seasons 2024-2027):

| | |
| --- | --- |
| franchises | 30 of 30, every nickname agreeing with ours |
| regular-season games | every one, the ten neutral-site games included |
| players, 2025-26 | 582 of 582, each by a name unique in its season |
| players, 2026-27 | 463 of 464 so far |

**THE TEAM MAP IS WRITTEN DOWN, NOT DERIVED.** Both sides are stable
franchise ids, so the thirty rows below are the whole mapping, taken from
the archive's own `pipeline/team_abbrevs.csv` (`espn_team_id` rows) and
checked against `team.name` here: all 30 nicknames agree. Deriving it from
names at run time would turn a settled fact into a yearly renaming risk --
and NBA abbreviations are not even unique within a season.

**A GAME IS MATCHED ON ITS DATE AND ITS TWO FRANCHISES**, which is unique:
no two NBA games the same day share both. The archive leaves `home_team_id`
NULL for a neutral-site game, so those are matched on the unordered pair
and the side ESPN itself states in the grade rows' `home` flag; over four
seasons that is 11 games and ESPN's own home/away agrees in all of them.

**A PLAYER IS MATCHED ON A NAME UNIQUE WITHIN THE SEASON, THEN CHECKED
AGAINST THE GAMES HE PLAYED.** The name alone would be a guess; the
appearance evidence is what makes it a resolution. A name matching two
people, or none, falls to the appearance fingerprint -- the set of games
played, which is nearly unique -- and anything the fingerprint cannot
settle is quarantined with its reason.

**THE GRADES RECONCILE TO ESPN'S OWN SEASON TOTALS**, exactly on team
possessions and games played, and within 0.0053 of a net point on the
season -- which is the rounding in the stored per-game values. Above 500
offensive possessions the per-100 rate agrees to 0.001 in 2023-24 and
2024-25. **2025-26 IS THE EXCEPTION AND THE COVERAGE ROW SAYS SO**: its
days were saved as they happened and ESPN has since revised some of them,
so 387 of 452 agree and the worst is 0.87 per 100. A replay over it is
reproducible from this archive but is not ESPN's current published season
number.
"""

import argparse
import sys
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from re import sub

from . import source
from .identity import team_key as make_team_key
from .store import connect, record_snapshot

ARCHIVE_ROOT = 'https://basketball-data.billmill.org'

# dataset -> the path under the archive root. Each is one file a season.
DATASETS = {
    # ESPN's net points, one row per player per game.
    'grades': 'nba/espn/player_box',
    # ESPN's own season totals, which the grades are reconciled against.
    'season_totals': 'nba/espn/net_pts_season',
    # The NBA's own game list: date, type and the two franchises.
    'games': 'nba/stats/games',
    # The NBA's own player list: the name each id went by that season.
    'players': 'nba/stats/player_seasons',
}

DEFAULT_RAW_ROOT = Path('data/nba/net_points')

# NBA franchise id -> ESPN team id. See the module docstring.
ESPN_TEAM_OF_NBA = {
    '1610612737': 1,    # Atlanta Hawks
    '1610612738': 2,    # Boston Celtics
    '1610612740': 3,    # New Orleans Pelicans
    '1610612741': 4,    # Chicago Bulls
    '1610612739': 5,    # Cleveland Cavaliers
    '1610612742': 6,    # Dallas Mavericks
    '1610612743': 7,    # Denver Nuggets
    '1610612765': 8,    # Detroit Pistons
    '1610612744': 9,    # Golden State Warriors
    '1610612745': 10,   # Houston Rockets
    '1610612754': 11,   # Indiana Pacers
    '1610612746': 12,   # LA Clippers
    '1610612747': 13,   # Los Angeles Lakers
    '1610612748': 14,   # Miami Heat
    '1610612749': 15,   # Milwaukee Bucks
    '1610612750': 16,   # Minnesota Timberwolves
    '1610612751': 17,   # Brooklyn Nets
    '1610612752': 18,   # New York Knicks
    '1610612753': 19,   # Orlando Magic
    '1610612755': 20,   # Philadelphia 76ers
    '1610612756': 21,   # Phoenix Suns
    '1610612757': 22,   # Portland Trail Blazers
    '1610612758': 23,   # Sacramento Kings
    '1610612759': 24,   # San Antonio Spurs
    '1610612760': 25,   # Oklahoma City Thunder
    '1610612762': 26,   # Utah Jazz
    '1610612764': 27,   # Washington Wizards
    '1610612761': 28,   # Toronto Raptors
    '1610612763': 29,   # Memphis Grizzlies
    '1610612766': 30,   # Charlotte Hornets
}

# The archive's own classification, decoded from the NBA game id. The Cup
# final has its own value, which is why this is read rather than inferred:
# it counts for neither side of the regular season.
REGULAR_SEASON = 'regular_season'

# The first season ESPN publishes these grades for. The refresh names no
# seasons, so this and the archive's own last season are what it imports.
FIRST_SEASON = 2024

# A fingerprint has to cover almost all of a player's games to stand in for
# a name. Measured over four seasons, a real match scores above 0.95 and
# the nearest wrong one below 0.4.
FINGERPRINT_FLOOR = 0.9

COLUMNS = ('person_key', 'game_key', 'season', 'team_key', 'source_player_id',
           'source_game_id', 'source_team_id', 'home', 'started', 'played',
           'seconds_played', 'offense_net_points', 'defense_net_points',
           'offense_team_possessions', 'defense_team_possessions',
           'resolved_by')


class NetPointsError(Exception):
    """The archive answered with something this module will not store."""


def asset_url(dataset, season):
    """The archive file for one dataset and season."""
    if dataset not in DATASETS:
        raise NetPointsError(f'Unknown dataset {dataset!r}')
    return (f'{ARCHIVE_ROOT}/{DATASETS[dataset]}'
            f'/season={int(season)}/data.parquet')


def cache_path(dataset, season, root=None):
    """Where a fetched file is kept. Resolved inside the function."""
    base = Path(root) if root is not None else DEFAULT_RAW_ROOT
    return base / dataset / f'{int(season)}.parquet'


def load_asset(dataset, season, *, root=None, reuse_cache=False, opener=None,
               sleep=None):
    """One dataset and season on disk, fetched unless the cache may be used.

    `reuse_cache` defaults to False for the reason `source.load_asset`
    gives: the current season's file is rewritten every day, and preferring
    the cache would freeze it under a fresh as-of stamp.
    """
    path = cache_path(dataset, season, root)
    if reuse_cache and path.exists():
        return path, 'cache'
    body = source.fetch_asset(asset_url(dataset, season), opener=opener,
                              sleep=sleep)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)
    return path, 'network'


def read(path):
    """One file as a list of dicts."""
    return source.read_parquet(path)


def normalised(name):
    """A name reduced to what two providers can be expected to agree on.

    Accents are folded, because the NBA writes `Luka Doncic` and ESPN
    writes `Luka Doncic` with them; suffixes go, because one provider's
    `Jr.` is another's nothing; everything else that is not a letter goes
    with them, so `Gilgeous-Alexander` and `Gilgeous Alexander` are one
    name.
    """
    if not name:
        return ''
    folded = unicodedata.normalize('NFKD', str(name))
    folded = folded.encode('ascii', 'ignore').decode('ascii').lower()
    folded = sub(r'\b(jr|sr|ii|iii|iv|v)\b', '', folded)
    return sub(r'[^a-z]', '', folded)


def _number(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return None if number != number else number


def _text(value):
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def archive_games(connection, season):
    """Our own games of a season, by date and by the pair of teams.

    Returns (by_side, by_pair): `(date, home, away) -> game` and
    `(date, frozenset(teams)) -> game`, each game a dict of what the
    crosswalk and the ingest need.
    """
    by_side, by_pair = {}, {}
    rows = connection.execute(
        'SELECT game_key, provider_id, game_date, home_team_id, '
        '       away_team_id, completed, season_type '
        'FROM game WHERE season = ?', (int(season),))
    for key, provider, date, home, away, completed, kind in rows:
        game = {'game_key': key, 'provider_id': str(provider),
                'date': str(date)[:10], 'home_team_id': home,
                'away_team_id': away, 'completed': bool(completed),
                'season_type': kind}
        by_side[(game['date'], home, away)] = game
        by_pair[(game['date'], frozenset((home, away)))] = game
    return by_side, by_pair


def sides_of(grades):
    """Which franchise each grade row calls home, per source game.

    ESPN's own `home` flag is the only statement of sides for a game the
    NBA lists at a neutral site with no home team.
    """
    sides = defaultdict(lambda: {True: set(), False: set()})
    for row in grades:
        game = _text(row.get('game_id'))
        team = _text(row.get('team_id'))
        if game and team:
            sides[game][bool(row.get('home'))].add(team)
    return sides


def crosswalk_games(games, grades, by_side, by_pair):
    """source game id -> our game, for the regular season only.

    Returns (resolved, unresolved). `unresolved` names every regular-season
    game of the archive that this did not place, with the reason.
    """
    sides = sides_of(grades)
    resolved, unresolved = {}, []
    for row in games:
        if _text(row.get('game_type')) != REGULAR_SEASON:
            continue
        source_id = _text(row.get('game_id'))
        if source_id is None:
            continue
        date = str(row.get('game_date'))[:10]
        home = ESPN_TEAM_OF_NBA.get(_text(row.get('home_team_id')) or '')
        away = ESPN_TEAM_OF_NBA.get(_text(row.get('away_team_id')) or '')
        if home and away:
            game = by_side.get((date, home, away))
            if game is None:
                unresolved.append((source_id, 'no game of ours on that date '
                                              'between those teams'))
                continue
            resolved[source_id] = game
            continue
        # A neutral site: the NBA lists neither team as home.
        stated = sides.get(source_id)
        if not stated:
            unresolved.append((source_id, 'a neutral site with no grades to '
                                          'say which side is which'))
            continue
        hosts = {ESPN_TEAM_OF_NBA.get(one) for one in stated[True]}
        guests = {ESPN_TEAM_OF_NBA.get(one) for one in stated[False]}
        if len(hosts) != 1 or len(guests) != 1 or None in hosts | guests:
            unresolved.append((source_id, 'a neutral site whose sides the '
                                          'grades do not agree on'))
            continue
        game = by_pair.get((date, frozenset(hosts | guests)))
        if game is None:
            unresolved.append((source_id, 'no game of ours on that date '
                                          'between those teams'))
            continue
        resolved[source_id] = game
    return resolved, unresolved


def appearances(grades, resolved):
    """The games each source player played, by our own game key."""
    played = defaultdict(set)
    for row in grades:
        if not row.get('played'):
            continue
        game = resolved.get(_text(row.get('game_id')))
        player = _text(row.get('player_id'))
        if game and player:
            played[player].add(game['game_key'])
    return played


def our_appearances(connection, season):
    """The games each of our own people played in a season."""
    played = defaultdict(set)
    rows = connection.execute(
        'SELECT person_key, game_key FROM person_game '
        'WHERE season = ? AND played = 1', (int(season),))
    for person, game in rows:
        played[person].add(game)
    return played


def our_names(connection):
    """Normalised name -> the people who go by it."""
    people = defaultdict(list)
    for key, full, display in connection.execute(
            'SELECT person_key, full_name, display_name FROM person'):
        name = normalised(full) or normalised(display)
        if name:
            people[name].append(key)
    return people


def _overlap(one, other):
    if not one or not other:
        return 0.0
    return len(one & other) / len(one | other)


def crosswalk_people(players, played, people_by_name, our_played):
    """source player id -> (our person key, how it was resolved).

    Returns (resolved, unresolved). A name unique in the season settles it;
    otherwise the games played do, or nothing does and the row is named.
    """
    named = {}
    for row in players:
        player = _text(row.get('player_id'))
        if player:
            named[player] = _text(row.get('name'))
    resolved, unresolved = {}, []

    def by_fingerprint(games):
        """(person, score, tied) for the closest of our people by games.

        **A TIE IS NOT A MATCH.** Two of our people can have the very same
        appearance set -- two men who have played only the season's opening
        night, which is every October -- and taking the first of them would
        file one man's grades under another, silently, or collide on the
        primary key where the other id resolved correctly. A tie at the
        top is quarantined with its reason instead.
        """
        best, score, tied = None, 0.0, 0
        for person, theirs in sorted(our_played.items()):
            overlap = _overlap(games, theirs)
            if overlap > score:
                best, score, tied = person, overlap, 1
            elif overlap == score and score > 0:
                tied += 1
        if score < FINGERPRINT_FLOOR:
            return None, score, tied
        return best, score, tied

    for player, games in sorted(played.items()):
        candidates = people_by_name.get(normalised(named.get(player)), [])
        if len(candidates) == 1:
            resolved[player] = (candidates[0], 'name')
            continue
        if len(candidates) > 1:
            scored = sorted(
                ((_overlap(games, our_played.get(one, set())), one)
                 for one in candidates), reverse=True)
            best, top = scored[0][1], scored[0][0]
            ties = sum(1 for score, _ in scored if score == top)
            if top >= FINGERPRINT_FLOOR and ties == 1:
                resolved[player] = (best, 'name and the games played')
                continue
            unresolved.append((player, named.get(player),
                               'the name belongs to more than one of our '
                               'people and the games played settle none'))
            continue
        person, score, tied = by_fingerprint(games)
        if person is not None and tied == 1:
            resolved[player] = (person, 'the games played')
            continue
        if person is not None:
            unresolved.append((player, named.get(player),
                               'no person of ours goes by that name, and '
                               '%d of them played the very same games'
                               % tied))
            continue
        unresolved.append((player, named.get(player),
                           'no person of ours goes by that name, and the '
                           'closest by games played overlaps %.2f'
                           % score))
    return resolved, unresolved


def rows_for(season, grades, resolved_games, resolved_people):
    """The grade rows, in storable shape, with everything resolved.

    A row is kept only where the game and the player both resolved. The
    source ids are stored beside ours, because a crosswalk nobody can audit
    after the fact is a crosswalk nobody should trust.
    """
    kept, dropped = [], Counter()
    for row in grades:
        source_game = _text(row.get('game_id'))
        source_player = _text(row.get('player_id'))
        source_team = _text(row.get('team_id'))
        game = resolved_games.get(source_game)
        if game is None:
            dropped['game not in the regular season or not resolved'] += 1
            continue
        person = resolved_people.get(source_player)
        if person is None:
            dropped['player not resolved'] += 1
            continue
        espn_team = ESPN_TEAM_OF_NBA.get(source_team or '')
        kept.append({
            'person_key': person[0],
            'game_key': game['game_key'],
            'season': int(season),
            'team_key': make_team_key(espn_team) if espn_team else None,
            'source_player_id': source_player,
            'source_game_id': source_game,
            'source_team_id': source_team,
            'home': 1 if row.get('home') else 0,
            'started': 1 if row.get('starter') else 0,
            'played': 1 if row.get('played') else 0,
            'seconds_played': int(_number(row.get('seconds_played')) or 0),
            'offense_net_points': _number(row.get('o_net_pts')),
            'defense_net_points': _number(row.get('d_net_pts')),
            'offense_team_possessions': _number(row.get('o_team_poss')),
            'defense_team_possessions': _number(row.get('d_team_poss')),
            'resolved_by': person[1],
        })
    return kept, dict(dropped)


def gradeable(row):
    """Whether a stored row is a grade a rating may read.

    A did-not-play is not a grade. Neither is a row with no possessions on
    one side of it: the rate it would carry divides by zero, and a player
    who was on the floor for none of his team's offensive possessions has
    told us nothing about its offence. **A ZERO NET POINTS IS A GRADE** --
    it is the average performance, not a missing one.
    """
    if not row.get('played'):
        return False
    offense = _number(row.get('offense_net_points'))
    defense = _number(row.get('defense_net_points'))
    if offense is None or defense is None:
        return False
    for possessions in (_number(row.get('offense_team_possessions')),
                        _number(row.get('defense_team_possessions'))):
        if possessions is None or possessions <= 0:
            return False
    return True


def replace_season(connection, season, rows, unresolved, *, snapshot_id=None):
    """Store one season's grades, replacing whatever was there.

    DELETE then INSERT, as the team box does and for the same reason: the
    archive rewrites the current season's file every day and recomputes
    past ones, so a row that has stopped existing must stop existing here.
    """
    season = int(season)
    connection.execute('DELETE FROM player_net_points WHERE season = ?',
                       (season,))
    connection.execute(
        'DELETE FROM player_net_points_unresolved WHERE season = ?',
        (season,))
    if rows:
        names = ', '.join(COLUMNS + ('snapshot_id',))
        marks = ', '.join('?' * (len(COLUMNS) + 1))
        connection.executemany(
            f'INSERT INTO player_net_points ({names}) VALUES ({marks})',
            [tuple(row[column] for column in COLUMNS) + (snapshot_id,)
             for row in rows])
    if unresolved:
        connection.executemany(
            'INSERT OR REPLACE INTO player_net_points_unresolved '
            '(season, kind, source_id, label, reason, snapshot_id) '
            'VALUES (?, ?, ?, ?, ?, ?)',
            [(season, kind, source_id, label, reason, snapshot_id)
             for kind, source_id, label, reason in unresolved])
    connection.commit()
    return dict(stored=len(rows),
                gradeable=sum(1 for row in rows if gradeable(row)),
                quarantined=len(unresolved),
                people=len({row['person_key'] for row in rows}),
                games=len({row['game_key'] for row in rows}))


def season_rows(connection, season, *, gradeable_only=True):
    """One season's stored grades, oldest game first.

    Ordered by date, then tip-off, then game key, which is the order the
    replay has to walk and the only order that makes it deterministic.
    """
    names = ', '.join('n.' + column for column in COLUMNS)
    rows = connection.execute(
        f'SELECT {names}, g.game_date, g.tipoff_utc, g.completed '
        'FROM player_net_points n JOIN game g ON g.game_key = n.game_key '
        'WHERE n.season = ? '
        'ORDER BY g.game_date, COALESCE(g.tipoff_utc, \'\'), n.game_key, '
        '         n.person_key', (int(season),))
    stored = []
    for row in rows:
        entry = dict(zip(COLUMNS + ('game_date', 'tipoff_utc', 'completed'),
                         row))
        if gradeable_only and not gradeable(entry):
            continue
        stored.append(entry)
    return stored


def has_net_points(connection, season=None):
    """Whether any grade is stored, for a season or at all."""
    if season is None:
        return bool(connection.execute(
            'SELECT 1 FROM player_net_points LIMIT 1').fetchone())
    return bool(connection.execute(
        'SELECT 1 FROM player_net_points WHERE season = ? LIMIT 1',
        (int(season),)).fetchone())


def coverage(connection, season):
    """What a season's ingest is worth, for the refresh to report."""
    row = connection.execute(
        'SELECT COUNT(*), COUNT(DISTINCT person_key), '
        '       COUNT(DISTINCT game_key), SUM(played) '
        'FROM player_net_points WHERE season = ?',
        (int(season),)).fetchone()
    quarantined = connection.execute(
        'SELECT kind, COUNT(*) FROM player_net_points_unresolved '
        'WHERE season = ? GROUP BY kind', (int(season),)).fetchall()
    final = connection.execute(
        'SELECT COUNT(*) FROM game WHERE season = ? AND completed = 1 '
        "AND season_type = 'regular' AND rateable = 1", (int(season),)
    ).fetchone()[0]
    return {'season': int(season), 'rows': row[0], 'people': row[1],
            'games': row[2], 'played': row[3] or 0,
            'final_games': final,
            'games_without_grades': max(0, final - (row[2] or 0)),
            'quarantined': {kind: count for kind, count in quarantined}}


def ingest_season(connection, season, *, raw_root=None, reuse_cache=False,
                  opener=None, sleep=None, now=None, given=None):
    """One season: fetch the three files, resolve, store, and clean up.

    A file this run FETCHED is deleted afterwards, as the play-by-play
    reduction does: nothing downstream reads it again. A file that was
    already cached is left alone, because deleting what the caller asked to
    reuse would make `--reuse-cache` mean its opposite on the second run.
    """
    stamp = (now or datetime.now(timezone.utc)).isoformat()
    paths, read_rows, origin = [], {}, 'given'
    try:
        if given is not None:
            read_rows = dict(given)
        else:
            for dataset in ('grades', 'games', 'players'):
                path, origin = load_asset(dataset, season, root=raw_root,
                                          reuse_cache=reuse_cache,
                                          opener=opener, sleep=sleep)
                if origin == 'network':
                    paths.append(path)
                read_rows[dataset] = read(path)
        by_side, by_pair = archive_games(connection, season)
        games, game_notes = crosswalk_games(
            read_rows['games'], read_rows['grades'], by_side, by_pair)
        played = appearances(read_rows['grades'], games)
        people, people_notes = crosswalk_people(
            read_rows['players'], played, our_names(connection),
            our_appearances(connection, season))
        rows, dropped = rows_for(season, read_rows['grades'], games, people)
        unresolved = [('game', one, None, why) for one, why in game_notes]
        unresolved += [('player', one, label, why)
                       for one, label, why in people_notes]
        snapshot_id = record_snapshot(
            connection, 'net_points', stamp, season=season, source=origin,
            source_revision=ARCHIVE_ROOT,
            rows_seen=len(read_rows['grades']),
            notes=asset_url('grades', season))
        stored = replace_season(connection, season, rows, unresolved,
                                snapshot_id=snapshot_id)
    finally:
        for path in paths:
            path.unlink(missing_ok=True)
    return dict(season=int(season), seen=len(read_rows.get('grades', ())),
                resolved_games=len(games), dropped=dropped, **stored)


def archive_seasons(connection, first=FIRST_SEASON):
    """The seasons to import when nobody names any: every season of the
    schedule from the first ESPN publishes grades for.

    The refresh runs this step with no `--seasons`, so a CLI that demanded
    them would stop the nightly run before the replay and the publish.
    """
    rows = connection.execute(
        'SELECT DISTINCT season FROM game WHERE season >= ? ORDER BY season',
        (int(first),))
    return [int(one[0]) for one in rows]


def ingest(connection, seasons, **kwargs):
    """Every season named, oldest first. A failure stops the run."""
    return [ingest_season(connection, season, **kwargs)
            for season in seasons]


def summarise(reports):
    lines = []
    for report in reports:
        lines.append(
            '    %d  rows %6d  gradeable %6d  people %4d  games %5d  '
            'quarantined %d'
            % (report['season'], report['stored'], report['gradeable'],
               report['people'], report['games'], report['quarantined']))
        for reason, count in sorted(report['dropped'].items()):
            lines.append('        dropped %d: %s' % (count, reason))
    return '\n'.join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', default=None)
    parser.add_argument('--raw-root', default=None)
    parser.add_argument('--seasons', default='',
                        help='comma-separated, e.g. 2024,2025,2026,2027; '
                             'every season of the schedule from %d by '
                             'default' % FIRST_SEASON)
    parser.add_argument('--reuse-cache', action='store_true')
    args = parser.parse_args(argv)
    named = [int(one) for one in args.seasons.replace(' ', '').split(',')
             if one]
    connection = connect(args.db)
    seasons = named or archive_seasons(connection)
    if not seasons:
        print('No season of the schedule is at or after %d, so there is '
              'nothing to import.' % FIRST_SEASON)
        connection.close()
        return 1
    try:
        reports = ingest(connection, seasons, raw_root=args.raw_root,
                         reuse_cache=args.reuse_cache)
    finally:
        connection.close()
    print('%d seasons, %d grades stored'
          % (len(reports), sum(one['stored'] for one in reports)))
    print(summarise(reports))
    return 0


if __name__ == '__main__':
    sys.exit(main())
