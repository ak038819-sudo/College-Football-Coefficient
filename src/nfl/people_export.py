"""The people layer as static files: an index, career shards, the season in
progress, season leaders and the draft.

**THE SPLIT BETWEEN SETTLED AND IN-PROGRESS SEASONS IS THE POINT OF THIS
MODULE.** A player page wants one file, so careers are sharded by player —
64 shards, the rule the college side settled on — and a season in progress
changes some players in EVERY shard every week. Writing the current season
into the shards would rewrite all 64 of them on each refresh that saw a game,
which is megabytes of git history a week for numbers that will all be
rewritten again the week after.

So the shards hold settled seasons only, and the season in progress goes in
one file of its own, `people/current.js`. A refresh during the season rewrites
that one file; the shards change once a year, when a season settles, or when
the source revises history. The boundary is `identity.current_season`, the
same one the coverage rows use to mark a season complete, so the two can
never disagree about which season is still being played.

Statistic NAMES are interned per file: each shard carries the list of names it
uses once and every value refers to it by position. 711,914 values each
repeating a name like `def_tackles_with_assist` would be most of the bytes.
"""

import json
import zlib
from collections import defaultdict

from .identity import current_season
from .people import MAX_STATS, MEAN_STATS

PLAYER_SHARDS = 64
PREFIX = 'nfl:nflverse:player:'
FRANCHISE_PREFIX = 'nfl:nflverse:franchise:'

# The regular-season leaderboards a season view shows. Ten deep, because a
# leaderboard is for finding the top of a category, and the player page is
# where the rest of a career lives.
LEADER_STATS = (
    'passing_yards', 'passing_tds', 'rushing_yards', 'rushing_tds',
    'receptions', 'receiving_yards', 'receiving_tds', 'def_sacks',
    'def_interceptions', 'def_tackles_solo', 'fg_made',
)
LEADERS_DEEP = 10

# The statistics a player page SHOWS, by table, in the order it shows them.
#
# This is the export's allowlist, not the database's: every non-zero value the
# source carries is stored (see `people.parse_stats`), and only these reach
# the browser. Exporting all 140 would put fumble-recovery yardage and the
# fifteen field-goal distance buckets into every page load for nothing on the
# page to read, and the careers came to 11.2 MB that way.
#
# The order is the page's order, never alphabetical: the college side's
# player tables were alphabetical by stat name for a season before anybody
# noticed that "attempts" sat between "air yards" and "completions".
STAT_TABLES = (
    ('passing', 'Passing', (
        'completions', 'attempts', 'passing_yards', 'passing_tds',
        'passing_interceptions', 'sacks_suffered', 'sack_yards_lost',
        'passing_first_downs', 'passing_epa', 'passing_cpoe')),
    ('rushing', 'Rushing', (
        'carries', 'rushing_yards', 'rushing_tds', 'rushing_first_downs',
        'rushing_fumbles_lost', 'rushing_epa')),
    ('receiving', 'Receiving', (
        'targets', 'receptions', 'receiving_yards', 'receiving_tds',
        'receiving_yards_after_catch', 'receiving_first_downs',
        'receiving_epa', 'target_share')),
    ('defense', 'Defense', (
        'def_tackles_solo', 'def_tackle_assists', 'def_tackles_for_loss',
        'def_sacks', 'def_qb_hits', 'def_interceptions',
        'def_interception_yards', 'def_pass_defended', 'def_fumbles_forced',
        'fumble_recovery_opp', 'def_tds', 'def_safeties')),
    ('kicking', 'Kicking', (
        'fg_made', 'fg_att', 'fg_long', 'fg_made_40_49', 'fg_made_50_59',
        'fg_made_60_', 'pat_made', 'pat_att')),
    ('punting', 'Punting', (
        'pt_att', 'pt_yards', 'pt_net_yards', 'pt_inside_20', 'pt_long')),
    ('returns', 'Returns', (
        'kickoff_returns', 'kickoff_return_yards', 'punt_returns',
        'punt_return_yards', 'special_teams_tds')),
    ('fantasy', 'Fantasy', ('fantasy_points_ppr',)),
)
EXPORTED_STATS = frozenset(stat for _, _, stats in STAT_TABLES
                           for stat in stats)


def shard_of(gsis_id):
    """Which career shard a player lives in.

    crc32 rather than Python's `hash()`, which is salted per process and would
    move every player to a different shard on every run. The browser never
    computes this: the index carries each player's shard, so there is no
    second implementation of the rule to drift.
    """
    return zlib.crc32(gsis_id.encode('utf-8')) % PLAYER_SHARDS


def _code(team_key):
    return team_key[len(FRANCHISE_PREFIX):] if team_key else None


def _gsis(player_key):
    return player_key[len(PREFIX):]


def _number(value):
    """A stored value as the export writes it: integers stay integers."""
    if value is None:
        return None
    if float(value).is_integer():
        return int(value)
    # Three places is finer than any rate here is measured to (a target share
    # of 0.2345 is a fraction of one target) and halves the bytes of EPA.
    return round(value, 3)


def players(connection):
    """Every player who has a page: anybody with a roster or statistics row.

    18,109 player rows are stored; 16,608 of them have a row of either kind.
    The other 1,501 are in the player file with a career that reaches 1999 and
    nothing else this archive can show, and a page with a biography and no
    season on it is not worth an index entry.
    """
    rows = connection.execute(
        'SELECT p.* FROM player p'
        ' WHERE EXISTS (SELECT 1 FROM player_season s'
        '                WHERE s.player_key = p.player_key)'
        '    OR EXISTS (SELECT 1 FROM player_stat_season s'
        '                WHERE s.player_key = p.player_key)'
        ' ORDER BY p.player_key').fetchall()
    return [dict(row) for row in rows]


def _bio(row, in_progress):
    out = dict(name=row['display_name'])
    for field, column in (('position', 'position'), ('height', 'height'),
                          ('weight', 'weight'), ('born', 'birth_date'),
                          ('college', 'college'), ('jersey', 'jersey_number'),
                          ('rookie', 'rookie_season'),
                          ('last', 'last_season'),
                          ('experience', 'years_experience')):
        if row[column] is not None:
            out[field] = row[column]
    # The source's status is AS OF THE SNAPSHOT, so for a player whose career
    # is over it is whatever his last transaction left behind: Larry Allen,
    # retired in 2007, reads ACT. It is shown only for a player still active
    # in the season in progress, where "as of now" is what it means.
    if row['status'] and (row['last_season'] or 0) >= in_progress:
        out['status'] = row['status']
    if row['latest_team']:
        out['team'] = _code(row['latest_team'])
    if row['stub']:
        out['stub'] = 1
    return out


def _roster_rows(connection, where, params):
    rows = connection.execute(
        'SELECT s.player_key, s.season, s.team_key, s.code, s.position,'
        '       s.jersey_number, s.status, s.last_week, s.last_game_type'
        '  FROM player_season s WHERE ' + where
        + ' ORDER BY s.player_key, s.season, s.last_week, s.team_key',
        params).fetchall()
    out = defaultdict(list)
    for row in rows:
        entry = dict(season=row['season'], team=_code(row['team_key']))
        for field, column in (('position', 'position'),
                              ('jersey', 'jersey_number'),
                              ('status', 'status')):
            if row[column] is not None:
                entry[field] = row[column]
        out[row['player_key']].append(entry)
    return out


def _stat_rows(connection, where, params):
    """(player -> [season row]) with each row's values keyed by stat name."""
    contexts = connection.execute(
        'SELECT player_key, season, season_type, team_key, games'
        '  FROM player_stat_season WHERE ' + where
        + ' ORDER BY player_key, season, season_type', params).fetchall()
    values = defaultdict(dict)
    for row in connection.execute(
            'SELECT player_key, season, season_type, stat, value'
            '  FROM player_stat WHERE ' + where, params):
        if row['stat'] not in EXPORTED_STATS:
            continue
        values[(row['player_key'], row['season'], row['season_type'])][
            row['stat']] = _number(row['value'])
    out = defaultdict(list)
    for row in contexts:
        key = (row['player_key'], row['season'], row['season_type'])
        out[row['player_key']].append(dict(
            season=row['season'], type=row['season_type'],
            team=_code(row['team_key']), games=row['games'],
            stats=values.get(key, {})))
    return out


def _draft_of(connection):
    return {row['player_key']: dict(season=row['season'], round=row['round'],
                                    pick=row['pick'],
                                    team=_code(row['team_key']))
            for row in connection.execute(
                'SELECT player_key, season, round, pick, team_key'
                '  FROM draft_pick WHERE player_key IS NOT NULL')}


SEASON_TYPES = ('reg', 'post')


def _interned(players_out):
    """Pack each player's rows into positional arrays over one name list.

    A season row becomes `[season, type, team, games, i, v, i, v, ...]`, where
    `type` is 0 for the regular season and 1 for the postseason and each `i`
    is a position in the file's `stats` list. A roster row becomes
    `[season, team, position, jersey, status]`. Written as objects the
    careers came to 18.4 MB; most of that was the same eight keys repeated
    700,000 times.
    """
    names = sorted({name for player in players_out.values()
                    for row in player.get('seasons', [])
                    for name in row['stats']})
    position = {name: index for index, name in enumerate(names)}
    for player in players_out.values():
        packed = []
        for row in player.get('seasons', []):
            line = [row['season'], SEASON_TYPES.index(row['type']),
                    row['team'], row['games']]
            for name, value in sorted(row['stats'].items(),
                                      key=lambda item: position[item[0]]):
                line.extend((position[name], value))
            packed.append(line)
        if packed:
            player['seasons'] = packed
        if player.get('roster'):
            player['roster'] = [[item['season'], item['team'],
                                 item.get('position'), item.get('jersey'),
                                 item.get('status')]
                                for item in player['roster']]
    return names


def build(connection, *, now=None, settled_before=None):
    """Every people file, as Python objects, keyed by published path.

    Nothing is written here; `export.write` stages these beside the rest of
    the export so one gate covers every file and one manifest names them.
    """
    in_progress = (settled_before if settled_before is not None
                   else current_season(now) if now is not None else None)
    if in_progress is None:
        raise ValueError('build() needs `now` or `settled_before`')
    everyone = players(connection)
    drafted = _draft_of(connection)
    settled_rosters = _roster_rows(connection, 's.season < ?', (in_progress,))
    settled_stats = _stat_rows(connection, 'season < ?', (in_progress,))
    live_rosters = _roster_rows(connection, 's.season >= ?', (in_progress,))
    live_stats = _stat_rows(connection, 'season >= ?', (in_progress,))

    shards = defaultdict(dict)
    index = []
    for row in everyone:
        gsis = _gsis(row['player_key'])
        shard = shard_of(gsis)
        entry = _bio(row, in_progress)
        if row['player_key'] in drafted:
            entry['draft'] = drafted[row['player_key']]
        if settled_rosters.get(row['player_key']):
            entry['roster'] = settled_rosters[row['player_key']]
        if settled_stats.get(row['player_key']):
            entry['seasons'] = settled_stats[row['player_key']]
        shards[shard][gsis] = entry
        spans = [item['season'] for item in
                 settled_rosters.get(row['player_key'], [])
                 + live_rosters.get(row['player_key'], [])
                 + settled_stats.get(row['player_key'], [])
                 + live_stats.get(row['player_key'], [])]
        index.append([gsis, row['display_name'], row['position'],
                      min(spans) if spans else row['rookie_season'],
                      max(spans) if spans else row['last_season'],
                      _code(row['latest_team']), shard])

    current = {}
    for key in sorted(set(live_rosters) | set(live_stats)):
        entry = {}
        if live_rosters.get(key):
            entry['roster'] = live_rosters[key]
        if live_stats.get(key):
            entry['seasons'] = live_stats[key]
        current[_gsis(key)] = entry

    files = {}
    for shard in range(PLAYER_SHARDS):
        body = shards.get(shard, {})
        names = _interned(body)
        files[f'people/careers/{shard}.js'] = (
            'NFL_PEOPLE_SHARDS', shard, dict(
                stats=names, players=body,
                season_fields=['season', 'type', 'team', 'games'],
                roster_fields=['season', 'team', 'position', 'jersey',
                               'status']))
    current_names = _interned(current)
    files['people/current.js'] = ('NFL_PEOPLE_CURRENT', None, dict(
        season=in_progress, stats=current_names, players=current,
        season_fields=['season', 'type', 'team', 'games'],
        roster_fields=['season', 'team', 'position', 'jersey', 'status']))
    files['people/index.js'] = ('NFL_PEOPLE_INDEX', None, dict(
        shards=PLAYER_SHARDS,
        fields=['id', 'name', 'position', 'first', 'last', 'team', 'shard'],
        players=index,
        tables=[dict(key=key, label=label, stats=list(stats))
                for key, label, stats in STAT_TABLES],
        mean_stats=sorted(MEAN_STATS), max_stats=sorted(MAX_STATS)))
    files['people/draft.js'] = ('NFL_DRAFT', None, draft(connection))
    return files


DRAFT_FIELDS = ('round', 'pick', 'team', 'id', 'name', 'position', 'college',
                'hall_of_fame', 'all_pro', 'pro_bowls', 'seasons_started',
                'games', 'weighted_av', 'draft_av')


def draft(connection):
    """Every pick 1999 onward, by season, as positional rows.

    `id` is the GSIS id where the pick resolves to a player with a page and
    null where it does not, so the board can link the ones it can and print
    the rest as plain names rather than dropping them.
    """
    paged = {row[0] for row in connection.execute(
        'SELECT player_key FROM player p'
        ' WHERE EXISTS (SELECT 1 FROM player_season s'
        '                WHERE s.player_key = p.player_key)'
        '    OR EXISTS (SELECT 1 FROM player_stat_season s'
        '                WHERE s.player_key = p.player_key)')}
    seasons = defaultdict(list)
    for row in connection.execute(
            'SELECT * FROM draft_pick ORDER BY season, round, pick'):
        key = row['player_key']
        seasons[str(row['season'])].append([
            row['round'], row['pick'], _code(row['team_key']),
            _gsis(key) if key and key in paged else None, row['name'],
            row['position'], row['college'], row['hall_of_fame'] or 0,
            row['all_pro'], row['pro_bowls'], row['seasons_started'],
            row['games'], row['weighted_av'], row['draft_av']])
    return dict(fields=list(DRAFT_FIELDS), seasons=dict(seasons))


def leaders(connection, season, *, deep=LEADERS_DEEP):
    """A season's regular-season leaders in each `LEADER_STATS` category.

    Ties at the cut are all kept, so tenth place is never decided by the
    order SQLite happened to return two equal rows in.
    """
    out = {}
    for stat in LEADER_STATS:
        rows = connection.execute(
            'SELECT v.player_key, v.value, p.display_name, p.position,'
            '       t.team_key'
            '  FROM player_stat v JOIN player p USING (player_key)'
            '  LEFT JOIN player_stat_season t'
            '    ON t.player_key = v.player_key AND t.season = v.season'
            '   AND t.season_type = v.season_type'
            " WHERE v.season = ? AND v.season_type = 'reg' AND v.stat = ?"
            ' ORDER BY v.value DESC, p.display_name', (season, stat)
        ).fetchall()
        if not rows:
            continue
        cut = rows[min(deep, len(rows)) - 1]['value']
        out[stat] = [dict(id=_gsis(row['player_key']),
                          name=row['display_name'],
                          position=row['position'],
                          team=_code(row['team_key']),
                          value=_number(row['value']))
                     for row in rows if row['value'] >= cut]
    return out


def render(variable, key, payload):
    body = json.dumps(payload, separators=(',', ':'), sort_keys=True)
    if key is None:
        return f'window.{variable}={body};\n'
    return (f'window.{variable}=(window.{variable}||{{}});'
            f'window.{variable}[{key}]={body};\n')
