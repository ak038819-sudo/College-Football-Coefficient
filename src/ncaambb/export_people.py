"""The people half of the basketball export: who played, and what they did.

B02 loaded rosters, player identities and box scores nationally; nothing ever
published them, so the site's People tab has been drawn disabled since the
league launched. This builds the files behind it.

Three kinds of file, because three different screens ask three different
questions and one file that answered all of them would be downloaded by
readers who wanted none of it:

| File | Answers | Read by |
| --- | --- | --- |
| `people/index.js` | who exists, and which seasons have people at all | the People tab, for search |
| `people/<season>/roster.js` | who is on this club, and their season line | a club's page |
| `people/<season>/<shard>.js` | this player's membership and every game he played | a player's page |

The shards exist because a season is 127,501 player-game lines. A player page
needs one player's, so the season is split sixteen ways on the player's own id
and a page fetches the one shard it needs.

NOTHING HERE IS A SUM OF SOMETHING ELSE. A season line is the player's own box
score lines added up, which is a measurement; it is never reconciled against
the team's score, because the pilot found 25 box scores in 2025-26 that
disagree with the final score and the right answer there is to report the
disagreement, not to average it away.
"""

import json

# How many ways a season's player-game lines are split.
SHARDS = 16

# The export contract's own version for these files, inside the manifest's
# `people` section. Separate from the manifest's schema_version, which governs
# the manifest's shape.
PEOPLE_VERSION = 2

# How many of the most recent seasons get people files, by default.
#
# A BOUND, not a preference. A season is around 127,500 player-game lines and
# the archive holds thirteen of them; published in full that is tens of
# megabytes added to a site measured at 461 MB against GitHub Pages' 1 GB cap
# (docs/published-payload.md), for seasons whose rosters nobody is looking up.
# Six covers every player now playing for his whole career, which is the
# question a player page answers. Raise it with --people-seasons, having
# measured what it costs; the manifest states which seasons are carried, and
# the page says so rather than drawing a blank for the rest.
DEFAULT_SEASONS = 6


def shard_of(provider_id, shards=SHARDS):
    """Which shard a player's lines live in.

    A FLOORED modulo, written out rather than left to the language: Python's
    `%` is floored and JavaScript's is not, so a negative provider id -- which
    the football archive really does carry -- would land in shard -3 on one
    side and 13 on the other. Both sides must compute this the same way.
    """
    return ((int(provider_id) % shards) + shards) % shards


def _number(value):
    """A measured number, or None. 0 is a measurement and stays one."""
    if value is None:
        return None
    if float(value) == int(float(value)):
        return int(float(value))
    return round(float(value), 1)


def people_seasons(connection):
    """Seasons that have any people data at all, oldest first.

    Measured from the rows rather than assumed from the schedule: rosters and
    box scores are fetched separately from games, so a season can have a full
    schedule and no people, and the page must be able to tell.
    """
    rows = connection.execute(
        'SELECT season FROM player_team_season'
        ' UNION SELECT season FROM game g JOIN player_game_stat p'
        '   ON p.game_key = g.game_key'
        ' ORDER BY season').fetchall()
    return [row['season'] for row in rows]


def identities(connection, seasons):
    """Every player who appears in one of these seasons, by provider id.

    Keyed by the bare provider id, which the page turns back into a full
    `ncaambb:cbbd:player:<id>` key. The prefix is constant inside a file that
    exists only for basketball, and leaving it out of 13,000 keys is most of
    this file's size; the rule that a bare id is never a JOIN key across
    sports is unaffected, because nothing joins on what is written here.
    """
    if not seasons:
        return {}
    marks = ','.join('?' for _ in seasons)
    rows = connection.execute(
        'SELECT DISTINCT p.provider_id, p.name, p.hometown, p.hometown_state'
        '  FROM player p'
        '  JOIN player_team_season m ON m.player_key = p.player_key'
        f' WHERE m.season IN ({marks})'
        ' UNION '
        'SELECT DISTINCT p.provider_id, p.name, p.hometown, p.hometown_state'
        '  FROM player p'
        '  JOIN player_game_stat s ON s.player_key = p.player_key'
        '  JOIN game g ON g.game_key = s.game_key'
        f' WHERE g.season IN ({marks})'
        ' ORDER BY provider_id', (*seasons, *seasons)).fetchall()
    out = {}
    for row in rows:
        entry = {'n': row['name']} if row['name'] else {}
        if row['hometown']:
            entry['h'] = row['hometown']
        if row['hometown_state']:
            entry['s'] = row['hometown_state']
        out[str(row['provider_id'])] = entry
    return out


def _provider_id(key):
    """The numeric id at the end of an entity key."""
    return int(str(key).rsplit(':', 1)[-1])


def season_lines(connection, season):
    """Every player-game line in a season, grouped by player provider id.

    Each line is LINE_FIELDS. Minutes and points stay NULL when the provider
    recorded none: a player who was on the bench all night and one whose
    minutes nobody wrote down are different things, and 0 would merge them.

    The DAY and the OPPONENT are carried in the line itself rather than looked
    up in the season's own payload. A career page spans four or five seasons
    and loads one; measured in a browser against the real export, every season
    but the loaded one was a table of em dashes. Fetching four 1.8 MB season
    files to date forty games is the wrong trade -- a date and two small
    integers per line cost a fraction of that, and they make a player page
    self-contained.
    """
    rows = connection.execute(
        'SELECT g.provider_id AS game_id, p.provider_id AS player_id,'
        '       s.team_key, s.minutes, s.points, s.starter,'
        '       g.playing_date, g.start_date_utc, g.home_team_key,'
        '       g.away_team_key, g.neutral_site'
        '  FROM player_game_stat s'
        '  JOIN game g ON g.game_key = s.game_key'
        '  JOIN player p ON p.player_key = s.player_key'
        ' WHERE g.season = ?'
        ' ORDER BY g.provider_id, p.provider_id', (season,)).fetchall()
    teams, index, lines = [], {}, {}

    def slot(key):
        if key not in index:
            index[key] = len(teams)
            teams.append(key)
        return index[key]

    for row in rows:
        mine = slot(row['team_key'])
        # The opponent is the side of the game that is not the club this line
        # was recorded for. A line whose club is neither side of its own game
        # has no opponent to name, and says so rather than naming one at
        # random.
        other = None
        if row['team_key'] == row['home_team_key']:
            other = row['away_team_key']
        elif row['team_key'] == row['away_team_key']:
            other = row['home_team_key']
        site = None
        if row['neutral_site']:
            site = 'N'
        elif other is not None:
            site = 'H' if row['team_key'] == row['home_team_key'] else 'A'
        lines.setdefault(str(row['player_id']), []).append([
            row['game_id'], mine, _number(row['minutes']),
            _number(row['points']),
            None if row['starter'] is None else int(row['starter']),
            # The day it was PLAYED on, in Eastern time, which is what the
            # scoreboard groups by. `start_date_utc` is a fallback for a row
            # stored before that column existed, never a preference.
            row['playing_date'] or row['start_date_utc'],
            None if other is None else slot(other), site])
    return teams, index, lines


def season_memberships(connection, season):
    """Each player's club, jersey, position and listed size for a season.

    A transfer mid-career is two rows here and never one merged player: the
    provider id is the identity and this table is the membership.
    """
    rows = connection.execute(
        'SELECT p.provider_id AS player_id, m.team_key, m.jersey, m.position,'
        '       m.height, m.weight'
        '  FROM player_team_season m'
        '  JOIN player p ON p.player_key = m.player_key'
        ' WHERE m.season = ?'
        ' ORDER BY p.provider_id, m.team_key', (season,)).fetchall()
    return rows


def season_people(connection, season):
    """One season's people, as the shard payloads and the roster payload."""
    teams, index, lines = season_lines(connection, season)
    memberships = {}
    for row in season_memberships(connection, season):
        key = row['team_key']
        if key not in index:
            index[key] = len(teams)
            teams.append(key)
        memberships.setdefault(str(row['player_id']), []).append([
            index[key], row['jersey'], row['position'],
            _number(row['height']), _number(row['weight'])])

    # A season line is the player's OWN lines added up, per club, so a
    # mid-season transfer does not get one merged row.
    totals = {}
    for player, rows in lines.items():
        for line in rows:
            game_id, team_index, minutes, points = line[0], line[1], line[2], line[3]
            tally = totals.setdefault((player, team_index),
                                      dict(g=0, min=0.0, pts=0.0,
                                           min_known=False, pts_known=False))
            tally['g'] += 1
            if minutes is not None:
                tally['min'] += minutes
                tally['min_known'] = True
            if points is not None:
                tally['pts'] += points
                tally['pts_known'] = True

    shards = {}
    for player, rows in lines.items():
        shard = shards.setdefault(shard_of(player), dict(p={}, m={}))
        shard['p'][player] = rows
    for player, rows in memberships.items():
        shard = shards.setdefault(shard_of(player), dict(p={}, m={}))
        shard['m'][player] = rows

    roster = {}
    for player, rows in memberships.items():
        for team_index, jersey, position, _height, _weight in rows:
            tally = totals.get((player, team_index))
            roster.setdefault(teams[team_index], []).append([
                int(player), jersey, position,
                tally['g'] if tally else 0,
                _number(tally['min']) if tally and tally['min_known'] else None,
                _number(tally['pts']) if tally and tally['pts_known'] else None,
            ])
    # A club with a player who recorded a line but is on no roster snapshot --
    # 29 Division I players in 2025-26 -- would otherwise be missing from his
    # own club's roster.
    listed = {player for player, rows in memberships.items()}
    for (player, team_index), tally in totals.items():
        if player in listed:
            continue
        roster.setdefault(teams[team_index], []).append([
            int(player), None, None, tally['g'],
            _number(tally['min']) if tally['min_known'] else None,
            _number(tally['pts']) if tally['pts_known'] else None,
        ])
    for rows in roster.values():
        # Most used first: a roster ordered by name buries the five players a
        # reader came to look at. Points, then games, then the id, which is
        # stable across rebuilds where a name would not be.
        rows.sort(key=lambda row: (-(row[5] or 0), -(row[3] or 0), row[0]))
    return teams, shards, roster


def recent(seasons, limit=DEFAULT_SEASONS):
    """The newest `limit` seasons, oldest first. All of them when limit is 0.

    `limit` is None for the default and 0 for no bound at all, which are
    different answers and must not collapse into one: `limit or DEFAULT` would
    turn "every season" into six.
    """
    count = DEFAULT_SEASONS if limit is None else int(limit)
    ordered = sorted(seasons)
    return ordered if count <= 0 else ordered[-count:]


def payloads(connection, seasons=None, limit=None):
    """Every people file's payload, keyed by its path inside the export.

    Returns (files, summary). Built whole before anything is written, like the
    rest of the export, so the gate sees what a reader will.
    """
    available = people_seasons(connection)
    wanted = sorted(set(seasons) & set(available)) if seasons else available
    wanted = recent(wanted, limit)
    files, summary = {}, []
    if not wanted:
        return files, summary
    index = identities(connection, wanted)
    files['people/index.js'] = (
        'window.NCAAMBB_PEOPLE', dict(v=PEOPLE_VERSION, seasons=wanted,
                                      shards=SHARDS, players=index))
    for season in wanted:
        teams, shards, roster = season_people(connection, season)
        files[f'people/{season}/roster.js'] = (
            f'window.NCAAMBB_ROSTERS[{season}]',
            dict(v=PEOPLE_VERSION, season=season, fields=ROSTER_FIELDS,
                 teams=roster))
        for shard in range(SHARDS):
            body = shards.get(shard, dict(p={}, m={}))
            files[f'people/{season}/{shard}.js'] = (
                'NCAAMBB_PLAYER_GAMES_PUSH',
                dict(v=PEOPLE_VERSION, season=season, shard=shard, t=teams,
                     fields=LINE_FIELDS, member_fields=MEMBER_FIELDS,
                     m=body['m'], p=body['p']))
        summary.append(dict(season=season,
                            players=len({*[p for s in shards.values()
                                           for p in s['p']],
                                         *[p for s in shards.values()
                                           for p in s['m']]}),
                            clubs=len(roster),
                            lines=sum(len(rows) for s in shards.values()
                                      for rows in s['p'].values())))
    return files, summary


# What each row in those payloads means, carried in the payload itself so a
# reader is never decoding positions from memory.
ROSTER_FIELDS = ['player', 'jersey', 'position', 'games', 'minutes', 'points']
LINE_FIELDS = ['game', 'team', 'minutes', 'points', 'starter', 'day',
               'opponent', 'site']
MEMBER_FIELDS = ['team', 'jersey', 'position', 'height', 'weight']


def render(global_expr, payload):
    """One people file's text.

    `window.NCAAMBB_ROSTERS[<season>]` needs its container to exist first, and
    the shards push onto a queue so sixteen files can load in any order.
    """
    body = json.dumps(payload, separators=(',', ':'), sort_keys=True)
    if global_expr == 'NCAAMBB_PLAYER_GAMES_PUSH':
        return ('(window.NCAAMBB_PLAYER_GAMES='
                'window.NCAAMBB_PLAYER_GAMES||[]).push(' + body + ');\n')
    if global_expr.startswith('window.NCAAMBB_ROSTERS['):
        return ('window.NCAAMBB_ROSTERS=(window.NCAAMBB_ROSTERS||{});'
                + global_expr + '=' + body + ';\n')
    return global_expr + '=' + body + ';\n'


def manifest_section(files, stamp):
    """The `people` entry for the manifest: every path, with its hash.

    Nested under the manifest's `files`, so the gate's own file check walks it
    and refuses a people file that is missing, empty or stale without knowing
    anything about people.
    """
    seasons = {}
    for path in files:
        parts = path.split('/')
        if len(parts) != 3:
            continue
        season, name = parts[1], parts[2]
        entry = seasons.setdefault(season, dict(shards=[]))
        if name == 'roster.js':
            entry['roster'] = f'{path}?v={stamp(path)}'
        else:
            entry['shards'].append(f'{path}?v={stamp(path)}')
    for entry in seasons.values():
        entry['shards'].sort(key=lambda value: int(
            value.split('/')[-1].split('.')[0]))
    return dict(version=PEOPLE_VERSION, shards=SHARDS,
                index=f'people/index.js?v={stamp("people/index.js")}',
                seasons=seasons)
