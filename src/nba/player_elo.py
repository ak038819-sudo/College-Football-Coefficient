"""Player Elo: replaying ESPN's net points into one rating a player.

`src/nba/net_points.py` stores a grade for every appearance -- how many
points above average a player's team scored, and allowed, per 100
possessions he was on the floor for. This module turns a career of those
into one number, by walking them in order and keeping a weighted average
that forgets at a fixed rate.

**THE RATING IS A WEIGHTED AVERAGE, NOT AN ELO UPDATE**, whatever it is
called on the page. There is no expectation and no error term: a grade is
already a measurement against the league, so the replay has nothing to
predict. What it does have is an exposure problem -- a 12-possession
appearance and a 95-possession one are not equal evidence -- and that is
what `exposure` weights and what the half-life is measured in.

    offence  = 100 * offensive net points / offensive team possessions
    defence  = 100 * defensive net points / defensive team possessions
    exposure = (offensive + defensive team possessions) / 2
    decay    = 2 ** (-exposure / HALF_LIFE_POSSESSIONS)

    weight  <- decay * weight  + (1 - decay)
    offence <- decay * offence + (1 - decay) * this game's offence
    defence <- decay * defence + (1 - decay) * this game's defence

    impact = offence / weight + defence / weight
    rating = 1500 + DISPLAY_SCALE * impact

**THE TWO SIDES HAVE DIFFERENT DENOMINATORS** and neither may stand in for
the other. A player's team has the ball for one count of possessions while
he is on the floor and the opponent for another; the archive carries both,
and `t_team_poss` is their sum, which is why the sum is not used here.

**ALL THREE ACCUMULATORS START AT ZERO, so the first grade IS the rating**:
`weight` is 1 - decay and the totals are (1 - decay) times the grade, so
the division returns the grade itself. That is deliberate -- a new player
is not asserted to be average -- and it is why the rating is provisional
until there are enough minutes behind it, rather than being pulled towards
1500 by a prior nobody measured.

**MINUTES DECIDE ELIGIBILITY, AND THEY COME FROM OUR OWN ARCHIVE.** A
player is shown once he has 500 minutes in the season being displayed,
counted from `person_game`, not from the grades. A game the grades missed
is a gap in the evidence, and it must not make a player who played look
like one who did not.

**NOTHING IS CARRIED FORWARD EXCEPT BY POSSESSIONS.** An offseason, an
injury, a trade: none of them touch the three accumulators, because only
new possessions move them. So a rating is continuous through a trade and
unchanged by a year out, and the replay is deterministic from its fixed
start season -- which lives in the model config, because changing it
changes every opening rating.
"""

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone

from . import net_points
from .store import connect, record_snapshot

# The exposure, in possessions, at which a grade's weight has halved. 2,000
# is about eleven games of a starter's floor time: a 70-possession game
# then carries about 2.4% of a long history, which is the responsiveness
# the pilot settled on.
HALF_LIFE_POSSESSIONS = 2000.0
# Display only. 1500 is zero net impact and 25 points is +1 per 100.
DISPLAY_SCALE = 25.0
BASE_ELO = 1500.0
# Minutes in the displayed season before a rating is ranked or shown as
# settled.
ELIGIBLE_MINUTES = 500.0
# The replay's fixed start. Moving it changes every opening rating, so it
# is config, not an argument's default.
FIRST_SEASON = 2024
RATE_BASE = 100.0

MODEL_NAME = 'player-net-points'

RATED = 'rated'
PROVISIONAL = 'provisional'
UNAVAILABLE = 'unavailable'


def rate(net, possessions):
    """A per-100 rate, or None where there is nothing to divide by."""
    if net is None or possessions is None or possessions <= 0:
        return None
    return RATE_BASE * net / possessions


def graded(row):
    """(offence, defence, exposure) for one appearance, or None.

    None is not a zero. A missing grade gives no update at all, which is
    what keeps a feed outage from reading as a run of average games.
    """
    offense = rate(row.get('offense_net_points'),
                   row.get('offense_team_possessions'))
    defense = rate(row.get('defense_net_points'),
                   row.get('defense_team_possessions'))
    if offense is None or defense is None:
        return None
    exposure = ((row.get('offense_team_possessions') or 0.0)
                + (row.get('defense_team_possessions') or 0.0)) / 2.0
    if exposure <= 0:
        return None
    return offense, defense, exposure


def decay(exposure, half_life=HALF_LIFE_POSSESSIONS):
    """How much of the history survives an appearance of this exposure."""
    if half_life <= 0:
        return 0.0
    return 2.0 ** (-float(exposure) / float(half_life))


class Impact:
    """One player's running offence, defence and weight.

    Nothing here is a rating until `weight` is above zero; before that the
    player has no evidence and `impact` is None rather than 1500.
    """

    __slots__ = ('weight', 'offense_total', 'defense_total', 'games',
                 'possessions')

    def __init__(self):
        self.weight = 0.0
        self.offense_total = 0.0
        self.defense_total = 0.0
        self.games = 0
        self.possessions = 0.0

    @property
    def measured(self):
        return self.weight > 0

    @property
    def offense(self):
        return self.offense_total / self.weight if self.measured else None

    @property
    def defense(self):
        return self.defense_total / self.weight if self.measured else None

    @property
    def impact(self):
        if not self.measured:
            return None
        return self.offense + self.defense

    def update(self, offense, defense, exposure,
               half_life=HALF_LIFE_POSSESSIONS):
        """Fold one appearance in, and return the impact after it."""
        kept = decay(exposure, half_life)
        fresh = 1.0 - kept
        self.weight = kept * self.weight + fresh
        self.offense_total = kept * self.offense_total + fresh * offense
        self.defense_total = kept * self.defense_total + fresh * defense
        self.games += 1
        self.possessions += exposure
        return self.impact


def elo(impact, scale=DISPLAY_SCALE):
    """The displayed rating for an impact, or None for no evidence."""
    if impact is None:
        return None
    return BASE_ELO + float(scale) * impact


def status(minutes, games, *, threshold=ELIGIBLE_MINUTES):
    """What a page may say about a rating.

    A player with no graded game at all is `unavailable`, not a 1500.
    """
    if not games:
        return UNAVAILABLE
    if (minutes or 0.0) >= threshold:
        return RATED
    return PROVISIONAL


def replay(rows, *, half_life=HALF_LIFE_POSSESSIONS, impacts=None):
    """Walk graded appearances in order and return one record each.

    `rows` must already be in the replay's order -- date, tip-off, game,
    person -- which `net_points.season_rows` returns them in. `impacts`
    carries state in from earlier seasons, so a multi-season replay is the
    same walk run on more rows.
    """
    carried = impacts if impacts is not None else defaultdict(Impact)
    records = []
    for row in rows:
        measured = graded(row)
        if measured is None:
            continue
        offense, defense, exposure = measured
        state = carried[row['person_key']]
        before = state.impact
        after = state.update(offense, defense, exposure, half_life)
        records.append({
            'person_key': row['person_key'],
            'game_key': row['game_key'],
            'season': row['season'],
            'game_date': row.get('game_date'),
            'offense_grade': offense,
            'defense_grade': defense,
            'game_grade': offense + defense,
            'exposure': exposure,
            'pregame_impact': before,
            'postgame_impact': after,
            # THE RUNNING HALVES, not this game's. `offense_grade` above is
            # what he was worth in this game alone; these two are what the
            # rating is made of, and they add to `postgame_impact`. A
            # snapshot that reported the game's halves beside the career
            # impact showed a man with an impact of +6.9 and an offence of
            # -8.1, which was his last night and not his season.
            'offense_impact': state.offense,
            'defense_impact': state.defense,
            'change': None if before is None else after - before,
        })
    return records, carried


def season_minutes(connection, season):
    """Minutes each person played in a season, from our own archive."""
    rows = connection.execute(
        'SELECT person_key, COALESCE(SUM(minutes), 0) FROM person_game '
        "WHERE season = ? AND season_type = 'regular' AND played = 1 "
        'GROUP BY person_key', (int(season),))
    return {person: float(minutes) for person, minutes in rows}


def snapshots(records, minutes, *, as_of, threshold=ELIGIBLE_MINUTES,
              scale=DISPLAY_SCALE):
    """One row a player: the rating after his last graded game of a season.

    The rating itself is a career quantity; the exposure and eligibility
    beside it are the season's, which is what the leaderboard ranks on.
    """
    latest, counted = {}, defaultdict(
        lambda: {'games': 0, 'possessions': 0.0})
    for record in records:
        latest[record['person_key']] = record
        tally = counted[record['person_key']]
        tally['games'] += 1
        tally['possessions'] += record['exposure']
    rows = []
    for person, record in latest.items():
        impact = record['postgame_impact']
        played = minutes.get(person, 0.0)
        tally = counted[person]
        rows.append({
            'person_key': person,
            'season': record['season'],
            'as_of': as_of,
            'elo': elo(impact, scale),
            'impact_per_100': impact,
            'offense_per_100': record['offense_impact'],
            'defense_per_100': record['defense_impact'],
            'season_minutes': played,
            'graded_games': tally['games'],
            'graded_possessions': tally['possessions'],
            'last_graded_game': record['game_key'],
            'status': status(played, tally['games'], threshold=threshold),
        })
    return rows


def next_window_error(rows, records, *, after=5, window=500.0,
                     eligible=None):
    """How wrong the rating is about a player's next `window` possessions.

    The diagnostic the pilot was validated on, and the reason the rating
    carries across seasons rather than resetting. For every player with
    enough of a previous season behind him, take the rating after his
    `after`th graded game and compare it with what he actually did over his
    next `window` possessions of exposure. Returned as a
    ROOT-MEAN-SQUARE error, in net points per 100.

    Measured on this archive, replaying from 2023-24:

    | season | carried across | reset each season |
    | --- | --- | --- |
    | 2024-25 | 2.26 | 4.28 |
    | 2025-26 | 2.69 | 4.22 |

    A rating that starts every season from nothing is about twice as wrong
    about the next 500 possessions as one that remembers.
    """
    grades = {(row['person_key'], row['game_key']): row for row in rows}
    seen = defaultdict(list)
    for record in records:
        seen[record['person_key']].append(record)
    squares, counted = 0.0, 0
    for person, appearances in seen.items():
        if eligible is not None and person not in eligible:
            continue
        if len(appearances) <= after:
            continue
        predicted = appearances[after - 1]['postgame_impact']
        offense = defense = made = allowed = exposure = 0.0
        for record in appearances[after:]:
            row = grades.get((person, record['game_key']))
            if row is None:
                continue
            offense += row['offense_net_points']
            defense += row['defense_net_points']
            made += row['offense_team_possessions']
            allowed += row['defense_team_possessions']
            exposure += record['exposure']
            if exposure >= window:
                break
        if exposure < window or made <= 0 or allowed <= 0:
            continue
        actual = rate(offense, made) + rate(defense, allowed)
        squares += (predicted - actual) ** 2
        counted += 1
    if not counted:
        return None, 0
    return (squares / counted) ** 0.5, counted


def model_config(*, half_life=HALF_LIFE_POSSESSIONS, scale=DISPLAY_SCALE,
                 threshold=ELIGIBLE_MINUTES, first_season=FIRST_SEASON):
    """The versioned settings a replay is reproducible from."""
    return {'half_life_possessions': half_life, 'display_scale': scale,
            'eligible_minutes': threshold, 'first_season': int(first_season),
            'base': BASE_ELO, 'source': 'espn-analytics-net-points'}


def register_model(connection, config, *, now=None, snapshot_id=None):
    """The model row this replay's rows belong to, by name."""
    stamp = (now or datetime.now(timezone.utc)).isoformat()
    body = json.dumps(config, sort_keys=True)
    existing = connection.execute(
        'SELECT model_id FROM rating_model WHERE name = ?',
        (MODEL_NAME,)).fetchone()
    if existing:
        connection.execute(
            'UPDATE rating_model SET config = ?, fitted_on = ?, '
            'snapshot_id = ? WHERE model_id = ?',
            (body, stamp, snapshot_id, existing[0]))
        return existing[0]
    cursor = connection.execute(
        'INSERT INTO rating_model (name, config, fitted_on, created_at, '
        'snapshot_id) VALUES (?, ?, ?, ?, ?)',
        (MODEL_NAME, body, stamp, stamp, snapshot_id))
    return cursor.lastrowid


def store(connection, model_id, records, rows, *, snapshot_id=None):
    """Replace this model's replay. A rebuild is the normal path."""
    connection.execute('DELETE FROM player_rating_game WHERE model_id = ?',
                       (model_id,))
    connection.execute('DELETE FROM player_rating WHERE model_id = ?',
                       (model_id,))
    connection.executemany(
        'INSERT INTO player_rating_game (person_key, game_key, model_id, '
        'season, game_date, offense_grade, defense_grade, game_grade, '
        'exposure, pregame_impact, postgame_impact, change) '
        'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        [(one['person_key'], one['game_key'], model_id, one['season'],
          one['game_date'], one['offense_grade'], one['defense_grade'],
          one['game_grade'], one['exposure'], one['pregame_impact'],
          one['postgame_impact'], one['change']) for one in records])
    connection.executemany(
        'INSERT INTO player_rating (person_key, season, model_id, as_of, '
        'elo, impact_per_100, offense_per_100, defense_per_100, '
        'season_minutes, graded_games, graded_possessions, '
        'last_graded_game, status, snapshot_id) '
        'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        [(one['person_key'], one['season'], model_id, one['as_of'],
          one['elo'], one['impact_per_100'], one['offense_per_100'],
          one['defense_per_100'], one['season_minutes'],
          one['graded_games'], one['graded_possessions'],
          one['last_graded_game'], one['status'], snapshot_id)
         for one in rows])
    connection.commit()
    return dict(appearances=len(records), snapshots=len(rows),
                people=len({one['person_key'] for one in records}))


def seasons_present(connection, first=FIRST_SEASON):
    """The seasons with grades stored, oldest first, from the fixed start."""
    rows = connection.execute(
        'SELECT DISTINCT season FROM player_net_points WHERE season >= ? '
        'ORDER BY season', (int(first),))
    return [int(one[0]) for one in rows]


def build(connection, *, seasons=None, now=None, config=None):
    """Replay every season of grades and store the result.

    One walk, oldest season first, with the accumulators carried across the
    boundary: that is the whole of "persist or replay across seasons".
    """
    settings = config or model_config()
    stamp = (now or datetime.now(timezone.utc)).isoformat()
    first = int(settings['first_season'])
    walk = seasons if seasons is not None else seasons_present(connection,
                                                               first)
    walk = [one for one in walk if int(one) >= first]
    snapshot_id = record_snapshot(
        connection, 'player_elo', stamp,
        source='player_net_points',
        source_revision=net_points.ARCHIVE_ROOT,
        notes=json.dumps(settings, sort_keys=True))
    model_id = register_model(connection, settings, now=now,
                              snapshot_id=snapshot_id)
    impacts = defaultdict(Impact)
    records, rows = [], []
    for season in walk:
        stored = net_points.season_rows(connection, season)
        made, impacts = replay(stored,
                               half_life=settings['half_life_possessions'],
                               impacts=impacts)
        records.extend(made)
        rows.extend(snapshots(
            made, season_minutes(connection, season), as_of=stamp,
            threshold=settings['eligible_minutes'],
            scale=settings['display_scale']))
    counts = store(connection, model_id, records, rows,
                   snapshot_id=snapshot_id)
    ranked = sum(1 for one in rows if one['status'] == RATED)
    return dict(seasons=[int(one) for one in walk], model_id=model_id,
                ranked=ranked, **counts)


def leaderboard(connection, season, *, limit=None, model_id=None,
                statuses=(RATED,)):
    """A season's rated players, best first, ties broken by person key.

    `statuses` widens it: the export asks for the provisional men as well,
    because a season two weeks old has nobody over the minutes cutoff and a
    page that drew nothing there would hide the layer for a month. They
    arrive carrying their own status, and it is the page's business to say
    so -- nothing here calls a provisional man ranked.
    """
    model = model_id
    if model is None:
        row = connection.execute(
            'SELECT model_id FROM rating_model WHERE name = ?',
            (MODEL_NAME,)).fetchone()
        if row is None:
            return []
        model = row[0]
    sql = ('SELECT person_key, elo, impact_per_100, offense_per_100, '
           '       defense_per_100, season_minutes, graded_games, '
           '       graded_possessions, status, last_graded_game, as_of '
           'FROM player_rating WHERE season = ? AND model_id = ? '
           '  AND status IN (%s) '
           'ORDER BY elo DESC, person_key'
           % ','.join('?' * len(statuses)))
    if limit:
        sql += ' LIMIT %d' % int(limit)
    names = ('person_key', 'elo', 'impact_per_100', 'offense_per_100',
             'defense_per_100', 'season_minutes', 'graded_games',
             'graded_possessions', 'status', 'last_graded_game', 'as_of')
    params = (int(season), model) + tuple(statuses)
    return [dict(zip(names, row))
            for row in connection.execute(sql, params)]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', default=None)
    parser.add_argument('--seasons', default='',
                        help='comma-separated; the default is every season '
                             'with grades stored')
    args = parser.parse_args(argv)
    seasons = [int(one) for one in args.seasons.replace(' ', '').split(',')
               if one] or None
    connection = connect(args.db)
    try:
        report = build(connection, seasons=seasons)
    finally:
        connection.close()
    print('%d appearances replayed for %d people over seasons %s'
          % (report['appearances'], report['people'],
             ', '.join(str(one) for one in report['seasons'])))
    print('%d season rows, %d of them ranked'
          % (report['snapshots'], report['ranked']))
    return 0


if __name__ == '__main__':
    sys.exit(main())
