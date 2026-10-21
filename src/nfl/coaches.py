"""Head coaches, and their teams' results against what the model expected.

The archive names the home and away coach on EVERY game it holds — all 7,548
of them, 1999 through the week being played. That is a stronger starting point
than the college side had: CFBD never says whose games were whose, so 361 of
its 5,657 head-coaching tenures carry no wins-above-expectation at all. Here
every single game is attributed to the person who coached it, which means a
midseason change splits a season cleanly instead of being averaged across two
people.

**WINS ABOVE EXPECTATION is the one number here worth reading twice.** It is
the team's actual wins minus the sum of the model's PREGAME probability that
the team won each game. Three properties make it mean something:

- The probability comes from the stored rating chain, so it is the number the
  model would have published the morning of the game, not one computed with
  the benefit of the season's results.
- A tie counts half a win on both sides of the subtraction, so the 16 level
  games since 1999 neither vanish nor count as losses.
- It is NOT a measure of coaching. A team that beat its rating may have been
  lucky, healthy, or underrated by the model; this says the model was wrong in
  that team's favour and nothing more. The page says so too.

A coach is keyed by name (`nfl:nflverse:coach:Andy Reid`) because the archive
numbers coaches nowhere. See `identity.coach_key`.
"""

import argparse
import sys
from collections import defaultdict

from .identity import coach_key
from .store import connect

# One row per (coach, season, team): the shape that holds a midseason change
# and a coach who moves, without averaging either away.
KEY_FIELDS = ('coach_key', 'season', 'team_key')


def game_rows(connection, model_id):
    """Every finished game with its coaches and the model's pregame call.

    An unfinished game is left out rather than counted as a half: a schedule
    is not a record. A game the replay declined has no expectation, so it
    cannot contribute to a comparison with one.
    """
    rows = connection.execute(
        'SELECT g.game_key, g.season, g.postseason, g.home_team_key,'
        '       g.away_team_key, g.home_points, g.away_points, g.home_coach,'
        '       g.away_coach, r.home_expected, r.rating_change'
        '  FROM game g JOIN rating_game r'
        '    ON r.game_key = g.game_key AND r.model_id = ?'
        ' WHERE g.home_points IS NOT NULL AND g.away_points IS NOT NULL'
        '   AND r.counted = 1'
        "  ORDER BY g.season, g.game_date, COALESCE(g.kickoff_utc, ''),"
        '       g.provider_id', (model_id,)).fetchall()
    return [dict(row) for row in rows]


def _blank():
    return dict(games=0, wins=0, losses=0, ties=0, postseason_games=0,
                postseason_wins=0, expected_wins=0.0, elo_change=0.0)


def tally(rows):
    """Per (coach, season, team) records and expectations.

    A game with no coach named on one side contributes to the other side only.
    Inventing a coach, or dropping the whole game, would both be worse: one
    makes up a person and the other quietly shortens a real coach's record.
    """
    out, names = {}, {}
    for row in rows:
        margin = row['home_points'] - row['away_points']
        for side in ('home', 'away'):
            name = row[f'{side}_coach']
            if not name or not str(name).strip():
                continue
            key, cleaned = coach_key(name)
            names[key] = cleaned
            team = row[f'{side}_team_key']
            bucket = out.setdefault((key, row['season'], team), _blank())
            # The model's pregame probability that THIS team won.
            expected = row['home_expected'] if side == 'home' \
                else 1.0 - row['home_expected']
            # The chain stores the home team's change; the away team's is its
            # negative, which is what makes the two sides add to zero.
            change = row['rating_change'] if side == 'home' \
                else -row['rating_change']
            bucket['games'] += 1
            bucket['expected_wins'] += expected
            bucket['elo_change'] += change
            won = margin > 0 if side == 'home' else margin < 0
            if margin == 0:
                bucket['ties'] += 1
            elif won:
                bucket['wins'] += 1
            else:
                bucket['losses'] += 1
            if row['postseason']:
                bucket['postseason_games'] += 1
                if margin != 0 and won:
                    bucket['postseason_wins'] += 1
    return out, names


def seasons(connection, model_id):
    """The stored rows for every coach-season-team, ready to upsert."""
    counted, names = tally(game_rows(connection, model_id))
    out = []
    for (key, season, team), bucket in sorted(counted.items()):
        # A tie is half a win on BOTH sides of the subtraction: the actual
        # record counts it half and the expectation already can be any
        # fraction, so the comparison stays symmetric.
        actual = bucket['wins'] + bucket['ties'] / 2.0
        out.append(dict(
            coach_key=key, coach_name=names[key], season=season,
            team_key=team, games=bucket['games'], wins=bucket['wins'],
            losses=bucket['losses'], ties=bucket['ties'],
            postseason_games=bucket['postseason_games'],
            postseason_wins=bucket['postseason_wins'],
            expected_wins=bucket['expected_wins'],
            wins_above=actual - bucket['expected_wins'],
            elo_change=bucket['elo_change']))
    return out


def careers(rows):
    """Each coach's whole career, summed from the season rows.

    Summed here rather than stored: a career is the sum of its seasons and
    storing it twice is a second number to keep in step with the first. Wins
    above expectation sums because each term is a difference of counts over
    disjoint games — it is not an average being averaged.
    """
    out = defaultdict(lambda: dict(
        coach_name=None, seasons=0, teams=set(), games=0, wins=0, losses=0,
        ties=0, postseason_games=0, postseason_wins=0, expected_wins=0.0,
        wins_above=0.0, first_season=None, last_season=None))
    for row in rows:
        career = out[row['coach_key']]
        career['coach_name'] = row['coach_name']
        career['teams'].add(row['team_key'])
        for field in ('games', 'wins', 'losses', 'ties', 'postseason_games',
                      'postseason_wins', 'expected_wins', 'wins_above'):
            career[field] += row[field]
        first, last = career['first_season'], career['last_season']
        career['first_season'] = row['season'] if first is None \
            else min(first, row['season'])
        career['last_season'] = row['season'] if last is None \
            else max(last, row['season'])
    result = []
    for key, career in out.items():
        # A coach's season count is the number of DISTINCT seasons, not the
        # number of rows: a coach who moved midseason has two rows for one
        # season and claiming two seasons would inflate every career that
        # ever changed team.
        career = dict(career, coach_key=key, teams=sorted(career['teams']))
        career['seasons'] = len({row['season'] for row in rows
                                 if row['coach_key'] == key})
        result.append(career)
    result.sort(key=lambda row: (-row['wins_above'], row['coach_name']))
    return result


FIELDS = ('coach_key', 'coach_name', 'season', 'team_key', 'games', 'wins',
          'losses', 'ties', 'postseason_games', 'postseason_wins',
          'expected_wins', 'wins_above', 'elo_change')


def store_seasons(connection, model_id, rows):
    columns = ', '.join(FIELDS) + ', model_id'
    placeholders = ', '.join('?' * (len(FIELDS) + 1))
    updates = ', '.join(f'{name} = excluded.{name}' for name in FIELDS
                        if name not in KEY_FIELDS)
    for row in rows:
        connection.execute(
            f'INSERT INTO coach_season ({columns}) VALUES ({placeholders})'
            f' ON CONFLICT(coach_key, season, team_key, model_id)'
            f' DO UPDATE SET {updates}',
            tuple(row[name] for name in FIELDS) + (model_id,))
    connection.commit()
    return len(rows)


def build(connection, model_id):
    rows = seasons(connection, model_id)
    stored = store_seasons(connection, model_id, rows)
    return dict(coach_seasons=stored, coaches=len(careers(rows)), rows=rows)


def latest_model(connection):
    row = connection.execute(
        'SELECT model_id FROM rating_model ORDER BY model_id DESC'
        ' LIMIT 1').fetchone()
    if row is None:
        raise ValueError('No rating model is stored; run build_ratings first')
    return row['model_id']


def main(argv=None):
    parser = argparse.ArgumentParser(
        description='Measure NFL head coaches against the model')
    parser.add_argument('--database', default=None)
    args = parser.parse_args(argv)
    connection = connect(args.database)
    try:
        model_id = latest_model(connection)
        summary = build(connection, model_id)
    except ValueError as error:
        print(f'Cannot measure coaches: {error}', file=sys.stderr)
        return 1
    finally:
        connection.close()
    print(f'{summary["coach_seasons"]} coach-season-team rows for '
          f'{summary["coaches"]} coaches')
    leaders = careers(summary['rows'])[:5]
    for row in leaders:
        print(f'  {row["coach_name"]:<22} {row["wins"]}-{row["losses"]}'
              f'-{row["ties"]}  wins above expectation '
              f'{row["wins_above"]:+.1f}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
