"""NFL team Elo. Nothing here is inherited from football or basketball.

The college football engine is tuned for twelve games a season with a
success-rate performance layer behind it; the basketball one for thirty-odd
games and hard roster turnover. The NFL plays seventeen, keeps most of a roster
from one year to the next, and is the most tightly matched of the three. Every
parameter in this file is therefore fitted on NFL games and scored once on NFL
seasons the search never saw: `src/nfl/fit_elo.py` is that fit and
`config/nfl_model.json` is where the result lives.

**A TIE IS A RESULT HERE.** The basketball replay declines a level final,
because a tied college basketball game means the data is wrong. The NFL has
tied 16 times since 1999, and those games say something precise: two teams the
model separated turned out to be level. A tie scores 0.5 and updates both
ratings. Reusing basketball's `_decided()` would have thrown them away and
nothing would have looked wrong.

**The margin multiplier is normalised to average 1**, as in the basketball
engine, so that k keeps the same meaning across margin models and a model
comparison is a test of the model rather than of a hidden change in k.
"""

import math

INITIAL_RATING = 1500.0
SCALE = 400.0

MARGIN_MODELS = ('result_only', 'mov', 'mov_adjusted')


def expected_home(home_rating, away_rating, *, neutral=False,
                  home_advantage=0.0, scale=SCALE):
    """Probability the home team wins, before the game is played.

    A neutral-site game gets no venue term: every Super Bowl, and the handful
    of international games each season, are played at neither team's home.
    """
    edge = 0.0 if neutral else home_advantage
    return 1.0 / (1.0 + 10 ** ((away_rating - (home_rating + edge)) / scale))


def outcome(home_points, away_points):
    """1.0, 0.5 or 0.0 from the home team's point of view. None if unplayed."""
    if home_points is None or away_points is None:
        return None
    if home_points > away_points:
        return 1.0
    if home_points < away_points:
        return 0.0
    return 0.5


def margin_multiplier(model, *, margin, elo_diff, reference_margin,
                      exponent=1.0, m_min=0.25, m_max=4.0,
                      points_per_elo=0.0):
    """How far a result moves a rating, relative to an average win.

    `margin` is the winner's winning margin and is positive; a tie never
    reaches here and scores 1.0 by definition, because there is no winner
    whose margin could be compared with an average one. `elo_diff` is the
    WINNER's venue-adjusted pregame rating minus the loser's, so a negative
    value is an upset.
    """
    if model == 'result_only':
        return 1.0
    if margin <= 0:
        raise ValueError('A winning margin is positive; a tie is not a win')
    if reference_margin <= 0:
        raise ValueError('The reference margin must be measured, not assumed')
    if model == 'mov':
        raw = (margin / reference_margin) ** exponent
    elif model == 'mov_adjusted':
        implied = points_per_elo * elo_diff
        surplus = margin - implied
        # A heavy favourite can win by far less than implied, which makes the
        # base negative, and a negative base raised to a fractional exponent
        # is a complex number in Python rather than an error. Clamp first.
        base = max((surplus + reference_margin) / (2 * reference_margin), 0.0)
        raw = base ** exponent
    else:
        raise ValueError(f'Unknown margin model {model!r}')
    return min(max(raw, m_min), m_max)


def mean_winning_margin(games):
    """The mean winning margin of the games given. Measured, never assumed."""
    margins = [abs(game['home_points'] - game['away_points']) for game in games
               if game['home_points'] is not None
               and game['away_points'] is not None
               and game['home_points'] != game['away_points']]
    if not margins:
        raise ValueError('No decided game to measure a margin from')
    return sum(margins) / len(margins)


def fit_points_per_elo(games, *, home_advantage=0.0):
    """Points of final margin per point of venue-adjusted pregame gap.

    Least squares through the origin on the games given, so `mov_adjusted`
    compares a margin against an NFL number rather than one carried over from
    another sport. Needs pregame ratings, so it runs on a replay's output.
    """
    numerator = denominator = 0.0
    for game in games:
        gap = game['home_pregame'] \
            + (0.0 if game['neutral_site'] else home_advantage) \
            - game['away_pregame']
        margin = game['home_points'] - game['away_points']
        numerator += gap * margin
        denominator += gap * gap
    if denominator == 0:
        raise ValueError('Every pregame gap is zero; nothing to fit')
    return numerator / denominator


def regress(rating, retention, *, initial=INITIAL_RATING):
    """Carry a rating into the next season, pulled toward the mean."""
    if not 0.0 <= retention <= 1.0:
        raise ValueError('Retention is a fraction between 0 and 1')
    return initial + (rating - initial) * retention


def replay(games, config, *, initial=INITIAL_RATING):
    """Replay games in order, returning the per-game record and final ratings.

    `games` must already be ordered by kickoff. Season boundaries are detected
    from the season field and a rating is regressed once when the season
    changes, which is why order matters rather than being a convenience. The
    NFL numbers its postseason weeks from 1 again, so ordering by week would
    put the Super Bowl before the opening Sunday — the same trap that once
    zeroed 1,353 college football season rating changes.
    """
    k = config['k']
    home_advantage = config['home_advantage']
    retention = config['offseason_retention']
    scale = config.get('scale', SCALE)
    model = config.get('margin_model', 'result_only')
    reference = config.get('reference_margin', 1.0)
    exponent = config.get('margin_exponent', 1.0)
    m_min = config.get('margin_min', 0.25)
    m_max = config.get('margin_max', 4.0)
    points_per_elo = config.get('points_per_elo', 0.0)

    ratings, rows, season = {}, [], None
    for game in games:
        if season is not None and game['season'] != season:
            ratings = {team: regress(value, retention, initial=initial)
                       for team, value in ratings.items()}
        season = game['season']
        home, away = game['home_team_key'], game['away_team_key']
        home_rating = ratings.get(home, initial)
        away_rating = ratings.get(away, initial)
        neutral = bool(game['neutral_site'])
        expectation = expected_home(home_rating, away_rating, neutral=neutral,
                                    home_advantage=home_advantage, scale=scale)
        result = outcome(game['home_points'], game['away_points'])
        row = dict(game_key=game['game_key'], season=season,
                   game_date=game.get('game_date'),
                   home_team_key=home, away_team_key=away,
                   neutral_site=1 if neutral else 0,
                   home_pregame=home_rating, away_pregame=away_rating,
                   home_expected=expectation,
                   home_points=game['home_points'],
                   away_points=game['away_points'],
                   outcome=result)
        if result is None:
            # Unplayed: no information, no update. Stored so a caller can see
            # the model declined rather than scored it 0.5 — which is exactly
            # what a tie scores, and the two must never look alike.
            row.update(home_postgame=home_rating, away_postgame=away_rating,
                       rating_change=0.0, counted=0, multiplier=None)
            rows.append(row)
            continue
        margin = abs(game['home_points'] - game['away_points'])
        venue = 0.0 if neutral else home_advantage
        if margin == 0:
            multiplier = 1.0
        else:
            home_won = result == 1.0
            winner_gap = ((home_rating + venue) - away_rating if home_won
                          else away_rating - (home_rating + venue))
            multiplier = margin_multiplier(
                model, margin=margin, elo_diff=winner_gap,
                reference_margin=reference, exponent=exponent, m_min=m_min,
                m_max=m_max, points_per_elo=points_per_elo)
        change = k * multiplier * (result - expectation)
        ratings[home] = home_rating + change
        ratings[away] = away_rating - change
        row.update(home_postgame=ratings[home], away_postgame=ratings[away],
                   rating_change=change, counted=1, multiplier=multiplier)
        rows.append(row)
    return rows, ratings


def _scored(rows):
    scored = [row for row in rows if row['counted']]
    if not scored:
        raise ValueError('No counted game to score')
    return scored


def brier(rows):
    """Mean squared error of the home-win probability over counted games.

    A tie is scored against 0.5, not dropped: a model that called a tied game
    a coin flip was right and should be credited for it.
    """
    scored = _scored(rows)
    return sum((row['home_expected'] - row['outcome']) ** 2
               for row in scored) / len(scored)


def log_loss(rows, *, floor=1e-12):
    scored = _scored(rows)
    total = 0.0
    for row in scored:
        p = min(max(row['home_expected'], floor), 1 - floor)
        total -= row['outcome'] * math.log(p) \
            + (1 - row['outcome']) * math.log(1 - p)
    return total / len(scored)


def accuracy(rows):
    """How often the more likely side won. A tie, or a 0.5 call, counts half."""
    scored = _scored(rows)
    hits = 0.0
    for row in scored:
        if row['home_expected'] == 0.5 or row['outcome'] == 0.5:
            hits += 0.5
        elif (row['home_expected'] > 0.5) == (row['outcome'] == 1.0):
            hits += 1.0
    return hits / len(scored)


def calibration(rows, *, buckets=10):
    """Predicted against observed, in equal-width probability buckets.

    Returns (error, table). The error is the count-weighted mean absolute gap,
    which is what says whether a probability means what it says: a model can
    score well on Brier and still be systematically over-confident.
    """
    scored = _scored(rows)
    bins = [[] for _ in range(buckets)]
    for row in scored:
        index = min(int(row['home_expected'] * buckets), buckets - 1)
        bins[index].append(row)
    table, weighted, total = [], 0.0, 0
    for index, group in enumerate(bins):
        if not group:
            continue
        predicted = sum(row['home_expected'] for row in group) / len(group)
        observed = sum(row['outcome'] for row in group) / len(group)
        table.append(dict(bucket=index / buckets, games=len(group),
                          predicted=predicted, observed=observed))
        weighted += abs(predicted - observed) * len(group)
        total += len(group)
    return weighted / total, table


def score(rows):
    error, table = calibration(rows)
    return dict(games=sum(1 for row in rows if row['counted']),
                brier=brier(rows), log_loss=log_loss(rows),
                accuracy=accuracy(rows), calibration_error=error,
                calibration=table)
