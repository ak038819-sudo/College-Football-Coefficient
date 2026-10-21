"""Basketball team Elo. Nothing here is inherited from football.

Football's rating engine is tuned for twelve games a season with a
success-rate performance layer behind it. Basketball plays thirty-odd games a
season, has no success-rate feed, and turns its rosters over far harder. So
every parameter in this file is fitted on basketball games and held out on a
basketball season it never saw; `src/ncaambb/fit_elo.py` is that fit and
`config/ncaambb_model.json` is where the result lives.

Provider Elo is never read. `/games` returns `homeTeamEloStart` and
`homeTeamEloEnd`; no loader stores them and nothing here looks at them.

Two design choices worth stating because they are not the obvious ones:

**The margin multiplier is normalised to average 1.** k only ever acts on a
rating through `k * M`, so a margin layer whose M averages 2.4 silently makes k
mean something different from one whose M averages 1.7. Football's config
records that trap costing a real comparison. Here every margin model divides by
the mean winning margin of the fitting data, so k keeps its meaning across all
three and a model comparison is a test of the model.

**Only Division I against Division I updates a rating.** A game against a team
the provider does not list for the season has a real result and an opponent of
unknown strength, so it stays in the schedule and out of the ratings.
"""

import math

INITIAL_RATING = 1500.0
SCALE = 400.0

# Margin models compared in the fit. 'result_only' is the null: who won, by how
# much ignored.
MARGIN_MODELS = ('result_only', 'mov', 'mov_adjusted')


def expected_home(home_rating, away_rating, *, neutral=False,
                  home_advantage=0.0, scale=SCALE):
    """Probability the home team wins, before any game is played.

    A neutral-site game gets no venue term, which is most of a conference
    tournament and about one game in nine of a season.
    """
    edge = 0.0 if neutral else home_advantage
    return 1.0 / (1.0 + 10 ** ((away_rating - (home_rating + edge)) / scale))


def margin_multiplier(model, *, margin, elo_diff, reference_margin,
                      exponent=1.0, m_min=0.25, m_max=4.0,
                      points_per_elo=0.0):
    """How far a result moves a rating, relative to an average win.

    `margin` is the winner's winning margin and is always positive.
    `elo_diff` is the WINNER's venue-adjusted pregame rating minus the
    loser's, so a negative value is an upset. `reference_margin` is the mean
    winning margin measured in the fitting data, which is what makes an
    average win score 1.0 in every model.
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
        # What the pregame gap implied, in points. A favourite that wins by
        # exactly its expected margin tells us nothing new and scores 1.0.
        implied = points_per_elo * elo_diff
        surplus = margin - implied
        # Shifted so an average win against an evenly matched opponent is 1.
        # A heavy favourite can win by far less than implied, which makes the
        # base NEGATIVE, and a negative base raised to a fractional exponent
        # is a complex number rather than an error. Clamp BEFORE the power.
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
    """Points of final margin per point of venue-adjusted pregame rating gap.

    Measured by least squares through the origin on the games given, so
    `mov_adjusted` compares a margin against a basketball number rather than
    one carried over from another sport. Needs pregame ratings, so it runs on
    the output of a replay rather than on raw games.
    """
    numerator = denominator = 0.0
    for game in games:
        gap = game['home_pregame'] + (0.0 if game['neutral_site'] else home_advantage) \
            - game['away_pregame']
        margin = game['home_points'] - game['away_points']
        numerator += gap * margin
        denominator += gap * gap
    if denominator == 0:
        raise ValueError('Every pregame gap is zero; nothing to fit')
    return numerator / denominator


def regress(rating, retention, *, initial=INITIAL_RATING):
    """Carry a rating into the next season, pulled toward the mean.

    Basketball rosters turn over harder than football's, so retention is
    fitted rather than borrowed: see `fit_elo.py`.
    """
    if not 0.0 <= retention <= 1.0:
        raise ValueError('Retention is a fraction between 0 and 1')
    return initial + (rating - initial) * retention


def _decided(game):
    return (game['home_points'] is not None and game['away_points'] is not None
            and game['home_points'] != game['away_points'])


def replay(games, config, *, initial=INITIAL_RATING):
    """Replay games in order, returning the per-game record and final ratings.

    `games` must already be ordered by date. Season boundaries are detected
    from the season field, and a rating is regressed once when the season
    changes, which is why order matters here rather than being a convenience.

    Returns (rows, ratings). Each row carries the PREGAME ratings, the
    expectation and the rating change, so nothing downstream has to recompute
    what the model knew before the game.
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
        row = dict(game_key=game['game_key'], season=season,
                   start_date=game.get('start_date_utc'),
                   home_team_key=home, away_team_key=away,
                   neutral_site=1 if neutral else 0,
                   home_pregame=home_rating, away_pregame=away_rating,
                   home_expected=expectation,
                   home_points=game['home_points'],
                   away_points=game['away_points'])
        if not _decided(game):
            # Unplayed, cancelled or level: no information, no update. Stored
            # so a caller can see the model declined rather than scored 0.5.
            row.update(home_postgame=home_rating, away_postgame=away_rating,
                       rating_change=0.0, counted=0, multiplier=None)
            rows.append(row)
            continue
        home_won = game['home_points'] > game['away_points']
        margin = abs(game['home_points'] - game['away_points'])
        venue = 0.0 if neutral else home_advantage
        winner_gap = ((home_rating + venue) - away_rating if home_won
                      else away_rating - (home_rating + venue))
        multiplier = margin_multiplier(
            model, margin=margin, elo_diff=winner_gap,
            reference_margin=reference, exponent=exponent, m_min=m_min,
            m_max=m_max, points_per_elo=points_per_elo)
        change = k * multiplier * ((1.0 if home_won else 0.0) - expectation)
        ratings[home] = home_rating + change
        ratings[away] = away_rating - change
        row.update(home_postgame=ratings[home], away_postgame=ratings[away],
                   rating_change=change, counted=1, multiplier=multiplier)
        rows.append(row)
    return rows, ratings


def brier(rows):
    """Mean squared error of the home-win probability over counted games."""
    scored = [row for row in rows if row['counted']]
    if not scored:
        raise ValueError('No counted game to score')
    return sum((row['home_expected']
                - (1.0 if row['home_points'] > row['away_points'] else 0.0)) ** 2
               for row in scored) / len(scored)


def log_loss(rows, *, floor=1e-12):
    scored = [row for row in rows if row['counted']]
    if not scored:
        raise ValueError('No counted game to score')
    total = 0.0
    for row in scored:
        p = min(max(row['home_expected'], floor), 1 - floor)
        total -= math.log(p) if row['home_points'] > row['away_points'] \
            else math.log(1 - p)
    return total / len(scored)


def accuracy(rows):
    """How often the more likely side won. A game at exactly 0.5 counts half."""
    scored = [row for row in rows if row['counted']]
    if not scored:
        raise ValueError('No counted game to score')
    hits = 0.0
    for row in scored:
        home_won = row['home_points'] > row['away_points']
        if row['home_expected'] == 0.5:
            hits += 0.5
        elif (row['home_expected'] > 0.5) == home_won:
            hits += 1.0
    return hits / len(scored)


def calibration(rows, *, buckets=10):
    """Predicted against observed, in equal-width probability buckets.

    Returns (error, table). The error is the count-weighted mean absolute gap,
    which is what says whether a probability means what it says — a model can
    score well on Brier and still be systematically over-confident.
    """
    scored = [row for row in rows if row['counted']]
    if not scored:
        raise ValueError('No counted game to score')
    bins = [[] for _ in range(buckets)]
    for row in scored:
        index = min(int(row['home_expected'] * buckets), buckets - 1)
        bins[index].append(row)
    table, weighted, total = [], 0.0, 0
    for index, group in enumerate(bins):
        if not group:
            continue
        predicted = sum(row['home_expected'] for row in group) / len(group)
        observed = sum(1 for row in group
                       if row['home_points'] > row['away_points']) / len(group)
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
