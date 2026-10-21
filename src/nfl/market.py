"""The model against the betting market, season by season.

This is the honest version of "is the rating any good". A held-out Brier
against a coin flip says a model is not noise; a Brier against the closing
moneyline says whether it knows anything the market does not. The market is
the strongest baseline available for an NFL game and it is in the archive
already, so there is no excuse for comparing against a weaker one.

**THE MARKET'S PROBABILITY IS A DE-VIGGED MONEYLINE, NEVER A SPREAD.** A
spread is a margin, and turning a margin into a probability takes a model —
our own, most likely — so a "market Brier" built that way would measure our
curve against itself and come out looking like whatever we chose. A moneyline
IS a probability, once the bookmaker's overround is removed. The cost of
insisting on that is coverage: moneylines are on 72% of the archive overall
and 90%+ from 2007, so every row below states how many games its comparison
rests on, and a season where the two Brier scores are measured on different
games is not reported.

The margin comparison is separate and uses the spread, because there both
sides are margins and no probability model is involved: the model's predicted
margin is `points_per_elo` times the venue-adjusted pregame gap, and
`points_per_elo` is itself a least-squares fit of exactly that relationship
(see `elo.fit_points_per_elo`), so it is the model's own answer rather than a
number chosen to make this table look good.

Against the spread (ATS) is reported as a record and nothing more. It is not
evidence of an edge: 7,397 games across 28 seasons is enough to see a bias and
nowhere near enough to call a 51% ATS rate profitable, and the page says so.
"""

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

from .build_ratings import load_config
from .store import connect

# A pick landing exactly on the line is no pick, and a model margin exactly
# equal to the spread is exactly that case.
PUSH = 'push'


def implied_probability(moneyline):
    """American odds as a probability, before the overround is removed."""
    if moneyline is None:
        return None
    price = float(moneyline)
    if -100 < price < 100:
        raise ValueError(f'{moneyline!r} is not an American price')
    if price < 0:
        return -price / (-price + 100.0)
    return 100.0 / (price + 100.0)


def devig(home_moneyline, away_moneyline):
    """The market's probability the home team wins, overround removed.

    Removed PROPORTIONALLY, which is the standard and the only one that needs
    no further assumption. The two raw probabilities sum to about 1.05 on an
    NFL game; scaling both by the same factor is what makes them a
    distribution again. Returns None unless BOTH prices are present: a single
    price carries the vig with it and there is nothing to divide it out by.
    """
    home = implied_probability(home_moneyline)
    away = implied_probability(away_moneyline)
    if home is None or away is None:
        return None
    total = home + away
    if total <= 0:
        return None
    return home / total


def outcome(home_points, away_points):
    """1.0, 0.5 or 0.0 from the home team's side. None if unplayed."""
    if home_points is None or away_points is None:
        return None
    if home_points > away_points:
        return 1.0
    if home_points < away_points:
        return 0.0
    return 0.5


def model_margin(row, *, points_per_elo, home_advantage):
    """The model's predicted home margin, in points."""
    venue = 0.0 if row['neutral_site'] else home_advantage
    gap = row['home_pregame'] + venue - row['away_pregame']
    return points_per_elo * gap


def ats_result(predicted, spread, actual):
    """Did backing the model's side against the line win, lose or push?

    `spread` is the home team's expected margin, positive when the home team
    is favoured, which is the convention this archive uses — measured, not
    assumed: over the 7,369 games carrying both a line and a final,
    mean(actual - spread) is 0.07, and 4.56 with the sign flipped.
    """
    if predicted == spread:
        return PUSH
    on_home = predicted > spread
    covered = actual - spread
    if covered == 0:
        return PUSH
    home_covered = covered > 0
    return 'win' if on_home == home_covered else 'loss'


def rows_for(connection, model_id):
    """Every finished, counted game with the model's call and the market's."""
    rows = connection.execute(
        'SELECT g.season, g.neutral_site, g.home_points, g.away_points,'
        '       g.spread_line, g.home_moneyline, g.away_moneyline,'
        '       r.home_pregame, r.away_pregame, r.home_expected'
        '  FROM game g JOIN rating_game r'
        '    ON r.game_key = g.game_key AND r.model_id = ?'
        ' WHERE r.counted = 1 AND g.home_points IS NOT NULL'
        '   AND g.away_points IS NOT NULL'
        '  ORDER BY g.season', (model_id,)).fetchall()
    return [dict(row) for row in rows]


def _blank():
    return dict(rated_games=0, model_error=0.0, model_hits=0.0,
                priced_games=0, model_error_priced=0.0, market_error=0.0,
                market_hits=0.0, spread_games=0, ats_wins=0, ats_losses=0,
                ats_pushes=0, model_abs=0.0, market_abs=0.0)


def _hit(probability, result):
    """Half credit where either side is exactly level — the same rule
    `elo.accuracy` uses, so the two numbers are comparable."""
    if probability == 0.5 or result == 0.5:
        return 0.5
    return 1.0 if (probability > 0.5) == (result == 1.0) else 0.0


def seasons(connection, model_id, config):
    elo = config['elo']
    points_per_elo = elo.get('points_per_elo', 0.0)
    home_advantage = elo.get('home_advantage', 0.0)
    buckets = defaultdict(_blank)
    for row in rows_for(connection, model_id):
        bucket = buckets[row['season']]
        result = outcome(row['home_points'], row['away_points'])
        predicted = row['home_expected']
        bucket['rated_games'] += 1
        bucket['model_error'] += (predicted - result) ** 2
        bucket['model_hits'] += _hit(predicted, result)
        market = devig(row['home_moneyline'], row['away_moneyline'])
        if market is not None:
            # Both Brier scores are accumulated on THE SAME games, so the
            # comparison is not the model scored on 285 games against the
            # market scored on 190.
            bucket['priced_games'] += 1
            bucket['model_error_priced'] += (predicted - result) ** 2
            bucket['market_error'] += (market - result) ** 2
            bucket['market_hits'] += _hit(market, result)
        spread = row['spread_line']
        if spread is not None:
            actual = row['home_points'] - row['away_points']
            margin = model_margin(row, points_per_elo=points_per_elo,
                                  home_advantage=home_advantage)
            bucket['spread_games'] += 1
            bucket['model_abs'] += abs(actual - margin)
            bucket['market_abs'] += abs(actual - spread)
            verdict = ats_result(margin, spread, actual)
            if verdict == 'win':
                bucket['ats_wins'] += 1
            elif verdict == 'loss':
                bucket['ats_losses'] += 1
            else:
                bucket['ats_pushes'] += 1
    out = []
    for season in sorted(buckets):
        bucket = buckets[season]
        rated, priced, spread = (bucket['rated_games'],
                                 bucket['priced_games'],
                                 bucket['spread_games'])
        out.append(dict(
            season=season,
            rated_games=rated,
            model_brier=bucket['model_error'] / rated if rated else None,
            model_accuracy=bucket['model_hits'] / rated if rated else None,
            priced_games=priced,
            model_brier_priced=(bucket['model_error_priced'] / priced
                                if priced else None),
            market_brier=bucket['market_error'] / priced if priced else None,
            market_accuracy=bucket['market_hits'] / priced if priced else None,
            spread_games=spread,
            ats_wins=bucket['ats_wins'], ats_losses=bucket['ats_losses'],
            ats_pushes=bucket['ats_pushes'],
            model_margin_mae=bucket['model_abs'] / spread if spread else None,
            market_margin_mae=(bucket['market_abs'] / spread
                               if spread else None)))
    return out


FIELDS = ('season', 'rated_games', 'model_brier', 'model_accuracy',
          'priced_games', 'model_brier_priced', 'market_brier',
          'market_accuracy', 'spread_games', 'ats_wins', 'ats_losses',
          'ats_pushes', 'model_margin_mae', 'market_margin_mae')


def store_seasons(connection, model_id, rows):
    columns = ', '.join(FIELDS) + ', model_id'
    placeholders = ', '.join('?' * (len(FIELDS) + 1))
    updates = ', '.join(f'{name} = excluded.{name}' for name in FIELDS
                        if name != 'season')
    for row in rows:
        connection.execute(
            f'INSERT INTO market_season ({columns}) VALUES ({placeholders})'
            f' ON CONFLICT(season, model_id) DO UPDATE SET {updates}',
            tuple(row[name] for name in FIELDS) + (model_id,))
    connection.commit()
    return len(rows)


def totals(rows):
    """One line for the whole archive, weighted by games rather than seasons.

    Averaging 28 season Brier scores would weight 1999's 267 games the same as
    2026's 93, which is not what "the model's record" means.
    """
    rated = sum(row['rated_games'] for row in rows)
    priced = sum(row['priced_games'] for row in rows)
    spread = sum(row['spread_games'] for row in rows)

    def weighted(field, denominator_field, denominator):
        if not denominator:
            return None
        total = sum((row[field] or 0.0) * row[denominator_field]
                    for row in rows if row[field] is not None)
        return total / denominator

    return dict(
        seasons=len(rows),
        rated_games=rated,
        model_brier=weighted('model_brier', 'rated_games', rated),
        model_accuracy=weighted('model_accuracy', 'rated_games', rated),
        priced_games=priced,
        model_brier_priced=weighted('model_brier_priced', 'priced_games',
                                    priced),
        market_brier=weighted('market_brier', 'priced_games', priced),
        market_accuracy=weighted('market_accuracy', 'priced_games', priced),
        spread_games=spread,
        ats_wins=sum(row['ats_wins'] for row in rows),
        ats_losses=sum(row['ats_losses'] for row in rows),
        ats_pushes=sum(row['ats_pushes'] for row in rows),
        model_margin_mae=weighted('model_margin_mae', 'spread_games', spread),
        market_margin_mae=weighted('market_margin_mae', 'spread_games',
                                   spread))


def build(connection, model_id, *, config=None, config_path=None):
    config = config or load_config(config_path)
    rows = seasons(connection, model_id, config)
    stored = store_seasons(connection, model_id, rows)
    return dict(seasons=stored, rows=rows, totals=totals(rows))


def latest_model(connection):
    row = connection.execute(
        'SELECT model_id FROM rating_model ORDER BY model_id DESC'
        ' LIMIT 1').fetchone()
    if row is None:
        raise ValueError('No rating model is stored; run build_ratings first')
    return row['model_id']


def main(argv=None):
    parser = argparse.ArgumentParser(
        description='Score the NFL model against the betting market')
    parser.add_argument('--database', default=None)
    parser.add_argument('--config', default=None)
    parser.add_argument('--json', default=None,
                        help='also write the per-season rows here')
    args = parser.parse_args(argv)
    connection = connect(args.database)
    try:
        model_id = latest_model(connection)
        summary = build(connection, model_id, config_path=args.config)
    except (OSError, ValueError) as error:
        print(f'Cannot score against the market: {error}', file=sys.stderr)
        return 1
    finally:
        connection.close()
    whole = summary['totals']
    print(f'{whole["seasons"]} seasons, {whole["rated_games"]} rated games')
    print(f'  model  Brier {whole["model_brier"]:.5f}  '
          f'accuracy {whole["model_accuracy"]:.4f}')
    print(f'  on the {whole["priced_games"]} priced games: model '
          f'{whole["model_brier_priced"]:.5f} vs market '
          f'{whole["market_brier"]:.5f}')
    print(f'  margin MAE: model {whole["model_margin_mae"]:.2f} vs spread '
          f'{whole["market_margin_mae"]:.2f} points')
    print(f'  against the spread {whole["ats_wins"]}-{whole["ats_losses"]}-'
          f'{whole["ats_pushes"]}')
    if args.json:
        Path(args.json).write_text(
            json.dumps(dict(seasons=summary['rows'], totals=whole), indent=2),
            encoding='utf-8')
    return 0


if __name__ == '__main__':
    sys.exit(main())
