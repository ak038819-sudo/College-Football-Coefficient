"""Fit basketball Elo on basketball games, then score a season it never saw.

The plan's rule is that no football parameter may be inherited without
basketball validation, so this does the validation and reports what inheriting
would have cost. The search chooses on the fitting seasons only; the holdout
season is scored once, at the end, with the chosen configuration.

Two quantities are measured rather than chosen: the mean winning margin, which
normalises every margin multiplier so k keeps one meaning, and points of margin
per point of pregame rating gap, which `mov_adjusted` needs. Both are measured
on the FITTING seasons alone. Measuring either on the holdout would leak it.

Read-only. Fetches nothing, writes no database row; `--out` writes the chosen
configuration and the full search to disk when asked.
"""

import argparse
import json
import sys
from pathlib import Path

from .elo import (INITIAL_RATING, SCALE, MARGIN_MODELS, expected_home,
                  fit_points_per_elo, mean_winning_margin, replay, score)
from .store import connect

# Deliberately wide and coarse. A fine grid on three seasons would be fitting
# noise; the point is to find the region, and to see whether the region is
# anywhere near football's.
K_GRID = (8, 12, 16, 20, 25, 30, 35, 40, 45)
HOME_GRID = (0, 20, 40, 60, 80, 100, 120)
RETENTION_GRID = (0.5, 0.6, 0.7, 0.8, 0.9, 1.0)
EXPONENT_GRID = (0.5, 0.75, 1.0)

# What football ships, for the comparison the plan asks for. Football's own
# performance layer reads a success-rate feed basketball does not have, so the
# honest transfer is its k, home field and retention with no margin layer.
FOOTBALL = dict(k=45, home_advantage=50, offseason_retention=0.8,
                margin_model='result_only')


def load_games(connection, *, rateable_only=True):
    """Rateable games in date order, oldest first.

    Ordered by `start_date_utc`, never by anything week-shaped: the football
    archive taught this project that a provider can number a postseason game
    week 1 and put a title game first.
    """
    where = 'WHERE rateable = 1' if rateable_only else ''
    rows = connection.execute(
        'SELECT game_key, season, start_date_utc, neutral_site, home_team_key,'
        '       away_team_key, home_points, away_points, status, rateable'
        f'  FROM game {where} ORDER BY start_date_utc, game_key').fetchall()
    return [dict(row) for row in rows]


def seasons_of(games):
    return sorted({game['season'] for game in games})


def measure(games, fit_seasons):
    """The two measured quantities, from the fitting seasons only."""
    fitting = [game for game in games if game['season'] in fit_seasons]
    reference = mean_winning_margin(fitting)
    # points_per_elo needs pregame ratings, so it takes a first pass with no
    # margin layer at all. A margin layer here would make the measurement
    # depend on the thing it is measured for.
    rows, _ = replay(games, dict(k=20, home_advantage=50,
                                 offseason_retention=0.8,
                                 margin_model='result_only'))
    counted = [row for row in rows
               if row['counted'] and row['season'] in fit_seasons]
    points_per_elo = fit_points_per_elo(counted, home_advantage=50)
    return dict(reference_margin=reference, points_per_elo=points_per_elo,
                fit_games=len(counted))


def configuration(k, home_advantage, retention, model, exponent, measured):
    return dict(initial_rating=INITIAL_RATING, scale=SCALE, k=k,
                home_advantage=home_advantage,
                offseason_retention=retention, margin_model=model,
                margin_exponent=exponent, margin_min=0.25, margin_max=4.0,
                reference_margin=measured['reference_margin'],
                points_per_elo=measured['points_per_elo'])


def evaluate(games, config, score_seasons):
    """Replay everything, score only the seasons asked for.

    Every season before the scored ones is burn-in: ratings start level, so a
    first season is mostly the model learning who is who, and scoring it would
    measure the burn-in rather than the model.
    """
    rows, ratings = replay(games, config)
    scored = [row for row in rows if row['season'] in score_seasons]
    return score(scored), rows, ratings


def _grid_point(games, fit_seasons, measured, k, home, retention, model,
                exponent):
    config = configuration(k, home, retention, model, exponent, measured)
    measurement, _, _ = evaluate(games, config, set(fit_seasons))
    return dict(k=k, home_advantage=home, offseason_retention=retention,
                margin_model=model, margin_exponent=exponent,
                **{key: value for key, value in measurement.items()
                   if key != 'calibration'})


def search(games, fit_seasons, measured, *, models=MARGIN_MODELS,
           k_grid=K_GRID, home_grid=HOME_GRID, retention_grid=RETENTION_GRID,
           exponent_grid=EXPONENT_GRID):
    """Staged, because the full product is thousands of replays of a season.

    Stage one sweeps k, home advantage and retention with NO margin layer.
    Stage two adds each margin layer and **refits k across the whole grid**
    rather than carrying stage one's k over: k only acts through `k * M`, so a
    k fitted without a multiplier means something different once there is one,
    and keeping it would make the comparison a test of k rather than of the
    layer. That mistake is on the record in football's own config.

    Home advantage and retention are narrowed to stage one's best few, which
    is the one shortcut here: both are close to orthogonal to the margin
    layer, and sweeping everything jointly costs an order of magnitude.

    Returns every point scored, lowest Brier first.
    """
    stage_one = [_grid_point(games, fit_seasons, measured, k, home, retention,
                             'result_only', 1.0)
                 for k in k_grid for home in home_grid
                 for retention in retention_grid]
    stage_one.sort(key=lambda row: (row['brier'], row['log_loss']))
    homes, retentions = [], []
    for row in stage_one:
        if row['home_advantage'] not in homes and len(homes) < 3:
            homes.append(row['home_advantage'])
        if row['offseason_retention'] not in retentions and len(retentions) < 2:
            retentions.append(row['offseason_retention'])
    stage_two = [_grid_point(games, fit_seasons, measured, k, home, retention,
                             model, exponent)
                 for model in models if model != 'result_only'
                 for exponent in exponent_grid
                 for k in k_grid for home in homes for retention in retentions]
    results = stage_one + stage_two
    results.sort(key=lambda row: (row['brier'], row['log_loss']))
    return results


def baselines(games, score_seasons, measured):
    """What there is to beat, including football's own parameters."""
    out = []
    scored = [game for game in games if game['season'] in score_seasons
              and game['home_points'] is not None
              and game['away_points'] is not None
              and game['home_points'] != game['away_points']]
    home_rate = sum(1 for game in scored
                    if game['home_points'] > game['away_points']) / len(scored)
    for name, probability in (('coin flip', 0.5),
                              (f'home team at its base rate {home_rate:.4f}',
                               home_rate)):
        rows = [dict(counted=1, home_expected=probability,
                     home_points=game['home_points'],
                     away_points=game['away_points']) for game in scored]
        out.append(dict(name=name,
                        **{key: value for key, value in score(rows).items()
                           if key != 'calibration'}))
    football = configuration(FOOTBALL['k'], FOOTBALL['home_advantage'],
                             FOOTBALL['offseason_retention'],
                             FOOTBALL['margin_model'], 1.0, measured)
    measurement, _, _ = evaluate(games, football, set(score_seasons))
    out.append(dict(name="football's parameters, carried over unchanged",
                    **{key: value for key, value in measurement.items()
                       if key != 'calibration'}))
    return out, home_rate


def render(report):
    best = report['chosen']
    lines = ['# Basketball Elo fit', '',
             f'Fitted on seasons {report["fit_seasons"]}'
             f' ({report["measured"]["fit_games"]} rateable games),'
             f' scored on {report["holdout_seasons"]}'
             f' ({report["holdout"]["games"]} rateable games) which the search'
             ' never saw.', '',
             '## Measured, not chosen', '',
             f'- mean winning margin: {report["measured"]["reference_margin"]:.4f}'
             ' points, which normalises every margin multiplier so k keeps one'
             ' meaning',
             f'- points of margin per point of pregame rating gap:'
             f' {report["measured"]["points_per_elo"]:.6f}', '',
             '## Chosen', '', '| parameter | value |', '| --- | --- |']
    for key in ('k', 'home_advantage', 'offseason_retention', 'margin_model',
                'margin_exponent'):
        lines.append(f'| {key} | {best[key]} |')
    lines += ['', '## Held out', '', '| model | games | Brier | log loss |'
              ' accuracy | calibration error |', '| --- | --- | --- | --- |'
              ' --- | --- |']
    rows = [dict(name='this fit', **report['holdout'])] + report['baselines']
    for row in rows:
        lines.append(f'| {row["name"]} | {row["games"]} | {row["brier"]:.5f} |'
                     f' {row["log_loss"]:.5f} | {row["accuracy"]:.4f} |'
                     f' {row["calibration_error"]:.5f} |')
    lines += ['', '## Calibration on the holdout', '',
              '| bucket | games | predicted | observed |', '| --- | --- | --- | --- |']
    for row in report['holdout_calibration']:
        lines.append(f'| {row["bucket"]:.1f} | {row["games"]} |'
                     f' {row["predicted"]:.4f} | {row["observed"]:.4f} |')
    lines += ['', '## The top of the search, on the fitting seasons', '',
              '| k | home | retention | margin model | exponent | Brier |',
              '| --- | --- | --- | --- | --- | --- |']
    for row in report['search'][:12]:
        lines.append(f'| {row["k"]} | {row["home_advantage"]} |'
                     f' {row["offseason_retention"]} | {row["margin_model"]} |'
                     f' {row["margin_exponent"]} | {row["brier"]:.5f} |')
    return lines


def run(connection, *, holdout=None, out=None):
    games = load_games(connection)
    if not games:
        raise ValueError('No rateable game is loaded; run the ingestion first')
    available = seasons_of(games)
    if len(available) < 2:
        raise ValueError(f'A holdout needs at least two seasons, found {available}')
    holdout_seasons = [holdout] if holdout is not None else [available[-1]]
    fit_seasons = [season for season in available
                   if season not in holdout_seasons]
    if not fit_seasons:
        raise ValueError('Every loaded season is the holdout')
    measured = measure(games, set(fit_seasons))
    results = search(games, fit_seasons, measured)
    best = results[0]
    chosen = configuration(best['k'], best['home_advantage'],
                           best['offseason_retention'], best['margin_model'],
                           best['margin_exponent'], measured)
    measurement, rows, ratings = evaluate(games, chosen, set(holdout_seasons))
    bases, home_rate = baselines(games, holdout_seasons, measured)
    report = dict(fit_seasons=fit_seasons, holdout_seasons=holdout_seasons,
                  measured=measured, search=results,
                  chosen={**chosen, **{key: best[key] for key in
                                       ('k', 'home_advantage',
                                        'offseason_retention', 'margin_model',
                                        'margin_exponent')}},
                  holdout={key: value for key, value in measurement.items()
                           if key != 'calibration'},
                  holdout_calibration=measurement['calibration'],
                  home_win_rate=home_rate, baselines=bases)
    if out is not None:
        Path(out).parent.mkdir(parents=True, exist_ok=True)
        Path(out).write_text(json.dumps(report, indent=2, sort_keys=True),
                             encoding='utf-8')
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', default=None, help='Defaults to db/ncaambb.db')
    parser.add_argument('--holdout', type=int, default=None,
                        help='Ending year to score; defaults to the latest loaded')
    parser.add_argument('--out', default=None,
                        help='Write the full report as JSON')
    args = parser.parse_args(argv)
    connection = connect(args.database)
    try:
        report = run(connection, holdout=args.holdout, out=args.out)
    except ValueError as exc:
        print(f'Cannot fit: {exc}', file=sys.stderr)
        return 1
    finally:
        connection.close()
    print('\n'.join(render(report)))
    return 0


if __name__ == '__main__':
    sys.exit(main())
