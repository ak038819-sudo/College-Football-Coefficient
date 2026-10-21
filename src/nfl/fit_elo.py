"""Fit NFL Elo on NFL games, then score seasons the search never saw.

Nothing is inherited. The two models this repository already ships are scored
here as baselines, carried over unchanged, so the report says what inheriting
would have cost instead of asserting that it would have cost something.

Two quantities are measured rather than chosen: the mean winning margin, which
normalises every margin multiplier so k keeps one meaning, and points of margin
per point of pregame rating gap, which `mov_adjusted` needs. Both are measured
on the FITTING seasons alone; measuring either on the holdout would leak it.

The holdout is five seasons, not one. An NFL season is 285 games against
college basketball's 5,752, so a single-season holdout would be a measurement
with a wide error bar around it, and the differences between candidate models
here are smaller than that bar.

A SEASON IN PROGRESS IS NEITHER FITTED NOR SCORED. Completeness is read from
the data — a finished season has a Super Bowl in it — rather than from the
calendar.

Read-only. Fetches nothing, writes no database row; `--out` writes the chosen
configuration and the full search to disk when asked.
"""

import argparse
import json
import sys
from pathlib import Path

from .elo import (INITIAL_RATING, SCALE, MARGIN_MODELS, fit_points_per_elo,
                  mean_winning_margin, replay, score)
from .store import connect

# Coarse and wide, and wider than the first pass: that one topped out at
# k = 40 and bottomed out at retention = 0.5, and the search chose both of
# those edges. A parameter chosen at the edge of its grid is not a fitted
# parameter, it is the grid's boundary wearing a fitted parameter's clothes,
# so the grid now extends past where the answer sits. A fit that lands on an
# edge again should widen it again rather than be reported.
K_GRID = (8, 12, 16, 20, 24, 28, 32, 40, 48, 56, 64)
HOME_GRID = (0, 20, 30, 40, 50, 60, 70, 85)
RETENTION_GRID = (0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.9, 1.0)
EXPONENT_GRID = (0.5, 0.75, 1.0)

HOLDOUT_SEASONS = 5

# What this repository's other two models ship, for the comparison. Each is
# its own sport's fitted answer; neither has ever seen an NFL game.
INHERITED = (
    ("college football's parameters, carried over unchanged",
     dict(k=45, home_advantage=50, offseason_retention=0.8,
          margin_model='result_only')),
    ("college basketball's parameters, carried over unchanged",
     dict(k=45, home_advantage=100, offseason_retention=0.8,
          margin_model='mov')),
)


def load_games(connection):
    """Rateable games in kickoff order, oldest first."""
    rows = connection.execute(
        'SELECT game_key, season, game_date, kickoff_utc, neutral_site,'
        '       home_team_key, away_team_key, home_points, away_points'
        '  FROM game WHERE rateable = 1'
        "  ORDER BY game_date, COALESCE(kickoff_utc, ''), provider_id"
    ).fetchall()
    return [dict(row) for row in rows]


def complete_seasons(connection):
    """Seasons that reached a Super Bowl, which is what finished means here."""
    rows = connection.execute(
        "SELECT DISTINCT season FROM game"
        "  WHERE season_type = 'superbowl' AND home_points IS NOT NULL"
        "  ORDER BY season").fetchall()
    return [row['season'] for row in rows]


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
               if row['counted'] and row['season'] in fit_seasons
               and row['outcome'] != 0.5]
    points_per_elo = fit_points_per_elo(counted, home_advantage=50)
    return dict(reference_margin=reference, points_per_elo=points_per_elo,
                fit_games=sum(1 for game in fitting))


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
    first season is mostly the model learning who is who.
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
    """Staged, because the full product is thousands of replays of an archive.

    Stage one sweeps k, home advantage and retention with NO margin layer.
    Stage two adds each margin layer and refits k across the whole grid rather
    than carrying stage one's k over: k only acts through `k * M`, so a k
    fitted without a multiplier means something different once there is one.
    That mistake is on the record in this repository's own football config.
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
    """What there is to beat, including the other two sports' parameters."""
    out = []
    scored = [game for game in games if game['season'] in score_seasons
              and game['home_points'] is not None
              and game['away_points'] is not None]
    outcomes = [1.0 if game['home_points'] > game['away_points']
                else 0.0 if game['home_points'] < game['away_points'] else 0.5
                for game in scored]
    home_rate = sum(outcomes) / len(outcomes)
    for name, probability in (('coin flip', 0.5),
                              (f'home team at its base rate {home_rate:.4f}',
                               home_rate)):
        rows = [dict(counted=1, home_expected=probability, outcome=value)
                for value in outcomes]
        out.append(dict(name=name,
                        **{key: value for key, value in score(rows).items()
                           if key != 'calibration'}))
    for name, parameters in INHERITED:
        config = configuration(parameters['k'], parameters['home_advantage'],
                               parameters['offseason_retention'],
                               parameters['margin_model'], 1.0, measured)
        measurement, _, _ = evaluate(games, config, set(score_seasons))
        out.append(dict(name=name,
                        **{key: value for key, value in measurement.items()
                           if key != 'calibration'}))
    return out, home_rate


def render(report):
    best = report['chosen']
    lines = ['# NFL Elo fit', '',
             f'Fitted on seasons {report["fit_seasons"][0]}-'
             f'{report["fit_seasons"][-1]}'
             f' ({report["measured"]["fit_games"]} rateable games), scored on'
             f' {report["holdout_seasons"][0]}-{report["holdout_seasons"][-1]}'
             f' ({report["holdout"]["games"]} rateable games) which the search'
             ' never saw.', '',
             '## Measured, not chosen', '',
             f'- mean winning margin:'
             f' {report["measured"]["reference_margin"]:.4f} points, which'
             ' normalises every margin multiplier so k keeps one meaning',
             f'- points of margin per point of pregame rating gap:'
             f' {report["measured"]["points_per_elo"]:.6f}', '',
             '## Chosen', '', '| parameter | value |', '| --- | --- |']
    for key in ('k', 'home_advantage', 'offseason_retention', 'margin_model',
                'margin_exponent'):
        lines.append(f'| {key} | {best[key]} |')
    lines += ['', '## Held out', '', '| model | games | Brier | log loss |'
              ' accuracy | calibration error |',
              '| --- | --- | --- | --- | --- | --- |']
    rows = [dict(name='this fit', **report['holdout'])] + report['baselines']
    for row in rows:
        lines.append(f'| {row["name"]} | {row["games"]} | {row["brier"]:.5f} |'
                     f' {row["log_loss"]:.5f} | {row["accuracy"]:.4f} |'
                     f' {row["calibration_error"]:.5f} |')
    lines += ['', '## Calibration on the holdout', '',
              '| bucket | games | predicted | observed |',
              '| --- | --- | --- | --- |']
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


def run(connection, *, holdout_count=HOLDOUT_SEASONS, out=None):
    games = load_games(connection)
    if not games:
        raise ValueError('No rateable game is loaded; run the ingestion first')
    complete = set(complete_seasons(connection))
    if not complete:
        raise ValueError('No completed season is loaded')
    usable = [season for season in seasons_of(games) if season in complete]
    if len(usable) <= holdout_count:
        raise ValueError(f'{holdout_count} holdout seasons need more than '
                         f'{len(usable)} completed seasons')
    holdout_seasons = usable[-holdout_count:]
    fit_seasons = usable[:-holdout_count]
    # The in-progress season is dropped from the replay entirely: its games
    # would shift ratings under the holdout's feet if it came first, and it
    # cannot be scored fairly either way.
    games = [game for game in games if game['season'] in complete]
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
    parser = argparse.ArgumentParser(description='Fit NFL Elo')
    parser.add_argument('--database', default=None, help='Defaults to db/nfl.db')
    parser.add_argument('--holdout-seasons', type=int, default=HOLDOUT_SEASONS)
    parser.add_argument('--out', default=None, help='Write the report as JSON')
    args = parser.parse_args(argv)
    connection = connect(args.database)
    try:
        report = run(connection, holdout_count=args.holdout_seasons,
                     out=args.out)
    except ValueError as exc:
        print(f'Cannot fit: {exc}', file=sys.stderr)
        return 1
    finally:
        connection.close()
    print('\n'.join(render(report)))
    return 0


if __name__ == '__main__':
    sys.exit(main())
