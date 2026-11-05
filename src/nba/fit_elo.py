"""Fit NBA Elo on NBA games, then score seasons the search never saw.

Nothing is inherited. The three models this repository already ships are
scored here as baselines, carried over unchanged, so the report says what
inheriting would have cost instead of asserting that it would have cost
something.

Two quantities are measured rather than chosen: the mean winning margin,
which normalises every margin multiplier so k keeps one meaning, and points
of margin per point of pregame rating gap, which `mov_adjusted` needs. Both
are measured on the FITTING seasons alone; measuring either on the holdout
would leak it.

The holdout is five seasons, about 6,500 games. An NBA season is 1,300 games
against the NFL's 285, so one season would already be a wider measurement
than the NFL's five — but the differences between candidate models are small,
and five seasons also spreads the measurement across more than one era of how
the league plays.

A SEASON IN PROGRESS IS NEITHER FITTED NOR SCORED, and neither is one whose
playoffs have not finished. Completeness is read from the data — a complete
season has postseason games and none of them is still unplayed — rather than
from the calendar. It is not "every game was played": 12 games of 2019-20 and
32 of 2020-21 were cancelled outright and never will be.

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

# Coarse and wide. A parameter chosen at the EDGE of its grid is not a fitted
# parameter, it is the grid's boundary wearing a fitted parameter's clothes,
# so a fit that lands on an edge widens the grid and runs again rather than
# being reported. The NFL fit had to do exactly that, twice.
K_GRID = (8, 12, 16, 20, 24, 28, 32, 40, 48, 56, 64)
# Wider at the top than any other league's grid. College basketball fitted to
# 100 against football's 50, and a possession is worth about a point with a
# great many of them in an NBA game, so the answer may well sit above 100 —
# a grid that stopped there could not say so.
HOME_GRID = (0, 20, 40, 60, 80, 100, 120, 140, 160)
RETENTION_GRID = (0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.9, 1.0)
EXPONENT_GRID = (0.5, 0.75, 1.0)

HOLDOUT_SEASONS = 5

# The seed replay that `measure` uses to get pregame ratings. Its values are
# a plausible starting point, not a result: they affect only the measured
# points_per_elo, and then only slightly, because that is a slope fitted
# through a cloud of games rather than a quantity read off one.
SEED = dict(k=20, home_advantage=100, offseason_retention=0.8,
            margin_model='result_only')

# What this repository's other three models ship, for the comparison. Each is
# its own sport's fitted answer; none has ever seen an NBA game.
INHERITED = (
    ("college football's parameters, carried over unchanged",
     dict(k=45, home_advantage=50, offseason_retention=0.8,
          margin_model='result_only')),
    ("college basketball's parameters, carried over unchanged",
     dict(k=45, home_advantage=100, offseason_retention=0.8,
          margin_model='mov')),
    ("the NFL's parameters, carried over unchanged",
     dict(k=48, home_advantage=50, offseason_retention=0.5,
          margin_model='mov')),
)


def load_games(connection):
    """Rateable games in tip-off order, oldest first."""
    rows = connection.execute(
        'SELECT game_key, season, season_type, game_date, tipoff_utc,'
        '       neutral_site, home_team_key, away_team_key, home_points,'
        '       away_points'
        '  FROM game WHERE rateable = 1'
        "  ORDER BY game_date, COALESCE(tipoff_utc, ''), provider_id"
    ).fetchall()
    return [dict(row) for row in rows]


def complete_seasons(connection):
    """Seasons whose playoffs ran to their end, read from the data.

    The test is that the LAST postseason game by date was played. Two
    simpler tests both get this wrong, and each drops a real season:

    - "every game in the season was played" — 12 games of 2019-20 and 32 of
      2020-21 were cancelled outright and never made up, so this would call
      two real seasons incomplete forever.
    - "no postseason game is outstanding" — 2019-20 carries NINE abandoned
      postseason rows dated 26-28 August 2020, the games called off by the
      players' boycott. They were replayed as new rows with new ids, and
      ESPN left the originals behind marked not completed. The 2020 playoffs
      plainly finished, in October, but this test would silently drop the
      whole season from the fit.

    The abandoned rows are harmless to the replay — not completed means not
    rateable — but they are exactly the kind of row that quietly removes a
    season from a measurement.
    """
    rows = connection.execute(
        "SELECT season, completed FROM game g"
        " WHERE season_type = 'postseason'"
        "   AND game_date = (SELECT max(game_date) FROM game x"
        "                     WHERE x.season = g.season"
        "                       AND x.season_type = 'postseason')"
        " GROUP BY season, completed").fetchall()
    finished = {}
    for row in rows:
        # A date can hold more than one game; the season is complete if any
        # game on that last day was played.
        finished[row['season']] = finished.get(row['season'], False) \
            or bool(row['completed'])
    return sorted(season for season, done in finished.items() if done)


def seasons_of(games):
    return sorted({game['season'] for game in games})


def measure(games, fit_seasons):
    """The two measured quantities, from the fitting seasons only."""
    fitting = [game for game in games if game['season'] in fit_seasons]
    reference = mean_winning_margin(fitting)
    # points_per_elo needs pregame ratings, so it takes a first pass with no
    # margin layer at all. A margin layer here would make the measurement
    # depend on the thing it is measured for.
    rows, _ = replay(games, dict(SEED))
    counted = [row for row in rows
               if row['counted'] and row['season'] in fit_seasons]
    points_per_elo = fit_points_per_elo(
        counted, home_advantage=SEED['home_advantage'])
    return dict(reference_margin=reference, points_per_elo=points_per_elo,
                fit_games=len(fitting))


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


def at_grid_edge(chosen):
    """Which chosen parameters sit on the boundary of their own grid.

    A parameter at the edge means the search was stopped by the grid, not by
    the data, so the honest response is to widen and run again. Reported
    rather than silently accepted.
    """
    edges = []
    for name, grid in (('k', K_GRID), ('home_advantage', HOME_GRID),
                       ('offseason_retention', RETENTION_GRID)):
        value = chosen[name]
        if value in (min(grid), max(grid)):
            edges.append(name)
    return edges


def search(games, fit_seasons, measured, *, models=MARGIN_MODELS,
           k_grid=K_GRID, home_grid=HOME_GRID, retention_grid=RETENTION_GRID,
           exponent_grid=EXPONENT_GRID):
    """Staged, because the full product is thousands of replays of an archive.

    Stage one sweeps k, home advantage and retention with NO margin layer.
    Stage two adds each margin layer and refits k across the whole grid
    rather than carrying stage one's k over: k only acts through `k * M`, so
    a k fitted without a multiplier means something different once there is
    one. That mistake is on the record in this repository's football config.
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
    """What there is to beat, including the other three sports' parameters."""
    out = []
    scored = [game for game in games if game['season'] in score_seasons
              and game['home_points'] is not None
              and game['away_points'] is not None]
    outcomes = [1.0 if game['home_points'] > game['away_points'] else 0.0
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
    lines = ['# NBA Elo fit', '',
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
    if report['grid_edges']:
        lines += ['', f'**At the edge of its grid: '
                  f'{", ".join(report["grid_edges"])}.** A parameter the grid '
                  'stopped is not a fitted parameter. Widen and run again '
                  'before trusting this.']
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
                  grid_edges=at_grid_edge(best),
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
    parser = argparse.ArgumentParser(description='Fit NBA Elo')
    parser.add_argument('--database', default=None,
                        help='Defaults to db/nba.db')
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
