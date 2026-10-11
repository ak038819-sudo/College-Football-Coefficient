#!/usr/bin/env python3
"""
Does returning production predict the offseason better than one constant does?

THE QUESTION. `offseason_retention` in config/model_config.json is 0.8 for
every program in every year: a team keeps 80% of its distance from 1500 across
an offseason whether it returns its quarterback or loses twenty starters.
CFBD publishes returning production from 2014. This script asks whether
letting the rate depend on it lowers out-of-sample error, and prints a
recommendation. It NEVER edits the config -- that is the repository's
convention for every calibrator here, and the point of it is that a number
only moves when a person decides it should.

THE FORM FITTED. One line, two parameters:

    rate(team, season) = base + slope * (returning(season) - pivot)

clipped to [0, 1]. `pivot` is the median returning production of that SEASON,
not of the whole archive, which matters more than it looks: the median has
fallen from 0.60 in 2014 to 0.36 in 2025 as the transfer portal opened, so a
fixed pivot would quietly turn `base` into a decade trend and read that trend
as a returning-production effect. Centring per season asks the narrower and
honest question -- does a team returning MORE than its contemporaries hold its
rating better than one returning less.

Two parameters is the ceiling, not a simplification. 13 offseasons is the
whole of CFBD's coverage, so there are about 1,700 team-seasons and far fewer
effectively independent observations; a curve with more shape than a line
would fit this sample's noise and not the next one's.

WHAT IS SCORED. Early-season games only, from 2014 on. An offseason
assumption is a claim about what a team's rating should be BEFORE the season
re-measures it, and by November the games themselves have overwritten it. So
the metric is Brier over games where both teams are inside their first
`--early-games` of the season. Scoring the full schedule would bury the
effect under thousands of games the parameter barely touches -- and that is
how a real improvement gets reported as a rounding error.

AND IT IS SCORED ON SEASONS THE FIT NEVER SAW. Elo's walk-forward property
makes every GAME prediction out of sample -- an expectation is formed from
pregame ratings only. It says nothing about the PARAMETERS, which this script
chooses. A two-parameter curve picked on all 13 seasons and graded on those
same 13 seasons flatters itself, and this one did: a league-level era trend
scored +0.13% in sample and -1.44% on held-out seasons, with the fitted slope
reversing sign between the two. That is what fitting noise looks like, and
only a holdout shows it. So the parameters are fitted on `--train-through`
and the number that decides anything is the held-out one.

The comparison is always against the SAME machinery with slope forced to
zero, never against the config's current constant. A sloped model and a plain
constant differ in the 34 pre-2014 seasons too -- those can have no
team-specific rate -- so comparing to the constant would credit returning
production with whatever changing three decades of untouched history does.

Usage:
    python src/calibrate_returning_retention.py
    python src/calibrate_returning_retention.py --early-games 3 --out /tmp/grid.csv
"""
from __future__ import annotations

import argparse
import csv
import glob
import sqlite3
import statistics
from pathlib import Path

import team_identity
from build_elo import fetch_games_chronological, load_config, run_elo
from validation import brier_score as brier_of_pairs

RAW_GLOB = 'data/raw/returning_production_*.csv'
FIRST_SEASON = 2014
# Returning production is a share, but CFBD's can land outside [0, 1]: a player
# back from a redshirt adds production that was not in last season's total, and
# returning players with negative PPA make it negative. 2021 holds a -3.100.
# Those are real values, not corrupt ones, but a single team at -3 would swing
# a least-squares fit, so the PREDICTOR is clipped while the rate is clipped
# separately. Clipping here is a statement about leverage, not about validity.
PREDICTOR_FLOOR = -0.5
PREDICTOR_CEILING = 1.5


def load_returning(conn, pattern=RAW_GLOB):
    """{(team_id, season): percent_ppa}, resolved through the team registry.

    An unresolved school FAILS rather than being skipped. A bare name lookup
    with `continue` on a miss is this repository's recurring silent-drop shape
    (it cost six conference memberships in 2026); a team quietly absent here
    would just fall back to the flat rate and make the fit look weaker than it
    is, which is the same bug wearing a different hat.
    """
    cur = conn.cursor()
    out, unresolved = {}, set()
    for path in sorted(glob.glob(pattern)):
        for row in csv.DictReader(open(path, encoding='utf-8')):
            if row['percent_ppa'] == '':
                continue
            name = team_identity.resolve_database_name(cur, row['team'])
            if name is None:
                unresolved.add(row['team'])
                continue
            team_id = cur.execute(
                'SELECT team_id FROM teams WHERE team_name=?', (name,)).fetchone()
            if team_id is None:
                unresolved.add(row['team'])
                continue
            out[(team_id[0], int(row['season_year']))] = float(row['percent_ppa'])
    if unresolved:
        raise ValueError(
            'Returning production names this project cannot resolve to a team: '
            + ', '.join(sorted(unresolved))
            + '. Add them to the team registry rather than dropping them.')
    return out


def season_pivots(returning):
    """Median returning production per season -- the per-season centre."""
    by_season: dict = {}
    for (_team, season), value in returning.items():
        by_season.setdefault(season, []).append(value)
    return {season: statistics.median(values) for season, values in by_season.items()}


def make_provider(returning, pivots, flat, base, slope):
    """A `retention_for_team` for run_elo: the fitted line, else the flat rate.

    A season CFBD does not cover, and a team it has no row for, both get `flat`.
    That is what keeps this honest about its own reach: 34 of the 47 seasons in
    this archive predate 2014 and cannot be given a team-specific rate at all.
    """
    def provider(team_id, season):
        value = returning.get((team_id, season))
        if value is None or season not in pivots:
            return flat
        centred = min(max(value, PREDICTOR_FLOOR), PREDICTOR_CEILING) - pivots[season]
        return min(max(base + slope * centred, 0.0), 1.0)
    return provider


def early_game_ids(games, limit):
    """Games where BOTH teams are inside their first `limit` of the season.

    Both, not either: a game between a week-two team and a week-ten team is
    half a test of the offseason and half a test of ten weeks of results.
    """
    played: dict = {}
    chosen = set()
    season = None
    for g in games:
        if g['season_year'] != season:
            season, played = g['season_year'], {}
        home, away = g['home_team_id'], g['away_team_id']
        if played.get(home, 0) < limit and played.get(away, 0) < limit:
            chosen.add(g['game_id'])
        played[home] = played.get(home, 0) + 1
        played[away] = played.get(away, 0) + 1
    return chosen


def brier_on(games, cfg, scored_ids, provider=None):
    """Brier over `scored_ids` only, from the home team's row.

    The engine always runs over EVERY game: a rating carried into 2014 is the
    product of every season before it, so scoring a subset must never mean
    simulating a subset.
    """
    rows, _, _ = run_elo(games, cfg, retention_for_team=provider)
    lookup = {g['game_id']: g for g in games}
    pairs = []
    for row in rows:
        game_id, team_id, _, _, expectation, *_ = row
        game = lookup[game_id]
        if team_id != game['home_team_id'] or game_id not in scored_ids:
            continue
        home, away = game['home_score'], game['away_score']
        pairs.append((expectation, 1.0 if home > away else 0.0 if away > home else 0.5))
    return brier_of_pairs(pairs), len(pairs)


def fit(games, cfg, maker, scored, bases, slopes):
    """Lowest-Brier (base, slope) over a grid, on one set of games."""
    best = None
    for base in bases:
        for slope in slopes:
            score, _ = brier_on(games, cfg, scored, maker(base, slope))
            if best is None or score < best[2]:
                best = (base, slope, score)
    return best


BASES = [round(0.60 + 0.05 * i, 2) for i in range(9)]
TEAM_SLOPES = [round(-0.40 + 0.05 * i, 2) for i in range(17)]
ERA_SLOPES = [round(-1.0 + 0.1 * i, 1) for i in range(21)]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--db', default='db/league.db')
    parser.add_argument('--config', default='config/model_config.json')
    parser.add_argument('--early-games', type=int, default=4,
                        help="how many of each team's first games count as early (default 4)")
    parser.add_argument('--train-through', type=int, default=2021,
                        help='last season the fit may see; later seasons are the holdout')
    parser.add_argument('--out', default='data/processed/returning_retention_grid.csv')
    args = parser.parse_args(argv)

    cfg = load_config(args.config)['elo']
    flat = cfg['offseason_retention']

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    games = fetch_games_chronological(conn)
    returning = load_returning(conn)
    conn.close()

    pivots = season_pivots(returning)
    early = early_game_ids(games, args.early_games)

    def span(predicate):
        return {g['game_id'] for g in games
                if g['game_id'] in early and predicate(g['season_year'])}

    train = span(lambda y: FIRST_SEASON <= y <= args.train_through)
    test = span(lambda y: y > args.train_through)
    if not train or not test:
        parser.error('--train-through leaves one side of the split empty')

    def team_maker(base, slope):
        return make_provider(returning, pivots, flat, base, slope)

    def era_maker(base, slope):
        """Retention tracks the LEAGUE's median continuity that season.

        A separate hypothesis from the per-team one, and the reason the
        per-team predictor is centred per season: the median has fallen from
        0.604 in 2014 to 0.362 in 2025, so the two questions would otherwise
        be tangled together.
        """
        def provider(_team_id, season):
            if season not in pivots:
                return flat
            return min(max(base + slope * (pivots[season] - 0.60), 0.0), 1.0)
        return provider

    print(f'{len(returning)} team-seasons of returning production, '
          f'{min(pivots)}-{max(pivots)}')
    print(f'Fitting on {FIRST_SEASON}-{args.train_through} ({len(train)} early games), '
          f'scoring on {args.train_through + 1}+ ({len(test)} early games), '
          f'of {len(games)} simulated.')
    print('Season pivots: ' + ', '.join(f'{s}:{p:.3f}' for s, p in sorted(pivots.items())))

    results = []
    for label, maker, slopes in (('per-team returning production', team_maker, TEAM_SLOPES),
                                 ('league-wide era trend', era_maker, ERA_SLOPES)):
        zero = fit(games, cfg, maker, train, BASES, [0.0])
        best = fit(games, cfg, maker, train, BASES, slopes)
        zero_test, _ = brier_on(games, cfg, test, maker(zero[0], zero[1]))
        best_test, _ = brier_on(games, cfg, test, maker(best[0], best[1]))
        gain = zero_test - best_test
        results.append({'form': label, 'base': best[0], 'slope': best[1],
                        'train_brier': best[2], 'test_brier': best_test,
                        'flat_base': zero[0], 'flat_train_brier': zero[2],
                        'flat_test_brier': zero_test, 'held_out_gain': gain})
        print(f'\n{label}')
        print(f'  slope 0 (a refitted constant): base={zero[0]:.2f}  '
              f'train {zero[2]:.7f}  held out {zero_test:.7f}')
        print(f'  best fitted:                  base={best[0]:.2f} slope={best[1]:+.2f}  '
              f'train {best[2]:.7f}  held out {best_test:.7f}')
        print(f'  HELD-OUT GAIN: {gain:+.7f} ({gain / zero_test * 100:+.3f}%)')

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, 'w', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=list(results[0]))
        writer.writeheader()
        writer.writerows(results)
    print(f'\nWrote {len(results)} rows to {args.out}')

    # For scale: what a WRONG constant costs on the same games, so a gain can
    # be read against something. Without this a reader cannot tell a small
    # improvement from no improvement.
    reference = {}
    for base in (0.60, 0.80, 1.00):
        score, _ = brier_on(games, cfg, test, lambda t, s, b=base: b)
        reference[base] = score
    print('\nFor scale, held-out Brier with one constant everywhere:')
    for base, score in reference.items():
        print(f'  retention {base:.2f}: {score:.7f}')
    spread = max(reference.values()) - min(reference.values())
    print(f'  spread across that range: {spread:.7f}')

    winner = max(results, key=lambda r: r['held_out_gain'])
    if winner['held_out_gain'] <= 0:
        print('\nRECOMMENDATION: nothing beats a constant on seasons it was not '
              'fitted on. Leave config/model_config.json alone.')
    elif winner['held_out_gain'] < spread / 10:
        print(f"\nRECOMMENDATION: the best form ({winner['form']}) gains "
              f"{winner['held_out_gain']:.7f} held out, under a tenth of what "
              'getting the constant right is worth. Not worth a piecewise model '
              'that only 2014+ can use. Leave the config alone.')
    else:
        print(f"\nRECOMMENDATION: {winner['form']} gains "
              f"{winner['held_out_gain']:.7f} held out. Worth reviewing against "
              'the cost of a piecewise model. This script changes nothing.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
