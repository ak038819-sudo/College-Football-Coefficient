# Returning production and the offseason

**Measured 2026-10-11. The answer is no, and the config does not change.**

## The question

`config/model_config.json` regresses every team across every offseason by one
constant:

```
new = initial_rating + offseason_retention * (old - initial_rating)
```

with `offseason_retention` = 0.8. A team returning its quarterback, its line
and its secondary regresses toward average by exactly as much as one that lost
twenty starters. CFBD publishes returning production, so the question is
whether letting that rate depend on it predicts early-season games better.

Run it yourself:

```
python src/calibrate_returning_retention.py
```

It prints a recommendation and writes a grid. Like every calibrator here, it
never edits the config.

## What the data is

`/player/returning`, backfilled by `.github/workflows/backfill-returning-production.yml`
into `data/raw/returning_production_<season>.csv`.

- **Coverage is 2014-2026.** Every season from 2001 to 2013 returns zero rows.
  So 34 of the 47 seasons in this archive can have no team-specific rate, and
  any adoption would make the model piecewise.
- **It is offense only.** The fields are passing, rushing and receiving PPA
  and usage. There is no defensive returning production in this endpoint, so
  the best case was ever only a better estimate for part of a team.
- CFBD's `year` is the season production returns **into**, which is the
  offseason that would consume it. The fetcher fails rather than renumber a
  row that disagrees.
- Values fall outside [0, 1] legitimately: 2026 Air Force returns 1.017 of its
  passing production (a player back from a redshirt adds production that was
  not in last year's total) and 2021 holds a -3.100.

## What was fitted

Two forms, each two parameters, each clipped to a usable rate:

1. **Per-team**: `rate = base + slope * (returning - that season's median)`.
   Centred per season deliberately -- see the trend below.
2. **League-wide era trend**: `rate = base + slope * (that season's median - 0.60)`,
   the same rate for everyone in a season.

Scored on **early-season games only** -- those where both teams are inside
their first four of the season. An offseason assumption is a claim about a
rating before the season re-measures it; by November the games have overwritten
it, and scoring the full schedule would bury the effect under thousands of
games the parameter barely touches.

The baseline is always the **same machinery with slope forced to zero**, never
the config's current constant. A sloped model and a plain constant also differ
across the 34 pre-2014 seasons, so comparing against the constant would credit
returning production with whatever rewriting three decades of history does.

## The result

Fitted on 2014-2021, scored on 2022-2026, which the fit never saw:

| Form | Held-out Brier | vs. slope 0 |
| --- | ---: | ---: |
| slope 0 (refitted constant, base 0.80) | 0.1854222 | — |
| per-team returning production (base 0.80, slope +0.10) | 0.1853840 | **+0.0000381** |
| league-wide era trend (base 0.85, slope -1.00) | 0.1880946 | **-0.0026724** |

For scale, on the same held-out games, one constant everywhere:

| retention | Brier |
| --- | ---: |
| 0.60 | 0.1868709 |
| 0.80 | 0.1854222 |
| 1.00 | 0.1848465 |

So getting the constant roughly right is worth about 0.0020 of Brier, and
returning production is worth 0.0000381 beyond it — under **two percent** of
that. It is indistinguishable from nothing.

The direction is at least sensible: the fitted slope is positive in every
window and for every predictor tried (`percent_ppa`, `usage`,
`percent_passing_ppa`), meaning a team returning more than its contemporaries
does hold its rating slightly better. The effect is real and it is far too
small to pay for a piecewise model.

## The era trend is noise, and only the holdout showed it

Fitted and scored on all 13 seasons, the league-wide trend looked like the
better idea: **+0.13%**, three times the per-team effect, with a sensible story
behind it (continuity has collapsed, so teams should carry less of last year).
Split the seasons and it collapses to **-1.44%**, and the fitted slope flips
from +0.7 to -1.0 depending on which seasons it sees.

That is what fitting noise looks like. Thirteen points is not enough to fit a
league-level time trend, and Elo's walk-forward property does not protect
against it: that property makes every *game* prediction out of sample, and says
nothing about *parameters* chosen using every season they are scored on. The
calibrator therefore splits train from test, and its recommendation reads the
held-out column.

## The finding that is real

Returning production did not improve the model, but it measured something
worth knowing. Median returning production by season:

| 2014 | 2015 | 2016 | 2017 | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 | 2025 | 2026 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| .604 | .611 | .659 | .613 | .637 | .595 | .631 | .742 | .594 | .523 | .466 | .362 | .395 |

The average FBS team returned about 60% of its production for a decade, and
returns under 40% now. 2021 at .742 is the COVID extra-eligibility year.

Early-season games have become harder to predict to match: held-out Brier on
2022-2026 is about 0.1854 against 0.1798 on 2014-2021, with the same model.
Roster continuity collapsed and season openers got less predictable, and
knowing *which* teams kept their production does not recover it.

## What would change this answer

- **More seasons.** 13 is the whole of CFBD's coverage. The question is worth
  re-asking when it is 20.
- **A defensive side.** Half of a team is missing from the predictor.
- **A different place to use it.** This tested returning production as a
  multiplier on offseason regression. A team's Elo entering a season is the
  product of years of results, so a roster fact may simply be the wrong shape
  for that slot — a direct pregame adjustment for early-season games is a
  different experiment, and not one this result rules out either way.
