# Elocast: Metric Integrity & Schedule Strength (research backend)

## Purpose

The scouting layer is **descriptive**, not a game-prediction or win-probability
product. It describes what FBS teams have actually accomplished against their
2025/2026 schedules and why adjusted versus raw statistics differ. Elo and
Merits remain separate.

## What is implemented

`src/describe_matchup_metrics.py` uses the **same**
`src/build_adjusted_stats.solve()` already used by Elocast's existing
adjusted statistics. It exposes, for each of the six rushing/passing
offensive metrics, separate offensive and defensive components.

* **Raw offense:** weighted mean of actual observed offensive values.
* **Raw defense allowed:** weighted mean of opponents' offensive values
  observed against that defense, not an independent re-estimation.
* **Opponent-adjusted offense:** league mean + fitted team offense effect.
* **Opponent-adjusted defense allowed:** league mean + fitted defense
  allowance effect; lower means better defense for the default metrics.
* **Offense schedule difficulty:** negative weighted mean of opposing
  defenses' fitted allowances (high means tougher).
* **Defense schedule difficulty:** weighted mean of opposing offenses'
  fitted effects (high means tougher).
* **Venue effect:** actual home/away exposure in the metric's units.
* **Ridge residual:** raw minus adjusted minus opponent effect minus venue
  effect. This is essential: **raw–adjusted is not purely schedule
  strength**, because penalization shrinks fitted effects toward zero.
* **Percentiles:** offense strength, defense strength, and both unit schedule
  difficulties against the *same sufficiently sampled FBS population*.
  All display-oriented percentiles favor strong performance/tough schedules.
  Insufficient samples return null percentiles, never a guessed 50th percentile.
* **Reliability:** existing games/opponent-diversity labels; these are
  descriptive data-sufficiency warnings, not probabilities or error bars.

The point-in-time adapter excludes same-day and future games. These fields
are added to the **research-only** snapshot (`descriptive_units`) and
offline JSON contract (`descriptive_profiles`). No production page or API
automatically publishes them.

## Interpretation and precautions

These are *schedule exposure* estimates computed from the same set of games
as the team's metrics. They are not independent causal estimates of what
would have occurred against a different opponent. Strong teams receive
adjusted values determined by their actual schedule and the connectivity of
their competition graph. Values depend on the training sample and ridge
strength, which for split metrics remains research-stage.

The historical 2026 held-out evaluation showed improvements in success
rates but **not** in both explosiveness splits. That failure matters for
claims about predicting future explosiveness. It does **not**, by itself,
invalidate a correctly labeled historical descriptive explosiveness metric.
However, before publishing its opponent-adjusted percentile, test stability
under bootstrap/schedule perturbations and across season-to-date cutoffs,
check outliers and boundedness, and quantify how much of its variation comes
from weakly connected schedules. Retain raw explosiveness prominently.

## Release gates before UI design

1. Preserve the verified, staged CFBD 2025/2026 split data permanently through
   an explicitly reviewed source promotion. Its existing Actions artifact is
   temporary.
2. Run this new descriptive pipeline on the verified full-season FBS data.
   Check metric sanity ranges, NULL handling, both-sided game coverage,
   FBS identity, and repeated schedule opponents.
3. Compute percentile stability by game/week and under leave-one-opponent-out
   diagnostics. Do not equate percentiles to predictive confidence.
4. Agree on the interpretation and neutral-site treatment, then run full CI
   and no-regression tests. UI design can begin when those claims are supported.

Tests: `tests/test_describe_matchup_metrics.py` verifies numeric identity
with the production solver, exact accounting of opponent/venue/ridge
effects, distinct offense/defense schedules, missing samples and zero values.
