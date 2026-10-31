# Elocast Matchup Intelligence — first implementation slice

## Goal

Deliver two separate, complementary experiences:
1. **Scouting profiles:** team offense and defense graded by empirical FBS percentiles after opponent adjustment.
2. **Matchup estimates:** how a particular offense is expected to perform against a particular defense, using the same statistical model.

These are **experimental descriptive estimates**, not score projections, win probabilities, or Elo/Merits replacements. Do not put them into playoff selection.

## First committed model

`src/matchup_intelligence.py` fits an additive regularized model to individual **offensive** team-game observations:

`observed_metric = FBS_mean + offensive_effect(team) + defensive_allowance_effect(opponent) + residual`

Fit is weighted and ridge-shrunk to reduce tiny-sample extremes. Opponent defense is inferred from the offensive observations it faces; do **not** insert a separate defensive duplicate of each row.

Defensive percentile is ordered so that **allowing less** is good for success rate, PPA, and explosiveness; offensive percentile is ordered so that **producing more** is good. Percentiles compare eligible FBS teams using midranks. A team requires at least three offensive and three opponent-facing games in the fitted sample; insufficient coverage means **unavailable**, not a zero grade. `higher_is_better=False` handles reversed-sign metric semantics.

`project_matchup(profiles, mean, offense, defense)` returns a point estimate in the metric's native units plus component contributions. This is not yet a calibrated forecast: there are no confidence intervals, location/tempo factors, or win odds.

## Existing data and integration path

- Existing `src/fetch_cfbd_game_advanced.py` and `src/load_game_advanced.py` ingest `game_team_advanced` offensive success rate, PPA and explosiveness; missing data stays NULL.
- Audit FBS-only team/game identity mapping, duplicate game-team rows, and coverage by season before fitting or exporting.
- Build a point-in-time adapter that joins `game_team_advanced` to `games` and canonical teams; feed one `Observation(team, opponent, value, weight)` **per completed team-game**. Verify that all games precede the requested pregame snapshot time, including postseason season-year boundaries. Never use full-season aggregates in historical previews.
- The existing per-game advanced collection does **not** provide rushing-vs-passing splits. Audit and ingest appropriate per-game split inputs before promising rush/pass charts. Do not infer them from total PPA.
- Fit separate metrics and persist snapshots with `season`, `as_of`, `metric`, `coverage_n`, `model_version`, FBS baseline and resulting grades. Publish through the established static `ui/data` export pipeline, only after schema/coverage validation.
- Phase 2: Team-page percentile bars, explainers, raw/adjusted toggle, season/time selection, unavailable labels, and neutral color scale.
- Phase 3: Upcoming-game views comparing offensive to opposing defensive profiles, matchup estimate plus sampling/uncertainty, and separate opposing-unit panels for both directions. Use matchup-specific deltas for explainable narratives; never call percentile difference a probability.
- Once historical walk-forward evaluation supports it, add calibrated predictive intervals and potentially integrate into the existing hypothetical matchup tool **without modifying production Elo**.

## Validation gates before deployment

- Chronological walk-forward folds: only games completed before each preview kickoff (no future opponents or future season totals).
- Exclude or flag missing advanced stats, unpaired sides, FCS games, and small samples; report coverage by season/week.
- Compare estimated success rate/PPA against unadjusted FBS averages, simple opponent averages and Elo-based baselines on untouched holdout seasons; publish MAE/RMSE and reliability checks.
- Evaluate shrinkage strength and weighting on training folds only; identify disconnected early-season opponent schedules and document uncertainty.
- Check offensive/defensive percentile orientation, FBS population, per-team game counts, and stability under row permutations.
- No pipeline, UI, or Elo integration until data-contract and historical leakage tests pass.

## Run focused tests

`python -m pytest tests/test_matchup_intelligence.py -q`

## Status

**Foundation only.** The mathematics and tests are added on an isolated feature branch. No live data export, visual interface, split-stat collection, or production matchup forecast has been deployed.
