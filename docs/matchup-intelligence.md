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


## Phase 2 progress: rushing and passing source fields

Verified against CFBD's current official `/stats/game/advanced` response
schema: both `offense` and `defense` contain nested `rushingPlays` and
`passingPlays`, each with `successRate`, `ppa`, and `explosiveness`.
`src/fetch_cfbd_game_advanced.py` now captures these twelve nullable fields,
`sql/game_advanced_tables.sql` describes them, and
`src/load_game_advanced.py` adds nullable columns to older databases before
loading. The existing overall metrics used for Elo are unchanged.

**Important:** historical `data/raw/game_advanced_YYYY.csv` files fetched
before this change do not have the new columns. Loading them again will store
NULL splits for those seasons, not backfill historical split values. The next
data pass must re-fetch and reload the seasons of interest using the revised
fetcher; do not claim rushing/passing coverage from schema presence alone.

The experimental read-only matchup adapter now accepts:
`rush_success_rate`, `rush_ppa`, `rush_explosiveness`,
`pass_success_rate`, `pass_ppa`, `pass_explosiveness`.
It rejects missing split columns explicitly. Both teams' FBS membership and
full historical cutoff remain mandatory.

**Pending checks before production:** execute the full test suite, re-fetch a
real season under the configured CFBD token and audit paired-game coverage,
confirm garbage-time conventions, backtest as-of snapshots chronologically,
compare calibration against baselines and add uncertainty intervals and UI.
No production export or merge is authorized.


## Validation harness (experimental)

`src/validate_matchup_intelligence.py` now provides a **read-only**, strict
prior-calendar-date backtest over completed FBS team-game advanced statistics.
For each evaluated date it refits from prior games, predicts an offense's
metric against the opposing defense, and reports MAE, RMSE, number of evaluated
team-games, and eligibility coverage. It also reports national-average and
shrunken prior-team-offense baselines on both all eligible observations and
the exact **matched subset** for which the opponent-adjusted model predicts.

Example after historical per-game data has been backfilled:

```bash
python src/validate_matchup_intelligence.py --db db/league.db --season 2025 --metric rush_success_rate
python src/validate_matchup_intelligence.py --db db/league.db --season 2026 --metric pass_ppa
python -m pytest tests/test_matchup_intelligence.py tests/test_build_matchup_intelligence.py tests/test_matchup_split_ingestion.py tests/test_validate_matchup_intelligence.py -q
```

**Not yet calibrated:** shrinkage=8, min_games=3, and minimum-training-games=4
are provisional; the first evaluation is not sufficient evidence to choose
them. Compare on multiple seasons, tune on prior folds only, and reserve later
seasons for honest out-of-sample reporting. Current backtest evaluates
metric prediction, not win probability, final score, or win/loss accuracy.

**Known limitations:** the advanced-stat game dataset may have asymmetric
coverage; team-game predictions are not independent, some earlier years are
sparse, and date-level cutoff sacrifices intra-day updates for safety. A
production backtest needs coverage stratification by week, season, metric,
conference and schedule connectivity, plus uncertainty/error bars.


## Real-data coverage audit

The read-only `src/audit_matchup_coverage.py` reports actual source
availability for each supported metric. It distinguishes completed FBS games,
paired observations (both teams present), single-sided observations, missing
FBS membership, stale-schema missing columns, and advanced rows not matched
to a completed FBS game.

```bash
python src/audit_matchup_coverage.py --season 2025 --db db/league.db
python src/audit_matchup_coverage.py --season 2026 --db db/league.db
```

Review the coverage before fitting. In particular, a newly added SQLite
column does **not** prove the historical API values were fetched. The first
successful feature-workflow run tested synthetic fixtures only; an actual
data backfill and full pipeline dry run are outstanding. Existing production
refresh automation is not modified by this draft PR.
