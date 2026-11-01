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


## Real committed-data reports in pull-request CI

The normal `Test, Rebuild Dashboard, and Deploy` workflow now performs
**read-only** matchup coverage audits for 2025 and 2026 after building its
test SQLite database from committed game and advanced-stat CSVs. It also
runs strict prior-date `success_rate` backtests for both seasons. Results
appear in the workflow job logs. The preexisting `rebuild-and-deploy`
job remains restricted to `main` and is not invoked by draft PRs.

These reports only describe the **currently committed source CSVs**; they
do not re-fetch the CFBD API, do not guarantee rushing/passing split
coverage, and do not assert positive forecast skill. Historical rush/pass
re-fetch and controlled calibration remain required before release.


## Architectural consolidation (PR #161)

The original PR prototype independently fitted offense/defense coefficients.
**This is retired.** `src/matchup_intelligence.py` now invokes the existing
`src/build_adjusted_stats.py::solve()` for the fitted league mean, offensive
effects, defensive allowance effects and venue coefficient. It creates only
percentile and pregame projection *views* of those fitted quantities.
`tests/test_matchup_shared_solver.py` checks equality with direct calls to
that shared solver.

For `success_rate` and `ppa`, when per-game offensive play counts exist,
point-in-time profiles use the production `LAMBDA[metric] = 200` and
play weighting. This matches the **model formulation and its configured ridge
strength**; the pregame cutoff means a weekly estimate will naturally differ
from a full-season end-of-year adjusted statistic. The pregame model enforces
FBS-only membership and strict prior-date observations.

For `explosiveness` and rushing/passing breakdowns, the shared *solver* is
used, but source metrics and weights are not yet calibrated to the existing
adjusted-stat settings. They use provisional per-team-game weights with ridge
2.0, and should NOT be presented as calibrated. (Overall explosiveness can
use available `off_plays` weighting; its ridge remains provisional.) To
make the missing split exposure robust, distinguish unavailable source
fields from genuine zero observations and do not label incomplete data
as a national percentile.

The `home_effect` is retained on each scouting profile for the same fit.
An estimated matchup can supply `--venue-side 0.5` when its offense is home,
`-0.5` when away or `0` for neutral, consistent with the established
model's venue convention. Historical walk-forward evaluation uses the
observed home/away/neutral designation.

**Remaining work**: real-data replay, calibration for splits, uncertainty,
static exports and UI. No feature is connected to production Elo or Merits.


## Backend-to-UI design handoff (no visual implementation yet)

`src/export_matchup_scouting.py` produces an explicit research JSON artifact
with `schema_version=matchup-scouting-v1`, season, strict pregame `as_of`,
metric-by-metric coverage and eligibility, team-level adjusted effects and
empirical percentiles, and two matchup directions with separate venue
adjustments. It never changes production `ui/data` or Elo/Merits.
Unavailable metrics have `team_profiles={}` and a null baseline/matchup,
not artificial 50th-percentile scores.

Example:

```bash
python src/export_matchup_scouting.py --db db/league.db \
  --season 2026 --as-of 2026-10-31 \
  --out /tmp/elocast-scouting-2026.json
```

A draft-PR CI build writes 2025 and 2026 research samples and attaches them
as the `matchup-scouting-contract` artifact, so UI designers can review the
real data shape without the site using it. Those artifacts are **not**
evidence of completed rushing/passing coverage or calibrated performance.

### Verified input gap

Inspected the branch's committed `data/raw/game_advanced_2025.csv` and
`data/raw/game_advanced_2026.csv` headers: **neither contains any
rushing/passing split column**. The new parser and migration can accommodate
those values, but they must be fetched again from CFBD. Without the backfill,
rush/pass profile cards must show unavailable and be excluded from production.

### Remaining backend gates before activating the UI

- Obtain 2025/2026 split-stat data through an authorized CFBD-secret workflow;
  retain original CSVs until a staged fetch and completeness check succeeds.
- Confirm count of paired FBS observations per metric using
  `src/audit_matchup_coverage.py`.
- Measure strictly chronological held-out error using
  `src/validate_matchup_intelligence.py`, compare **matched** baselines,
  and calibrate split metric weights/shrinkage only on training periods.
- Calibrate uncertainty or suppress confidence language entirely.
- Check latest repository-wide CI and export artifacts. Do not merge/deploy
  until the UI and integration tests are also completed.


## Safe research backfill, prior to UI work

A new manual-only script stages refreshed CFBD per-game advanced files
**outside** the committed `data/raw` directory:

```bash
CFBD_API_KEY=... python src/stage_matchup_backfill.py 2025 2026 --out-dir /tmp/elocast-split-staging
```

It writes CSVs with the new rushing/passing columns plus
`split_coverage_report.json`, distinguishing reported metrics, truly zero
values and missing fields. Incomplete or failed fetches are flagged.
**Nothing is automatically promoted, merged, or deployed.** The next
verification is to load staged files into a disposable DB and compare
canonical FBS pairing and date coverage against the report before replacing
any raw dataset.

This script needs the existing repository CFBD key available in its
invocation environment. The GitHub connector available for this task does
not expose that key or a safe workflow-dispatch action, so no historical
split-data backfill has actually been run by this PR.


## Data-certainty release gate (2026-10-31)

A read-only, conservative audit is implemented in
`src/check_matchup_data_readiness.py`:

```bash
python src/check_matchup_data_readiness.py --db db/league.db --season 2025 --season 2026
```

For **each** rush/pass metric and season, this requires at least 90% of
completed FBS games with *both* observed teams, and at least 95% of potential
FBS team-game sides observed. These are provisional **minimum** coverage
thresholds, not proof of correctness. The tool exits nonzero when its
requirements fail, and always reports `production_ready=false` because
coverage alone says nothing about point-in-time reproducibility, historical
forecast error, or calibrated uncertainty. Any missing schema field fails.

The historical data adapter also rejects nonfinite measurements,
nonpositive/nonfinite play weights, and success rates outside [0,1]. True
zero success rates are retained. Tests now cover these scenarios.

**Actual data status:** committed 2025 and 2026 game-advanced CSVs lack
all rushing/passing split fields; do not publish rush/pass percentiles from
those files. Staged CFBD re-fetch and a disposable-database verification
are still mandatory, as are strictly walk-forward held-out metrics
and a complete CI pass.

**Design hold:** No UI implementation or production export should begin
until the necessary source and validation gates have reported actual
satisfactory results. Pending work is explicitly a data-quality blocker,
not a cosmetic task.


## Per-metric calibration and early-season reliability (research)

A training-only selection tool, `src/calibrate_matchup_splits.py`, evaluates
rush/pass success-rate, PPA, and explosiveness independently across
regularization strengths 0.5, 2, 8 and 24 on **2025**, then scores the
chosen configuration **once** on untouched **2026**. Selection cannot use
2026 errors. If all candidates fail the training coverage threshold or
fail to improve the matched prior-offense baseline by at least 0.2%,
the metric explicitly falls back to the baseline policy rather than
being automatically promoted.

The experimental model uses the established
`build_adjusted_stats.solve()` routine; only split-specific ridge
strengths change. The current objective remains raw-scale metric MAE,
which can be inappropriate for unstable explosiveness tails. If
explosiveness continues to underperform, do not force adjustment into
production; assess robust prediction losses and alternative variance
models on a **new** strictly chronological training window.

Early-season scouting profiles now expose `distinct_opponents` and
`reliability`. The descriptive reliability tiers are:
- insufficient: fewer than 3 games or 3 distinct opponents
- provisional: fewer than 5 games or 5 distinct opponents
- developing: fewer than 8 games or 7 distinct opponents
- established: otherwise

These are transparent sample-adequacy labels, **not statistical
confidence levels or calibrated error bars**. Opponent graph connectivity
and predictive uncertainty need separate work before a numeric confidence
score or an estimated range can be trusted. A grade's percentile should
never be interpreted as a win probability.

A one-off CI job `calibrate-staged` downloads the previously verified
CFBD Actions artifact, builds a disposable database and runs the train/holdout
procedure. No CFBD API request or production CSV change is involved.
Remove the job after extracting the evaluation artifact so it does not
repeat computation or depend on an expiring source artifact.
