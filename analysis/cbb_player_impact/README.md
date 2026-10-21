# College basketball: team strength and player impact

Research foundation, 2026-10-12. Scope: men's Division I basketball.

## Question and status

Integration boundaries and staged release gates are in
[`docs/basketball-integration.md`](../../docs/basketball-integration.md). What the
real pilot runs measured, including the lineup feed's missing floor time, is in
[`docs/basketball-pilot-evidence.md`](../../docs/basketball-pilot-evidence.md).
The collector now includes a versioned, basketball-namespaced `integration.json`
schedule export in its artifact. It is always marked as a non-public team sample;
it does not enable the basketball UI or publish ratings.

How much does expected team strength change when a player is unavailable and
his minutes go to specified replacements? Describe current-season strength;
use future-game accuracy as a diagnostic rather than the only objective.

Implemented: a bounded CBBD pilot collector, structural lineup checks, and an
independent result-only team Elo replay. Tests use synthetic API-shaped data.
No real basketball data has been fetched or fitted in this change. No player
ratings, calibrated Elo parameters, injury effects, or national rankings exist
yet. These files have no production pipeline, database, or dashboard integration.

## First data run

Obtain basketball API access at https://collegebasketballdata.com/key and set
`CBBD_API_KEY` in the local environment or a secret manager. Do not put the key
in source files or chat. Do not assume the football key grants basketball access.
From the repository root:

```sh
python analysis/cbb_player_impact/collect.py --season 2026 --team BYU --limit 3 --out analysis/cbb_player_impact/snapshots/byu-2026-pilot
python -m pytest tests/test_cbb_player_impact.py -q
```

The pilot makes one schedule request and three requests per selected game
(lineups, substitutions, plays), ten requests with the default limit. It selects
the earliest completed games, not a representative national sample. Check access
and quota first. It stops on access/network errors rather than silently substituting
missing values; incomplete runs are marked in the manifest. Raw JSON is kept with
retrieval time and SHA-256 hashes. Existing snapshot directories are never overwritten.
Snapshots are gitignored. Retain validated snapshots in durable dataset storage
before any expensive model fitting. API corrections mean a modern historical
snapshot is not necessarily what was available in real time.

### Run with a GitHub repository secret

The manual `College basketball player-impact pilot` workflow reads the repository
secret named exactly `CBBD_API_KEY`. It tests the prototype, collects three BYU
games (ten API requests), and uploads the snapshot and audit as a workflow artifact
retained for 30 days, including partial output on failure. The job summary states
the structural-check results separately from player-model readiness. It has
read-only repository permissions and does not publish ratings or alter the site.

After the workflow is merged onto the default branch, select it in GitHub Actions,
choose **Run workflow**, and select ending year 2026 for the initial pilot. Manual
dispatch requires the workflow to exist on the default branch:
https://docs.github.com/actions/managing-workflow-runs/manually-running-a-workflow
The repository secret is consumed inside Actions; it is not readable from the
local collector session. Download validated artifacts before their retention expires.

`audit.json` checks five distinct player IDs per lineup, both teams, unique
lineups, aggregate time including overtime, and score reconciliation. Passing
these necessary checks does NOT verify who opposed whom, stint order, individual
minutes, or possession attribution. `player_model_ready` remains false.

## Data-source findings

Official CBBD documentation checked 2026-10-12:

- https://api.collegebasketballdata.com/data-availability : ending-year seasons;
  substitution and lineup coverage starts in 2024 (2023-24). Plan to investigate
  2024, 2025, and 2026; actual usable coverage must be measured per team/game.
  The games endpoint caps requests at 3,000 records. A national fetch must split
  date windows and deduplicate IDs, then reconcile against schedule coverage.
- https://api.collegebasketballdata.com/api/lineups : five-player aggregate
  statistics expose points, possessions and seconds. They do not expose paired
  opposing five-player stints, so they are insufficient alone for adjusted impact.
- https://api.collegebasketballdata.com/api/plays : substitutions and plays are
  available by game. Reconstruct their shared timeline before fitting players.
- https://api.collegebasketballdata.com/getting-started and
  https://api.collegebasketballdata.com/usage-and-access : bearer authentication;
  endpoint access and quotas must be verified with the account.

## Experiments in order

1. Inspect BYU pilot responses. Add player box scores and reconcile each player's
   reconstructed minutes. Check starters, halftime resets, simultaneous substitutions,
   overtime, free throws during substitutions, and missing events. Quarantine uncertain
   intervals, never invent lineups. Report the fraction of games/time/possessions retained.
2. Expand to the national D-I schedule for 2023-24 through 2025-26, using stable
   provider team/player IDs and season-specific D-I membership. Preserve non-D-I
   opponents explicitly; decide their treatment before rating teams. Fetch BYU's
   opponents' full schedules as part of that national dataset, not just BYU games.
3. Replay independent team Elo chronologically. `elo.replay` supplies pregame history
   and ending ratings, skips unfinished games, and rejects invalid/duplicate finals.
   Initial defaults (1500, scale 400, K20, home 0, retention .5) are uncalibrated
   plumbing values, not inherited football settings or recommended production choices.
   Compare responsiveness, rank stability, early-season behavior, calibration, Brier
   and log loss; a BYU-only replay cannot produce meaningful national ratings.
4. Build paired ten-player stints with points, possessions, game ID, season, court
   location, start/end time, and quality flags. Fit a regularized adjusted plus-minus
   model: signed player indicators, venue term, possession weights, and shrinkage.
   Keep impact in points per 100 possessions initially. Players who always share
   minutes are weakly identifiable even with regularization: report uncertainty and
   stability, not invented precision. Raw on/off is a diagnostic comparison only.
5. Compare changed rotations against a baseline rotation with the SAME player-impact
   snapshot. For player i, a_i = minutes_i / game_duration, so sum(a_i) = 5, not 1.
   Require each share in [0,1] and a feasible five-player rotation. The additive
   scenario delta is sum((a_new_i - a_reference_i) * impact_i). Replacement minutes
   must be allocated explicitly. This is a conditional estimate, not a causal claim.
6. Estimate the mapping from efficiency deltas to Elo using training data only.
   Apply only the rotation delta to team Elo; adding all player values double-counts
   the roster already represented by team results. The reference rotation must be
   time-weighted consistently with the rating's history. Validate that approximation
   before exposing injury/trade simulations. No arbitrary points-to-Elo multiplier.

## Validation boundary

Use 2023-24 for development, 2024-25 for tuning, and reserve 2025-26 as a final
historical test before inspecting performance results. Within each evaluation
season, every forecast uses only games completed beforehand; any online player
updates use prior games only and fixed hyperparameters. Never estimate a player's
early-season rating from his full-season numbers. A post-season descriptive table
is a separate output with a clear as-of date.

Compare team-only Elo, raw on/off, adjusted impact, and the roster adjustment.
Evaluate next-game/team efficiency errors as well as win-probability calibration;
report player ranking stability under game-block resampling and small-sample flags.
For a pregame injury evaluation, observed game minutes are not legitimate projected
minutes: use timestamped availability data or label rotations as conditional scenarios.
Handle transfers with identity continuity but uncertain role/context transfer;
new players need explicit priors. Do not declare player effects portable without testing.

Next concrete deliverable: a measured coverage report and one manually reconciled
BYU game with paired lineups. Only then fit an experimental player leaderboard.
