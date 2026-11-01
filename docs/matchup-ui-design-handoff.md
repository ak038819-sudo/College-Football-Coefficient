# Elocast Matchup Intelligence — UI Design Handoff (Backend v1)

**Status:** Backend research contract is **ready for UI design**, NOT ready
for deployment. No PR merge or production source promotion authorized.
No win prediction, points projection, or gambling model is included in
this contract.

## Verified real-data handoff

GitHub Actions [run 41258250774](https://github.com/ak038819-sudo/College-Football-Coefficient/actions/runs/41258250774)
ran the archived CFBD 2025/2026 rushing and passing data through a disposable
canonical league database, checked FBS coverage, exported full scouting
contracts and validated schema invariants. Both jobs succeeded.

- 2025: **139 team records**, 9 metrics, **1,224 rated team/metric pairs**,
  no contract errors. Snapshot `as_of_exclusive=2025-12-31`.
- 2026: **139 team records**, 9 metrics, **1,242 rated team/metric pairs**,
  no contract errors. Snapshot `as_of_exclusive=2026-10-31`.
- The 139 records represent the team registry, **not** a claim that 139
  teams all held FBS status or had rated observations. Unrated entries
  carry explicit nulls.
- Original independent CFBD split research source remains archived under
  `data/research/matchup_cfbd/`, with provenance. Existing
  `data/raw`, main branch, Elo, Merits and live UI are unchanged.
- The [seven-day Actions artifact](https://github.com/ak038819-sudo/College-Football-Coefficient/actions/runs/41258250774/artifacts/13063598404)
  includes the validated JSONs. They can also be regenerated from the
  archived research source using the commands below.
- The one-off validation workflow job was removed following success.
  Dedicated feature tests remain permanently in CI.

## Stable versioned data contract

Primary implementation: `src/export_matchup_descriptive.py`.
Schema version: **`elocast-descriptive-v1`**.

Top-level:
- `schema_version`, `data_kind`, `season`, `as_of_exclusive`,
  `garbage_time`, `classification`, `disclaimer`
- `metric_definitions`: national population, minimum population,
  source observation count, weighting and actual denominator provenance.
- `teams`: keyed by canonical stringified team ID; each has ID, name and
  nine metric records.

Each team/metric record:
- `status`: `rated`, `insufficient_sample`,
  `insufficient_population`, or `no_observations`
- `sample_games`, `distinct_opponents`, `reliability`
- `raw_offense`, `raw_defense_allowed`
- `adjusted_offense`, `adjusted_defense_allowed`
- `offense_percentile`, `defense_percentile`
- `offensive_schedule_difficulty`,
  `defensive_schedule_difficulty`,
  `offensive_schedule_percentile`,
  `defensive_schedule_percentile`
- `offense_venue_effect`, `defense_venue_effect`,
  `offense_ridge_residual`, `defense_ridge_residual`

Every field is derived strictly from completed FBS-vs-FBS games **before**
`as_of_exclusive`; no same-day leakage. Explicit source coverage and
relative sample warnings are available by metric. Empty samples and
insufficient eligible national populations cannot produce fake percentiles.

`compare_units(bundle, offense_team_id=..., defense_team_id=...)` provides
side-by-side evidence of **one offense versus an opponent's defense**
without forecasting a game. It supports rushing, passing, and overall
metric comparisons. The comparison's `projection` is explicitly `null`.

## Semantic rules for UI design

1. **Observed beats invented.** Show raw measures and denominators honestly.
   Only show adjusted/ranked views when `status == rated`.
2. **Percentiles have a fixed direction:** higher offense percentile =
   better offense; higher defense percentile = stronger defense (lower
   offensive metric allowed); higher schedule percentile = tougher
   opposing units. Raw defensive values are allowed offense metrics,
   so **lower raw defensive allowance is better**.
3. **Show time and sample:** as-of date, number of games, distinct
   opponents and descriptive reliability tier. Distinct opponents
   supplement game count; repeated rivals do not manufacture diversity.
4. **Don't draw false precision:** ordinary game-to-game percentile
   movement is inevitable with ~12 games a season. Reliability tiers
   are not confidence intervals or percentages of truth.
5. **Do not misattribute raw-adjusted differences to schedule alone.**
   Opponent effects, home exposure and ridge shrinkage all contribute.
6. **Do not imply all nine metrics are identically weighted.** Overall
   success rate/PPA use reported total offensive plays; rushing/passing
   rates, PPA and all explosiveness metrics are currently *equal-game*
   research fits because matching eligible split-play/successful-play
   denominators were not provided by CFBD game advanced.
7. **Explosiveness remains valuable descriptive information.** Its
   adjusted rankings may be more sample-sensitive; do not use a
   universal unvalidated stability badge.
8. **No scores/spreads/win odds.** Matchup view presents how unit
   strengths compare, not what will happen next.

## Rebuild commands on a disposable database

```bash
python src/bootstrap_league_db.py --backup db/league_backup_before_playoff_migration.db --out db/league.db
for year in 2025 2026; do python src/load_games.py "data/raw/games_${year}.csv"; done
python src/patch_known_membership_gaps.py --db db/league.db
python src/load_membership_snapshot.py data/raw/membership_2026.csv --db db/league.db --year 2026
for year in 2025 2026; do
    python src/load_game_advanced.py "data/research/matchup_cfbd/game_advanced_${year}.csv" --db db/league.db
done
python src/check_matchup_data_readiness.py --db db/league.db --season 2025 --season 2026
python src/export_matchup_descriptive.py --db db/league.db --season 2026 --as-of 2026-10-31 --out /tmp/elocast-descriptive-2026.json
python src/validate_matchup_descriptive_contract.py /tmp/elocast-descriptive-2026.json
```

These commands are for an **isolated scratch DB**; do not run a
source-replacing import on the production league database.

## Scope separation

**Design-ready now:** layouts and visual hierarchy for team offensive /
defensive scouting cards, raw vs adjusted, unit schedule strength, year /
week as-of state, and opposing-unit comparisons. The backend data contract
is validated on real 2025/2026 data.

**Not production-ready yet:** visual design and browser integration; UI
regression tests; reviewed promotion of split CSVs from research into
regular production data ingestion; scheduled per-season refresh; and final
full-repository CI/merge verification. Matching *split* play counts can
improve future metrics, but must not be invented or misrepresented to
begin UI design. The feature PR remains a **draft**.
