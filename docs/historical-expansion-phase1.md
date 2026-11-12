# Historical expansion — Phase 1 audit

Tracking: #194. Implementation: #195 (merged as `50ee43ec`, 2026-11-12).

## First real run — 2026-11-12

The audit now runs in CI, in the `test` job of `ci-and-deploy.yml`, against the database
that job bootstraps from committed raw data, and its reports upload as the
`elocast-historical-audit` artifact. The run fails if a table named as a baseline is
absent, so a fingerprint of `null` can no longer pass for a baseline.

Results of the first run against the real archive (30,641 games, 1980-2026):

- **Season coverage is clean in all 47 seasons**: no duplicate ids, no missing scores, no
  self-matchups, no team without a membership row. Ties run 6-16 a season through 1995 and
  are exactly zero from 1996, which is when overtime was introduced; 2020 is short at 534
  games against 774 in 2019.
- **Raw-vs-loaded reconciles exactly**: 31,378 raw rows minus 737 with an unresolvable team
  equals the 30,641 loaded, and that identity holds in each of the 47 seasons individually.
  Nothing is dropped silently.
- **488 of 933 unresolved team-slots are five former Division I-A programs** that dropped
  football: Pacific (156 games, 1980-1995), Cal State Fullerton (120, 1980-1992), Long
  Beach State (120, 1980-1991), Wichita State (54, 1980-1986) and West Texas State (38,
  1980-1985). Each range ends the year that program left I-A. The remaining 445 are FCS,
  Ivy League, and NAIA/Division III schools appearing in FCS fixtures, which are correctly
  excluded. Whether to add those five is a scope decision for Phase 2, not a data defect.

Two limits of the tooling the real run exposed:

- **Four of the ten coverage metrics cannot fail.** `games.game_id` is
  `INTEGER PRIMARY KEY`, so it aliases the rowid and can be neither NULL nor duplicated,
  and `home_team_id`/`away_team_id` are `NOT NULL`. `duplicate_game_ids`,
  `missing_game_ids` and `missing_team_ids` are therefore guaranteed zero by the schema and
  their zeros say nothing about data quality. The unit tests only exercise them because
  they build an unconstrained in-memory table. The metrics that genuinely measure are
  `games`, `distinct_game_ids`, `missing_scores`, `self_matchups`, `ties` and
  `teams_without_membership`.
- **`team_coefficient_by_year` is no longer fingerprinted.** It is defined only in
  `sql/team_coe_tables.sql`, which neither `bootstrap_league_db.py`, `run_pipeline.py` nor
  either CI job runs, so it exists in no database this project builds. It is a legacy CoE
  v1 table that only `report_year.py` and `coe_cli.py` read.

## Run against a **local copy** of the production database

```bash
python scripts/audit_historical_coverage.py --db db/league.db --out /tmp/elocast-phase1
python scripts/audit_historical_baselines.py --db db/league.db --raw-dir data/raw --out /tmp/elocast-phase1
python -m pytest tests/test_historical_coverage_audit.py -q
```

Both scripts open the source SQLite database read-only. Output is written outside the DB, to the specified directory. Keep `baseline_fingerprints.json` from the **same exact DB snapshot** used to create the season report. Preserve it as an immutable release artifact; rerun after future backfills to detect changes. A hash mismatch needs investigation, not automatic rollback.

## Interpret the reports

- `season_coverage.csv/json`: 1980–2026 game counts, score missingness, duplicate/nonunique IDs, ties, invalid same-team matchups and missing season membership. These are *internal* consistency counts, not proof that CFBD's season is complete.
- `raw_vs_loaded.csv`: how many normalized FBS source rows appear in the current DB. A missing ID can be a deliberately excluded opponent, a missing identity, or another ingestion failure.
- `unresolved_aliases.csv`: names requiring human review. Never auto-add them to FBS.
- `baseline_fingerprints.json`: SHA-256 of DB, model config and existing model tables (when present). Preserve this before any historical data is introduced.

## Required manual signoff

1. Audit each year and investigate suspicious gaps; specifically check 2020's pandemic-shortened/cancelled schedules without deleting that season.
2. Review missing source game IDs and unresolved team names, separating legitimate non-FBS filtering from actual data loss.
3. Preserve exact production Elo, Merits and game-table fingerprints. Confirm unchanged config K=45, offseason retention=0.8.
4. Verify source CSV availability and distinguish missing CSVs from missing DB seasons.
5. Run tests and review the `elocast-historical-audit` artifact from the CI run. Item 2 is now the open one: the five former I-A programs above are identified but no decision has been taken on them.

## Known limitations

The current `src/load_games.py` discards games when either team identity cannot be resolved. Raw CSV comparisons can surface these records but cannot prove which should enter modern FBS ratings. The current `src/fetch_cfbd_games.py` uses division-based filters and can classify historical games conservatively when historical classification is unavailable. Both require era-aware ingestion design in Phase 2.

No data migration, model rebuilding, or live deployment is part of Phase 1.
