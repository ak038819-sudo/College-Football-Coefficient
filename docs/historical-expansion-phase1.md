# Historical expansion — Phase 1 audit

Tracking: #194. Draft implementation: #195.

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
5. Run tests and attach reports to PR #195. Do not mark the audit complete until a real DB snapshot has been inspected.

## Known limitations

The current `src/load_games.py` discards games when either team identity cannot be resolved. Raw CSV comparisons can surface these records but cannot prove which should enter modern FBS ratings. The current `src/fetch_cfbd_games.py` uses division-based filters and can classify historical games conservatively when historical classification is unavailable. Both require era-aware ingestion design in Phase 2.

No data migration, model rebuilding, or live deployment is part of Phase 1.
