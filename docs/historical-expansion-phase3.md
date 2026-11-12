# Phase 3 — 1946–1979 historical backfill

**Status:** implementation started; no historical seasons imported or promoted by this PR.

## Dependency and release gates

- Phase 2 (#196, the staging importer) is merged, so a full import can be run against a staging database.
- Phase 1 (#195 plus #197, the audit of existing production baselines) is merged and runs in CI; its findings must be reviewed and accepted before any production promotion.
- Never run production Elo or Merits against unreviewed staging records.

## Backfill batches

1. Pilot 1946–1947 (Phase 2 handoff)
2. 1948–1959
3. 1960–1969
4. 1970–1979

Each batch uses Phase 2's staging importer with a persistent staging database; checkpointed imports are resumable. Export read-only integrity reports after each batch. Treat unknown classification as **unknown**, not FBS.

## Run after Phase 2 merge

```bash
export CFBD_API_KEY="..."  # supply securely; never commit credentials
python scripts/import_historical_archive.py --db db/historical_staging.db --start 1948 --end 1959
python scripts/audit_historical_backfill.py --db db/historical_staging.db --start 1948 --end 1959 --out /tmp/elocast-1948-1959
python -m pytest tests/test_historical_backfill_audit.py -q
```

Repeat for 1960–1969 and 1970–1979. Staging DB and raw historical data should remain outside Git; back up securely and retain source provenance.

## Review before promotion

- Reconcile source counts with independent historical references for each season, especially empty or unexpectedly sparse years.
- Create a reviewed canonical historical school identity map including renamed, merged, discontinued, and similarly named programs.
- Create season-specific major-college / division eligibility mapping; do not infer historical membership from present-day FBS.
- Preserve ties, neutral-site uncertainty, missing scores/dates, and unknown classifications.
- Report all unresolved ambiguities; never silently discard records to satisfy modern FBS constraints.
- Freeze pre-promotion Elo/Merits fingerprints; compare outputs before and after promotion and require explicit approval.

The audit's `ready_for_source_reconciliation` flag means only that basic structural checks passed. It is **not** a declaration that the season is complete or suitable for rating calculations.
