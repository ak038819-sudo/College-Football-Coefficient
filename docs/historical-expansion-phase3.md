# Phase 3 — 1946–1979 historical backfill

**Status:** the importer, the auditor and the workflow that runs them are in place. No historical season has been imported or promoted yet.

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

## How a batch is run

**On a runner, not by hand.** `api.collegefootballdata.com` is unreachable
from the containers this repository is worked in, and the key exists only as
the `CFBD_API_KEY` repository secret, so
`.github/workflows/historical-backfill.yml` is where the import happens.
Dispatch it with the batch's first and last season.

The staging database is **carried between runs as an artifact** rather than
committed — the plan keeps staging records and raw payloads out of Git — and
it is uploaded **even when the import fails**, which is the run where it
matters: the importer checkpoints per season and season type, so the next
dispatch of the same range resumes rather than re-fetching what the provider
has already served. `fresh: true` starts from nothing, for a rehearsal.

Each run audits the range it imported and publishes
`backfill_readiness.csv` and `backfill_summary.json` as a second artifact,
and writes the blocked partitions into the run summary.

The same two scripts can be run directly where the provider is reachable:

```bash
export CFBD_API_KEY="..."  # supply securely; never commit credentials
python scripts/import_historical_archive.py --db staging.db --start 1948 --end 1959
python scripts/audit_historical_backfill.py --db staging.db --start 1948 --end 1959 --out /tmp/elocast-1948-1959
python -m pytest tests/test_historical_backfill_audit.py tests/test_historical_backfill_workflow.py -q
```

Repeat for 1960–1969 and 1970–1979. Back up the staging database securely and
retain source provenance.

**The workflow promotes nothing.** It opens no production table, builds no
rating and touches no file under `ui/`, so no deploy follows it. Everything
under "Review before promotion" below still has to happen first, and the
promotion itself is a separate, approved step.

## Review before promotion

- Reconcile source counts with independent historical references for each season, especially empty or unexpectedly sparse years.
- Create a reviewed canonical historical school identity map including renamed, merged, discontinued, and similarly named programs.
- Create season-specific major-college / division eligibility mapping; do not infer historical membership from present-day FBS.
- Preserve ties, neutral-site uncertainty, missing scores/dates, and unknown classifications.
- Report all unresolved ambiguities; never silently discard records to satisfy modern FBS constraints.
- Freeze pre-promotion Elo/Merits fingerprints; compare outputs before and after promotion and require explicit approval.

The audit's `ready_for_source_reconciliation` flag means only that basic structural checks passed. It is **not** a declaration that the season is complete or suitable for rating calculations.
