# Phase 3 — 1946–1979 historical backfill

**Status:** all 34 seasons are staged. 1946–1979 was imported on 2026-11-13 — 22,706 games in 68 season-and-type partitions, audited in one pass with no blocked partition, no missing score and no missing date. Nothing has been promoted. The staged archive exists only as the `historical-staging-db` artifact, which expires 90 days after the run that produced it.

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
has already served. `fresh: true` starts from nothing, for a rehearsal, and such a run is kept
under its own artifact name: the carry step takes the newest artifact of the
chain's name, so a rehearsal published into the chain would be what the next
ordinary dispatch resumed from, dropping every earlier batch in silence.

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

## Source reconciliation

`.github/workflows/historical-reconcile.yml` compares the staged fixtures
against the provider's own season records (`/records`), per team per season
per season type, and reports every disagreement: a game count that differs,
the same count with different results, a team with a record line and no
staged games, and a staged team the records feed never mentions.

That last one is not a fault to be fixed but the input to the identity map:
an unmatched name is reported, never dropped and never guessed at, and the
summary ranks those names by how many seasons they appear in.

A record line of all zeroes beside nothing staged is the two feeds
**agreeing** that a team did not play — the provider carries lines for
programs it has no fixtures for — so it is counted apart
(`empty_reference_lines`) and is not a finding.

**It is a cross-feed check, not an independent one.** Both feeds are
collegefootballdata.com, so agreement means the import is faithful to the
provider — not that the history is complete or correct. The independent
references below are still owed.

### Result, 1946–1979 (2026-11-13)

8,087 team-seasons compared. **No game-count mismatch and no result
mismatch anywhere in the range** — every team the two feeds share agrees
exactly on games, wins, losses and ties. 7,333 team-seasons reconcile
outright; 2,111 are an all-zero record line beside nothing staged, which is
agreement.

All 754 remaining findings are the same one: a staged team the records feed
never carries, across **191 distinct names**. The most persistent are
Presbyterian (29 seasons), Quantico Marines (24), Wofford (23), Trinity
(Texas) (22), McMurry (17), Boston University and Catawba (16 each), Santa
Clara and Tampa (14 each). They are opponents of major-college programs
that the records feed does not carry, and they are the work list for the
identity map and the season-specific eligibility mapping below — not
records to drop.

This says the import is faithful to the provider. It says nothing yet about
completeness, which the independent references below are for.

The run reads the backfill's artifact chain and never republishes it, so the
backfill stays that chain's only writer; the records it collects are carried
in a chain of their own (`historical-records-db`). `offline: true`
reconciles against records already collected and asks the provider for
nothing.

## Review before promotion

- Reconcile source counts with independent historical references for each season, especially empty or unexpectedly sparse years. (The provider's own records feed is now reconciled by `historical-reconcile.yml`; independent references are still outstanding.)
- Create a reviewed canonical historical school identity map including renamed, merged, discontinued, and similarly named programs.
- Create season-specific major-college / division eligibility mapping; do not infer historical membership from present-day FBS.
- Preserve ties, neutral-site uncertainty, missing scores/dates, and unknown classifications.
- Report all unresolved ambiguities; never silently discard records to satisfy modern FBS constraints.
- Freeze pre-promotion Elo/Merits fingerprints; compare outputs before and after promotion and require explicit approval.

The audit's `ready_for_source_reconciliation` flag means only that basic structural checks passed. It is **not** a declaration that the season is complete or suitable for rating calculations.
