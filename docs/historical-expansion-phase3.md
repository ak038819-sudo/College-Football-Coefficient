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

## Identity candidates

`.github/workflows/historical-identity.yml` reads the staged archive and the
collected records — both chains, neither replaced, no credential needed,
nothing fetched — and writes `historical_schools.csv`: every distinct school
name in the range with the evidence for it (seasons, games, home and away
counts, distinct opponents, every classification and conference it appears
under), sorted into three states:

| status | meaning |
| --- | --- |
| `records_matched` | the provider carries the name as a team, so its id is known |
| `registry_matched` | our own reviewed `team_identities.json` knows the name |
| `unresolved` | neither: a person has to rule on it |

**It decides nothing.** Where an unresolved name resembles a known one the
`suggestion` column says so, as a lead to check; a suggestion is never
written into `canonical_name`, and `verdict` and `verdict_note` come back
empty to be filled in. Two schools that merely look alike stay two schools
— "Trinity (Texas)" is not "Trinity (Connecticut)", and a defunct program
is not the modern team sharing its name.

The run prints the whole map into its own log as well as the CSV, because
the artifact store is not reachable from everywhere this gets read — and
the staging database expires 90 days after the run that built it, while
the evidence should not.

### Result, 1946–1979 (2026-11-13)

405 distinct school names. 239 are carried by the provider as a team and
have an id, 1 matches our reviewed registry, and **165 need a verdict** —
753 games between them, about 3% of the archive. The committed map is
`data/identity/historical_schools.csv`, which is where the verdicts go as
they are made.

The unresolved are small colleges and service teams: Trinity (Texas) over
22 seasons, McMurry 17, Catawba and Newberry 16, then Cal State Northridge,
Randolph Macon, West Chester, Colorado College. Some are programmes that
later changed name (East Texas A&M, Texas A&M-Kingsville, Midwestern Texas
State, Missouri-Rolla); a few are not colleges at all (San Diego Navy,
Parris Island Marines, Hawaii All-Stars).

Two of the suggestions in that file are wrong — "Georgia State" for
Emporia State, "Northwestern" for Northeastern — which is the argument for
the rule that a suggestion is never applied.

### The verdict vocabulary

| verdict | meaning | rated? |
| --- | --- | --- |
| `small_college` | a college programme outside the major-college tier in that era | no |
| `service_team` | a military or base team | no |
| `exhibition` | an all-star or non-college side | no |
| `renamed` | the same programme under a later name; `canonical_name` must say which | follows that programme |
| `ambiguous` | cannot be settled from what we have; reported, never guessed | no |

Only `renamed` asserts that two names are one programme, so it is the only
one that must name the programme, and `tests/test_historical_identity_map.py`
refuses a `renamed` row without a canonical name. The other four mean the
name is never rated and only has to be a consistent opponent.

## Review before promotion

- Reconcile source counts with independent historical references for each season, especially empty or unexpectedly sparse years. (The provider's own records feed is now reconciled by `historical-reconcile.yml`; independent references are still outstanding.)
- Create a reviewed canonical historical school identity map including renamed, merged, discontinued, and similarly named programs. (`.github/workflows/historical-identity.yml` builds the **candidates**; the verdicts are a person's.)
- Create season-specific major-college / division eligibility mapping; do not infer historical membership from present-day FBS.
- Preserve ties, neutral-site uncertainty, missing scores/dates, and unknown classifications.
- Report all unresolved ambiguities; never silently discard records to satisfy modern FBS constraints.
- Freeze pre-promotion Elo/Merits fingerprints; compare outputs before and after promotion and require explicit approval.

The audit's `ready_for_source_reconciliation` flag means only that basic structural checks passed. It is **not** a declaration that the season is complete or suitable for rating calculations.
