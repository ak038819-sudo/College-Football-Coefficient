# Phase 4 — Early college football archive, 1869–1945

**Status:** Source discovery. Nothing from this period is staged, rated or
published, and this phase authorizes none of those.

The specification is in [`historical-expansion-phases-4-6.md`](historical-expansion-phases-4-6.md).
Phase 3's conventions — read-only gates, reported ambiguity, no silent
coercion — carry over unchanged; see
[`historical-expansion-phase3.md`](historical-expansion-phase3.md).

## Why a probe comes first

The plan's first deliverable is a source inventory, not an import, and it
carries a warning that shapes everything here: **an empty API response is
not evidence that no games occurred.** Before deciding how to batch
1869–1945, or whether the provider can support the period at all, the
honest first move is to ask it what it has and write the answer down.

`scripts/probe_early_football_coverage.py`, run by
`.github/workflows/early-football-probe.yml`, does exactly that and stops.
It stages nothing, opens no database, and builds no rating. It is the only
historical workflow that needs `CFBD_API_KEY`, because it is the only one
that asks the provider anything — every Phase 3 gate runs on what the
backfill already staged.

## Three states, kept apart

For each season and season type the probe records one of:

| state | meaning | what it licenses |
| --- | --- | --- |
| games | the provider returned records | counted and described |
| **empty** | the provider answered, with nothing | a **gap to confirm** against another source |
| **incomplete** | the request failed | nothing at all |

Collapsing any two of these would be the real failure. An outage read as a
gap sends someone looking for records that were never missing; an empty
answer read as "no football that year" writes a historical claim the
provider never made. A season counts as empty only when *every* one of its
requests succeeded and returned nothing, and a run with any failed request
exits non-zero so a partial probe cannot be mistaken for a complete one —
after writing its report, because a failed probe is still evidence of how
far it got.

## What it writes down

Per season and season type: record count, how many games carry both
scores, ties, dates, venues and classifications, neutral site counted
three ways, plus duplicate ids, missing ids and rows whose own season
disagrees with the one requested. Across the range: every distinct school
name, every classification label, and the first and last season with any
games at all.

Those are the inputs to the decisions the plan defers to a person — how to
batch the period, which seasons need a source other than CFBD, and whether
any of it is suitable for modeling. The probe decides none of them.
