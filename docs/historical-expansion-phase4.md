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

## Result, 1869–1945 (2026-11-18)

Run [44263813322](https://github.com/ak038819-sudo/College-Football-Coefficient/actions/runs/44263813322),
the full range: 154 requests, **none failed**.

| | |
| --- | --- |
| games | **26,986** |
| seasons with games | 76 of 77 |
| seasons empty | 1 (**1871**) |
| seasons never answered | 0 |
| first / last season with games | 1869 / 1945 |
| distinct school names | **1,214** |
| conferences named | 63 |

**The provider can support the whole period.** It answers for 1869 — the
first season anyone played — and every season after it but one, which is
not what a probe of a 157-year-old sport had any right to expect.

### What the records carry

| field | games |
| --- | --- |
| both scores | 26,986 (all) |
| a date | 26,986 (all) |
| **a venue** | **0** |
| ties | 1,669 |
| neutral site: true | 86 |
| neutral site: false | 26,900 |
| neutral site: unknown | 0 |
| home classification | 24,463 |
| away classification | 19,485 |

No missing ids, no duplicate ids, and no record whose own season
disagrees with the one requested.

Every game has a score and a date, which is more than the period
promised. Classification is partial and lopsided — 2,523 games carry no
home classification and 7,501 no away classification — which is the
expected shape for a period largely predating the divisions themselves,
and the same per-team-season treatment Phase 3 built will apply.

Two findings carry straight over from Phase 3's preservation audit, and
are now known to hold for all 111 seasons from 1869 to 1979, not just
the 34 Phase 3 measured:
**the provider has no venue for any game before 1946 either**, 0 of
26,986; and **neutral site is two-valued here too**, 86 true against
26,900 false and not one unknown, because `neutralSite` is a required
non-nullable boolean in the provider's schema. Nothing in the archive can
corroborate a neutral-site flag anywhere from 1869 to 1979.

### 1871

The one empty season. Under this phase's own rule it is a **gap to
confirm against another source**, not a season without football — though
the conventional history is that no intercollegiate games were played
that year, so this is a confirmation to make rather than a hole to fill.
The probe does not make that call, and the report does not record it as
one.

### Shape of the period

Games climb from 2 in 1869 to a few hundred by 1900 and roughly 600–700
through the 1930s. Two dips interrupt it: **1918** falls to 292 from 476
the year before, and **1943** to 389 from 664. Both match the historical
record — the influenza pandemic with the end of the First World War, and
the Second World War. That the curve has those notches in the right
places is the best evidence available here that the feed is the period's
real shape and not a partial scrape.

## Staging, 1869–1945

`.github/workflows/historical-backfill.yml` now takes **1869–1979**, and
Phase 4 stages into **the same chain Phase 3 built** rather than opening a
second one. There is one archive, so there is one staging database.

That is safe because of two properties the backfill already had. The
importer checkpoints per season and season type, so a batch covering
ground already staged reports `SKIP` and refetches nothing — the 1946–1979
seasons are untouched by any Phase 4 run. And every gate that reads this
database filters by season (`WHERE season BETWEEN ? AND ?`), so widening
the archive cannot change what any of them says about 1946–1979; Phase 3's
results stand as recorded.

Batches follow the plan's suggestion, and the coverage matrix supports the
boundaries — the first is light and the last is dense:

| batch | games |
| --- | --- |
| 1869–1899 | 2,596 |
| 1900–1919 | 8,467 |
| 1920–1939 | 12,543 |
| 1940–1945 | 3,380 |
| total | 26,986 |

The Phase 3 gates — reconciliation, identity, eligibility, preservation —
still carry their own 1946–1979 ranges and are **not** widened here.
Widening them is its own piece of work, and the identity gate in
particular has 1,214 names to face rather than 405.

### What this settles, and what it does not

It settles that Phase 4 does not need a second provider to exist. It does
not settle accuracy, identity or eligibility: 1,214 school names is three
times Phase 3's 405, and every one of them will need the same treatment.
Nothing is staged, and `production_promotion_allowed` is `false`.
