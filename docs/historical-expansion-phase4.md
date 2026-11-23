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

### Staged, 2026-11-23

Four batch runs, every one green, each staging exactly what the probe
predicted:

| batch | run | games |
| --- | --- | --- |
| 1869–1899 | [45143890880](https://github.com/ak038819-sudo/College-Football-Coefficient/actions/runs/45143890880) | 2,596 |
| 1900–1919 | [45144499737](https://github.com/ak038819-sudo/College-Football-Coefficient/actions/runs/45144499737) | 8,467 |
| 1920–1939 | [45144839012](https://github.com/ak038819-sudo/College-Football-Coefficient/actions/runs/45144839012) | 12,543 |
| 1940–1945 | [45145094081](https://github.com/ak038819-sudo/College-Football-Coefficient/actions/runs/45145094081) | 3,380 |

A fifth run over the whole range,
[45145340734](https://github.com/ak038819-sudo/College-Football-Coefficient/actions/runs/45145340734),
answered **SKIP for all 222 partitions** and fetched nothing. That is the
checkpointing proven end to end across the widened range, and it gives one
audit over the whole archive:

| | |
| --- | --- |
| staged games, 1869–1979 | **49,692** |
| completed season-and-type partitions | 222 |
| missing scores | 0 |
| missing dates | 0 |
| ties | 2,349 |
| games with a classification unknown on one side or both | 10,828 |
| blocked partitions | **1** |

The arithmetic closes: 22,706 games from Phase 3 plus 26,986 from Phase 4
is 49,692, and the ties add up the same way (680 + 137 + 515 + 856 + 161).
Phase 3's seasons were not touched — every one of them skipped.

**The one blocked partition is 1871 regular**, flagged
`empty_regular_season_requires_source_review`. The gate found the same
thing the probe did and refused to pass it, which is the behaviour both
were built for: an empty regular season goes to a person, not through.

Nothing is promoted. `production_promotion_allowed` is `false`, no rating
reads any of this, and `ui/` is untouched.

### What this settles, and what it does not

It settles that Phase 4 does not need a second provider to exist. It does
not settle accuracy, identity or eligibility: 1,214 school names is three
times Phase 3's 405, and every one of them will need the same treatment.
Nothing is staged, and `production_promotion_allowed` is `false`.

## Widening the gates, 1869–1979

Staging the early archive left the four Phase 3 gates still answering only
for 1946–1979. That is the shape of mistake worth naming: each gate filters
by season, so a gate left at its old range does not fail on the early
period — it returns a clean report about a third of the archive, and a
clean report is exactly what nobody re-reads.

So the range is now one range. Reconciliation, identity, eligibility and
preservation all take **1869–1979**, as the backfill and its audit already
do, and each one's workflow offers 1869 as its default rather than 1946.

### One range, named in nine places

A gate has to carry the range twice over — once in the script, which must
refuse an out-of-range request on its own, and once in the workflow, which
cannot import a Python constant. Across the five scripts and four
workflows that is nine copies of one number, and nine copies is how a
widening goes half-done.

`tests/test_historical_range.py` holds the range once and asserts every
copy matches it: the `FIRST, LAST` line in each gate, the guard in each
workflow, and the absence of any lingering `default: "1946"`. It is a test
about numbers rather than behaviour — each gate's own file tests the
behaviour — and it is the thing that fails first the next time the archive
widens.

It also pins the one range that is deliberately *different*:
`probe_early_football_coverage.py` stays at 1869–1945, because widening the
probe would re-ask the provider for seasons already staged.

### The order the gates have to run in

They are not independent. Identity and eligibility both read the records
database that reconciliation fills from CFBD's `/records` feed, so:

1. **Reconciliation** — the only one that needs the network, and the only
   one that needs `CFBD_API_KEY`. It checkpoints per season exactly as the
   backfill does, so a 1869–1945 run adds to the records chain and reports
   `SKIP` for the 34 seasons already collected.
2. **Identity** and **eligibility**, which read those records beside the
   staged games.
3. **Preservation**, which needs neither: it reads the staged payloads.

Nothing here runs a gate. Widening is a source change, and what each gate
then says about 1869–1945 is a result to be recorded when it is measured —
not predicted here. Identity in particular faces 1,214 school names
against Phase 3's 405, and every unresolved one needs a verdict from a
person.

### A trap closed on the way past

`collect()` in `reconcile_historical_sources.py` carried `fetcher=fetch` in
its signature. A default argument binds at def time, so the module
attribute could not be replaced and any test that drove `main()` would
have reached the provider for real — the same trap the probe hit, now
caught a second time. The default is resolved inside the function, and
there is a test that drives `main()` online through a replaced `fetch`;
reverting the signature makes that test try to open a socket.

## The gates over the whole archive, 2026-11-23

All four, run in the order they depend on each other, over 1869–1979 in one
pass each. Every run green.

### Reconciliation

Run [45147630470](https://github.com/ak038819-sudo/College-Football-Coefficient/actions/runs/45147630470).
77 new seasons of the `/records` feed collected, the 34 already collected
reported `SKIP`.

| | |
| --- | --- |
| team-seasons compared | **23,439** |
| agreeing with the records feed | 16,927 |
| with findings | 6,512 |
| of those, `no_reference_line` | 6,510 |
| of those, **`regular_result_mismatch`** | **2** |
| all-zero reference lines beside nothing staged | 5,479 |
| staged names the records feed never carries | 1,105 |

`no_reference_line` is the identity map's work list, not a disagreement:
the records feed carries lines for the programs it knows of, and in the
1890s it does not know of most of them. An all-zero line beside nothing
staged is the two feeds agreeing that a team did not play.

### The two mismatches, and what they are

Phase 3's 1946–1979 run had **zero** mismatches. The early period has two,
and both are in 1929:

| 1929 | staged (games feed) | records feed |
| --- | --- | --- |
| Boston College | 4 games, 1-2-**1** | 4 games, 1-2-**0** |
| Villanova | 8 games, 6-1-**1** | 8 games, 6-1-**0** |

Neither is an import fault, and the arithmetic is what says so: **the
records feed's line does not sum to its own game count.** It claims four
games for Boston College and gives three results, eight for Villanova and
gives seven. The staged line sums correctly in both cases. So the
disagreement is inside the provider's *derived* records feed, and the games
feed — the one that was staged — is the internally consistent one.

Both lines are short exactly one tie in the same season, which invites the
guess that one shared game was counted in neither column. That is a guess:
nothing here confirms Boston College and Villanova met in 1929, and the
gate does not claim it.

This is also why the gate now **names** its mismatches rather than only
counting them. Two counted mismatches sent me to a rendered run page and a
report artifact, and the artifact store is not reachable from everywhere
this is read. The summary carries up to 25 of them with both feeds'
games-wins-losses-ties.

### Identity

| | |
| --- | --- |
| distinct school names, 1869–1979 | **1,308** |
| matched to a records-feed team | 326 |
| **unresolved, needing a verdict** | **982** |
| games played by unresolved names | 6,919 |

1,308 rather than 1,619 because 311 names appear in both eras — the 1,214
of 1869–1945 and the 405 of 1946–1979 overlap by exactly that much, which
is the two measurements agreeing.

The 982 are a long tail, not 982 equal problems: **481 appear in a single
season** and 139 in two, while the heaviest are small colleges that played
for decades — Trinity (Texas) in 51 seasons, Gettysburg 50, Randolph Macon
48. Only 27 carry a resemblance to a known name, and a suggestion is a
lead, never a resolution.

`data/identity/historical_schools.csv` is **not** regenerated here. Every
unresolved name in it has a verdict, and 982 new ones do not; ruling on
them is its own piece of work.

### Eligibility

| | |
| --- | --- |
| team-seasons classified | 23,439 |
| major-college | 10,375 |
| lower-division | 6,554 |
| unknown | 6,510 |
| **conflicts** | **0** |

Zero conflicts over the whole archive: nowhere does a game row's
classification contradict the same team's record line for that season.

The 6,510 unknown is the same number as reconciliation's 6,510
`no_reference_line` findings. That is **agreement in size, not a proven
identity** — the two reports were not cross-checked row by row, and equal
counts are not the same set. The mechanism makes the coincidence
unsurprising: a team-season with no record line has no second source to
classify it from.

### Preservation

| | |
| --- | --- |
| games audited | **49,692** |
| payloads verified against their sha256 | 49,692 |
| corrupt or unreadable | 0 |
| **columns that drifted from their payload** | **0** |
| missing scores / missing dates | 0 / 0 |
| ties | 2,349 |
| **games with a venue** | **0 of 49,692** |
| neutral site: true / false / unknown | 425 / 49,267 / **0** |

110 seasons present, which is 1869–1979 less 1871.

Two findings from Phase 3 are now measured over the whole archive rather
than inferred: **the provider has no venue for any of the 49,692 games**,
and **neutral site is two-valued everywhere** — 425 true, 49,267 false, not
one unknown, because `neutralSite` is a required non-nullable boolean in
the provider's schema. A neutral-site flag is uncorroborable anywhere in
this archive, which Phase 5 has to take as given when it fits a home
advantage.

### Where that leaves it

The archive is staged, faithful to the provider column by column, and
reconciled against the provider's own records with two disagreements, both
explained and both the provider's. What it is **not** is identified: 982
school names have no verdict, and 6,510 team-seasons cannot be classified
on this evidence. Nothing is promoted; `production_promotion_allowed` is
`false` in every one of these reports.
