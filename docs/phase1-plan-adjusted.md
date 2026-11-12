# Phase 1 plan, adjusted against the audit

Austin's step-by-step Phase 1 plan (working draft, November–December 2026) set
out twelve steps across four workstreams. The audit in
[`phase1-live-architecture-audit.md`](phase1-live-architecture-audit.md) has now
run part of it. This document says what the audit closed, what it changed, and
what the remaining work is in order.

**The plan's structure survives intact.** Nothing below reorganises it. Four
steps are done or nearly done, one step needs to be larger than drafted, two
new items appeared that the plan did not anticipate, and one of the plan's own
watch-outs turned out to be the single most important line in it.

---

## Status of the twelve steps

| Step | Plan's intent | Status after the audit |
| --- | --- | --- |
| **1** Frozen baseline | Document what production does today | **Mostly done.** Path traced, workflows inventoried. Missing: the architecture diagram and the issue register as artifacts. |
| **2** Audit Actions executions | Is the session workaround reliable? | **Done.** Answer: no. 50.8% cron delivery, median 106 min late, 46 of 48 handoffs leave a gap. |
| **3** Instrument freshness end-to-end | Measure real latency per layer | **Partly done, and bigger than drafted.** See below. |
| **4** Observe two complete live slates | Validate kickoff/halftime/OT/final | **Not started, and still required.** See below. |
| **5** Inventory sports, data, routes | Avoid breaking football | **Done.** Four leagues, routes, datasets and providers inventoried in §3–§5. |
| **6** Canonical identities and game states | Prevent cross-sport collisions | **Starting point established.** Convention already exists in three leagues; two concrete gaps found. |
| **7** Versioned HTTPS API contracts | One source of truth for web + iOS | **Not started.** Unblocked — and cheaper than drafted, see below. |
| **8** Validate providers and permissions | Don't promise feeds we can't use | **Half done.** NFL and NBA licences confirmed; NCAAM blocked; **CFBD unread and now the critical path.** |
| **9** Compare hosting and persistence | Smallest architecture that works | **Not started.** Audit supplies the cost baseline it needs. |
| **10** Measurable SLOs | Make "trustworthy" testable | **Not started.** Audit supplies the observed baseline it needs. |
| **11** Prioritise Phase 2 backlog | Turn investigation into work | **Not started.** |
| **12** Go/no-go review | Approve the foundation | **Not started.** Two of its four decisions now have evidence. |

---

## What the audit changed

### Step 2 is answered, and the answer is harder than the plan assumed

The plan's watch-out — *"the four-hour schedule does not guarantee
uninterrupted coverage"* — was right, and understated. Two independent faults:
the 4-hourly cron is delivered **50.8%** of the time, and the sessions are
scheduled to **abut with zero slack**, so a median start lateness of 106 minutes
opens a hole even when nothing is dropped.

The consequence, measured on the reader's side: **70 of 163 completed games
(43%)** had their last live update more than fifteen minutes before their final
appeared. Buffalo at Miami (OH) showed the wrong team winning for four hours
and twenty minutes.

**Effect on the plan:** Step 2's "consider independent publisher" response in
the risk register is no longer a contingency. It is the finding. The plan can
stop asking whether the session workaround is reliable and start costing its
replacement.

### Step 3 is partly done and needs to be wider than drafted

The plan asks for six timestamps from provider fetch to browser display. The
audit measured two legs:

| Leg | Measured | Source |
| --- | ---: | --- |
| CFBD fetch → snapshot written → `git push` done | **2–10 s** | job logs |
| `git push` → first visible on public Pages | **74–112 s** when confirmed | `measure_live_delivery.py` |
| Publish cadence while games are live | median 360 s, **p95 44 min** | git history |
| Pages → service worker → browser display | **unmeasured** | — |
| Provider event → CFBD has it | **unmeasured** | — |

Two things the plan did not anticipate:

1. **`src/measure_live_delivery.py` already exists and already runs** at the end
   of every session, writing its result to the job summary. Step 3 does not need
   a new measurement script for the publish leg; it needs that script's output
   collected, and its **120-second timeout widened**, because **27 of 40 sessions
   report "Pages visibility unconfirmed"** against a confirmed range of 74–112 s.
   The leg is characterised only up to two minutes and its tail is unknown. This
   is the cheapest open measurement in the whole phase.

2. **The two unmeasured legs are the two that need a provider or a browser.**
   Everything measurable from the repository alone has now been measured. The rest
   of Step 3 genuinely requires live observation, which is Step 4.

**Effect on the plan:** Step 3 splits. The repository-side half is done. The
remaining half — client display time and provider-event lag — merges into Step 4,
because both need a live slate anyway.

### Step 4 is still required, and the audit does not substitute for it

This is worth being plain about, because it would be easy to read the audit as
having done Step 4. It has not.

The audit reads **committed snapshots from git history**. A commit is not a
deployment, and the audit cannot see what a browser rendered, whether the
service worker served a cached copy, or whether CFBD itself was late. Its
numbers are a **lower bound** on reader-visible staleness: true staleness is
worse by at least the 75–112 s publish leg and by an unknown amount beyond it.

The plan's own watch-out stated this before any of it was measured: *"A fresh
fetch timestamp is not the same as a fresh score event; do not mislabel provider
delay as Elocast delay."* That line should govern Step 4. The audit's 43% figure
says the pipeline is failing; it does not yet apportion the failure between
GitHub's scheduler, the publish leg, the CDN, the service worker and CFBD.
Apportioning it is what Step 4 is for, and the fault tree the plan asks for is
exactly the right artifact.

One simplification: Step 4 no longer has to establish *whether* there is a
problem, only *where* it sits. That makes a two-slate observation a
measurement exercise rather than an investigation, and it can be scoped
tighter than drafted.

### Step 6 has a concrete starting point and two concrete gaps

The plan asks to *"keep BYU football distinct from BYU basketball while
optionally linking both to a shared institution."* The audit found the first
half already built and the second half entirely absent.

`sport:provider:kind:id` is enforced in three leagues — `nba:espn:team:25`,
`nfl:nflverse:player:00-0019596`, `ncaambb:cbbd:*` — each refusing a bare
provider id as a key. So Step 6 is not a design exercise from scratch; it is
**extending an existing convention to the one league that does not follow it**,
which is college football, keyed on `canonical_name` through
`data/reference/team_identities.json`.

The two gaps, in priority order:

1. **No institutional grouping exists anywhere.** No code path references both
   `ncaaf` and `ncaambb` team identities. This is a hard blocker on My Teams, which
   is P0.
2. **Whether CBBD and CFBD team ids coincide for the same school is
   unmeasured.** Both are CFBD-family providers, which makes a coincidence
   plausible and therefore dangerous. The plan's instruction — *"do not infer
   identity from display name alone"* — applies equally to not inferring it from a
   shared id space. This needs a measurement before any mapping is written.

### Step 7 is cheaper than drafted

The plan lists six `/v1/` endpoints and asks for an OpenAPI draft, example
payloads and contract tests. That stands. What the plan did not record is that
**the web client's half of the contract already exists**:
[`live-score-delivery.md`](live-score-delivery.md) defines `LIVE_FEED_URL` and
the exact shape an endpoint must return, and the page routes all three of its
live fetches through one accessor, `liveFeedUrl()`.

So `/v1/games?date=&league=` has a consumer and a specification today. Drafting
the contract can start from a shape already in production rather than a blank
page, and the resulting backend is testable against the live website long before
any SwiftUI exists.

### Step 8's open question is now the critical path

NFL (`nflverse-data`, CC BY 4.0) and NBA (`sportsdataverse`, MIT) are confirmed.
NCAAM is entitlement-blocked, so under the roadmap's own quality bar it cannot
be a launch league. That leaves **CFBD, whose terms are not written down
anywhere in the repository and which I could not read** — this container's
network policy denies `collegefootballdata.com`, and the request to allow it
could not be displayed on Austin's app version.

CFBD is the only live feed in the product. Its redistribution and commercial-use
terms gate the P0 promise for the flagship league, so this is the highest-value
unknown in Phase 1 and the cheapest to close: one person reading one page.

The plan's instruction stands and should be quoted into the rights matrix: *"Do
not assume a paid API key automatically grants redistribution rights."*

### Steps 9 and 10 now have their baseline

Step 9 needs a cost comparison; the audit supplies the starting point. **Current
recurring cost is approximately zero** — the repo is public, so Actions and Pages
are free while the live sessions alone burn ~21,800 runner-minutes a month. The
architecture is not cheap because it is efficient; it is free because it runs on
CI as though CI were production, and the reliability numbers are what that costs
instead of money. Any Phase 2 option is therefore a comparison against zero,
which makes the spend ceiling a product decision rather than an optimisation.

Step 10 needs an observed baseline before setting targets, and now has one:
p95 44 minutes while games are live, against the roadmap's aspiration of 15
seconds — a factor of roughly 175. The plan's watch-out that *"a 15-second
target is aspirational"* is confirmed with a number. A first SLO somewhere
around **p95 ≤ 60 s** would be a 44× improvement and is probably achievable
without a premium feed; 15 s should stay a stretch goal until a provider quote
exists.

---

## Two items the plan did not anticipate

**1. A second deadline.** `ui/` was **637 MB against GitHub Pages' hard 1 GB
ceiling** and grows with every season and refresh. Two rounds of re-encoding have
bought headroom; the trend is one direction. This is an architectural deadline
independent of August 2027, and it has the same answer as the freshness problem,
which is a backend. The plan's risk register has "Static publishing ceiling" as a
freshness risk; it is also a capacity risk, and the capacity one may bite first.

**2. The publish→visible leg is a third failure layer.** The plan's fault tree
lists "commit/publish delay" and "CDN cache" as separate branches. The audit
found the combined leg costs 75–112 s and cannot be confirmed at all in 68% of
sessions. Neither §2's scheduler faults nor §1's gaps include it. It deserves to
be its own line in the issue register rather than a branch of the tree.

---

## Recommended order for the rest of Phase 1

Reordered by what unblocks the most and costs the least, rather than by step
number. The plan's windows still work; this is the sequence inside them.

1. **Read CFBD's terms.** (Step 8) One page, one person, and it gates whether
   NCAAF can be a launch league at all. Nothing else in Phase 1 is this cheap or
   this decisive. If the terms forbid app redistribution, the launch-league
   question changes shape completely and everything downstream is re-planned — so
   it should not wait for a window.

2. **Widen `measure_live_delivery.py`'s timeout and collect a slate's worth of
   output.** (Step 3) A one-line change that closes the last repository-side
   measurement gap and turns 68% "unconfirmed" into a real distribution.

3. **Measure whether CBBD and CFBD team ids coincide.** (Step 6) Small, and it
   decides whether institutional grouping is a mapping table or a join.

4. **Draft the API contract from `LIVE_FEED_URL`'s existing shape.** (Step 7)
   Unblocked now, needs no provider and no slate, and it is what lets a Phase 2
   backend be built against the current website.

5. **Observe two live slates.** (Steps 3 + 4 merged) The remaining measurement,
   and the only one that needs real games. Scope it to apportioning the 43%
   between layers, not to re-establishing that it exists.

6. **Compare hosting options and set the ceiling.** (Step 9) Needs the CFBD
   answer and a feed quote first, or it compares against an unknown.

7. **Set SLOs, then order the Phase 2 backlog.** (Steps 10, 11) Both depend on
   everything above.

8. **Go/no-go.** (Step 12) Two of its four decisions — LIVE targets and launch
   leagues — now have evidence. The other two, budget and backend design, need
   items 1 and 6.

### What this does to the exit checklist

| Plan's completion criterion | Status |
| --- | --- |
| Live-score delivery measured across two slates, evidence retained | **Not met.** Repository-side measured; two-slate observation outstanding. |
| Workflow and publishing architecture diagrammed | **Substantially met** in prose; diagram outstanding. |
| Provider licensing, coverage, quotas, costs documented | **Not met.** CFBD unread. |
| Canonical identifiers and versioned API contract drafted | **Not met.** Starting point and gaps established. |
| Hosting/persistence compared, spend ceiling approved | **Not met.** Baseline supplied. |
| Phase 2 backlog ordered, go/no-go recorded | **Not met.** |

One of six met, one partly. That is roughly where a plan should be two weeks
into an eight-week phase, and the two cheapest remaining items — items 1 and 2
above — are both single-afternoon tasks that unblock disproportionate amounts of
the rest.

### One thing worth not doing

The plan is explicit that Phase 1 is *"primarily investigation, specifications,
decisions, and small instrumentation changes—not a full backend migration."*
That boundary is worth defending now that the audit has made the backend's
necessity obvious. The temptation after a 43% figure is to start building the
publisher this month. Items 1 and 6 above — the CFBD terms and the feed quote —
can still invalidate the design, and a backend built before them is a backend
built against an unknown.
