# Review: Elocast Live-State Backend, design v0.1

Reviewed 2026-11-08 against `main` at `f4223089` plus PR #191. Austin's
proposal is sound and I would build it. This document records four things to
change before Milestone B, two of which are measured defects in the proposal's
numbers rather than matters of taste.

## What is right, and worth not re-litigating

- **One ingestion process, many readers.** This is the correct core and it is
  the thing that removes the constraint the audit found. 500 readers must not
  become 500 provider requests.
- **Keep Pages, keep the Python/SQLite pipeline, add a small service.** Right
  scope. The audit's finding was never that the analytics stack is wrong; it was
  that live state is being carried by git commits.
- **Shadow backend before public exposure, with the old publisher left intact.**
  Right sequencing, and it matches the plan's rule that Phase 1 is not a
  migration.
- **Three separate timestamps** (`provider_updated_at`, `observed_at`,
  `published_at`), with `provider_updated_at` nullable. This is exactly right and
  it is the thing the current pipeline cannot do. Keep the nullability strict:
  the moment it gets defaulted to `observed_at`, provider lag silently becomes
  our lag.
- **§9's caveat** that snapshot age is measured from our own observation and
  that true event latency needs independent reference timestamps. That is the
  distinction the audit could not make and the one most often fudged.
- **Game-aware polling** over fixed 15-second polling. See §1 below for why this
  matters more than the proposal assumes.
- **Not buying a feed yet.** Consistent with
  [`live-engine-provider-decision.md`](live-engine-provider-decision.md).

## 1. The request budget is understated by 3× (§4)

**§4 assumes "Provider requests per polling cycle: 1". The current fetcher makes
three.** `src/fetch_live_scores.py` issues:

| Call | Purpose |
| --- | --- |
| `GET /scoreboard?classification=fbs` | the live scoreboard |
| `GET /calendar` | find the current week |
| `GET /games/players` | the weekly player box-score batch |

and `src/run_live_score_session.py` says so in as many words next to
`DEFAULT_IDLE_INTERVAL`: *"Three CFBD requests a tick is what makes it worth
slowing down at all."*

So every figure in §4's table is a third of the real number:

| Polling schedule | §4 says | at 3 requests/cycle |
| --- | ---: | ---: |
| 8 h, one day/week | ~8,320 | **~24,960** |
| 12 h, two days/week | ~24,960 | **~74,880** |
| 24/7 | 172,800 | **518,400** |

(The arithmetic in §4 is otherwise correct — 86,400 / 15 = 5,760/day, and the
52/12 weekly conversions check out. The error is only the per-cycle multiplier.)

**The fix is a design improvement, not just a corrected number.** The live
scoreboard genuinely needs one request per cycle — `/scoreboard` alone. The
other two belong to the box-score path, which does not need 15-second
granularity and should poll on its own much slower schedule. Splitting them
makes §4's "1 request per cycle" true rather than aspirational, and it is
cleaner anyway: the live worker's job is game state, not statistics.

## 2. The quota question, with the evidence we actually have

§4 cites CFBD's documented free tier of 1,000 calls/month and correctly says the
paid allowance needs verifying. Two things to add.

**Measured, same provider family:** on 2026-11-03 the *basketball* side of this
account (CBBD) was refused with `{"message":"Monthly call quota exceeded."}`
after roughly 1,010 requests in the month, with no `Retry-After`. So ~1,000/month
is a real observed cap somewhere on this account, and it is monthly rather than
rolling.

**Measured, football, which is the relevant key:** the live sessions currently
run roughly 3 ticks × 10/hour × ~12 session-hours a day, which is on the order
of **10,000–11,000 CFBD requests a month** — and I found **zero quota refusals
across the last 8 sessions** (searched the job logs for `quota exceeded`,
`Retry-After`, genuine HTTP 429s; the only apparent hits were the digits "429"
inside log timestamps).

So the football key already sustains roughly ten times the 1,000/month figure
without complaint, which means the free-tier number is not our ceiling. It also
means the useful framing is not "is 8,320/month safe?" — we are already above
that today. The actual allowance remains unverified, and it is the one number
that decides whether 15-second polling is affordable at all.

**Caveat worth stating:** whether football and basketball share one account-level
allowance is *not* established. If they do, the basketball exhaustion on
2026-11-03 would have taken football down with it, and it did not — so they
appear separate. That is an inference from an absence, not a measurement, and it
should be confirmed rather than relied upon.

## 3. The feature flag cannot do what §7 and §8 need

This is the finding most likely to bite, because the proposal's rollback story
depends on it.

**`LIVE_FEED_URL` is baked in at build time.** `build_dashboard.py` substitutes
`__LIVE_FEED_URL__` into the HTML (line 117), and the page reads a module
constant:

```js
const LIVE_FEED_URL = '__LIVE_FEED_URL__';
function liveFeedUrl() {
  const base = LIVE_FEED_URL || 'data/live_scores.json';
```

Three consequences:

1. **Rollback is not "a single frontend feature-flag change."** It is: set an
   environment variable, rebuild `dashboard.html`, commit, and run
   `ci-and-deploy.yml` — the exact publishing path whose latency and reliability
   the audit criticised. In an outage that is the slowest possible lever.
2. **Milestone D's canary cannot be scoped to internal testers.** The baked
   value is global per build, so it is all readers or none.
3. It is one more thing that only changes by deploying, which is the pattern
   this whole project is trying to get away from.

**Recommended change, and it is small.** Make the endpoint a **runtime**
decision:

- Keep the baked value as the *default*, not the only source.
- Let a query parameter or `localStorage` key override it, which gives
  Milestone D a real canary with no deploy.
- Have the page fall back to `data/live_scores.json` automatically when the live
  endpoint fails or returns a snapshot older than the stale threshold.

That last point is the important one: **§7 already asks for exactly this
behaviour** ("If no live snapshot exists, it can fall back to our existing static
game data"). Implementing it is what makes rollback free — the page degrades by
itself and nobody has to deploy anything. Build the fallback and the rollback
problem disappears as a side effect.

## 4. The normalized game object should use the repository's identity convention (§5)

§5's example uses `"game_id": "cfbd:123456"` and `"home": {"id": "team-home"}`.
Two changes, both grounded in measurements already in the repo.

**Namespace the keys.** Three of the four leagues already enforce
`sport:provider:kind:id` and refuse a bare provider id as a key — see
`src/nba/identity.py`, `src/nfl/identity.py`, `src/ncaambb/identity.py`. So the
game key should be `ncaaf:cfbd:game:123456`, not `cfbd:123456`. Since this
service is explicitly meant to be the multi-sport foundation, it should adopt
the convention the rest of the repository landed on rather than introduce a
fifth spelling.

**College teams need `institution_id`.** Measured 2026-11-08
([`identity-cross-league-measurement.md`](identity-cross-league-measurement.md)):
CBBD and CFBD team ids agree for **0 of 139** schools, and **81 CBBD ids are
valid CFBD football ids belonging to a different school** — a naive join returns
a confidently wrong team rather than nothing. The safe cross-league key is the
ESPN team id, which both feeds already carry.

Since §5 is the contract every future sport will be built against, and My Teams
is P0, the team object should carry it:

```json
"home": {
  "team_key": "ncaaf:cfbd:team:57",
  "institution_id": 57,
  "score": 21
}
```

Professional teams carry no `institution_id`. Without this field in the
contract, My Teams has no way to put a school's football and basketball teams
together, and it would have to be retrofitted into every adapter later.

## 5. Smaller notes

- **§6's "confirm again after 30–60 seconds" on a final is right**, and worth
  strengthening: the audit found finals arrive correct but late. A final should
  also be *immutable without an explicit correction record*, so a provider
  outage cannot revert it — §6 says this in prose and it should be a schema
  constraint, not a convention.
- **§7's freshness thresholds** (fresh <30 s, delayed 30–120 s, stale >120 s)
  are reasonable, but note the current publish leg alone measured 74–112 s. On
  the new architecture that leg disappears; the thresholds should be re-checked
  against the shadow service's real numbers before they ship, or "delayed" will
  be the normal state on day one.
- **Cloudflare specifics are unverified from here.** Durable Object alarms,
  SQLite storage, the 100,000 free requests/day and the paid Workers minimum are
  all taken from the proposal; this container's network policy blocks the vendor
  sites, so I could not read the pricing or limits pages. Treat §3's numbers as
  needing the same confirmation §10 already asks for.
- **§11's layout is fine.** One addition: the finalization path needs somewhere
  to live that the existing pipeline can read, and `run_pipeline.py` currently
  runs before `build_elo.py` — worth checking where an idempotent finalization
  event is consumed so it does not land in the gap that already caught
  `build_coach_metrics.py`.

## Verdict

**Build it.** The architecture is right, the sequencing is right, and the
proposal is unusually careful about not claiming more than it has measured.

Before Milestone B: fix the 3× request budget and split the box-score path from
the live path (§1), and make the frontend endpoint a runtime decision with
automatic static fallback (§3) — the second is what turns the rollback plan from
a deploy into a no-op. Before the contract is frozen: adopt
`sport:provider:kind:id` and add `institution_id` (§4), because every later sport
inherits that shape.

Milestone A's first item — verify PR #191's evidence — is ready: the audit, its
three corrected claims, and the measurements behind §1 and §3 above are all in
that PR, with CI green.
