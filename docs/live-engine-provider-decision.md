# Does NCAAF need a different live provider?

Written 2026-11-08, in answer to a direct question: *NCAAF is the bread and
butter; if CFBD isn't giving us an effective live game engine, find something
that does.*

The instinct is right — NCAAF does not have an effective live game engine
today. **But CFBD is not what is stopping it**, and buying a faster feed onto
this architecture would change nothing a reader can see. The evidence is below,
because this is a spending decision and the measurements point somewhere other
than the provider.

## 1. CFBD is delivering live game state

Measured across the 456 published snapshots of the last 15 days. Taking only
**normal-cadence pairs** — consecutive snapshots 300–450 s apart, so the
pipeline was working as designed — and only games `in_progress` in both:

| | |
| --- | ---: |
| Observations | 3,359 |
| CFBD's game clock or period had **advanced** | **3,049 (90.8%)** |
| Clock completely unchanged | 310 (9.2%) |
| Score had changed | 1,010 (30.1%) |

When we actually ask CFBD, it has new game state nine times out of ten. The
9.2% unchanged is what halftime, timeouts, reviews and clock stoppages look
like at six-minute granularity, not a stale feed.

Separately, from the audit: **every final score arrived and every one was
correct.** The 43%-of-games failure was games freezing *between* polls, then
jumping straight to a correct final. That is not a provider that lacks the
data. It is a pipeline that did not ask.

## 2. The six-minute cadence is a GitHub Pages limit, not a CFBD limit

This is the decisive fact, and it is written in our own code.
`src/run_live_score_session.py`:

```python
# Pages builds from a branch are soft-limited to roughly ten an hour, so a
# shorter interval would spend publishes it cannot deliver.
DEFAULT_INTERVAL = 360
```

Ten builds an hour is one every six minutes. And we are **already running at
that ceiling**: the busiest clock hours in the sample contain exactly **10
publishes**, repeatedly.

| Busiest hours observed | Publishes |
| --- | ---: |
| 2026-10-24 19Z, 23Z; 10-25 00Z, 01Z, 05Z | **10 each** |

So the publishing platform caps the product at six-minute granularity
**regardless of the provider**. A push feed delivering sub-second updates would
be throttled to the same six minutes, because every update has to become a git
commit and a Pages build before a reader sees it. On top of that cap sits the
75–112 s publish leg and the scheduler gaps in the audit.

**A new provider on this architecture cannot beat six minutes.** The roadmap's
target is 15 seconds.

## 3. Therefore the order of spending is fixed

The backend has to come first, and not because a backend is more interesting:
**until live state leaves git and Pages, a provider upgrade is unobservable.**
You would pay for a feed and measure no improvement, because the improvement
would be discarded at the publish step.

Once a backend owns live state and serves the existing site through
`LIVE_FEED_URL`, the picture inverts: the delivery cap disappears, and **then**
the provider becomes the binding constraint, and a feed upgrade buys exactly
what it promises. At that point the questions below are worth money.

A useful consequence: the backend is also what makes the provider question
*answerable*. Today we cannot tell how far behind the real game CFBD itself is,
because our own six-minute sampling is coarser than the lag we would be trying
to measure.

## 4. What is genuinely unknown about CFBD

CFBD is not the current bottleneck. That is **not** the same as saying it is the
right provider for a 15-second product. Three things about it are unmeasured and
unread, and they decide whether it survives Phase 2:

1. **Its own event lag.** How long between a touchdown and CFBD having it? Our
   sampling is too coarse to see this. Measurable in Phase 2 against a second
   source, or against a clock on a televised game.
2. **Its rate limits.** At 15-second polling a single game day is roughly
   **240 requests an hour**, against 10 today. Whether CFBD permits that, and at
   what tier, is not documented anywhere in this repository.
3. **Its terms.** Whether redistribution to end users of a commercial iOS app is
   permitted at all. This is the audit's largest open item and still unanswered.

If any of those three comes back wrong, CFBD has to be replaced — and that is a
real possibility, not a formality. The point of this document is only that the
replacement is a Phase 2 decision informed by Phase 2 measurements, not a fix
for the problem we have now.

## 5. The provider comparison I could not do

Austin asked for alternatives. **I could not research them**: this container's
network policy denies every relevant host — `sportsdata.io`,
`developer.sportradar.com`, `collegefootballdata.com` — through both the shell
and the fetch path, and the request to allow them could not be displayed on the
app version in use. Nothing in this section is from a vendor page, and nothing
is filled in from memory.

What `live-data-options.md` already recorded from a 2026-10-04 review, which
should be re-verified rather than trusted:

| Candidate | What was noted then |
| --- | --- |
| Sportradar NCAA Football | Coverage flags distinguish full play-by-play and extended box scores; `expected_latency` reports feed lag; **realtime customers can use push**. |
| SportsDataIO College Football | Live game state and a live/final box-score endpoint; guidance suggests **polling every 3–5 s** under an appropriate subscription. |

Push support is the feature that matters most, because it removes polling
cadence from the equation entirely — which is the same shape of fix as removing
the Pages build from the delivery path.

### The questions to put to a vendor

Worth asking in one email, so the answers are comparable:

1. NCAAF live score and game-state latency, p50 and p95, and whether push
   (websocket/streaming) is offered or only polling.
2. Whether FBS coverage is complete, including weeknight MAC games and bowls —
   the games that froze worst in the audit were Tuesday and Wednesday MAC games.
3. Play-by-play and live box score: included, or a separate tier.
4. Rate limits or call allowance at the intended polling rate.
5. **Redistribution rights for a commercial iOS app**, in writing.
6. Price, and what drives it.

Question 5 is the one that disqualifies, so it goes first in practice.

## What I would do

1. **Don't buy a feed yet.** It would be unobservable until the backend exists.
2. **Build the Phase 2 backend and point the existing website at it.** The
   contract already exists (`LIVE_FEED_URL`), so this is testable on the live
   site with no iOS code, and it is what removes the six-minute cap.
3. **Then measure CFBD properly** — its own event lag and its rate limits, which
   only become measurable once we can poll faster than six minutes.
4. **Read CFBD's terms now**, in parallel, because a "no" there forces the
   provider change regardless of performance and is the one answer that
   re-plans everything downstream.
5. **Email both vendors the six questions** whenever convenient. The answers
   keep, and having them before the backend is finished costs nothing.

The short version: CFBD may well need replacing, but not for the reason it looks
like today, and not before the thing that is actually throttling the product
gets fixed.
