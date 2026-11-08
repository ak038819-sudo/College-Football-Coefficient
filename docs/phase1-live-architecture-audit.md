# Phase 1 audit: architecture and the live feed

Measured 2026-11-08, against `main` at `f4223089`. This is the deliverable the
Elocast roadmap's first phase asks for: *"Document existing refresh paths,
production freshness, API contracts, licensing, cost envelope and launch league
shortlist."*

It extends [`live-data-options.md`](live-data-options.md), which measured the
**delivery** side a month ago and concluded that GitHub's cron, not the
provider, was the bottleneck. That conclusion was right and the fix it
motivated — long-running polling sessions in
[`live-score-session.yml`](../.github/workflows/live-score-session.yml) —
reduced the problem. **It did not solve it.** This audit measures what a reader
of the published site actually sees, and the answer is worse than the delivery
numbers alone suggest.

---

## 1. The headline: 43% of games freeze before their final appears

Over **15 days** (2026-10-24 16:05Z to 2026-11-08 07:15Z), every published
revision of `ui/data/live_scores.json` was read from git — **456 snapshots**,
covering **13 days on which at least one game was live**.

For each completed game, compare the last snapshot that showed it `in_progress`
against the first snapshot that showed it `completed`:

| | |
| --- | ---: |
| Completed games tracked | 163 |
| Games whose last live update was **>15 min** before their final appeared | **70 (43%)** |
| Of those, games that scored points after their last shown update | 51 |
| Median points the published scoreboard never showed, for those games | 10 |
| Worst case | **55 points** |

The worst ten, with the score the site showed and how long it showed it:

| Game | Froze at | For | True final | Points never shown |
| --- | --- | ---: | --- | ---: |
| Buffalo at Miami (OH) | 3–7, 1st qtr | 260 min | 34–31 | **55** |
| Sam Houston at Jacksonville State | 0–10, 2nd qtr | 147 min | 14–47 | 51 |
| Virginia Tech at SMU | 14–20, 2nd qtr | 180 min | 28–51 | 45 |
| Boise State at Colorado State | 43–17, 3rd qtr | 147 min | 64–38 | 42 |
| Air Force at Army | 3–0, 2nd qtr | 147 min | 24–21 | 42 |
| Ball State at Massachusetts | 10–14, 2nd qtr | 281 min | 30–31 | 37 |
| Kennesaw State at Middle Tennessee | 19–17, 3rd qtr | 279 min | 44–28 | 36 |
| Ohio at Akron | 7–6, 2nd qtr | 260 min | 28–20 | 35 |
| South Florida at East Carolina | 7–0, 2nd qtr | 180 min | 17–20 | 30 |
| Central Michigan at Eastern Michigan | 14–0, 2nd qtr | 281 min | 35–7 | 28 |

Buffalo at Miami (OH) is the clearest statement of the problem. For **four
hours and twenty minutes** the site showed Miami leading 3–7 in the first
quarter. Buffalo won 34–31. A reader following that game was not shown a stale
number; they were shown the wrong team winning, for the length of the game and
beyond.

Note what these rows are *not*. They are not provider errors, and they are not
games the pipeline failed to fetch. Every one of these finals did eventually
arrive and is correct in the archive. The failure is entirely in **when** the
published file was rewritten.

### Staleness while games are live

Taking every interval between consecutive snapshots in which the earlier
snapshot showed at least one game in progress — 437 intervals, 93.5 hours of
live-coverage time:

| | |
| --- | ---: |
| Median gap | **360 s** (6 min — the design target) |
| p95 gap | **2,639 s (44 min)** |
| Worst gap | **17,424 s (290 min)** |
| Intervals longer than 15 min | 24 (5.5% of intervals) |
| Share of live-coverage **time** spent inside one of those 24 gaps | **55.6%** |

That last row is the one to read twice. The median looks healthy because most
intervals are fine. But the long gaps are so long that **more than half of all
the wall-clock time during which games were live** fell inside a gap of over
fifteen minutes.

Measured against the roadmap's proposed gate of **p95 ≤ 15 s**, the current
architecture is off by a factor of roughly **175** at p95.

One caveat, stated precisely: a game recorded as `in_progress` at the start of
a long gap may have finished during it. So these intervals measure "the
published scoreboard asserted that games were live and did not change for N
minutes" — which is the user-visible failure — rather than "N minutes of
genuinely live play went unpolled."

---

## 2. Why: the session chain depends on a cron GitHub does not deliver

`live-score-session.yml` starts a job every four hours and polls in-process for
four hours, so that *"four-hour sessions hand off to each other with nothing in
between."* The design is sound. Its premise is not.

Across **66 session runs** (2026-10-11 to 2026-11-08), and over the same
15-day window as §1 where noted:

| | |
| --- | ---: |
| Scheduled starts specified by the crons, over 21 full days | 126 |
| Starts GitHub actually created | **64 (50.8%)** |
| Starts per day in the §1 window | **3.2** (design: 6) |
| Median start lateness vs. the cron | **106 min** |
| p95 start lateness | 230 min |
| Worst start lateness | 238 min |
| Median session duration | 242 min (design: 240) |
| Share of the §1 window with a session running | **37.3%** |

Two independent faults, and the second is the interesting one:

1. **Half the firings are dropped.** The 4-hourly cron is delivered 50.8% of the
   time. This is far better than the 3.1% `live-data-options.md` measured for the
   5-minute cron, so the "use a low-frequency cron" insight was correct — but
   "low-frequency enough to be delivered" turned out to mean *half* delivered, not
   reliably delivered.

2. **The sessions abut with zero slack, so lateness alone opens holes.** A
   session runs for exactly four hours and the next is scheduled exactly four
   hours later, leaving no margin. A median start lateness of 106 minutes
   therefore opens a hole even when a firing is not dropped at all, and the two
   faults compound: in the §1 window, **46 of 48 consecutive session pairs leave
   a gap**, with a median gap of **299 minutes** and a smallest of **77
   minutes**. Across all 66 runs, 59 of 65 pairs leave a gap and 6 overlap
   instead.

The 37.3% coverage figure overstates the damage on its own, because 16 of the 66
sessions ran under an hour — those are sessions that correctly exit early when
nothing is scheduled, which is the script working as intended. The honest
measure of harm is §1, which counts live games rather than clock time.

The concrete instance behind the Boise State row: run 65 ended at
2026-11-08T00:10Z and run 66 started at 05:50Z — **5 h 40 m with no session at
all**, across the heart of a Friday night slate with 11–14 FBS games in
progress. Only two snapshots were published in that window, both from the
surviving 5-minute cron limping through at its 3.1% rate.

**No edit to a workflow file fixes this.** The dropped and delayed firings are
GitHub's scheduler, and the roadmap already names the right answer: the backend
should own live state, and GitHub Actions should keep archival ETL, scheduled
analytics and deploys.

---

## 3. Refresh paths, as they exist today

29 workflows. The ones that touch published data:

| Path | Trigger | What it writes |
| --- | --- | --- |
| `live-score-session.yml` | 4-hourly cron, in-process polling loop | `ui/data/live_scores.json` |
| `live-scores.yml` | 5-minute cron (3.1% delivered) | the same file, as fallback |
| `ci-and-deploy.yml` | push to `main`, dispatch | regenerates `ui/data` and `dashboard.html`, publishes Pages |
| `nfl-refresh.yml`, `nba-refresh.yml`, `cbb-refresh.yml` | cron | each league's export |
| `sync-people.yml`, `sync-awards.yml`, `matchup-intelligence.yml` | cron, dispatch | archive layers |
| `backfill-*.yml` | dispatch only | one-off archive loads |

Two structural problems inherited from this shape, both already recorded in
project history and both still open:

- The refresh and backfill workflows commit with `[skip ci]`, so new data does
  not reach the site until `ci-and-deploy.yml` is dispatched by hand.
- The deploy's publish step uses `git pull --rebase`, which cannot resolve
  conflicts in generated files and dies when two publishes race. A separate
  thread owns that fix.

Both are symptoms of the same thing: **live state is being carried by git
commits.** A publish is a merge, so it can conflict; a refresh is a commit, so
it can be skipped.

### The static-payload ceiling

GitHub Pages refuses a site over **1 GB**. `ui/` was **637 MB** on 2026-10-30
(see [`published-payload.md`](published-payload.md)), up from 466 MB eleven days
earlier, and it grows with every season and every refresh. Two rounds of
re-encoding and scope-narrowing have bought headroom, but the trend is one
direction. This is an architectural deadline independent of the roadmap's: the
current delivery model has a hard ceiling it is roughly two-thirds of the way
into.

---

## 4. Identity: three leagues share a convention, football does not

The roadmap asks for shared identifiers for *sport → league → season → team →
game → player*, distinguishing a university's football and basketball teams
while allowing institutional grouping.

**Good news: the convention already exists and is enforced in three leagues.**
`src/nba/identity.py`, `src/nfl/identity.py` and `src/ncaambb/identity.py` each
namespace every entity as `sport:provider:kind:id` — `nba:espn:team:25`,
`nfl:nflverse:player:00-0019596` — and each refuses a bare provider id as a key.
The modules state the reasoning and encode the traps (ESPN reusing "Charlotte
Hornets" across two franchises; the NFL source naming teams by code, with a
relocation table).

| League | Id space | Module |
| --- | --- | --- |
| NCAAF | **canonical name** + ESPN/CFBD ids in a registry | `src/team_identity.py` |
| NCAAM | `ncaambb:cbbd:*` | `src/ncaambb/identity.py` |
| NFL | `nfl:nflverse:*` | `src/nfl/identity.py` |
| NBA | `nba:espn:*` | `src/nba/identity.py` |

**Two gaps, both real:**

1. **College football is the odd one out.** It keys teams by `canonical_name`
   through `data/reference/team_identities.json` (139 FBS teams, mapping to ESPN
   and CFBD ids), not by a namespaced key. The flagship league is the one that
   does not follow the convention the other three adopted.

2. **Nothing joins a university's football team to its basketball team.** There
   is no institutional grouping anywhere in `src/`: no code path references both
   `ncaaf` and `ncaambb` team identities, and `team_identity.py` has no concept of
   an institution. This is precisely the P0 "My Teams" requirement — a reader
   favouriting Michigan should get both — and it does not exist yet. Whether CBBD
   team ids coincide with CFBD's for the same school is **unmeasured**, and must be
   measured rather than assumed; both are CFBD-family providers, which makes a
   coincidence plausible and therefore dangerous to rely on.

`src/person_identity.py` handles people for football, and the known problems are
already catalogued in [`identity-problems.md`](identity-problems.md) — 119,054
unresolved rows, and 74 people holding two pages each, of which 64 were read by
hand and linked. That work is a good model for what cross-sport identity will
need.

---

## 5. Provider coverage and rights, per league

| League | Source | Licence position | Live feed? |
| --- | --- | --- | --- |
| NCAAF | CollegeFootballData (`api.collegefootballdata.com`) | **Unverified — see below** | Yes, via polling |
| NCAAM | CollegeBasketballData | **Blocked: entitlement, not quota** | No |
| NFL | `nflverse-data` (static releases) | **CC BY 4.0**, read and confirmed | No |
| NBA | `sportsdataverse` / hoopR-nba-data (static parquet) | **MIT**, ESPN-derived underneath | No |

Four things worth stating plainly:

**CFBD's terms are not written down anywhere in this repository, and I could not
read them.** This environment's network policy denies `collegefootballdata.com`,
and I was unable to get the allowance approved from this session. CFBD powers
the flagship league and is the only live feed in the product. Its redistribution
and commercial-use terms are therefore **the single largest unknown in the
audit**, and they must be confirmed in writing before any league is advertised
in an App Store application. This is the one open item I could not close.

**Basketball is rights-blocked, not rate-limited.** The usage probe measured 9
basketball requests over 7 days, all refused. That is an entitlement matter on
the CBBD account; waiting does not fix it. Under the roadmap's own rule — launch
only leagues whose data meets the quality bar — NCAAM cannot be a launch league
until that is resolved, despite being in the suggested first evaluation group.

**Only one of four leagues has a live feed at all.** NFL and NBA are served from
static archive releases that update on their maintainers' schedule, which is
adequate for ratings and history and is **not** a Project LIVE feed. The roadmap's
P0 promise is "reliable score, game state, game detail... for each launched
league". Today that is deliverable for one league, imperfectly.

**A free archive licence is not a live-data licence.** CC BY 4.0 on
`nflverse-data` covers the historical files; it says nothing about a real-time
feed, because there isn't one. Any league that needs live coverage needs a
commercial agreement, and the candidates and what to measure against them are
already listed in `live-data-options.md` (Sportradar, SportsDataIO).

---

## 6. Cost envelope

**Current recurring cost is approximately zero, and that is the problem.**

The repository is public, so GitHub Actions minutes are free and GitHub Pages
hosting is free. Current consumption, for scale: the live sessions alone run
about three times a day at a median 242 minutes, roughly **21,800 runner-minutes
a month**, plus every refresh, backfill and deploy. On a private repository that
would be an order of magnitude over the 2,000-minute allowance; on a public one
it costs nothing.

So the architecture is not cheap because it is efficient. It is free because it
is running on a continuous-integration service as though it were a production
service, which is exactly what it is not, and the reliability numbers in §1 and
§2 are what that costs instead of money.

Moving live state to a backend introduces the project's **first real recurring
bill**. The roadmap asks for a monthly ceiling with alerting; the components to
price are:

| Component | Note |
| --- | ---: |
| Live data feed with redistribution rights | The dominant and least predictable line; enterprise-quoted |
| Always-on backend host | Small; one process polling and serving |
| Database / persistence | Small at this volume |
| Monitoring and alerting | Often free at this scale |
| Apple Developer Program | $99/year, fixed |
| Test devices | One-off |

I have deliberately put no dollar figures against the first four. Every one
depends on a quote or a plan this project does not yet hold, and a made-up
number in an audit is worse than a blank, because it gets quoted back. Getting
the feed quote is the gating action.

---

## 7. What this audit concludes

1. **Project LIVE cannot be built on GitHub Actions.** This is now measured
   twice, from two different angles, with the second measurement taken *after* the
   fix the first one motivated. The roadmap's instinct to make a backend own live
   state is correct and is the critical path.

2. **The backend already has a consumer.**
   [`live-score-delivery.md`](live-score-delivery.md) defines `LIVE_FEED_URL` and
   the contract an endpoint must satisfy, and the web page routes all three of its
   live fetches through one accessor. A Phase 2 service can be pointed at the
   existing site on day one, which makes the backend testable long before any
   SwiftUI exists. **This is the cheapest available de-risking of the whole plan
   and it should be done first.**

3. **The identity convention is three-quarters built.** Adopting
   `sport:provider:kind:id` for college football, and adding an institution
   concept above it, is well-understood work with a clear precedent in the repo.
   It blocks My Teams, so it is on the critical path too.

4. **Launch-league readiness, by the roadmap's own quality bar:**

   | League | Archive | Analytics | Live feed | Rights | Launch-ready? |
   | --- | --- | --- | --- | --- | --- |
   | NCAAF | Strong | Strong (Elo, Merits) | Polling only | **Unverified** | Not yet — feed and rights |
   | NFL | Strong | Strong | None | CC BY 4.0 | Not yet — no live feed |
   | NBA | Strong | Strong | None | MIT | Not yet — no live feed |
   | NCAAM | Partial | Fitted, unpublished | None | **Blocked** | No |

   On the roadmap's rule — *ship fewer leagues brilliantly* — the shortlist for a
   1.0 launch is **NCAAF first**, with NFL and NBA as the second wave once a feed
   exists for them, and **NCAAM excluded** until the CBBD entitlement is resolved.
   That is one certified league at launch, not three, and the roadmap's own scope
   control says to prefer that to shipping four unreliably.

5. **Two deadlines, not one.** Besides August 2027, the static payload is 637 MB
   against a 1 GB ceiling and rising. Whichever arrives first, the answer is the
   same backend.

## Open items this audit could not close

- **CFBD's redistribution and commercial-use terms.** Network-blocked from this
  environment; needs either an allowance for `collegefootballdata.com` or a
  direct read by Austin. **Highest-value single unknown.**
- **Whether CBBD and CFBD team ids coincide** for the same school. Needs a
  measurement, not an assumption.
- **A live-feed quote** from Sportradar or SportsDataIO, which gates the entire
  cost envelope.

## How these numbers were produced

Reproducible from the repository alone, no provider calls:

- Freshness and the frozen-game table: every revision of
  `ui/data/live_scores.json` on `main` read via `git cat-file --batch` over a
  clone deepened to 2026-10-24; per-game status transitions compared across
  consecutive snapshots.
- Session health: the GitHub Actions API for `live-score-session.yml`, 66 runs,
  with start lateness computed against the crons declared in the workflow file.
- Rights: repository documentation plus the licence files of each source, except
  CFBD, which is unread.
