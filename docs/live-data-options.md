# Live data options

Reviewed 2026-10-04 against provider documentation. No credentials purchased, contract assumed or production feed replaced.

| Option | Documented capabilities | Next evaluation |
| --- | --- | --- |
| Current CFBD pipeline | Existing scores, archive and scoped team mapping in this repository | Measure end-to-end scoreboard age and errors before changing providers. |
| Sportradar NCAA Football | Game coverage flags distinguish full play-by-play and extended box scores; expected_latency reports feed lag. Realtime customers can use push. | Trial exact FBS/FCS coverage, ID mapping, corrections, game completion transitions and latency; confirm redistribution terms and quote. |
| SportsDataIO College Football | Live game state/scores, live/final box score endpoint; realtime guidance suggests polling scores/boxscores every 3–5 seconds under an appropriate subscription. | Trial the same slate and measure discrepancies; confirm plan, coverage and redistribution rights. |

Sources:
- https://developer.sportradar.com/football/docs/ncaafb-ig-live-game-retrieval
- https://developer.sportradar.com/football/reference/ncaafb-game-statistics
- https://sportsdata.io/developers/coverages/ncaa-football
- https://sportsdata.io/developers/api-documentation/ncaa-football
- https://sportsdata.io/help/refresh-rates-feeds-and-timing

A provider adapter should emit the existing normalized snapshot, retain the original provider IDs, require exact reviewed identity mappings, distinguish provider update time from fetch time, and preserve the last good snapshot on failure. Test postponed/canceled games, neutral venues, overtime, clock corrections and late final-score changes. Never substitute two providers' numeric IDs as if they shared a namespace. Keep API keys server-side.

Recommended experiment: compare at least two complete game windows with the current feed. Record median/p95 score-age, missing fixtures, identity conflicts, HTTP errors and cost at the intended polling rate. A static deployment per play is a separate latency bottleneck; choose hosting/polling architecture only after measuring both fetch and publish lag. An undocumented public scoreboard endpoint is not evidence of permission to redistribute data or a stability guarantee.

## Measured 2026-10-10: the bottleneck is the scheduler, not the provider

The experiment this page asks for has now been run on the delivery side, and
it changes which question is worth money. **The current provider is not the
reason scores are stale. GitHub's cron is.**

`live-scores.yml` asks for a refresh every five minutes across the Thursday,
Friday, Saturday and Sunday game windows. Counting the firings those crons
specify between 2026-09-27 02:08 and 2026-10-10 06:15 UTC:

| | |
| --- | ---: |
| Scheduled firings specified by the crons | 699 |
| Runs GitHub actually created | 22 |
| Delivered | **3.1%** |

Observed consequence, on Saturday 2026-10-10 at 06:15 UTC, inside a window
the cron covers: the published snapshot was timestamped 04:06 UTC — **2 hours
10 minutes old, with four games in progress**.

GitHub documents five minutes as the shortest interval it accepts, and also
that scheduled runs may be delayed or dropped under load. 3.1% is what that
means in practice for a high-frequency cron. Nothing in this repository can
raise it: the workflow is correct, the crons parse as intended, and the job
itself takes about 100 seconds.

So a faster provider bought today would be read once every few hours. Any
work on score latency has to start with how the refresh is scheduled and
published, not with who supplies the numbers.

Two repository-side faults were found alongside the measurement and are
fixed:

- The refresh shared the deploy's concurrency group
  (`rebuild-and-deploy-main`). GitHub keeps only one pending run per group,
  so a refresh that fired while a six-minute deploy held the lock waited, and
  the next refresh five minutes later cancelled it. It has its own group now.
  Racing is safe: `publish_to_main.sh` merges a main that moved and retries a
  rejected push.
- The page said only "Feed may be delayed" past fifteen minutes. It now
  states the snapshot's actual age, and past two hours says newer scores may
  exist.

Neither raises the delivery rate. They stop it being made worse, and stop the
page implying a freshness it does not have.

### What would actually raise it

| Path | Effect | Cost |
| --- | --- | --- |
| Keep Actions, accept the cadence | Scores minutes-to-hours old on game days. The page is now honest about it. | None. Rules out the roadmap's "first stop for scores" goal. |
| Poll from the browser against a small hosted endpoint | Seconds-to-a-minute old; the static site stays as it is for everything else | One always-on service and its key handling. Lowest change to this repository. |
| Move the whole site to a host with real scheduling | Same freshness, plus no publish race | Larger migration; Pages hosting goes away |
| Change provider | Does not help on its own | Spend with no gain until scheduling is solved |

A public but undocumented scoreboard endpoint is still not permission to
redistribute, and is not a path here — the warning above this section stands.

The decision between these is the owner's: the middle two both mean paying
for something always on.

## Shipped 2026-10-11: a long-running session, which the table above missed

The four paths above are not exhaustive. A fifth exists, it costs nothing, and
it was not considered because the measurement framed the problem as "GitHub
will not run a job often enough" when the real constraint is narrower:
**GitHub will not *schedule* a job often enough.** It will happily run one job
for six hours.

`.github/workflows/live-score-session.yml` polls the scoreboard from inside a
single long-running job (`src/run_live_score_session.py`). The loop keeps its
own cadence, so none of the 3.1% delivery rate applies to it; GitHub only has
to start the job.

The first version started one session per game day, on the four days of the
week college football was assumed to be played. That assumption cost three
days of a stale scoreboard in October 2026, and it does not survive the
schedule: 54 of 761 kickoffs in 2026 fall on other days, and bowl season has
games every weekday. Sessions now start every four hours, every day of the
season, and poll for four hours each, so they tile the day end to end. A cron
every four hours is still far below the frequency GitHub throttles, and a
firing that is dropped anyway now costs four hours of coverage rather than a
whole day of it. See docs/live-score-refresh.md for what keeps that affordable.

What this does and does not buy:

| | Before | After |
| --- | ---: | ---: |
| Refreshes delivered per hour, game day | ~0.3 | up to 10 |
| Typical snapshot age | hours | under 6 minutes |
| Cost | — | none (public repository) |

**Six minutes is the floor, and it is not this script's fault.** Publishing to
`main` is how the site updates, and a Pages site built from a branch is
soft-limited to roughly ten builds an hour. `DEFAULT_INTERVAL` is 360 seconds
for that reason, and a test pins it: a shorter interval would queue publishes
Pages will not build. Anything faster than six minutes still means moving off
Pages or polling the browser against a hosted endpoint — the two paid rows
above. Those remain open, and this change does not pre-empt them.

Design notes worth keeping:

- Each tick goes through `fetch_live_scores.refresh`, the same function a cron
  run calls. There is one definition of a published snapshot; a second would
  drift, and the drift would surface as a game attributed differently
  depending on which job published it.
- A publish happens only when a reader would notice: status, period, clock or
  either score. `updated_at` is excluded deliberately, because `refresh`
  stamps it every tick and comparing whole files would publish on every poll.
- The session ends early once every game on the scoreboard is final **and that
  state is published**. Stopping on a snapshot that failed to push would leave
  the site showing a running clock on a finished game, which is the defect the
  session exists to clear.
- A failed poll is logged and the loop continues; the last good snapshot stays
  published. The run fails only if every poll failed.
- `live-scores.yml` is unchanged. It covers windows the sessions do not reach
  and is still the path a manual refresh takes. The two can race safely:
  `publish_to_main.sh` merges a `main` that moved and keeps the running job's
  own file. The one cost of that is a visible step backwards if a cron run
  publishes a snapshot staler than the session's seconds after it; the next
  tick corrects it within the interval.
