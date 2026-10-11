# Live score refresh and delivery

There are two ways the scoreboard gets published, and on a game day the
second one does nearly all the work.

`.github/workflows/live-scores.yml` fetches CFBD's subscriber scoreboard and
publishes `ui/data/live_scores.json`. The key is confined to the fetch step.
Failed fetches leave the last good snapshot in place.

`.github/workflows/live-score-session.yml` is the game-day path. GitHub
delivers about 3% of the five-minute cron firings the workflow above asks for
(docs/live-data-options.md), which left finished games showing a running clock
for hours. The session workflow is started once per game day and polls from
inside one long-running job, so GitHub's scheduler is not involved in its
cadence at all. It publishes when a score, status, period or clock changes, at
most once per `--interval` (360 seconds by default, because Pages builds from a
branch are soft-limited to about ten an hour), and it ends early once every
game on the scoreboard is final and that final state is published.

Dispatch it by hand for a slate outside the crons: Actions -> Live score
session -> Run workflow, with `duration` in minutes and `interval` in seconds.

`live-scores.yml`'s own schedule is unchanged. In August–November it asks for
a run every five minutes during Thursday afternoon/night,
Friday night, and the Saturday slate, including the late Central hours that
fall on the next UTC date. In December and January it runs every fifteen
minutes during a daily 09:00–01:00 Central Standard Time window for bowls and
playoff games on weekdays. Run the workflow manually for games outside those
windows. The clock is UTC in the workflow, so the August–November local bounds
shift by an hour at the daylight-saving transition.

Each successful run reports two measurements in its Actions summary:

- `updated_at` in the JSON is when our runner fetched CFBD, not when CFBD
  changed an individual game's score.
- Fetch-to-push is the approximate time until the git push finishes.
- Fetch-to-Pages is the time until an uncached public request first sees that
  exact `updated_at` value. It includes Actions, Pages build, and CDN delay.
  It is an observation bound, not GitHub's internal deployment timestamp or
  the exact time a fan's browser rendered the change.

The Pages check uses no CFBD calls and does not block a successful score
publish. If the new file is not visible within two minutes, the summary says
visibility was unconfirmed. Compare these summaries with the on-page “Scores
fetched” timestamp during live games before deciding whether static hosting
is fast enough.
