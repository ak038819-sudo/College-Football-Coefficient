# Live score refresh and delivery

There are two ways the scoreboard gets published, and on a game day the
second one does nearly all the work.

`.github/workflows/live-scores.yml` fetches CFBD's subscriber scoreboard and
publishes `ui/data/live_scores.json`. The key is confined to the fetch step.
Failed fetches leave the last good snapshot in place.

`.github/workflows/live-score-session.yml` is the path that does the work.
GitHub delivers about 3% of the five-minute cron firings the workflow above
asks for (docs/live-data-options.md), which left finished games showing a
running clock for hours. A session polls from inside one long-running job, so
GitHub's scheduler is not involved in its cadence at all. It publishes when a
score, status, period or clock changes, at most once per `--interval` (360
seconds by default, because Pages builds from a branch are soft-limited to
about ten an hour), and it ends early once every game on the scoreboard is
final and that final state is published.

**The sessions tile the day.** Six start every day of August through January,
four hours apart, and each polls for four hours, so they hand off to each other
with nothing in between. `DEFAULT_DURATION` in
`src/run_live_score_session.py` is the same four hours as the spacing in the
workflow, and the two are one decision: shorter leaves a hole, longer queues a
run that GitHub cancels once another is already pending in the concurrency
group.

The schedule deliberately says nothing about when football is played, because
the version that did was wrong about it. Sessions first ran Thursday through
Sunday; 54 of the 761 kickoffs in 2026 are on other days — Tuesday and
Wednesday evenings from the MAC, a Monday night, a Friday lunchtime — and on
2026-10-21 the published snapshot was three days old for exactly that reason,
still showing the previous week's finals with Tuesday's three games missing
entirely. A table of windows drawn around 2026's schedule was tried next and
covered 2023 badly, bowl season having games every weekday from early
afternoon. Tiling is checked against every kickoff of 2023 through 2026: no
game spends a minute of its first four hours outside a session.

**What makes tiling an empty day cheap** is in the script, not the schedule.
A session whose whole scoreboard is already final exits after one tick. A
session watching games that have not kicked off yet polls at `--idle-interval`
(1200 seconds) instead of `--interval`, because there is nothing to be fresh
about until somebody kicks off and each tick costs three CFBD requests. The
cadence returns to `--interval` as soon as a game is in progress, and before
that: a scheduled game whose kickoff falls inside the idle window counts as
live, so a game starting in eight minutes is not found twenty minutes into the
first quarter. A kickoff time that is missing or unreadable counts as imminent,
which costs a normal-rate poll — the opposite error would be a slate that
silently never speeds up.

Dispatch a session by hand for an unusual slate: Actions -> Live score
session -> Run workflow. Every input is optional and a blank one takes the
script's default, so a bare dispatch and a scheduled run behave identically.

`live-scores.yml`'s own schedule is unchanged, and its windows are now the
narrower of the two: in August–November it asks for a run every five minutes
during Thursday afternoon/night, Friday night and the Saturday slate, and in
December and January every fifteen minutes during a daily 09:00–01:00 Central
Standard Time window. At 3% delivery it is a fallback and a manual-refresh
path rather than the thing keeping the scoreboard current; the sessions are
what cover the rest of the week. The clock is UTC in the workflow, so the
August–November local bounds shift by an hour at the daylight-saving
transition.

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
