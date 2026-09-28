# Live score refresh and delivery

`.github/workflows/live-scores.yml` fetches CFBD's subscriber scoreboard and
publishes `ui/data/live_scores.json`. The key is confined to the fetch step.
Failed fetches leave the last good snapshot in place.

In August–November it runs every five minutes during Thursday afternoon/night,
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
