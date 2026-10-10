# Live score delivery

The page polls the scoreboard every 60 seconds. It always has. The limit has
never been polling — it is how often the file it polls gets rewritten.

Measured 2026-09-27 to 2026-10-10: `live-scores.yml` asks GitHub for a refresh
every five minutes across the game windows, and GitHub created **22 of the 699
firings those crons specify, 3.1%**. On Saturday 2026-10-10 at 06:15 UTC,
inside a covered window, the published snapshot was 2 hours 10 minutes old with
four games in progress. GitHub documents five minutes as the shortest interval
it accepts and also that scheduled runs may be delayed or dropped under load;
3.1% is what that means for a high-frequency cron. Nothing in this repository
raises it. The measurement and the alternatives are in
[`live-data-options.md`](live-data-options.md).

So the page can now read its scoreboard from somewhere else, and this page is
the contract for what that somewhere else must do.

## Pointing the page at a publisher

Set `LIVE_FEED_URL` when building:

```sh
LIVE_FEED_URL=https://scores.example.com/live_scores.json ./build_dashboard.py
```

`build_dashboard.py` bakes it into the page. Unset — the default, including
every build CI runs today — the page reads its committed
`ui/data/live_scores.json` and behaves exactly as it did before this existed.

The build refuses a URL it cannot use rather than shipping a page that quietly
stops updating:

- It must be absolute `https://`. The published site is https, so an `http`
  endpoint is blocked as mixed content, and a relative value resolves against
  `ui/` — right in one place and broken in another.
- No quote or backslash. It is substituted into a single-quoted JavaScript
  string literal.

Nothing else in the page changes. One accessor, `liveFeedUrl()`, is the only
place the scoreboard's location is decided, and all three live fetches — the
Home and Live scoreboards, the rating-freshness check, and the open game page
— go through it.

## What the endpoint must return

**Byte-for-byte the same object `src/fetch_live_scores.py` writes.** Do not
reimplement the normalization. That script is the only thing in this project
that knows how a CFBD scoreboard row becomes a game here, and
`data/reference/team_identities.json` is the only reviewed mapping from
provider identities to these teams. A second implementation in another
language will drift, and the first symptom is a game attributed to the wrong
school. Run the existing script on a timer and serve what it produces.

Required of the response:

- `Content-Type: application/json`.
- `Access-Control-Allow-Origin` admitting the site's origin. Without it the
  browser refuses the response and the scoreboard silently stops updating;
  this is the single most common way this goes wrong.
- `updated_at`, an ISO-8601 UTC instant, being **when the data was fetched
  from the provider**, not when the request was served. The page prints the
  snapshot's age from this field and now says "3 hours old, newer scores may
  exist" when it is stale. A publisher that stamps serve-time makes the page
  claim a freshness it does not have, which is worse than the problem this
  replaces.
- The last good snapshot on failure, never an error body or a partial one. The
  page shows a cached snapshot while it refetches, so a single bad response
  blanks nothing — but a 200 carrying half a slate does.

The CFBD key stays with the publisher. It must never reach the page: anything
baked into `dashboard.html` is public, and the site is static with no server of
its own to hide a secret behind.

## What has not been decided

Where the publisher runs. The two realistic shapes both mean paying for
something always on, and that is the owner's call:

- A small always-on service running `src/fetch_live_scores.py` on its own
  timer and serving the result. Smallest change here — this file and
  `LIVE_FEED_URL` are the whole of it.
- Moving the site to a host with real scheduling, which also ends the publish
  race, at the cost of a migration off GitHub Pages.

Until one is chosen, `LIVE_FEED_URL` stays unset, the committed snapshot stays
the source, and the page states its real age rather than implying it is live.

Changing provider is not on this list: a faster provider read once every few
hours is still read once every few hours.
