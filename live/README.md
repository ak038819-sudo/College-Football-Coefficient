# `live/` — the live-state service, Milestone A

This is the shadow service from the November 2026 backend proposal. Milestone A
is contracts and tests only: **nothing in here runs in production, nothing is
deployed, and nothing the website reads has changed.** The existing publisher
(`src/fetch_live_scores.py`, the two crons, `ui/data/live_scores.json`) is
untouched and still the only thing serving live scores.

Run it with `npm test` from the repository root, or on its own:

```
node --test live/tests/*.test.mjs
```

## What is here

| File | What it is |
| --- | --- |
| `src/providers/provider.mjs` | The provider interface every sport's adapter implements, the normalized status set, and `entityKey` |
| `src/providers/cfbd.mjs` | The CFBD adapter. One `/scoreboard` request per cycle |
| `src/ingestion/validate.mjs` | State transition rules: versioned writes, score corrections, no un-finishing a final game, no vanishing games |
| `src/state/freshness.mjs` | How stale a snapshot is, and what the page must say about it |
| `src/legacy.mjs` | Projects normalized games back into the exact payload the current website reads |
| `tests/fixtures/*.json` | Three real published snapshots from this repository's history, 175 games |

## Two deviations from the proposal, both deliberate

**`.mjs`, not TypeScript.** §11 of the proposal says TypeScript. Cloudflare
Workers run ES modules natively, so plain `.mjs` needs no build step, no
`tsconfig`, and no second way for `npm test` to fail. The modules run unchanged
under `node --test` and under Workers. If the service grows enough that types
would catch things tests do not, converting it later is mechanical; adding a
build step now is not free.

**`src/legacy.mjs` is an addition.** The proposal's new `/v1/ncaaf/live` shape is
not what the published website reads — the site reads the object
`fetch_live_scores.py` writes, and `docs/live-score-delivery.md` requires any
endpoint serving it to emit that object exactly. So `/v1` alone could never be
pointed at the live site, and the proposal's Milestone D canary would not have
worked. Serving both shapes means `LIVE_FEED_URL` can target the shadow service
with **zero frontend change**, which turns that canary from a rebuild-and-deploy
into a URL.

## What the tests prove, and what they do not

The fixtures are real **output** of the Python publisher. Raw CFBD
`/scoreboard` responses are committed nowhere and the provider is unreachable
from the development container, so the raw-shaped inputs are reconstructed by
inverting `clean_game`'s field mapping. **These tests hold the adapter to this
project's normalization contract, not to bytes CFBD actually sent.** A field
CFBD sends that `clean_game` silently ignores would not be caught. Capturing one
real response is Milestone B work.

Every test here was mutation-checked: 40 mutations of the five source modules,
all 40 caught. Three of them survived the first pass — strict-`true`
`neutral_site`, the 120-character venue trim, and an omitted
`player_stats_checked_at` — because no fixture happens to exercise them. That is
the argument for mutation-checking a differential suite rather than trusting its
size.

## Three findings worth carrying forward

**The published snapshot has six top-level keys, not three.** Besides `source`,
`updated_at` and `games` it carries `player_boxscores`,
`player_boxscore_times` and `player_stats_checked_at`, and
`ui/dashboard_shell.html` reads all three to draw the player panel on an open
game. The first version of `legacy.mjs` emitted only the first three; the
differential test caught it. They are passed through from whatever last fetched
`/games/players`, which is deliberately not on the live cycle.

**"Byte for byte" is not fully achievable in JavaScript, by one measurable
amount.** A JS object orders integer-like keys first and ascending whatever
order they arrived in; Python keeps insertion order. `player_boxscores` and
`player_boxscore_times` are keyed by game id, and a real published snapshot does
list those ids out of ascending order. So the projection is byte-identical
everywhere else and parse-equal inside those two maps. The consumer looks them
up by key, so the order is not information to it — but the Milestone D canary
has to compare accordingly, and no future consumer may depend on that order.

**Two bugs the tests found in code written the same day.** `reconcile` accepted
a write on the strength of the poll's observation time while storing the game's
own older stamp, which is the number `freshness.mjs` later shows a reader; it now
stamps the time the decision was made on. And `health` reported `'no games
tracked'` for a perfectly healthy slate, because a FRESH verdict's reason is
`null` by design and `??` fell through it.
