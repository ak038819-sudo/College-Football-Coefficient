# `/v1` API contract — DRAFT, nothing implemented

Phase 1 item 4, Step 7 of the plan. **Every route here is proposed. None is
implemented, and nothing in the repository calls any of them.** This document
exists so a Phase 2 backend and a SwiftUI client can be built against the same
agreed shape rather than discovering it in integration.

## The one decision that is already made

**`/v1/games` returns the object `src/fetch_live_scores.py` already writes, and
the backend does not reimplement normalization.**

This is not a new choice; it is
[`live-score-delivery.md`](live-score-delivery.md)'s existing contract, and the
website already consumes it through `LIVE_FEED_URL` and a single accessor,
`liveFeedUrl()`. That makes the live scoreboard the **cheapest possible first
integration test of a Phase 2 backend**: point the existing site at the new
service and the whole published scoreboard either works or does not, with no
iOS code written.

Carried forward from that contract, unchanged and non-negotiable:

- `updated_at` is **when the data was fetched from the provider**, never
  serve-time. The page prints the snapshot's age from this field. A publisher
  that stamps serve-time makes the page claim a freshness it does not have,
  which is worse than staleness.
- On failure, serve the **last good snapshot**, never an error body and never a
  partial slate. A 200 carrying half a slate is the harmful case.
- `Access-Control-Allow-Origin` must admit the site's origin. Without it the
  browser refuses the response and the scoreboard silently stops updating —
  the single most common way this goes wrong.
- The provider key stays server-side. The site is static and can hide nothing.

## Identity in every response

Teams are addressed by the keys the repository already enforces, not by a bare
provider id:

- Each league uses its own namespaced entity key — `ncaaf:cfbd:team:…`,
  `ncaambb:cbbd:team:…`, `nfl:nflverse:team:…`, `nba:espn:team:…` — following
  `src/{nba,nfl,ncaambb}/identity.py`. **College football does not follow this
  convention yet**; it keys on `canonical_name`, and bringing it into line is
  Phase 2 work the audit flagged.
- A college team additionally carries `institution_id`, the **ESPN team id**,
  which is what joins a school's football and basketball teams. See
  [`identity-cross-league-measurement.md`](identity-cross-league-measurement.md):
  the provider ids are 0-for-139 and 81 of them collide on the wrong school, so
  `institution_id` is the only safe cross-league key. Professional teams carry
  no `institution_id`.

```json
{
  "team_key": "ncaaf:cfbd:team:57",
  "institution_id": 57,
  "display_name": "Florida Gators",
  "league": "ncaaf"
}
```

## Routes

| Route | Purpose |
| --- | --- |
| `GET /v1/leagues` | Which leagues this deployment serves, and each one's coverage and certification state. |
| `GET /v1/games?date=&league=` | A day's slate. The live scoreboard. |
| `GET /v1/games/{game_key}` | One game: status, clock, line score, available detail. |
| `GET /v1/teams/{team_key}` | One team's identity, record and rating. |
| `GET /v1/teams/{team_key}/schedule?season=` | That team's season. |
| `GET /v1/institutions/{institution_id}` | A school's teams across leagues. **My Teams depends on this one.** |
| `GET /v1/health/feed` | Per-league freshness and last successful fetch. |

`/v1/institutions/{id}` is not in the plan's list of six. It is added because
My Teams is P0 and "favourite Michigan, get both Michigans" has no other route
to hang on.

## Freshness fields, on every payload

The audit's central finding is that the current pipeline cannot say how stale
it is. The contract makes that unrepresentable: **three separate timestamps,
never collapsed into one.**

```json
{
  "provider_updated_at": "2026-11-08T07:14:58Z",
  "fetched_at":          "2026-11-08T07:15:11Z",
  "served_at":           "2026-11-08T07:15:13Z",
  "stale": false,
  "staleness_reason": null
}
```

- `provider_updated_at` — the provider's own stamp, where it gives one. **Null
  where it does not.** Never substituted with `fetched_at`: the difference
  between them is provider lag, and conflating the two is how provider delay
  gets misreported as ours. This is the plan's own watch-out.
- `fetched_at` — when we got it. The field the page already reads.
- `served_at` — when this response was generated. Diagnostic only; a client must
  never compute age from it.
- `stale` — the **server's** judgement, so every client agrees. A client may
  additionally decide, but must not disagree downward.
- `staleness_reason` — one of `provider_outage`, `no_recent_fetch`,
  `partial_slate`, `postponed_unknown`, or null.

## Game status

The normalized set, which is the plan's Step 6 list plus what the archive
actually contains:

`scheduled` · `delayed` · `in_progress` · `halftime` · `final` · `postponed` ·
`canceled` · `suspended` · `unknown`

Rules the audit and the existing code make necessary:

- **`unknown` is a real value and must be served, not hidden.** A status the
  adapter cannot map is not `scheduled`.
- **A missing clock or score is `null`, never `0`.** `Number.isFinite(Number(null))`
  is `true`, so a null clock renders as `0:00` and a null score as `0–0`. That
  bug has shipped in this project three times. The contract forbids the zero.
- **An unplayed game carries `null` scores**, not `0–0`. The NBA source writes
  0–0 for scheduled fixtures and `src/nba/` already guards against reading it;
  the API must not reintroduce it.
- **`final` is not reversible by a later poll** without an explicit
  `correction` record. Late score corrections are real and have to be
  distinguishable from a flapping feed.
- Clock and period semantics **differ by sport** and are not interchangeable.
  `period` carries a `kind` (`quarter`, `half`, `overtime`, `shootout`) rather
  than a bare integer, because a basketball "2" and a football "2" are not the
  same fraction of a game.

## Coverage, stated per league rather than implied

```json
{
  "league": "ncaambb",
  "certified": false,
  "live": false,
  "reason": "provider entitlement unresolved",
  "features": { "scores": true, "box_score": true, "play_by_play": false }
}
```

A client renders what a league declares, so an uncertified league cannot be
advertised by accident. This is the roadmap's "launch only leagues whose data
meets the quality bar", expressed in the payload rather than in a release note.

## Favorites in 1.0

**Device-local**, per the plan. The contract's only job is to make them portable
later, so a stored favorite is `{"kind": "institution", "id": 57}` or
`{"kind": "team", "key": "nfl:nflverse:team:KC"}` — never a display name and
never a bare provider integer. Account sync can then migrate the list verbatim.

## Still undecided

Listed rather than guessed, because a contract that invents these is worse than
one that admits them:

- **Pagination.** A day's slate does not need it; a season's box scores might.
- **Rate limits and auth.** Depends on whether the API is public or app-only,
  which depends on the CFBD terms nobody has read yet.
- **Whether `/v1/games` serves all leagues at once** or one per request. The
  current file is football-only; My Teams wants a mixed feed.
- **Schema evolution.** `/v1` is a URL prefix here. Whether a minor addition
  bumps anything is not settled, though the existing per-league export
  `SCHEMA_VERSION` (NFL 5, NBA 3, NCAAM 3) is the precedent to follow.

## Next step

Contract tests from fixtures, including the stale and missing-data cases above,
per Step 7's deliverable. Those can be written before any server exists and
will then be the server's acceptance suite.

**This draft should not be implemented before the CFBD terms are read.** If
redistribution to an app is not permitted, the shape of `/v1/games` for the
flagship league changes, and so does whether the API can be public at all.
