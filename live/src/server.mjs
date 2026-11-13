/**
 * The shadow service's HTTP surface.
 *
 * Two shapes, deliberately, and the second one is why Milestone D is a URL
 * change rather than a deploy:
 *
 *   GET /v1/health                  what we hold and how old it is
 *   GET /v1/ncaaf/live              the normalized slate
 *   GET /v1/ncaaf/games/:key        one normalized game
 *   GET /ncaaf/live_scores.json     the EXACT payload the website reads today
 *
 * The last one is the point. `docs/live-score-delivery.md` requires any
 * endpoint serving the live scoreboard to emit the object
 * `src/fetch_live_scores.py` writes, and the published site reads nothing else.
 * So `LIVE_FEED_URL` can be pointed here with zero frontend change, and with
 * the runtime override shipped in `ui/dashboard_shell.html` a single reader can
 * be pointed here with no deploy at all.
 *
 * WHAT THIS IS BUILT ON. A plain `Request -> Response` handler, which is the
 * Workers interface and also trivially driveable from `node --test`. No
 * framework, no router library, no build step. `createServer` takes the
 * coordinator (or anything exposing `state` and `health()`), so the handler
 * never fetches anything itself: a reader can never cause a provider request.
 * That is the whole architectural claim -- 500 readers must not become 500
 * provider requests -- and it is enforced here by construction rather than by
 * discipline.
 *
 * NO PROVIDER DATA LEAVES HERE THAT WE DO NOT ALREADY PUBLISH. The legacy
 * payload is byte-compatible with a file already served publicly from Pages,
 * and `/v1` is the same games in a different shape. `weather` and `betting`
 * are fetched by nothing and carried by nothing, so they cannot leak from
 * here. The CFBD key lives in the ingestion half and is never read in this
 * file.
 */
import { toLegacySnapshot, serializeLegacy } from './legacy.mjs';

/** Routes, so a 404 can list them rather than being a blank wall. */
export const ROUTES = Object.freeze([
  'GET /v1/health',
  'GET /v1/ncaaf/live',
  'GET /v1/ncaaf/games/:game_key',
  'GET /ncaaf/live_scores.json',
]);

const JSON_HEADERS = Object.freeze({ 'Content-Type': 'application/json; charset=utf-8' });

/**
 * Cache headers.
 *
 * `no-store` on everything, and that is a decision rather than a default. The
 * page polls on its own schedule and asks for `no-store` itself; a cached live
 * score is the exact failure this service exists to remove, and an intermediary
 * holding a fifteen-second-old slate for five minutes would reintroduce it
 * invisibly. `Access-Control-Allow-Origin` is required because the page is
 * served from a different origin than this; without it the browser discards a
 * perfectly good response and the page falls back to the static file, which
 * would look like the service being down.
 */
function headers(extra = {}) {
  return {
    ...JSON_HEADERS,
    'Cache-Control': 'no-store',
    'Access-Control-Allow-Origin': '*',
    ...extra,
  };
}

function json(body, { status = 200, extra = {} } = {}) {
  return new Response(JSON.stringify(body), { status, headers: headers(extra) });
}

/**
 * Build the handler.
 *
 * `source` must expose `state` (with `games`) and `health()`. `players` is the
 * box-score block for the legacy payload, supplied by whatever last fetched it
 * -- see `legacy.mjs`; it is a function so a later batch is picked up without
 * rebuilding the server.
 */
export function createServer({ source, players = () => null, now = () => new Date() } = {}) {
  if (!source || typeof source.health !== 'function') {
    throw new TypeError('a source exposing state and health() is required');
  }

  /**
   * The observation time the legacy payload reports.
   *
   * It is the NEWEST observation across the games, not the time of this
   * request. The page prints the snapshot's age from this field, so stamping
   * serve time here would make it claim a freshness it does not have -- which
   * `live-score-delivery.md` calls out as worse than the staleness it replaces.
   *
   * An empty slate has no observation, so there is nothing honest to report
   * and the endpoint says so with a 503 rather than inventing `now`.
   *
   * THE ORIGINAL STRING IS RETURNED, NOT A REBUILT ONE, and the differential
   * test is what insisted on it. `src/fetch_live_scores.py` stamps
   * `datetime.isoformat()`, which carries MICROSECONDS --
   * `2026-11-08T07:15:11.395792Z` -- and JavaScript's `Date` holds only
   * milliseconds, so `new Date(Date.parse(s)).toISOString()` silently rewrites
   * that as `...395Z`. Parsing to compare and then serving the survivor
   * verbatim loses nothing. It is the same mistake as rebuilding a map whose
   * key order cannot be reproduced: compare on a lossy representation if you
   * must, but never emit one.
   */
  function newestObservation(games) {
    let newest = null;
    let text = null;
    for (const game of Object.values(games ?? {})) {
      const at = Date.parse(game?.observed_at);
      if (Number.isNaN(at)) continue;
      if (newest === null || at > newest) {
        newest = at;
        text = game.observed_at;
      }
    }
    return text;
  }

  return async function handle(request) {
    const url = new URL(request.url);
    const path = url.pathname.replace(/\/+$/, '') || '/';

    // A browser preflights a cross-origin GET with custom headers. Answering
    // it is cheap and not answering it looks exactly like an outage.
    if (request.method === 'OPTIONS') {
      return new Response(null, {
        status: 204,
        headers: headers({ 'Access-Control-Allow-Methods': 'GET, OPTIONS', 'Access-Control-Max-Age': '86400' }),
      });
    }
    if (request.method !== 'GET' && request.method !== 'HEAD') {
      return json({ error: 'method not allowed', routes: ROUTES }, { status: 405, extra: { Allow: 'GET, HEAD, OPTIONS' } });
    }

    if (path === '/v1/health') {
      const report = source.health(now());
      // 200 even when the news is bad. This endpoint's job is to REPORT, and a
      // monitor that only sees a status code cannot tell "stale" from
      // "unreachable" if staleness is also a 5xx. The level is in the body.
      return json(report);
    }

    if (path === '/v1/ncaaf/live') {
      const games = source.state?.games ?? {};
      return json({
        sport: 'ncaaf',
        served_at: now().toISOString(),
        health: source.health(now()),
        games: Object.values(games),
      });
    }

    if (path.startsWith('/v1/ncaaf/games/')) {
      const key = decodeURIComponent(path.slice('/v1/ncaaf/games/'.length));
      const game = (source.state?.games ?? {})[key];
      if (!game) {
        return json({ error: 'no such game', game_key: key }, { status: 404 });
      }
      return json({ served_at: now().toISOString(), game });
    }

    if (path === '/ncaaf/live_scores.json') {
      const games = Object.values(source.state?.games ?? {});
      const updatedAt = newestObservation(source.state?.games);
      if (updatedAt === null) {
        // Nothing observed yet. The page falls back to the committed snapshot
        // on a failure, which is the correct outcome here: serving an empty
        // slate with a serve-time stamp would look like a real answer meaning
        // "no games today".
        return json({ error: 'no observation yet' }, { status: 503, extra: { 'Retry-After': '15' } });
      }
      const { snapshot, dropped } = toLegacySnapshot(games, { updatedAt, players: players() });
      return new Response(serializeLegacy(snapshot), {
        headers: headers({
          // Named so an operator can see a drop without diffing payloads. A
          // game with no legacy status is refused rather than mapped to
          // something plausible, and silence about it is how that becomes
          // invisible.
          'X-Elocast-Dropped': String(dropped.length),
        }),
      });
    }

    return json({ error: 'not found', routes: ROUTES }, { status: 404 });
  };
}
