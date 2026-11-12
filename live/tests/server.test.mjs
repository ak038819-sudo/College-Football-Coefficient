/**
 * The HTTP surface, driven as a real `Request -> Response` handler.
 *
 * The one that matters most is `/ncaaf/live_scores.json`: it must emit the
 * payload the published website already reads, because that is what lets
 * `LIVE_FEED_URL` be pointed here with no frontend change. It is checked
 * against a REAL published snapshot run through the whole chain -- normalize,
 * merge, serve -- rather than against a hand-built object.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

import { ROUTES, createServer } from '../src/server.mjs';
import { createCoordinator } from '../src/ingestion/coordinator.mjs';
import { normalizeScoreboard } from '../src/providers/cfbd.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const FIXTURE = join(HERE, 'fixtures', 'live_mid_slate.json');
const NOW = new Date('2026-11-08T19:00:00.000Z');

/** Invert `clean_game`'s mapping, as the contract test does. */
function toRaw(game) {
  const team = (side) => ({
    id: side.id, name: side.name, points: side.points, classification: side.classification,
  });
  return {
    id: game.id,
    startDate: game.start_date,
    venue: game.venue,
    status: game.status,
    period: game.period,
    clock: game.clock,
    tv: game.tv,
    neutralSite: game.neutral_site,
    awayTeam: team(game.away),
    homeTeam: team(game.home),
  };
}

function published() {
  return JSON.parse(readFileSync(FIXTURE, 'utf8'));
}

/** A source holding the real fixture, normalized through the real adapter. */
async function loaded({ observedAt = null } = {}) {
  const snapshot = published();
  const stamp = observedAt ?? snapshot.updated_at;
  const games = normalizeScoreboard(snapshot.games.map(toRaw), { observedAt: stamp });
  const coordinator = createCoordinator({
    provider: { async getLiveGames() { return games; } },
    now: () => new Date(stamp),
  });
  await coordinator.poll();
  return { coordinator, snapshot, stamp };
}

function get(path, { method = 'GET' } = {}) {
  return new Request('https://live.example.com' + path, { method });
}

// --- the legacy payload, which is the whole point ---------------------------

test('the legacy endpoint emits what the published website already reads', async () => {
  const { coordinator, snapshot, stamp } = await loaded();
  const handle = createServer({
    source: coordinator,
    players: () => ({
      boxscores: snapshot.player_boxscores,
      times: snapshot.player_boxscore_times,
      checkedAt: snapshot.player_stats_checked_at,
    }),
    now: () => NOW,
  });

  const response = await handle(get('/ncaaf/live_scores.json'));
  assert.equal(response.status, 200);
  assert.match(response.headers.get('content-type'), /application\/json/);
  const body = JSON.parse(await response.text());

  assert.deepEqual(body, snapshot, 'the served payload differs from the published one');
  // Key order too, which a deepEqual would not catch.
  assert.deepEqual(Object.keys(body), Object.keys(snapshot));
  assert.equal(body.games.length, 63);
  // `updated_at` is the OBSERVATION, never the serve time. The page prints the
  // snapshot's age from this field.
  assert.equal(body.updated_at, stamp);
  assert.notEqual(body.updated_at, NOW.toISOString());
});

/**
 * A source holding exactly the games given, with their own observation times.
 *
 * A coordinator cannot be used for this: `mergePoll` stamps every game it
 * accepts in one poll with that poll's single observation time, so games that
 * went in disagreeing come back out agreeing and "newest" and "oldest" become
 * the same string. That is correct of the ingestion half and it is why this
 * test has to build the state directly -- state that arrives from two separate
 * polls DOES carry two times, which is the case being covered here.
 */
function sourceOf(games) {
  const map = Object.fromEntries(games.map((g) => [g.game_key, g]));
  return { state: { games: map }, health: () => ({ level: 'fresh' }) };
}

test('updated_at is the NEWEST observation, not the oldest and not now', async () => {
  const older = '2026-11-08T18:00:00.000Z';
  const newer = '2026-11-08T18:59:00.000Z';
  const row = (key, observedAt) => ({
    game_key: key, status: 'in_progress', observed_at: observedAt,
    provider_game_id: Number(key.charCodeAt(0)), home: {}, away: {},
  });

  // Newest second, so a reader taking the last one it saw would also pass.
  const ascending = createServer({ source: sourceOf([row('a', older), row('b', newer)]), now: () => NOW });
  const first = JSON.parse(await (await ascending(get('/ncaaf/live_scores.json'))).text());
  assert.equal(first.updated_at, newer);
  assert.notEqual(first.updated_at, older);
  assert.notEqual(first.updated_at, NOW.toISOString());

  // And newest FIRST, so neither iteration order can pass by accident.
  const descending = createServer({ source: sourceOf([row('b', newer), row('a', older)]), now: () => NOW });
  const second = JSON.parse(await (await descending(get('/ncaaf/live_scores.json'))).text());
  assert.equal(second.updated_at, newer);
});

test('a game with an unreadable observation does not become the reported time', async () => {
  const good = '2026-11-08T18:59:00.000Z';
  const handle = createServer({
    source: sourceOf([
      { game_key: 'a', status: 'in_progress', observed_at: 'not a time', provider_game_id: 1, home: {}, away: {} },
      { game_key: 'b', status: 'in_progress', observed_at: good, provider_game_id: 2, home: {}, away: {} },
    ]),
    now: () => NOW,
  });
  const body = JSON.parse(await (await handle(get('/ncaaf/live_scores.json'))).text());
  assert.equal(body.updated_at, good);
});

test('an empty slate is a 503, NOT an empty slate stamped with now', async () => {
  // Serving `{games: []}` with a serve-time stamp would be a real-looking
  // answer meaning "no games today". The page falls back to the committed
  // snapshot on a failure, which is the correct outcome.
  const coordinator = createCoordinator({
    provider: { async getLiveGames() { return []; } },
    now: () => NOW,
  });
  await coordinator.poll();
  const handle = createServer({ source: coordinator, now: () => NOW });
  const response = await handle(get('/ncaaf/live_scores.json'));
  assert.equal(response.status, 503);
  assert.equal(response.headers.get('retry-after'), '15');
  const body = JSON.parse(await response.text());
  assert.match(body.error, /no observation/);
  assert.ok(!('games' in body));
});

test('the box-score block is omitted when there is none, and present when there is', async () => {
  const { coordinator } = await loaded();
  const bare = createServer({ source: coordinator, now: () => NOW });
  const body = JSON.parse(await (await bare(get('/ncaaf/live_scores.json'))).text());
  assert.deepEqual(body.player_boxscores, {});
  assert.deepEqual(body.player_boxscore_times, {});
  assert.ok(!('player_stats_checked_at' in body));
});

test('a game with no legacy status is counted in a header rather than lost quietly', async () => {
  const coordinator = createCoordinator({
    provider: {
      async getLiveGames() {
        return [
          { game_key: 'ok', status: 'in_progress', provider_game_id: 1,
            observed_at: NOW.toISOString(), home: {}, away: {} },
          // `unknown` has no legacy spelling; it is refused rather than mapped
          // to something plausible.
          { game_key: 'weird', status: 'unknown', provider_game_id: 2,
            observed_at: NOW.toISOString(), home: {}, away: {} },
        ];
      },
    },
    now: () => NOW,
  });
  await coordinator.poll();
  const handle = createServer({ source: coordinator, now: () => NOW });
  const response = await handle(get('/ncaaf/live_scores.json'));
  assert.equal(response.headers.get('x-elocast-dropped'), '1');
  const body = JSON.parse(await response.text());
  assert.equal(body.games.length, 1);
});

// --- /v1 --------------------------------------------------------------------

test('the normalized slate carries the games, the health and a serve time', async () => {
  const { coordinator } = await loaded();
  const handle = createServer({ source: coordinator, now: () => NOW });
  const body = JSON.parse(await (await handle(get('/v1/ncaaf/live'))).text());
  assert.equal(body.sport, 'ncaaf');
  assert.equal(body.served_at, NOW.toISOString());
  assert.equal(body.games.length, 63);
  assert.match(body.games[0].game_key, /^ncaaf:cfbd:game:\d+$/);
  // served_at and the observation are SEPARATE fields and must not be equal
  // here: collapsing them is how serve time gets reported as freshness.
  assert.ok(body.health);
  assert.notEqual(body.games[0].observed_at, body.served_at);
});

test('one game is addressable by its namespaced key', async () => {
  const { coordinator } = await loaded();
  const handle = createServer({ source: coordinator, now: () => NOW });
  const key = Object.keys(coordinator.state.games)[0];
  const response = await handle(get('/v1/ncaaf/games/' + encodeURIComponent(key)));
  assert.equal(response.status, 200);
  const body = JSON.parse(await response.text());
  assert.equal(body.game.game_key, key);
  assert.equal(body.served_at, NOW.toISOString());
});

test('an unknown game key is a 404 that names the key rather than a blank wall', async () => {
  const { coordinator } = await loaded();
  const handle = createServer({ source: coordinator, now: () => NOW });
  const response = await handle(get('/v1/ncaaf/games/ncaaf:cfbd:game:999999'));
  assert.equal(response.status, 404);
  const body = JSON.parse(await response.text());
  assert.equal(body.game_key, 'ncaaf:cfbd:game:999999');
});

test('health is a 200 even when the news is bad', async () => {
  // A monitor that only sees a status code cannot tell "stale" from
  // "unreachable" if staleness is also a 5xx. The level is in the body.
  const { coordinator, stamp } = await loaded();
  const handle = createServer({
    source: coordinator,
    now: () => new Date(Date.parse(stamp) + 3600_000),
  });
  const response = await handle(get('/v1/health'));
  assert.equal(response.status, 200);
  const body = JSON.parse(await response.text());
  assert.equal(body.level, 'stale');
  assert.ok(body.age_seconds >= 3600);
  assert.equal(body.polls, 1);
});

test('health on a service that has never polled says so rather than looking well', async () => {
  const handle = createServer({
    source: createCoordinator({ provider: { async getLiveGames() { return []; } } }),
    now: () => NOW,
  });
  const body = JSON.parse(await (await handle(get('/v1/health'))).text());
  assert.equal(body.level, 'unknown');
  assert.equal(body.games_tracked, 0);
  assert.equal(body.polls, 0);
  assert.equal(body.last_accepted_at, null);
  assert.match(body.reason, /no games/);
});

// --- the rest of the surface ------------------------------------------------

test('every live response is no-store and cross-origin readable', async () => {
  // Without the CORS header the browser discards a perfectly good response and
  // the page falls back to the static file, which looks like an outage. And a
  // cached live score is the exact failure this service exists to remove.
  const { coordinator } = await loaded();
  const handle = createServer({ source: coordinator, now: () => NOW });
  for (const path of ['/v1/health', '/v1/ncaaf/live', '/ncaaf/live_scores.json']) {
    const response = await handle(get(path));
    assert.equal(response.headers.get('cache-control'), 'no-store', path);
    assert.equal(response.headers.get('access-control-allow-origin'), '*', path);
  }
});

test('a preflight is answered', async () => {
  const { coordinator } = await loaded();
  const handle = createServer({ source: coordinator, now: () => NOW });
  const response = await handle(get('/v1/ncaaf/live', { method: 'OPTIONS' }));
  assert.equal(response.status, 204);
  assert.match(response.headers.get('access-control-allow-methods'), /GET/);
});

test('a write method is refused with Allow, not quietly treated as a read', async () => {
  const { coordinator } = await loaded();
  const handle = createServer({ source: coordinator, now: () => NOW });
  for (const method of ['POST', 'PUT', 'DELETE', 'PATCH']) {
    const response = await handle(get('/v1/ncaaf/live', { method }));
    assert.equal(response.status, 405, method);
    assert.match(response.headers.get('allow'), /GET/, method);
  }
});

test('an unknown path lists the routes', async () => {
  const { coordinator } = await loaded();
  const handle = createServer({ source: coordinator, now: () => NOW });
  const response = await handle(get('/v1/nope'));
  assert.equal(response.status, 404);
  const body = JSON.parse(await response.text());
  assert.deepEqual(body.routes, [...ROUTES]);
  assert.equal(ROUTES.length, 4);
});

test('a trailing slash and a query string do not change the route', async () => {
  const { coordinator } = await loaded();
  const handle = createServer({ source: coordinator, now: () => NOW });
  for (const path of ['/v1/health', '/v1/health/', '/v1/health//', '/v1/health?t=123']) {
    const response = await handle(get(path));
    assert.equal(response.status, 200, path);
    assert.equal(JSON.parse(await response.text()).polls, 1, path);
  }
  // The cache-buster the page appends must not break the legacy endpoint.
  const busted = await handle(get('/ncaaf/live_scores.json?t=1762628400000'));
  assert.equal(busted.status, 200);
});

test('the server refuses to exist without a source', () => {
  assert.throws(() => createServer(), TypeError);
  assert.throws(() => createServer({ source: {} }), /health/);
});

test('A READER NEVER CAUSES A PROVIDER REQUEST', async () => {
  // 500 readers must not become 500 provider requests. This is the whole
  // architectural claim and it is enforced by construction: the handler is
  // given a source, never a provider.
  let calls = 0;
  const coordinator = createCoordinator({
    provider: { async getLiveGames() { calls += 1; return []; } },
    now: () => NOW,
  });
  await coordinator.poll();
  assert.equal(calls, 1);
  const handle = createServer({ source: coordinator, now: () => NOW });
  for (let i = 0; i < 50; i += 1) {
    await handle(get('/v1/health'));
    await handle(get('/v1/ncaaf/live'));
    await handle(get('/ncaaf/live_scores.json'));
  }
  assert.equal(calls, 1, 'serving a reader called the provider');
});

test('a microsecond observation is served verbatim, not truncated to milliseconds', async () => {
  // Python stamps datetime.isoformat(), which carries microseconds. A JS Date
  // holds only milliseconds, so rebuilding the string rewrites ...395792Z as
  // ...395Z. The differential test above caught exactly this.
  const precise = '2026-11-08T07:15:11.395792Z';
  const coordinator = createCoordinator({
    provider: {
      async getLiveGames() {
        return [{ game_key: 'a', status: 'in_progress', provider_game_id: 1,
          observed_at: precise, home: {}, away: {} }];
      },
    },
    now: () => new Date(precise),
  });
  await coordinator.poll();
  const handle = createServer({ source: coordinator, now: () => NOW });
  const body = JSON.parse(await (await handle(get('/ncaaf/live_scores.json'))).text());
  assert.equal(body.updated_at, precise);
  assert.ok(body.updated_at.endsWith('395792Z'), body.updated_at);
});
