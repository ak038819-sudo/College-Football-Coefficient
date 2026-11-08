/**
 * Milestone A's exit condition: contract tests passing against saved fixtures.
 *
 * The fixtures in `fixtures/` are REAL published snapshots, taken verbatim from
 * three commits of `ui/data/live_scores.json` in this repository's history
 * (2026-10-24 16:11, 2026-11-01 06:37, 2026-11-08 07:15). 175 games. Key order
 * is the publisher's own, so these tests can hold the projection to it.
 *
 * ONE EDIT WAS MADE TO THEM, and it is the only one: `player_boxscores` and
 * `player_boxscore_times` were cut to their first two games each, because a
 * full block is roughly 650 KB per fixture and the projection only passes that
 * block through without reading it. Everything else, including every top-level
 * key and `player_stats_checked_at`, is as published.
 *
 * WHAT IS REAL AND WHAT IS RECONSTRUCTED, stated plainly because it bears on
 * how much these tests prove. The fixtures are real OUTPUT of
 * `src/fetch_live_scores.py`. Raw CFBD `/scoreboard` responses are not
 * committed anywhere and the provider is unreachable from this container, so
 * the raw-shaped inputs below are reconstructed by inverting the field mapping
 * in `clean_game` (`awayTeam`/`homeTeam`, `startDate`, `neutralSite`,
 * camelCase). That means these tests verify the adapter against the project's
 * own normalization contract, NOT against bytes the provider actually sent. A
 * capture of one real response should replace the reconstruction in Milestone
 * B, and until it does, a field CFBD sends that `clean_game` silently ignores
 * would not be caught here.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, readdirSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

import { SCHEMA_VERSION, STATUSES, assertProvider, entityKey } from '../src/providers/provider.mjs';
import { createCfbdProvider, normalizeGame, normalizeScoreboard } from '../src/providers/cfbd.mjs';
import { serializeLegacy, toLegacyGame, toLegacySnapshot } from '../src/legacy.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const FIXTURES = join(HERE, 'fixtures');
const OBSERVED = '2026-11-08T07:15:11.395792Z';

function fixtures() {
  return readdirSync(FIXTURES).filter((name) => name.endsWith('.json')).map((name) => ({
    name,
    snapshot: JSON.parse(readFileSync(join(FIXTURES, name), 'utf8')),
  }));
}

/** Invert `clean_game`'s field mapping to get a raw-shaped row. */
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

/** One raw-shaped row, for the cases no published fixture happens to contain. */
const RAW = Object.freeze({
  id: 401871071,
  startDate: '2026-10-27T23:00:00.000Z',
  venue: 'Houchens-Smith Stadium',
  status: 'in_progress',
  period: 2,
  clock: '07:21',
  tv: 'CBSSN',
  neutralSite: false,
  awayTeam: { id: 48, name: 'Delaware Blue Hens', points: 35, classification: 'fbs' },
  homeTeam: { id: 98, name: 'Western Kentucky Hilltoppers', points: 21, classification: 'fbs' },
});

test('every fixture is a real published snapshot with games in it', () => {
  const all = fixtures();
  assert.equal(all.length, 3);
  let games = 0;
  for (const { name, snapshot } of all) {
    assert.equal(snapshot.source, 'CollegeFootballData.com', name);
    assert.match(snapshot.updated_at, /^\d{4}-\d{2}-\d{2}T.*Z$/, name);
    assert.ok(snapshot.games.length > 0, name);
    // Every published snapshot carries these. A fixture without them would let
    // the round-trip test pass while the projection drops the player panel.
    assert.equal(typeof snapshot.player_boxscores, 'object', name);
    assert.equal(typeof snapshot.player_boxscore_times, 'object', name);
    assert.match(snapshot.player_stats_checked_at, /^\d{4}-\d{2}-\d{2}T.*Z$/, name);
    games += snapshot.games.length;
  }
  assert.equal(games, 175);
});

// --- the differential test that makes the rewrite trustworthy ---------------

test('a real snapshot survives a normalize/legacy round trip unchanged', () => {
  for (const { name, snapshot } of fixtures()) {
    const raw = snapshot.games.map(toRaw);
    const normalized = normalizeScoreboard(raw, { observedAt: snapshot.updated_at });
    // The box-score block is pass-through data from the slower batch, so the
    // round trip is handed the same block the publisher had. If the projection
    // dropped, reordered or rewrote any of it, the comparison below fails.
    const { snapshot: rebuilt, dropped } = toLegacySnapshot(normalized, {
      updatedAt: snapshot.updated_at,
      players: {
        boxscores: snapshot.player_boxscores,
        times: snapshot.player_boxscore_times,
        checkedAt: snapshot.player_stats_checked_at,
      },
    });
    assert.deepEqual(dropped, [], `${name}: games dropped`);
    assert.deepEqual(rebuilt, snapshot, `${name}: round trip changed the payload`);
    // Key order too: the legacy contract asks for a byte-for-byte match, and a
    // deepEqual would pass on a reordered object.
    assert.equal(JSON.stringify(rebuilt), JSON.stringify(snapshot), `${name}: key order changed`);
  }
});

test('the round trip preserves every game of every status present', () => {
  const seen = new Set();
  for (const { snapshot } of fixtures()) {
    for (const game of snapshot.games) seen.add(game.status);
  }
  // If a future fixture introduces a status, this fails rather than silently
  // testing less than it claims.
  assert.deepEqual([...seen].sort(), ['completed', 'in_progress', 'scheduled']);
});

// --- identity ---------------------------------------------------------------

test('game and team keys are namespaced, never a bare provider id', () => {
  const { snapshot } = fixtures()[0];
  const games = normalizeScoreboard(snapshot.games.map(toRaw), { observedAt: OBSERVED });
  for (const game of games) {
    assert.match(game.game_key, /^ncaaf:cfbd:game:\d+$/);
    for (const side of ['home', 'away']) {
      if (game[side].provider_team_id !== null) {
        assert.equal(game[side].team_key, `ncaaf:cfbd:team:${game[side].provider_team_id}`);
      }
    }
  }
});

test('entityKey refuses components that would make an ambiguous key', async () => {
  const key = entityKey;
  assert.equal(key('ncaaf', 'cfbd', 'game', 42), 'ncaaf:cfbd:game:42');
  for (const bad of [['', 'cfbd', 'game', 1], ['nc:af', 'cfbd', 'game', 1], ['ncaaf', '', 'game', 1]]) {
    assert.throws(() => key(...bad), TypeError, `accepted ${JSON.stringify(bad)}`);
  }
  for (const badId of [1.5, '', 'a:b', null, undefined, {}]) {
    assert.throws(() => key('ncaaf', 'cfbd', 'game', badId), TypeError, `accepted id ${JSON.stringify(badId)}`);
  }
});

test('institution_id comes from the lookup and is null when unknown', () => {
  const raw = [toRaw(fixtures()[0].snapshot.games[0])];
  const withLookup = normalizeScoreboard(raw, {
    observedAt: OBSERVED,
    institutionLookup: (id) => (id === raw[0].homeTeam.id ? 57 : null),
  });
  assert.equal(withLookup[0].home.institution_id, 57);
  assert.equal(withLookup[0].away.institution_id, null);
  const without = normalizeScoreboard(raw, { observedAt: OBSERVED });
  assert.equal(without[0].home.institution_id, null);
});

// --- never invent a value ---------------------------------------------------

test('a missing score is null and never zero', () => {
  const base = toRaw(fixtures()[0].snapshot.games[0]);
  const game = normalizeGame({ ...base, awayTeam: { ...base.awayTeam, points: null } }, { observedAt: OBSERVED });
  assert.equal(game.away.score, null);
  assert.notEqual(game.away.score, 0);
});

test('a missing clock, period, venue and possession are null, not defaults', () => {
  const base = toRaw(fixtures()[0].snapshot.games[0]);
  const game = normalizeGame(
    { ...base, clock: null, period: null, venue: '   ', tv: '' },
    { observedAt: OBSERVED },
  );
  assert.equal(game.clock, null);
  assert.equal(game.period, null);
  assert.equal(game.venue, null);
  assert.equal(game.tv, null);
  assert.equal(game.possession_team_key, null);
});

test('provider_updated_at is always null for this provider, never the observation time', () => {
  const games = normalizeScoreboard(fixtures()[0].snapshot.games.map(toRaw), { observedAt: OBSERVED });
  for (const game of games) {
    assert.equal(game.provider_updated_at, null);
    assert.equal(game.observed_at, OBSERVED);
  }
  // The specific confusion this guards: conflating the two turns provider lag
  // into our lag, which is the plan's own watch-out.
  assert.notEqual(games[0].provider_updated_at, games[0].observed_at);
});

test('a nonsense score is rejected to null rather than passed through', () => {
  const base = toRaw(fixtures()[0].snapshot.games[0]);
  for (const bad of [-1, 1.5, '21', true, {}]) {
    const game = normalizeGame({ ...base, homeTeam: { ...base.homeTeam, points: bad } }, { observedAt: OBSERVED });
    assert.equal(game.home.score, null, `accepted ${JSON.stringify(bad)}`);
  }
});

// --- status -----------------------------------------------------------------

test('completed becomes final, and an unrecognised status is kept as unknown', () => {
  const base = toRaw(fixtures()[0].snapshot.games[0]);
  assert.equal(normalizeGame({ ...base, status: 'completed' }, { observedAt: OBSERVED }).status, 'final');
  const odd = normalizeGame({ ...base, status: 'weather_delay' }, { observedAt: OBSERVED });
  assert.equal(odd.status, 'unknown');
  // The provider's own word is retained so a new status can be diagnosed
  // without re-polling.
  assert.equal(odd.provider_status, 'weather_delay');
  // And it must NOT be quietly read as scheduled.
  assert.notEqual(odd.status, 'scheduled');
});

test('an unknown status has no legacy spelling and is refused rather than guessed', () => {
  const base = toRaw(fixtures()[0].snapshot.games[0]);
  const odd = normalizeGame({ ...base, status: 'weather_delay' }, { observedAt: OBSERVED });
  assert.equal(toLegacyGame(odd), null);
  const { snapshot, dropped } = toLegacySnapshot([odd], { updatedAt: OBSERVED });
  assert.deepEqual(snapshot.games, []);
  assert.deepEqual(dropped, [{ game_key: odd.game_key, status: 'unknown' }]);
});

// --- failing the poll rather than serving a short slate ---------------------

test('a duplicate game id fails the whole poll', () => {
  const raw = fixtures()[0].snapshot.games.slice(0, 2).map(toRaw);
  raw[1].id = raw[0].id;
  assert.throws(() => normalizeScoreboard(raw, { observedAt: OBSERVED }), /duplicate scoreboard game id/);
});

test('one unparseable row fails the poll instead of being skipped', () => {
  const raw = fixtures()[0].snapshot.games.slice(0, 3).map(toRaw);
  raw[1] = { ...raw[1], id: 'not-an-id' };
  assert.throws(() => normalizeScoreboard(raw, { observedAt: OBSERVED }), /invalid scoreboard game id/);
});

test('a non-list payload is refused', () => {
  for (const bad of [null, {}, 'games', 7]) {
    assert.throws(() => normalizeScoreboard(bad, { observedAt: OBSERVED }), /must be a list/);
  }
});

test('a team with no name fails the row', () => {
  const base = toRaw(fixtures()[0].snapshot.games[0]);
  assert.throws(
    () => normalizeGame({ ...base, homeTeam: { ...base.homeTeam, name: '  ' } }, { observedAt: OBSERVED }),
    /team has no name/,
  );
});

test('observedAt is required, so a game can never be stamped by accident', () => {
  const base = toRaw(fixtures()[0].snapshot.games[0]);
  assert.throws(() => normalizeGame(base, {}), TypeError);
  assert.throws(() => normalizeGame(base, { observedAt: '' }), TypeError);
});

// --- the adapter ------------------------------------------------------------

test('the adapter makes exactly ONE provider request per cycle', async () => {
  const calls = [];
  const payload = fixtures()[0].snapshot.games.map(toRaw);
  const provider = createCfbdProvider({
    apiKey: 'test-key',
    now: () => new Date(OBSERVED),
    fetchImpl: async (url, init) => {
      calls.push({ url, init });
      return { ok: true, status: 200, json: async () => payload };
    },
  });
  const games = await provider.getLiveGames();
  assert.equal(calls.length, 1, 'the §4 budget assumes one request per cycle');
  assert.match(calls[0].url, /\/scoreboard\?classification=fbs$/);
  assert.equal(games.length, payload.length);
});

test('the key goes in the Authorization header and never into the URL', async () => {
  const calls = [];
  const provider = createCfbdProvider({
    apiKey: 'secret-key',
    fetchImpl: async (url, init) => {
      calls.push({ url, init });
      return { ok: true, status: 200, json: async () => [] };
    },
  });
  await provider.getLiveGames();
  assert.equal(calls[0].init.headers.Authorization, 'Bearer secret-key');
  assert.ok(!calls[0].url.includes('secret-key'), 'the key leaked into the URL');
});

test('the adapter refuses to exist without a key', () => {
  for (const bad of [undefined, '', null, 7]) {
    assert.throws(() => createCfbdProvider({ apiKey: bad }), TypeError);
  }
});

test('a 429 or 5xx is marked retryable and a 401 or 404 is not', async () => {
  for (const [status, retryable] of [[429, true], [500, true], [503, true], [401, false], [404, false]]) {
    const provider = createCfbdProvider({
      apiKey: 'k',
      fetchImpl: async () => ({ ok: false, status, json: async () => ({}) }),
    });
    await assert.rejects(provider.getLiveGames(), (error) => {
      assert.equal(error.status, status);
      assert.equal(error.retryable, retryable, `status ${status}`);
      return true;
    });
  }
});

test('every game the adapter emits carries the schema version and sport', async () => {
  const payload = fixtures()[0].snapshot.games.map(toRaw);
  const provider = createCfbdProvider({
    apiKey: 'k',
    now: () => new Date(OBSERVED),
    fetchImpl: async () => ({ ok: true, status: 200, json: async () => payload }),
  });
  const version = SCHEMA_VERSION;
  const statuses = STATUSES;
  for (const game of await provider.getLiveGames()) {
    assert.equal(game.schema_version, version);
    assert.equal(game.sport, 'ncaaf');
    assert.ok(statuses.includes(game.status), `status ${game.status} is not in the contract`);
  }
});

test('the adapter satisfies the declared provider interface', async () => {
  const check = assertProvider;
  const provider = createCfbdProvider({ apiKey: 'k', fetchImpl: async () => ({ ok: true, status: 200, json: async () => [] }) });
  assert.equal(check(provider), provider);
  assert.throws(() => check({ sport: 'ncaaf', provider: 'cfbd' }), /getLiveGames/);
  assert.throws(() => check({ getLiveGames() {}, getGame() {}, getProviderHealth() {} }), /sport/);
});

// --- updated_at is the observation, not the serve time ---------------------

test('the legacy snapshot refuses to be built without an observation time', () => {
  assert.throws(() => toLegacySnapshot([], {}), TypeError);
  assert.throws(() => toLegacySnapshot([], { updatedAt: '' }), TypeError);
});

test('updated_at carries straight through from the observation', () => {
  const { snapshot: source } = fixtures()[0];
  const games = normalizeScoreboard(source.games.map(toRaw), { observedAt: source.updated_at });
  const { snapshot } = toLegacySnapshot(games, { updatedAt: source.updated_at });
  assert.equal(snapshot.updated_at, source.updated_at);
});

// --- serialization ----------------------------------------------------------

/**
 * The publisher's bytes, not my reading of them.
 *
 * `serializeLegacy` claims to produce what `json.dumps(snapshot,
 * separators=(',', ':'), ensure_ascii=False)` produces. Rather than trust the
 * claim, this runs the real Python against the real fixture and compares.
 *
 * IT FOUND ONE DIVERGENCE JAVASCRIPT CANNOT CLOSE, and the test now pins it so
 * it cannot widen. A JS object puts integer-like keys FIRST, in ascending
 * numeric order, whatever order they were inserted or parsed in; Python keeps
 * insertion order. `player_boxscores` and `player_boxscore_times` are keyed by
 * provider game id, so the moment a published snapshot lists those ids out of
 * ascending order -- `live_afternoon.json` does -- no amount of care with
 * `JSON.stringify` reproduces it.
 *
 * Everything else, the whole `games` array and every scalar, matches byte for
 * byte. That is asserted below on the real fixtures, with the two maps removed
 * and then compared again parse-equal. The divergence does not reach the
 * consumer: `ui/dashboard_shell.html` reads
 * `liveSnapshot?.player_boxscores?.[String(g.id)]`, a lookup by key, so the
 * order of that map is not information. It does mean the Milestone D canary
 * must compare bytes of the trimmed payload plus parse-equality of the two
 * maps, rather than bytes of the whole thing.
 */
function pythonCompact(text) {
  const result = spawnSync('python3', [
    '-c',
    'import json,sys;sys.stdout.write(json.dumps(json.load(sys.stdin),separators=(",",":"),ensure_ascii=False))',
  ], { input: text, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 });
  assert.equal(result.status, 0, `python failed: ${result.stderr}`);
  return result.stdout;
}

test('serializeLegacy matches the publisher byte for byte outside the id-keyed maps', (t) => {
  if (spawnSync('python3', ['-c', 'print(1)'], { encoding: 'utf8' }).status !== 0) {
    t.skip('python3 unavailable, so the byte comparison cannot be made');
    return;
  }
  let sawOutOfOrderIds = false;
  for (const { name, snapshot } of fixtures()) {
    const text = readFileSync(join(FIXTURES, name), 'utf8');

    // The id-keyed maps, compared as data rather than as bytes.
    const published = JSON.parse(text);
    assert.deepEqual(snapshot.player_boxscores, published.player_boxscores, name);
    assert.deepEqual(snapshot.player_boxscore_times, published.player_boxscore_times, name);

    // Everything else, compared as bytes.
    const strip = (value) => {
      const { player_boxscores: _a, player_boxscore_times: _b, ...rest } = value;
      return rest;
    };
    assert.equal(
      serializeLegacy(strip(snapshot)),
      pythonCompact(JSON.stringify(strip(published))),
      `${name}: serialization differs from the publisher outside the id-keyed maps`,
    );

    // The published order CANNOT be read back with JSON.parse -- that is the
    // whole defect -- so Python reports it from the file's own bytes.
    const ids = JSON.parse(spawnSync('python3', [
      '-c',
      'import json,sys;sys.stdout.write(json.dumps(list(json.load(sys.stdin)["player_boxscores"].keys())))',
    ], { input: text, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 }).stdout);
    const ascending = [...ids].sort((a, b) => Number(a) - Number(b));
    if (JSON.stringify(ids) !== JSON.stringify(ascending)) sawOutOfOrderIds = true;
  }
  // If no fixture exercises the divergence, the carve-out above is untested and
  // this test is claiming more care than it takes.
  assert.ok(sawOutOfOrderIds, 'no fixture has out-of-ascending-order game ids; the carve-out is untested');
});

test('a JS object cannot hold id-keyed insertion order, which is why the carve-out exists', () => {
  // Pinning the language behaviour itself, so a reader does not have to take
  // the comment above on faith and a future engine change is caught here.
  assert.deepEqual(Object.keys(JSON.parse('{"9":1,"2":1,"zz":1,"aa":1}')), ['2', '9', 'zz', 'aa']);
});

// --- the three gaps the mutation check found --------------------------------

/**
 * None of these three is exercised by a real fixture, and the mutation check
 * said so: a mutant that broke each one left the suite green. They are kept
 * together because they share a cause -- the fixtures are a sample of a quiet
 * season, not a specification -- and because that is the whole argument for
 * mutation-checking a differential suite rather than trusting its size.
 */

test('neutral_site is STRICTLY true, never merely truthy', () => {
  // `clean_game` writes `raw.get('neutralSite') is True`. A provider sending
  // the string "false", or 1, or "N", would come out of a truthy coercion as a
  // neutral-site game, which changes which stadium the page names.
  for (const value of [1, 'false', 'no', {}, [], 'true']) {
    const game = normalizeGame({ ...RAW, neutralSite: value }, { observedAt: OBSERVED });
    assert.equal(game.neutral_site, false, `${JSON.stringify(value)} became a neutral site`);
  }
  assert.equal(normalizeGame({ ...RAW, neutralSite: true }, { observedAt: OBSERVED }).neutral_site, true);
  for (const value of [false, null, undefined]) {
    assert.equal(normalizeGame({ ...RAW, neutralSite: value }, { observedAt: OBSERVED }).neutral_site, false);
  }
});

test('the venue is trimmed to 120 characters, as the publisher trims it', () => {
  // No published fixture has a venue this long, so only a direct test holds the
  // limit. It exists because the legacy payload is size-sensitive and the
  // Python has always capped it; dropping the cap here would be silent drift.
  const long = 'A'.repeat(200);
  const game = normalizeGame({ ...RAW, venue: long }, { observedAt: OBSERVED });
  assert.equal(game.venue.length, 120);
  assert.equal(game.venue, 'A'.repeat(120));
  // Exactly 120 is untouched, and a short one is not padded.
  assert.equal(normalizeGame({ ...RAW, venue: 'B'.repeat(120) }, { observedAt: OBSERVED }).venue, 'B'.repeat(120));
  assert.equal(normalizeGame({ ...RAW, venue: '  Kinnick Stadium  ' }, { observedAt: OBSERVED }).venue, 'Kinnick Stadium');
});

test('player_stats_checked_at is OMITTED when there is none, never emitted as null', () => {
  // The page reads this field to label the player panel with when the stats
  // were last checked. `merge_player_boxscores` leaves the key off when it has
  // no time to report; emitting null instead would hand the page a value where
  // it expects absence, and the two maps must still be present and empty.
  const { snapshot } = toLegacySnapshot([], { updatedAt: OBSERVED });
  assert.deepEqual(Object.keys(snapshot), ['source', 'updated_at', 'games', 'player_boxscores', 'player_boxscore_times']);
  assert.ok(!('player_stats_checked_at' in snapshot));
  assert.deepEqual(snapshot.player_boxscores, {});
  assert.deepEqual(snapshot.player_boxscore_times, {});

  // An empty string is not a time either.
  const blank = toLegacySnapshot([], { updatedAt: OBSERVED, players: { checkedAt: '' } }).snapshot;
  assert.ok(!('player_stats_checked_at' in blank));

  // And when there IS one, it is last, as the publisher writes it.
  const full = toLegacySnapshot([], {
    updatedAt: OBSERVED,
    players: { boxscores: { 7: [] }, times: { 7: OBSERVED }, checkedAt: OBSERVED },
  }).snapshot;
  assert.deepEqual(Object.keys(full), ['source', 'updated_at', 'games', 'player_boxscores', 'player_boxscore_times', 'player_stats_checked_at']);
  assert.equal(full.player_stats_checked_at, OBSERVED);
});
