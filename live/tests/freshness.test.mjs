/**
 * Freshness classification, and the one rule in it that is not negotiable:
 * never quietly present an old score as live.
 *
 * Most of these tests are about the cases where the honest answer is "I do not
 * know", because those are the ones a naive implementation gets wrong in the
 * dangerous direction. A missing timestamp defaulting to fresh, or a
 * future-stamped observation clamped to zero age, both render as live. Each is
 * tested here, and each was checked by mutating `freshness.mjs` to the plausible
 * wrong behaviour and confirming the test goes red.
 */
import test from 'node:test';
import assert from 'node:assert/strict';

import {
  DEFAULT_THRESHOLDS,
  DELAYED,
  FRESH,
  STALE,
  UNKNOWN,
  classify,
  health,
} from '../src/state/freshness.mjs';

const NOW = '2026-11-08T07:00:00.000Z';
const at = (secondsAgo) => new Date(Date.parse(NOW) - secondsAgo * 1000).toISOString();

// --- classify ---------------------------------------------------------------

test('the published thresholds are the proposal\'s 30 and 120 seconds', () => {
  assert.equal(DEFAULT_THRESHOLDS.freshUnderSeconds, 30);
  assert.equal(DEFAULT_THRESHOLDS.delayedUnderSeconds, 120);
});

test('ages either side of each boundary classify as documented', () => {
  const cases = [
    [0, FRESH], [29, FRESH], [29.9, FRESH],
    [30, DELAYED], [31, DELAYED], [119, DELAYED], [119.9, DELAYED],
    [120, STALE], [121, STALE], [3600, STALE],
  ];
  for (const [seconds, level] of cases) {
    const verdict = classify(at(seconds), NOW);
    assert.equal(verdict.level, level, `${seconds}s should be ${level}, got ${verdict.level}`);
    assert.equal(Math.round(verdict.ageSeconds * 10) / 10, seconds);
  }
});

test('a missing or unparseable observation is UNKNOWN, never FRESH', () => {
  for (const bad of [undefined, null, '', 'just now', 0, {}, new Date(NOW)]) {
    const verdict = classify(bad, NOW);
    assert.equal(verdict.level, UNKNOWN, `${JSON.stringify(bad)} classified as ${verdict.level}`);
    assert.equal(verdict.ageSeconds, null);
    assert.ok(verdict.reason, 'UNKNOWN with no reason tells the page nothing to say');
  }
});

test('an observation stamped in the FUTURE is UNKNOWN, not clamped to fresh', () => {
  // Clock skew and a provider stamping serve-time both produce this. Treating
  // the future as maximally fresh is exactly backwards.
  const verdict = classify(at(-45), NOW);
  assert.equal(verdict.level, UNKNOWN);
  assert.ok(verdict.ageSeconds < 0, 'the negative age is hidden rather than reported');
  assert.match(verdict.reason, /skew/);
});

test('an unparseable NOW is UNKNOWN rather than a huge age', () => {
  assert.equal(classify(at(5), 'whenever').level, UNKNOWN);
});

test('now may be a Date or a string, and they agree', () => {
  assert.deepEqual(classify(at(45), new Date(NOW)), classify(at(45), NOW));
});

test('custom thresholds are honoured, and a partial override keeps the other default', () => {
  assert.equal(classify(at(45), NOW, { freshUnderSeconds: 60 }).level, FRESH);
  assert.equal(classify(at(10), NOW, { freshUnderSeconds: 5 }).level, DELAYED);
  // delayedUnderSeconds left out: still 120, so 45s is delayed and not stale.
  assert.equal(classify(at(45), NOW, { freshUnderSeconds: 5 }).level, DELAYED);
  assert.equal(classify(at(200), NOW, { freshUnderSeconds: 5 }).level, STALE);
});

test('a fresh verdict carries no reason and a late one does', () => {
  assert.equal(classify(at(1), NOW).reason, null);
  assert.ok(classify(at(60), NOW).reason, 'a delayed snapshot has nothing to say');
  assert.ok(classify(at(600), NOW).reason, 'a stale snapshot has nothing to say');
});

// --- health -----------------------------------------------------------------

function game(key, status, secondsAgo) {
  return { game_key: key, status, observed_at: secondsAgo === null ? null : at(secondsAgo) };
}

test('health reports the OLDEST live game, not the newest', () => {
  // This is the exact failure the audit measured: one game updating keeps the
  // scoreboard looking alive while another sits frozen. A health check reading
  // the newest observation called that healthy for weeks.
  const games = {
    a: game('a', 'in_progress', 2),
    b: game('b', 'in_progress', 1800),
    c: game('c', 'in_progress', 5),
  };
  const result = health(games, NOW);
  assert.equal(result.worst_game_key, 'b');
  assert.equal(result.level, STALE);
  assert.equal(result.age_seconds, 1800);
  assert.equal(result.games_live, 3);
  assert.equal(result.games_tracked, 3);
});

test('a frozen game among fresh ones makes the whole slate unhealthy', () => {
  const games = { a: game('a', 'in_progress', 1), b: game('b', 'in_progress', 400) };
  assert.equal(health(games, NOW).level, STALE);
});

test('final and scheduled games are ignored while anything is live', () => {
  const games = {
    done: game('done', 'final', 86400),
    soon: game('soon', 'scheduled', 86400),
    live: game('live', 'in_progress', 3),
  };
  const result = health(games, NOW);
  assert.equal(result.level, FRESH);
  assert.equal(result.worst_game_key, 'live');
  assert.equal(result.games_live, 1);
  assert.equal(result.games_tracked, 3);
  assert.equal(result.basis, 'oldest live game');
});

test('halftime counts as live: a game at halftime is still being watched', () => {
  const games = { h: game('h', 'halftime', 900), f: game('f', 'final', 1) };
  const result = health(games, NOW);
  assert.equal(result.games_live, 1);
  assert.equal(result.worst_game_key, 'h');
  assert.equal(result.level, STALE);
});

test('with no live games the basis is stated rather than implied', () => {
  const games = { a: game('a', 'final', 50), b: game('b', 'scheduled', 500) };
  const result = health(games, NOW);
  assert.equal(result.basis, 'oldest game of any status');
  assert.equal(result.games_live, 0);
  assert.equal(result.worst_game_key, 'b');
});

test('UNKNOWN outranks any age: a game we cannot vouch for is the worst case', () => {
  const games = {
    old: game('old', 'in_progress', 99999),
    blank: game('blank', 'in_progress', null),
  };
  const result = health(games, NOW);
  assert.equal(result.level, UNKNOWN);
  assert.equal(result.worst_game_key, 'blank');
});

test('UNKNOWN wins whichever order the games are iterated in', () => {
  // The comparison has two branches, one for each order. A mutation that keeps
  // only the first leaves this red.
  const blank = game('blank', 'in_progress', null);
  const old = game('old', 'in_progress', 99999);
  assert.equal(health({ blank, old }, NOW).worst_game_key, 'blank');
  assert.equal(health({ old, blank }, NOW).worst_game_key, 'blank');
});

test('no games tracked reports UNKNOWN with a reason, not a healthy zero', () => {
  for (const empty of [{}, null, undefined]) {
    const result = health(empty, NOW);
    assert.equal(result.level, UNKNOWN);
    assert.equal(result.age_seconds, null);
    assert.equal(result.games_tracked, 0);
    assert.equal(result.worst_game_key, null);
    assert.match(result.reason, /no games/);
  }
});

test('health passes its thresholds through to the classification', () => {
  const games = { a: game('a', 'in_progress', 45) };
  assert.equal(health(games, NOW).level, DELAYED);
  assert.equal(health(games, NOW, { freshUnderSeconds: 60 }).level, FRESH);
});

test('a single live game reports its own age as the slate age', () => {
  const result = health({ a: game('a', 'in_progress', 7) }, NOW);
  assert.equal(result.age_seconds, 7);
  assert.equal(result.level, FRESH);
  assert.equal(result.reason, null);
});
