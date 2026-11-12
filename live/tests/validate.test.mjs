/**
 * The four hazards from the proposal's §6, each as a test that fails if the
 * rule is removed.
 *
 * These are not contract tests against a fixture; they are tests of a decision
 * procedure, so they are written as the smallest pair of states that can tell
 * the right answer from a plausible wrong one. Every one of them was checked by
 * mutating `validate.mjs` and confirming it goes red -- a test of a guard that
 * passes with the guard deleted is the failure mode this repository has been
 * bitten by repeatedly.
 */
import test from 'node:test';
import assert from 'node:assert/strict';

import {
  ACCEPT,
  ACCEPT_CORRECTION,
  REJECT_REVERTS_FINAL,
  REJECT_STALE,
  finalizationEvents,
  mergePoll,
  reconcile,
} from '../src/ingestion/validate.mjs';

const T1 = '2026-11-08T07:00:00.000Z';
const T2 = '2026-11-08T07:00:15.000Z';

function game(overrides = {}) {
  return {
    game_key: 'ncaaf:cfbd:game:1',
    sport: 'ncaaf',
    provider_game_id: 1,
    status: 'in_progress',
    observed_at: T1,
    home: { score: 7 },
    away: { score: 3 },
    ...overrides,
  };
}

// --- hazard 4: overlapping polls -------------------------------------------

test('a first observation is accepted at version 1', () => {
  const result = reconcile(undefined, game());
  assert.equal(result.outcome, ACCEPT);
  assert.equal(result.version, 1);
  assert.equal(result.game.version, 1);
});

test('an older observation is rejected and the stored game stands', () => {
  const stored = { ...game({ observed_at: T2 }), version: 4 };
  const result = reconcile(stored, game({ observed_at: T1, home: { score: 99 } }));
  assert.equal(result.outcome, REJECT_STALE);
  assert.equal(result.version, 4);
  assert.equal(result.game, stored);
  assert.equal(result.game.home.score, 7, 'the stored score was overwritten by an older poll');
});

test('an observation with the SAME timestamp is rejected, not accepted', () => {
  // Two polls in the same millisecond carry no information about which is
  // current. Accepting the second would make the outcome depend on arrival
  // order, which is exactly the race the versioning is here to remove.
  const stored = { ...game({ observed_at: T1 }), version: 2 };
  const result = reconcile(stored, game({ observed_at: T1, home: { score: 14 } }));
  assert.equal(result.outcome, REJECT_STALE);
  assert.equal(result.game.home.score, 7);
});

test('a newer observation increments the version by exactly one', () => {
  const stored = { ...game({ observed_at: T1 }), version: 7 };
  const result = reconcile(stored, game({ observed_at: T2 }));
  assert.equal(result.outcome, ACCEPT);
  assert.equal(result.version, 8);
  assert.equal(result.game.version, 8);
});

test('a stored game with no version number still produces version 1', () => {
  const stored = { ...game({ observed_at: T1 }) };
  delete stored.version;
  assert.equal(reconcile(stored, game({ observed_at: T2 })).version, 1);
});

test('an unparseable observation time throws rather than being treated as now', () => {
  assert.throws(() => reconcile(undefined, game({ observed_at: 'yesterday' })), TypeError);
  assert.throws(() => reconcile(undefined, game({ observed_at: undefined })), TypeError);
  assert.throws(() => reconcile(undefined, null), TypeError);
});

test('an explicit observedAt overrides the one on the game', () => {
  const stored = { ...game({ observed_at: T2 }), version: 1 };
  // The game says T2 -- not newer than stored -- but the poll says T2+1s.
  const result = reconcile(stored, game({ observed_at: T2 }), { observedAt: '2026-11-08T07:00:16.000Z' });
  assert.equal(result.outcome, ACCEPT);
});

// --- hazard 3: a final game does not go back to live -----------------------

test('a final game is not reverted to in_progress by a provider outage', () => {
  const stored = { ...game({ status: 'final', observed_at: T1 }), version: 3 };
  const result = reconcile(stored, game({ status: 'in_progress', observed_at: T2 }));
  assert.equal(result.outcome, REJECT_REVERTS_FINAL);
  assert.equal(result.game.status, 'final');
  assert.equal(result.version, 3);
});

test('a final game is not reverted to scheduled or unknown either', () => {
  for (const status of ['scheduled', 'unknown', 'halftime']) {
    const stored = { ...game({ status: 'final', observed_at: T1 }), version: 1 };
    assert.equal(reconcile(stored, game({ status, observed_at: T2 })).outcome, REJECT_REVERTS_FINAL, status);
  }
});

test('a final game DOES accept a later final: that is how a correction arrives', () => {
  const stored = { ...game({ status: 'final', observed_at: T1, home: { score: 21 } }), version: 1 };
  const result = reconcile(stored, game({ status: 'final', observed_at: T2, home: { score: 20 } }));
  assert.equal(result.outcome, ACCEPT_CORRECTION);
  assert.equal(result.game.home.score, 20);
});

test('a live game may still move forward to final', () => {
  const stored = { ...game({ status: 'in_progress', observed_at: T1 }), version: 1 };
  assert.equal(reconcile(stored, game({ status: 'final', observed_at: T2 })).outcome, ACCEPT);
});

// --- hazard 2: a score going down is a correction, not an error ------------

test('a decreased score is accepted and labelled a correction', () => {
  const stored = { ...game({ home: { score: 14 }, observed_at: T1 }), version: 1 };
  const result = reconcile(stored, game({ home: { score: 7 }, observed_at: T2 }));
  assert.equal(result.outcome, ACCEPT_CORRECTION);
  assert.equal(result.game.home.score, 7, 'the correction was not written');
});

test('a decrease on either side is labelled, and an increase is not', () => {
  const base = { ...game({ home: { score: 14 }, away: { score: 10 }, observed_at: T1 }), version: 1 };
  const away = reconcile(base, game({ home: { score: 14 }, away: { score: 3 }, observed_at: T2 }));
  assert.equal(away.outcome, ACCEPT_CORRECTION, 'an away-side decrease was not labelled');
  const up = reconcile(base, game({ home: { score: 21 }, away: { score: 10 }, observed_at: T2 }));
  assert.equal(up.outcome, ACCEPT, 'an increase was labelled a correction');
});

test('a score becoming null, or arriving from null, is not called a correction', () => {
  // null is "we do not know", not a lower number. Labelling it a correction
  // would put a provider gap in the audit trail as an officials' reversal.
  const stored = { ...game({ home: { score: 14 }, observed_at: T1 }), version: 1 };
  assert.equal(reconcile(stored, game({ home: { score: null }, observed_at: T2 })).outcome, ACCEPT);
  const fromNull = { ...game({ home: { score: null }, observed_at: T1 }), version: 1 };
  assert.equal(reconcile(fromNull, game({ home: { score: 0 }, observed_at: T2 })).outcome, ACCEPT);
});

// --- hazard 1: a game missing from one poll has not disappeared ------------

test('a game absent from a poll is KEPT, untouched, at its existing version', () => {
  const stored = {
    'ncaaf:cfbd:game:1': { ...game({ game_key: 'ncaaf:cfbd:game:1', observed_at: T1 }), version: 5 },
    'ncaaf:cfbd:game:2': { ...game({ game_key: 'ncaaf:cfbd:game:2', observed_at: T1 }), version: 2 },
  };
  const result = mergePoll(stored, [game({ game_key: 'ncaaf:cfbd:game:1', observed_at: T2 })], { observedAt: T2 });
  assert.deepEqual(result.missing, ['ncaaf:cfbd:game:2']);
  assert.equal(result.games['ncaaf:cfbd:game:2'].version, 2, 'the absent game was rewritten');
  assert.deepEqual(result.games['ncaaf:cfbd:game:2'], stored['ncaaf:cfbd:game:2']);
  assert.equal(result.games['ncaaf:cfbd:game:1'].version, 6);
});

test('a poll reports applied and rejected separately, and rejections do not stop it', () => {
  // Every game in one poll shares one observation time, so the rejection case
  // inside a poll is a game the store already has at that time or later -- a
  // retried or overlapping poll -- not a per-game older stamp.
  const stored = {
    'ncaaf:cfbd:game:1': { ...game({ game_key: 'ncaaf:cfbd:game:1', observed_at: T2 }), version: 1 },
  };
  const result = mergePoll(stored, [
    game({ game_key: 'ncaaf:cfbd:game:1' }),   // store is already at T2: rejected
    game({ game_key: 'ncaaf:cfbd:game:9' }),   // not stored at all: applied
  ], { observedAt: T2 });
  assert.deepEqual(result.applied.map((a) => a.game_key), ['ncaaf:cfbd:game:9']);
  assert.deepEqual(result.rejected.map((r) => r.game_key), ['ncaaf:cfbd:game:1']);
  assert.equal(result.rejected[0].outcome, REJECT_STALE);
  assert.ok(result.rejected[0].reason.includes(T2), 'the rejection does not say what was rejected');
});

test('the stored observation time is the one the decision was made on', () => {
  // The poll's time, not the per-game stamp the adapter happened to carry.
  // freshness.mjs reads the stored field, so a disagreement here would show a
  // reader an age that did not justify the write.
  const accepted = reconcile(undefined, game({ observed_at: T1 }), { observedAt: T2 });
  assert.equal(accepted.game.observed_at, T2);
  const stored = { ...game({ observed_at: T1 }), version: 1 };
  const second = reconcile(stored, game({ observed_at: T1 }), { observedAt: T2 });
  assert.equal(second.outcome, ACCEPT);
  assert.equal(second.game.observed_at, T2);
  const corrected = reconcile(
    { ...game({ observed_at: T1, home: { score: 14 } }), version: 1 },
    game({ observed_at: T1, home: { score: 7 } }),
    { observedAt: T2 },
  );
  assert.equal(corrected.outcome, ACCEPT_CORRECTION);
  assert.equal(corrected.game.observed_at, T2);
});

test('with no explicit observedAt the game keeps its own stamp', () => {
  assert.equal(reconcile(undefined, game({ observed_at: T1 })).game.observed_at, T1);
});

test('an empty poll changes nothing and reports every game missing', () => {
  const stored = { 'ncaaf:cfbd:game:1': { ...game(), version: 3 } };
  const result = mergePoll(stored, [], { observedAt: T2 });
  assert.deepEqual(result.games, stored);
  assert.deepEqual(result.missing, ['ncaaf:cfbd:game:1']);
  assert.deepEqual(result.applied, []);
});

test('merging into an empty store accepts everything at version 1', () => {
  const result = mergePoll(undefined, [game()], { observedAt: T1 });
  assert.equal(result.games['ncaaf:cfbd:game:1'].version, 1);
  assert.deepEqual(result.missing, []);
});

// --- finalization events ----------------------------------------------------

test('a finalization event is emitted on the transition into final, once', () => {
  const before = { 'k': game({ status: 'in_progress' }) };
  const after = { 'k': { ...game({ status: 'final' }), version: 4 } };
  const events = finalizationEvents(before, after);
  assert.equal(events.length, 1);
  assert.equal(events[0].event_key, 'k');
  assert.equal(events[0].status, 'final');
  assert.equal(events[0].home_score, 7);
  assert.equal(events[0].version, 4);
});

test('re-polling a game that was ALREADY final emits nothing', () => {
  const before = { 'k': game({ status: 'final' }) };
  const after = { 'k': game({ status: 'final' }) };
  assert.deepEqual(finalizationEvents(before, after), []);
});

test('an already-emitted key is not emitted again even on a fresh transition', () => {
  const before = { 'k': game({ status: 'in_progress' }) };
  const after = { 'k': game({ status: 'final' }) };
  assert.deepEqual(finalizationEvents(before, after, new Set(['k'])), []);
});

test('a live game emits nothing', () => {
  assert.deepEqual(finalizationEvents({}, { 'k': game({ status: 'in_progress' }) }), []);
});

test('a canceled game finalizes too: it is terminal and the pipeline needs it', () => {
  const events = finalizationEvents({ 'k': game({ status: 'in_progress' }) }, { 'k': game({ status: 'canceled' }) });
  assert.equal(events.length, 1);
  assert.equal(events[0].status, 'canceled');
});

test('a game final on its FIRST observation still emits', () => {
  // No `before` entry at all. A game that was already over when we first saw
  // it must reach the pipeline, or a service restart loses the result.
  const events = finalizationEvents({}, { 'k': game({ status: 'final' }) });
  assert.equal(events.length, 1);
});
