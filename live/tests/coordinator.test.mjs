/**
 * The polling loop, driven for real against a fake provider.
 *
 * No network, no clock, no sleeping: `tick` takes the time as an argument and
 * the provider is a function, so every one of these is deterministic. What
 * they are actually protecting is the behaviour the audit says was missing --
 * that a failed poll keeps the last good state, that a finished game reaches
 * the pipeline exactly once, and that the cadence speeds up BEFORE a kickoff
 * rather than after it.
 */
import test from 'node:test';
import assert from 'node:assert/strict';

import {
  DEFAULT_IDLE_INTERVAL_SECONDS,
  DEFAULT_INTERVAL_SECONDS,
  backoffSeconds,
  createCoordinator,
  emptyState,
  nextIntervalSeconds,
  pollObservation,
  tick,
} from '../src/ingestion/coordinator.mjs';

const NOW = new Date('2026-11-08T19:00:00.000Z');
const at = (seconds) => new Date(NOW.getTime() + seconds * 1000);

function game(key, status, overrides = {}) {
  return {
    game_key: key,
    sport: 'ncaaf',
    provider_game_id: Number(key.split(':').pop()),
    status,
    observed_at: NOW.toISOString(),
    start_date: NOW.toISOString(),
    home: { score: 7 },
    away: { score: 3 },
    ...overrides,
  };
}

/**
 * A provider that answers with whatever each call's script says.
 *
 * `stampedAt` makes it behave like the real adapter, which stamps every game
 * in one response with a single observation time taken at fetch time. Without
 * it these fixtures carried a fixed `observed_at`, so a second poll looked like
 * a retried one and was correctly rejected as stale -- which is a property of
 * the fixture, not of the loop.
 */
function fakeProvider(script, stampedAt = null) {
  const calls = [];
  const queue = [...script];
  const stamp = (answer) => (stampedAt && Array.isArray(answer)
    ? answer.map((g) => ({ ...g, observed_at: stampedAt.toISOString() }))
    : answer);
  return {
    calls,
    sport: 'ncaaf',
    provider: 'cfbd',
    async getLiveGames() {
      calls.push(Date.now());
      const answer = queue.length > 1 ? queue.shift() : queue[0];
      if (answer instanceof Error) throw answer;
      if (typeof answer === 'function') return stamp(answer());
      return stamp(answer);
    },
    async getGame() { return null; },
    async getProviderHealth() { return { ok: true, detail: 'fake' }; },
  };
}

// --- the cadence ------------------------------------------------------------

test('the defaults are the roadmap target, not the git-publishing compromise', () => {
  // src/run_live_score_session.py uses 360 seconds, and says why: Pages builds
  // from a branch are soft-limited to about ten an hour. That constraint is
  // what this service removes, so the default here is the 15 seconds the
  // roadmap actually asks for.
  assert.equal(DEFAULT_INTERVAL_SECONDS, 15);
  assert.equal(DEFAULT_IDLE_INTERVAL_SECONDS, 300);
});

test('a live game polls at the fast interval', () => {
  const games = { a: game('a', 'in_progress') };
  assert.equal(nextIntervalSeconds(games, NOW), DEFAULT_INTERVAL_SECONDS);
});

test('halftime is live: the clock is stopped but the reader is still watching', () => {
  assert.equal(nextIntervalSeconds({ a: game('a', 'halftime') }, NOW), DEFAULT_INTERVAL_SECONDS);
});

test('a slate of finished games is idle', () => {
  const games = { a: game('a', 'final'), b: game('b', 'canceled') };
  assert.equal(nextIntervalSeconds(games, NOW), DEFAULT_IDLE_INTERVAL_SECONDS);
});

test('an empty slate is idle: there is nothing to be late for', () => {
  assert.equal(nextIntervalSeconds({}, NOW), DEFAULT_IDLE_INTERVAL_SECONDS);
  assert.equal(nextIntervalSeconds(undefined, NOW), DEFAULT_IDLE_INTERVAL_SECONDS);
});

test('a kickoff inside the idle window pulls the cadence up BEFORE it kicks off', () => {
  // The whole point of anticipating: a game starting in four minutes must not
  // be found five minutes into the first quarter.
  const soon = game('a', 'scheduled', { start_date: at(240).toISOString() });
  assert.equal(nextIntervalSeconds({ a: soon }, NOW), DEFAULT_INTERVAL_SECONDS);
  // Exactly at the horizon counts as imminent.
  const edge = game('a', 'scheduled', { start_date: at(DEFAULT_IDLE_INTERVAL_SECONDS).toISOString() });
  assert.equal(nextIntervalSeconds({ a: edge }, NOW), DEFAULT_INTERVAL_SECONDS);
  // Just beyond it does not.
  const later = game('a', 'scheduled', { start_date: at(DEFAULT_IDLE_INTERVAL_SECONDS + 1).toISOString() });
  assert.equal(nextIntervalSeconds({ a: later }, NOW), DEFAULT_IDLE_INTERVAL_SECONDS);
});

test('a kickoff already in the past keeps the cadence fast', () => {
  // A scheduled game whose start has passed is about to go live, or the
  // provider is behind. Either way this is not the moment to slow down.
  const overdue = game('a', 'scheduled', { start_date: at(-600).toISOString() });
  assert.equal(nextIntervalSeconds({ a: overdue }, NOW), DEFAULT_INTERVAL_SECONDS);
});

test('an unreadable kickoff counts as imminent, not as a reason to slow down', () => {
  // Being wrong this way costs polling at the normal rate. Being wrong the
  // other way is a slate that silently never speeds up.
  for (const start of [undefined, null, '', 'tonight', 0, {}]) {
    const unknown = game('a', 'scheduled', { start_date: start });
    assert.equal(nextIntervalSeconds({ a: unknown }, NOW), DEFAULT_INTERVAL_SECONDS,
      JSON.stringify(start));
  }
});

test('an unreadable game, or an unreadable clock, keeps the cadence fast', () => {
  assert.equal(nextIntervalSeconds({ a: null }, NOW), DEFAULT_INTERVAL_SECONDS);
  assert.equal(nextIntervalSeconds({ a: 'nonsense' }, NOW), DEFAULT_INTERVAL_SECONDS);
  assert.equal(nextIntervalSeconds({ a: game('a', 'final') }, 'whenever'), DEFAULT_INTERVAL_SECONDS);
});

test('one live game among a hundred finished ones still wins', () => {
  const games = {};
  for (let i = 0; i < 100; i += 1) games[`g${i}`] = game(`g${i}`, 'final');
  games.live = game('live', 'in_progress');
  assert.equal(nextIntervalSeconds(games, NOW), DEFAULT_INTERVAL_SECONDS);
});

test('an idle interval no larger than the fast one collapses to the fast one', () => {
  const games = { a: game('a', 'final') };
  assert.equal(nextIntervalSeconds(games, NOW, { intervalSeconds: 30, idleIntervalSeconds: 30 }), 30);
  assert.equal(nextIntervalSeconds(games, NOW, { intervalSeconds: 30, idleIntervalSeconds: 10 }), 30);
});

// --- backoff ----------------------------------------------------------------

test('backoff grows exponentially and is CAPPED AT the idle interval', () => {
  assert.equal(backoffSeconds(0), DEFAULT_INTERVAL_SECONDS);
  assert.equal(backoffSeconds(1), 15);
  assert.equal(backoffSeconds(2), 30);
  assert.equal(backoffSeconds(3), 60);
  assert.equal(backoffSeconds(4), 120);
  assert.equal(backoffSeconds(5), 240);
  // Capped, not unbounded: a provider outage during a live slate must not
  // quietly become a cadence that outlasts the outage.
  assert.equal(backoffSeconds(6), DEFAULT_IDLE_INTERVAL_SECONDS);
  assert.equal(backoffSeconds(50), DEFAULT_IDLE_INTERVAL_SECONDS);
});

test('backoff never returns less than the fast interval', () => {
  assert.equal(backoffSeconds(1, { intervalSeconds: 20, idleIntervalSeconds: 5 }), 20);
  assert.equal(backoffSeconds(-3), DEFAULT_INTERVAL_SECONDS);
});

// --- tick -------------------------------------------------------------------

test('a first poll accepts everything at version 1', async () => {
  const provider = fakeProvider([[game('a', 'in_progress'), game('b', 'scheduled')]]);
  const { state, outcome } = await tick(emptyState(), provider, { now: NOW });
  assert.equal(outcome.ok, true);
  assert.equal(outcome.applied.length, 2);
  assert.equal(state.games.a.version, 1);
  assert.equal(state.polls, 1);
  assert.equal(state.lastAcceptedAt, NOW.toISOString());
});

test('A FAILED POLL KEEPS THE LAST GOOD STATE, untouched', async () => {
  const first = await tick(emptyState(), fakeProvider([[game('a', 'in_progress')]]), { now: NOW });
  const error = Object.assign(new Error('CFBD /scoreboard returned 503'), { retryable: true });
  const second = await tick(first.state, fakeProvider([error]), { now: at(15) });

  assert.equal(second.outcome.ok, false);
  assert.equal(second.outcome.retryable, true);
  assert.match(second.outcome.reason, /503/);
  // The games survive, at their existing version.
  assert.deepEqual(second.state.games, first.state.games);
  assert.equal(second.state.games.a.version, 1);
  // And the accepted-at time does NOT advance: a failed poll learned nothing,
  // and advancing it would report a freshness that had not been earned.
  assert.equal(second.state.lastAcceptedAt, NOW.toISOString());
  assert.equal(second.state.failures, 1);
  assert.equal(second.state.consecutiveFailures, 1);
  assert.equal(second.state.polls, 2);
});

test('a non-retryable error is not marked retryable', async () => {
  // Retrying a deterministic failure is a busy loop. Only the provider says
  // which kind it was; an error without the flag is not assumed retryable.
  const plain = await tick(emptyState(), fakeProvider([new Error('cannot read property of undefined')]), { now: NOW });
  assert.equal(plain.outcome.retryable, false);
  const flagged = await tick(emptyState(), fakeProvider([Object.assign(new Error('429'), { retryable: true })]), { now: NOW });
  assert.equal(flagged.outcome.retryable, true);
});

test('a non-list response is a failure, not an empty slate', async () => {
  // Treating it as an empty slate would wipe every game on the board.
  const first = await tick(emptyState(), fakeProvider([[game('a', 'in_progress')]]), { now: NOW });
  const second = await tick(first.state, fakeProvider([{ games: [] }]), { now: at(15) });
  assert.equal(second.outcome.ok, false);
  assert.equal(second.outcome.retryable, false);
  assert.deepEqual(second.state.games, first.state.games);
});

test('consecutive failures reset on the first success', async () => {
  let state = emptyState();
  for (let i = 0; i < 3; i += 1) {
    ({ state } = await tick(state, fakeProvider([new Error('down')]), { now: at(i * 15) }));
  }
  assert.equal(state.consecutiveFailures, 3);
  ({ state } = await tick(state, fakeProvider([[game('a', 'in_progress')]]), { now: at(60) }));
  assert.equal(state.consecutiveFailures, 0);
  assert.equal(state.failures, 3, 'the running total was reset along with the streak');
});

test('a poll whose every write is rejected does not advance the accepted time', async () => {
  const first = await tick(emptyState(), fakeProvider([[game('a', 'in_progress')]]), { now: NOW });
  // Same observation time: mergePoll rejects it as not newer.
  const second = await tick(first.state, fakeProvider([[game('a', 'in_progress')]]), { now: NOW });
  assert.equal(second.outcome.ok, true);
  assert.equal(second.outcome.applied.length, 0);
  assert.equal(second.outcome.rejected.length, 1);
  assert.equal(second.state.lastAcceptedAt, NOW.toISOString());
});

test('a NEWER poll rejected on its merits still does not advance the accepted time', async () => {
  // The case above cannot tell the two behaviours apart, because the rejected
  // poll carries the same observation the state already holds -- stamping it
  // anyway would be a no-op. Here the poll is genuinely fifteen seconds newer
  // and is refused on the provider-outage rule, so advancing the accepted time
  // would claim we had learned something at 19:00:15 when we had not.
  const first = await tick(emptyState(), fakeProvider([[game('a', 'final')]], NOW), { now: NOW });
  assert.equal(first.state.lastAcceptedAt, NOW.toISOString());
  const second = await tick(
    first.state,
    fakeProvider([[game('a', 'in_progress')]], at(15)),
    { now: at(15) },
  );
  assert.equal(second.outcome.ok, true);
  assert.deepEqual(second.outcome.applied, []);
  assert.equal(second.outcome.rejected.length, 1);
  assert.match(second.outcome.rejected[0].outcome, /reverts_final/);
  assert.equal(second.state.lastAcceptedAt, NOW.toISOString(),
    'a poll that learned nothing moved the clock on what we know');
  assert.equal(second.state.games.a.status, 'final');
});

test('a game missing from a poll is reported, not dropped', async () => {
  const first = await tick(emptyState(), fakeProvider([[game('a', 'final'), game('b', 'in_progress')]], NOW), { now: NOW });
  const second = await tick(first.state, fakeProvider([[game('b', 'in_progress')]], at(15)), { now: at(15) });
  assert.deepEqual(second.outcome.missing, ['a']);
  assert.ok(second.state.games.a, 'the absent game was dropped');
});

// --- finalization events ----------------------------------------------------

test('a game finishing emits exactly once, however many times it is re-polled', async () => {
  let state = emptyState();
  ({ state } = await tick(state, fakeProvider([[game('a', 'in_progress')]], NOW), { now: NOW }));
  const finishing = await tick(state, fakeProvider([[game('a', 'final')]], at(15)), { now: at(15) });
  assert.equal(finishing.outcome.events.length, 1);
  assert.equal(finishing.outcome.events[0].event_key, 'a');
  assert.ok(finishing.state.emitted.has('a'));

  // Three more polls of the same finished game.
  state = finishing.state;
  for (let i = 2; i <= 4; i += 1) {
    const again = await tick(state, fakeProvider([[game('a', 'final')]], at(15 * i)), { now: at(15 * i) });
    assert.deepEqual(again.outcome.events, [], `re-emitted on poll ${i}`);
    state = again.state;
  }
});

// --- createCoordinator ------------------------------------------------------

test('the coordinator refuses to exist without a provider', () => {
  assert.throws(() => createCoordinator(), TypeError);
  assert.throws(() => createCoordinator({ provider: {} }), /getLiveGames/);
});

test('a reader never causes a provider request: polling is the only caller', async () => {
  // The architectural claim, enforced by construction. The coordinator holds
  // state; nothing on the read path calls the provider.
  const provider = fakeProvider([[game('a', 'in_progress')]]);
  const coordinator = createCoordinator({ provider, now: () => NOW });
  await coordinator.poll();
  assert.equal(provider.calls.length, 1);
  for (let i = 0; i < 20; i += 1) {
    coordinator.health();
    coordinator.nextDelaySeconds();
    void coordinator.state.games;
  }
  assert.equal(provider.calls.length, 1, 'reading the state called the provider');
});

test('onPublish is called when something changed, and not when nothing did', async () => {
  let clock = NOW;
  const published = [];
  const provider = fakeProvider([() => [game('a', 'in_progress', { observed_at: clock.toISOString() })]]);
  const coordinator = createCoordinator({
    provider,
    now: () => clock,
    onPublish: (state, outcome) => { published.push(outcome.applied.length); },
  });
  await coordinator.poll();
  assert.deepEqual(published, [1]);
  // Same clock, so the write is rejected as not newer and nothing is published.
  await coordinator.poll();
  assert.deepEqual(published, [1], 'published a poll that changed nothing');
  clock = at(15);
  await coordinator.poll();
  assert.deepEqual(published, [1, 1]);
});

test('a failed poll does not publish, does not emit, and SAYS SO', async () => {
  // Two separate guards stop the publish -- the failure branch returns early,
  // and the publish is conditional on an applied write, which a failure has
  // none of. The log line is what distinguishes them: it is only reached on
  // the failure branch, so asserting it pins that branch rather than letting
  // the second guard silently stand in for it.
  const published = [];
  const emitted = [];
  const logs = [];
  const coordinator = createCoordinator({
    provider: fakeProvider([new Error('down')]),
    now: () => NOW,
    onPublish: () => { published.push(1); },
    onEvents: (events) => { emitted.push(events); },
    onLog: (entry) => { logs.push(entry); },
  });
  const outcome = await coordinator.poll();
  assert.equal(outcome.ok, false);
  assert.deepEqual(published, []);
  assert.deepEqual(emitted, []);
  assert.equal(logs.length, 1);
  assert.equal(logs[0].level, 'warn');
  assert.match(logs[0].message, /keeping the last good state/);
});

test('onEvents THROWING does not lose the events: they are offered again', async () => {
  // Live ingestion must not touch Elo or Merits directly, so this handler is
  // the only seam to the existing pipeline. Losing an event here would
  // silently lose a game result.
  let clock = NOW;
  let fail = true;
  const seen = [];
  const coordinator = createCoordinator({
    provider: fakeProvider([() => [game('a', 'final', { observed_at: clock.toISOString() })]]),
    now: () => clock,
    onEvents: (events) => {
      if (fail) throw new Error('the pipeline is down');
      seen.push(...events.map((e) => e.event_key));
    },
  });

  await assert.rejects(() => coordinator.poll(), /pipeline is down/);
  assert.deepEqual(seen, []);
  // The event is OWED, not forgotten: generated, so it cannot be produced a
  // second time, and queued, so it is retried.
  assert.deepEqual(coordinator.state.pending.map((e) => e.event_key), ['a']);
  assert.equal(coordinator.state.emitted.has('a'), true);

  fail = false;
  clock = at(15);
  await coordinator.poll();
  assert.deepEqual(seen, ['a'], 'the event was lost when the handler first failed');
  assert.deepEqual(coordinator.state.pending, [], 'the queue was not cleared on delivery');

  // And now it does not come a second time.
  clock = at(30);
  await coordinator.poll();
  assert.deepEqual(seen, ['a']);
});

test('an owed event is retried even once the provider stops reporting the game', async () => {
  // This is the case that makes the queue necessary rather than tidy. A
  // finished game leaves the scoreboard: holding the event inside the game's
  // own stored row means a handler that failed once can never be offered the
  // event again, because nothing will regenerate it.
  let clock = NOW;
  let fail = true;
  const seen = [];
  const coordinator = createCoordinator({
    provider: fakeProvider([
      () => [game('a', 'in_progress', { observed_at: clock.toISOString() })],
      () => [game('a', 'final', { observed_at: clock.toISOString() })],
      // The slate is over. The game is gone from the provider for good.
      () => [],
      () => [],
    ]),
    now: () => clock,
    onEvents: (events) => {
      if (fail) throw new Error('the pipeline is down');
      seen.push(...events.map((e) => e.event_key));
    },
  });

  await coordinator.poll();
  clock = at(15);
  await assert.rejects(() => coordinator.poll(), /pipeline is down/);
  assert.deepEqual(seen, []);

  fail = false;
  clock = at(30);
  await coordinator.poll();
  assert.deepEqual(seen, ['a'], 'the result was lost with the game that produced it');

  clock = at(45);
  await coordinator.poll();
  assert.deepEqual(seen, ['a'], 'delivered twice');
});

test('a downstream outage does not stop scores reaching readers', async () => {
  // The other half of holding the state back: an unrelated live game in the
  // same poll was withheld from every reader because a DIFFERENT game's
  // finalization could not be handed over.
  let clock = NOW;
  const published = [];
  const coordinator = createCoordinator({
    provider: fakeProvider([
      () => [
        game('a', 'in_progress', { observed_at: clock.toISOString() }),
        game('b', 'in_progress', { observed_at: clock.toISOString() }),
      ],
      () => [
        game('a', 'final', { observed_at: clock.toISOString() }),
        game('b', 'in_progress', { observed_at: clock.toISOString(), home: { score: 21 } }),
      ],
    ]),
    now: () => clock,
    onEvents: () => { throw new Error('the pipeline is down'); },
    onPublish: (state) => { published.push(state.games.b.home.score); },
  });

  await coordinator.poll();
  clock = at(15);
  await assert.rejects(() => coordinator.poll(), /pipeline is down/);
  assert.equal(coordinator.state.games.b.home.score, 21, 'the unrelated score was withheld');
  assert.equal(coordinator.state.games.a.status, 'final');
  assert.deepEqual(published, [7, 21], 'readers did not see the poll that the pipeline refused');
});

test('overlapping polls are serialized, not raced', async () => {
  // Both polls would otherwise reconcile against the same pre-poll state, so
  // the slower response could not be compared against what the faster one
  // installed -- an older in-progress row would reinstate a game the newer
  // response had already finalized.
  let release;
  const gate = new Promise((resolve) => { release = resolve; });
  let call = 0;
  const provider = {
    async getLiveGames() {
      call += 1;
      if (call === 1) {
        // The newer, final response, held open until the second poll is away.
        await gate;
        return [game('a', 'final', { observed_at: at(15).toISOString() })];
      }
      return [game('a', 'in_progress', { observed_at: NOW.toISOString() })];
    },
  };
  const coordinator = createCoordinator({ provider, now: () => at(30) });

  const first = coordinator.poll();
  const second = coordinator.poll();
  release();
  await Promise.all([first, second]);

  assert.equal(call, 2);
  assert.equal(coordinator.state.polls, 2, 'a poll was recorded over the top of another');
  assert.equal(coordinator.state.games.a.status, 'final',
    'the older response reinstated a game that had already finalized');
});

test('a failed poll does not poison the polls queued behind it', async () => {
  let clock = NOW;
  const coordinator = createCoordinator({
    provider: fakeProvider([
      new Error('down'),
      () => [game('a', 'in_progress', { observed_at: clock.toISOString() })],
    ]),
    now: () => clock,
  });
  const first = coordinator.poll();
  clock = at(15);
  const second = coordinator.poll();
  assert.equal((await first).ok, false);
  assert.equal((await second).ok, true, 'the chain stayed rejected after one failure');
});

test('the delay backs off after a failure and recovers after a success', async () => {
  let clock = NOW;
  let answer = new Error('down');
  const coordinator = createCoordinator({
    provider: { async getLiveGames() { if (answer instanceof Error) throw answer; return answer; } },
    now: () => clock,
  });
  await coordinator.poll();
  assert.equal(coordinator.nextDelaySeconds(), 15);
  await coordinator.poll();
  assert.equal(coordinator.nextDelaySeconds(), 30);
  await coordinator.poll();
  assert.equal(coordinator.nextDelaySeconds(), 60);

  answer = [game('a', 'final', { observed_at: clock.toISOString() })];
  await coordinator.poll();
  // Back to the state-derived cadence: everything is finished, so idle.
  assert.equal(coordinator.nextDelaySeconds(), DEFAULT_IDLE_INTERVAL_SECONDS);
});

test('health reports the oldest live game plus the loop\'s own counters', async () => {
  let clock = NOW;
  const provider = fakeProvider([() => [
    game('fresh', 'in_progress', { observed_at: clock.toISOString() }),
    game('frozen', 'in_progress', { observed_at: at(-1800).toISOString() }),
  ]]);
  const coordinator = createCoordinator({ provider, now: () => clock });
  await coordinator.poll();
  const report = coordinator.health(clock);

  // mergePoll stamps the poll's observation onto every accepted game, so both
  // carry the poll time -- which is correct: that IS when we observed them.
  assert.equal(report.games_live, 2);
  assert.equal(report.polls, 1);
  assert.equal(report.failures, 0);
  assert.equal(report.consecutive_failures, 0);
  assert.equal(report.last_accepted_at, NOW.toISOString());
  assert.equal(report.basis, 'oldest live game');
});

test('health reports a failure streak while still naming what it holds', async () => {
  let clock = NOW;
  let answer = [game('a', 'in_progress', { observed_at: NOW.toISOString() })];
  const coordinator = createCoordinator({
    provider: { async getLiveGames() { if (answer instanceof Error) throw answer; return answer; } },
    now: () => clock,
  });
  await coordinator.poll();
  answer = new Error('down');
  clock = at(600);
  await coordinator.poll();

  const report = coordinator.health(clock);
  assert.equal(report.consecutive_failures, 1);
  assert.equal(report.games_tracked, 1);
  // Ten minutes since the observation it still holds, so the slate is stale --
  // which is the honest answer and the one a reader must be told.
  assert.equal(report.level, 'stale');
  assert.equal(report.age_seconds, 600);
});

// --- which observation time a poll is recorded under ------------------------

test('a poll is recorded under the response\'s own observation, carried verbatim', async () => {
  // The adapter stamps every game in one response with one time, and that is
  // the thing that actually looked at the wire. Re-deriving it from the
  // coordinator's clock threw away precision the response carried: Python
  // stamps microseconds and a JS Date holds milliseconds.
  const precise = '2026-11-08T07:15:11.395792Z';
  const games = [game('a', 'in_progress', { observed_at: precise })];
  const { state } = await tick(emptyState(), fakeProvider([games]), { now: NOW });
  assert.equal(state.games.a.observed_at, precise);
  assert.equal(state.lastAcceptedAt, precise);
  assert.ok(state.games.a.observed_at.endsWith('395792Z'));
});

test('pollObservation takes the shared stamp, and falls back to the clock otherwise', () => {
  const clock = NOW.toISOString();
  const one = '2026-11-08T07:15:11.395792Z';
  const other = '2026-11-08T07:15:12.000000Z';
  // Agreed and parseable: used verbatim.
  assert.equal(pollObservation([{ observed_at: one }, { observed_at: one }], clock), one);
  // Disagreement means there is no single poll observation to speak of, and
  // picking one would be a guess about which game we really saw.
  assert.equal(pollObservation([{ observed_at: one }, { observed_at: other }], clock), clock);
  // Unusable stamps fall back rather than propagating.
  for (const bad of [undefined, null, '', 'just now', 0, {}]) {
    assert.equal(pollObservation([{ observed_at: bad }], clock), clock, JSON.stringify(bad));
  }
  // An empty or non-list response has no observation of its own.
  assert.equal(pollObservation([], clock), clock);
  assert.equal(pollObservation(undefined, clock), clock);
});

test('a response carrying no stamp is still recorded, under our own clock', async () => {
  const bare = [{ game_key: 'a', status: 'in_progress', provider_game_id: 1, home: {}, away: {} }];
  const { state, outcome } = await tick(emptyState(), fakeProvider([bare]), { now: NOW });
  assert.equal(outcome.ok, true);
  assert.equal(state.games.a.observed_at, NOW.toISOString());
});
