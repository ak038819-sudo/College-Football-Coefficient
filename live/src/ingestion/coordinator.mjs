/**
 * The polling loop: one ingestion process, many readers.
 *
 * This is the thing that removes the constraint the audit found. The published
 * site polls a file that a GitHub Actions cron rewrites, and that cron was
 * delivered 50.8% of the time, so 70 of 163 completed games (43%) had their
 * last live update more than fifteen minutes before the final appeared. The
 * fix is not a faster cron. It is a process that holds state, polls on its own
 * schedule, and serves every reader from one provider request.
 *
 * WHAT THIS MODULE IS NOT. It does not fetch, sleep, or know the time. The
 * provider, the clock and the sleep are injected, so `tick()` is a pure
 * function of (state, provider response) and the whole loop is testable without
 * a network or a wall clock. It also does not publish: `onPublish` is handed
 * the new state and decides what to do with it, which is what keeps the
 * Workers runtime, a Node process and a test from needing three copies of this
 * logic.
 *
 * THE CADENCE IS CARRIED OVER FROM `src/run_live_score_session.py`, deliberately
 * and with its reasoning intact, because that function is the one part of the
 * current publisher that is right: fast while a game is live, fast again once a
 * kickoff is close enough that an idle wait would swallow it, and anything
 * unreadable treated as a reason to poll at the normal rate rather than to slow
 * down on the strength of a response it could not understand.
 *
 * ONE THING IS DELIBERATELY DIFFERENT. The Python's fast interval is 360
 * seconds, and its comment says why: "Pages builds from a branch are
 * soft-limited to roughly ten an hour, so a shorter interval would spend
 * publishes it cannot deliver." That is a constraint of publishing through git,
 * and it is the constraint this service exists to remove. The default here is
 * 15 seconds, the roadmap's target, because nothing downstream of `onPublish`
 * is a commit any more. The provider budget is what bounds it now, which is
 * why the box-score batch had to come off this cycle first.
 */
import { mergePoll, finalizationEvents } from './validate.mjs';
import { health } from '../state/freshness.mjs';

/** Seconds between polls while a game is live. */
export const DEFAULT_INTERVAL_SECONDS = 15;
/** Seconds between polls when nothing is live and no kickoff is close. */
export const DEFAULT_IDLE_INTERVAL_SECONDS = 300;
/** Statuses that mean a reader would see a difference if we polled now. */
const LIVE_STATUSES = ['in_progress', 'halftime'];

/**
 * How long to wait before the next poll.
 *
 * `games` is the state's game map, not a provider response: the decision is
 * made from what we believe, so a failed poll does not also lose the cadence.
 */
export function nextIntervalSeconds(games, now, {
  intervalSeconds = DEFAULT_INTERVAL_SECONDS,
  idleIntervalSeconds = DEFAULT_IDLE_INTERVAL_SECONDS,
} = {}) {
  const at = now instanceof Date ? now.getTime() : Date.parse(now);
  // An unreadable clock is not a reason to slow down.
  if (Number.isNaN(at)) return intervalSeconds;
  if (idleIntervalSeconds <= intervalSeconds) return intervalSeconds;
  const entries = Object.values(games ?? {});
  // An empty slate is idle. There is nothing to be late for.
  //
  // This guard is deliberately REDUNDANT: an empty map falls through the loop
  // below and returns the same value, and a mutation run confirmed that
  // deleting it changes no behaviour. It stays because "no games means idle" is
  // the decision, and a reader should not have to derive it from the absence of
  // loop iterations.
  if (!entries.length) return idleIntervalSeconds;
  const horizon = at + idleIntervalSeconds * 1000;
  for (const game of entries) {
    // Anything unreadable is treated the way an unreadable kickoff is.
    if (!game || typeof game !== 'object') return intervalSeconds;
    if (LIVE_STATUSES.includes(game.status)) return intervalSeconds;
    if (game.status !== 'scheduled') continue;
    // A kickoff that is absent, blank or not a timestamp all land here: a
    // scheduled game we cannot place in time counts as imminent. Being wrong
    // that way costs polling at the normal rate; being wrong the other way is
    // a slate that silently never speeds up.
    const kickoff = Date.parse(game.start_date);
    if (Number.isNaN(kickoff) || kickoff <= horizon) return intervalSeconds;
  }
  return idleIntervalSeconds;
}

/** The state a coordinator carries between polls. */
export function emptyState() {
  return {
    games: {},
    // Finalization events already handed downstream, so re-polling a finished
    // game does not re-emit. A Set, not a count: the keys are the identity.
    emitted: new Set(),
    // The observation time of the last poll we ACCEPTED anything from. Not the
    // last poll attempted -- a failed poll must not advance it, or freshness
    // would report a staleness that had not been earned.
    lastAcceptedAt: null,
    polls: 0,
    failures: 0,
    consecutiveFailures: 0,
  };
}

/**
 * The one observation time this poll is recorded under.
 *
 * The adapter already stamps every game in a response with a single shared
 * observation time -- that is documented in `../providers/cfbd.mjs` and it is
 * the thing that actually looked at the wire. So when every game agrees on one
 * parseable stamp, that stamp is used VERBATIM and the coordinator's own clock
 * is only the fallback.
 *
 * WHY THIS IS NOT JUST TIDINESS. Re-deriving the time from `new Date()` and
 * calling `toISOString()` throws away precision the response may carry: the
 * Python publisher stamps `datetime.isoformat()`, which has MICROSECONDS, and
 * a JavaScript `Date` holds only milliseconds, so `...395792Z` came back out as
 * `...395Z`. The differential test against a real published snapshot is what
 * caught it. Carrying the string through loses nothing and claims nothing.
 *
 * A response whose games DISAGREE, or carry no usable stamp, falls back to the
 * clock. Disagreement means there is no single poll observation to speak of,
 * and picking one of them would be a guess about which game we really saw.
 */
export function pollObservation(games, clockStamp) {
  if (!Array.isArray(games) || !games.length) return clockStamp;
  const first = games[0]?.observed_at;
  if (typeof first !== 'string' || Number.isNaN(Date.parse(first))) return clockStamp;
  for (const game of games) {
    if (game?.observed_at !== first) return clockStamp;
  }
  return first;
}

/**
 * One poll, applied to state. Returns `{state, outcome}`.
 *
 * A FAILED POLL KEEPS THE LAST GOOD STATE and says so. That is the first rule
 * of the current publisher -- "a failed request leaves the last good snapshot
 * untouched" -- and it is worth more here, not less: this process holds the
 * only copy. The outcome names the failure and whether it is worth retrying,
 * and the caller decides; it is not this function's job to sleep.
 */
export async function tick(state, provider, { now }) {
  const at = now instanceof Date ? now : new Date(now);
  const clockStamp = at.toISOString();
  const next = { ...state, polls: state.polls + 1 };

  let games;
  try {
    games = await provider.getLiveGames();
  } catch (error) {
    return {
      state: {
        ...next,
        failures: state.failures + 1,
        consecutiveFailures: state.consecutiveFailures + 1,
      },
      outcome: {
        ok: false,
        // A provider error carries `retryable`; anything else -- a bug in
        // normalization, a malformed response -- is NOT assumed retryable,
        // because retrying a deterministic failure is a busy loop.
        retryable: error?.retryable === true,
        reason: String(error?.message ?? error),
        applied: [],
        rejected: [],
        events: [],
      },
    };
  }

  if (!Array.isArray(games)) {
    return {
      state: { ...next, failures: state.failures + 1, consecutiveFailures: state.consecutiveFailures + 1 },
      outcome: { ok: false, retryable: false, reason: 'provider returned a non-list', applied: [], rejected: [], events: [] },
    };
  }

  const observedAt = pollObservation(games, clockStamp);
  const before = state.games;
  const { games: merged, applied, rejected, missing } = mergePoll(before, games, { observedAt });
  const events = finalizationEvents(before, merged, state.emitted);
  const emitted = new Set(state.emitted);
  for (const event of events) emitted.add(event.event_key);

  return {
    state: {
      ...next,
      games: merged,
      emitted,
      // Only an accepted write advances this. A poll that was entirely
      // rejected as stale learned nothing.
      lastAcceptedAt: applied.length ? observedAt : state.lastAcceptedAt,
      consecutiveFailures: 0,
    },
    outcome: { ok: true, retryable: false, reason: null, applied, rejected, missing, events },
  };
}

/**
 * Backoff after a failed poll.
 *
 * Capped, and capped BELOW the idle interval rather than above it: a provider
 * outage during a live slate must not quietly turn into a twenty-minute
 * cadence that outlasts the outage. Exponential from the interval, so a brief
 * reset is ridden out without hammering.
 */
export function backoffSeconds(consecutiveFailures, {
  intervalSeconds = DEFAULT_INTERVAL_SECONDS,
  idleIntervalSeconds = DEFAULT_IDLE_INTERVAL_SECONDS,
} = {}) {
  if (consecutiveFailures <= 0) return intervalSeconds;
  const grown = intervalSeconds * 2 ** Math.min(consecutiveFailures - 1, 10);
  return Math.min(grown, Math.max(intervalSeconds, idleIntervalSeconds));
}

/**
 * Build a coordinator.
 *
 * `onPublish(state, outcome)` is called after every poll that changed
 * something, and `onEvents(events)` with finalization events exactly once per
 * game. Both may be async; both are awaited, because a publish that is still
 * in flight when the next poll lands is how two writers race.
 *
 * `onEvents` THROWING DOES NOT DROP THE EVENTS. They are only recorded as
 * emitted once the handler has returned, so a downstream failure means they are
 * offered again on the next poll rather than lost. Live ingestion must not
 * touch Elo or Merits directly, so this handler is the only seam to the
 * existing pipeline and losing an event there would silently lose a result.
 */
export function createCoordinator({
  provider,
  now = () => new Date(),
  intervalSeconds = DEFAULT_INTERVAL_SECONDS,
  idleIntervalSeconds = DEFAULT_IDLE_INTERVAL_SECONDS,
  onPublish = null,
  onEvents = null,
  onLog = null,
  state = emptyState(),
} = {}) {
  if (!provider || typeof provider.getLiveGames !== 'function') {
    throw new TypeError('a provider with getLiveGames is required');
  }
  const options = { intervalSeconds, idleIntervalSeconds };
  let current = state;

  return {
    get state() { return current; },

    /** The health payload, from the state we hold. */
    health(at = now()) {
      const report = health(current.games, at, undefined);
      return {
        ...report,
        polls: current.polls,
        failures: current.failures,
        consecutive_failures: current.consecutiveFailures,
        last_accepted_at: current.lastAcceptedAt,
      };
    },

    /** Seconds to wait before polling again, given what we now believe. */
    nextDelaySeconds(at = now()) {
      if (current.consecutiveFailures > 0) {
        return backoffSeconds(current.consecutiveFailures, options);
      }
      return nextIntervalSeconds(current.games, at, options);
    },

    /** Poll once, publish, emit. Returns the outcome. */
    async poll() {
      const at = now();
      const { state: nextState, outcome } = await tick(current, provider, { now: at });

      if (!outcome.ok) {
        // State still advances: the failure counters are the point, and the
        // games are carried through untouched by `tick`.
        current = nextState;
        onLog?.({ level: 'warn', message: 'poll failed, keeping the last good state', ...outcome });
        return outcome;
      }

      // Events first, and the state recorded as emitted only after the handler
      // returns. If it throws, `current` keeps the older `emitted` set and the
      // same events are offered again next poll.
      if (outcome.events.length && onEvents) {
        await onEvents(outcome.events);
      }
      current = nextState;

      if (onPublish && (outcome.applied.length || outcome.events.length)) {
        await onPublish(current, outcome);
      }
      if (outcome.rejected.length) {
        onLog?.({ level: 'info', message: 'rejected writes', rejected: outcome.rejected });
      }
      return outcome;
    },
  };
}
