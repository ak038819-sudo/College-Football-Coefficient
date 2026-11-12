/**
 * State transition rules. The proposal's §6, made enforceable.
 *
 * Four hazards it names, each of which this module refuses:
 *
 *   "A temporary missing game shouldn't mean it disappeared."
 *   "A score correction shouldn't be rejected simply because a score decreased."
 *   "A provider outage shouldn't cause a previously final game to revert to live."
 *   "We should never let two overlapping polls overwrite newer state with older
 *    responses."
 *
 * The last one is why every accepted write carries a version and why a write
 * built from an older observation is rejected outright. Versioned writes are
 * the only defence against a retry landing behind a newer poll.
 */
import { TERMINAL_STATUSES } from '../providers/provider.mjs';

/** Outcomes, so a caller can branch without matching on strings. */
export const ACCEPT = 'accept';
export const REJECT_STALE = 'reject_stale_observation';
export const REJECT_REVERTS_FINAL = 'reject_reverts_final';
export const ACCEPT_CORRECTION = 'accept_correction';

function parseInstant(value) {
  if (typeof value !== 'string' || !value) return null;
  const ms = Date.parse(value);
  return Number.isNaN(ms) ? null : ms;
}

/**
 * Decide what to do with an incoming game against the one already stored.
 *
 * Returns `{outcome, reason, version, game}`. The caller writes `game` at
 * `version` or does nothing.
 *
 * AN EXPLICIT `observedAt` IS STAMPED ONTO WHAT IS STORED, not just used for
 * the comparison. It decides whether the write is accepted, and the stored
 * `observed_at` is what `../state/freshness.mjs` later reads to tell a reader
 * how old the score is. Leaving the game's own, older stamp in place while
 * accepting it on the strength of a newer one would make those two numbers
 * disagree, and the one the reader sees would be the one that did not justify
 * the write. One observation time per decision, and it is the same number in
 * both places.
 */
export function reconcile(stored, incoming, { observedAt } = {}) {
  if (!incoming || typeof incoming !== 'object') {
    throw new TypeError('incoming game is required');
  }
  const basis = observedAt ?? incoming.observed_at;
  const seen = parseInstant(basis);
  if (seen === null) throw new TypeError('incoming game has no parseable observed_at');
  const observed = { ...incoming, observed_at: basis };

  if (!stored) {
    return { outcome: ACCEPT, reason: 'first observation', version: 1, game: { ...observed, version: 1 } };
  }

  const storedSeen = parseInstant(stored.observed_at);
  // An observation no newer than the stored one is an overlapping or retried
  // poll. Equal counts as stale: two polls in the same millisecond carry no
  // information about which is current, so the stored one stands.
  if (storedSeen !== null && seen <= storedSeen) {
    return { outcome: REJECT_STALE, reason: `observation ${basis} is not newer than stored ${stored.observed_at}`, version: stored.version, game: stored };
  }

  // A final game does not go back to live. This is the provider-outage rule:
  // a feed that drops a game's detail and re-reports it as in_progress must not
  // un-finish it. A correction to a FINAL SCORE is still allowed below, because
  // that arrives as another final.
  if (TERMINAL_STATUSES.includes(stored.status) && !TERMINAL_STATUSES.includes(incoming.status)) {
    return { outcome: REJECT_REVERTS_FINAL, reason: `stored status ${stored.status} cannot revert to ${incoming.status}`, version: stored.version, game: stored };
  }

  const version = (Number.isInteger(stored.version) ? stored.version : 0) + 1;

  // A score moving DOWN is a correction, not an error. Officials reverse
  // touchdowns. It is accepted and labelled so the change is auditable, rather
  // than being filtered out by a "scores only increase" rule that would pin a
  // wrong number on the page forever.
  const decreased = ['home', 'away'].some((side) => {
    const before = stored[side]?.score;
    const after = incoming[side]?.score;
    return Number.isInteger(before) && Number.isInteger(after) && after < before;
  });

  return {
    outcome: decreased ? ACCEPT_CORRECTION : ACCEPT,
    reason: decreased ? 'score decreased: treated as an official correction' : 'newer observation',
    version,
    game: { ...observed, version },
  };
}

/**
 * Merge a poll into the store.
 *
 * A game present in the store but ABSENT from this poll is KEPT, untouched and
 * at its existing version. That is the proposal's first hazard: provider feeds
 * drop rows, and a game vanishing from the scoreboard because one response was
 * short is a visible defect, while a game lingering a few minutes too long is
 * not. Freshness marking, not deletion, is how a reader learns the difference
 * -- see `../state/freshness.mjs`.
 *
 * Returns `{games, applied, rejected, missing}`.
 */
export function mergePoll(storedGames, incomingGames, { observedAt } = {}) {
  const next = new Map(Object.entries(storedGames ?? {}));
  const applied = [];
  const rejected = [];
  const seenKeys = new Set();

  for (const incoming of incomingGames) {
    seenKeys.add(incoming.game_key);
    const result = reconcile(next.get(incoming.game_key), incoming, { observedAt });
    if (result.outcome === ACCEPT || result.outcome === ACCEPT_CORRECTION) {
      next.set(incoming.game_key, result.game);
      applied.push({ game_key: incoming.game_key, outcome: result.outcome, version: result.version });
    } else {
      rejected.push({ game_key: incoming.game_key, outcome: result.outcome, reason: result.reason });
    }
  }

  const missing = [...next.keys()].filter((key) => !seenKeys.has(key));
  return { games: Object.fromEntries(next), applied, rejected, missing };
}

/**
 * Finalization events for the existing analytics pipeline.
 *
 * Idempotent and keyed by sport plus game id, as the proposal requires, and
 * emitted only on the transition INTO a terminal status -- so re-polling a
 * finished game does not re-emit. `alreadyEmitted` is the set of keys the
 * caller has already handed downstream.
 *
 * Live ingestion must not touch Elo or Merits directly; this produces an event
 * the existing pipeline imports and verifies.
 */
export function finalizationEvents(before, after, alreadyEmitted = new Set()) {
  const events = [];
  for (const [key, game] of Object.entries(after ?? {})) {
    if (!TERMINAL_STATUSES.includes(game.status)) continue;
    if (alreadyEmitted.has(key)) continue;
    const previous = (before ?? {})[key];
    if (previous && TERMINAL_STATUSES.includes(previous.status)) continue;
    events.push({
      event_key: key,
      sport: game.sport,
      provider_game_id: game.provider_game_id,
      status: game.status,
      home_score: game.home?.score ?? null,
      away_score: game.away?.score ?? null,
      observed_at: game.observed_at,
      version: game.version,
    });
  }
  return events;
}
