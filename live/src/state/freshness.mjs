/**
 * How stale is this, and what should a reader be told?
 *
 * The proposal's §7 thresholds: fresh under 30s, delayed 30-120s, stale beyond
 * 120s, configurable.
 *
 * ONE MEASURED CAUTION, carried from docs/phase1-live-architecture-audit.md.
 * On the CURRENT architecture the publish leg alone measured 74-112 seconds
 * (push to first visible on Pages), which means these thresholds would put the
 * production site in "delayed" or "stale" essentially always. On the new
 * architecture that leg disappears, which is the point -- but the thresholds
 * should be re-checked against the shadow service's real numbers before they
 * ship, or "delayed" becomes the normal state on day one and the indicator
 * stops meaning anything.
 *
 * The rule that is not negotiable, from §7: never quietly present an old score
 * as live. Every level below is a thing the page must SAY, not a thing it may
 * hide.
 */

export const FRESH = 'fresh';
export const DELAYED = 'delayed';
export const STALE = 'stale';
export const UNKNOWN = 'unknown';

export const DEFAULT_THRESHOLDS = Object.freeze({ freshUnderSeconds: 30, delayedUnderSeconds: 120 });

/**
 * Classify a snapshot's age.
 *
 * `observedAt` missing or unparseable returns UNKNOWN, never FRESH. A snapshot
 * that cannot say when it was taken is not a fresh snapshot, and defaulting the
 * other way is how a blank timestamp renders as live.
 *
 * A NEGATIVE age -- an observation stamped in the future -- also returns
 * UNKNOWN rather than being clamped to fresh. Clock skew and a provider
 * stamping serve-time both produce it, and treating the future as maximally
 * fresh is exactly backwards.
 */
export function classify(observedAt, now, thresholds = DEFAULT_THRESHOLDS) {
  const seen = typeof observedAt === 'string' ? Date.parse(observedAt) : NaN;
  const at = now instanceof Date ? now.getTime() : Date.parse(now);
  if (Number.isNaN(seen) || Number.isNaN(at)) {
    return { level: UNKNOWN, ageSeconds: null, reason: 'no parseable observation time' };
  }
  const ageSeconds = (at - seen) / 1000;
  if (ageSeconds < 0) {
    return { level: UNKNOWN, ageSeconds, reason: 'observation is in the future; check clock skew' };
  }
  const { freshUnderSeconds, delayedUnderSeconds } = { ...DEFAULT_THRESHOLDS, ...thresholds };
  if (ageSeconds < freshUnderSeconds) return { level: FRESH, ageSeconds, reason: null };
  if (ageSeconds < delayedUnderSeconds) return { level: DELAYED, ageSeconds, reason: 'update is late' };
  return { level: STALE, ageSeconds, reason: 'newer scores may exist' };
}

/**
 * The health payload for `GET /v1/health`.
 *
 * Reports the OLDEST game's freshness, not the newest. One game updating keeps
 * the snapshot's own timestamp current while another sits frozen, which is the
 * precise shape of the failure the audit measured: 43% of games froze while the
 * scoreboard as a whole looked alive. A health check that reads the newest
 * observation would have called that healthy throughout.
 */
export function health(games, now, thresholds = DEFAULT_THRESHOLDS) {
  const entries = Object.values(games ?? {});
  const live = entries.filter((game) => game.status === 'in_progress' || game.status === 'halftime');
  const considered = live.length ? live : entries;

  let worst = null;
  for (const game of considered) {
    const verdict = classify(game.observed_at, now, thresholds);
    if (worst === null) { worst = { game, verdict }; continue; }
    // UNKNOWN is worse than any age: we cannot vouch for it at all.
    if (verdict.level === UNKNOWN && worst.verdict.level !== UNKNOWN) { worst = { game, verdict }; continue; }
    if (worst.verdict.level === UNKNOWN) continue;
    if ((verdict.ageSeconds ?? -1) > (worst.verdict.ageSeconds ?? -1)) worst = { game, verdict };
  }

  return {
    games_tracked: entries.length,
    games_live: live.length,
    // Named for what it is. `oldest_live_observation` on a slate with no live
    // games would be a lie, so the basis is stated.
    basis: live.length ? 'oldest live game' : 'oldest game of any status',
    level: worst?.verdict.level ?? UNKNOWN,
    age_seconds: worst?.verdict.ageSeconds ?? null,
    // `??` on the verdict's reason was wrong here: a FRESH verdict's reason is
    // null by design, so a perfectly healthy slate reported 'no games tracked'.
    // Whether there is a game at all is what `worst` says, not what the reason
    // says.
    reason: worst === null ? 'no games tracked' : worst.verdict.reason,
    worst_game_key: worst?.game.game_key ?? null,
  };
}
