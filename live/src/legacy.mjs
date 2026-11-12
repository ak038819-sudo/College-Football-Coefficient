/**
 * Project normalized games back into the snapshot the existing website reads.
 *
 * WHY THIS EXISTS, AND WHY IT IS THE MOST IMPORTANT FILE IN MILESTONE B.
 *
 * The design proposal introduces a new normalized shape at `/v1/ncaaf/live`.
 * The published website does not read that shape -- it reads the object
 * `src/fetch_live_scores.py` writes, and `docs/live-score-delivery.md` requires
 * an endpoint serving it to emit that object BYTE FOR BYTE. Those two facts
 * together mean the new API alone cannot be pointed at the live site, and the
 * "cheapest first integration test" in the Phase 1 plan would not actually
 * work.
 *
 * So the shadow service serves both: `/v1/...` for the future, and a
 * legacy-compatible payload that `LIVE_FEED_URL` can target with ZERO frontend
 * change. That turns Milestone D's canary from a rebuild-and-deploy into a URL,
 * and it means the whole live scoreboard -- Home, the Live board, the open game
 * page, the rating-freshness check -- exercises the new backend on day one.
 *
 * THE TEST THAT MAKES THIS TRUSTWORTHY is in `tests/legacy_projection.test.mjs`:
 * real published snapshots are run back through `toLegacySnapshot` and must come
 * out identical, key order included. That is a differential test against
 * production output rather than against my reading of the Python, which is the
 * only way to catch validation drift.
 */

/**
 * Key order matters. `JSON.stringify` emits insertion order, and the
 * requirement is a byte-for-byte match, so these arrays are the contract.
 * They mirror the dict literals in `clean_game` and `make_snapshot`.
 */
const GAME_KEY_ORDER = Object.freeze([
  'id', 'start_date', 'venue', 'status', 'period', 'clock', 'tv',
  'neutral_site', 'away', 'home',
]);
const TEAM_KEY_ORDER = Object.freeze(['id', 'name', 'points', 'classification']);

/** The legacy status spelling. `final` was `completed` on the wire. */
const LEGACY_STATUS = Object.freeze({
  scheduled: 'scheduled',
  in_progress: 'in_progress',
  final: 'completed',
});

function legacyTeam(team) {
  const out = {};
  for (const key of TEAM_KEY_ORDER) {
    out[key] = key === 'id' ? team.provider_team_id
      : key === 'points' ? team.score
        : team[key] ?? null;
  }
  return out;
}

/**
 * One game, in the legacy shape.
 *
 * A game whose status has no legacy spelling -- `unknown`, or any of the
 * statuses `/scoreboard` cannot produce -- is REFUSED rather than mapped to
 * something plausible. The legacy consumer only understands three values, and
 * inventing a fourth, or quietly calling an unknown game `scheduled`, would
 * put a wrong state on the published scoreboard. The caller drops it and says
 * so; see `toLegacySnapshot`.
 */
export function toLegacyGame(game) {
  const status = LEGACY_STATUS[game.status];
  if (!status) return null;
  const out = {};
  for (const key of GAME_KEY_ORDER) {
    if (key === 'status') out[key] = status;
    else if (key === 'away') out[key] = legacyTeam(game.away);
    else if (key === 'home') out[key] = legacyTeam(game.home);
    else if (key === 'neutral_site') out[key] = game.neutral_site === true;
    else if (key === 'id') out[key] = game.provider_game_id;
    else out[key] = game[key] ?? null;
  }
  return out;
}

/**
 * The whole snapshot.
 *
 * `updated_at` is the OBSERVATION time, never serve time. The page prints the
 * snapshot's age from this field, so stamping serve-time here would make it
 * claim a freshness it does not have -- which `live-score-delivery.md` calls
 * out as worse than the staleness it replaces.
 *
 * THE PLAYER BOX-SCORE BLOCK. A published snapshot carries three more
 * top-level keys that this projection did not emit until the differential test
 * caught it: `player_boxscores`, `player_boxscore_times` and
 * `player_stats_checked_at`. `ui/dashboard_shell.html` reads all three to draw
 * the player panel on an open game, so a shadow service that dropped them
 * would silently empty that panel -- a regression no test of the /v1 shape
 * would ever see.
 *
 * They are PASSED THROUGH, not produced here. That block comes from CFBD's
 * `/games/players`, which the adapter deliberately does not call on the live
 * cycle (see `providers/cfbd.mjs`); it belongs to the slower batch. So the
 * caller supplies `players` from whatever most recently fetched it, and this
 * function neither invents nor ages it. Omitting `players` emits the two maps
 * as empty objects, matching `merge_player_boxscores` on a snapshot that has
 * never had a box score, and leaves `player_stats_checked_at` off entirely --
 * which is exactly what the Python does when it has no time to report, rather
 * than writing a null the page would read as a time.
 *
 * Returns `{snapshot, dropped}`; `dropped` names any game with no legacy
 * status, so the caller can report rather than lose it silently.
 */
export function toLegacySnapshot(games, { updatedAt, source = 'CollegeFootballData.com', players } = {}) {
  if (typeof updatedAt !== 'string' || !updatedAt) {
    throw new TypeError('updatedAt is required and must be the observation time');
  }
  const dropped = [];
  const out = [];
  for (const game of games) {
    const legacy = toLegacyGame(game);
    if (legacy === null) dropped.push({ game_key: game.game_key, status: game.status });
    else out.push(legacy);
  }
  // Key order mirrors make_snapshot followed by merge_player_boxscores.
  const snapshot = { source, updated_at: updatedAt, games: out };
  snapshot.player_boxscores = players?.boxscores ?? {};
  snapshot.player_boxscore_times = players?.times ?? {};
  if (typeof players?.checkedAt === 'string' && players.checkedAt) {
    snapshot.player_stats_checked_at = players.checkedAt;
  }
  return { snapshot, dropped };
}

/**
 * Serialize exactly as the publisher writes it.
 *
 * `src/fetch_live_scores.py` writes
 * `json.dumps(snapshot, separators=(',', ':'), ensure_ascii=False) + '\n'`.
 * Compact separators and literal non-ASCII are what `JSON.stringify(value)`
 * produces too, and the test asserts that against the real fixtures rather
 * than taking it on trust.
 *
 * ONE DIVERGENCE IS UNCLOSEABLE IN JAVASCRIPT and is therefore stated here
 * rather than claimed away. A JS object orders integer-like keys FIRST and
 * ASCENDING, no matter how they were inserted or parsed; Python keeps
 * insertion order. `player_boxscores` and `player_boxscore_times` are keyed by
 * provider game id, and a published snapshot does list those ids out of
 * ascending order, so this function's output is NOT byte-identical inside
 * those two maps. It is byte-identical everywhere else and parse-equal there.
 * The consumer looks those maps up by key, so the order is not information to
 * it -- but the Milestone D canary has to compare accordingly, and no future
 * consumer may depend on that order. The contract test pins exactly this.
 *
 * THE TRAILING NEWLINE IS NOT REPRODUCED, and that is deliberate. It is an
 * artifact of writing a file, not part of the document, and an HTTP response
 * body is handed to the page's `fetch` and re-parsed. If a byte comparison
 * against the published file is ever wanted, compare `serializeLegacy(s) +
 * '\n'`.
 */
export function serializeLegacy(snapshot) {
  return JSON.stringify(snapshot);
}
