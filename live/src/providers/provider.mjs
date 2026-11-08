/**
 * The provider contract every sports-data adapter must satisfy.
 *
 * WHY THIS IS JAVASCRIPT AND NOT TYPESCRIPT. The design proposal's §11 shows
 * `.ts` files. These are `.mjs` instead, deliberately and reversibly:
 * Cloudflare Workers run standard ES modules natively, so TypeScript buys types
 * and costs a build step, a toolchain and a second way for `npm test` to fail.
 * Milestone A's deliverable is contract tests passing against saved fixtures,
 * and that is reachable today with the repository's existing `node --test`
 * harness and no new dependency. Porting to TypeScript later is mechanical --
 * the module boundaries here are the ones the proposal asked for. Austin's call
 * if he wants the types now.
 *
 * WHAT AN ADAPTER MAY AND MAY NOT DO.
 *
 * An adapter translates one provider's response into the Elocast game object
 * and does nothing else. It may not decide policy: not how often to poll, not
 * whether a snapshot is stale, not whether a transition is legal. Those live in
 * `ingestion/` and `state/` so that adding a provider cannot change them.
 *
 * It must NEVER invent a value. The proposal says this and it is the rule most
 * easily broken by accident: a game clock, a possession, or a
 * `provider_updated_at` that the provider did not send is `null`, never a
 * plausible default. A fabricated timestamp turns provider lag into our lag,
 * and a fabricated clock renders as 0:00, which this project has shipped three
 * times.
 */

/**
 * @typedef {object} Team
 * @property {string} team_key      Namespaced: `<sport>:<provider>:team:<id>`.
 * @property {number|null} institution_id  ESPN team id, joining a school's
 *   teams across leagues. `null` for professional teams and where unknown.
 * @property {string} name
 * @property {number|null} score    `null` when not reported. NEVER 0 as a
 *   stand-in: an unplayed game is null-null, not 0-0.
 * @property {string|null} classification
 */

/**
 * @typedef {object} Game
 * @property {number} schema_version
 * @property {string} sport
 * @property {string} game_key      `<sport>:<provider>:game:<id>`.
 * @property {string} provider
 * @property {GameStatus} status
 * @property {number|null} period
 * @property {string|null} clock    As the provider spells it, or null.
 * @property {string|null} possession_team_key
 * @property {string|null} provider_updated_at  Null unless the provider says.
 * @property {string} observed_at   When ingestion received this snapshot.
 * @property {Team} home
 * @property {Team} away
 * @property {string|null} start_date
 * @property {string|null} venue
 * @property {boolean} neutral_site
 */

/**
 * The normalized status set.
 *
 * MEASURED CAVEAT, and it bears on the proposal's §6. CFBD's `/scoreboard`
 * emits only `scheduled`, `in_progress` and `completed` -- `clean_game` in
 * `src/fetch_live_scores.py` raises on anything else, and fifteen days of
 * published snapshots contain only those three. So `halftime`, `delayed`,
 * `postponed`, `canceled` and `suspended` are part of this contract but are
 * NOT OBTAINABLE from the endpoint the live worker polls. §6's cadence table
 * has a halftime row; satisfying it needs either inference (period 2, clock
 * 00:00) or a second endpoint, and inference should be marked as inferred
 * rather than served as if the provider said it.
 */
export const STATUSES = Object.freeze([
  'scheduled',
  'delayed',
  'in_progress',
  'halftime',
  'final',
  'postponed',
  'canceled',
  'suspended',
  'unknown',
]);

/** Statuses that mean the game is over and its score should not move again. */
export const TERMINAL_STATUSES = Object.freeze(['final', 'canceled']);

export const SCHEMA_VERSION = 1;

/**
 * Build a namespaced entity key.
 *
 * Refuses a bare or malformed id rather than producing a key that looks valid.
 * The three existing league modules (`src/nba/identity.py`,
 * `src/nfl/identity.py`, `src/ncaambb/identity.py`) each refuse a bare provider
 * id as a key, for the reason measured in
 * `docs/identity-cross-league-measurement.md`: 81 CBBD ids are also valid CFBD
 * ids for a different school, so an unnamespaced id silently identifies the
 * wrong team.
 */
export function entityKey(sport, provider, kind, id) {
  for (const part of [sport, provider, kind]) {
    if (typeof part !== 'string' || part === '' || part.includes(':')) {
      throw new TypeError(`invalid key component: ${JSON.stringify(part)}`);
    }
  }
  if (typeof id === 'number') {
    if (!Number.isInteger(id)) throw new TypeError(`non-integer entity id: ${id}`);
  } else if (typeof id !== 'string' || id === '' || id.includes(':')) {
    throw new TypeError(`invalid entity id: ${JSON.stringify(id)}`);
  }
  return `${sport}:${provider}:${kind}:${id}`;
}

/**
 * The interface. Implementations are plain objects with these methods, so a
 * test can substitute one without a class hierarchy.
 *
 * @typedef {object} LiveProvider
 * @property {string} sport
 * @property {string} provider
 * @property {() => Promise<Game[]>} getLiveGames
 * @property {(gameKey: string) => Promise<Game|null>} getGame
 * @property {() => Promise<{ok: boolean, detail: string}>} getProviderHealth
 */

/** Throws unless `candidate` implements the interface above. */
export function assertProvider(candidate) {
  for (const method of ['getLiveGames', 'getGame', 'getProviderHealth']) {
    if (typeof candidate?.[method] !== 'function') {
      throw new TypeError(`provider is missing ${method}()`);
    }
  }
  for (const field of ['sport', 'provider']) {
    if (typeof candidate?.[field] !== 'string' || !candidate[field]) {
      throw new TypeError(`provider is missing ${field}`);
    }
  }
  return candidate;
}
