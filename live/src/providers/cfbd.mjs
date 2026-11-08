/**
 * The CFBD adapter: one provider request per cycle.
 *
 * ONE REQUEST, NOT THREE. The proposal's §4 budget assumes one provider call
 * per polling cycle, but `src/fetch_live_scores.py` makes three per tick --
 * `/scoreboard`, `/calendar` and `/games/players` -- which made every figure in
 * that table a third of the real number. This adapter fetches `/scoreboard`
 * alone. The other two serve the weekly player box-score batch, which does not
 * need 15-second granularity and belongs on its own slower schedule. That is
 * what makes "1 request per cycle" true rather than aspirational.
 *
 * REIMPLEMENTING `clean_game` IS SAFE HERE, AND THAT NEEDED CHECKING.
 * `docs/live-score-delivery.md` says an endpoint must emit byte-for-byte what
 * `fetch_live_scores.py` writes and must never reimplement the normalization,
 * because `data/reference/team_identities.json` is the only reviewed mapping
 * from provider identities to this project's teams and a drifting second
 * implementation shows up as a game attributed to the wrong school.
 *
 * That warning is correct for the ARCHIVE path. It does not apply to the live
 * scoreboard, because the live path does no identity mapping at all: read
 * `clean_game` and it passes CFBD's own team id and name straight through. What
 * it actually does is validate types, whitelist three statuses, rename
 * camelCase to snake_case, trim the venue to 120 characters and coerce
 * `neutralSite` to a strict boolean. All of that is mechanical and portable.
 *
 * So the risk is not identity drift, it is VALIDATION drift -- and the tests
 * close that by requiring this module to reproduce real published snapshots
 * byte for byte. The legacy projection in `../legacy.mjs` is what the existing
 * website consumes unchanged.
 */
import { SCHEMA_VERSION, entityKey } from './provider.mjs';

export const SPORT = 'ncaaf';
export const PROVIDER = 'cfbd';
export const SCOREBOARD_URL = 'https://api.collegefootballdata.com/scoreboard?classification=fbs';

/**
 * The statuses `/scoreboard` is documented and observed to emit, mapped to the
 * normalized set. `completed` becomes `final`.
 *
 * An unrecognised status becomes `unknown` and is KEPT, not dropped and not
 * silently read as `scheduled`. `fetch_live_scores.py` raises instead, which is
 * right for a file that must be perfect or absent; a live service that throws
 * away the whole slate because one game carries a new string is worse than one
 * that serves the game marked unknown.
 */
const STATUS_MAP = Object.freeze({
  scheduled: 'scheduled',
  in_progress: 'in_progress',
  completed: 'final',
});

/** Integer, or null. Never coerces: `Number(null)` is 0 and that has shipped here. */
function intOrNull(value) {
  if (value === null || value === undefined) return null;
  return Number.isInteger(value) ? value : null;
}

/**
 * A score: a non-negative integer, or null.
 *
 * A negative or fractional score is data we do not understand, so it becomes
 * null rather than being passed on. Crucially null is NOT 0: an unplayed game
 * is null-null. The NBA source writes 0-0 for scheduled fixtures and
 * `src/nba/` already guards against reading it; this must not reintroduce it.
 */
function scoreOrNull(value) {
  if (value === null || value === undefined) return null;
  if (typeof value !== 'number' || !Number.isInteger(value) || value < 0) return null;
  return value;
}

/** A non-empty string, or null. Whitespace-only is null. */
function textOrNull(value, limit = null) {
  if (typeof value !== 'string') return null;
  const trimmed = value.trim();
  if (!trimmed) return null;
  return limit === null ? trimmed : trimmed.slice(0, limit);
}

function normalizeTeam(raw, institutionLookup) {
  const name = textOrNull(raw?.name);
  if (!name) throw new Error('scoreboard team has no name');
  const providerId = intOrNull(raw?.id);
  return {
    team_key: providerId === null ? null : entityKey(SPORT, PROVIDER, 'team', providerId),
    provider_team_id: providerId,
    institution_id: providerId === null ? null : institutionLookup(providerId),
    name,
    score: scoreOrNull(raw?.points),
    classification: textOrNull(raw?.classification),
  };
}

/**
 * Normalize one `/scoreboard` row.
 *
 * `observedAt` is supplied by the caller rather than read from a clock inside
 * here, so a test is deterministic and so every game in one poll carries the
 * same observation time.
 */
export function normalizeGame(raw, { observedAt, institutionLookup = () => null } = {}) {
  if (typeof observedAt !== 'string' || !observedAt) {
    throw new TypeError('observedAt is required');
  }
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
    throw new Error('scoreboard row is not an object');
  }
  if (!Number.isInteger(raw.id)) throw new Error('invalid scoreboard game id');

  const status = STATUS_MAP[raw.status] ?? 'unknown';
  const away = normalizeTeam(raw.awayTeam, institutionLookup);
  const home = normalizeTeam(raw.homeTeam, institutionLookup);

  return {
    schema_version: SCHEMA_VERSION,
    sport: SPORT,
    provider: PROVIDER,
    game_key: entityKey(SPORT, PROVIDER, 'game', raw.id),
    provider_game_id: raw.id,
    status,
    provider_status: typeof raw.status === 'string' ? raw.status : null,
    period: intOrNull(raw.period),
    clock: textOrNull(raw.clock),
    // CFBD's /scoreboard carries no possession field and no update stamp.
    // Both stay null rather than being guessed. See the module header.
    possession_team_key: null,
    provider_updated_at: null,
    observed_at: observedAt,
    start_date: textOrNull(raw.startDate),
    venue: textOrNull(raw.venue, 120),
    tv: textOrNull(raw.tv),
    neutral_site: raw.neutralSite === true,
    away,
    home,
  };
}

/**
 * Normalize a whole `/scoreboard` payload.
 *
 * A duplicate game id fails the whole poll, matching `make_snapshot`: two rows
 * for one game means we cannot tell which is current, and serving either is a
 * guess. A single unparseable row also fails the poll rather than being
 * skipped, because a slate quietly missing a game is the defect this project
 * spent two weeks not noticing.
 */
export function normalizeScoreboard(payload, options) {
  if (!Array.isArray(payload)) throw new Error('scoreboard response must be a list');
  const games = payload.map((row) => normalizeGame(row, options));
  const keys = new Set(games.map((g) => g.game_key));
  if (keys.size !== games.length) throw new Error('duplicate scoreboard game id');
  return games;
}

/**
 * Build the adapter. `fetchImpl` and `now` are injected so tests never reach
 * the network and never depend on the wall clock.
 */
export function createCfbdProvider({ apiKey, fetchImpl = fetch, now = () => new Date(), institutionLookup } = {}) {
  if (typeof apiKey !== 'string' || !apiKey) {
    throw new TypeError('a CFBD API key is required; it must stay server-side');
  }

  async function scoreboard() {
    const response = await fetchImpl(SCOREBOARD_URL, {
      headers: {
        Authorization: `Bearer ${apiKey}`,
        Accept: 'application/json',
        'User-Agent': 'elocast-live/1.0',
      },
    });
    if (!response.ok) {
      const error = new Error(`CFBD /scoreboard returned ${response.status}`);
      error.status = response.status;
      // 429 and 5xx are worth retrying; 401 and 404 are not. The coordinator
      // decides, so this only records which kind it was.
      error.retryable = response.status === 429 || response.status >= 500;
      throw error;
    }
    return response.json();
  }

  return {
    sport: SPORT,
    provider: PROVIDER,

    async getLiveGames() {
      const observedAt = now().toISOString();
      return normalizeScoreboard(await scoreboard(), { observedAt, institutionLookup });
    },

    async getGame(gameKey) {
      const games = await this.getLiveGames();
      return games.find((game) => game.game_key === gameKey) ?? null;
    },

    async getProviderHealth() {
      try {
        const payload = await scoreboard();
        return { ok: Array.isArray(payload), detail: `scoreboard returned ${Array.isArray(payload) ? payload.length : 0} rows` };
      } catch (error) {
        return { ok: false, detail: String(error.message ?? error) };
      }
    },
  };
}
