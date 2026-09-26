/* Global search for the static dashboard (Milestone A2). No framework or build dependency.
 *
 * Works on the compact index written by src/export_static_data.py
 *   { teams: [{id, name, slug, aliases}], seasons: [..], games: [[game_id, season, home_id, away_id], ..],
 *     conferences: [[slug, name, [aliases]], ..] }
 * and understands plain queries:
 *   "byu"                     -> Teams
 *   "miami"                   -> Miami (FL) via the alias table
 *   "2019"                    -> Seasons
 *   "texas a&m lsu 2019"      -> both Teams + their 2019 meeting(s)
 *   "michigan ohio state"     -> every meeting in the dataset, newest first
 *   "indiana 2025"            -> Indiana's 2025 schedule
 *   "tex"                     -> team suggestions while typing (from the start of any word,
 *                                never mid-word: "am" finds Texas A&M, not Miami)
 *   "sec" / "big ten" / "MAC" -> Conferences, by name, slug or common shorthand
 * Conferences match the text the user is still typing, exactly like team suggestions.
 * They never consume a token, so "michigan ohio state" stays a team-vs-team lookup.
 * Team mentions are whole-word matches (longest first), so "texas a&m" never
 * also matches "Texas", while partial words still produce suggestions.
 */
(function (root, factory) {
  const nav = (typeof module === 'object' && module.exports) ? require('./navigation.js') : root.CfbNavigation;
  const api = factory(nav);
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.CfbSearch = api;
})(typeof globalThis !== 'undefined' ? globalThis : this, function (nav) {
  'use strict';
  const normalize = nav.normalizeSearch;
  const SEP = '\u0001';

  // Words as the user sees them: split on spaces, hyphens and slashes, then
  // normalize each word ("Texas A&M" -> ["texas", "am"], "miami-fl" -> ["miami", "fl"]).
  function words(text) {
    return String(text || '').split(/[\s\-\/]+/).map(normalize).filter(Boolean);
  }

  // Name/alias/slug -> the word forms a prefix search runs against. Shared by teams and
  // conferences so both behave identically while typing.
  function nameVariants(labels) {
    const out = [], seen = new Set();
    labels.filter(Boolean).forEach(v => {
      const w = words(v);
      const key = w.join(SEP);
      // starts: the name read from each word onward ("texas a&m" -> "texasam", "am"), so a
      // suggestion can begin at any WORD but never mid-word ("am" must not match "miami").
      if (w.length && !seen.has(key)) {
        seen.add(key);
        out.push({ words: w, key, joined: w.join(''), starts: w.map((_, i) => w.slice(i).join('')) });
      }
    });
    return out;
  }

  // Best (lowest) prefix score for a query across a set of variants: 0 = the whole name
  // starts with it, 1 = a later word does, null = no match.
  function prefixScore(variants, q) {
    let best = null;
    variants.forEach(v => {
      const score = v.joined.startsWith(q) ? 0 : v.starts.some(x => x.startsWith(q)) ? 1 : null;
      if (score !== null && (best === null || score < best)) best = score;
    });
    return best;
  }

  function buildSearcher(index) {
    const teams = (index && index.teams) || [];
    // Conferences (Milestone E pages): [slug, name, [aliases]].
    const conferences = ((index && index.conferences) || []).map(c => ({
      slug: c[0], name: c[1], aliases: c[2] || [],
      variants: nameVariants([c[1], c[0], ...(c[2] || [])])
    }));
    const seasons = new Set(((index && index.seasons) || []).map(Number));
    const games = (index && index.games) || [];
    const variants = [];
    teams.forEach(t => {
      nameVariants([t.name, ...(t.aliases || []), t.slug]).forEach(v => variants.push({ team: t, ...v }));
    });
    const gamesByTeam = new Map();
    games.forEach((g, i) => {
      [g[2], g[3]].forEach(id => {
        if (!gamesByTeam.has(id)) gamesByTeam.set(id, []);
        gamesByTeam.get(id).push(i);
      });
    });
    const asGame = g => ({ game_id: g[0], season: g[1], home_id: g[2], away_id: g[3] });

    function search(query, opts) {
      const limits = Object.assign({ teams: 8, games: 12, conferences: 5 }, opts || {});
      const tokens = words(String(query || '').slice(0, 150));
      // A four-digit token that is a real season is the year; only the first one counts.
      const yearAt = tokens.findIndex(t => /^\d{4}$/.test(t) && seasons.has(Number(t)));
      const year = yearAt >= 0 ? Number(tokens[yearAt]) : null;
      const rest = tokens.filter((_, i) => i !== yearAt);

      // Whole-word team mentions, longest match first, left to right.
      const found = [], leftover = [];
      for (let i = 0; i < rest.length;) {
        let best = null;
        for (const v of variants) {
          const n = v.words.length;
          if ((!best || n > best.n) && rest.slice(i, i + n).join(SEP) === v.key) best = { v, n };
        }
        if (best) { if (!found.includes(best.v.team)) found.push(best.v.team); i += best.n; }
        else { leftover.push(rest[i]); i += 1; }
      }

      // Suggestions for whatever wasn't a complete team name (or the whole text while typing).
      const q = (leftover.length ? leftover : (found.length > 1 ? [] : rest)).join('');
      const scored = new Map();
      if (q.length >= 2) {
        variants.forEach(v => {
          const score = v.joined.startsWith(q) ? 0 : v.starts.some(x => x.startsWith(q)) ? 1 : null;
          if (score === null) return;
          const prev = scored.get(v.team.id);
          if (prev === undefined || score < prev) scored.set(v.team.id, score);
        });
      }
      const suggestions = teams.filter(t => scored.has(t.id) && !found.includes(t))
        .sort((a, b) => scored.get(a.id) - scored.get(b.id) || a.name.localeCompare(b.name));
      const teamHits = [...found, ...suggestions];

      // Conferences run off the same typed text, so they appear while typing and never
      // steal a word from a team-vs-team query.
      const confScored = [];
      if (q.length >= 2) {
        conferences.forEach(c => {
          const score = prefixScore(c.variants, q);
          if (score !== null) confScored.push({ conf: c, score });
        });
      }
      confScored.sort((a, b) => a.score - b.score || a.conf.name.localeCompare(b.conf.name));
      const confHits = confScored.map(x => x.conf);

      let gameIdx = [];
      if (found.length >= 2) {
        const [a, b] = found;
        gameIdx = (gamesByTeam.get(a.id) || []).filter(i => {
          const g = games[i];
          return (g[2] === b.id || g[3] === b.id) && (year === null || g[1] === year);
        }).reverse();                                   // newest first
      } else if (found.length === 1 && year !== null) {
        gameIdx = (gamesByTeam.get(found[0].id) || []).filter(i => games[i][1] === year);   // season order
      }

      return {
        year,
        mentioned: found.map(t => t.id),
        teams: teamHits.slice(0, limits.teams),
        teamsTotal: teamHits.length,
        games: gameIdx.slice(0, limits.games).map(i => asGame(games[i])),
        gamesTotal: gameIdx.length,
        conferences: confHits.slice(0, limits.conferences).map(c => ({ slug: c.slug, name: c.name })),
        conferencesTotal: confHits.length,
        seasons: year !== null ? [year] : []
      };
    }

    return { search };
  }

  return { buildSearcher, words };
});
