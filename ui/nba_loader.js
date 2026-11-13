/* The reader's half of the NBA export contract.
 *
 * The build side of this refusal is src/nba/publish_gate.py; this is the side
 * that runs in the browser, against whatever manifest the network actually
 * returned. Both are needed, because they stop different things: the build
 * gate stops a bad export being written, and this stops a stale deploy, a
 * cached manifest from before a schema change, a half-served directory or a
 * hand-edited file from being drawn as a rating.
 *
 * Every function here is pure. Nothing fetches, nothing touches the DOM, and
 * nothing is shared with the other three leagues: no college football,
 * college basketball or NFL global is read or written, and a manifest that
 * does not say it is the NBA is refused outright.
 */

(function (root) {
  'use strict';

  /* Every version this page can draw, in the same order as
   * SUPPORTED_SCHEMA_VERSIONS in src/nba/publish_gate.py. A reader that
   * refused the older ones would blank the page for anyone served this
   * script before the next refresh rewrites the export, and one that refused
   * the newest would blank it for everyone after. */
  var SUPPORTED_SCHEMA_VERSIONS = [1, 2, 3, 4, 5];

  /* The NBA is a fixed-size league, so this is an equality, not a floor: 30
   * franchises from the 2004-05 Charlotte expansion and 29 before it. An
   * export missing one franchise fails here instead of clearing a floor
   * comfortably. The season is named for the year it ENDS, so `2005` is
   * 2004-05, the expansion's first. */
  var EXPANSION_SEASON = 2005;
  var TEAMS_BEFORE_EXPANSION = 29;
  var TEAMS_AFTER_EXPANSION = 30;

  /* MEASURED on this league, not copied from another. NBA home teams won
   * 55.53% of the 6,605 held-out games, which puts the all-home baseline at
   * a Brier of 0.24694. Matches MAX_HOLDOUT_BRIER in the build gate; its
   * closeness to the NFL's 0.245 is a coincidence of two sports having a
   * similar home edge. */
  var MAX_HOLDOUT_BRIER = 0.246;

  /* An export older than this is still shown — it is the last good snapshot —
   * but shown as stale, with its age, rather than as current. */
  var STALE_AFTER_MINUTES = 180;

  function isNumber(value) {
    return typeof value === 'number' && isFinite(value);
  }

  function expectedTeams(season) {
    return season >= EXPANSION_SEASON
      ? TEAMS_AFTER_EXPANSION : TEAMS_BEFORE_EXPANSION;
  }

  function parseMoment(value) {
    if (typeof value !== 'string' || !value) return null;
    var parsed = Date.parse(value);
    return isNaN(parsed) ? null : parsed;
  }

  /* Why this manifest may not be drawn as NBA ratings, if it may not. */
  function refusals(manifest) {
    var reasons = [];
    if (!manifest || typeof manifest !== 'object') {
      return ['no manifest was loaded'];
    }
    if (SUPPORTED_SCHEMA_VERSIONS.indexOf(manifest.schema_version) < 0) {
      return ['this page does not understand export schema ' +
              JSON.stringify(manifest.schema_version)];
    }
    if (manifest.sport !== 'nba') {
      return ['this manifest is for ' + JSON.stringify(manifest.sport) +
              ', not the NBA'];
    }
    var model = manifest.model;
    if (!model || typeof model !== 'object') {
      reasons.push('the export does not say which model made these ratings');
    } else {
      var held = model.held_out || [];
      if (!held.length) {
        reasons.push('the model was never scored on held-out seasons');
      }
      if (!isNumber(model.holdout_brier)
          || model.holdout_brier >= MAX_HOLDOUT_BRIER) {
        reasons.push('the model has no held-out score better than picking ' +
                     'the home team');
      }
    }
    var coverage = manifest.coverage;
    if (!Array.isArray(coverage) || !coverage.length) {
      reasons.push('the export measures no coverage, so it cannot be called ' +
                   'a league export');
      return reasons;
    }
    coverage.forEach(function (season) {
      var teams = season && season.teams;
      var rated = season && season.rated_teams;
      /* A season still being played is judged against the franchises that
       * have finished a rateable game. In September none have, and demanding
       * the whole league would make this page refuse a correct export for the
       * only season a reader came to look at. Mirrors check_coverage in
       * src/nba/publish_gate.py; an unmarked season is read as finished, so
       * a missing flag cannot be the way a thin export gets through. */
      var complete = !season || season.complete !== false;
      var wanted = expectedTeams(season && season.season);
      if (teams !== wanted) {
        reasons.push('season ' + (season && season.season) + ' carries ' +
                     teams + ' franchises and the league had ' + wanted);
        return;
      }
      if (complete) {
        if (rated !== teams) {
          reasons.push('season ' + season.season + ' rates only ' + rated +
                       ' of ' + teams + ' franchises');
        }
        return;
      }
      var played = season.played_teams;
      if (!isNumber(played) || played < 0) {
        reasons.push('season ' + season.season + ' is still being played but ' +
                     'does not say how many franchises have played');
      } else if (rated !== played) {
        reasons.push('season ' + season.season + ' rates ' + rated +
                     ' franchises but ' + played + ' have played');
      }
    });
    return reasons;
  }

  /* How old this export is, and whether that should be said out loud.
   *
   * Age is reported even when the manifest is refused, because "we have
   * nothing to show you and the last attempt was two days ago" is more useful
   * than either half alone.
   */
  function freshness(manifest, now) {
    var moment = parseMoment(manifest && manifest.generated_at);
    var current = isNumber(now) ? now : Date.now();
    if (moment === null) {
      return { known: false, stale: true, minutes: null,
               label: 'last updated: unknown' };
    }
    var minutes = Math.max(0, Math.round((current - moment) / 60000));
    return {
      known: true,
      stale: minutes >= STALE_AFTER_MINUTES,
      minutes: minutes,
      label: describeAge(minutes)
    };
  }

  function describeAge(minutes) {
    if (minutes < 1) return 'updated just now';
    if (minutes < 60) return 'updated ' + minutes + 'm ago';
    var hours = Math.floor(minutes / 60);
    if (hours < 48) return 'updated ' + hours + 'h ago';
    return 'updated ' + Math.floor(hours / 24) + 'd ago';
  }

  /* The one call a page makes before it draws anything. A refused manifest
   * yields ok:false and reasons; it never yields a partial board. */
  function accept(manifest, now) {
    var reasons = refusals(manifest);
    var fresh = freshness(manifest, now);
    return {
      ok: reasons.length === 0,
      reasons: reasons,
      stale: fresh.stale,
      age: fresh,
      latestSeason: reasons.length === 0 ? manifest.latest_season : null,
      model: reasons.length === 0 ? manifest.model : null
    };
  }

  /* The URL for a season's payload, version stamp included. Taken from the
   * manifest rather than built from the season number, so a cached file can
   * never be served against a newer manifest. */
  function seasonSource(manifest, season, base) {
    if (!manifest || !manifest.seasons) return null;
    var entry = manifest.seasons[String(season)];
    if (typeof entry !== 'string' || !entry) return null;
    return (base || 'data/nba/') + entry;
  }

  function fileSource(manifest, name, base) {
    var files = manifest && manifest.files;
    var entry = files && files[name];
    if (typeof entry !== 'string' || !entry) return null;
    return (base || 'data/nba/') + entry;
  }

  /* Every season the manifest names, newest first.
   *
   * Sorted NUMERICALLY. The manifest's season keys are strings, because JSON
   * object keys are, and `Object.keys` on them is insertion order rather than
   * any order at all — a page that trusted it would open on whichever season
   * the export happened to write last.
   */
  function seasons(manifest) {
    var listed = manifest && manifest.seasons;
    if (!listed || typeof listed !== 'object') return [];
    return Object.keys(listed).map(Number).filter(function (season) {
      return isFinite(season);
    }).sort(function (a, b) { return b - a; });
  }

  /* `2027` shown as `2026-27`, the way a season straddling two calendar
   * years is written. The same rule as identity.season_label, and the reason
   * it is here as well is that the board's season picker is drawn from the
   * manifest's keys, which carry the number alone. */
  function seasonLabel(season) {
    var year = Number(season);
    if (!isFinite(year)) return String(season);
    var tail = year % 100;
    return (year - 1) + '-' + (tail < 10 ? '0' + tail : String(tail));
  }

  /* The file holding one game's line score, shape and margin curve.
   *
   * The path is DERIVED, not looked up: `games/<season>/<YYYY-MM>.js`, with
   * the month taken from the date of the game the reader just clicked. The
   * manifest states only a cache token per season, because naming all 175
   * month files in a file every reader fetches would cost more than the one
   * month file a reader who opens a game fetches. The token still has to be
   * there: a URL with no version stamp is one a reader can be served from a
   * cache a previous export wrote.
   *
   * A month the manifest does not list yields null rather than a URL that
   * 404s, which is how a page knows to say "no play-by-play for this game"
   * instead of showing a spinner that never stops. */
  function gameMonthKey(game) {
    var date = game && game.date;
    if (typeof date !== 'string' || date.length < 7) return null;
    return date.slice(0, 7);
  }

  function gameMonthSource(manifest, season, month, base) {
    var games = manifest && manifest.files && manifest.files.games;
    var listed = games && games.seasons;
    var entry = listed && listed[String(season)];
    if (!entry || typeof entry.v !== 'string' || !entry.v) return null;
    if (!Array.isArray(entry.months) || entry.months.indexOf(month) < 0) {
      return null;
    }
    return (base || 'data/nba/') + 'games/' + season + '/' + month +
      '.js?v=' + entry.v;
  }

  /* THE PEOPLE LAYER: an index, and a file for each of 2,635 players.
   *
   * One token covers every player file and a second covers the index, for
   * the reason the game layer has one token a season: a player's path is
   * derived here from his own id, so the manifest does not name 2,635 files,
   * and the one file a reader fetches when he opens a player is smaller than
   * a manifest that named them all would be.
   *
   * An export written before the loader ran carries no `files.people`, and
   * both of these then yield null rather than a URL that 404s — which is how
   * the page knows to leave the People tab out instead of drawing an empty
   * one. */
  function peopleIndexSource(manifest, base) {
    var people = manifest && manifest.files && manifest.files.people;
    if (!people || typeof people.index !== 'string' || !people.index) {
      return null;
    }
    return (base || 'data/nba/') + people.index;
  }

  function playerSource(manifest, athleteId, base) {
    var people = manifest && manifest.files && manifest.files.people;
    if (!people || typeof people.v !== 'string' || !people.v) return null;
    var id = String(athleteId === null || athleteId === undefined
      ? '' : athleteId).trim();
    // A PLAYER ID IS DIGITS. The id reaches here off the URL, where a reader
    // may have typed anything, and it is about to become a path: anything
    // else is refused rather than fetched.
    if (!/^[0-9]+$/.test(id)) return null;
    return (base || 'data/nba/') + 'people/' + id + '.js?v=' + people.v;
  }

  var api = {
    SUPPORTED_SCHEMA_VERSIONS: SUPPORTED_SCHEMA_VERSIONS,
    gameMonthKey: gameMonthKey,
    gameMonthSource: gameMonthSource,
    peopleIndexSource: peopleIndexSource,
    playerSource: playerSource,
    EXPANSION_SEASON: EXPANSION_SEASON,
    MAX_HOLDOUT_BRIER: MAX_HOLDOUT_BRIER,
    STALE_AFTER_MINUTES: STALE_AFTER_MINUTES,
    expectedTeams: expectedTeams,
    accept: accept,
    refusals: refusals,
    freshness: freshness,
    describeAge: describeAge,
    seasonSource: seasonSource,
    fileSource: fileSource,
    seasons: seasons,
    seasonLabel: seasonLabel
  };

  if (typeof module === 'object' && module.exports) {
    module.exports = api;
  }
  root.nbaLoader = api;
}(typeof globalThis === 'object' ? globalThis : this));
