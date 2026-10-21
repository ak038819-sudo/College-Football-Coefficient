/* The reader's half of the NFL export contract.
 *
 * The build side of this refusal is src/nfl/publish_gate.py; this is the side
 * that runs in the browser, against whatever manifest the network actually
 * returned. Both are needed, because they stop different things: the build
 * gate stops a bad export being written, and this stops a stale deploy, a
 * cached manifest from before a schema change, a half-served directory or a
 * hand-edited file from being drawn as a rating.
 *
 * Every function here is pure. Nothing fetches, nothing touches the DOM, and
 * nothing is shared with the other two leagues: no college football or
 * basketball global is read or written, and a manifest that does not say it
 * is the NFL is refused outright.
 */

(function (root) {
  'use strict';

  var SUPPORTED_SCHEMA_VERSIONS = [1];

  /* The NFL is a fixed-size league, so this is an equality, not a floor: 32
   * franchises from the 2002 expansion and 31 before it. An export missing
   * one franchise fails here instead of clearing a floor comfortably. */
  var EXPANSION_SEASON = 2002;
  var TEAMS_BEFORE_EXPANSION = 31;
  var TEAMS_AFTER_EXPANSION = 32;

  /* The home team wins about 55% of NFL games, so a coin flip is a weak thing
   * to beat. The floor is the home-base-rate baseline. Matches
   * MAX_HOLDOUT_BRIER in the build gate. */
  var MAX_HOLDOUT_BRIER = 0.245;

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

  /* Why this manifest may not be drawn as NFL ratings, if it may not. */
  function refusals(manifest) {
    var reasons = [];
    if (!manifest || typeof manifest !== 'object') {
      return ['no manifest was loaded'];
    }
    if (SUPPORTED_SCHEMA_VERSIONS.indexOf(manifest.schema_version) < 0) {
      return ['this page does not understand export schema ' +
              JSON.stringify(manifest.schema_version)];
    }
    if (manifest.sport !== 'nfl') {
      return ['this manifest is for ' + JSON.stringify(manifest.sport) +
              ', not the NFL'];
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
      /* A season still being played is judged against the teams that have
       * finished a rateable game. In August none have, and demanding the
       * whole league would make this page refuse a correct export for the
       * only season a reader came to look at. Mirrors check_coverage in
       * src/nfl/publish_gate.py; an unmarked season is read as finished, so
       * a missing flag cannot be the way a thin export gets through. */
      var complete = !season || season.complete !== false;
      var wanted = expectedTeams(season && season.season);
      if (teams !== wanted) {
        reasons.push('season ' + (season && season.season) + ' carries ' +
                     teams + ' teams and the league had ' + wanted);
        return;
      }
      if (complete) {
        if (rated !== teams) {
          reasons.push('season ' + season.season + ' rates only ' + rated +
                       ' of ' + teams + ' teams');
        }
        return;
      }
      var played = season.played_teams;
      if (!isNumber(played) || played < 0) {
        reasons.push('season ' + season.season + ' is still being played but ' +
                     'does not say how many teams have played');
      } else if (rated !== played) {
        reasons.push('season ' + season.season + ' rates ' + rated +
                     ' teams but ' + played + ' have played');
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
    return (base || 'data/nfl/') + entry;
  }

  function fileSource(manifest, name, base) {
    var files = manifest && manifest.files;
    var entry = files && files[name];
    if (typeof entry !== 'string' || !entry) return null;
    return (base || 'data/nfl/') + entry;
  }

  var api = {
    SUPPORTED_SCHEMA_VERSIONS: SUPPORTED_SCHEMA_VERSIONS,
    EXPANSION_SEASON: EXPANSION_SEASON,
    MAX_HOLDOUT_BRIER: MAX_HOLDOUT_BRIER,
    STALE_AFTER_MINUTES: STALE_AFTER_MINUTES,
    expectedTeams: expectedTeams,
    accept: accept,
    refusals: refusals,
    freshness: freshness,
    describeAge: describeAge,
    seasonSource: seasonSource,
    fileSource: fileSource
  };

  if (typeof module === 'object' && module.exports) {
    module.exports = api;
  }
  root.nflLoader = api;
}(typeof globalThis === 'object' ? globalThis : this));
