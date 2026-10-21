/* The reader's half of the basketball export contract.
 *
 * Release gate 4 says a production loader must refuse a team sample or an
 * unsupported schema rather than show it as national rankings. The build side
 * of that refusal is src/ncaambb/publish_gate.py; this is the side that runs in
 * the browser, against whatever manifest the network actually returned.
 *
 * It exists because the two failures are different. The build gate stops a bad
 * export from being written. This stops a stale deploy, a cached manifest from
 * before a schema change, a half-served directory, or a file someone edited by
 * hand from being drawn as a national rating. Both are needed: the build gate
 * never runs on the reader's machine.
 *
 * Every function here is pure. Nothing fetches, nothing touches the DOM, and
 * nothing is shared with football: no football global is read or written, and a
 * manifest that does not say it is basketball is refused outright.
 */

(function (root) {
  'use strict';

  // The schemas this build understands. A manifest from a newer deploy is
  // refused rather than read optimistically: a field that moved would
  // otherwise be drawn as a number.
  var SUPPORTED_SCHEMA_VERSIONS = [1];

  // Matches the build gate. A sample of one team, one conference or one
  // tournament field is far below this; every real season is far above.
  var MIN_DIVISION_ONE_TEAMS = 300;
  var MIN_RATED_FRACTION = 0.9;

  // An export older than this is still shown — it is the last good snapshot —
  // but it is shown as stale, with its age, rather than as current.
  var STALE_AFTER_MINUTES = 180;

  function isNumber(value) {
    return typeof value === 'number' && isFinite(value);
  }

  function parseMoment(value) {
    if (typeof value !== 'string' || !value) return null;
    var parsed = Date.parse(value);
    return isNaN(parsed) ? null : parsed;
  }

  /* Why this manifest may not be drawn as national rankings, if it may not. */
  function refusals(manifest) {
    var reasons = [];
    if (!manifest || typeof manifest !== 'object') {
      return ['no manifest was loaded'];
    }
    if (SUPPORTED_SCHEMA_VERSIONS.indexOf(manifest.schema_version) < 0) {
      return ['this page does not understand export schema ' +
              JSON.stringify(manifest.schema_version)];
    }
    if (manifest.sport !== 'ncaambb') {
      return ['this manifest is for ' + JSON.stringify(manifest.sport) +
              ', not college basketball'];
    }
    var model = manifest.model;
    if (!model || typeof model !== 'object') {
      reasons.push('the export does not say which model made these ratings');
    } else {
      var held = model.held_out || [];
      if (!held.length) {
        reasons.push('the model was never scored on a held-out season');
      }
      if (!isNumber(model.holdout_brier) || model.holdout_brier >= 0.25) {
        reasons.push('the model has no held-out score better than a coin flip');
      }
    }
    var coverage = manifest.coverage;
    if (!Array.isArray(coverage) || !coverage.length) {
      reasons.push('the export measures no coverage, so it cannot be called ' +
                   'national');
      return reasons;
    }
    coverage.forEach(function (season) {
      var teams = season && season.division_one_teams;
      var rated = season && season.rated_teams;
      /* A season still being played is judged against the teams that have
       * finished a rateable game, not against the whole Division I list. In
       * November most teams have played nobody, and demanding 90% of the
       * country would make this page refuse a correct export for the only
       * season a reader came to look at. Mirrors check_coverage in
       * src/ncaambb/publish_gate.py; an unmarked season is read as finished,
       * so a missing flag cannot be the way a thin export gets through. */
      var complete = !season || season.complete !== false;
      if (!isNumber(teams) || teams < MIN_DIVISION_ONE_TEAMS) {
        reasons.push('season ' + (season && season.season) + ' covers ' +
                     teams + ' Division I teams, which is a sample rather ' +
                     'than the country');
        return;
      }
      if (complete) {
        if (!isNumber(rated) || rated < teams * MIN_RATED_FRACTION) {
          reasons.push('season ' + season.season + ' rates only ' + rated +
                       ' of ' + teams + ' Division I teams');
        }
        return;
      }
      var played = season.played_teams;
      if (!isNumber(played) || played < 0) {
        reasons.push('season ' + season.season + ' is still being played but ' +
                     'does not say how many teams have played');
      } else if (!isNumber(rated) || rated < played * MIN_RATED_FRACTION) {
        reasons.push('season ' + season.season + ' rates only ' + rated +
                     ' of the ' + played + ' Division I teams that have played');
      }
    });
    return reasons;
  }

  /* How old this export is, and whether that should be said out loud.
   *
   * Age is reported even when the manifest is refused, because "we have
   * nothing to show you and the last attempt was two days ago" is a more
   * useful thing to say than either half alone.
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

  /* The one call a page makes before it draws anything.
   *
   * Returns what may be drawn and what must be said. A refused manifest yields
   * ok:false and reasons; it never yields a partial board to render anyway.
   */
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

  /* The URL for a season's payload, version stamp included.
   *
   * Taken from the manifest rather than built from the season number, so a
   * cached file can never be served against a newer manifest.
   */
  function seasonSource(manifest, season, base) {
    if (!manifest || !manifest.seasons) return null;
    var entry = manifest.seasons[String(season)];
    if (typeof entry !== 'string' || !entry) return null;
    return (base || 'data/ncaambb/') + entry;
  }

  function fileSource(manifest, name, base) {
    var files = manifest && manifest.files;
    var entry = files && files[name];
    if (typeof entry !== 'string' || !entry) return null;
    return (base || 'data/ncaambb/') + entry;
  }

  var api = {
    SUPPORTED_SCHEMA_VERSIONS: SUPPORTED_SCHEMA_VERSIONS,
    MIN_DIVISION_ONE_TEAMS: MIN_DIVISION_ONE_TEAMS,
    MIN_RATED_FRACTION: MIN_RATED_FRACTION,
    STALE_AFTER_MINUTES: STALE_AFTER_MINUTES,
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
  root.ncaambbLoader = api;
}(typeof globalThis === 'object' ? globalThis : this));
