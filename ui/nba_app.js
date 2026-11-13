/* The NBA's own rules, as pure functions over the export's payloads.
 *
 * Nothing here fetches, touches the DOM, or reads another league's global.
 * ui/nba.html is the page that uses it, ui/nba_loader.js decides whether the
 * export may be drawn at all, and ui/ncaambb_app.js holds what is shared
 * across all four leagues.
 *
 * Four NBA facts live here and nowhere else in Elocast.
 *
 * **A GAME'S DAY IS ITS LOCAL DATE, NOT ITS UTC TIP-OFF.** A 7:30pm tip in
 * Los Angeles is 03:30Z the NEXT day, and almost half an NBA night tips after
 * midnight UTC. The basketball page's `dayOf` reads the UTC timestamp, which
 * is right for a noon Saturday college game and wrong for most of this
 * league. The export carries `date`, the game's own local date, and that is
 * what a scoreboard groups on.
 *
 * **THE NBA CUP FINAL IS NOT IN THE STANDINGS.** It is a real, rated game and
 * the league does not count it, which is why two franchises play 83. The
 * export has already kept it out of the board's won-lost columns; what is
 * here is the label, so a reader who sees the game on a schedule is told why
 * it is not in the record above it.
 *
 * **THERE ARE NO DIVISIONS IN THIS DATA.** The standings feed groups by
 * conference only. Writing the six divisions down here would be inventing a
 * fact the source does not carry, so standings are by conference and say so.
 *
 * **A FRANCHISE IS NOT ITS NAME.** Team 25 is the SuperSonics in 2007 and the
 * Thunder now; team 3 and team 30 are both "Charlotte Hornets" in different
 * decades. Every lookup here is by franchise key.
 */

(function (root) {
  'use strict';

  /* The season types the export emits, and what a reader should be told
   * about each. `regular` is unlabelled: it is the default and a badge on
   * 1,230 of 1,320 rows is noise. */
  var TYPE_LABELS = {
    postseason: 'Playoffs',
    playin: 'Play-In',
    cup_final: 'NBA Cup final'
  };

  /* The note a type needs beyond its name. Only the Cup final does: a reader
   * looking at a schedule that shows 83 games above a record of 82 is owed
   * the reason. */
  var TYPE_NOTES = {
    cup_final: 'A real game, rated here. The NBA does not count it in the ' +
               'standings, which is why two clubs play 83.'
  };

  function isFiniteNumber(value) {
    return typeof value === 'number' && isFinite(value);
  }

  /* `2027` shown as `2026-27`. */
  function seasonLabel(season) {
    var year = Number(season);
    if (!isFinite(year)) return String(season);
    var tail = year % 100;
    return (year - 1) + '-' + (tail < 10 ? '0' + tail : String(tail));
  }

  function typeLabel(game) {
    var type = game && game.type;
    return TYPE_LABELS[type] || null;
  }

  function typeNote(game) {
    var type = game && game.type;
    return TYPE_NOTES[type] || null;
  }

  /* Whether a game counts toward a won-lost record. Mirrors RECORD_TYPES in
   * src/nba/export.py, and the one type it excludes is the point. */
  function countsInRecord(game) {
    return Boolean(game) && game.type === 'regular';
  }

  /* THE DAY A GAME BELONGS TO: its local date.
   *
   * `date` first, always. Falling back to the UTC timestamp's first ten
   * characters is only for a payload with no local date at all, and it is
   * wrong for any game tipping after 7pm Eastern — which is most of them.
   */
  function dayOf(game) {
    if (game && typeof game.date === 'string' && game.date.length >= 10) {
      return game.date.slice(0, 10);
    }
    var at = game && game.at;
    return typeof at === 'string' && at.length >= 10 ? at.slice(0, 10) : null;
  }

  function gamesOn(games, day) {
    return (games || []).filter(function (game) {
      return dayOf(game) === day;
    });
  }

  /* Every day the loaded season has a game on, in order. */
  function daysIn(games) {
    var seen = {};
    (games || []).forEach(function (game) {
      var day = dayOf(game);
      if (day) seen[day] = true;
    });
    return Object.keys(seen).sort();
  }

  /* Whether a game's tip-off time is known.
   *
   * 44% of a newly published NBA schedule carries no time. Rendering an
   * unknown time as midnight would put those games first and read as a real
   * tip, so they get their own bucket at the end of the day.
   */
  function timeKnown(game) {
    if (!game) return false;
    if (game.time_tbd) return false;
    return typeof game.at === 'string' && game.at.length > 10;
  }

  function isFinal(game) {
    return Boolean(game) && isFiniteNumber(game.home_points)
      && isFiniteNumber(game.away_points);
  }

  function scoreLine(game) {
    if (!isFinal(game)) return null;
    return game.away_points + ' – ' + game.home_points;
  }

  /* What to say about a game that has no score: the tip-off time where it is
   * known, and that it is undecided where it is not. Never a zero, and never
   * a time invented from a date. */
  function statusLine(game, formatTime) {
    if (isFinal(game)) return 'Final';
    if (!timeKnown(game)) return 'Time TBD';
    return typeof formatTime === 'function' ? formatTime(game.at) : game.at;
  }

  /* An NBA game cannot be drawn. A level completed score is a fault in the
   * data, so neither side is called the winner. */
  function winnerOf(game) {
    if (!isFinal(game)) return null;
    if (game.home_points === game.away_points) return null;
    return game.home_points > game.away_points ? game.home : game.away;
  }

  function recordText(row) {
    if (!row) return '';
    return (row.wins || 0) + '–' + (row.losses || 0);
  }

  function winPct(row) {
    var wins = (row && row.wins) || 0;
    var losses = (row && row.losses) || 0;
    var played = wins + losses;
    return played ? wins / played : null;
  }

  /* Elo at render, as a whole number.
   *
   * Austin, 2026-10-11: "All Elo should be presented as the nearest whole
   * integer." Rounded HERE and never in the payload, so nothing downstream
   * computes from a rounded value. A rating CHANGE keeps one decimal, which
   * is why it has its own function.
   */
  function ratingText(value) {
    return isFiniteNumber(value) ? String(Math.round(value)) : '';
  }

  function ratingChangeText(value) {
    if (!isFiniteNumber(value)) return '';
    var rounded = Math.round(value * 10) / 10;
    return (rounded > 0 ? '+' : '') + rounded.toFixed(1);
  }

  function percentText(value, digits) {
    if (!isFiniteNumber(value)) return '';
    return (value * 100).toFixed(isFiniteNumber(digits) ? digits : 1) + '%';
  }

  /* STANDINGS BY CONFERENCE, because conference is all this source carries.
   *
   * A franchise whose season row has no conference goes into its own group
   * rather than being assigned one: a season whose standings file never
   * arrived should say so, not be sorted into the East.
   */
  function standings(board) {
    var groups = {};
    (board || []).forEach(function (row) {
      var name = row.conference || null;
      var key = name || '';
      if (!groups[key]) groups[key] = { conference: name, rows: [] };
      groups[key].rows.push(row);
    });
    return Object.keys(groups).sort().map(function (key) {
      var group = groups[key];
      return {
        conference: group.conference,
        rows: sortByRecord(group.rows)
      };
    });
  }

  /* By record, then by rating. Rating is the tiebreak rather than the sort,
   * because this is the standings: a reader comparing it against any other
   * source expects the club with more wins above the club with fewer.
   */
  function sortByRecord(rows) {
    return (rows || []).slice().sort(function (a, b) {
      var pa = winPct(a);
      var pb = winPct(b);
      if (pa === null && pb !== null) return 1;
      if (pb === null && pa !== null) return -1;
      if (pa !== null && pb !== null && pa !== pb) return pb - pa;
      var ra = isFiniteNumber(a.rating) ? a.rating : -Infinity;
      var rb = isFiniteNumber(b.rating) ? b.rating : -Infinity;
      if (ra !== rb) return rb - ra;
      return String(a.team).localeCompare(String(b.team));
    });
  }

  /* Games behind the leader of a group, which is how an NBA standings page
   * is read. Half-games, because a game between two clubs of the group moves
   * both. */
  function gamesBehind(rows) {
    var leader = (rows || [])[0];
    if (!leader) return [];
    var top = ((leader.wins || 0) - (leader.losses || 0)) / 2;
    return rows.map(function (row) {
      var own = ((row.wins || 0) - (row.losses || 0)) / 2;
      var behind = top - own;
      return behind <= 0 ? 0 : Math.round(behind * 10) / 10;
    });
  }

  function sideOf(game, key) {
    if (!game) return null;
    if (game.home === key) return 'home';
    if (game.away === key) return 'away';
    return null;
  }

  function resultLetter(forPoints, againstPoints) {
    if (!isFiniteNumber(forPoints) || !isFiniteNumber(againstPoints)) {
      return null;
    }
    if (forPoints === againstPoints) return null;
    return forPoints > againstPoints ? 'W' : 'L';
  }

  /* One franchise's season, in order: every game it played or will play.
   *
   * Every game, including the Cup final — it happened and a reader wants to
   * see it — with a flag saying whether it counts in the record above.
   */
  function teamSchedule(games, key) {
    var rows = [];
    (games || []).forEach(function (game) {
      var side = sideOf(game, key);
      if (!side) return;
      var other = side === 'home' ? 'away' : 'home';
      var forPoints = game[side + '_points'];
      var againstPoints = game[other + '_points'];
      rows.push({
        game: game,
        day: dayOf(game),
        opponent: game[other],
        home: side === 'home',
        neutral: Boolean(game.neutral),
        points: forPoints,
        against: againstPoints,
        result: resultLetter(forPoints, againstPoints),
        counts: countsInRecord(game),
        label: typeLabel(game),
        elo: game[side + '_elo'],
        opponentElo: game[other + '_elo'],
        // The model's probability from THIS club's side. The payload states
        // the home side's, so an away row is one minus it — and only where
        // there is one to invert.
        chance: isFiniteNumber(game.home_wp)
          ? (side === 'home' ? game.home_wp : 1 - game.home_wp)
          : null,
        estimated: Boolean(game.wp_estimated)
      });
    });
    return rows.sort(function (a, b) {
      if (a.day !== b.day) return String(a.day).localeCompare(String(b.day));
      var ka = timeKnown(a.game) ? 0 : 1;
      var kb = timeKnown(b.game) ? 0 : 1;
      if (ka !== kb) return ka - kb;
      return String(a.game.at || '').localeCompare(String(b.game.at || ''));
    });
  }

  /* A franchise's won-lost from its own schedule rows, counting only what
   * the league counts. The board already carries this; this is what a team
   * page uses when it wants the record of a FILTERED set of games. */
  function scheduleRecord(rows) {
    var wins = 0;
    var losses = 0;
    (rows || []).forEach(function (row) {
      if (!row.counts || !row.result) return;
      if (row.result === 'W') wins += 1;
      else losses += 1;
    });
    return { wins: wins, losses: losses };
  }

  function sortGames(games, mode, favorites) {
    var following = favorites || [];
    var rows = (games || []).slice();
    rows.sort(function (a, b) {
      if (mode === 'featured') {
        var fa = (following.indexOf(a.home) >= 0
                  || following.indexOf(a.away) >= 0) ? 0 : 1;
        var fb = (following.indexOf(b.home) >= 0
                  || following.indexOf(b.away) >= 0) ? 0 : 1;
        if (fa !== fb) return fa - fb;
      }
      var ka = timeKnown(a) ? 0 : 1;
      var kb = timeKnown(b) ? 0 : 1;
      if (ka !== kb) return ka - kb;
      return String(a.at || '').localeCompare(String(b.at || ''));
    });
    return rows;
  }

  /* The conference history file as rows a chart can draw: one series a
   * conference, each point a season. Built here so the page does not
   * reshape the payload twice. */
  function conferenceSeries(history) {
    var rows = (history && history.conferences) || [];
    var series = {};
    rows.forEach(function (row) {
      (row.conferences || []).forEach(function (entry) {
        var name = entry.conference;
        if (!name) return;
        if (!series[name]) series[name] = { conference: name, points: [] };
        series[name].points.push({
          season: row.season,
          label: row.season_label || seasonLabel(row.season),
          mean: entry.mean,
          median: entry.median,
          best: entry.best,
          bestRating: entry.best_rating,
          teams: entry.teams
        });
      });
    });
    return Object.keys(series).sort().map(function (name) {
      series[name].points.sort(function (a, b) { return a.season - b.season; });
      return series[name];
    });
  }

  /* Every season's champion, newest first. A season still being played has
   * none, and is absent rather than listed with a blank. */
  function championList(history) {
    var titles = (history && history.champions) || {};
    return Object.keys(titles).map(Number).filter(function (season) {
      return isFinite(season);
    }).sort(function (a, b) { return b - a; }).map(function (season) {
      var row = titles[String(season)];
      return {
        season: season,
        label: seasonLabel(season),
        champion: row.champion,
        runnerUp: row.runner_up
      };
    });
  }

  /* How many titles each franchise has in the loaded history, for a team
   * page. Keyed by franchise, so a relocation keeps its banners. */
  function titleCount(history, key) {
    return championList(history).filter(function (row) {
      return row.champion === key;
    }).length;
  }

  function brierText(value) {
    return isFiniteNumber(value) ? value.toFixed(5) : '';
  }

  function normalizeName(value) {
    return String(value === null || value === undefined ? '' : value)
      .toLowerCase().normalize('NFKD').replace(/[̀-ͯ]/g, '')
      .replace(/[^a-z0-9]+/g, ' ').trim();
  }

  /* The header box, over the franchise index. By name, nickname, location and
   * abbreviation, because a reader types any of the four. */
  function searchTeams(index, query, limit) {
    var needle = normalizeName(query);
    if (needle.length < 2) return [];
    var found = [];
    Object.keys(index || {}).forEach(function (key) {
      var entry = index[key];
      var haystacks = [entry.name, entry.nickname, entry.location, entry.abbr];
      var best = -1;
      haystacks.forEach(function (text) {
        var position = normalizeName(text).indexOf(needle);
        if (position >= 0 && (best < 0 || position < best)) best = position;
      });
      if (best >= 0) {
        found.push({ key: key, entry: entry, rank: best });
      }
    });
    found.sort(function (a, b) {
      if (a.rank !== b.rank) return a.rank - b.rank;
      return String(a.entry.name).localeCompare(String(b.entry.name));
    });
    return found.slice(0, limit || 10);
  }

  /* ------------------------------------------------------------------
   * The game layer. `src/nba/game_export.py` writes these payloads and the
   * rules for reading them live here, not in the page: a packed row read by
   * index in one place and by another index in a second place is the defect
   * this file exists to make impossible.
   * ---------------------------------------------------------------- */

  // Regulation is 2880 seconds — four twelve-minute periods — and an
  // overtime is 300. Football's 3600 is not a number that belongs anywhere
  // on this page.
  var REGULATION_SECONDS = 2880;
  var OVERTIME_SECONDS = 300;
  var REGULATION_PERIODS = 4;

  /* `1`, `2`, `3`, `4`, then `OT`, `2OT`. The NBA writes a second overtime
   * `2OT`, not `OT2`. */
  function periodLabel(period) {
    var number = Number(period);
    if (!isFiniteNumber(number) || number < 1) return '';
    if (number <= REGULATION_PERIODS) return String(number);
    var overtime = number - REGULATION_PERIODS;
    return overtime === 1 ? 'OT' : overtime + 'OT';
  }

  /* How long a game of this many periods ran, in seconds. Used as the
   * x-axis of the curve, so an overtime game's chart is wider than a
   * regulation one rather than compressing five periods into four. */
  function gameLength(periods) {
    var count = Number(periods);
    if (!isFiniteNumber(count) || count <= REGULATION_PERIODS) {
      return REGULATION_SECONDS;
    }
    return REGULATION_SECONDS
      + (count - REGULATION_PERIODS) * OVERTIME_SECONDS;
  }

  /* The margin curve as objects. `entry.c` is four PARALLEL arrays —
   * elapsed, home, away, win probability in per mille — because the same
   * points as objects would repeat four key names a million times across the
   * archive. A probability of null is a state the model cannot price, which
   * is every state inside overtime. */
  function unpackCurve(entry) {
    var packed = entry && entry.c;
    if (!Array.isArray(packed) || packed.length < 3) return [];
    var elapsed = packed[0] || [];
    var home = packed[1] || [];
    var away = packed[2] || [];
    var chance = packed[3] || [];
    var out = [];
    for (var i = 0; i < elapsed.length; i += 1) {
      out.push({
        elapsed: elapsed[i],
        home: home[i],
        away: away[i],
        margin: home[i] - away[i],
        wp: isFiniteNumber(chance[i]) ? chance[i] : null
      });
    }
    return out;
  }

  /* The shape row as a named object. `payload.shape_fields` states the
   * order, so the page never hard-codes an index: the export may add a
   * field and this keeps reading the right ones. */
  function unpackShape(entry, fields) {
    var packed = entry && entry.sh;
    if (!Array.isArray(packed) || !Array.isArray(fields)) return null;
    var out = {};
    fields.forEach(function (name, index) {
      out[name] = packed[index];
    });
    return out;
  }

  /* The line score as `[{period, home, away}, ...]`. */
  function lineScore(entry) {
    var packed = entry && entry.p;
    if (!Array.isArray(packed)) return [];
    return packed.map(function (pair, index) {
      return { period: index + 1, home: pair[0], away: pair[1] };
    });
  }

  /* The key plays as objects, with the side resolved to the game's own team
   * key and the reason to its word. */
  function unpackKeyPlays(entry, game, reasons) {
    var packed = entry && entry.k;
    if (!Array.isArray(packed)) return [];
    var words = Array.isArray(reasons) ? reasons : [];
    return packed.map(function (row) {
      return {
        side: row[0],
        team: row[0] === 1 ? (game && game.home)
          : (row[0] === 0 ? (game && game.away) : null),
        period: row[1],
        clock: row[2],
        points: row[3],
        scorer: row[4],
        description: row[5],
        home: row[6],
        away: row[7],
        reason: words[row[8]] || null,
        kind: row[9]
      };
    });
  }

  /* The trace the chart draws: tip-off, every priced state, and the result.
   *
   * `at` is a fraction of the GAME's own length, so an overtime game's
   * regulation occupies the first 90% of the chart and the overtime the
   * rest. The curve stops where the model does: an overtime state carries no
   * probability, so the line ends at the end of regulation and the page says
   * why rather than drawing a flat certainty across five minutes the model
   * never looked at. */
  function winProbabilityTrace(entry, periods) {
    var length = gameLength(periods);
    var points = [];
    if (entry && isFiniteNumber(entry.wp)) {
      points.push({ at: 0, value: entry.wp / 1000, label: 'Tip-off' });
    }
    unpackCurve(entry).forEach(function (point) {
      if (point.wp === null || !isFiniteNumber(point.elapsed)) return;
      points.push({
        at: point.elapsed / length,
        value: point.wp / 1000,
        label: point.home + '-' + point.away
      });
    });
    return points;
  }

  /* An SVG path through a trace, in a box the caller scales.
   *
   * Straight segments, not a curve: the model says nothing about what
   * happened between two states, and a smoothed line would say it did. */
  function tracePath(points, width, height) {
    if (!points || points.length < 2) return '';
    return points.map(function (point, index) {
      var x = (point.at * width).toFixed(2);
      var y = ((1 - point.value) * height).toFixed(2);
      return (index ? 'L' : 'M') + x + ' ' + y;
    }).join(' ');
  }

  /* A path through the margin, scaled to the biggest lead either side held.
   * Returns null where there is nothing to draw. */
  function marginPath(points, width, height) {
    if (!points || points.length < 2) return null;
    var peak = 0;
    points.forEach(function (point) {
      if (isFiniteNumber(point.margin) && Math.abs(point.margin) > peak) {
        peak = Math.abs(point.margin);
      }
    });
    if (!peak) return null;
    var last = points[points.length - 1].elapsed || 1;
    return points.map(function (point, index) {
      var x = ((point.elapsed / last) * width).toFixed(2);
      var y = ((0.5 - (point.margin / (peak * 2))) * height).toFixed(2);
      return (index ? 'L' : 'M') + x + ' ' + y;
    }).join(' ');
  }

  /* Whether the reduction adds up to the final score the archive states.
   *
   * `ok` is 1 when it does, 0 when it does not and null when there was
   * nothing to check against — a game not yet played. A page must not read
   * null as a failure: nothing disagreed, there was nothing to disagree
   * with. */
  function detailReconciles(entry) {
    if (!entry || entry.ok === null || entry.ok === undefined) return true;
    return entry.ok === 1;
  }

  /* Whether this game has states INSIDE OVERTIME that the model cannot
   * price.
   *
   * Not simply "a point with no probability": the last point of every game
   * is the buzzer, where regulation seconds remaining is 0 and the model
   * would answer 1 or 0 by the score alone, so every game has one. Saying
   * "the line stops at the end of regulation" on a game that ended in
   * regulation is a sentence about nothing, and it was on the first game
   * drawn in a browser. */
  function hasUnpricedStates(entry) {
    return unpackCurve(entry).some(function (point) {
      return point.wp === null && point.elapsed > REGULATION_SECONDS;
    });
  }

  /* `0:09.5 Q4`, from a key play's period and clock.
   *
   * The seconds are padded. The feed writes them unpadded — `0:9.5` — and a
   * column of times where one row reads 0:9.5 and the next 0:41.1 does not
   * sort by eye. The tenths are kept, because under a minute is where the
   * NBA itself shows them. */
  function playTime(play) {
    var label = periodLabel(play && play.period);
    if (!label) return '';
    var clock = padClock(play && play.clock);
    return (clock ? clock + ' ' : '') + (label.length > 1 ? label : 'Q' + label);
  }

  function padClock(clock) {
    if (typeof clock !== 'string') return '';
    var parts = clock.split(':');
    if (parts.length !== 2) return clock;
    var seconds = parts[1];
    var whole = seconds.split('.')[0];
    if (whole.length !== 1) return clock;
    return parts[0] + ':0' + seconds;
  }

  /* `6:42` of game time as a readable duration, for "time in front". */
  function durationText(seconds) {
    var value = Number(seconds);
    if (!isFiniteNumber(value) || value < 0) return '';
    var minutes = Math.floor(value / 60);
    var rest = Math.round(value % 60);
    return minutes + ':' + (rest < 10 ? '0' + rest : String(rest));
  }

  /* ---------------------------------------------------------------------
   * THE PEOPLE LAYER (N4)
   *
   * The payload is positional, because every one of these arrays appears
   * 2,635 times: a season row and a game row both carry the counted columns
   * in the order `src/nba/people_export.COUNTS` states, and these unpackers
   * are the only place that order is read. A page that indexed into them
   * itself would be a page that breaks the day a column is added.
   * ------------------------------------------------------------------- */

  var COUNTS = ['points', 'rebounds', 'offensive_rebounds',
    'defensive_rebounds', 'assists', 'steals', 'blocks', 'turnovers',
    'fouls', 'field_goals_made', 'field_goals_attempted', 'threes_made',
    'threes_attempted', 'free_throws_made', 'free_throws_attempted',
    'plus_minus'];

  /* The totals a career or season table prints, in the order it prints
   * them, with the label each one carries. */
  var STAT_COLUMNS = [
    ['points', 'PTS'], ['rebounds', 'REB'], ['assists', 'AST'],
    ['steals', 'STL'], ['blocks', 'BLK'], ['turnovers', 'TO'],
    ['fouls', 'PF']
  ];

  function unpackSeason(row) {
    if (!Array.isArray(row)) return null;
    var out = { season: row[0], type: row[1], team: row[2], games: row[3],
      starts: row[4], minutes: row[5] };
    COUNTS.forEach(function (name, index) { out[name] = row[6 + index]; });
    out.feed_games = row[6 + COUNTS.length];
    out.matches_feed = row[7 + COUNTS.length];
    return out;
  }

  function unpackGame(row) {
    if (!Array.isArray(row)) return null;
    var out = { game: row[0], date: row[1], team: row[2], opponent: row[3],
      home: row[4] === 1, started: row[5] === 1, minutes: row[6] };
    COUNTS.forEach(function (name, index) { out[name] = row[7 + index]; });
    return out;
  }

  /* Every season row a player's file carries, newest first, which is the
   * order a career table reads in. */
  function playerSeasons(payload) {
    var rows = (payload && payload.s) || [];
    return rows.map(unpackSeason).filter(Boolean).sort(function (one, two) {
      return two.season - one.season || one.type - two.type;
    });
  }

  /* His game log as `[{season, type, games: [...]}, ...]`, newest season
   * first and, within a season, the games in the order they were played. */
  function playerLog(payload) {
    var groups = (payload && payload.g) || [];
    return groups.map(function (group) {
      return { season: group[0], type: group[1],
        games: (group[2] || []).map(unpackGame).filter(Boolean) };
    }).sort(function (one, two) {
      return two.season - one.season || one.type - two.type;
    });
  }

  /* A career total over the rows of ONE season type. Nothing is summed
   * across a split record: a player's own file holds only his own id's
   * seasons, and the other id's are a link, not an addend. */
  function careerTotals(rows, type) {
    var totals = { games: 0, starts: 0, minutes: 0, seasons: 0 };
    COUNTS.forEach(function (name) { totals[name] = 0; });
    var known = {};
    (rows || []).forEach(function (row) {
      if (type !== undefined && type !== null && row.type !== type) return;
      totals.games += row.games || 0;
      totals.starts += row.starts || 0;
      totals.minutes += row.minutes || 0;
      known[row.season] = true;
      COUNTS.forEach(function (name) {
        if (isFiniteNumber(row[name])) totals[name] += row[name];
      });
    });
    totals.seasons = Object.keys(known).length;
    return totals;
  }

  /* A per-game average, to one decimal, or null where there are no games.
   * Zero games must not read as 0.0 points a game. */
  function perGame(total, games) {
    if (!isFiniteNumber(total) || !isFiniteNumber(games) || games <= 0) {
      return null;
    }
    return Math.round((total / games) * 10) / 10;
  }

  function perGameText(total, games) {
    var value = perGame(total, games);
    return value === null ? '—' : value.toFixed(1);
  }

  /* `11-20` for made and attempted, and `—` where the archive has neither.
   * A made-attempted pair with no attempts is printed, because 0-0 is a
   * fact about a game a man played. */
  function shotLine(made, attempted) {
    if (!isFiniteNumber(made) && !isFiniteNumber(attempted)) return '—';
    return String(made || 0) + '-' + String(attempted || 0);
  }

  /* A shooting percentage from the two counts, never from a stored
   * percentage: the export deliberately carries neither, so there is one
   * place the quotient is taken. */
  function shotPct(made, attempted) {
    if (!isFiniteNumber(made) || !isFiniteNumber(attempted)
        || attempted <= 0) {
      return null;
    }
    return made / attempted;
  }

  function shotPctText(made, attempted) {
    var value = shotPct(made, attempted);
    return value === null ? '—' : (value * 100).toFixed(1) + '%';
  }

  /* `6' 9"` from inches. */
  function heightText(inches) {
    if (!isFiniteNumber(inches) || inches <= 0) return null;
    return Math.floor(inches / 12) + "' " + (inches % 12) + '"';
  }

  /* The birthplace as one line, with the parts the archive has. */
  function birthplaceText(parts) {
    if (!Array.isArray(parts)) return null;
    var kept = parts.filter(function (part) {
      return typeof part === 'string' && part.trim();
    });
    return kept.length ? kept.join(', ') : null;
  }

  /* `1st round, 1st pick, 2003`, or null for a man the feed has no draft
   * for — which is 16-26% of players a season, undrafted men among them, so
   * the page says nothing rather than "undrafted". */
  function draftText(draft) {
    if (!Array.isArray(draft) || !isFiniteNumber(draft[0])) return null;
    var parts = [];
    if (isFiniteNumber(draft[1])) parts.push(ordinal(draft[1]) + ' round');
    if (isFiniteNumber(draft[2])) parts.push(ordinal(draft[2]) + ' pick');
    parts.push(String(draft[0]));
    return parts.join(', ');
  }

  function ordinal(value) {
    var n = Math.abs(Math.round(value));
    var last = n % 100;
    if (last >= 11 && last <= 13) return n + 'th';
    if (n % 10 === 1) return n + 'st';
    if (n % 10 === 2) return n + 'nd';
    if (n % 10 === 3) return n + 'rd';
    return n + 'th';
  }

  /* The seasons the ARCHIVE has him playing, which for a man who debuted in
   * 1996 is 2002 onwards. The wording is the page's business; what this
   * returns is null where he never played a minute in it. */
  function archiveSpan(bio) {
    if (!bio || !isFiniteNumber(bio.first) || !isFiniteNumber(bio.last)) {
      return null;
    }
    return { first: bio.first, last: bio.last };
  }

  /* The players an index search matches, by name. Two characters at least,
   * for the reason the club search has the same floor: one letter matches a
   * third of the league. */
  function searchPeople(index, query, limit) {
    var needle = normalizeName(query);
    if (needle.length < 2) return [];
    var rows = (index && index.players) || [];
    var found = [];
    for (var i = 0; i < rows.length; i += 1) {
      if (normalizeName(rows[i][1]).indexOf(needle) >= 0) {
        found.push(indexEntry(rows[i]));
      }
    }
    found.sort(function (one, two) {
      // The men with the most minutes on the floor first: a reader typing
      // "james" wants LeBron before a man who played four games in 2004.
      return (two.points || 0) - (one.points || 0);
    });
    return limit ? found.slice(0, limit) : found;
  }

  function indexEntry(row) {
    if (!Array.isArray(row)) return null;
    return { id: row[0], name: row[1], position: row[2], first: row[3],
      last: row[4], games: row[5], points: row[6] };
  }

  /* The index as entries, for a list that is not a search: the most
   * prolific scorers the archive carries, which is what the People tab
   * shows a reader who has typed nothing. */
  function topScorers(index, limit) {
    var rows = ((index && index.players) || []).map(indexEntry)
      .filter(Boolean);
    rows.sort(function (one, two) {
      return (two.points || 0) - (one.points || 0);
    });
    return limit ? rows.slice(0, limit) : rows;
  }

  /* ------------------------------------------------- team statistics
   *
   * The reader's half of src/nba/team_metrics.py. Nothing here computes a
   * rate: the export carries them at three decimals and these functions
   * decide how many of those a reader sees. An index is drawn to one
   * decimal and a grade to two, which is the precision each was quoted at
   * in the trial these came from.
   *
   * AN ABSENT NUMBER DRAWS AS A DASH, NEVER AS ZERO OR 100. A club with no
   * measured game has no estimate, and "exactly league average" is a
   * measurement somebody could act on. Null is checked BEFORE any
   * coercion, because Number(null) is 0 and isFinite(0) is true — the trap
   * that has read a missing clock as 0:00 three times in this repository.
   */
  var NO_VALUE = '—';

  function indexText(value) {
    if (value === null || value === undefined) return NO_VALUE;
    return isFiniteNumber(value) ? value.toFixed(1) : NO_VALUE;
  }

  function netOpportunityText(value) {
    if (value === null || value === undefined) return NO_VALUE;
    if (!isFiniteNumber(value)) return NO_VALUE;
    var rounded = Math.round(value * 100) / 100;
    return (rounded > 0 ? '+' : '') + rounded.toFixed(2);
  }

  /* Whether a season's payload carries team statistics worth drawing.
   *
   * An export written before `src/nba/team_box.py` ran has no section at
   * all, and a season whose feed measured nothing has one that says zero.
   * Both mean the same thing to a page: draw no columns rather than a
   * column of dashes. */
  function teamStatsAvailable(payload) {
    var stats = payload && payload.team_stats;
    if (!stats || typeof stats !== 'object') return false;
    return isFiniteNumber(stats.measured_games) && stats.measured_games > 0;
  }

  /* What a season's numbers were measured from, where that is not the
   * modern feed. Empty for a season measured the usual way, so a page adds
   * no note at all rather than a note saying nothing happened. */
  function turnoverBasisNote(payload) {
    var stats = payload && payload.team_stats;
    var basis = stats && stats.turnover_basis;
    if (!Array.isArray(basis) || basis.indexOf('team-only') < 0) return '';
    return 'This season\u2019s turnovers are the narrower count: the feed\u2019s '
      + 'combined total is either absent or the team\u2019s own count doubled, so '
      + 'possessions here are estimated without team turnovers. Rates are '
      + 'comparable within the season and read a little high against a '
      + 'season measured from the full count.';
  }

  /* One side of a game's opportunity numbers, or null.
   *
   * `role` is 'home' or 'away', which is what the month file keys them by:
   * the season payload already tells the page which club is which, so the
   * entry does not repeat two club keys for every game in the archive. */
  function opportunityOf(entry, role) {
    var stats = entry && entry.ts;
    var side = stats && stats[role];
    if (!side || typeof side !== 'object') return null;
    return {
      possessions: isFiniteNumber(stats.p) ? stats.p : null,
      pxp: side.pxp === undefined ? null : side.pxp,
      rate: side.rate === undefined ? null : side.rate,
      expected: side.exp === undefined ? null : side.exp,
      residual: side.res === undefined ? null : side.res,
      net: side.net === undefined ? null : side.net,
      chances: side.chances === undefined ? null : side.chances,
      offensiveRebounds: side.orb === undefined ? null : side.orb,
      turnovers: side.tov === undefined ? null : side.tov,
      priorGames: isFiniteNumber(side.prior_games) ? side.prior_games : null
    };
  }


  /* --------------------------------------------- the player rating
   *
   * The reader's half of src/nba/player_elo.py. NOTHING HERE RATES
   * ANYBODY: the replay runs at build time and these functions name the
   * fields it wrote and decide how many decimals a reader sees. An absent
   * number draws as a dash and never as zero, and null is checked BEFORE
   * any coercion, for the reason the team statistics give above.
   */

  var RATING_STATUS = { rated: 'rated', provisional: 'provisional',
                        unavailable: 'unavailable' };

  /* One season's rating off a player payload's `r`:
   * `[season, elo, impact, off, def, min, games, poss, status]`. */
  function ratingEntry(row) {
    if (!Array.isArray(row)) return null;
    return { season: row[0], elo: row[1], impact: row[2], offense: row[3],
      defense: row[4], minutes: row[5], games: row[6], possessions: row[7],
      status: row[8] };
  }

  /* A man's seasons of rating, NEWEST FIRST, or an empty list where the
   * replay has never reached him. A payload written before schema 5 has no
   * `r` at all, which is an export without the layer rather than a player
   * without a rating: both draw nothing, and the page says which. */
  function playerRatings(payload) {
    var rows = (payload && payload.r) || [];
    return rows.map(ratingEntry).filter(Boolean).sort(function (one, two) {
      return two.season - one.season;
    });
  }

  /* One graded game off a payload's `rg`: `[game, date, grade, exposure,
   * before, after, change]`. `before` IS NULL FOR A FIRST GRADE and is
   * left null here: 1500 there would draw a man arriving at exactly
   * average, which never happened. */
  function ratingGame(row) {
    if (!Array.isArray(row)) return null;
    return { game: row[0], date: row[1], grade: row[2], exposure: row[3],
      before: row[4], after: row[5], change: row[6] };
  }

  /* The replay as `[{season, games}]`, newest season first and each
   * season's games oldest first, which is the order a rating moved in. */
  function ratingLog(payload) {
    var groups = (payload && payload.rg) || [];
    return groups.map(function (pair) {
      if (!Array.isArray(pair)) return null;
      return { season: pair[0],
        games: (pair[1] || []).map(ratingGame).filter(Boolean) };
    }).filter(function (group) {
      return group && group.games.length;
    }).sort(function (one, two) { return two.season - one.season; });
  }

  /* A season payload's Player Elo board, or null. */
  function playerBoard(payload) {
    var board = payload && payload.player_elo;
    if (!board || !Array.isArray(board.players) || !board.players.length) {
      return null;
    }
    return board;
  }

  /* The board's players in rating order, highest first. The export writes
   * them in that order already; sorting here keeps the page honest about
   * what it claims rather than trusting a file's order.
   *
   * `status` SPLITS THE BOARD and is not a label on one list: the rated men
   * are ranked and the provisional ones are not, because a man on forty
   * minutes placed 7th in the league is a claim the measurement cannot
   * carry. Pass a status to get one side of it.
   */
  function playerBoardRows(board, status, by) {
    var rows = ((board && board.players) || []).filter(function (row) {
      return !status || row.status === status;
    });
    var key = by === 'minutes' ? 'season_minutes' : 'elo';
    rows.sort(function (one, two) {
      return (two[key] || 0) - (one[key] || 0);
    });
    return rows;
  }

  /* An index's players by id, for a board that carries ids and no names. */
  function peopleById(index) {
    var map = {};
    ((index && index.players) || []).forEach(function (row) {
      var entry = indexEntry(row);
      if (entry) map[String(entry.id)] = entry;
    });
    return map;
  }

  /* A rate per 100 possessions, to two decimals and signed, because the
   * sign IS the measurement: -1.20 is a man his club was worse with. */
  function impactText(value) {
    if (value === null || value === undefined) return '—';
    var number = Number(value);
    if (!isFinite(number)) return '—';
    return (number > 0 ? '+' : '') + number.toFixed(2);
  }

  /* A player Elo as a whole number, the rule the team ratings follow. */
  function playerEloText(value) {
    if (value === null || value === undefined) return '—';
    var number = Number(value);
    return isFinite(number) ? String(Math.round(number)) : '—';
  }

  var api = {
    // Exported because a page drawing these numbers needs the same guard
    // the module uses: `Number.isFinite(Number(x))` is TRUE for null, and
    // reading a missing value as zero is the trap this repository has hit
    // three times.
    isFiniteNumber: isFiniteNumber,
    NO_VALUE: NO_VALUE,
    indexText: indexText,
    netOpportunityText: netOpportunityText,
    teamStatsAvailable: teamStatsAvailable,
    turnoverBasisNote: turnoverBasisNote,
    opportunityOf: opportunityOf,
    REGULATION_SECONDS: REGULATION_SECONDS,
    OVERTIME_SECONDS: OVERTIME_SECONDS,
    REGULATION_PERIODS: REGULATION_PERIODS,
    periodLabel: periodLabel,
    gameLength: gameLength,
    unpackCurve: unpackCurve,
    unpackShape: unpackShape,
    lineScore: lineScore,
    unpackKeyPlays: unpackKeyPlays,
    winProbabilityTrace: winProbabilityTrace,
    tracePath: tracePath,
    marginPath: marginPath,
    detailReconciles: detailReconciles,
    hasUnpricedStates: hasUnpricedStates,
    playTime: playTime,
    padClock: padClock,
    durationText: durationText,
    TYPE_LABELS: TYPE_LABELS,
    TYPE_NOTES: TYPE_NOTES,
    seasonLabel: seasonLabel,
    typeLabel: typeLabel,
    typeNote: typeNote,
    countsInRecord: countsInRecord,
    dayOf: dayOf,
    gamesOn: gamesOn,
    daysIn: daysIn,
    timeKnown: timeKnown,
    isFinal: isFinal,
    scoreLine: scoreLine,
    statusLine: statusLine,
    winnerOf: winnerOf,
    recordText: recordText,
    winPct: winPct,
    ratingText: ratingText,
    ratingChangeText: ratingChangeText,
    percentText: percentText,
    standings: standings,
    sortByRecord: sortByRecord,
    gamesBehind: gamesBehind,
    sideOf: sideOf,
    resultLetter: resultLetter,
    teamSchedule: teamSchedule,
    scheduleRecord: scheduleRecord,
    sortGames: sortGames,
    conferenceSeries: conferenceSeries,
    championList: championList,
    titleCount: titleCount,
    brierText: brierText,
    normalizeName: normalizeName,
    searchTeams: searchTeams,
    COUNTS: COUNTS,
    STAT_COLUMNS: STAT_COLUMNS,
    unpackSeason: unpackSeason,
    unpackGame: unpackGame,
    playerSeasons: playerSeasons,
    playerLog: playerLog,
    careerTotals: careerTotals,
    perGame: perGame,
    perGameText: perGameText,
    shotLine: shotLine,
    shotPct: shotPct,
    shotPctText: shotPctText,
    heightText: heightText,
    birthplaceText: birthplaceText,
    draftText: draftText,
    ordinal: ordinal,
    archiveSpan: archiveSpan,
    searchPeople: searchPeople,
    indexEntry: indexEntry,
    topScorers: topScorers,
    peopleById: peopleById,
    RATING_STATUS: RATING_STATUS,
    ratingEntry: ratingEntry,
    playerRatings: playerRatings,
    ratingGame: ratingGame,
    ratingLog: ratingLog,
    playerBoard: playerBoard,
    playerBoardRows: playerBoardRows,
    impactText: impactText,
    playerEloText: playerEloText
  };

  if (typeof module === 'object' && module.exports) {
    module.exports = api;
  }
  root.nbaApp = api;
}(typeof globalThis === 'object' ? globalThis : this));
