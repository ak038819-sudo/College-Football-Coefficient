/* The NFL league's own rules: pure logic, no DOM, no fetch.
 *
 * The league registry — which leagues exist, how a route names one, which
 * favorites key each reads — lives in ui/ncaambb_app.js and the NFL is
 * registered there rather than copied here. Two registries would be two
 * answers to "which key holds my teams", and the wrong one silently renders
 * somebody's NFL teams as college teams. (That file's name is now narrower
 * than its contents; renaming it belongs with the shared league core, which
 * is deliberately deferred until a fourth league is approved.)
 *
 * What IS here is everything the NFL does differently:
 *
 * - A record can have ties. 16 games since 1999 ended level, and "10-6-1" is
 *   not "10-6" with a rounding error.
 * - A week is not a number past the regular season. The archive restarts its
 *   week numbering in January, so a postseason game is named, never numbered.
 * - There is no in-game clock. The export carries finals and schedules, so a
 *   card says Final, Scheduled or Time TBD, and never a zeroed clock.
 * - A franchise is not a team code. A 2015 game says STL and the franchise is
 *   the Rams; the code is shown where the game is shown, and the franchise
 *   name everywhere else.
 */

(function (root) {
  'use strict';

  var LEAGUE = 'nfl';

  var POSTSEASON_LABELS = {
    wildcard: 'Wild Card',
    divisional: 'Divisional',
    conference: 'Conference Championship',
    superbowl: 'Super Bowl'
  };

  var CONFERENCES = ['AFC', 'NFC'];
  var DIVISIONS = ['East', 'North', 'South', 'West'];

  /* The 2002 realignment. Before it the export carries no division, so the
   * standings fall back to one table a conference rather than inventing the
   * eight-division shape backwards over three seasons. */
  var ALIGNMENT_FROM = 2002;

  function seasonLabel(season) {
    var year = Number(season);
    return isFinite(year) && year > 0 ? String(year) : '';
  }

  /* What to call a game's slot in the season. */
  function weekLabel(game) {
    if (!game) return '';
    var type = game.type;
    if (type && type !== 'regular') {
      return POSTSEASON_LABELS[type] || 'Postseason';
    }
    var week = Number(game.week);
    return isFinite(week) && week > 0 ? 'Week ' + week : '';
  }

  function isFiniteNumber(value) {
    return typeof value === 'number' && isFinite(value);
  }

  function timeKnown(game) {
    if (!game) return false;
    if (game.time_tbd) return false;
    return typeof game.at === 'string' && game.at.length > 10;
  }

  function isFinal(game) {
    return !!game && game.home_points !== null
      && game.home_points !== undefined
      && game.away_points !== null && game.away_points !== undefined;
  }

  /* A card's status line. There is no live NFL feed in this export, so a
   * game is finished or it is not; nothing here can say "3rd quarter". */
  function statusLine(game) {
    if (!game) return '';
    if (isFinal(game)) return game.ot ? 'Final / OT' : 'Final';
    if (!timeKnown(game)) return 'Time TBD';
    return 'Scheduled';
  }

  function scoreLine(game) {
    if (!isFinal(game)) return null;
    return game.away_points + ' - ' + game.home_points;
  }

  /* The code to print for a side of a game: what the source called that team
   * in that season, falling back to the franchise's current code. */
  function gameCode(game, side) {
    if (!game) return '';
    var historical = game[side + '_code'];
    if (typeof historical === 'string' && historical) return historical;
    var key = game[side];
    return typeof key === 'string' ? key.split(':').pop() : '';
  }

  /* "12-5" or "10-6-1". A tie is shown only when there is one, because a
   * trailing "-0" on every row is noise, and dropping a real tie is a lie. */
  function recordText(row) {
    if (!row) return '';
    var wins = Number(row.wins) || 0;
    var losses = Number(row.losses) || 0;
    var ties = Number(row.ties) || 0;
    return ties > 0 ? wins + '-' + losses + '-' + ties : wins + '-' + losses;
  }

  /* Win percentage, counting a tie as half a win, which is how the league
   * itself breaks a standings tie. */
  function winPct(row) {
    if (!row) return null;
    var wins = Number(row.wins) || 0;
    var losses = Number(row.losses) || 0;
    var ties = Number(row.ties) || 0;
    var played = wins + losses + ties;
    if (!played) return null;
    return (wins + ties / 2) / played;
  }

  /* Standings, grouped the way the league is organised.
   *
   * Ordered by win percentage and then by rating, which is a stated
   * tiebreak rather than the league's own (head-to-head, then common games,
   * then several more). Calling this the official order would be wrong, so
   * the page labels it as sorted by record.
   */
  function standings(board, season) {
    var rows = (board || []).slice();
    if (Number(season) < ALIGNMENT_FROM) {
      return [{ conference: null, division: null, teams: sortByRecord(rows) }];
    }
    var groups = [];
    CONFERENCES.forEach(function (conference) {
      DIVISIONS.forEach(function (division) {
        var members = rows.filter(function (row) {
          return row.conference === conference && row.division === division;
        });
        if (members.length) {
          groups.push({ conference: conference, division: division,
                        teams: sortByRecord(members) });
        }
      });
    });
    var ungrouped = rows.filter(function (row) {
      return !row.conference || !row.division;
    });
    if (ungrouped.length) {
      groups.push({ conference: null, division: null,
                    teams: sortByRecord(ungrouped) });
    }
    return groups;
  }

  function sortByRecord(rows) {
    return rows.slice().sort(function (a, b) {
      var pa = winPct(a);
      var pb = winPct(b);
      if (pa === null) pa = -1;
      if (pb === null) pb = -1;
      if (pa !== pb) return pb - pa;
      if (a.rating !== b.rating) return b.rating - a.rating;
      return String(a.team).localeCompare(String(b.team));
    });
  }

  function dayOf(game) {
    if (!game) return null;
    if (typeof game.date === 'string' && game.date.length >= 10) {
      return game.date.slice(0, 10);
    }
    return typeof game.at === 'string' && game.at.length >= 10
      ? game.at.slice(0, 10) : null;
  }

  function gamesInWeek(games, week) {
    return (games || []).filter(function (game) {
      return !game.postseason && Number(game.week) === Number(week);
    });
  }

  /* Which week a schedule opens on: the earliest week that still has a game
   * without a final, else the last week played. Never simply the first week,
   * which in December shows a reader three months of history. */
  function openingWeek(games) {
    var unplayed = (games || []).filter(function (game) {
      return !game.postseason && !isFinal(game);
    });
    if (unplayed.length) {
      return unplayed.reduce(function (lowest, game) {
        var week = Number(game.week);
        return isFinite(week) && week < lowest ? week : lowest;
      }, Infinity);
    }
    var played = (games || []).filter(function (game) {
      return !game.postseason && isFiniteNumber(Number(game.week));
    });
    if (!played.length) return 1;
    return played.reduce(function (highest, game) {
      var week = Number(game.week);
      return week > highest ? week : highest;
    }, 1);
  }

  /* Whole numbers at display, one decimal in the data: the rest of Elocast
   * settled this, and a rounded value must never feed the next calculation. */
  function ratingText(value) {
    if (value === null || value === undefined) return '—';
    var number = Number(value);
    return isFinite(number) ? String(Math.round(number)) : '—';
  }

  function searchPlaceholder() {
    return 'Search the NFL…';
  }

  var api = {
    LEAGUE: LEAGUE,
    ALIGNMENT_FROM: ALIGNMENT_FROM,
    CONFERENCES: CONFERENCES,
    DIVISIONS: DIVISIONS,
    POSTSEASON_LABELS: POSTSEASON_LABELS,
    seasonLabel: seasonLabel,
    weekLabel: weekLabel,
    timeKnown: timeKnown,
    isFinal: isFinal,
    statusLine: statusLine,
    scoreLine: scoreLine,
    gameCode: gameCode,
    recordText: recordText,
    winPct: winPct,
    standings: standings,
    sortByRecord: sortByRecord,
    dayOf: dayOf,
    gamesInWeek: gamesInWeek,
    openingWeek: openingWeek,
    ratingText: ratingText,
    searchPlaceholder: searchPlaceholder
  };

  if (typeof module === 'object' && module.exports) {
    module.exports = api;
  }
  root.nflApp = api;
}(typeof globalThis === 'object' ? globalThis : this));
