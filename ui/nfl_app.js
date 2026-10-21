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

  /* ---- game context: the archive always carried this and schema 1 of the
   * export threw it away. Every reader below treats ABSENT as absent: a dome
   * game has no temperature and a 1999 game may have no posted line, and
   * neither is a zero. `Number(null)` is 0 and `Number.isFinite(0)` is true,
   * which is the trap that once read a missing clock as 0:00, so each of
   * these checks for null BEFORE it coerces. */

  function present(value) {
    return value !== null && value !== undefined && value !== '';
  }

  function number(value) {
    if (!present(value)) return null;
    var parsed = Number(value);
    return isFinite(parsed) ? parsed : null;
  }

  /* "KC -3", "BAL -3.5", "Pick'em", or null where no line was posted.
   * `game.spread` is the HOME team's expected margin, positive when the home
   * team is favoured — the convention measured against this archive, not
   * assumed. */
  function spreadText(game, nameFor) {
    var spread = number(game && game.spread);
    if (spread === null) return null;
    var label = function (key) {
      return typeof nameFor === 'function' ? nameFor(key) : gameCodeOf(key);
    };
    if (spread === 0) return 'Pick\u2019em';
    var side = spread > 0 ? game.home : game.away;
    return label(side) + ' \u2212' + Math.abs(spread);
  }

  function gameCodeOf(key) {
    return typeof key === 'string' ? key.split(':').pop() : '';
  }

  /* American odds as a probability, overround removed proportionally. The
   * same arithmetic as devig() in src/nfl/market.py, and for the same
   * reason: a moneyline IS a probability and a spread is not. Returns null
   * unless BOTH prices are there, because one price carries the vig with it
   * and there is nothing to divide it out by. */
  function impliedProbability(moneyline) {
    var price = number(moneyline);
    if (price === null) return null;
    if (price > -100 && price < 100) return null;
    return price < 0 ? -price / (-price + 100) : 100 / (price + 100);
  }

  function marketProbability(game) {
    var home = impliedProbability(game && game.home_ml);
    var away = impliedProbability(game && game.away_ml);
    if (home === null || away === null) return null;
    var total = home + away;
    return total > 0 ? home / total : null;
  }

  var ROOF_LABELS = {
    outdoors: 'Outdoors', dome: 'Dome', closed: 'Roof closed',
    open: 'Roof open'
  };

  /* The weather line. A roof with no temperature says so; an outdoor game
   * with no reading says nothing rather than implying a mild day. */
  function weatherText(game) {
    if (!game) return null;
    var parts = [];
    if (present(game.roof)) {
      parts.push(ROOF_LABELS[game.roof] || game.roof);
    }
    var temp = number(game.temp);
    if (temp !== null) parts.push(temp + '\u00b0F');
    var wind = number(game.wind);
    if (wind !== null) parts.push(wind + ' mph wind');
    return parts.length ? parts.join(' \u00b7 ') : null;
  }

  /* "Rest 7 and 7", or the side with the advantage named. A bye shows as 14
   * because that is what the archive counted, not as "bye". */
  function restText(game) {
    var home = number(game && game.home_rest);
    var away = number(game && game.away_rest);
    if (home === null || away === null) return null;
    if (home === away) return 'Both on ' + home + " days' rest";
    return 'Rest ' + away + ' away, ' + home + ' home';
  }

  function contextLines(game, nameFor) {
    return [weatherText(game), restText(game), spreadText(game, nameFor),
            present(game && game.total_line)
              ? 'O/U ' + number(game.total_line) : null,
            present(game && game.surface) ? game.surface : null]
      .filter(Boolean);
  }

  /* ---- coaches. Wins above expectation is actual wins minus the sum of the
   * model's PREGAME win probability over the same games, a tie counting half
   * on both sides. It says the model was wrong in a team's favour. It does
   * NOT say the coach is good, and the page prints that sentence. */

  function signedWins(value) {
    var parsed = number(value);
    if (parsed === null) return '\u2014';
    var rounded = Math.round(parsed * 10) / 10;
    return (rounded > 0 ? '+' : rounded < 0 ? '\u2212' : '')
      + Math.abs(rounded).toFixed(1);
  }

  function coachRecord(row) {
    if (!row) return '';
    var wins = Number(row.wins) || 0;
    var losses = Number(row.losses) || 0;
    var ties = Number(row.ties) || 0;
    return ties > 0 ? wins + '-' + losses + '-' + ties : wins + '-' + losses;
  }

  function coachSpan(row) {
    if (!row) return '';
    var first = number(row.first_season);
    var last = number(row.last_season);
    if (first === null) return '';
    return first === last ? String(first) : first + '\u2013' + last;
  }

  /* ---- the playoff field. The seeding is computed in src/nfl/playoff.py and
   * exported; nothing here re-derives it, so the page and the data cannot
   * disagree about who is the 7 seed. */

  function playoffConference(field, conference) {
    var groups = (field && field.conferences) || [];
    for (var index = 0; index < groups.length; index += 1) {
      if (groups[index].conference === conference) return groups[index];
    }
    return null;
  }

  /* "division winner · bye", or "division lead" while the season is still
   * being played: a team in front of its division in week 6 has not won
   * anything, and a field computed in October is the seeding as things
   * stand. The numbers are the same either way; only the claim changes. */
  function seedNote(seed, settled) {
    if (!seed) return '';
    var parts = [];
    if (seed.division_winner) {
      parts.push(settled === false ? 'division lead' : 'division winner');
    }
    if (seed.bye) parts.push('bye');
    return parts.join(' \u00b7 ');
  }

  /* ---- the track record. A Brier is printed to five places because the
   * differences that matter here are in the fourth: 0.21893 against the
   * market's 0.21101 is the whole story and 0.22 against 0.21 is not. */
  function brierText(value) {
    var parsed = number(value);
    return parsed === null ? '\u2014' : parsed.toFixed(5);
  }

  function percentText(value, digits) {
    var parsed = number(value);
    if (parsed === null) return '\u2014';
    return (parsed * 100).toFixed(digits === undefined ? 1 : digits) + '%';
  }

  /* Who did better on the games BOTH were measured on, and by how much.
   * Never the model's Brier over all its games against the market's over the
   * priced subset: that comparison is between two different sets of games. */
  function marketVerdict(row) {
    var model = number(row && row.model_brier_priced);
    var market = number(row && row.market_brier);
    if (model === null || market === null) return null;
    var gap = market - model;
    return {
      games: number(row.priced_games),
      gap: gap,
      better: gap > 0 ? 'model' : gap < 0 ? 'market' : 'level'
    };
  }

  /* An against-the-spread record, and the fact that it is not an edge. A
   * bettor needs about 52.4% to clear a standard \u2212110 vig, so a 51%
   * record is a loss and the page must not let it read as a win. */
  var ATS_BREAK_EVEN = 0.524;

  function atsRate(row) {
    var wins = Number(row && row.ats_wins) || 0;
    var losses = Number(row && row.ats_losses) || 0;
    var decided = wins + losses;
    return decided ? wins / decided : null;
  }

  function atsText(row) {
    var wins = Number(row && row.ats_wins) || 0;
    var losses = Number(row && row.ats_losses) || 0;
    var pushes = Number(row && row.ats_pushes) || 0;
    if (!(wins + losses + pushes)) return '\u2014';
    return wins + '-' + losses + (pushes ? '-' + pushes : '');
  }

  function atsBeatsVig(row) {
    var rate = atsRate(row);
    return rate === null ? null : rate > ATS_BREAK_EVEN;
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
    searchPlaceholder: searchPlaceholder,
    ATS_BREAK_EVEN: ATS_BREAK_EVEN,
    ROOF_LABELS: ROOF_LABELS,
    impliedProbability: impliedProbability,
    marketProbability: marketProbability,
    spreadText: spreadText,
    weatherText: weatherText,
    restText: restText,
    contextLines: contextLines,
    signedWins: signedWins,
    coachRecord: coachRecord,
    coachSpan: coachSpan,
    playoffConference: playoffConference,
    seedNote: seedNote,
    brierText: brierText,
    percentText: percentText,
    marketVerdict: marketVerdict,
    atsRate: atsRate,
    atsText: atsText,
    atsBeatsVig: atsBeatsVig
  };

  if (typeof module === 'object' && module.exports) {
    module.exports = api;
  }
  root.nflApp = api;
}(typeof globalThis === 'object' ? globalThis : this));
