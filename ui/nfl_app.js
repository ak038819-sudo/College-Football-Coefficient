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

  /* ---------------------------------------------------------------------
   * The people layer. Everything below reads the packed files
   * src/nfl/people_export.py writes, so the two halves of each format live
   * one file apart and a test drives both.
   * ------------------------------------------------------------------- */

  /* Labels in the order the export's STAT_TABLES lists the stats. A stat
   * with no label here prints its own name rather than disappearing. */
  var STAT_LABELS = {
    completions: 'Cmp', attempts: 'Att', passing_yards: 'Yds',
    passing_tds: 'TD', passing_interceptions: 'Int', sacks_suffered: 'Sk',
    sack_yards_lost: 'Sk yds', passing_first_downs: '1st',
    passing_epa: 'EPA', passing_cpoe: 'CPOE',
    carries: 'Car', rushing_yards: 'Yds', rushing_tds: 'TD',
    rushing_first_downs: '1st', rushing_fumbles_lost: 'Fum lost',
    rushing_epa: 'EPA',
    targets: 'Tgt', receptions: 'Rec', receiving_yards: 'Yds',
    receiving_tds: 'TD', receiving_yards_after_catch: 'YAC',
    receiving_first_downs: '1st', receiving_epa: 'EPA',
    target_share: 'Tgt share',
    def_tackles_solo: 'Solo', def_tackle_assists: 'Ast',
    def_tackles_for_loss: 'TFL', def_sacks: 'Sacks', def_qb_hits: 'QB hits',
    def_interceptions: 'Int', def_interception_yards: 'Int yds',
    def_pass_defended: 'PD', def_fumbles_forced: 'FF',
    fumble_recovery_opp: 'FR', def_tds: 'TD', def_safeties: 'Sfty',
    fg_made: 'FGM', fg_att: 'FGA', fg_long: 'Long', fg_made_40_49: '40-49',
    fg_made_50_59: '50-59', fg_made_60_: '60+', pat_made: 'XPM',
    pat_att: 'XPA',
    pt_att: 'Punts', pt_yards: 'Yds', pt_net_yards: 'Net', pt_inside_20: 'In 20',
    pt_long: 'Long',
    kickoff_returns: 'KR', kickoff_return_yards: 'KR yds',
    punt_returns: 'PR', punt_return_yards: 'PR yds',
    special_teams_tds: 'TD',
    fantasy_points_ppr: 'PPR pts'
  };

  /* Lower case, accents gone, punctuation gone: "Ja'Marr" finds Ja'Marr from
   * "jamarr" and "Kaʻimi" from "kaimi". */
  function normalizeName(value) {
    var textValue = String(value === null || value === undefined ? '' : value);
    if (textValue.normalize) {
      textValue = textValue.normalize('NFD').replace(/[̀-ͯ]/g, '');
    }
    return textValue.toLowerCase().replace(/[^a-z0-9 ]+/g, '')
      .replace(/\s+/g, ' ').trim();
  }

  /* Players matching a query, best first.
   *
   * A name that STARTS with the query beats one where any word does, which
   * beats one that merely contains it; within a tier the more recent career
   * comes first, because "Josh Allen" means the quarterback to nearly
   * everybody who types it, and the linebacker of the same name is one row
   * further down rather than gone. */
  function searchPlayers(index, query, limit) {
    var wanted = normalizeName(query);
    var rows = (index && index.players) || [];
    if (!wanted) return [];
    var found = [];
    rows.forEach(function (row) {
      var name = normalizeName(row[1]);
      var tier = null;
      if (name.indexOf(wanted) === 0) tier = 0;
      else if ((' ' + name).indexOf(' ' + wanted) >= 0) tier = 1;
      else if (name.indexOf(wanted) >= 0) tier = 2;
      if (tier !== null) found.push({ tier: tier, row: row });
    });
    found.sort(function (a, b) {
      if (a.tier !== b.tier) return a.tier - b.tier;
      var last = (Number(b.row[4]) || 0) - (Number(a.row[4]) || 0);
      if (last) return last;
      return String(a.row[1]).localeCompare(String(b.row[1]));
    });
    return found.slice(0, limit || 25).map(function (item) {
      var row = item.row;
      return { id: row[0], name: row[1], position: row[2], first: row[3],
               last: row[4], team: row[5], shard: row[6] };
    });
  }

  /* One player's packed season rows as objects, oldest first, the regular
   * season before the postseason within a year. */
  function unpackSeasons(file, gsis) {
    var player = file && file.players && file.players[gsis];
    var names = (file && file.stats) || [];
    if (!player || !player.seasons) return [];
    return player.seasons.map(function (line) {
      var stats = {};
      for (var at = 4; at + 1 < line.length; at += 2) {
        var name = names[line[at]];
        if (name !== undefined) stats[name] = line[at + 1];
      }
      return { season: line[0], type: line[1] === 1 ? 'post' : 'reg',
               team: line[2], games: line[3], stats: stats };
    }).sort(function (a, b) {
      return a.season - b.season || (a.type === 'reg' ? -1 : 1);
    });
  }

  /* Snap counts by season, regular season then postseason.
   *
   * The share is the MEAN OF THE PER-GAME SHARES over the games he was on
   * that unit for, not his snaps over the team's: the archive stores no team
   * snap total, because the source gives a share per game and nothing to add
   * up. The page says "per game" beside it for that reason. */
  function unpackUsage(file, gsis) {
    var player = file && file.players && file.players[gsis];
    if (!player || !player.usage) return [];
    return player.usage.map(function (line) {
      return { season: line[0], type: line[1] === 1 ? 'post' : 'reg',
               games: line[2], off: line[3], offShare: line[4],
               def: line[5], defShare: line[6], st: line[7],
               stShare: line[8] };
    }).sort(function (a, b) {
      return a.season - b.season || (a.type === 'reg' ? -1 : 1);
    });
  }

  /* Injury-report appearances by season. */
  function unpackAvailability(file, gsis) {
    var player = file && file.players && file.players[gsis];
    if (!player || !player.availability) return [];
    return player.availability.map(function (line) {
      return { season: line[0], out: line[1], doubtful: line[2],
               questionable: line[3], probable: line[4], listed: line[5],
               injuries: line[6] || [] };
    }).sort(function (a, b) { return a.season - b.season; });
  }

  /* A share, 0-1, as a whole percentage. Null and undefined print nothing;
   * a share the source gives as 0 is a real measurement and prints 0%. */
  function shareText(share) {
    if (share === null || share === undefined) return '';
    if (typeof share !== 'number' || !isFinite(share)) return '';
    return Math.round(share * 100) + '%';
  }

  function unpackRoster(file, gsis) {
    var player = file && file.players && file.players[gsis];
    if (!player || !player.roster) return [];
    return player.roster.map(function (line) {
      return { season: line[0], team: line[1], position: line[2],
               jersey: line[3], status: line[4] };
    });
  }

  /* Career totals over season rows of ONE type.
   *
   * Counting stats add up. A best (`fg_long`) is the largest season one. An
   * average or a share (`passing_cpoe`, `target_share`) is LEFT OUT: a
   * career target share is not a thing, and the mean of twelve season CPOEs
   * weights a four-game season like a seventeen-game one. Rows of the other
   * season type are refused rather than summed in, because a postseason
   * line added to a regular season is neither. */
  function careerTotals(rows, meanStats, maxStats) {
    var means = {};
    var maxes = {};
    (meanStats || []).forEach(function (name) { means[name] = true; });
    (maxStats || []).forEach(function (name) { maxes[name] = true; });
    var type = null;
    var totals = { games: 0, stats: {} };
    (rows || []).forEach(function (row) {
      if (type === null) type = row.type;
      if (row.type !== type) {
        throw new Error('career totals mix regular season and postseason');
      }
      totals.games += Number(row.games) || 0;
      Object.keys(row.stats || {}).forEach(function (name) {
        if (means[name]) return;
        var value = Number(row.stats[name]);
        if (!isFinite(value)) return;
        if (maxes[name]) {
          totals.stats[name] = Math.max(totals.stats[name] || 0, value);
        } else {
          totals.stats[name] = (totals.stats[name] || 0) + value;
        }
      });
    });
    totals.type = type;
    return totals;
  }

  /* The tables a player's page draws: only the tables he has a number in,
   * only the columns somebody in those rows has a number in, and only the
   * seasons with at least one of them. A quarterback's page with an empty
   * twelve-column defence table under it is noise. */
  function statTablesFor(rows, tables) {
    var out = [];
    (tables || []).forEach(function (table) {
      var columns = table.stats.filter(function (name) {
        return (rows || []).some(function (row) {
          return row.stats && row.stats[name] !== undefined;
        });
      });
      if (!columns.length) return;
      var seasons = rows.filter(function (row) {
        return columns.some(function (name) {
          return row.stats[name] !== undefined;
        });
      });
      out.push({ key: table.key, label: table.label, columns: columns,
                 rows: seasons });
    });
    return out;
  }

  /* A statistic for display. A missing value is a dash, never a zero: the
   * export does not store zeros, so a missing counting stat IS zero, but
   * printing 0 for a kicker's passing yards would claim a measurement. Half
   * sacks are real, EPA and CPOE are signed to one place, a share is a
   * percentage. */
  function statText(name, value) {
    if (value === null || value === undefined) return '—';
    var number = Number(value);
    if (!isFinite(number)) return '—';
    if (name === 'target_share') return (number * 100).toFixed(1) + '%';
    if (/_epa$/.test(name) || name === 'passing_cpoe') {
      return (number > 0 ? '+' : number < 0 ? '−' : '')
        + Math.abs(number).toFixed(1);
    }
    if (name === 'fantasy_points_ppr') return number.toFixed(1);
    // A true minus rather than a hyphen, as every other signed number on
    // the page: sack yardage is stored negative by the source.
    var sign = number < 0 ? '\u2212' : '';
    var size = Math.abs(number);
    if (size !== Math.floor(size)) return sign + size.toFixed(1);
    return sign + size.toLocaleString('en-US');
  }

  function heightText(inches) {
    var number = Number(inches);
    if (!isFinite(number) || number <= 0 || inches === null) return '';
    return Math.floor(number / 12) + '-' + (number % 12);
  }

  /* "2012–2024", or one year for a one-season career. */
  function playerSpan(first, last) {
    if (first === null || first === undefined) return '';
    if (last === null || last === undefined || last === first) {
      return String(first);
    }
    return first + '–' + last;
  }

  /* A season's draft as rounds of pick objects. */
  function draftRounds(file, season) {
    var rows = (file && file.seasons && file.seasons[String(season)]) || [];
    var fields = (file && file.fields) || [];
    var rounds = [];
    rows.forEach(function (line) {
      var pick = {};
      fields.forEach(function (field, at) { pick[field] = line[at]; });
      var last = rounds[rounds.length - 1];
      if (!last || last.round !== pick.round) {
        last = { round: pick.round, picks: [] };
        rounds.push(last);
      }
      last.picks.push(pick);
    });
    return rounds;
  }



  /* ------------------------------------------------------------------
   * The game layer: drives, the scoring summary and the win-probability
   * trace, unpacked from the positional rows `src/nfl/game_export.py`
   * writes. The rows are positional for size — 170,528 drives with named
   * keys would be several megabytes of repeated field names — so this is
   * the one place that knows what each slot means, and nothing downstream
   * reads an index.
   * ---------------------------------------------------------------- */

  /* Field order of a packed drive row. Must match DRIVE row order in
   * src/nfl/game_export.py. */
  var DRIVE_FIELDS = ['drive', 'side', 'quarter', 'seconds', 'start', 'plays',
                      'yards', 'top', 'result', 'homeScore', 'awayScore',
                      'wp'];
  var SCORE_FIELDS = ['side', 'quarter', 'clock', 'kind', 'points', 'scorer',
                      'distance', 'homeScore', 'awayScore', 'description'];

  var KIND_LABELS = {
    TD: 'Touchdown', FG: 'Field goal', XP: 'Extra point',
    '2PT': 'Two-point conversion', SAFETY: 'Safety',
    D2PT: 'Defensive two-point conversion'
  };

  function unpackRow(fields, row) {
    var out = {};
    for (var i = 0; i < fields.length; i += 1) {
      out[fields[i]] = row && i < row.length ? row[i] : null;
    }
    return out;
  }

  function unpackDrives(entry, results) {
    var rows = (entry && entry.d) || [];
    var vocabulary = results || [];
    return rows.map(function (row) {
      var drive = unpackRow(DRIVE_FIELDS, row);
      /* The result is interned to an index, and an index with no word behind
       * it is left null rather than printed as a number. */
      drive.result = typeof drive.result === 'number'
        ? (vocabulary[drive.result] || null) : null;
      return drive;
    });
  }

  var INJURY_FIELDS = ['side', 'status', 'name', 'position', 'injury'];

  /* The week's injury report for one game, DESIGNATED PLAYERS ONLY.
   *
   * Ordered by how likely the player was to miss it, Out first, because that
   * is the order the question is asked in. Within a status the export's own
   * order, which is by team and then by name, is kept. */
  function unpackInjuries(entry, statuses) {
    var rows = (entry && entry.i) || [];
    var vocabulary = statuses || [];
    return rows.map(function (row, at) {
      var item = unpackRow(INJURY_FIELDS, row);
      item.rank = typeof item.status === 'number' ? item.status
        : vocabulary.length;
      item.status = typeof item.status === 'number'
        ? (vocabulary[item.status] || null) : null;
      item.at = at;
      return item;
    }).sort(function (a, b) { return a.rank - b.rank || a.at - b.at; });
  }

  function unpackScoring(entry, kinds) {
    var rows = (entry && entry.s) || [];
    var vocabulary = kinds || [];
    return rows.map(function (row) {
      var play = unpackRow(SCORE_FIELDS, row);
      play.kind = typeof play.kind === 'number'
        ? (vocabulary[play.kind] || null) : null;
      play.label = KIND_LABELS[play.kind] || play.kind || 'Score';
      return play;
    });
  }

  /* A per-mille integer as the percentage a page prints.
   *
   * NEVER 0% OR 100%, AND THE ENDPOINTS ARE INCLUDED. A model that has given
   * a team one chance in a thousand has not given it none, and printing none
   * is a claim it did not make — but the exporter rounds to per-mille, so a
   * real 0.0004 arrives here as exactly 0 and an excluded endpoint would
   * print it as certainty. Every state that reaches this function has time
   * left on the clock: an overtime state, where the model really does answer
   * 1 or 0 by the score alone, carries no probability and never gets here. */
  function probabilityText(perMille) {
    if (typeof perMille !== 'number' || !isFinite(perMille)) return null;
    var pct = perMille / 10;
    if (pct < 0.5) return '<1%';
    if (pct > 99.5) return '>99%';
    return Math.round(pct) + '%';
  }

  /* The win-probability trace as points on a 0-1 line, in clock order.
   *
   * Drive starts only, which is what the archive states, plus the final
   * outcome as the closing point so the line ends where the game did. A
   * drive the export could not price is dropped rather than drawn at 0.5. */
  function winProbabilityTrace(entry, game) {
    var points = [];
    if (entry && typeof entry.wp === 'number') {
      points.push({ at: 0, value: entry.wp / 1000, label: 'Kickoff' });
    }
    unpackDrives(entry).forEach(function (drive) {
      if (typeof drive.wp !== 'number'
          || typeof drive.seconds !== 'number') return;
      points.push({
        at: 1 - drive.seconds / 3600,
        value: drive.wp / 1000,
        label: 'Drive ' + drive.drive
      });
    });
    if (game && isFinal(game)) {
      var margin = game.home_points - game.away_points;
      points.push({
        at: 1,
        value: margin > 0 ? 1 : (margin < 0 ? 0 : 0.5),
        label: 'Final'
      });
    }
    return points;
  }

  /* An SVG path through the trace, drawn in a unit box the caller scales.
   * Straight segments, not a curve: the model says nothing about what
   * happened between two drive starts, and a smoothed line would say it did. */
  function tracePath(points, width, height) {
    if (!points || points.length < 2) return '';
    return points.map(function (point, index) {
      var x = (point.at * width).toFixed(2);
      var y = ((1 - point.value) * height).toFixed(2);
      return (index ? 'L' : 'M') + x + ' ' + y;
    }).join(' ');
  }

  /* Which side a packed row belongs to, as the game's own team key. */
  function sideTeam(game, side) {
    if (side === 1) return game ? game.home : null;
    if (side === 0) return game ? game.away : null;
    return null;
  }

  function driveSummary(drive) {
    var parts = [];
    if (typeof drive.plays === 'number') {
      parts.push(drive.plays + (drive.plays === 1 ? ' play' : ' plays'));
    }
    if (typeof drive.yards === 'number') parts.push(drive.yards + ' yds');
    if (drive.top) parts.push(drive.top);
    return parts.join(', ');
  }

  /* Whether any drive of this game went unpriced, so the page can say why.
   *
   * It is always an overtime drive: the exporter prices every drive that has
   * a score, a clock and a rated game, and leaves out only the states the
   * model's regulation clock cannot speak for. */
  function hasUnpricedDrives(entry, payload) {
    return unpackDrives(entry, payload && payload.results)
      .some(function (drive) { return typeof drive.wp !== 'number'; });
  }

  /* Whether this game's scoring summary may be drawn as a scoreboard.
   *
   * 18 of 7,366 games in the archive have a play log whose points do not add
   * up to the final — sixteen of them Jacksonville home games of 2001 and
   * 2002, where the source's own running score is wrong. The exporter marks
   * them and the page says so rather than drawing a summary that contradicts
   * the score beside it. */
  function scoringReconciles(entry) {
    return !entry || !entry.s ? true : entry.ok === 1;
  }

  var api = {
    LEAGUE: LEAGUE,
    KIND_LABELS: KIND_LABELS,
    unpackDrives: unpackDrives,
    unpackScoring: unpackScoring,
    unpackInjuries: unpackInjuries,
    probabilityText: probabilityText,
    winProbabilityTrace: winProbabilityTrace,
    tracePath: tracePath,
    sideTeam: sideTeam,
    driveSummary: driveSummary,
    hasUnpricedDrives: hasUnpricedDrives,
    scoringReconciles: scoringReconciles,
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
    atsBeatsVig: atsBeatsVig,
    STAT_LABELS: STAT_LABELS,
    normalizeName: normalizeName,
    searchPlayers: searchPlayers,
    unpackSeasons: unpackSeasons,
    unpackRoster: unpackRoster,
    unpackUsage: unpackUsage,
    unpackAvailability: unpackAvailability,
    shareText: shareText,
    careerTotals: careerTotals,
    statTablesFor: statTablesFor,
    statText: statText,
    heightText: heightText,
    playerSpan: playerSpan,
    draftRounds: draftRounds
  };

  if (typeof module === 'object' && module.exports) {
    module.exports = api;
  }
  root.nflApp = api;
}(typeof globalThis === 'object' ? globalThis : this));
