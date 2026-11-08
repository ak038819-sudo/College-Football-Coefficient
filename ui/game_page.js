/* The game page, for every league.
 *
 * College football's game page was built inside ui/dashboard_shell.html and
 * could not be used anywhere else. The NFL had game detail with no URL, the
 * NBA had its own page in its own design, and college basketball had none at
 * all. The design now lives in ui/game_page.css; the parts of the page that
 * are a DECISION rather than a style live here, once, so four leagues cannot
 * answer the same question four ways.
 *
 * Everything here is pure: a function takes a game and returns markup or a
 * plain object, and nothing touches the document or the network. That is what
 * lets tests/game_page.test.cjs drive it directly rather than through a page.
 *
 * THE FIELD NAMES ARE THE CONTRACT. A league's export states a game's model
 * chain as `home_elo`, `away_elo`, `home_wp`, and, where the model learned
 * from the result, `home_elo_post`, `away_elo_post` and `elo_change`. A
 * league whose export carries none of them gets no Elo group rather than a
 * row of dashes -- see `eloGroup`.
 */
(function (root) {
  'use strict';

  function esc(value) {
    return String(value === null || value === undefined ? '' : value)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }

  /* A number, or null. `Number(null)` is 0 and `Number('')` is 0, so a
   * missing value coerces to a real-looking zero: this project has shipped
   * that bug three times, as a missing clock reading 0:00 and a missing score
   * reading 0-0. The null check comes FIRST, every time.
   */
  function num(value) {
    if (value === null || value === undefined || value === '') return null;
    var n = Number(value);
    return isFinite(n) ? n : null;
  }

  /* An Elo rating, as the nearest whole integer.
   *
   * Austin's rule, and it is a display rule only: round at render, never in
   * anything that is then aggregated. A rating CHANGE keeps a decimal,
   * because a 0.4 move rounded to zero reads as a game that did not count.
   */
  function eloValue(value) {
    var n = num(value);
    return n === null ? '—' : String(Math.round(n));
  }

  function signed(value, places) {
    var n = num(value);
    if (n === null) return '—';
    var digits = places === undefined ? 1 : places;
    return (n >= 0 ? '+' : '−') + Math.abs(n).toFixed(digits);
  }

  /* The two win chances a stored home probability implies, as percentages
   * that add to 100. Rounded together rather than separately: rounding each
   * side on its own showed 51% against 48% on a game page, which reads as a
   * third outcome nobody modelled.
   */
  function percents(homeProbability) {
    var p = num(homeProbability);
    if (p === null || p < 0 || p > 1) return null;
    var home = Math.round(p * 1000) / 10;
    return { home: home.toFixed(1), away: (100 - home).toFixed(1) };
  }

  /* The Elo column group for one game, or null.
   *
   * Null when the export carries no chain for this game -- an unrated
   * fixture against a non-Division I opponent, a season before the model
   * begins. The matchup table then leaves the group out entirely, which is
   * the rule the college football page already followed: a group whose data
   * this game does not have is omitted rather than filled with dashes, so
   * the table never widens to say nothing.
   */
  function eloGroup(game, options) {
    var g = game || {};
    var opts = options || {};
    var pre = num(g.home_elo) !== null || num(g.away_elo) !== null;
    var chance = percents(g.home_wp);
    if (!pre && !chance) return null;
    var done = Boolean(opts.completed);
    var after = done && (num(g.home_elo_post) !== null
                         || num(g.away_elo_post) !== null);
    // `wp_live` marks an export that computed this game's numbers from the
    // ratings the clubs hold NOW, because the game has not been played and
    // there is no stored chain to read. The column is named for what it
    // holds: "Before" is a measurement, "Now" is a moving number, and giving
    // both the same heading is how a reader comes to believe the model
    // predicted something it never did.
    var live = Boolean(g.wp_live) && !done;
    var columns = [[live ? 'Now' : 'Before', live
                     ? 'Each team’s rating as it stands today.'
                     : 'Each team’s rating entering the game.'],
                   ['Win chance', done
                     ? 'The pregame expectation, home court included.'
                     : 'The current expectation, home court included.']];
    if (after) {
      columns.push(['After', 'The rating this game produced.'],
                   ['Δ Elo', 'How far the result moved the rating.']);
    }
    return {
      name: opts.name || 'Elo',
      columns: columns,
      cells: function (side) {
        var out = [eloValue(side === 'home' ? g.home_elo : g.away_elo),
                   chance ? (side === 'home' ? chance.home : chance.away) + '%'
                     : '—'];
        if (after) {
          out.push(eloValue(side === 'home' ? g.home_elo_post : g.away_elo_post));
          // The stored change is the HOME side's. The away side moved by
          // exactly as much in the other direction, which is what makes the
          // chain add up, so it is negated rather than looked up.
          var change = num(g.elo_change);
          out.push(signed(change === null ? null
            : (side === 'home' ? change : -change)));
        }
        return out;
      }
    };
  }

  /* Once a table has this many columns it fills its panel; below it, the game
   * simply has less to show and a stretched table is the sparseness the
   * layout exists to remove, not a fix for it. */
  var FILL_COLUMNS = 9;

  function matchupTable(groups, names) {
    var kept = (groups || []).filter(Boolean);
    if (!kept.length) return '';
    var side = names || {};
    var columns = kept.reduce(function (n, group) {
      return n + group.columns.length;
    }, 1);
    var groupHead = '<tr><th scope="col"></th>' + kept.map(function (group) {
      return '<th scope="colgroup" colspan="' + group.columns.length
        + '" class="gp-group">' + esc(group.name) + '</th>';
    }).join('') + '</tr>';
    var metricHead = '<tr><th scope="col">Team</th>' + kept.map(function (group) {
      return group.columns.map(function (pair) {
        return '<th scope="col" title="' + esc(pair[1]) + '">'
          + esc(pair[0]) + '</th>';
      }).join('');
    }).join('') + '</tr>';
    var row = function (key, name) {
      var cells = [];
      kept.forEach(function (group) {
        group.cells(key).forEach(function (value) { cells.push(value); });
      });
      return '<tr><th scope="row">' + esc(name) + '</th>' + cells.map(function (v) {
        return '<td>' + esc(v) + '</td>';
      }).join('') + '</tr>';
    };
    return '<table class="gp-table gp-matchup'
      + (columns >= FILL_COLUMNS ? ' is-wide' : '') + '"><thead>'
      + groupHead + metricHead + '</thead><tbody>'
      + row('away', side.away) + row('home', side.home)
      + '</tbody></table>';
  }

  /* The period-by-period line score, or null.
   *
   * The guard is load-bearing and is the college football one, generalised:
   * both sides present, the same length, at least `regulation` periods, AND
   * each side's periods summing to that side's final score. A line score that
   * fails any of those is OMITTED rather than shown wrong -- a table that
   * disagrees with the scoreboard above it is worse than no table.
   *
   * `regulation` is the league's own: four quarters of football or the NBA,
   * two halves of college basketball.
   */
  function lineScore(spec) {
    var s = spec || {};
    var regulation = s.regulation === undefined ? 4 : s.regulation;
    var home = s.home, away = s.away;
    var whole = function (periods) {
      return Array.isArray(periods) && periods.length
        && periods.every(function (n) {
          return typeof n === 'number' && isFinite(n)
            && Math.floor(n) === n && n >= 0;
        });
    };
    if (!whole(home) || !whole(away)) return null;
    if (home.length !== away.length || home.length < regulation) return null;
    var total = function (periods) {
      return periods.reduce(function (sum, n) { return sum + n; }, 0);
    };
    if (total(home) !== num(s.homeScore)) return null;
    if (total(away) !== num(s.awayScore)) return null;
    var labels = home.map(function (unused, i) {
      if (i < regulation) return String(i + 1);
      return i === regulation ? 'OT' : (i - regulation + 1) + 'OT';
    });
    return { labels: labels, home: home, away: away };
  }

  function lineScoreTable(rows, names, scores) {
    if (!rows) return '';
    var side = names || {};
    var final = scores || {};
    var head = '<tr><th scope="col"></th>' + rows.labels.map(function (label) {
      return '<th scope="col">' + esc(label) + '</th>';
    }).join('') + '<th scope="col" class="gp-line-total">T</th></tr>';
    var row = function (name, periods, total) {
      return '<tr><th scope="row">' + esc(name) + '</th>'
        + periods.map(function (n) { return '<td>' + esc(n) + '</td>'; }).join('')
        + '<td class="gp-line-total">' + esc(total) + '</td></tr>';
    };
    return '<table class="gp-table gp-line"><thead>' + head + '</thead><tbody>'
      + row(side.away, rows.away, final.away)
      + row(side.home, rows.home, final.home) + '</tbody></table>';
  }

  function badges(list) {
    return (list || []).filter(Boolean).map(function (badge) {
      var kind = typeof badge === 'string' ? '' : (badge.kind || '');
      var text = typeof badge === 'string' ? badge : badge.text;
      return '<span class="gbadge' + (kind ? ' ' + esc(kind) : '') + '">'
        + esc(text) + '</span>';
    }).join('');
  }

  /* One fact box per entry, each a label and a value. An entry with nothing
   * to say is dropped rather than shown empty. `html` marks a value that is
   * already markup, such as a link; anything else is escaped. */
  function facts(list) {
    var kept = (list || []).filter(function (fact) {
      return fact && fact.label && (fact.value || fact.html);
    });
    if (!kept.length) return '';
    return '<div class="gp-facts">' + kept.map(function (fact) {
      return '<div class="gp-fact"><b>' + esc(fact.label) + '</b>'
        + (fact.html ? fact.html : esc(fact.value)) + '</div>';
    }).join('') + '</div>';
  }

  var api = {
    esc: esc,
    num: num,
    eloValue: eloValue,
    signed: signed,
    percents: percents,
    eloGroup: eloGroup,
    FILL_COLUMNS: FILL_COLUMNS,
    matchupTable: matchupTable,
    lineScore: lineScore,
    lineScoreTable: lineScoreTable,
    badges: badges,
    facts: facts
  };

  if (typeof module === 'object' && module.exports) module.exports = api;
  root.gamePage = api;
}(typeof globalThis === 'object' ? globalThis : this));
