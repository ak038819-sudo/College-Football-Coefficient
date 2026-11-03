/* Several leagues in one Elocast: the state behind the interface contract.
 *
 * docs/basketball-ui.md is the contract. This is the part of it that is pure
 * logic — which league a route names, what a league switch keeps, which
 * favorites key a league reads, how a basketball day's games order when 44% of
 * them have no tip-off time, and which tabs a league may honestly show.
 *
 * Everything here is a pure function over its arguments. No DOM, no fetch, no
 * football global. The page that uses it is ui/basketball.html, which is
 * UNLINKED: release gate 6 decides whether a reader can reach it.
 *
 * The rule that matters most is the first one below. Every football deep link
 * published to date must keep opening the same page, so football is the
 * default and omits the league from the route entirely.
 */

(function (root) {
  'use strict';

  var FOOTBALL = 'cfb';
  var BASKETBALL = 'ncaambb';
  var NFL = 'nfl';

  /* `launched` is whether a league's export is published yet, and it is the
   * one thing that decides whether a league may appear in a picker. A league
   * listed with no data behind it sends a reader to a page that can only say
   * it has nothing, which is worse than not offering it at all.
   *
   * It lives here, in the shared registry, because every page draws its own
   * picker: gating only the football header let a reader reach basketball
   * from the NFL page instead, which is a hole a per-page check cannot see.
   * One flag, read by every picker, is the only shape that holds.
   */
  var LEAGUES = [
    { slug: FOOTBALL, label: 'College Football', short: 'Football',
      picker: 'College Football', launched: true },
    // "Men's Basketball" in the picker: the women's game is a different
    // competition and an unqualified label would claim something we are not.
    //
    // Launched 2026-11-01, with the export it waited for: ui/data/ncaambb
    // carries 2015-2026. It was held false for eleven days because the
    // provider's monthly quota was spent, and a league offered here with no
    // data behind it is the one thing this flag exists to prevent.
    { slug: BASKETBALL, label: 'College Basketball', short: "Men's Basketball",
      picker: "Men's Basketball", launched: true },
    { slug: NFL, label: 'NFL', short: 'NFL', picker: 'NFL', launched: true }
  ];

  var LEAGUE_KEY = 'elocast.league.v1';
  var FOOTBALL_FAVORITES_KEY = 'elocast.favorites.v1';

  // What each league can honestly show. An empty tab is worse than an absent
  // one, so basketball has no Stats until Player Impact or Roster Scenarios
  // is ready, and no coaches, awards or recruiting because B02 loaded players
  // only.
  var SECTIONS = {
    cfb: ['home', 'stats', 'standings', 'teams', 'people', 'games',
          'rankings'],
    ncaambb: ['home', 'standings', 'teams', 'people', 'games'],
    // The NFL export carries games, ratings, franchise records, head coaches,
    // the model's own track record and, from export schema 3, the people
    // layer: players 1999 onward and the draft. `people` is the same section
    // name the other two leagues use for the same thing. `coaches` stays its
    // own section, because the NFL's coach records come from the game
    // archive rather than from a people feed.
    nfl: ['home', 'standings', 'teams', 'people', 'games', 'rankings',
          'coaches', 'history']
  };

  /* THE FIVE TABS, in one order, under one set of names, for every league.
   *
   * Austin, 2026-10-29: "I want to make our visual layouts and Identity
   * consistent." Before this the NFL drew EIGHT tabs (Standings and Rankings
   * among them, which read as the same thing), basketball drew three, and
   * the same tab was called Players on one page and People on another. A
   * reader crossing leagues met a different nav each time.
   *
   * It lives here for the same reason `SECTIONS` does: a per-page list is a
   * list that drifts. A page decides which of the five it can DRAW -- an
   * empty tab is worse than an absent one -- but not what they are called or
   * what order they come in.
   */
  var TABS = [
    ['home', 'Home'],
    ['stats', 'Stats'],
    ['standings', 'Standings'],
    ['teams', 'Teams'],
    ['people', 'People']
  ];

  function tabs() {
    return TABS.map(function (pair) { return pair.slice(); });
  }

  function tabLabel(name) {
    for (var index = 0; index < TABS.length; index += 1) {
      if (TABS[index][0] === name) return TABS[index][1];
    }
    return null;
  }

  function league(slug) {
    for (var index = 0; index < LEAGUES.length; index += 1) {
      if (LEAGUES[index].slug === slug) return LEAGUES[index];
    }
    return null;
  }

  function known(slug) {
    return league(slug) !== null;
  }

  /* The leagues a picker may offer: the launched ones, plus the league the
   * reader is already on.
   *
   * `current` is included whatever its flag says, because the page drawing
   * the picker is the page they are looking at -- hiding it would leave the
   * control with nothing pressed. Everything else has to have shipped.
   */
  function launchedLeagues(current) {
    return LEAGUES.filter(function (entry) {
      return entry.launched === true || entry.slug === current;
    });
  }

  function isLaunched(slug) {
    var entry = league(slug);
    return entry !== null && entry.launched === true;
  }

  /* The route, from a hash.
   *
   * A hash with no league is football, unchanged, forever: that is what keeps
   * every published football link working. An unknown league is treated as
   * absent rather than as an error, because a typo in a shared link should
   * open football rather than a dead page.
   */
  function readRoute(hash) {
    var text = String(hash || '').replace(/^#/, '');
    var route = { league: FOOTBALL, params: {} };
    text.split('&').forEach(function (pair) {
      if (!pair) return;
      var index = pair.indexOf('=');
      var key = index < 0 ? pair : pair.slice(0, index);
      var value = index < 0 ? '' : decodeURIComponent(pair.slice(index + 1));
      if (!key) return;
      if (key === 'league') {
        if (known(value)) route.league = value;
        return;
      }
      route.params[key] = value;
    });
    return route;
  }

  /* A hash, from a route. Football omits the league; basketball states it.
   *
   * The league goes FIRST so a basketball link is recognisable at a glance and
   * two links to one place are the same string.
   */
  function writeRoute(route) {
    var parts = [];
    var slug = route && route.league;
    if (slug && slug !== FOOTBALL && known(slug)) {
      parts.push('league=' + slug);
    }
    var params = (route && route.params) || {};
    Object.keys(params).forEach(function (key) {
      if (key === 'league') return;
      var value = params[key];
      if (value === null || value === undefined || value === '') return;
      parts.push(key + '=' + encodeURIComponent(value));
    });
    return parts.length ? '#' + parts.join('&') : '#';
  }

  /* ---- people ------------------------------------------------------- */

  /* How many ways a season's player-game lines are split, and which shard a
   * player's are in. MUST agree with src/ncaambb/export_people.py.
   *
   * The modulo is written out rather than left to the language: JavaScript's
   * `%` keeps the sign, so a negative provider id -- which the football
   * archive really carries -- gives -3 here and 13 in Python, and -3 is a
   * file that does not exist. Python's is already floored, so its own tests
   * cannot catch this and these are the assertions that can.
   */
  var PEOPLE_SHARDS = 16;

  function peopleShard(providerId, shards) {
    var count = shards || PEOPLE_SHARDS;
    var id = parseInt(providerId, 10);
    if (isNaN(id)) return null;
    return ((id % count) + count) % count;
  }

  /* A player's full key, from the bare provider id the people files carry.
   *
   * The files leave the prefix off 13,000 keys because it is constant inside
   * a file that exists only for basketball. It is put back here, in one
   * place, so nothing downstream holds a bare integer that could be mistaken
   * for a football id.
   */
  function playerKey(providerId) {
    return 'ncaambb:cbbd:player:' + String(providerId);
  }

  function providerIdOf(key) {
    var parts = String(key || '').split(':');
    return parts[parts.length - 1];
  }

  /* A player's seasons, from the shards already loaded, newest first.
   *
   * `shards` is the pushed queue of shard payloads. A season is included when
   * the player has a membership OR a game line in it: 29 Division I players
   * in 2025-26 recorded a line without appearing in any roster snapshot, and
   * their minutes are real.
   */
  function playerSeasons(shards, providerId) {
    var id = String(providerId);
    var found = {};
    (shards || []).forEach(function (shard) {
      if (!shard || typeof shard.season !== 'number') return;
      var member = (shard.m || {})[id];
      var lines = (shard.p || {})[id];
      if (!member && !lines) return;
      var entry = found[shard.season] || { season: shard.season,
                                           clubs: [], games: [] };
      (member || []).forEach(function (row) {
        entry.clubs.push({
          team: (shard.t || [])[row[0]] || null,
          jersey: row[1], position: row[2], height: row[3], weight: row[4]
        });
      });
      (lines || []).forEach(function (row) {
        entry.games.push({
          game: row[0], team: (shard.t || [])[row[1]] || null,
          minutes: row[2], points: row[3], starter: row[4],
          // The day and the opponent come from the LINE, not from the
          // season's payload: a career page spans four or five seasons and
          // loads one, so looking them up there dated one season and left
          // the rest a table of em dashes.
          day: row[5] || null,
          opponent: row[6] === null || row[6] === undefined
            ? null : ((shard.t || [])[row[6]] || null),
          site: row[7] || null
        });
      });
      found[shard.season] = entry;
    });
    return Object.keys(found)
      .map(function (key) { return found[key]; })
      .sort(function (a, b) { return b.season - a.season; });
  }

  /* A season's totals for one player: games, and the minutes and points that
   * were actually recorded.
   *
   * `minutes` and `points` are null when NOTHING was recorded, which is not
   * the same as a measured zero -- and `games` counts every line either way,
   * because a player with a line played.
   */
  function playerTotals(games) {
    var rows = games || [];
    var minutes = null;
    var points = null;
    rows.forEach(function (row) {
      if (row.minutes !== null && row.minutes !== undefined) {
        minutes = (minutes === null ? 0 : minutes) + row.minutes;
      }
      if (row.points !== null && row.points !== undefined) {
        points = (points === null ? 0 : points) + row.points;
      }
    });
    return {
      games: rows.length,
      minutes: minutes === null ? null : Math.round(minutes * 10) / 10,
      points: points === null ? null : Math.round(points * 10) / 10,
      starts: rows.filter(function (row) { return row.starter === 1; }).length
    };
  }

  /* A per-game average, or null when there is nothing to average.
   *
   * Zero games is null and never 0.0, which would read as a player who
   * played and scored nothing.
   */
  function perGame(total, games) {
    if (total === null || total === undefined || !games) return null;
    return Math.round((total / games) * 10) / 10;
  }

  /* How to name a run of seasons in a sentence.
   *
   * `join(' and ')` is right for two and absurd for six: it read "2021-22 and
   * 2022-23 and 2023-24 and 2024-25 and 2025-26 and 2026-27" on the People
   * tab, which is how it looked in the browser.
   */
  function seasonRange(seasons) {
    var rows = (seasons || []).slice().sort(function (a, b) { return a - b; });
    if (!rows.length) return '';
    if (rows.length > 2) {
      return seasonLabel(rows[0]) + ' through '
        + seasonLabel(rows[rows.length - 1]);
    }
    return rows.map(seasonLabel).join(' and ');
  }

  /* The players a query finds, by name, best first.
   *
   * Reuses the same normalisation the club search uses, so "stjohns" finds
   * St. John's and "oconnell" finds O'Connell.
   */
  function searchPlayers(index, query, limit) {
    var wanted = normalizeName(query);
    if (!wanted) return [];
    var found = [];
    Object.keys(index || {}).forEach(function (id) {
      var entry = index[id] || {};
      var name = normalizeName(entry.n || '');
      if (!name) return;
      var at = name.indexOf(wanted);
      if (at < 0) return;
      found.push({ id: id, name: entry.n, hometown: entry.h || null,
                   state: entry.s || null, at: at });
    });
    found.sort(function (a, b) {
      if (a.at !== b.at) return a.at - b.at;
      return String(a.name).localeCompare(String(b.name));
    });
    return found.slice(0, limit || 25);
  }

  function sections(slug) {
    return SECTIONS[slug] || [];
  }

  function hasSection(slug, section) {
    return sections(slug).indexOf(section) >= 0;
  }

  /* Switching leagues keeps the destination where the destination exists in
   * both, and lands on Home where it does not — saying why, rather than
   * showing an empty panel.
   *
   * The view is dropped on a switch even when the section survives: a
   * football view name is not a promise about a basketball section's subviews,
   * and carrying one over is how a league switch lands on a blank tab.
   */
  function switchLeague(route, slug, options) {
    var target = known(slug) ? slug : FOOTBALL;
    var params = {};
    var source = (route && route.params) || {};
    Object.keys(source).forEach(function (key) {
      if (key !== 'section' && key !== 'view') params[key] = source[key];
    });
    var section = source.section || 'home';
    var carried = hasSection(target, section);
    if (carried && section !== 'home') params.section = section;
    var next = { league: target, params: params };
    var name = (league(target) || {}).short;
    return {
      route: next,
      hash: writeRoute(next),
      kept: carried,
      reason: carried ? null
        : 'There is no ' + section + ' for ' + name + ' yet, so this is its '
          + 'home screen.'
    };
  }

  /* Which league to open with, when the URL does not say.
   *
   * A URL carrying a league always wins over the remembered one, or a shared
   * link opens somebody else's sport. Storage can throw — a private window, a
   * blocked cookie jar — so every read and write is guarded and a failure
   * simply means football.
   */
  function rememberedLeague(storage) {
    try {
      var value = storage && storage.getItem(LEAGUE_KEY);
      return known(value) ? value : FOOTBALL;
    } catch (error) {
      return FOOTBALL;
    }
  }

  function rememberLeague(storage, slug) {
    if (!known(slug)) return false;
    try {
      storage.setItem(LEAGUE_KEY, slug);
      return true;
    } catch (error) {
      return false;
    }
  }

  function openingLeague(hash, storage) {
    var text = String(hash || '');
    if (/(^|[#&])league=/.test(text)) return readRoute(text).league;
    return rememberedLeague(storage);
  }

  /* Favorites are per league and never reinterpreted.
   *
   * ui/favorites.js stores football team slugs under elocast.favorites.v1.
   * That key stays football's; basketball reads its own. A basketball slug in
   * the football key would be rendered as a football team that does not
   * exist.
   */
  function favoritesKey(slug) {
    return slug === FOOTBALL || !known(slug)
      ? FOOTBALL_FAVORITES_KEY
      : 'elocast.favorites.' + slug + '.v1';
  }

  function readFavorites(storage, slug) {
    try {
      var raw = storage.getItem(favoritesKey(slug));
      var parsed = raw ? JSON.parse(raw) : [];
      return Array.isArray(parsed) ? parsed.filter(function (item) {
        return typeof item === 'string' && item;
      }) : [];
    } catch (error) {
      return [];
    }
  }

  function writeFavorites(storage, slug, teams) {
    try {
      storage.setItem(favoritesKey(slug), JSON.stringify(teams));
      return true;
    } catch (error) {
      return false;
    }
  }

  function toggleFavorite(storage, slug, team) {
    var current = readFavorites(storage, slug);
    var index = current.indexOf(team);
    if (index >= 0) current.splice(index, 1);
    else current.push(team);
    writeFavorites(storage, slug, current);
    return current;
  }

  /* Following a team in one league OFFERS the other, and never acts.
   *
   * One tap must not quietly create a second subscription, so this returns
   * something to show rather than something that has happened.
   */
  function crossLeagueOffer(slug, team, counterpart) {
    if (!counterpart) return null;
    var other = slug === FOOTBALL ? BASKETBALL : FOOTBALL;
    return {
      league: other,
      team: counterpart,
      label: 'Follow ' + (counterpart.name || counterpart) + ' '
        + (league(other) || {}).short + ' too?',
      applied: false
    };
  }

  /* 2026-27 is ending year 2027, in the label, the route and the database.
   * Nothing infers a season from the calendar year.
   */
  function seasonLabel(endingYear) {
    var year = Number(endingYear);
    if (!isFinite(year) || year <= 0) return '';
    var start = year - 1;
    return start + '-' + String(year % 100).padStart(2, '0');
  }

  function dayOf(game) {
    var at = game && game.at;
    return typeof at === 'string' && at.length >= 10 ? at.slice(0, 10) : null;
  }

  function gamesOn(games, day) {
    return (games || []).filter(function (game) {
      return dayOf(game) === day;
    });
  }

  /* Today's date, as the strip and the labels mean it.
   *
   * Resolved on every call rather than once at load, so a page left open
   * overnight does not keep calling yesterday Today.
   */
  /* The calendar a college basketball day is counted in.
   *
   * The same zone src/ncaambb/games.py writes `playing_date` with, and the
   * measurement is recorded there: 2,140 of the 2026-27 season's 3,759 timed
   * games fall on a different UTC date than the evening they are played on.
   * The page's notion of today has to agree with the export's notion of a
   * day, or an evening reader is shown tomorrow's card.
   *
   * Only the basketball page passes it. Football and the NFL state their
   * days in their own exports' conventions and keep the UTC default.
   */
  var NCAAMBB_DAY_ZONE = 'America/New_York';

  /* Today's date, in the calendar a league counts its game days in.
   *
   * `zone` is an IANA name and there is NO default, deliberately. The UTC
   * date is right for a league whose export states its days in UTC and
   * wrong for one that does not, and the same measurement that moved
   * basketball's `playing_date` to Eastern applies here: between midnight
   * UTC and midnight Eastern -- which is every evening a game is played --
   * the UTC date is already tomorrow. Reading it as today opened the
   * scoreboard on the next day's schedule while games were in progress.
   *
   * A browser without time zone data falls back to the UTC date, which is
   * wrong for part of the evening and better than no date at all.
   */
  function todayKey(now, zone) {
    var moment = now instanceof Date ? now : new Date();
    if (zone) {
      try {
        var parts = new Intl.DateTimeFormat('en-CA', {
          timeZone: zone, year: 'numeric', month: '2-digit', day: '2-digit'
        }).formatToParts(moment);
        var found = {};
        parts.forEach(function (part) { found[part.type] = part.value; });
        if (found.year && found.month && found.day) {
          return found.year + '-' + found.month + '-' + found.day;
        }
      } catch (error) {
        // Fall through to the UTC date.
      }
    }
    return moment.toISOString().slice(0, 10);
  }

  /* A compact date strip around `centre`. A week is both too coarse to
   * navigate 6,300 games and too fine to summarise them.
   *
   * `today` is a separate argument from the centre because they are different
   * things, and conflating them was a real bug: the strip marked its own
   * centre as today, so opening on any other day -- or simply moving off
   * today -- relabelled that day "Today".
   */
  function dayStrip(centre, radius, today) {
    var span = typeof radius === 'number' ? radius : 3;
    var base = Date.parse(centre + 'T12:00:00Z');
    if (isNaN(base)) return [];
    var now = typeof today === 'string' ? today : todayKey();
    var days = [];
    for (var offset = -span; offset <= span; offset += 1) {
      var moment = new Date(base + offset * 86400000);
      var day = moment.toISOString().slice(0, 10);
      days.push({ day: day, offset: offset, today: day === now });
    }
    return days;
  }

  /* Which day a scoreboard opens on.
   *
   * Today, whenever today is inside the season the payload describes. Outside
   * it -- in July, say, when the only loaded season is next November's
   * schedule -- the nearest end of the season, which is opening night rather
   * than the final. Never simply the last date in the payload: a published
   * schedule runs months ahead, so that put the reader on the championship
   * and called it Today.
   */
  function openingDay(days, today) {
    var sorted = (days || []).filter(Boolean).slice().sort();
    if (!sorted.length) return null;
    var now = typeof today === 'string' ? today : todayKey();
    if (now < sorted[0]) return sorted[0];
    if (now > sorted[sorted.length - 1]) return sorted[sorted.length - 1];
    return now;
  }

  /* Whether a game's tip-off time is known.
   *
   * 2,247 of the 2026-27 season's 6,006 games carry startTimeTbd. Rendering an
   * unknown time as midnight would put them first and read as a real time, so
   * they get their own bucket at the end of the day, labelled as undecided.
   *
   * The time lives in `tip`, an instant, and the day in `at`. They were one
   * field until the export was measured: `at` had always been a bare date, so
   * this read every game in every season as having no time at all, and the
   * whole scoreboard said "Scheduled" where it could have said when.
   */
  /* THE INSTANT A GAME STARTS, whichever league's payload this is.
   *
   * This module is the shared registry for all three leagues, so it meets two
   * payload shapes. The NFL export states the instant in `at` and the day in
   * `date`. Basketball states the day in `at` and the instant in `tip`, and
   * its `at` has only ever been a bare date -- which is the bug behind all of
   * this: reading the instant out of `at` alone meant every basketball game
   * in every season was a game with no known tip-off time.
   *
   * Ten characters is a date; longer is a timestamp.
   */
  function startInstant(game) {
    if (!game) return null;
    if (typeof game.tip === 'string' && game.tip.length > 10) return game.tip;
    if (typeof game.at === 'string' && game.at.length > 10) return game.at;
    return null;
  }

  function timeKnown(game) {
    if (!game) return false;
    if (game.time_tbd) return false;
    return startInstant(game) !== null;
  }

  /* The tip-off time in the reader's own timezone, or null.
   *
   * Deliberately reads `tip` and not `startInstant`: a league whose export
   * states a tip-off instant gets a time on the card, and one that does not
   * keeps saying "Scheduled" rather than having a clock appear on its pages
   * as a side effect of this change.
   *
   * The export states an instant and nothing else; which evening that is
   * belongs to whoever is reading. A timestamp the browser cannot parse
   * returns null rather than "Invalid Date".
   */
  function tipText(game, locale) {
    if (!game || game.time_tbd) return null;
    if (typeof game.tip !== 'string' || game.tip.length <= 10) return null;
    var when = new Date(game.tip);
    if (isNaN(when.getTime())) return null;
    try {
      return when.toLocaleTimeString(locale || undefined,
        { hour: 'numeric', minute: '2-digit' });
    } catch (error) {
      return null;
    }
  }

  /* The instant a game is ordered by within its day. A game whose time is
   * undecided has none, and callers put those last.
   */
  function tipKey(game) {
    return timeKnown(game) ? startInstant(game) : null;
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
      if (ka === 1) return String(a.id).localeCompare(String(b.id));
      if (tipKey(a) !== tipKey(b)) return tipKey(a) < tipKey(b) ? -1 : 1;
      return String(a.id).localeCompare(String(b.id));
    });
    return rows;
  }

  function filterGames(games, filter) {
    if (!filter || filter === 'all') return (games || []).slice();
    return (games || []).filter(function (game) {
      if (filter === 'final') return game.status === 'final';
      if (filter === 'upcoming') return game.status === 'scheduled';
      // No basketball live feed exists yet, so nothing is ever live. The
      // filter is kept so the control matches football's, and it honestly
      // returns nothing rather than guessing from a clock we do not have.
      if (filter === 'live') return false;
      return true;
    });
  }

  /* What a game card may say.
   *
   * There is no in-game clock for basketball: the archive holds final scores
   * and period points. So a card says scheduled, final, or a score with no
   * clock, and NEVER a zeroed clock, which reads as the period expiring.
   */
  function statusLine(game, locale) {
    if (!game) return '';
    if (game.status === 'final') return 'Final';
    if (game.status === 'cancelled') return 'Cancelled';
    if (game.status === 'postponed') return 'Postponed';
    // Under way, with no clock to show: the archive holds period points, not
    // a running clock, so this says that it has started and no more.
    if (game.status === 'in_progress') return 'In progress';
    // "Time TBD" is for a game whose time nobody has decided. A league whose
    // export states no tip-off instant keeps the old wording: it knows WHEN
    // the game is, it just has no time to put on the card.
    if (!timeKnown(game)) return 'Time TBD';
    var time = tipText(game, locale);
    return time === null ? 'Scheduled' : time;
  }

  function scoreLine(game) {
    if (!game) return null;
    if (game.home_points === null || game.home_points === undefined) {
      return null;
    }
    if (game.away_points === null || game.away_points === undefined) {
      return null;
    }
    return game.away_points + ' - ' + game.home_points;
  }

  /* The rating a page shows. Whole numbers at display, one decimal in the
   * data: football settled this, and a rounded value must never be what the
   * next calculation reads.
   */
  function ratingText(value) {
    if (value === null || value === undefined) return '—';
    var number = Number(value);
    return isFinite(number) ? String(Math.round(number)) : '—';
  }

  /* One club's season, oldest game first: every game it played or will
   * play, with the side it was on already worked out.
   *
   * A neutral-site game is NEITHER home nor away. Calling it home because
   * the provider listed it there would put a Final Four in a club's home
   * record, so `home` is false and `neutral` says why.
   */
  function teamSchedule(games, key) {
    var rows = [];
    (games || []).forEach(function (game) {
      if (game.home !== key && game.away !== key) return;
      var home = game.home === key;
      var neutral = Boolean(game.neutral);
      var points = home ? game.home_points : game.away_points;
      var against = home ? game.away_points : game.home_points;
      rows.push({
        game: game,
        opponent: home ? game.away : game.home,
        home: home && !neutral,
        neutral: neutral,
        points: points,
        against: against,
        result: decided(points, against)
      });
    });
    // THE DAY COMES FIRST. A scoreboard orders one day's games and rightly
    // puts an undecided tip-off at the end of it; a season is ordered by
    // date, and borrowing that rule here put a November game still to be
    // scheduled after a December game that has a time. Tip-off decides
    // only within a day.
    rows.sort(function (a, b) {
      var da = dayOf(a.game) || '';
      var db = dayOf(b.game) || '';
      // A game with no date at all cannot be placed, so it goes last
      // rather than sorting as the earliest game of the season.
      if (Boolean(da) !== Boolean(db)) return da ? -1 : 1;
      if (da !== db) return da < db ? -1 : 1;
      var ka = timeKnown(a.game) ? 0 : 1;
      var kb = timeKnown(b.game) ? 0 : 1;
      if (ka !== kb) return ka - kb;
      if (ka === 0 && tipKey(a.game) !== tipKey(b.game)) {
        return tipKey(a.game) < tipKey(b.game) ? -1 : 1;
      }
      return String(a.game.id).localeCompare(String(b.game.id));
    });
    return rows;
  }

  /* 'W', 'L' or null. A missing score is null, NEVER a loss: an unplayed
   * game is not a defeat.
   *
   * A LEVEL score is also null. Basketball plays overtime until somebody
   * wins, so a game the feed reports as level is bad data rather than a
   * draw -- and calling it a loss would be this page inventing a result.
   * The score is still shown; only the verdict is withheld.
   */
  function decided(points, against) {
    if (points === null || points === undefined) return null;
    if (against === null || against === undefined) return null;
    var mine = Number(points);
    var theirs = Number(against);
    if (!isFinite(mine) || !isFinite(theirs)) return null;
    if (mine === theirs) return null;
    return mine > theirs ? 'W' : 'L';
  }

  /* Wins and losses over the rows a schedule returned, counting only the
   * games that have a result. */
  function teamRecord(rows) {
    var record = { wins: 0, losses: 0 };
    (rows || []).forEach(function (row) {
      if (row.result === 'W') record.wins += 1;
      if (row.result === 'L') record.losses += 1;
    });
    return record;
  }

  function recordText(record) {
    var tally = record || { wins: 0, losses: 0 };
    return tally.wins + '-' + tally.losses;
  }

  /* A name reduced to what a reader typing it would get right: accents
   * folded, punctuation dropped, case and spacing ignored. So "St. John's"
   * is found by "st johns", and Hawai'i by "hawaii".
   *
   * The same rule the NFL page uses, deliberately: one house convention for
   * matching a name, not a third one per league.
   */
  function normalizeName(value) {
    var text = String(value === null || value === undefined ? '' : value);
    if (text.normalize) {
      text = text.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
    }
    return text.toLowerCase().replace(/[^a-z0-9 ]+/g, '')
      .replace(/\s+/g, ' ').trim();
  }

  /* Board rows matching a query, best first.
   *
   * A club is searched by every name the export gives it -- the display
   * name, the school and the abbreviation -- because a reader types "UNC"
   * as readily as "North Carolina", and the export carries both.
   *
   * A name that STARTS with the query beats one where any WORD does, which
   * beats one that merely contains it: typing "mia" should offer Miami
   * before it offers a club with "miami" buried mid-name. Inside a tier the
   * better-rated club comes first, since the board is the one ordering this
   * site has.
   */
  function searchTeams(board, teams, query, limit) {
    var wanted = normalizeName(query);
    if (!wanted) return [];
    var directory = teams || {};
    var found = [];
    (board || []).forEach(function (row) {
      var team = directory[row.team] || {};
      var tier = null;
      [team.name, team.school, team.short, team.abbr].forEach(
        function (value) {
        if (!value) return;
        var name = normalizeName(value);
        if (!name) return;
        var rank = null;
        if (name.indexOf(wanted) === 0) rank = 0;
        else if ((' ' + name).indexOf(' ' + wanted) >= 0) rank = 1;
        else if (name.indexOf(wanted) >= 0) rank = 2;
        if (rank !== null && (tier === null || rank < tier)) tier = rank;
      });
      if (tier !== null) found.push({ tier: tier, row: row });
    });
    found.sort(function (a, b) {
      if (a.tier !== b.tier) return a.tier - b.tier;
      var rating = Number(b.row.rating) - Number(a.row.rating);
      if (rating) return rating < 0 ? -1 : 1;
      return String(a.row.team).localeCompare(String(b.row.team));
    });
    return found.slice(0, limit || 25).map(function (item) {
      return item.row;
    });
  }

  function searchPlaceholder(slug) {
    if (slug === BASKETBALL) return 'Search college basketball…';
    if (slug === NFL) return 'Search the NFL…';
    return 'Search college football…';
  }

  var api = {
    FOOTBALL: FOOTBALL,
    BASKETBALL: BASKETBALL,
    NFL: NFL,
    LEAGUES: LEAGUES,
    LEAGUE_KEY: LEAGUE_KEY,
    FOOTBALL_FAVORITES_KEY: FOOTBALL_FAVORITES_KEY,
    league: league,
    known: known,
    readRoute: readRoute,
    writeRoute: writeRoute,
    sections: sections,
    TABS: TABS,
    tabs: tabs,
    tabLabel: tabLabel,
    launchedLeagues: launchedLeagues,
    isLaunched: isLaunched,
    hasSection: hasSection,
    switchLeague: switchLeague,
    rememberedLeague: rememberedLeague,
    rememberLeague: rememberLeague,
    openingLeague: openingLeague,
    favoritesKey: favoritesKey,
    readFavorites: readFavorites,
    writeFavorites: writeFavorites,
    toggleFavorite: toggleFavorite,
    crossLeagueOffer: crossLeagueOffer,
    seasonLabel: seasonLabel,
    dayOf: dayOf,
    gamesOn: gamesOn,
    dayStrip: dayStrip,
    NCAAMBB_DAY_ZONE: NCAAMBB_DAY_ZONE,
    todayKey: todayKey,
    openingDay: openingDay,
    PEOPLE_SHARDS: PEOPLE_SHARDS,
    peopleShard: peopleShard,
    playerKey: playerKey,
    providerIdOf: providerIdOf,
    playerSeasons: playerSeasons,
    playerTotals: playerTotals,
    perGame: perGame,
    searchPlayers: searchPlayers,
    seasonRange: seasonRange,
    timeKnown: timeKnown,
    startInstant: startInstant,
    tipText: tipText,
    tipKey: tipKey,
    sortGames: sortGames,
    filterGames: filterGames,
    statusLine: statusLine,
    scoreLine: scoreLine,
    ratingText: ratingText,
    teamSchedule: teamSchedule,
    teamRecord: teamRecord,
    recordText: recordText,
    normalizeName: normalizeName,
    searchTeams: searchTeams,
    searchPlaceholder: searchPlaceholder
  };

  if (typeof module === 'object' && module.exports) {
    module.exports = api;
  }
  root.ncaambbApp = api;
}(typeof globalThis === 'object' ? globalThis : this));
