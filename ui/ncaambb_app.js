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
    // NOT launched: ui/data/ncaambb is not published yet, because the
    // provider's rolling allowance does not recover until 2026-10-27. Flip
    // this to true in the same commit that publishes the export, and not
    // before. See docs/basketball-ui.md.
    { slug: BASKETBALL, label: 'College Basketball', short: "Men's Basketball",
      picker: "Men's Basketball", launched: false },
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
    // The NFL export carries games, ratings, franchise records, head coaches
    // and the model's own track record. There are no PLAYERS: the archive
    // behind it is schedules and scores, so `people` stays absent and
    // `coaches` is its own section rather than a corner of a people page
    // that does not exist.
    nfl: ['home', 'standings', 'teams', 'games', 'rankings', 'coaches',
          'history']
  };

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
  function todayKey(now) {
    return (now instanceof Date ? now : new Date()).toISOString().slice(0, 10);
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
   * 2,669 of the 2026-27 season's 6,015 games carry startTimeTbd. Rendering an
   * unknown time as midnight would put them first and read as a real time, so
   * they get their own bucket at the end of the day, labelled as undecided.
   */
  function timeKnown(game) {
    if (!game) return false;
    if (game.time_tbd) return false;
    return typeof game.at === 'string' && game.at.length > 10;
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
      if (a.at !== b.at) return a.at < b.at ? -1 : 1;
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
  function statusLine(game) {
    if (!game) return '';
    if (game.status === 'final') return 'Final';
    if (game.status === 'cancelled') return 'Cancelled';
    if (game.status === 'postponed') return 'Postponed';
    if (!timeKnown(game)) return 'Time TBD';
    return 'Scheduled';
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
    todayKey: todayKey,
    openingDay: openingDay,
    timeKnown: timeKnown,
    sortGames: sortGames,
    filterGames: filterGames,
    statusLine: statusLine,
    scoreLine: scoreLine,
    ratingText: ratingText,
    searchPlaceholder: searchPlaceholder
  };

  if (typeof module === 'object' && module.exports) {
    module.exports = api;
  }
  root.ncaambbApp = api;
}(typeof globalThis === 'object' ? globalThis : this));
