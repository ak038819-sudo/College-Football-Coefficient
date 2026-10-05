/* Shared URL contract for the static dashboard. No framework or build dependency. */
(function (root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.CfbNavigation = api;
})(typeof globalThis !== 'undefined' ? globalThis : this, function () {
  'use strict';
  const views = {
    people: [], stats: ['overview', 'teams', 'players'], home: [], live: [], games: [], matchups: [], teams: ['', 'stadiums'], methodology: [], coverage: [],
    // 'conference-coe' is CoE v1's five-year rolling value, which INCLUDES the
    // current season and feeds the live playoff model. 'conference-coe2' is CoE
    // 2.0's frozen value ENTERING the season. Separate views with separate names,
    // for the same reason they are separate tables: they must never be read as
    // one number.
    rankings: ['elo', 'conference-standings', 'home-field', 'conference-elo', 'team-coe', 'conference-coe', 'conference-coe2', 'elo-weekly', 'ap', 'cfp'],
    playoff: ['field', 'bracket', 'odds', 'history']
  };
  // Detail-page tabs (Milestone E). The first entry is the default and is left out
  // of the URL, so every existing #team= link keeps opening the same page.
  const pageTabs = {
    team: ['overview', 'schedule', 'roster', 'stats', 'leaders', 'history', 'analytics'],
    conference: ['overview', 'members', 'history', 'external'],
    game: [],
    // People pages (v0.1.1) carry no tabs yet, like #game=. What is known about a
    // person today -- who they are, and which teams and seasons they appear in --
    // is one page's worth; per-game and per-season statistics are not attributed
    // to people at all. Tabs for them would be addresses pointing at empty
    // panels. The `tab` parameter still works for every kind, so adding them with
    // the stats layer does not change any URL published before it.
    player: [],
    coach: []
  };
  // Where each kind of detail page goes when it has no return address of its own.
  const pageHome = { team: '#section=teams', conference: '#section=teams', game: '#section=games',
    player: '#section=stats&view=players', coach: '#section=teams' };
  const pagePrefix = { team: '#team=', conference: '#conference=', game: '#game=',
    player: '#player=', coach: '#coach=' };
  const legacy = {
    home: ['home', ''], teams: ['rankings', 'team-coe'],
    elo: ['rankings', 'elo'], conferences: ['rankings', 'conference-coe'],
    field: ['playoff', 'field'], bracket: ['playoff', 'bracket'],
    odds: ['playoff', 'odds'], history: ['playoff', 'history']
  };
  const has = (obj, key) => Object.prototype.hasOwnProperty.call(obj, key);
  const latest = years => years.length ? Math.max(...years) : null;

  function seasonsFor(section, config) {
    const years = section === 'games' ? (config.gameSeasons || []).map(s => s.season)
      : section === 'playoff' ? config.playoffYears : config.yearsAll;
    return [...new Set((years || []).map(Number).filter(Number.isInteger))].sort((a, b) => a - b);
  }

  // A detail page's `from` may name a section URL or ANOTHER detail page, never an
  // external URL. The target's own return address is dropped, so links can't nest
  // without limit, and a page can never be its own return target.
  function safeReturn(from, config, kind) {
    const fallback = pageHome[kind];
    if (!from) return fallback;
    if (from.startsWith('#section=')) {
      const back = new URLSearchParams(from.slice(1));
      return back.has('team') ? fallback : hashFor(readRoute(from, config));
    }
    for (const other of ['team', 'conference', 'game', 'player', 'coach']) {
      if (other === kind || !from.startsWith(pagePrefix[other])) continue;
      const r = readRoute(from, config);
      const id = other === 'game' ? r.gameParam : r[other + 'Param'];
      return (r.view === other && id != null && id !== '')
        ? hashFor({ ...r, tab: '', returnTo: pageHome[other] }) : fallback;
    }
    return fallback;
  }

  function readRoute(hash, config) {
    const p = new URLSearchParams(String(hash || '').replace(/^#/, ''));
    let section = p.get('section');
    let subview = p.get('view');
    // Canonical destinations retain the existing feature controllers during migration.
    if (section === 'home' && subview === 'games') { section = 'games'; subview = ''; }
    if (section === 'stats' && subview === 'matchup') { section = 'matchups'; subview = ''; }
    if (section === 'stats' && subview === 'playoff') { section = 'playoff'; subview = p.get('tool') || 'field'; }
    if (section === 'standings') section = 'rankings';
    // Older stadium bookmarks stay valid after the explorer moves under Teams.
    if (section === 'stadiums') { section = 'teams'; subview = 'stadiums'; }
    // `tab` means a detail page's tab when one is open, and only otherwise the pre-A1 tab names.
    const isPage = p.has('team') || p.has('conference');
    if (!p.has('section') && !isPage && has(legacy, p.get('tab'))) [section, subview] = legacy[p.get('tab')];
    if (!has(views, section)) section = 'home';
    if (!views[section].includes(subview)) subview = views[section][0] || '';
    const years = seasonsFor(section, config);
    let year = Number(p.get('season'));
    if (!p.has('season') || !years.includes(year)) year = latest(years);
    if (section === 'home' || section === 'live' || section === 'matchups' || section === 'teams' || section === 'methodology' || section === 'coverage' ||
        (section === 'rankings' && subview === 'home-field'))
      year = latest(config.yearsAll || []);
    const season = (config.gameSeasons || []).find(s => s.season === year);
    // The Games landing page searches every season. Explicit old explorer URLs
    // keep their season/status/week filters and remain valid.
    const finder = section === 'games' && (p.get('mode') === 'find' ||
      (!p.has('season') && !p.has('status') && !p.has('week') && !p.has('conf') && !p.has('game')));
    const findSeason = (finder || section === 'home') && (config.gameSeasons || []).some(s => s.season === Number(p.get('findseason')))
      ? Number(p.get('findseason')) : null;
    const status = ['upcoming', 'live', 'completed', 'all'].includes(p.get('status')) ? p.get('status')
      : section === 'home' ? 'all' : season && season.scheduled > 0 ? 'upcoming' : 'completed';
    // Games may point at one game (search results; game pages build on this later). Digits only.
    const gameParam = p.get('game');
    const game = section === 'games' && /^\d{1,12}$/.test(gameParam || '') ? Number(gameParam) : null;
    // Games explorer filters (Milestone B). Format-checked here; the page checks them against the
    // season's actual weeks/teams/conferences and says so when one doesn't apply.
    const isGames = section === 'games' || section === 'home';
    const w = p.get('week') || '';
    const week = !isGames ? null : w === 'post' ? 'post' : /^\d{1,2}$/.test(w) ? Number(w) : null;
    // "school", not "team": #team=<slug> already means "open that team's page".
    const t = p.get('school') || '';
    const teamFilter = isGames && /^[a-z0-9-]{1,60}$/.test(t) ? t : null;
    const opponent = finder && /^[a-z0-9-]{1,60}$/.test(p.get('opponent') || '') ? p.get('opponent') : null;
    const c = (p.get('conf') || '').trim();
    const isConfStandings = section === 'rankings' && subview === 'conference-standings';
    const conf = (isGames || isConfStandings) && c.length <= 60 && /^[A-Za-z0-9 &().'-]+$/.test(c) ? c : null;
    // Which point in a season the week-by-week Elo table is showing (P1-05):
    // 'pre', 'w<week>' or 'post', the keys src/elo_timeline.py writes. Only the
    // shape is checked here; the page checks it against the season's real stages
    // and says so when one doesn't apply, exactly as the week filter does.
    const st = p.get('stage') || '';
    const stage = (section === 'rankings' && subview === 'elo-weekly' &&
      (st === 'pre' || st === 'post' || /^w\d{1,2}$/.test(st))) ? st : null;
    const matchupId = key => /^\d{1,10}$/.test(p.get(key) || '') ? Number(p.get(key)) : null;
    const venue = ['home', 'away', 'neutral'].includes(p.get('venue')) ? p.get('venue') : 'home';
    const route = { section, subview, year, status,
      slateWeek: /^\d{1,2}$/.test(p.get('slateweek') || '') ? Number(p.get('slateweek')) : p.get('slateweek') === 'post' ? 'post' : null,
      category: (p.get('category') || '').replace(/[^a-z-]/g, '').slice(0, 30),
      statTeam: (p.get('statTeam') || '').slice(0, 100), statConf: (p.get('statConf') || '').slice(0, 100),
      statQuery: (p.get('statQuery') || '').slice(0, 100),
      minimum: Math.max(0, Math.min(10000, Number(p.get('minimum')) || 0)),
      sort: (p.get('sort') || '').replace(/[^a-zA-Z0-9_]/g, '').slice(0, 40),
      dir: p.get('dir') === 'asc' ? 'asc' : 'desc', page: Math.max(1, Math.min(10000, Number(p.get('page')) || 1)),
      peopleKind: section === 'people' && ['player','coach'].includes(p.get('kind')) ? p.get('kind') : '',
      query: (section === 'people' || section === 'teams' && subview !== 'stadiums') ? (p.get('q') || '').trim().slice(0, 150) : '',
      game, week, team: teamFilter, opponent, finder, findSeason, conf, stage,
      stadium: section === 'teams' && subview === 'stadiums' && /^\d{1,10}$/.test(p.get('stadium') || '') ? Number(p.get('stadium')) : null,
      matchupHome: section === 'matchups' ? matchupId('home') : null,
      matchupAway: section === 'matchups' ? matchupId('away') : null,
      venue: section === 'matchups' ? venue : 'home', view: 'tab', teamParam: null, conferenceParam: null,
      gameParam: null, playerParam: null, coachParam: null, tab: '', returnTo: '' };
    const openPage = (kind, param) => {
      route.view = kind;
      route.subview = '';
      route.query = '';
      route.game = null;
      route.week = null; route.team = null; route.opponent = null; route.finder = false;
      route.findSeason = null; route.conf = null; route.stage = null;
      route[kind === 'game' ? 'gameParam' : kind + 'Param'] = param;
      const wanted = p.get('tab') || '';
      route.tab = pageTabs[kind].includes(wanted) ? wanted : (pageTabs[kind][0] || '');
      route.returnTo = safeReturn(p.get('from'), config, kind);
    };
    // Existing team URLs remain valid. Team and conference pages belong to the Teams
    // section; a team link always wins over a conference or game parameter beside it.
    if (p.has('team')) {
      route.section = 'teams';
      openPage('team', p.get('team'));
      route.year = latest(config.yearsAll || []);
    } else if (p.has('conference') && !p.has('section')) {
      // Conference pages (Milestone E): historical CoE, members, external performance.
      // Guarded like #game=, so a stray parameter can't hijack an explicit #section= URL.
      route.section = 'teams';
      openPage('conference', p.get('conference'));
      route.year = latest(config.yearsAll || []);
    } else if (p.has('player') && !p.has('section')) {
      // A player page belongs to the Stats section, whose players view is where
      // the leaderboards live. Ids are immutable integers, never names.
      const id = p.get('player') || '';
      route.section = 'stats';
      // A person id is the source's own: a CFBD athlete id can be negative, and
      // an id derived for a person the source does not number reaches 13 digits.
      openPage('player', /^-?\d{1,13}$/.test(id) ? Number(id) : null);
      route.year = latest(config.yearsAll || []);
    } else if (p.has('coach') && !p.has('section')) {
      const id = p.get('coach') || '';
      route.section = 'teams';
      openPage('coach', /^-?\d{1,13}$/.test(id) ? Number(id) : null);
      route.year = latest(config.yearsAll || []);
    } else if (p.has('game') && !p.has('section')) {
      // A game page (Milestone C) belongs to the Games section. #section=games&...&game=
      // stays what it was: a season list with that game highlighted.
      const id = p.get('game') || '';
      route.section = 'games';
      openPage('game', /^\d{1,12}$/.test(id) ? Number(id) : null);
      const s = Number(p.get('season'));
      route.year = (config.gameSeasons || []).some(x => x.season === s) ? s : null;
    }
    return route;
  }

  function hashFor(route) {
    const p = new URLSearchParams();
    const kind = route.view;
    if (kind === 'team' || kind === 'conference') {
      p.set(kind, (kind === 'team' ? route.teamParam : route.conferenceParam) || '');
      if (route.tab && route.tab !== pageTabs[kind][0]) p.set('tab', route.tab);
      if (route.returnTo && route.returnTo !== pageHome[kind]) p.set('from', route.returnTo);
    } else if (kind === 'player' || kind === 'coach') {
      // An id of 0 is not a valid person, but it is falsy -- so this checks for
      // null rather than truthiness, the way the game branch does.
      const id = kind === 'player' ? route.playerParam : route.coachParam;
      p.set(kind, id == null ? '' : id);
      if (route.tab && route.tab !== pageTabs[kind][0]) p.set('tab', route.tab);
      if (route.returnTo && route.returnTo !== pageHome[kind]) p.set('from', route.returnTo);
    } else if (kind === 'game') {
      p.set('game', route.gameParam == null ? '' : route.gameParam);
      if (route.year != null) p.set('season', route.year);
      if (route.returnTo && route.returnTo !== pageHome.game) p.set('from', route.returnTo);
    } else {
      p.set('section', route.section === 'games' ? 'home' : ['playoff', 'matchups'].includes(route.section) ? 'stats' : route.section);
      if (route.section === 'games') p.set('view', 'games');
      else if (route.section === 'matchups') p.set('view', 'matchup');
      else if (route.section === 'playoff') { p.set('view', 'playoff'); p.set('tool', route.subview || 'field'); }
      else if (route.subview) p.set('view', route.subview);
      if (route.section === 'home') {
        if (route.slateWeek != null) p.set('slateweek', route.slateWeek);
        if (route.findSeason != null) p.set('findseason', route.findSeason);
        if (route.week != null) p.set('week', route.week);
        if (route.team) p.set('school', route.team);
        if (route.conf) p.set('conf', route.conf);
        if (route.status && route.status !== 'all') p.set('status', route.status);
      }
      if (route.section === 'stats') {
        if (route.year != null) p.set('season', route.year);
        for (const key of ['category', 'statTeam', 'statConf', 'statQuery', 'minimum', 'sort', 'dir', 'page'])
          if (route[key] != null && route[key] !== '') p.set(key, route[key]);
      }
      if (route.year != null && ((route.section === 'games' && !route.finder) || (route.section === 'rankings' && route.subview !== 'home-field') ||
          (route.section === 'playoff' && route.subview !== 'history'))) p.set('season', route.year);
      if (route.section === 'games' && route.finder) {
        p.set('mode', 'find');
        if (route.team) p.set('school', route.team);
        if (route.opponent) p.set('opponent', route.opponent);
        if (route.findSeason != null) p.set('findseason', route.findSeason);
      } else if (route.section === 'games') p.set('status', route.status);
      if (route.section === 'games' && !route.finder) {
        if (route.week != null) p.set('week', route.week);
        if (route.team) p.set('school', route.team);
        if (route.conf) p.set('conf', route.conf);
        if (route.game != null) p.set('game', route.game);
      }
      if (route.section === 'rankings' && route.subview === 'conference-standings' && route.conf) p.set('conf', route.conf);
      if (route.section === 'rankings' && route.subview === 'elo-weekly' && route.stage) p.set('stage', route.stage);
      if (route.section === 'matchups') {
        if (route.matchupAway != null) p.set('away', route.matchupAway);
        if (route.matchupHome != null) p.set('home', route.matchupHome);
        if (route.venue !== 'home') p.set('venue', route.venue);
      }
      if (route.section === 'people' && route.peopleKind) p.set('kind',route.peopleKind);
      if ((route.section === 'people' || route.section === 'teams' && route.subview !== 'stadiums') && route.query) p.set('q', route.query);
      if (route.section === 'teams' && route.subview === 'stadiums' && route.stadium != null) p.set('stadium', route.stadium);
    }
    return '#' + p.toString();
  }

  function normalizeSearch(value) {
    return String(value || '').normalize('NFKD').replace(/[\u0300-\u036f]/g, '')
      .toLowerCase().replace(/[^a-z0-9]/g, '');
  }

  return { readRoute, hashFor, seasonsFor, normalizeSearch, pageTabs };
});
