/* Shared URL contract for the static dashboard. No framework or build dependency. */
(function (root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.CfbNavigation = api;
})(typeof globalThis !== 'undefined' ? globalThis : this, function () {
  'use strict';
  const views = {
    home: [], games: [], teams: [], methodology: [],
    rankings: ['elo', 'team-coe', 'conference-coe', 'ap', 'cfp'],
    playoff: ['field', 'bracket', 'odds', 'history']
  };
  // Detail-page tabs (Milestone E). The first entry is the default and is left out
  // of the URL, so every existing #team= link keeps opening the same page.
  const pageTabs = {
    team: ['overview', 'schedule', 'history', 'analytics'],
    conference: ['overview', 'members', 'history', 'external'],
    game: []
  };
  // Where each kind of detail page goes when it has no return address of its own.
  const pageHome = { team: '#section=teams', conference: '#section=teams', game: '#section=games' };
  const pagePrefix = { team: '#team=', conference: '#conference=', game: '#game=' };
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
    for (const other of ['team', 'conference', 'game']) {
      if (other === kind || !from.startsWith(pagePrefix[other])) continue;
      const r = readRoute(from, config);
      const id = other === 'game' ? r.gameParam : other === 'team' ? r.teamParam : r.conferenceParam;
      return (r.view === other && id != null && id !== '')
        ? hashFor({ ...r, tab: '', returnTo: pageHome[other] }) : fallback;
    }
    return fallback;
  }

  function readRoute(hash, config) {
    const p = new URLSearchParams(String(hash || '').replace(/^#/, ''));
    let section = p.get('section');
    let subview = p.get('view');
    // `tab` means a detail page's tab when one is open, and only otherwise the pre-A1 tab names.
    const isPage = p.has('team') || p.has('conference');
    if (!p.has('section') && !isPage && has(legacy, p.get('tab'))) [section, subview] = legacy[p.get('tab')];
    if (!has(views, section)) section = 'home';
    if (!views[section].includes(subview)) subview = views[section][0] || '';
    const years = seasonsFor(section, config);
    let year = Number(p.get('season'));
    if (!p.has('season') || !years.includes(year)) year = latest(years);
    if (section === 'home' || section === 'teams' || section === 'methodology') year = latest(config.yearsAll || []);
    const season = (config.gameSeasons || []).find(s => s.season === year);
    const status = ['upcoming', 'completed', 'all'].includes(p.get('status')) ? p.get('status')
      : season && season.scheduled > 0 ? 'upcoming' : 'completed';
    // Games may point at one game (search results; game pages build on this later). Digits only.
    const gameParam = p.get('game');
    const game = section === 'games' && /^\d{1,12}$/.test(gameParam || '') ? Number(gameParam) : null;
    // Games explorer filters (Milestone B). Format-checked here; the page checks them against the
    // season's actual weeks/teams/conferences and says so when one doesn't apply.
    const isGames = section === 'games';
    const w = p.get('week') || '';
    const week = !isGames ? null : w === 'post' ? 'post' : /^\d{1,2}$/.test(w) ? Number(w) : null;
    // "school", not "team": #team=<slug> already means "open that team's page".
    const t = p.get('school') || '';
    const teamFilter = isGames && /^[a-z0-9-]{1,60}$/.test(t) ? t : null;
    const c = (p.get('conf') || '').trim();
    const conf = isGames && c.length <= 60 && /^[A-Za-z0-9 &().'-]+$/.test(c) ? c : null;
    const route = { section, subview, year, status,
      query: section === 'teams' ? (p.get('q') || '').trim().slice(0, 150) : '',
      game, week, team: teamFilter, conf, view: 'tab', teamParam: null, conferenceParam: null,
      gameParam: null, tab: '', returnTo: '' };
    const openPage = (kind, param) => {
      route.view = kind;
      route.subview = '';
      route.query = '';
      route.game = null;
      route.week = null; route.team = null; route.conf = null;
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
    } else if (kind === 'game') {
      p.set('game', route.gameParam == null ? '' : route.gameParam);
      if (route.year != null) p.set('season', route.year);
      if (route.returnTo && route.returnTo !== pageHome.game) p.set('from', route.returnTo);
    } else {
      p.set('section', route.section);
      if (route.subview) p.set('view', route.subview);
      if (route.year != null && (route.section === 'games' || route.section === 'rankings' ||
          (route.section === 'playoff' && route.subview !== 'history'))) p.set('season', route.year);
      if (route.section === 'games') p.set('status', route.status);
      if (route.section === 'games') {
        if (route.week != null) p.set('week', route.week);
        if (route.team) p.set('school', route.team);
        if (route.conf) p.set('conf', route.conf);
        if (route.game != null) p.set('game', route.game);
      }
      if (route.section === 'teams' && route.query) p.set('q', route.query);
    }
    return '#' + p.toString();
  }

  function normalizeSearch(value) {
    return String(value || '').normalize('NFKD').replace(/[\u0300-\u036f]/g, '')
      .toLowerCase().replace(/[^a-z0-9]/g, '');
  }

  return { readRoute, hashFor, seasonsFor, normalizeSearch, pageTabs };
});
