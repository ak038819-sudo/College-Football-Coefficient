/* Shared URL contract for the static dashboard. No framework or build dependency. */
(function (root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.CfbNavigation = api;
})(typeof globalThis !== 'undefined' ? globalThis : this, function () {
  'use strict';
  const views = {
    home: [], games: [], teams: [],
    rankings: ['elo', 'team-coe', 'conference-coe', 'ap', 'cfp'],
    playoff: ['field', 'bracket', 'odds', 'history']
  };
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

  function readRoute(hash, config) {
    const p = new URLSearchParams(String(hash || '').replace(/^#/, ''));
    let section = p.get('section');
    let subview = p.get('view');
    if (!p.has('section') && has(legacy, p.get('tab'))) [section, subview] = legacy[p.get('tab')];
    if (!has(views, section)) section = 'home';
    if (!views[section].includes(subview)) subview = views[section][0] || '';
    const years = seasonsFor(section, config);
    let year = Number(p.get('season'));
    if (!p.has('season') || !years.includes(year)) year = latest(years);
    if (section === 'home' || section === 'teams') year = latest(config.yearsAll || []);
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
      game, week, team: teamFilter, conf, view: 'tab', teamParam: null, gameParam: null, returnTo: '' };
    // Existing team URLs remain valid. A team page belongs to the Teams section.
    if (p.has('team')) {
      route.view = 'team';
      route.section = 'teams';
      route.subview = '';
      route.query = '';
      route.game = null;
      route.week = null;
      route.team = null;
      route.conf = null;
      route.year = latest(config.yearsAll || []);
      route.teamParam = p.get('team');
      // Only accept a section URL or a game page as the return target; never an external URL.
      // A game page's own return address is dropped, so links can't nest without limit.
      const from = p.get('from');
      if (from && from.startsWith('#section=')) {
        const back = new URLSearchParams(from.slice(1));
        if (!back.has('team')) route.returnTo = hashFor(readRoute(from, config));
      } else if (from && from.startsWith('#game=')) {
        const g = readRoute(from, config);
        if (g.view === 'game' && g.gameParam != null) route.returnTo = hashFor({ ...g, returnTo: '#section=games' });
      }
      if (!route.returnTo) route.returnTo = '#section=teams';
    } else if (p.has('game') && !p.has('section')) {
      // A game page (Milestone C) belongs to the Games section. #section=games&...&game=
      // stays what it was: a season list with that game highlighted.
      const id = p.get('game') || '';
      route.view = 'game';
      route.section = 'games';
      route.subview = '';
      route.query = '';
      route.week = null; route.team = null; route.conf = null;
      route.gameParam = /^\d{1,12}$/.test(id) ? Number(id) : null;
      route.game = null;
      const s = Number(p.get('season'));
      route.year = (config.gameSeasons || []).some(x => x.season === s) ? s : null;
      // Return to a section URL or a team page (whose own return address is dropped).
      const from = p.get('from');
      if (from && from.startsWith('#section=')) {
        const back = new URLSearchParams(from.slice(1));
        if (!back.has('team')) route.returnTo = hashFor(readRoute(from, config));
      } else if (from && from.startsWith('#team=')) {
        const t = readRoute(from, config);
        if (t.view === 'team' && t.teamParam) route.returnTo = hashFor({ ...t, returnTo: '#section=teams' });
      }
      if (!route.returnTo) route.returnTo = '#section=games';
    }
    return route;
  }

  function hashFor(route) {
    const p = new URLSearchParams();
    if (route.view === 'team') {
      p.set('team', route.teamParam || '');
      if (route.returnTo && route.returnTo !== '#section=teams') p.set('from', route.returnTo);
    } else if (route.view === 'game') {
      p.set('game', route.gameParam == null ? '' : route.gameParam);
      if (route.year != null) p.set('season', route.year);
      if (route.returnTo && route.returnTo !== '#section=games') p.set('from', route.returnTo);
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

  return { readRoute, hashFor, seasonsFor, normalizeSearch };
});
