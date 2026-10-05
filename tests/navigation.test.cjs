// Dependency-free route regressions: node --test tests/navigation.test.cjs
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const CfbNav = require('../ui/navigation.js');
const { readRoute, hashFor, normalizeSearch } = CfbNav;
const config = {
  yearsAll: [1980, 2024, 2025, 2026], playoffYears: [2024, 2025, 2026],
  gameSeasons: [{ season: 1980, scheduled: 0 }, { season: 2025, scheduled: 0 }, { season: 2026, scheduled: 604 }]
};
const read = hash => readRoute(hash, config);

test('the local dashboard identifies the in-progress site release', () => {
  const shell = fs.readFileSync(path.join(__dirname, '../ui/dashboard_shell.html'), 'utf8');
  assert.match(shell, /class="release-label">v0\.1 · People and Places of the Game<\/p>/);
});

test('stadium pages have shareable routes with safe IDs', () => {
  const route = read('#section=stadiums&stadium=42&season=1980');
  assert.equal(route.section, 'teams');
  assert.equal(route.subview, 'stadiums');
  assert.equal(route.stadium, 42);
  assert.equal(hashFor(route), '#section=teams&view=stadiums&stadium=42');
  assert.equal(read('#section=teams&view=stadiums&stadium=42').stadium, 42);
  assert.equal(read('#section=stadiums&stadium=42%22%3E').stadium, null);
  assert.equal(read('#section=teams&stadium=42').stadium, null);
});

test('team pages link only verified current home stadiums, including shared homes', () => {
  const shell = fs.readFileSync(path.join(__dirname, '../ui/dashboard_shell.html'), 'utf8');
  const source = ['stadiumHref', 'teamHomeStadiums', 'stadiumLinks'].map(name => {
    const hit = shell.match(new RegExp('function ' + name + '\\([^]*?\\n\\}'))?.[0];
    assert.ok(hit, name);
    return hit;
  }).join('\n');
  const context = {esc: value => String(value).replaceAll('&', '&amp;')};
  vm.runInNewContext(source + '\nthis.homes = teamHomeStadiums; this.links = stadiumLinks;', context);
  const data = {stadiums: [
    {id: 42, name: 'Shared & Field', teams: [{id: 1}, {id: 2}]},
    {id: 43, name: 'Former home', teams: []},
  ]};
  assert.match(context.links(context.homes(data, '1')), /#section=teams&amp;view=stadiums&amp;stadium=42/);
  assert.match(context.links(context.homes(data, 2)), /Shared &amp; Field/);
  assert.equal(context.homes(data, 3).length, 0);
  assert.doesNotMatch(context.links(context.homes(data, 1)), /Former home/);
  assert.doesNotMatch(shell, /data-section="stadiums"/);
});

test('a team page fills its home stadium link after the stadium file loads', async () => {
  const shell = fs.readFileSync(path.join(__dirname, '../ui/dashboard_shell.html'), 'utf8');
  const source = ['stadiumHref', 'teamHomeStadiums', 'stadiumLinks', 'hydrateTeamStadium']
    .map(name => shell.match(new RegExp('function ' + name + '\\([^]*?\\n\\}'))?.[0]).join('\n');
  const target = {innerHTML: '', textContent: ''};
  const context = {esc: String, document: {getElementById: () => target},
    state: {view: 'team'}, renderVersion: 3,
    loadStadiumExplorer: () => Promise.resolve({stadiums: [
      {id: 42, name: 'Alpha Field', teams: [{id: 1}]},
    ]})};
  vm.runInNewContext(source + '\nthis.hydrate = hydrateTeamStadium;', context);
  context.hydrate(3, 1);
  await new Promise(resolve => setImmediate(resolve));
  assert.match(target.innerHTML, /Current home stadium: <a href="#section=teams&view=stadiums&stadium=42">Alpha Field<\/a>/);
  context.hydrate(3, 2);
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(target.innerHTML, 'Current home stadium not verified');
});

test('rendering a team page hydrates both its stadium and its head coach', () => {
  // Drives render()'s own team branch rather than matching the source text for a
  // call: a regex over the shell passes or fails on spelling, and said nothing
  // about whether the call happens.
  const shell = fs.readFileSync(path.join(__dirname, '../ui/dashboard_shell.html'), 'utf8');
  const source = shell.match(/function render\(\) \{[^]*?\n\}/)[0];
  const called = [];
  const context = {
    state: { view: 'team', teamParam: 'alpha' }, renderVersion: 0,
    document: { getElementById: () => ({ innerHTML: '', textContent: '' }) },
    renderNavigation: () => {}, renderYearRow: () => {}, wrapWideTables: () => {},
    renderTeamPage: () => '', hydrateTeamPage: () => {},
    resolveTeam: () => ({ id: 7, name: 'Alpha' }),
    hydrateTeamStadium: (v, id) => called.push(['stadium', id]),
    hydrateTeamCoach: (v, id) => called.push(['coach', id]),
    hydrateRatingFreshness: () => {}
  };
  vm.runInNewContext(source + '\nthis.render = render;', context);
  context.render();
  assert.deepEqual(called, [['stadium', 7], ['coach', 7]]);
});

test('stadium explorer and venue-aware previews keep team HFA separate from physical venue', () => {
  const shell = fs.readFileSync(path.join(__dirname, '../ui/dashboard_shell.html'), 'utf8');
  const funcs = ['hfaEvidence', 'stadiumHref', 'stadiumGameRow', 'stadiumMap',
    'stadiumDetails', 'stadiumExplorerHtml', 'gameVenuePanel'].map(name => {
    const source = shell.match(new RegExp('function ' + name + '\\([^]*?\\n\\}'))?.[0];
    assert.ok(source, name + ' exists');
    return source;
  }).join('\n');
  const context = {
    esc: value => String(value).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('"', '&quot;'),
    CfbGeography: require('../ui/geography.js'), CfbNavigation: { normalizeSearch }, TEAM_BY_ID: new Map([
      ['1', { name: 'Alpha' }], ['2', { name: 'Bravo' }]]),
    teamLink: name => name, gameHref: id => '#game=' + id,
    DATA: { current_hfa: { teams: [{ team: 'Alpha', hfa: 1.1, games: 10,
      effective_games: 8, weighted_actual_wins: 6, weighted_expected_wins: 5,
      prior_hfa: 1.0 }] } },
    STATIC_MANIFEST: { model_params: { elo: { home_field: 55 } } }
  };
  vm.runInNewContext(funcs + '\nthis.explorer = stadiumExplorerHtml; this.preview = gameVenuePanel;', context);
  const stadium = { id: 42, name: 'Alpha & Sons Field', city: 'Somewhere', state: 'UT',
    lat: 40, lon: -111, teams: [{ name: 'Alpha' }], upcoming: [], recent: [], completed_count: 0 };
  assert.match(context.explorer({ stadiums: [stadium] }, null), /Alpha &amp; Sons Field/);
  assert.match(context.explorer({ stadiums: [stadium] }, 42), /No upcoming FBS games/);
  assert.match(context.explorer({ stadiums: [stadium] }, 42), /not this stadium alone/);
  assert.match(context.preview({ stadium_id: 42, stadium: stadium.name, neutral: false,
    completed: false, phase: 0 }, 'Alpha'), /55 Elo points/);
  assert.match(context.preview({ stadium_id: 42, stadium: stadium.name, neutral: true,
    completed: false, phase: 0 }, 'Alpha'), /no home-field bonus/);
  assert.doesNotMatch(context.preview({ stadium_id: null, stadium: null, neutral: false,
    completed: false, phase: 0 }, 'Alpha'), /href="#section=teams&amp;view=stadiums/);
});

test('archived game cards display only resolved stadiums and retain neutral badges', () => {
  const shell = fs.readFileSync(path.join(__dirname, '../ui/dashboard_shell.html'), 'utf8');
  const source = shell.match(/function listingGame\(g, season\) \{[\s\S]*?\n\}/)?.[0];
  assert.ok(source);
  const context = {
    UPCOMING_BY_ID: new Map(), fmtGameDate: () => 'Saturday', fmtKickoff: () => 'Saturday',
    probPct: () => ({ home: 50, away: 50 }), TEAM_BY_ID: new Map([
      ['1', { name: 'Alpha' }], ['2', { name: 'Bravo' }]]),
    teamLink: name => name, esc: value => String(value).replaceAll('<', '&lt;'),
    gameHref: () => '#game=1'
  };
  vm.runInNewContext(source + '\nthis.card = listingGame;', context);
  const game = { game_id: 1, completed: true, home_id: 1, away_id: 2,
    home_score: 3, away_score: 7, phase: 0, neutral: true, stadium: 'Actual <Field>' };
  assert.match(context.card(game, 2026), /Neutral site/);
  assert.match(context.card(game, 2026), /Venue: Actual &lt;Field>/);
  assert.doesNotMatch(context.card({ ...game, stadium: null }, 2026), /Venue:/);
  assert.match(shell, /g\.stadium \? ' · Venue: ' \+ esc\(g\.stadium\)/);
});

test('home-field table is a current Standings view, not a historical season', () => {
  const route = read('#section=rankings&view=home-field&season=1980');
  assert.equal(route.subview, 'home-field');
  assert.equal(route.year, 2026);
  assert.equal(hashFor(route), '#section=rankings&view=home-field');
  const shell = fs.readFileSync(path.join(__dirname, '../ui/dashboard_shell.html'), 'utf8');
  const source = shell.match(/function renderHomeFieldStandings\(\) \{[\s\S]*?\n\}/)?.[0];
  const evidence = shell.match(/function hfaEvidence\(r\) \{[\s\S]*?\n\}/)?.[0];
  assert.ok(source);
  const context = {
    DATA: { current_hfa: { as_of: '2026-09-28', teams: [
      { team: 'BYU', hfa: 1.12, elo_points: 60.1, games: 240, effective_games: 54.4, points_at_bound: false },
      { team: 'Sacramento State', hfa: 1.04, elo_points: 20, games: 0, effective_games: 0, points_at_bound: false }
    ] } },
    esc: String, teamLink: name => name, CfbNavigation: { normalizeSearch }
  };
  vm.runInNewContext(evidence + '\n' + source + '\nthis.render = renderHomeFieldStandings;', context);
  const html = context.render();
  assert.match(html, /do not drive live Elo/);
  assert.match(html, /BYU.*1\.120×.*\+60\.1.*240.*54\.4/);
  assert.match(html, /Sacramento State.*1\.040×.*\+20\.0.*0.*0\.0/);
  assert.match(html, /Search all 2 teams/);
});

test('live CFBD IDs cannot relabel another school', () => {
  const shell = fs.readFileSync(path.join(__dirname, '../ui/dashboard_shell.html'), 'utf8');
  const source = shell.match(/function liveTeamId\(t\) \{[\s\S]*?\n\}/)?.[0];
  assert.ok(source, 'live team lookup exists');
  const teams = [
    { id: 47, name: 'Louisiana Tech' }, { id: 104, name: 'Rutgers' },
    { id: 10, name: 'Texas' }, { id: 11, name: 'Texas Tech' }
  ];
  const context = { resolveLiveTeam: require('../ui/live_teams.js').createResolver(teams) };
  vm.runInNewContext(source + '\nthis.lookup = liveTeamId;', context);
  assert.equal(context.lookup({ id: 47, name: 'Howard Bison' }), null);
  assert.equal(context.lookup({ id: 164, name: 'Rutgers Scarlet Knights' }).name, 'Rutgers');
  assert.equal(context.lookup({ id: 10, name: 'Texas Tech Red Raiders' }), null);
  assert.equal(context.lookup({ id: 2641, name: 'Texas Tech Red Raiders' }).name, 'Texas Tech');
});

test('an open live game refreshes status and score even with a cached snapshot', async () => {
  const shell = fs.readFileSync(path.join(__dirname, '../ui/dashboard_shell.html'), 'utf8');
  const source = shell.match(/function hydrateLiveGamePage\(version\) \{[\s\S]*?\n\}/)?.[0];
  assert.ok(source);
  const target = { innerHTML: '' };
  const game = (status, points) => ({ id: 42, status, home: { points } });
  let requests = 0;
  const context = {
    state: { view: 'game', gameParam: 42 }, renderVersion: 1, liveGameRequest: 0,
    liveSnapshot: { games: [game('in_progress', 28)] }, liveSeasonData: {},
    document: { getElementById: () => target },
    liveDetail: g => g.status + ':' + g.home.points,
    fetch: () => { requests++; return Promise.resolve({ ok: true,
      json: () => Promise.resolve({ games: [game('completed', 34)] }) }); },
    hydrateGamePage: () => assert.fail('live game should still be present')
  };
  vm.runInNewContext(source + '\nthis.refresh = hydrateLiveGamePage;', context);
  context.refresh(1);
  assert.equal(target.innerHTML, 'in_progress:28');
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(requests, 1);
  assert.equal(target.innerHTML, 'completed:34');
  context.refresh(1);
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(requests, 2);
  assert.equal(target.innerHTML, 'completed:34');
});

test('a live final opens the full archive after season data loads', async () => {
  const shell = fs.readFileSync(path.join(__dirname, '../ui/dashboard_shell.html'), 'utf8');
  const source = shell.match(/function hydrateLiveGamePage\(version\) \{[\s\S]*?\n\}/)[0];
  const game = {id: 42, status: 'completed', home: {points: 34}, away: {points: 21}};
  const archived = {completed: true, home_score: 34, away_score: 21};
  let archiveViews = 0;
  const target = {innerHTML: ''};
  const context = {
    state: {view: 'game', gameParam: 42}, renderVersion: 1, liveGameRequest: 0,
    liveSnapshot: {games: [game]}, liveSeasonData: null,
    document: {getElementById: () => target}, liveDetail: () => 'live-only',
    fetch: () => Promise.resolve({ok: true, json: () => Promise.resolve({games: [game]})}),
    liveSeason: () => 2026,
    loadSeasonGames: () => Promise.resolve({byId: new Map([[42, archived]])}),
    hydrateGamePage: () => {archiveViews++; target.innerHTML = 'efficiency and official Elo';}
  };
  vm.runInNewContext(source + '\nthis.refresh = hydrateLiveGamePage;', context);
  context.refresh(1);
  assert.equal(target.innerHTML, 'live-only');
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(archiveViews, 1);
  assert.equal(target.innerHTML, 'efficiency and official Elo');
  context.refresh(1);
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(target.innerHTML, 'efficiency and official Elo', 'polling must retain the full page');

  archived.home_score = 33;
  archiveViews = 0;
  context.refresh(1);
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(archiveViews, 0, 'a stale archived score must not replace the live final');
  assert.equal(target.innerHTML, 'live-only');

  archived.home_score = 34;
  game.status = 'in_progress';
  context.refresh(1);
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(archiveViews, 0, 'in-progress games stay live');
});

test('all eight legacy tabs retain their original destination', () => {
  for (const [tab, section, view] of [
    ['home', 'home', ''], ['teams', 'rankings', 'team-coe'], ['elo', 'rankings', 'elo'],
    ['conferences', 'rankings', 'conference-coe'], ['field', 'playoff', 'field'],
    ['bracket', 'playoff', 'bracket'], ['odds', 'playoff', 'odds'], ['history', 'playoff', 'history']
  ]) {
    const route = read('#tab=' + tab);
    assert.equal(route.section, section);
    assert.equal(route.subview, view);
  }
  assert.equal(read('#section=teams').section, 'teams');
  assert.equal(read('#section=teams&tab=elo').section, 'teams');
});

test('season, subview, status and query survive URL round trips', () => {
  for (const hash of ['#section=rankings&view=conference-coe&season=1980',
    '#section=rankings&view=conference-elo&season=2025',
    '#section=matchups&away=12&home=17&venue=neutral',
    '#section=playoff&view=bracket&season=2025', '#section=games&season=1980&status=completed',
    '#section=teams&q=Texas%20A%26M']) {
    assert.deepEqual(read(hashFor(read(hash))), read(hash));
  }
});

test('conference standings keeps its selected league in a shareable URL', () => {
  const route = read('#section=rankings&view=conference-standings&season=2025&conf=Big%2012');
  assert.equal(route.subview, 'conference-standings');
  assert.equal(route.conf, 'Big 12');
  assert.deepEqual(read(hashFor(route)), route);
  assert.equal(read('#section=rankings&view=elo&conf=Big%2012').conf, null);
  assert.equal(read('#section=rankings&view=conference-standings&conf=' +
    encodeURIComponent('SEC"><script>')).conf, null);
  assert.equal(read('#section=rankings').subview, 'elo');
});

test('conference standings sorts generated ranks and displays both records', () => {
  const shell = fs.readFileSync(path.join(__dirname, '../ui/dashboard_shell.html'), 'utf8');
  const source = shell.match(/function fillConferenceStandings\(cp\) \{[\s\S]*?\n\}/)?.[0];
  assert.ok(source);
  const fields = ['team_id', 'conf_rank', 'w', 'l', 't', 'conf_w', 'conf_l', 'conf_t'];
  const F = Object.fromEntries(fields.map((field, i) => [field, i]));
  const target = { innerHTML: '' };
  const context = {
    document: { getElementById: () => target },
    state: { year: 2025, conf: 'Big 12' },
    TEAM_BY_ID: new Map([['1', { name: 'Alpha' }], ['2', { name: 'Beta' }]]),
    esc: value => String(value),
    conferenceLink: name => name,
    teamLink: name => name,
    recStr: (w, l, t) => `${w}–${l}${t ? '–' + t : ''}`
  };
  vm.runInNewContext(source + '\nthis.fill = fillConferenceStandings;', context);
  const cp = {
    mf: F,
    raw: { independents: 'FBS Independents', conferences: [
      { name: 'FBS Independents', slug: 'ind' }, { name: 'Big 12', slug: 'big12' }
    ] },
    membersOf: (slug, year) => slug === 'big12' && year === 2025 ? [
      [2, 2, 8, 4, 0, 5, 3, 0], [1, 1, 11, 2, 0, 8, 1, 0]
    ] : []
  };
  context.fill(cp);
  assert.match(target.innerHTML, /Big 12.*2 members/);
  assert.ok(target.innerHTML.indexOf('Alpha</td>') < target.innerHTML.indexOf('Beta</td>'));
  assert.match(target.innerHTML, /Alpha<\/td><td class="num">8–1<\/td><td class="num">11–2/);
  assert.match(target.innerHTML, /Beta<\/td><td class="num">5–3<\/td><td class="num">8–4/);
  assert.doesNotMatch(target.innerHTML, /FBS Independents.*<option/);
  context.state.year = 1980;
  context.fill(cp);
  assert.match(target.innerHTML, /No derived conference standings for 1980/);
});

test('matchups have shareable teams and a guarded venue', () => {
  const route = read('#section=matchups&away=12&home=17&venue=away');
  assert.equal(route.matchupAway, 12);
  assert.equal(route.matchupHome, 17);
  assert.equal(route.venue, 'away');
  assert.equal(read('#section=matchups&away=bogus&venue=moon').matchupAway, null);
  assert.equal(read('#section=matchups&away=bogus&venue=moon').venue, 'home');
});

test('hypothetical odds use the scheduled game Elo formula and symmetric venue edge', () => {
  const shell = fs.readFileSync(path.join(__dirname, '../ui/dashboard_shell.html'), 'utf8');
  const source = shell.match(/function matchupProbability\(a, b, venue, cfg\) \{[\s\S]*?\n\}/)?.[0];
  assert.ok(source);
  const context = {};
  vm.runInNewContext(source + '\nthis.probability = matchupProbability;', context);
  const cfg = { scale: 400, home_field: 50 };
  assert.equal(context.probability(1500, 1500, 'neutral', cfg), 0.5);
  assert.ok(context.probability(1500, 1500, 'home', cfg) > 0.5);
  assert.equal(context.probability(1500, 1500, 'away', cfg), 1 - context.probability(1500, 1500, 'home', cfg));
  assert.ok(Math.abs(context.probability(1600, 1400, 'home', cfg) -
    1 / (1 + 10 ** ((1400 - 1600 - 50) / 400))) < 1e-12);
});

test('missing and invalid URLs have deterministic defaults', () => {
  assert.equal(read('').section, 'home');
  assert.equal(read('#section=nope').section, 'home');
  assert.equal(read('#section=__proto__').section, 'home');
  assert.equal(read('#tab=constructor').section, 'home');
  assert.equal(read('#section=rankings&view=odds').subview, 'elo');
  assert.equal(read('#section=playoff&season=1980').year, 2026);
  assert.equal(read('#section=games&season=NaN').year, 2026);
  assert.equal(read('#section=games&season=2025').status, 'completed');
  assert.equal(read('#section=games').status, 'upcoming');
  assert.equal(read('#section=games&season=2025&status=upcoming').status, 'upcoming');
});

test('team links carry a safe return destination including the selected season', () => {
  const from = '#section=rankings&view=team-coe&season=2025';
  const team = read('#team=byu&from=' + encodeURIComponent(from));
  assert.equal(team.view, 'team');
  assert.equal(team.section, 'teams');
  assert.equal(team.returnTo, from);
  assert.deepEqual(read(hashFor(team)), team);
  for (const from of ['https://example.com', 'javascript:alert(1)', '#team=byu', '#section=teams&team=byu']) {
    assert.equal(read('#team=byu&from=' + encodeURIComponent(from)).returnTo, '#section=teams');
  }
});

test('normalization handles punctuation, accents, and common name variants', () => {
  assert.equal(normalizeSearch('Texas A&M'), 'texasam');
  assert.equal(normalizeSearch("Hawaiʻi"), 'hawaii');
  assert.equal(normalizeSearch('San José State'), 'sanjosestate');
  assert.equal(normalizeSearch('Miami (FL)'), normalizeSearch('miami-fl'));
});

test('no-season exports stay usable without inventing a year', () => {
  const empty = { yearsAll: [], playoffYears: [], gameSeasons: [] };
  const route = readRoute('#section=games', empty);
  assert.equal(route.year, null);
  assert.equal(hashFor(route), '#section=home&view=games&mode=find');
});

test('Find a Game preserves two teams and an optional season, without changing legacy Games links', () => {
  const finder = read('#section=games&mode=find&school=byu&opponent=utah&findseason=2025');
  assert.equal(finder.finder, true);
  assert.equal(finder.team, 'byu');
  assert.equal(finder.opponent, 'utah');
  assert.equal(finder.findSeason, 2025);
  assert.deepEqual(read(hashFor(finder)), finder);
  assert.equal(read('#section=games').finder, true);
  assert.equal(read('#section=games&mode=find&findseason=3000').findSeason, null);
  assert.equal(read('#section=games&season=2025&status=completed').finder, false);
});

test('Find a Game shows head-to-head results across seasons without an endless list', async () => {
  const shell = fs.readFileSync(path.join(__dirname, '../ui/dashboard_shell.html'), 'utf8');
  const source = shell.match(/let finderCount = 10;[\s\S]*?\nfunction hydrateGames\(version\)/)?.[0];
  assert.ok(source);
  const filters = { innerHTML: '' }, results = { innerHTML: '' };
  const context = {
    state: { finder: true, team: 'byu', opponent: 'utah', findSeason: null },
    renderVersion: 1, gamesFocusAfterLoad: null,
    document: { getElementById: id => ({ 'games-filters': filters, 'games-results': results })[id] },
    loadSearchIndex: () => Promise.resolve({
      teams: [{ id: 1, name: 'BYU', slug: 'byu', aliases: [] },
        { id: 2, name: 'Utah', slug: 'utah', aliases: [] },
        { id: 3, name: 'Other', slug: 'other', aliases: [] }],
      seasons: [2014, 2015, 2016],
      games: [
        ...Array.from({ length: 12 }, (_, i) => [100 + i, i < 2 ? 2014 : 2015, i % 2 ? 2 : 1,
          i % 2 ? 1 : 2, '2015-09-01', 24, 17, 1]),
        [999, 2016, 1, 3, '2016-09-01', 30, 7, 1]
      ]
    }),
    esc: v => String(v), gameHref: (id, season) => '#game=' + id + '&season=' + season
  };
  vm.runInNewContext(source.replace(/\nfunction hydrateGames\(version\)$/, ''), context);
  context.hydrateGameFinder(1);
  await new Promise(resolve => setImmediate(resolve));
  assert.match(results.innerHTML, /12 games found/);
  assert.equal((results.innerHTML.match(/<li>/g) || []).length, 10);
  assert.match(results.innerHTML, /Show more \(2 remaining\)/);
  assert.doesNotMatch(results.innerHTML, /#game=999/);
});

test('Games routes use a status dropdown rather than the old status tabs', () => {
  const shell = fs.readFileSync(path.join(__dirname, '../ui/dashboard_shell.html'), 'utf8');
  const source = shell.match(/function renderGames\(\) \{[\s\S]*?\n\}/)?.[0];
  assert.ok(source);
  const context = { state: { finder: false, status: 'completed', year: 2025 }, esc: String };
  vm.runInNewContext(source + '\nthis.renderGames = renderGames;', context);
  const legacy = context.renderGames();
  assert.match(legacy, /<select id="games-status"/);
  assert.match(legacy, /value="completed" selected/);
  assert.doesNotMatch(legacy, /<nav class="subnav"/);
  context.state.finder = true;
  assert.doesNotMatch(context.renderGames(), /games-status|<nav class="subnav"/);
});

test('a games URL can point at one game; anything but digits is dropped', () => {
  const r = read('#section=games&season=2019&status=completed&game=401112233');
  assert.equal(r.game, 401112233);
  assert.deepEqual(read(hashFor(r)), r);
  for (const bad of ['abc', '12e4', '-5', '1234567890123', '<script>', '']) {
    assert.equal(read('#section=games&season=2019&game=' + encodeURIComponent(bad)).game, null);
  }
  assert.equal(read('#section=rankings&game=401112233').game, null);     // only Games keeps it
  assert.equal(read('#team=byu&game=401112233').game, null);
  assert.equal(hashFor(read('#section=games&season=2019&game=5')).includes('game=5'), true);
});

test('games filters: week, postseason, team, conference and "all" survive round trips', () => {
  for (const hash of ['#section=games&season=2025&status=all&week=7',
    '#section=games&season=2025&status=completed&week=post',
    '#section=games&season=2025&status=completed&school=texas-am&conf=SEC',
    '#section=games&season=2025&status=completed&conf=Big%2012&week=3&school=byu']) {
    const r = read(hash);
    assert.deepEqual(read(hashFor(r)), r);
  }
  const r = read('#section=games&season=2025&status=all&week=post&school=byu&conf=Mountain%20West');
  assert.equal(r.status, 'all'); assert.equal(r.week, 'post'); assert.equal(r.team, 'byu'); assert.equal(r.conf, 'Mountain West');
});

test('malformed games filters are dropped, and filters never leak into other sections', () => {
  const bad = read('#section=games&season=2025&week=abc&school=' + encodeURIComponent('<b>x</b>') +
    '&conf=' + encodeURIComponent('SEC"><script>') + '&status=weird');
  assert.equal(bad.week, null); assert.equal(bad.team, null); assert.equal(bad.conf, null);
  assert.equal(bad.status, 'completed');
  assert.equal(read('#section=games&season=2025&week=123').week, null);
  assert.equal(read('#section=games&season=2025&conf=' + 'x'.repeat(61)).conf, null);
  const other = read('#section=rankings&week=3&school=byu&conf=SEC');
  assert.equal(other.week, null); assert.equal(other.team, null); assert.equal(other.conf, null);
  const team = read('#team=byu&week=3');
  assert.equal(team.week, null);
});

test('the school filter never turns a Games URL into a team page', () => {
  const r = read('#section=games&season=2025&status=completed&school=byu');
  assert.equal(r.view, 'tab'); assert.equal(r.section, 'games'); assert.equal(r.team, 'byu');
  assert.equal(hashFor(r).includes('team='), false);
});

test('#game=<id> opens a game page in the Games section; the list highlight is unchanged', () => {
  const r = read('#game=401110869&season=2025');
  assert.equal(r.view, 'game'); assert.equal(r.section, 'games'); assert.equal(r.gameParam, 401110869);
  assert.equal(r.year, 2025); assert.equal(r.returnTo, '#section=games');
  assert.deepEqual(read(hashFor(r)), r);
  const list = read('#section=games&season=2025&status=completed&game=401110869');
  assert.equal(list.view, 'tab'); assert.equal(list.game, 401110869);
});

test('game pages: bad ids and seasons are dropped, return links stay safe', () => {
  assert.equal(read('#game=abc').gameParam, null);
  assert.equal(read('#game=401110869&season=1066').year, null);
  const from = '#section=home&view=games&season=2025&status=completed&week=post';
  const r = read('#game=5&from=' + encodeURIComponent(from));
  assert.equal(r.returnTo, from);
  assert.deepEqual(read(hashFor(r)), r);
  for (const bad of ['https://example.com', 'javascript:alert(1)', '#section=teams&team=byu', '#team=']) {
    assert.equal(read('#game=5&from=' + encodeURIComponent(bad)).returnTo, '#section=games');
  }
  assert.equal(read('#team=byu&game=5').view, 'team');           // a team link always wins
});

test('game and team pages return to each other, one level deep (URLs can never nest)', () => {
  const game = read('#game=7&season=2025&from=' + encodeURIComponent('#team=byu&from=' + encodeURIComponent('#section=rankings&view=elo&season=2020')));
  assert.equal(game.returnTo, '#team=byu');                       // the team page's own return is dropped
  const team = read('#team=byu&from=' + encodeURIComponent(hashFor(game)));
  assert.equal(team.returnTo, '#game=7&season=2025');             // ...and the game page's too
  let h = '#game=7&season=2025';
  for (let i = 0; i < 20; i++) {                                  // click back and forth 20 times
    const t = hashFor({ ...read('#team=byu'), returnTo: h });
    h = hashFor({ ...read('#game=7&season=2025'), returnTo: hashFor(read(t)) });
  }
  assert.ok(h.length < 120, 'return URLs stay short: ' + h.length);
  assert.equal(read('#team=byu&from=' + encodeURIComponent('#game=abc')).returnTo, '#section=teams');
});

test('the methodology page has a stable URL and ignores seasons', () => {
  const r = read('#section=methodology&season=1999');
  assert.equal(r.section, 'methodology'); assert.equal(r.view, 'tab');
  assert.equal(r.year, 2026);
  assert.equal(hashFor(r), '#section=methodology');
  assert.deepEqual(read(hashFor(r)), r);
});

// ---------------- Milestone E: detail-page tabs and conference pages ----------------

test('team pages carry a tab, defaulting to overview and left out of the URL', () => {
  const plain = read('#team=byu');
  assert.equal(plain.tab, 'overview');
  assert.equal(hashFor(plain), '#team=byu');
  for (const tab of ['schedule', 'history', 'analytics']) {
    const r = read('#team=byu&tab=' + tab);
    assert.equal(r.view, 'team');
    assert.equal(r.section, 'teams');
    assert.equal(r.tab, tab);
    assert.equal(hashFor(r), '#team=byu&tab=' + tab);
    assert.deepEqual(read(hashFor(r)), r);
  }
  // An unknown or conference-only tab falls back to the default, never to a blank page.
  for (const bad of ['', 'members', 'external', 'nope', '__proto__', 'constructor'])
    assert.equal(read('#team=byu&tab=' + bad).tab, 'overview');
});

test('a team-page tab never revives the pre-A1 tab names', () => {
  // #tab=history used to mean the Playoff history view; beside #team= it is the team's own tab.
  const r = read('#team=byu&tab=history');
  assert.equal(r.section, 'teams');
  assert.equal(r.view, 'team');
  assert.equal(r.tab, 'history');
  assert.equal(r.subview, '');
  assert.equal(read('#tab=history').section, 'playoff');      // unchanged without #team=
  assert.equal(read('#tab=history').subview, 'history');
});

test('#conference=<slug> opens a conference page in the Teams section', () => {
  const r = read('#conference=sec');
  assert.equal(r.view, 'conference');
  assert.equal(r.section, 'teams');
  assert.equal(r.conferenceParam, 'sec');
  assert.equal(r.tab, 'overview');
  assert.equal(r.returnTo, '#section=teams');
  assert.equal(r.year, 2026);
  assert.equal(hashFor(r), '#conference=sec');
  assert.deepEqual(read(hashFor(r)), r);
  for (const tab of ['members', 'history', 'external']) {
    const t = read('#conference=big-12&tab=' + tab);
    assert.equal(t.tab, tab);
    assert.deepEqual(read(hashFor(t)), t);
  }
  for (const bad of ['schedule', 'analytics', 'nope']) assert.equal(read('#conference=sec&tab=' + bad).tab, 'overview');
});

test('a conference page ignores the Games explorer filters and season', () => {
  const r = read('#conference=sec&season=1980&week=3&school=byu&conf=SEC&status=all&q=x&game=5');
  assert.equal(r.year, 2026);
  assert.equal(r.week, null); assert.equal(r.team, null); assert.equal(r.conf, null);
  assert.equal(r.query, ''); assert.equal(r.game, null); assert.equal(r.gameParam, null);
  assert.equal(hashFor(r), '#conference=sec');
});

test('a team parameter always wins, and #section= keeps its own meaning', () => {
  assert.equal(read('#team=byu&conference=sec').view, 'team');
  assert.equal(read('#team=byu&conference=sec').conferenceParam, null);
  assert.equal(read('#conference=sec&game=5').view, 'conference');
  // #section= is a section URL, so a stray conference parameter cannot hijack it.
  const s = read('#section=rankings&view=conference-coe&conference=sec');
  assert.equal(s.view, 'tab'); assert.equal(s.section, 'rankings'); assert.equal(s.conferenceParam, null);
});

test('team, conference and game pages return to each other, still one level deep', () => {
  const pages = ['#team=byu', '#conference=sec', '#game=7&season=2025'];
  for (const from of pages) {
    for (const target of pages) {
      if (target === from) continue;
      const r = read(target + '&from=' + encodeURIComponent(from));
      assert.equal(r.returnTo, from, target + ' <- ' + from);
      assert.deepEqual(read(hashFor(r)), r);
    }
  }
  // A page is never its own return target, and a nested return address is dropped.
  assert.equal(read('#conference=sec&from=' + encodeURIComponent('#conference=big-12')).returnTo, '#section=teams');
  const nested = '#team=byu&from=' + encodeURIComponent('#section=rankings&view=elo&season=2020');
  assert.equal(read('#conference=sec&from=' + encodeURIComponent(nested)).returnTo, '#team=byu');
  // ...and a tab on the return target is dropped too, so a return URL can't grow.
  assert.equal(read('#conference=sec&from=' + encodeURIComponent('#team=byu&tab=schedule')).returnTo, '#team=byu');
  let h = '#conference=sec&tab=external';
  for (let i = 0; i < 20; i++) {
    const t = hashFor({ ...read('#team=byu&tab=schedule'), returnTo: h });
    h = hashFor({ ...read('#conference=sec&tab=external'), returnTo: hashFor(read(t)) });
  }
  assert.ok(h.length < 120, 'return URLs stay short: ' + h.length);
});

test('conference pages reject unsafe and empty return addresses', () => {
  for (const bad of ['https://example.com', 'javascript:alert(1)', '#section=teams&team=byu',
    '#team=', '#game=abc', '#conference=', 'section=teams'])
    assert.equal(read('#conference=sec&from=' + encodeURIComponent(bad)).returnTo, '#section=teams');
});

test('section routes carry no detail-page state', () => {
  for (const hash of ['#section=home', '#section=games&season=2025&status=completed', '#section=rankings&view=elo']) {
    const r = read(hash);
    assert.equal(r.view, 'tab');
    assert.equal(r.tab, '');
    assert.equal(r.teamParam, null);
    assert.equal(r.conferenceParam, null);
  }
});

test('the CoE 2.0 conference view is a first-class rankings view', () => {
  const r = read('#section=rankings&view=conference-coe2&season=2025');
  assert.equal(r.section, 'rankings');
  assert.equal(r.subview, 'conference-coe2');
  assert.equal(r.year, 2025);
  // It round-trips, so the tab can be linked to and bookmarked.
  assert.equal(hashFor(r), '#section=rankings&view=conference-coe2&season=2025');
  // CoE v1's rolling value keeps its own URL: the two are different numbers and
  // must never collapse into one view.
  assert.equal(read('#section=rankings&view=conference-coe').subview, 'conference-coe');
  assert.equal(read('#tab=conferences').subview, 'conference-coe');
});

test('data coverage is a section with no season of its own', () => {
  const r = read('#section=coverage');
  assert.equal(r.section, 'coverage');
  assert.equal(r.subview, '');
  assert.equal(r.year, 2026, 'coverage shows every season, so it pins to the latest like the other season-less sections');
  assert.equal(hashFor(r), '#section=coverage');
  // A season in the URL is not part of this section's contract and is dropped.
  assert.equal(hashFor(read('#section=coverage&season=1980')), '#section=coverage');
});

test('an unknown view falls back to the section default, not to a blank page', () => {
  assert.equal(read('#section=rankings&view=conference-coe3').subview, 'elo');
  assert.equal(read('#section=coverage&view=anything').subview, '');
});

// ---- Week by week: the point-in-time Elo view (P1-05, P1-06) ----

test('a week-by-week stage survives a URL round trip', () => {
  for (const hash of ['#section=rankings&view=elo-weekly&season=2025&stage=w8',
    '#section=rankings&view=elo-weekly&season=2025&stage=pre',
    '#section=rankings&view=elo-weekly&season=2025&stage=post']) {
    assert.deepEqual(read(hashFor(read(hash))), read(hash));
    assert.equal(hashFor(read(hash)), hash);
  }
});

test('a malformed stage is dropped rather than carried into the page', () => {
  for (const bad of ['w', 'w123', 'week8', '8', 'PRE', '__proto__', 'w8; drop', '']) {
    const r = read('#section=rankings&view=elo-weekly&season=2025&stage=' + encodeURIComponent(bad));
    assert.equal(r.stage, null, bad);
    assert.equal(hashFor(r), '#section=rankings&view=elo-weekly&season=2025');
  }
});

test('stage belongs to the week-by-week view alone', () => {
  // Another rankings view, another section, and a detail page must all ignore it.
  assert.equal(read('#section=rankings&view=elo&season=2025&stage=w8').stage, null);
  assert.equal(read('#section=games&season=2025&stage=w8').stage, null);
  assert.equal(read('#team=byu&stage=w8').stage, null);
  assert.equal(read('#section=rankings&view=elo-weekly&season=2025&stage=w8&week=3').week, null);
});

test('the week-by-week view keeps every other rankings URL unchanged', () => {
  for (const hash of ['#section=rankings&view=elo&season=2025',
    '#section=rankings&view=conference-coe&season=1980', '#tab=elo', '#tab=conferences']) {
    const r = read(hash);
    assert.notEqual(r.subview, 'elo-weekly');
    assert.equal(r.stage, null);
  }
  // It is reachable, and it is not the default.
  assert.equal(read('#section=rankings&view=elo-weekly').subview, 'elo-weekly');
  assert.equal(read('#section=rankings').subview, 'elo');
});

test('live scoreboard has a stable section URL with no season selector', () => {
  const route = read('#section=live');
  assert.equal(route.section, 'live');
  assert.equal(hashFor(route), '#section=live');
});

test('Home rolls from old finals to the next scheduled week, while a matching live week stays live', () => {
  const shell = fs.readFileSync(path.join(__dirname, '../ui/dashboard_shell.html'), 'utf8');
  const source = shell.match(/function homeSlate\(feedGames\) \{[\s\S]*?\n\}/)?.[0];
  assert.ok(source, 'Home slate selection exists');
  const next = { id: 501, season: 2026, week: 5, kickoff: '2026-10-02T00:00:00Z',
    home: 1, away: 2, neutral: false };
  const context = {
    upcomingGroups: () => [{ label: 'Week 5', games: [next] }],
    TEAM_BY_ID: new Map([['1', { name: 'BYU' }], ['2', { name: 'Utah' }]])
  };
  vm.runInNewContext(source + '\nthis.choose = homeSlate;', context);
  const old = [{ id: 401, status: 'completed', start_date: '2026-09-26T19:00:00Z' }];
  const rollover = context.choose(old);
  assert.equal(rollover.label, 'Week 5');
  assert.equal(rollover.games.length, 1);
  assert.equal(rollover.games[0].id, 501);
  assert.equal(rollover.games[0].status, 'scheduled');
  assert.equal(rollover.games[0].home.name, 'BYU');
  assert.equal(context.choose([]).games[0].id, 501);
  const live = [{ id: 501, status: 'in_progress', start_date: next.kickoff }];
  assert.equal(context.choose(live).label, null);
  assert.equal(context.choose(live).games[0].status, 'in_progress');
  const late = [{ id: 502, status: 'in_progress', start_date: '2026-09-27T23:00:00Z' }];
  assert.equal(context.choose(late).label, null, 'an ongoing Sunday game remains visible');
});

test('home scoreboard leads with ranked, competitive games instead of kickoff order', () => {
  const shell = fs.readFileSync(path.join(__dirname, '../ui/dashboard_shell.html'), 'utf8');
  const score = shell.match(/function hypeScore\(g\) \{[\s\S]*?\n\}/)?.[0];
  const compare = shell.match(/function compareHype\(a, b\) \{[\s\S]*?\n\}/)?.[0];
  assert.ok(score && compare);
  const teams = new Map([
    ['Elite A', { id: 1 }], ['Elite B', { id: 2 }],
    ['Local A', { id: 3 }], ['Local B', { id: 4 }]
  ]);
  const context = {
    liveTeamId: t => teams.get(t.name),
    CURRENT_ELO: new Map([
      [1, { elo: 1810, rank: 2 }], [2, { elo: 1770, rank: 5 }],
      [3, { elo: 1420, rank: 75 }], [4, { elo: 1410, rank: 80 }]
    ]),
    livePrediction: g => ({ pHome: g.p })
  };
  vm.runInNewContext(score + '\n' + compare + '\nthis.compare = compareHype;', context);
  const early = { id: 10, home: { name: 'Local A' }, away: { name: 'Local B' },
    p: .5, start_date: '2026-10-03T16:00:00Z' };
  const marquee = { id: 11, home: { name: 'Elite A' }, away: { name: 'Elite B' },
    p: .52, start_date: '2026-10-03T23:00:00Z' };
  assert.ok(context.compare(marquee, early) < 0, 'top-five matchup goes first despite later kickoff');
  assert.ok(context.compare(early, marquee) > 0);
  const other = { ...marquee, id: 12, start_date: '2026-10-04T00:00:00Z' };
  assert.ok(context.compare(marquee, other) < 0, 'kickoff breaks equal-hype ties');
});

// ---- Player and coach pages (v0.1.1) ----------------------------------------

test('a player URL opens a player page and survives a round trip', () => {
  const r = read('#player=10155');
  assert.equal(r.view, 'player');
  assert.equal(r.playerParam, 10155);
  assert.equal(r.section, 'stats');          // the section a player belongs to
  assert.deepEqual(read(hashFor(r)), r);
  assert.match(hashFor(r), /player=10155/);
});

test('a coach URL opens a coach page and survives a round trip', () => {
  const r = read('#coach=518');
  assert.equal(r.view, 'coach');
  assert.equal(r.coachParam, 518);
  assert.equal(r.section, 'teams');
  assert.deepEqual(read(hashFor(r)), r);
  assert.match(hashFor(r), /coach=518/);
});

test('a person id is an integer or nothing: no name is ever an identity', () => {
  // The whole point of the id. A page addressed by name would break the moment
  // a source corrected a spelling, and two people sharing one would collide.
  for (const bad of ['Cade Klubnik', 'abc', '12e4', '12345678901234', '5.5', '<script>', '']) {
    assert.equal(read('#player=' + encodeURIComponent(bad)).playerParam, null,
      'player=' + bad + ' must not resolve to an id');
    assert.equal(read('#coach=' + encodeURIComponent(bad)).coachParam, null,
      'coach=' + bad + ' must not resolve to an id');
  }
  // An id is the SOURCE's own, so its shape is the source's: 29,162 CFBD
  // athlete ids in the archive are negative, and an id derived for a person
  // the feed does not number reaches 13 digits. Rejecting either would make
  // those people unreachable.
  for (const good of ['-1044360', '1044360', '1000250200058936'.slice(0, 13)]) {
    assert.equal(read('#player=' + good).playerParam, Number(good),
      'player=' + good + ' must resolve to an id');
    assert.equal(read('#coach=' + good).coachParam, Number(good),
      'coach=' + good + ' must resolve to an id');
  }
  // Still a player page, so it can say "not found" rather than falling through
  // to whatever section a bare #player= would otherwise land on.
  assert.equal(read('#player=abc').view, 'player');
  assert.equal(read('#coach=abc').view, 'coach');
});

test('a stray person parameter cannot hijack an explicit section URL', () => {
  const r = read('#section=rankings&view=elo&player=10155&coach=518');
  assert.equal(r.view, 'tab');
  assert.equal(r.section, 'rankings');
  assert.equal(r.playerParam, null);
  assert.equal(r.coachParam, null);
  // A team link beside it still wins, the way it does over a conference or game.
  assert.equal(read('#team=byu&player=10155').view, 'team');
});

test('a person page remembers where the reader came from, and will not nest', () => {
  // A section return address is canonicalized (it gains that section's default
  // parameters), so what matters is where it points, not its exact spelling.
  const fromStats = read('#player=10155&from=' + encodeURIComponent('#section=stats&view=players'));
  const back = read(fromStats.returnTo);
  assert.equal(back.section, 'stats');
  assert.equal(back.subview, 'players');
  const fromTeam = read('#coach=518&from=' + encodeURIComponent('#team=byu'));
  assert.equal(fromTeam.returnTo, '#team=byu');
  // A return address that is itself a person page keeps only the page, never its
  // own return address, so links cannot chain without limit.
  const nested = read('#player=1&from=' + encodeURIComponent('#coach=518&from=%23team%3Dbyu'));
  assert.equal(nested.returnTo, '#coach=518');
  // And a page can never be its own way back.
  assert.equal(read('#player=1&from=' + encodeURIComponent('#player=2')).returnTo,
    '#section=stats&view=players');
  assert.equal(read('#coach=1&from=' + encodeURIComponent('#coach=2')).returnTo, '#section=teams');
  assert.equal(read('#player=1&from=' + encodeURIComponent('https://example.com')).returnTo,
    '#section=stats&view=players');
});

test('person pages carry no tabs yet, and a tab parameter cannot invent one', () => {
  assert.deepEqual(CfbNav.pageTabs.player, []);
  assert.deepEqual(CfbNav.pageTabs.coach, []);
  assert.equal(read('#player=10155&tab=stats').tab, '');
  assert.equal(hashFor(read('#player=10155&tab=stats')).includes('tab='), false);
});

test('a negative athlete id loads the shard its data is actually in', async () => {
  // 29,162 of the archive's CFBD athlete ids are negative, and the exporter
  // puts them in the shard Python's floored modulo picks. JavaScript's
  // truncated modulo picks -56 for the same id, so the page would ask for a
  // file that does not exist and every one of those people would 404.
  const shell = fs.readFileSync(path.join(__dirname, '../ui/dashboard_shell.html'), 'utf8');
  const source = shell.match(/function loadPlayer\([^]*?\n\}/)[0];
  const asked = [];
  const context = {
    PEOPLE: {player_shards: 64, players: Object.fromEntries(
      Array.from({length: 64}, (_, i) => [String(i), 'player_' + i + '.js']))},
    loadDataScript: src => { asked.push(src); return Promise.resolve(); },
    window: {__CFB_PEOPLE_PLAYERS__: {56: {'-1044360': {display_name: 'Placeholder Id'}}}},
  };
  vm.runInNewContext(source + '\nthis.load = loadPlayer;', context);
  assert.deepEqual(await context.load(-1044360), {display_name: 'Placeholder Id'});
  assert.deepEqual(asked, ['player_56.js']);
});

test('a player page shows the season statistics attached to their athlete id', () => {
  // The panel exists because the season feed numbers every row with an athlete
  // id. Before it, a player page could only say who someone was.
  const shell = fs.readFileSync(path.join(__dirname, '../ui/dashboard_shell.html'), 'utf8');
  const source = ['statCell', 'playerStatsPanel'].map(name =>
    shell.match(new RegExp('function ' + name + '\\([^]*?\\n\\}'))[0]).join('\n');
  const context = {FRACTION_STATS: {PCT: true},
    esc: value => String(value).replaceAll('&', '&amp;'),
    teamCellById: id => 'TEAM' + id, personStatsNote: () => '<p>NO STATS</p>',
    STAT_CATEGORY_LABELS: {passing: 'Passing', fumbles: 'Fumbles'},
    STAT_CATEGORY_ORDER: ['passing', 'fumbles']};
  vm.runInNewContext(source + '\nthis.panel = playerStatsPanel;', context);

  const html = context.panel({
    2025: {passing: {team_id: 7, YDS: 4369, TD: 31}, fumbles: {team_id: 7, FUM: 2}},
    2024: {passing: {team_id: 7, YDS: 1200, TD: 9}},
  });
  // Newest season first, and a row per season inside one table per category.
  assert.match(html, /Passing[^]*?<td>2025<\/td><td>TEAM7<\/td>[^]*?4,369[^]*?<td>2024<\/td>/);
  assert.match(html, /Fumbles[^]*?<td>2025<\/td>/);
  // Categories in reading order, not alphabetical: a passer's page does not
  // open on his fumbles.
  assert.ok(html.indexOf('Passing') < html.indexOf('Fumbles'));
  assert.doesNotMatch(html, /NO STATS/);

  // A statistic the source wrote as text stays text; a missing one is a dash.
  assert.match(context.panel({2025: {passing: {team_id: 1, COMP: '19/30'}}}), /19\/30/);
  // A stat type one season has and another does not is a dash in that row, not
  // a missing cell that would shift the column under the wrong heading.
  assert.match(context.panel({2025: {passing: {team_id: 1, YDS: 5, TD: 1}},
                              2024: {passing: {team_id: 1, YDS: 3}}}), /&mdash;/);
  // A category the feed named but gave no statistics for is left out entirely:
  // a Season and Team table with no statistics in it says nothing.
  const bare = context.panel({2025: {passing: {team_id: 1, YDS: 5}, fumbles: {team_id: 1}}});
  assert.doesNotMatch(bare, /Fumbles/);
  // And with nothing at all, the page says why rather than showing an empty table.
  // The feed sends a 68.6% passer as 0.686. Printed as given under a PCT
  // header that reads as two thirds of one percent.
  assert.match(context.panel({2025: {passing: {team_id: 1, PCT: 0.686}}}), />68\.6%</);
  assert.equal(context.panel({}), '<p>NO STATS</p>');
  assert.equal(context.panel(undefined), '<p>NO STATS</p>');
});

test('a season a player has no statistics for is left out of their stat table', () => {
  const shell = fs.readFileSync(path.join(__dirname, '../ui/dashboard_shell.html'), 'utf8');
  const source = ['statCell', 'playerStatsPanel'].map(name =>
    shell.match(new RegExp('function ' + name + '\\([^]*?\\n\\}'))[0]).join('\n');
  const context = {esc: String, teamCellById: id => 'TEAM' + id, FRACTION_STATS: {PCT: true},
    personStatsNote: () => '<p>NO STATS</p>',
    STAT_CATEGORY_LABELS: {rushing: 'Rushing'}, STAT_CATEGORY_ORDER: ['rushing']};
  vm.runInNewContext(source + '\nthis.panel = playerStatsPanel;', context);
  const html = context.panel({2025: {rushing: {team_id: 1, YDS: 100}}, 2024: {}});
  assert.match(html, /<td>2025<\/td>/);
  assert.doesNotMatch(html, /<td>2024<\/td>/);
});

test('a name in the Stats table links by the same rule a box-score name does', () => {
  // One rule, one answer. A second rule here would mean the Stats page and the
  // game page could disagree about who a name belongs to.
  const shell = fs.readFileSync(path.join(__dirname, '../ui/dashboard_shell.html'), 'utf8');
  const source = ['rosterNameLookup', 'boxScorePlayerCell', 'playerHref'].map(name =>
    shell.match(new RegExp('function ' + name + '\\([^]*?\\n\\}'))[0]).join('\n');
  const context = {esc: String, state: {view: 'stats'}, CfbNavigation: {normalizeSearch: v =>
    String(v).toLowerCase().replace(/[^a-z ]/g, '').trim(),
    hashFor: r => '#player=' + (r.playerParam ?? '')}};
  vm.runInNewContext(source + '\nthis.lookup = rosterNameLookup; this.cell = boxScorePlayerCell;',
    context);
  const roster = {teams: new Map([[7, [
    {player_id: 4369001, name: 'Drew Mestemaker'},
    {player_id: 111, name: 'Same Name'},
    {player_id: 222, name: 'Same Name'},
  ]]])};

  const unique = context.lookup(roster, 7);
  assert.match(context.cell('Drew Mestemaker', unique), /#player=4369001/);
  // Two players on one roster share the name, so neither gets the link: a
  // leaderboard row must not guess which of them earned the statistic.
  assert.equal(context.cell('Same Name', unique), 'Same Name');
  // A team with no roster rows, and a season with no snapshot at all, leave the
  // name as plain text rather than failing the table.
  assert.equal(context.lookup(roster, 99), null);
  assert.equal(context.lookup(null, 7), null);
  assert.equal(context.cell('Drew Mestemaker', null), 'Drew Mestemaker');
  // A string team id from the row still finds its roster.
  assert.match(context.cell('Drew Mestemaker', context.lookup(roster, '7')), /#player=4369001/);
});

test('a season split across two athlete ids is marked on every table and named once', () => {
  // Measured on the real archive: 172 player-seasons across 2009-2025 carry
  // more than one CFBD athlete id under one name at one school. The totals a
  // page can show are then part of the season, and a page that prints them
  // bare asserts a fraction as the whole -- Sherod White's 2022 at New Mexico
  // reads 17 carries for 41 yards while CFBD's other record for that name
  // holds 23 for 101 and 3 touchdowns.
  const shell = fs.readFileSync(path.join(__dirname, '../ui/dashboard_shell.html'), 'utf8');
  const source = ['statCell', 'playerStatsPanel'].map(name =>
    shell.match(new RegExp('function ' + name + '\\([^]*?\\n\\}'))[0]).join('\n');
  const context = {FRACTION_STATS: {PCT: true}, esc: String,
    teamCellById: id => 'TEAM' + id, personStatsNote: () => '<p>NO STATS</p>',
    STAT_CATEGORY_LABELS: {rushing: 'Rushing', receiving: 'Receiving'},
    STAT_CATEGORY_ORDER: ['rushing', 'receiving']};
  vm.runInNewContext(source + '\nthis.panel = playerStatsPanel;', context);

  const html = context.panel({
    2022: {rushing: {team_id: 1, YDS: 41}, receiving: {team_id: 1, YDS: 20},
           _partial: 'CFBD lists more than one athlete id under this name at New Mexico in 2022'},
    2023: {rushing: {team_id: 1, YDS: 155}},
  });
  // Marked in BOTH categories: a reader looks at one table, not all of them.
  const marks = html.match(/<td>2022 <abbr class="stat-partial"/g) || [];
  assert.equal(marks.length, 2);
  assert.match(html, /title="CFBD lists more than one athlete id[^"]*New Mexico in 2022"/);
  // The season that is whole carries no mark.
  assert.match(html, /<td>2023<\/td>/);
  // And the reason is spelled out once, in words, not left to a dagger.
  assert.match(html, /In 2022, CFBD holds more than one athlete id/);
  assert.match(html, /part of that season rather than all of it/);
  assert.match(html, /not merged here/);

  // No caveat, no note and no dagger: the flag has to stay rare to mean anything.
  const clean = context.panel({2023: {rushing: {team_id: 1, YDS: 155}}});
  assert.doesNotMatch(clean, /stat-partial/);
  assert.doesNotMatch(clean, /CFBD holds more than one/);

  // Two flagged seasons read as a list, and the wording agrees with itself.
  const two = context.panel({
    2022: {rushing: {team_id: 1, YDS: 41}, _partial: 'a'},
    2021: {rushing: {team_id: 1, YDS: 30}, _partial: 'b'},
  });
  assert.match(two, /In 2022, 2021, CFBD holds/);
  assert.match(two, /part of those seasons rather than all of them/);
});
