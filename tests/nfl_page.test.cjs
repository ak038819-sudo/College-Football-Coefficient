// The NFL page, driven as a page rather than as a module.
//
// A green build is not a rendered page: this repository has learned that
// twice. So these load ui/nfl.html in jsdom with its real scripts, a real
// manifest and real payload files on disk, and then read the DOM. The
// refusal path is driven too, because an export that must not be shown has
// to produce no board at all rather than a board with a warning over it.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { JSDOM } = require('jsdom');

const UI = path.join(__dirname, '..', 'ui');
const loader = require(path.join(UI, 'nfl_loader.js'));
const app = require(path.join(UI, 'nfl_app.js'));

const KC = 'nfl:nflverse:franchise:KC';
const BUF = 'nfl:nflverse:franchise:BUF';
const LA = 'nfl:nflverse:franchise:LA';

function teams() {
  return {
    [KC]: { code: 'KC', name: 'Kansas City Chiefs', conference: 'AFC',
            division: 'West' },
    [BUF]: { code: 'BUF', name: 'Buffalo Bills', conference: 'AFC',
             division: 'East' },
    [LA]: { code: 'LA', name: 'Los Angeles Rams', conference: 'NFC',
            division: 'West' }
  };
}

function games() {
  return [
    { id: 'g1', date: '2026-09-13', at: '2026-09-13T17:00:00Z', week: 1,
      type: 'regular', home: KC, away: BUF, home_points: 24, away_points: 20,
      rateable: 1, venue: 'Arrowhead Stadium' },
    { id: 'g2', date: '2026-09-20', at: '2026-09-20T17:00:00Z', week: 2,
      type: 'regular', home: LA, away: KC, home_points: null,
      away_points: null, rateable: 0 },
    { id: 'g3', date: '2026-09-20', week: 2, time_tbd: 1, type: 'regular',
      home: BUF, away: LA, home_points: null, away_points: null,
      rateable: 0 },
    { id: 'g4', date: '2027-02-07', at: '2027-02-07T23:30:00Z', week: 1,
      type: 'superbowl', postseason: 1, neutral: 1, home: KC, away: LA,
      home_points: null, away_points: null, rateable: 0 }
  ];
}

function board() {
  return [
    { team: KC, code: 'KC', conference: 'AFC', division: 'West',
      rating: 1648.4, rank: 1, wins: 1, losses: 0, ties: 0, rated_games: 1 },
    { team: BUF, code: 'BUF', conference: 'AFC', division: 'East',
      rating: 1591.6, rank: 2, wins: 0, losses: 1, ties: 0, rated_games: 1 },
    { team: LA, code: 'LA', conference: 'NFC', division: 'West',
      rating: 1500.0, rank: 3, wins: 0, losses: 0, ties: 0, rated_games: 0 }
  ];
}

function manifest(overrides) {
  return Object.assign({
    schema_version: 1,
    sport: 'nfl',
    generated_at: new Date(Date.now() - 20 * 60000).toISOString(),
    latest_season: 2026,
    model: { name: 'nfl_elo_v1', fitted_on: [1999, 2020],
      held_out: [2021, 2025], holdout_brier: 0.22372, holdout_games: 1424 },
    coverage: [{ season: 2026, teams: 32, rated_teams: 3, games: 4,
      finals: 1, complete: false, played_teams: 3 }],
    seasons: {},
    files: {}
  }, overrides || {});
}

function stamp(text) {
  return require('node:crypto').createHash('sha256')
    .update(text).digest('hex').slice(0, 10);
}

function site(overrides, schedule, logos) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'nfl-page-'));
  const data = path.join(root, 'data', 'nfl', 'seasons');
  fs.mkdirSync(data, { recursive: true });
  const seasonText = 'window.NFL_SEASONS=(window.NFL_SEASONS||{});'
    + 'window.NFL_SEASONS[2026]='
    + JSON.stringify({ season: 2026, board: board(),
      games: schedule || games() }) + ';\n';
  const teamsText = 'window.NFL_TEAMS=' + JSON.stringify(teams()) + ';\n';
  fs.writeFileSync(path.join(data, '2026.js'), seasonText);
  fs.writeFileSync(path.join(root, 'data', 'nfl', 'teams.js'), teamsText);
  const payload = manifest(overrides);
  payload.seasons = { 2026: 'seasons/2026.js?v=' + stamp(seasonText) };
  payload.files = { teams: 'teams.js?v=' + stamp(teamsText) };
  fs.writeFileSync(path.join(root, 'data', 'nfl', 'manifest.json'),
    JSON.stringify(payload));
  for (const name of ['nfl.html', 'nfl_loader.js', 'nfl_app.js',
    'ncaambb_app.js']) {
    fs.copyFileSync(path.join(UI, name), path.join(root, name));
  }
  // The club marks live beside the page, not under data/nfl, because the
  // refresh replaces that directory wholesale.
  if (logos) {
    fs.writeFileSync(path.join(root, 'nfl_logo_assets.json'),
      JSON.stringify(logos));
  }
  return { root, manifest: payload };
}

// A page that works renders in about 80ms; this budget is 5 SECONDS because
// the number is not a claim about speed, it is only how long to wait before
// saying "never". `node --test` runs every file in parallel, so a jsdom page
// loading its scripts from disk competes with three other files doing the
// same, and a CI runner that is briefly busy can push one past a sub-second
// budget while the page is perfectly correct. That happened on main at
// 8ebbb63: two subtests failed with "the page never rendered" at 661ms, and
// a red test job means the deploy never runs, so the site cannot publish.
// Waiting longer costs nothing when the page renders and weakens no
// assertion when it does not -- a broken page still fails, just later.
function settle(dom, ready, tries = 500) {
  return new Promise((resolve, reject) => {
    let left = tries;
    const tick = () => {
      if (ready()) return resolve();
      if (left-- <= 0) return reject(new Error('the page never rendered'));
      setTimeout(tick, 10);
    };
    tick();
  });
}

// jsdom's own matchMedia never changes and has no way to fire a change, so
// a page that re-reads the theme can only be driven against a stub. This one
// answers the page's one query and hands the test `dom.setLight(true)` to
// flip the OS theme the way a reader does, listeners and all.
// `api` is which subscription shape the stub offers: 'modern' for
// addEventListener only, 'legacy' for addListener only (Safari before 14),
// and undefined for both. A stub that offers both cannot tell the two
// branches apart, so neither can a test using it.
function fakeMatchMedia(window, state, api) {
  const listeners = [];
  window.matchMedia = function (query) {
    const media = { media: query, get matches() { return state.light; } };
    if (api !== 'legacy') {
      media.addEventListener = function (kind, fn) {
        if (kind === 'change') listeners.push(fn);
      };
      media.removeEventListener = function (kind, fn) {
        const at = listeners.indexOf(fn);
        if (kind === 'change' && at >= 0) listeners.splice(at, 1);
      };
    }
    if (api !== 'modern') {
      media.addListener = function (fn) { listeners.push(fn); };
      media.removeListener = function (fn) {
        const at = listeners.indexOf(fn);
        if (at >= 0) listeners.splice(at, 1);
      };
    }
    return media;
  };
  return function setLight(light) {
    state.light = light;
    for (const fn of listeners.slice()) fn({ matches: light });
  };
}

async function open(overrides, schedule, logos, media) {
  const built = site(overrides, schedule, logos);
  const file = path.join(built.root, 'nfl.html');
  let setLight = null;
  const dom = new JSDOM(fs.readFileSync(file, 'utf8'), {
    url: 'file://' + file,
    runScripts: 'dangerously',
    resources: 'usable',
    pretendToBeVisual: true,
    beforeParse(window) {
      if (media) {
        setLight = fakeMatchMedia(window, { light: !!media.light }, media.api);
      }
      // jsdom has no fetch. The page's one fetch is its manifest, served
      // from the same directory its scripts are.
      window.fetch = function (url) {
        const target = path.join(built.root, String(url));
        return Promise.resolve({
          ok: fs.existsSync(target),
          json: () => Promise.resolve(
            JSON.parse(fs.readFileSync(target, 'utf8')))
        });
      };
    }
  });
  await settle(dom, () => dom.window.document.querySelector(
    '#cards .card, #refusal-list li'));
  dom.setLight = setLight;
  return dom;
}

function click(dom, selector) {
  const node = dom.window.document.querySelector(selector);
  assert.ok(node, 'no node for ' + selector);
  node.dispatchEvent(new dom.window.Event('click', { bubbles: true }));
}

// --- the page draws what the export says -----------------------------------

test('the scoreboard opens on the first week that is not finished',
  async () => {
    const dom = await open();
    const pressed = dom.window.document.querySelector(
      '#weeks button[aria-pressed="true"]');
    assert.equal(pressed.textContent, 'Wk 2');
    dom.window.close();
  });

test('a finished game shows its score and an unplayed one shows none',
  async () => {
    const dom = await open();
    click(dom, '#weeks button');            // week 1
    const card = dom.window.document.querySelector('#cards .card');
    assert.match(card.textContent, /Final/);
    assert.match(card.textContent, /24/);
    dom.window.close();
  });

test('a game with no kickoff time says so rather than showing midnight',
  async () => {
    const dom = await open();
    const text = dom.window.document.querySelector('#cards').textContent;
    assert.match(text, /Time TBD/);
    assert.doesNotMatch(text, /00:00/);
    dom.window.close();
  });

test('the rankings tab ranks teams with whole-number ratings', async () => {
  const dom = await open();
  click(dom, '#tabs button[data-tab="rankings"]');
  const rows = [...dom.window.document.querySelectorAll('#board tr')]
    .map(tr => [...tr.children].map(td => td.textContent));
  assert.equal(rows.length, 3);
  assert.deepEqual(rows[0],
    ['1', 'Kansas City Chiefs', 'AFC West', '1648', '1-0']);
  dom.window.close();
});

test('standings group by division and are ordered by record', async () => {
  const dom = await open();
  click(dom, '#tabs button[data-tab="standings"]');
  const headings = [...dom.window.document.querySelectorAll('#groups h3')]
    .map(node => node.textContent);
  assert.deepEqual(headings, ['AFC East', 'AFC West', 'NFC West']);
  dom.window.close();
});

test('the league selector offers the launched leagues, with the NFL selected',
  async () => {
    // This asserted a fixed list of three, basketball included. Codex found
    // that a reader could then tap through from here to a basketball page
    // with no export behind it, so the picker now renders only the launched
    // leagues (shared registry: `launchedLeagues`), plus the page's own. The
    // list is read from the registry rather than written out again, so
    // basketball reappears here on its own when its export ships.
    const leagues = require(path.join(UI, 'ncaambb_app.js'));
    const expected = leagues.launchedLeagues(leagues.NFL)
      .map(entry => entry.picker);
    const dom = await open();
    const buttons = [
      ...dom.window.document.querySelectorAll('#leagues button')];
    assert.deepEqual(buttons.map(b => b.textContent), expected);
    assert.ok(expected.includes('NFL') && expected.includes('College Football'),
      'football and the NFL are both launched, so both are offered');
    const pressed = buttons.filter(
      b => b.getAttribute('aria-pressed') === 'true');
    assert.equal(pressed.length, 1);
    assert.equal(pressed[0].textContent, 'NFL');
    dom.window.close();
  });

test('an export without the people layer says so on the Players tab',
  async () => {
    // This fixture's manifest names no `files.people`, which is every export
    // before schema 3 and the one on main until the first refresh after the
    // people layer merges. The tab must explain that, not draw an empty
    // search.
    assert.ok(require(path.join(UI, 'ncaambb_app.js'))
      .sections('nfl').includes('people'));
    const dom = await open();
    const document = dom.window.document;
    const tab = [...document.querySelectorAll('#tabs button')]
      .find(node => node.textContent === 'Players');
    assert.ok(tab && !tab.disabled, 'the Players tab is offered');
    tab.click();
    assert.equal(document.getElementById('people').hidden, false);
    assert.match(document.getElementById('people-body').textContent,
                 /does not carry the people layer/);
    dom.window.close();
  });

// --- the page refuses what must not be shown -------------------------------

test('a manifest for another sport draws no board at all', async () => {
  const dom = await open({ sport: 'ncaambb' });
  assert.equal(dom.window.document.querySelectorAll('#board tr').length, 0);
  assert.equal(dom.window.document.getElementById('refusal').hidden, false);
  assert.match(dom.window.document.getElementById('refusal-list').textContent,
    /not the NFL/);
  dom.window.close();
});

test('a season missing a franchise is refused, not shown short', async () => {
  const dom = await open({ coverage: [{ season: 2026, teams: 30,
    rated_teams: 30, games: 285, finals: 285, complete: true,
    played_teams: 30 }] });
  assert.equal(dom.window.document.querySelectorAll('#cards .card').length, 0);
  assert.match(dom.window.document.getElementById('refusal-list').textContent,
    /carries 30 teams and the league had 32/);
  dom.window.close();
});

test('a stale export is still drawn, badged with its age', async () => {
  const dom = await open({
    generated_at: new Date(Date.now() - 4 * 3600000).toISOString() });
  const badge = dom.window.document.querySelector('.badge');
  assert.match(badge.textContent, /Stale/);
  assert.ok(dom.window.document.querySelectorAll('#cards .card').length > 0);
  dom.window.close();
});

// --- the loader and the league rules, as modules ---------------------------

test('the loader refuses a schema it does not understand', () => {
  const reasons = loader.refusals({ schema_version: 9, sport: 'nfl' });
  assert.equal(reasons.length, 1);
  assert.match(reasons[0], /schema/);
});

test('the loader expects 31 teams before the 2002 expansion', () => {
  assert.equal(loader.expectedTeams(2001), 31);
  assert.equal(loader.expectedTeams(2002), 32);
  assert.deepEqual(loader.refusals(manifest({
    coverage: [{ season: 2001, teams: 31, rated_teams: 31, games: 259,
      finals: 259, complete: true, played_teams: 31 }] })), []);
});

test('the loader and the build gate agree on the Brier floor', () => {
  // Two gates that disagree are one gate: the stricter never runs.
  const python = fs.readFileSync(
    path.join(__dirname, '..', 'src', 'nfl', 'publish_gate.py'), 'utf8');
  const match = python.match(/MAX_HOLDOUT_BRIER = ([0-9.]+)/);
  assert.ok(match);
  assert.equal(Number(match[1]), loader.MAX_HOLDOUT_BRIER);
});

test('a record shows ties only when there are ties', () => {
  assert.equal(app.recordText({ wins: 10, losses: 6, ties: 0 }), '10-6');
  assert.equal(app.recordText({ wins: 10, losses: 6, ties: 1 }), '10-6-1');
});

test('a tie counts half in the win percentage', () => {
  assert.equal(app.winPct({ wins: 8, losses: 8, ties: 1 }), 8.5 / 17);
});

test('a postseason game is named, never numbered', () => {
  // The archive restarts week numbering in January, so "Week 1" on a Super
  // Bowl is exactly the bug this guards.
  assert.equal(app.weekLabel({ type: 'superbowl', week: 1 }), 'Super Bowl');
  assert.equal(app.weekLabel({ type: 'regular', week: 7 }), 'Week 7');
});

test('before 2002 the standings are one table, not eight divisions', () => {
  const groups = app.standings(board(), 2001);
  assert.equal(groups.length, 1);
  assert.equal(groups[0].division, null);
});

test('a historical code is shown where the game is shown', () => {
  assert.equal(app.gameCode({ home: LA, home_code: 'STL' }, 'home'), 'STL');
  assert.equal(app.gameCode({ home: LA }, 'home'), 'LA');
});

test('a game with no score is never final and never scores zero', () => {
  const game = { home_points: null, away_points: null };
  assert.equal(app.isFinal(game), false);
  assert.equal(app.scoreLine(game), null);
  assert.equal(app.statusLine(game), 'Time TBD');
});

// --- the page is linked from the football shell ----------------------------

test('the football header offers the NFL, as running markup', () => {
  // The selector went live once ui/data/nfl existed; before that the entry
  // was parked in a comment. The comments are stripped first because a parked
  // anchor reads identically to a live one in a plain text search, which is
  // how this test passed in both states before it was inverted.
  const shell = fs.readFileSync(
    path.join(UI, 'dashboard_shell.html'), 'utf8');
  const live = shell.replace(/<!--[\s\S]*?-->/g, '');
  assert.match(live, /<a data-league="nfl" href="nfl\.html">NFL<\/a>/,
    'the NFL entry must be live markup, not parked in a comment');
  // It belongs to the selector, not loose in the header: a link the nav does
  // not contain is not reached by the listener that remembers the choice.
  const nav = live.match(
    /<nav class="leagues" id="league-selector"[\s\S]*?<\/nav>/);
  assert.ok(nav, 'the league selector is live');
  assert.ok(nav[0].includes('href="nfl.html"'), 'and contains the NFL');
});

// --- the game card carries the model's own numbers -------------------------

test('a card shows each side\'s rank, record, rating and win probability',
  async () => {
    // The data for all four was already exported or stored; a card drew two
    // names and a score, which is what made the NFL page read as a stub
    // beside the college one.
    const schedule = games();
    schedule[0].home_elo = 1596.5;
    schedule[0].away_elo = 1621.9;
    schedule[0].home_wp = 0.5353;
    const dom = await open(null, schedule);
    // The page opens on the latest week with games, so step back to the one
    // this game is in rather than assuming the first card is it.
    click(dom, '#weeks button');            // week 1
    const card = dom.window.document.querySelector('#cards .card');
    const sides = card.querySelectorAll('.side');
    assert.equal(sides.length, 2);
    // Away first, as the card has always listed them.
    assert.match(sides[0].querySelector('b').textContent, /Buffalo Bills/);
    assert.equal(sides[0].querySelector('.sub').textContent,
      '#2 · 0-1 · Elo 1622');
    assert.equal(sides[1].querySelector('.sub').textContent,
      '#1 · 1-0 · Elo 1597');
    // The home side's probability is stored; the away side's is its exact
    // complement rather than a second rounding of the same number.
    assert.equal(sides[1].querySelector('.wp').textContent, '54%');
    assert.equal(sides[0].querySelector('.wp').textContent, '46%');
  });

test('the favourite is marked and the beaten side is dimmed', async () => {
  const schedule = games();
  schedule[0].home_wp = 0.5353;
  const dom = await open(null, schedule);
  click(dom, '#weeks button');              // week 1
  const sides = dom.window.document
    .querySelectorAll('#cards .card .side');
  // KC won 24-20 and was the favourite, so the home row carries the mark and
  // the away row is the dimmed one.
  assert.ok(sides[1].querySelector('.wp').classList.contains('lead'));
  assert.ok(!sides[0].querySelector('.wp').classList.contains('lead'));
  assert.ok(sides[0].classList.contains('lost'));
  assert.ok(!sides[1].classList.contains('lost'));
});

test('a card with no stored probability shows none, never a zero', async () => {
  // Number(null) is 0, and a card reading "0%" beside a team that went on to
  // win would be a confident lie rather than a missing number.
  const dom = await open();
  click(dom, '#weeks button');              // week 1
  const card = dom.window.document.querySelector('#cards .card');
  for (const side of card.querySelectorAll('.side')) {
    assert.equal(side.querySelector('.wp').textContent, '');
  }
});

test('the tabs sit under the header rather than after every panel',
  async () => {
    // The bar is fixed to the foot of the window at phone width and a row of
    // tabs above the content everywhere else, which only works if the nav is
    // inside the wrap and ahead of the panels in the document.
    const dom = await open();
    const doc = dom.window.document;
    const nav = doc.querySelector('nav.tabs');
    const header = doc.querySelector('header');
    const panel = doc.querySelector('#scoreboard');
    assert.equal(nav.parentElement, doc.querySelector('.wrap'));
    assert.ok(header.compareDocumentPosition(nav)
      & dom.window.Node.DOCUMENT_POSITION_FOLLOWING, 'nav follows the header');
    assert.ok(nav.compareDocumentPosition(panel)
      & dom.window.Node.DOCUMENT_POSITION_FOLLOWING, 'panels follow the nav');
  });

// --- the helpers behind those numbers --------------------------------------

test('a win probability of null is absent, not zero per cent', () => {
  assert.equal(app.winProbabilityText(null), null);
  assert.equal(app.winProbabilityText(undefined), null);
  assert.equal(app.winProbabilityText(0), '<1%');
  assert.equal(app.winProbabilityText(0.5), '50%');
  assert.equal(app.winProbabilityText(1), '>99%');
});

test('a tie has no loser and no favourite', () => {
  assert.equal(app.loserOf({ home_points: 20, away_points: 20 }), null);
  assert.equal(app.loserOf({ home_points: 24, away_points: 20 }), 'away');
  assert.equal(app.loserOf({ home_points: 20, away_points: 24 }), 'home');
  assert.equal(app.loserOf({ home_points: null, away_points: null }), null);
  assert.equal(app.sideFavoured({ home_wp: 0.5 }), null);
  assert.equal(app.sideFavoured({ home_wp: 0.51 }), 'home');
  assert.equal(app.sideFavoured({ home_wp: 0.49 }), 'away');
  assert.equal(app.sideFavoured({}), null);
});

// --- the club marks --------------------------------------------------------

const PIXEL = 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAf'
  + 'FcSJAAAAC0lEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==';

function logoFile() {
  return {
    [KC]: { light: PIXEL + '#kc-light', dark: PIXEL + '#kc-dark' },
    [BUF]: { light: PIXEL + '#buf-light', dark: PIXEL + '#buf-dark' }
  };
}

async function withLogos(schedule) {
  const dom = await open(null, schedule, logoFile());
  // The file is fetched after the first render, so wait for the re-render
  // that puts the marks on the page.
  await settle(dom, () => dom.window.document.querySelector('#cards img.mark'));
  return dom;
}

test('a card draws each club\'s mark, and keeps its column without one',
  async () => {
    const dom = await withLogos();
    click(dom, '#weeks button');                 // week 1: KC v BUF, both known
    const sides = dom.window.document
      .querySelectorAll('#cards .card .side');
    assert.equal(sides[0].querySelector('img.mark').getAttribute('src'),
      PIXEL + '#buf-dark');
    assert.equal(sides[1].querySelector('img.mark').getAttribute('src'),
      PIXEL + '#kc-dark');
    // LA has no entry in the file; its row keeps the grid rather than
    // sliding its name under the logo column.
    const second = dom.window.document.querySelectorAll('#cards .card')[0];
    assert.ok(second);
    click(dom, '#tabs button[data-tab="rankings"]');
    const marked = dom.window.document.querySelectorAll('#board img.mark');
    assert.equal(marked.length, 2, 'KC and BUF are marked, LA is not');
  });

test('a club with no mark gets no broken image', async () => {
  const dom = await withLogos();
  const rows = [...dom.window.document.querySelectorAll('#cards .card .side')];
  const la = rows.find(r => /Rams/.test(r.textContent));
  assert.ok(la, 'the Rams are on this week\'s slate');
  assert.equal(la.querySelector('img.mark'), null);
  assert.ok(la.classList.contains('nomark'));
});

test('the rank cell keeps the table layout when a mark is beside the name',
  async () => {
    // `td.rk { display: inline-block }` once took every rank cell out of the
    // table layout on the college side. The flex box here is the wrapper.
    const dom = await withLogos();
    click(dom, '#tabs button[data-tab="rankings"]');
    const cell = dom.window.document.querySelector('#board td.teamcell');
    assert.ok(cell.querySelector('span.with-mark'),
      'the mark and the name share a wrapper inside the cell');
  });

test('flipping the OS theme redraws the marks in the other cut', async () => {
  // The CSS repaints itself on prefers-color-scheme; the <img> src does not,
  // because it was chosen when the card was built. A reader who switches to
  // light mode with the page open would be left with the light-ink cut on a
  // light background, which is an invisible logo rather than a dim one.
  await flips({ light: false, api: 'modern' });
});

test('an engine with only the legacy addListener still gets the redraw',
  async () => {
    // Safari before 14 has no addEventListener on a MediaQueryList. Dropping
    // the fallback would leave those readers with an invisible logo, and a
    // stub that offers both APIs cannot tell the branches apart.
    await flips({ light: false, api: 'legacy' });
  });

// The flip itself, run against whichever subscription shape `media` offers.
async function flips(media) {
  const dom = await open(null, null, logoFile(), media);
  await settle(dom, () => dom.window.document.querySelector('#cards img.mark'));
  click(dom, '#weeks button');                   // week 1: KC v BUF
  const src = () => dom.window.document
    .querySelector('#cards img.mark').getAttribute('src');
  assert.equal(src(), PIXEL + '#buf-dark', 'the page starts dark');
  dom.setLight(true);
  await settle(dom, () => src() === PIXEL + '#buf-light');
  assert.equal(src(), PIXEL + '#buf-light');
  dom.setLight(false);
  await settle(dom, () => src() === PIXEL + '#buf-dark');
  assert.equal(src(), PIXEL + '#buf-dark', 'and back again');
  dom.window.close();
}

test('a theme flip before the marks land does not redraw for nothing',
  async () => {
    // The listener is wired at start, which is before the logo file has been
    // fetched. Firing it then must not throw on a null mark table.
    const dom = await open(null, null, null, { light: false });
    dom.setLight(true);
    click(dom, '#weeks button');
    assert.ok(dom.window.document.querySelector('#cards .card'),
      'the scoreboard still draws');
  });

test('the page works with no logo file at all', async () => {
  // The fetch 404s; nothing says so, because nothing the reader asked for is
  // missing.
  const dom = await open();
  click(dom, '#weeks button');
  const card = dom.window.document.querySelector('#cards .card');
  assert.ok(card, 'the scoreboard still draws');
  assert.equal(card.querySelector('img.mark'), null);
});
