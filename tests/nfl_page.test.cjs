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

function site(overrides, schedule) {
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

async function open(overrides, schedule) {
  const built = site(overrides, schedule);
  const file = path.join(built.root, 'nfl.html');
  const dom = new JSDOM(fs.readFileSync(file, 'utf8'), {
    url: 'file://' + file,
    runScripts: 'dangerously',
    resources: 'usable',
    pretendToBeVisual: true,
    beforeParse(window) {
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

test('a People tab is not offered for a league with no people', async () => {
  const tabs = [...require(path.join(UI, 'ncaambb_app.js')).sections('nfl')];
  assert.ok(!tabs.includes('people'));
  const dom = await open();
  const labels = [...dom.window.document.querySelectorAll('#tabs button')]
    .map(node => node.textContent);
  assert.ok(!labels.includes('People'));
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

// --- the page is not linked yet --------------------------------------------

test('no running football code links to the NFL page yet', () => {
  // The NFL page is complete and unlinked while Austin decides whether it
  // appears before basketball does. This asserts the staged-off state AND
  // that the parked markup is still present, so turning it on is a deliberate
  // edit and deleting it by accident fails rather than silently losing it.
  const shell = fs.readFileSync(
    path.join(UI, 'dashboard_shell.html'), 'utf8');
  const live = shell.replace(/<!--[\s\S]*?-->/g, '');
  assert.ok(!live.includes('nfl.html'),
    'the football shell links to nfl.html; if that is intended, invert this '
    + 'test rather than deleting it');
  assert.match(shell, /<a data-league="nfl" href="nfl\.html">NFL<\/a>/);
});
