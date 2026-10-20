// The unlinked basketball preview, driven as a page rather than as a module.
//
// This repository has learned twice that a green build is not a rendered page:
// the real check is whether the document ends up with rows in it. So these
// tests load ui/basketball.html in jsdom with its real scripts, a real
// manifest and real payload files on disk, and then read the DOM.
//
// They also drive the refusal path, because that is the behaviour release gate
// 4 asks for: a sample must produce no board at all, not a board with a
// warning above it.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { JSDOM } = require('jsdom');

const UI = path.join(__dirname, '..', 'ui');
const DAY = '2026-11-07';

function teams() {
  return {
    'ncaambb:cbbd:team:1': { name: 'Michigan', short: 'Michigan' },
    'ncaambb:cbbd:team:2': { name: 'Duke', short: 'Duke' },
    'ncaambb:cbbd:team:3': { name: 'BYU', short: 'BYU' }
  };
}

function games() {
  return [
    { id: 'g-timed', at: DAY + 'T19:00:00Z', status: 'scheduled',
      home: 'ncaambb:cbbd:team:1', away: 'ncaambb:cbbd:team:2',
      home_points: null, away_points: null, venue: 'Crisler Center',
      city: 'Ann Arbor', state: 'MI' },
    { id: 'g-tbd', at: DAY, status: 'scheduled', time_tbd: 1,
      home: 'ncaambb:cbbd:team:3', away: 'ncaambb:cbbd:team:1',
      home_points: null, away_points: null },
    { id: 'g-final', at: DAY + 'T17:00:00Z', status: 'final',
      home: 'ncaambb:cbbd:team:2', away: 'ncaambb:cbbd:team:3',
      home_points: 78, away_points: 61 }
  ];
}

function board() {
  return [
    { team: 'ncaambb:cbbd:team:1', conference: 'Big Ten', rating: 2273.7,
      rank: 1, wins: 30, losses: 4, d1_wins: 29, d1_losses: 4 },
    { team: 'ncaambb:cbbd:team:2', conference: 'ACC', rating: 2211.3,
      rank: 2, wins: 29, losses: 5, d1_wins: 29, d1_losses: 5 },
    { team: 'ncaambb:cbbd:team:3', conference: 'Big 12', rating: 1788.4,
      rank: 3, wins: 18, losses: 14, d1_wins: 17, d1_losses: 14 }
  ];
}

function manifest(overrides) {
  return Object.assign({
    schema_version: 1,
    sport: 'ncaambb',
    generated_at: new Date(Date.now() - 20 * 60000).toISOString(),
    latest_season: 2026,
    model: { name: 'ncaambb_elo_v1', fitted_on: [2024, 2025],
      held_out: [2026], holdout_brier: 0.18656, holdout_games: 5752 },
    coverage: [{ season: 2026, division_one_teams: 365, rated_teams: 364,
      games: 6318, finals: 6300 }],
    seasons: {},
    files: {}
  }, overrides || {});
}

function stamp(text) {
  return require('node:crypto').createHash('sha256')
    .update(text).digest('hex').slice(0, 10);
}

/* A whole export on disk, next to a copy of the page and its scripts. */
function site(overrides) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'cbb-preview-'));
  const data = path.join(root, 'data', 'ncaambb', 'seasons');
  fs.mkdirSync(data, { recursive: true });
  const seasonText = 'window.NCAAMBB_SEASONS=(window.NCAAMBB_SEASONS||{});'
    + 'window.NCAAMBB_SEASONS[2026]='
    + JSON.stringify({ season: 2026, board: board(), games: games() }) + ';\n';
  const teamsText = 'window.NCAAMBB_TEAMS=' + JSON.stringify(teams()) + ';\n';
  fs.writeFileSync(path.join(data, '2026.js'), seasonText);
  fs.writeFileSync(path.join(root, 'data', 'ncaambb', 'teams.js'), teamsText);
  const payload = manifest(overrides);
  payload.seasons = { 2026: 'seasons/2026.js?v=' + stamp(seasonText) };
  payload.files = { teams: 'teams.js?v=' + stamp(teamsText) };
  fs.writeFileSync(path.join(root, 'data', 'ncaambb', 'manifest.json'),
    JSON.stringify(payload));
  for (const name of ['basketball.html', 'ncaambb_loader.js',
    'ncaambb_app.js']) {
    fs.copyFileSync(path.join(UI, name), path.join(root, name));
  }
  return { root, manifest: payload };
}

async function open(overrides) {
  const built = site(overrides);
  const file = path.join(built.root, 'basketball.html');
  const dom = new JSDOM(fs.readFileSync(file, 'utf8'), {
    url: 'file://' + file,
    runScripts: 'dangerously',
    resources: 'usable',
    pretendToBeVisual: true,
    beforeParse(window) {
      // jsdom has no fetch. The page's one fetch is its manifest, served
      // from the same directory the scripts are.
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
    '#board tr, #refusal-list li'));
  return dom;
}

function settle(dom, ready, tries = 60) {
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

function rows(dom) {
  return [...dom.window.document.querySelectorAll('#board tr')]
    .map(tr => [...tr.children].map(td => td.textContent));
}

// --- the page draws what the export says -----------------------------------

test('the board renders, ranked, with ratings as whole numbers', async () => {
  const dom = await open();
  const table = rows(dom);
  assert.equal(table.length, 3);
  assert.deepEqual(table[0], ['1', 'Michigan', 'Big Ten', '2274', '30-4']);
  assert.deepEqual(table[2].slice(0, 2), ['3', 'BYU']);
  dom.window.close();
});

test('the league selector is there, with basketball selected', async () => {
  const dom = await open();
  const buttons = [...dom.window.document.querySelectorAll('#leagues button')];
  assert.deepEqual(buttons.map(b => b.textContent),
    ['College Football', "Men's Basketball"]);
  const pressed = buttons.filter(b => b.getAttribute('aria-pressed') === 'true');
  assert.equal(pressed.length, 1);
  assert.equal(pressed[0].textContent, "Men's Basketball");
  dom.window.close();
});

test('the Stats tab is present and disabled, not silently missing', async () => {
  // An empty tab is worse than an absent one, and a tab that vanishes is
  // worse than one that says "not yet".
  const dom = await open();
  const tabs = [...dom.window.document.querySelectorAll('#tabs button')];
  const stats = tabs.find(b => b.textContent === 'Stats');
  assert.ok(stats, 'Stats is listed');
  assert.equal(stats.disabled, true);
  assert.equal(tabs.find(b => b.textContent === 'Standings').disabled, false);
  dom.window.close();
});

test('search names the league it searches', async () => {
  const dom = await open();
  assert.match(dom.window.document.getElementById('search').placeholder,
    /college basketball/);
  dom.window.close();
});

test('a fresh export is badged with its age and not as stale', async () => {
  const dom = await open();
  const badge = dom.window.document.querySelector('#meta .badge');
  assert.equal(badge.classList.contains('stale'), false);
  assert.match(badge.textContent, /updated 20m ago/);
  dom.window.close();
});

test('the season label on the page is the 2025-26 form', async () => {
  const dom = await open();
  assert.match(dom.window.document.getElementById('meta').textContent,
    /2025-26/);
  dom.window.close();
});

test('the held-out score is on the page, next to the ratings it made', async () => {
  // A rating a reader cannot trace to a model scored on unseen games is
  // exactly what the gate exists to prevent.
  const dom = await open();
  const meta = dom.window.document.getElementById('meta').textContent;
  assert.match(meta, /ncaambb_elo_v1/);
  assert.match(meta, /held out 2026/);
  assert.match(meta, /0\.18656/);
  dom.window.close();
});

// --- the refusal path ------------------------------------------------------

test('a one-team sample draws no board at all', async () => {
  const dom = await open({
    coverage: [{ season: 2026, division_one_teams: 1, rated_teams: 1,
      games: 31 }]
  });
  const document = dom.window.document;
  assert.equal(document.getElementById('refusal').hidden, false);
  assert.equal(document.getElementById('standings').hidden, true);
  assert.equal(document.querySelectorAll('#board tr').length, 0);
  assert.match(document.getElementById('refusal-list').textContent,
    /sample rather than the country/);
  dom.window.close();
});

test('an unknown schema refuses, and says only that', async () => {
  const dom = await open({ schema_version: 2 });
  const list = dom.window.document.querySelectorAll('#refusal-list li');
  assert.equal(list.length, 1);
  assert.match(list[0].textContent, /does not understand export schema 2/);
  dom.window.close();
});

test('a stale export is still drawn, and badged red', async () => {
  const dom = await open({
    generated_at: new Date(Date.now() - 26 * 3600000).toISOString()
  });
  const document = dom.window.document;
  assert.equal(document.getElementById('refusal').hidden, true);
  assert.ok(document.querySelectorAll('#board tr').length >= 1,
    'the last good snapshot is still shown');
  const badge = document.querySelector('#meta .badge');
  assert.equal(badge.classList.contains('stale'), true);
  assert.match(badge.textContent, /Stale · updated 26h ago/);
  dom.window.close();
});

// --- the scoreboard --------------------------------------------------------

async function scoreboard(dom) {
  const document = dom.window.document;
  const games = [...document.querySelectorAll('#tabs button')]
    .find(button => button.textContent === 'Games');
  games.click();
  await settle(dom, () => document.querySelector('#cards .card'));
  return document;
}

test('a day of games renders, undecided tip-offs last', async () => {
  const dom = await open();
  const document = await scoreboard(dom);
  const cards = [...document.querySelectorAll('#cards .card')];
  assert.equal(cards.length, 3);
  const statuses = cards.map(card =>
    card.querySelector('.status').textContent);
  assert.deepEqual(statuses, ['Final', 'Scheduled', 'Time TBD']);
  dom.window.close();
});

test('a scheduled game shows no score, and a final one shows both', async () => {
  const dom = await open();
  const document = await scoreboard(dom);
  const cards = [...document.querySelectorAll('#cards .card')];
  const points = cards.map(card =>
    [...card.querySelectorAll('.pts')].map(node => node.textContent));
  assert.deepEqual(points[0], ['61', '78'], 'the final carries its score');
  assert.deepEqual(points[1], ['', ''], 'a scheduled game shows no zero');
  assert.deepEqual(points[2], ['', '']);
  dom.window.close();
});

test('no card anywhere shows a clock', async () => {
  // There is no basketball live feed. A 0:00 would read as a period expiring.
  const dom = await open();
  const document = await scoreboard(dom);
  assert.doesNotMatch(document.getElementById('cards').textContent,
    /\d+:\d\d/);
  dom.window.close();
});

test('the Live filter empties the day rather than guessing', async () => {
  const dom = await open();
  const document = await scoreboard(dom);
  [...document.querySelectorAll('#filters button')]
    .find(button => button.textContent === 'Live').click();
  assert.equal(document.querySelectorAll('#cards .card').length, 0);
  assert.match(document.getElementById('cards').textContent,
    /No games on this day/);
  dom.window.close();
});

test('the date strip centres Today and can move off it', async () => {
  const dom = await open();
  const document = await scoreboard(dom);
  const days = [...document.querySelectorAll('#days button')];
  assert.equal(days.length, 7);
  assert.equal(days[3].textContent, 'Today');
  assert.equal(days[3].getAttribute('aria-pressed'), 'true');
  days[4].click();
  await settle(dom, () => document.getElementById('cards')
    .textContent.includes('No games'));
  assert.equal(document.querySelectorAll('#cards .card').length, 0);
  dom.window.close();
});

// --- what the page must not do ---------------------------------------------

test('the football header reaches basketball, and football itself is unmoved', () => {
  // Gate 6: basketball is reachable. What must NOT change is football's own
  // routes — the selector is a link to another page, not a new route, so
  // every football link published to date opens the same page it always did.
  const shell = fs.readFileSync(path.join(UI, 'dashboard_shell.html'), 'utf8');
  assert.match(shell, /href="basketball\.html"/);
  assert.match(shell, /data-league="ncaambb"/);
  assert.match(shell, /id="league-selector"/);
  for (const football of ['#section=home', '#section=stats', '#section=rankings',
    '#section=teams', '#section=people']) {
    assert.ok(shell.includes(`href="${football}"`), `${football} still exists`);
  }
  assert.ok(!shell.includes('league=ncaambb'),
    'football gains no league parameter of its own');
});

test('both pages agree on where the remembered league is kept', () => {
  // Two files, one key. A drift here means tapping a league on one page and
  // being sent somewhere else by the other.
  const shell = fs.readFileSync(path.join(UI, 'dashboard_shell.html'), 'utf8');
  const app = require('../ui/ncaambb_app.js');
  assert.ok(shell.includes(app.LEAGUE_KEY));
});

test('the football page does not redirect a reader to basketball', () => {
  // A redirect would open somebody else's sport from a shared link, and move
  // the installed app's start URL on its own.
  const shell = fs.readFileSync(path.join(UI, 'dashboard_shell.html'), 'utf8');
  const selector = shell.slice(shell.indexOf('rememberLeagueChoice'));
  const block = selector.slice(0, selector.indexOf('}());'));
  for (const banned of ['location.href =', 'location.replace',
    'location.assign']) {
    assert.ok(!block.includes(banned), `${banned} must not appear`);
  }
});

test('the preview reads no football data file or global', () => {
  const page = fs.readFileSync(path.join(UI, 'basketball.html'), 'utf8');
  for (const banned of ['dashboard_data.json', 'static_manifest.json',
    'DASHBOARD_DATA', 'data/games/', 'elocast.favorites.v1']) {
    assert.ok(!page.includes(banned), `${banned} must not appear`);
  }
});

test("the selected league's colour outranks the site-wide link colour", () => {
  // It did not, at first: the shell paints every link amber through
  // `:root a:any-link:not(.active):not(.action-button)`, which is (0,3,1)
  // and beat `.leagues a[aria-current="page"]` at (0,2,1). The selected
  // league came out amber on amber -- a pill with no readable text, which no
  // DOM assertion noticed and a browser showed immediately.
  const shell = fs.readFileSync(path.join(UI, 'dashboard_shell.html'), 'utf8');
  const specificity = selector => {
    const ids = (selector.match(/#[\w-]+/g) || []).length;
    const classes = (selector.match(/\.[\w-]+|\[[^\]]+\]|:[\w-]+/g) || [])
      .length;
    const elements = (selector.match(/(^|[\s>+~(])[a-z]+\b/g) || []).length;
    return [ids, classes, elements];
  };
  const ruleFor = needle => {
    // The last occurrence: the same selector is quoted in a comment above
    // the rule it explains.
    const at = shell.lastIndexOf(needle);
    assert.ok(at > 0, `${needle} must still be in the shell`);
    const start = shell.lastIndexOf('\n', at) + 1;
    return shell.slice(start, shell.indexOf('{', at)).trim();
  };
  const global = specificity(
    ruleFor(':root a:any-link:not(.active):not(.action-button) {'));
  const selected = specificity(ruleFor('#league-selector a[aria-current'));
  assert.ok(selected > global || String(selected) > String(global),
    `${selected} must outrank ${global}`);
  assert.ok(selected[0] > global[0],
    'the id is what makes it win, not rule order');
});
