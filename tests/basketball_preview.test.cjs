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
// The fixture's games are TODAY's, because the page opens on today and the
// strip labels only the real today. A fixed past date would make every
// scoreboard assertion here test the out-of-season fallback instead of the
// case a reader meets. Out of season is tested on its own, below.
const DAY = new Date().toISOString().slice(0, 10);

function daysFromToday(count) {
  return new Date(Date.now() + count * 86400000).toISOString().slice(0, 10);
}

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
function site(overrides, schedule) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'cbb-preview-'));
  const data = path.join(root, 'data', 'ncaambb', 'seasons');
  fs.mkdirSync(data, { recursive: true });
  const seasonText = 'window.NCAAMBB_SEASONS=(window.NCAAMBB_SEASONS||{});'
    + 'window.NCAAMBB_SEASONS[2026]='
    + JSON.stringify({ season: 2026, board: board(),
      games: schedule || games() }) + ';\n';
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

async function open(overrides, schedule) {
  const built = site(overrides, schedule);
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
  // The registry gained the NFL, so the selector offers three leagues. The
  // assertion is the full list rather than a membership check: a league
  // appearing here that has no page behind it is exactly the kind of thing
  // this file exists to catch.
  assert.deepEqual(buttons.map(b => b.textContent),
    ['College Football', "Men's Basketball", 'NFL']);
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

test('every enabled tab has a panel, and clicking one never empties the page',
  async () => {
    // Teams and People are real basketball sections -- the league holds both,
    // and switchLeague is right to carry them -- but this page draws only the
    // board and the scoreboard. They were enabled anyway, so clicking either
    // hid both panels and left a blank page: the exact thing Stats is
    // disabled to avoid.
    const dom = await open();
    const document = dom.window.document;
    const tabs = [...document.querySelectorAll('#tabs button')];
    const drawn = () => !document.getElementById('standings').hidden
      || !document.getElementById('scoreboard').hidden;
    assert.ok(drawn(), 'something is drawn to begin with');
    for (const button of tabs) {
      if (button.disabled) continue;
      button.click();
      assert.ok(drawn(), `${button.textContent} leaves the page empty`);
    }
    for (const name of ['Teams', 'People']) {
      const button = tabs.find(b => b.textContent === name);
      assert.ok(button, `${name} is still listed rather than vanishing`);
      assert.equal(button.disabled, true,
        `${name} has no panel on this page, so it must not be clickable`);
    }
    // The league itself still claims those sections: the page lacking a panel
    // is not a statement about the data, and a league switch must still carry
    // a reader on Teams to basketball's Teams once one exists.
    const app = require('../ui/ncaambb_app.js');
    for (const section of ['teams', 'people']) {
      assert.ok(app.hasSection(app.BASKETBALL, section),
        `${section} stays a basketball section`);
    }
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
  // Moving off today must not rename the day it moved to. The strip used to
  // mark its own centre as today, so the reader saw tomorrow labelled Today.
  const moved = [...document.querySelectorAll('#days button')];
  assert.equal(moved[3].textContent, daysFromToday(1).slice(5),
    'the new centre is labelled by its date');
  assert.deepEqual(moved.filter(button => button.textContent === 'Today')
    .map(button => button.getAttribute('aria-pressed')), ['false'],
    'exactly one day is still Today, and it is no longer the selected one');
  dom.window.close();
});

test('a season whose schedule runs months ahead still opens on today',
  async () => {
    // The whole published schedule is in the payload, so the last date in it
    // is the championship. Opening there -- and labelling it Today -- is what
    // this replaces.
    const far = games().map((entry, index) => Object.assign({}, entry, {
      at: index === 0 ? DAY + 'T19:00:00Z'
        : daysFromToday(120) + 'T19:00:00Z'
    }));
    const dom = await open({}, far);
    const document = await scoreboard(dom);
    const days = [...document.querySelectorAll('#days button')];
    assert.equal(days[3].textContent, 'Today');
    assert.equal(days[3].getAttribute('aria-pressed'), 'true');
    assert.equal(document.querySelectorAll('#cards .card').length, 1,
      "today's one game is drawn, not the ones in four months");
    dom.window.close();
  });

// --- what the page must not do ---------------------------------------------

// The selector is STAGED OFF: the basketball side is finished and tested, but
// its first export cannot be built until the provider's allowance recovers,
// and a visible league tab with no data behind it is worse than no tab. These
// three tests assert that staged-off state. Turning the selector on means
// inverting them, and the parked markup they check is what gets re-added.

const live = () => {
  // What the browser actually runs: HTML comments and commented-out JS lines
  // removed, so a parked block cannot satisfy an assertion about live code.
  const shell = fs.readFileSync(path.join(UI, 'dashboard_shell.html'), 'utf8');
  return shell.replace(/<!--[\s\S]*?-->/g, '')
    .split('\n').filter(line => !/^\s*\/\//.test(line)).join('\n');
};

test('the football header does not reach basketball, and football is unmoved',
  () => {
    const shell = fs.readFileSync(path.join(UI, 'dashboard_shell.html'), 'utf8');
    const running = live();
    for (const parked of ['href="basketball.html"', 'data-league="ncaambb"',
      '<nav class="leagues" id="league-selector"']) {
      assert.ok(!running.includes(parked), `${parked} must not be live`);
      assert.ok(shell.includes(parked),
        `${parked} must stay parked in a comment, so it can be re-added`);
    }
    // What must not change either way is football's own routes: the selector
    // was a link to another page, not a new route, so every football link
    // published to date opens the same page it always did.
    for (const football of ['#section=home', '#section=stats',
      '#section=rankings', '#section=teams', '#section=people']) {
      assert.ok(running.includes(`href="${football}"`), `${football} exists`);
    }
    assert.ok(!shell.includes('league=ncaambb'),
      'football gains no league parameter of its own');
  });

test('nothing on the football page remembers a league while it is off', () => {
  const app = require('../ui/ncaambb_app.js');
  assert.ok(!live().includes(app.LEAGUE_KEY),
    'no live code on the football page touches the remembered league');
  // The parked listener still has to agree with the basketball page on the
  // key, or re-enabling the selector reintroduces the drift this test was
  // written to catch: tapping a league on one page, being sent elsewhere by
  // the other.
  const shell = fs.readFileSync(path.join(UI, 'dashboard_shell.html'), 'utf8');
  assert.ok(shell.includes('rememberLeagueChoice'), 'the listener is parked');
  assert.ok(shell.includes(app.LEAGUE_KEY), 'and parked with the same key');
});

test('the football page does not redirect a reader to basketball', () => {
  // A redirect would open somebody else's sport from a shared link, and move
  // the installed app's start URL on its own. The whole file is searched,
  // comments included: a parked redirect is still the wrong thing to re-add.
  const shell = fs.readFileSync(path.join(UI, 'dashboard_shell.html'), 'utf8');
  for (const banned of ['location.href =', 'location.replace',
    'location.assign']) {
    assert.ok(!shell.includes(banned), `${banned} must not appear`);
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

// --- November, when the newest season is nearly empty -----------------------
//
// The state a reader actually meets on opening night: the whole Division I
// team list is known, the schedule is published, and almost nobody has played.
// The page must draw the handful of rated teams rather than refuse, and the
// flag on the coverage row is what makes the difference.

const LIVE_SEASON = { season: 2026, division_one_teams: 365, rated_teams: 3,
  games: 6318, finals: 1, played_teams: 3, complete: false };

test('a season still being played draws its board, thin as it is', async () => {
  const dom = await open({ coverage: [LIVE_SEASON] });
  assert.equal(rows(dom).length, 3);
  assert.equal(dom.window.document.querySelectorAll('#refusal-list li').length,
    0);
  dom.window.close();
});

test('the same coverage claiming to be a finished season draws no board',
  async () => {
    // Proves the test above is not passing because the fraction check was
    // removed: 3 of 365 rated is a refusal in a season that says it is over.
    const dom = await open({
      coverage: [Object.assign({}, LIVE_SEASON, { complete: true })]
    });
    assert.equal(rows(dom).length, 0);
    assert.ok(dom.window.document.querySelectorAll('#refusal-list li').length
      > 0);
    dom.window.close();
  });
