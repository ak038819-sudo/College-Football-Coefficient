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

async function open(overrides, schedule, hash) {
  const built = site(overrides, schedule);
  const file = path.join(built.root, 'basketball.html');
  const dom = new JSDOM(fs.readFileSync(file, 'utf8'), {
    // The hash is part of the URL the reader arrived with, so it is set here
    // rather than assigned afterwards: a tab the page opens on and a tab it
    // is moved to later are different claims, and this one is the link.
    url: 'file://' + file + (hash || ''),
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
  // Rendered means whichever panel this route asks for has content in it: a
  // page opened on Home draws cards and no board at all, so waiting for a
  // board row there would call a correct page "never rendered".
  await settle(dom, () => dom.window.document.querySelector(
    '#board tr, #refusal-list li, #cards .card, #team-body .directory, '
    + '#team-body .clubhead, #team-body .empty'));
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

test('the tab row is the five every league shows, in that order',
  async () => {
    // This page used to draw six, Games among them, which opened the same
    // scoreboard Home opens. One nav across the three leagues is the point.
    const app = require('../ui/ncaambb_app.js');
    const dom = await open();
    const drawn = [...dom.window.document.querySelectorAll('#tabs button')]
      .map(node => [node.dataset.tab, node.textContent]);
    assert.deepEqual(drawn, app.tabs());
    dom.window.close();
  });

test('a link to the Games section still opens the scoreboard', async () => {
  // Games was a tab here before the five, so links naming it are published.
  const dom = await open(undefined, undefined, '#league=ncaambb&section=games');
  await settle(dom, () => dom.window.document.querySelector('#cards .card'));
  assert.equal(dom.window.document.getElementById('scoreboard').hidden, false);
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
    // Teams and People are real basketball sections -- the league holds
    // both, and switchLeague is right to carry them -- but this page draws
    // no People panel. It was enabled anyway, so clicking it hid every
    // panel and left a blank page: the exact thing Stats is disabled to
    // avoid. Teams has a panel now and is expected to be live.
    const dom = await open();
    const document = dom.window.document;
    const tabs = [...document.querySelectorAll('#tabs button')];
    const drawn = () => !document.getElementById('standings').hidden
      || !document.getElementById('scoreboard').hidden
      || !document.getElementById('teams').hidden;
    assert.ok(drawn(), 'something is drawn to begin with');
    for (const button of tabs) {
      if (button.disabled) continue;
      button.click();
      assert.ok(drawn(), `${button.textContent} leaves the page empty`);
    }
    for (const [name, live] of [['Teams', true], ['People', false]]) {
      const button = tabs.find(b => b.textContent === name);
      assert.ok(button, `${name} is still listed rather than vanishing`);
      assert.equal(button.disabled, !live,
        live ? `${name} has a panel now and must be clickable`
          : `${name} has no panel on this page, so it must not be clickable`);
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
  // The scoreboard is Home now: Games was a sixth tab drawing the panel
  // Home already draws, and every league shows the same five.
  const games = [...document.querySelectorAll('#tabs button')]
    .find(button => button.textContent === 'Home');
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

// --- a tab is a route ------------------------------------------------------
//
// Every tab but the one the page opens on used to be unreachable: no link, no
// reload, no back button. These drive the real page rather than the helpers,
// because the defect was that the buttons never touched the URL at all.

function visible(dom) {
  const document = dom.window.document;
  return ['standings', 'scoreboard']
    .filter(id => !document.getElementById(id).hidden);
}

function pressed(dom) {
  const button = [...dom.window.document.querySelectorAll('#tabs button')]
    .find(b => b.getAttribute('aria-pressed') === 'true');
  return button ? button.textContent : null;
}

test('a link to a section opens on that section', async () => {
  const dom = await open(undefined, undefined, '#league=ncaambb&section=home');
  // Home draws the scoreboard; the page's own default draws the board. So
  // this asserts the hash won, not merely that something rendered.
  assert.deepEqual(visible(dom), ['scoreboard']);
  assert.equal(pressed(dom), 'Home');
  dom.window.close();
});

test('a link with no section opens on the page default', async () => {
  const dom = await open();
  assert.deepEqual(visible(dom), ['standings']);
  assert.equal(pressed(dom), 'Standings');
  dom.window.close();
});

test('pressing a tab puts it in the URL, in the shared league form',
  async () => {
    const dom = await open();
    const document = dom.window.document;
    const home = [...document.querySelectorAll('#tabs button')]
      .find(b => b.textContent === 'Home');
    home.click();
    // Home states no section, exactly as switchLeague writes it, so the two
    // ways of arriving at basketball's home screen are one string.
    assert.equal(dom.window.location.hash, '#league=ncaambb');
    const standings = [...document.querySelectorAll('#tabs button')]
      .find(b => b.textContent === 'Standings');
    standings.click();
    assert.equal(dom.window.location.hash,
      '#league=ncaambb&section=standings');
    dom.window.close();
  });

test('the back button returns to the tab before it', async () => {
  const dom = await open();
  const document = dom.window.document;
  const tab = name => [...document.querySelectorAll('#tabs button')]
    .find(b => b.textContent === name);
  tab('Home').click();
  assert.deepEqual(visible(dom), ['scoreboard']);
  tab('Standings').click();
  assert.deepEqual(visible(dom), ['standings']);
  dom.window.history.back();
  await settle(dom, () => dom.window.location.hash === '#league=ncaambb');
  await settle(dom, () => visible(dom).join() === 'scoreboard');
  assert.equal(pressed(dom), 'Home');
  dom.window.close();
});

test('Back to the bare URL restores the tab that URL reloads as',
  async () => {
    // The first history entry has no hash at all, so Back after the first tab
    // press lands on it. It used to leave the previous tab drawn under a URL
    // that reloads as the board -- the same address showing two things.
    const dom = await open();
    const document = dom.window.document;
    const home = [...document.querySelectorAll('#tabs button')]
      .find(b => b.textContent === 'Home');
    home.click();
    assert.deepEqual(visible(dom), ['scoreboard']);
    dom.window.history.back();
    await settle(dom, () => dom.window.location.hash === '');
    await settle(dom, () => visible(dom).join() === 'standings');
    assert.equal(pressed(dom), 'Standings');
    dom.window.close();
  });

test('the bare URL draws the same tab whether reloaded or returned to',
  async () => {
    // Two ways of being at the same address, which must agree.
    const reloaded = await open();
    const returned = await open();
    const home = [...returned.window.document.querySelectorAll('#tabs button')]
      .find(b => b.textContent === 'Home');
    home.click();
    returned.window.history.back();
    await settle(returned, () => returned.window.location.hash === '');
    await settle(returned, () => visible(returned).length > 0);
    assert.deepEqual(visible(returned), visible(reloaded));
    reloaded.window.close();
    returned.window.close();
  });

test('a hash naming a section this page cannot draw leaves the tab alone',
  async () => {
    // Stats is a disabled tab: there is no panel behind it. A link to it must
    // land on something rather than hiding every panel, which is the empty
    // page the disabled state exists to prevent.
    const dom = await open(undefined, undefined,
      '#league=ncaambb&section=stats');
    assert.deepEqual(visible(dom), ['standings']);
    dom.window.close();
  });

test('a hash naming another league is not read as a basketball section',
  async () => {
    const dom = await open(undefined, undefined,
      '#league=nfl&section=home');
    assert.deepEqual(visible(dom), ['standings']);
    dom.window.close();
  });

test("another league's hash leaves the tab alone, rather than resetting it",
  async () => {
    // This is the assertion the test above cannot make: it lands on the board,
    // which is also the default, so "ignored" and "reset to the default" look
    // identical there. Pressing a tab first tells them apart -- and the
    // difference matters, because the bare URL now legitimately means "the
    // default" and that must not spread to every hash this page can't read.
    const dom = await open();
    const home = [...dom.window.document.querySelectorAll('#tabs button')]
      .find(b => b.textContent === 'Home');
    home.click();
    assert.deepEqual(visible(dom), ['scoreboard']);
    // Wait for the page's own listener to have run, not merely for the
    // assignment: location.hash is set synchronously and the hashchange
    // comes later, so asserting straight after it passes whatever the page
    // does. This listener is added second, so it runs second.
    let seen = false;
    dom.window.addEventListener('hashchange', () => { seen = true; });
    dom.window.location.hash = '#league=nfl&section=standings';
    await settle(dom, () => seen);
    assert.deepEqual(visible(dom), ['scoreboard'],
      'a hash this page cannot read must not move the reader');
    dom.window.close();
  });

test('pressing a tab keeps the rest of the reader\'s route', async () => {
  const dom = await open(undefined, undefined,
    '#league=ncaambb&day=2026-01-02&section=home');
  const standings = [...dom.window.document.querySelectorAll('#tabs button')]
    .find(b => b.textContent === 'Standings');
  standings.click();
  // The day belongs to the reader, not to the tab they pressed.
  assert.match(dom.window.location.hash, /day=2026-01-02/);
  assert.match(dom.window.location.hash, /section=standings/);
  dom.window.close();
});

// --- what the page must not do ---------------------------------------------

// Basketball LAUNCHED on 2026-11-01, with the export the gate was waiting
// for. These tests were the staging: they asserted that no live markup sent a
// reader to basketball while it had no data, and that the entry was parked
// rather than deleted. Both halves have now been inverted, and the invariant
// they existed for is kept in the direction that is still a defect: **nothing
// offers a league whose export is not on disk**. That reads the files, not
// the flag, so the two cannot drift.

const live = () => {
  // What the browser actually runs: HTML comments and commented-out JS lines
  // removed, so a parked block cannot satisfy an assertion about live code.
  const shell = fs.readFileSync(path.join(UI, 'dashboard_shell.html'), 'utf8');
  return shell.replace(/<!--[\s\S]*?-->/g, '')
    .split('\n').filter(line => !/^\s*\/\//.test(line)).join('\n');
};

test('the football page offers basketball, and only because it has data',
  () => {
    const running = live();
    const published = fs.existsSync(
      path.join(UI, 'data', 'ncaambb', 'manifest.json'));
    for (const live_markup of ['href="basketball.html"',
      'data-league="ncaambb"']) {
      assert.ok(running.includes(live_markup),
        `${live_markup} is live now that basketball has an export`);
      // The direction that is still a defect, and the reason this test is
      // worth keeping rather than deleting with the staging it guarded.
      assert.ok(published,
        `${live_markup} is live with no export on disk behind it`);
    }
    // Football's own routes gain no league parameter: its hash is unchanged
    // forever, which is what keeps every published football link working.
    for (const parameter of ['#league=ncaambb', '&league=ncaambb']) {
      assert.ok(!running.includes(parameter),
        `football states no ${parameter} of its own`);
    }
  });

test("football's own routes are unmoved, selector on or off", () => {
  // The selector is a link to another page, not a new route, so every
  // football link published to date opens the same page it always did.
  const running = live();
  for (const football of ['#section=home', '#section=stats',
    '#section=rankings', '#section=teams', '#section=people']) {
    assert.ok(running.includes(`href="${football}"`), `${football} exists`);
  }
});

test('wherever the remembered league is kept, both pages agree on the key',
  () => {
    // Not "no code touches the key": once a selector exists for the leagues
    // that do have data, remembering the choice is correct. What must not
    // drift is WHICH key, or tapping a league on one page sends a reader
    // somewhere else from the other.
    const shell = fs.readFileSync(path.join(UI, 'dashboard_shell.html'), 'utf8');
    const app = require('../ui/ncaambb_app.js');
    assert.ok(shell.includes('rememberLeagueChoice'),
      'the listener is present, parked or live');
    assert.ok(shell.includes(app.LEAGUE_KEY),
      'and uses the same key as the basketball page');
  });

/* Every page that draws a league picker, driven for real.
 *
 * The source-level check above reads the football shell, and a check that
 * reads one file cannot see a second page offering the same league: the NFL
 * page builds its picker in JavaScript from the shared registry, so gating
 * the football header left a reader able to tap through to basketball from
 * there instead. Codex caught that on this very PR. This drives each page and
 * asks the DOM, which is the only version of the question a reader asks.
 */
async function picker(page, extras) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'cbb-picker-'));
  for (const name of [page, 'ncaambb_app.js', 'ncaambb_loader.js']
    .concat(extras || [])) {
    const from = path.join(UI, name);
    if (fs.existsSync(from)) fs.copyFileSync(from, path.join(root, name));
  }
  const file = path.join(root, page);
  const dom = new JSDOM(fs.readFileSync(file, 'utf8'), {
    url: 'file://' + file,
    runScripts: 'dangerously',
    resources: 'usable',
    pretendToBeVisual: true,
    beforeParse(window) {
      // No data is staged on purpose: the picker is drawn before the manifest
      // is fetched, so a page with nothing behind it still has to answer this
      // question honestly.
      window.fetch = () => Promise.resolve({ ok: false, json: () =>
        Promise.reject(new Error('no manifest')) });
    }
  });
  await settle(dom, () => dom.window.document
    .querySelectorAll('#leagues button, #league-selector a').length > 0);
  return dom;
}

const PICKER_PAGES = ['basketball.html', 'nfl.html'];

// Where each league's export lives, so this reads the files rather than the
// flag that is supposed to describe them. Football IS the site: its data is
// spread across ui/data and ui/dashboard_data.json, so it is not checked here.
const EXPORTS = { ncaambb: 'ui/data/ncaambb/manifest.json',
                  nfl: 'ui/data/nfl/manifest.json' };

for (const page of PICKER_PAGES) {
  test(`${page}'s picker offers no league without an export`, async () => {
    const app = require('../ui/ncaambb_app.js');
    const dom = await picker(page);
    const document = dom.window.document;
    const buttons = [...document.querySelectorAll('#leagues button')];
    assert.ok(buttons.length > 0, 'the picker drew something');
    const offered = buttons.map(button => button.dataset.league);
    for (const slug of offered) {
      // The page you are on is always offered, launched or not, so the
      // control has something pressed.
      if (slug === app.BASKETBALL && page === 'basketball.html') continue;
      assert.ok(app.isLaunched(slug),
        `${page} offers ${slug}, which has no published export`);
      if (EXPORTS[slug]) {
        assert.ok(fs.existsSync(path.join(__dirname, '..', EXPORTS[slug])),
          `${page} offers ${slug}, whose export is not on disk`);
      }
    }
    // Basketball is launched, so every picker must now carry it -- the
    // inverse of what this block asserted while it was staged off. Checked
    // positively rather than dropped: a league that quietly disappears from
    // one page's picker is as wrong as one that appears too early.
    assert.ok(offered.includes(app.BASKETBALL),
      `${page} offers basketball now that its export is published`);
    dom.window.close();
  });
}

test('no league is flagged launched without a published export', () => {
    // The flag and the files have to agree in the direction that is a defect:
    // a league flagged launched with nothing published offers readers an
    // empty tab, which is the whole reason this gate exists.
    //
    // The REVERSE is deliberately not asserted, and this used to assert it.
    // An export on disk with the flag still false is a finished league that
    // has not been announced yet -- a state this release is actually in --
    // and cbb-refresh.yml can commit one to main on its own schedule. A test
    // that then failed would turn that into a red test job, and a red test
    // job means the deploy never runs and FOOTBALL stops publishing. This
    // repository has been bitten by a test reading generated data five times
    // now, and the plan's own rule is that a basketball problem must never
    // stop football. Flipping the flag is a person's job, not a tripwire's.
    const app = require('../ui/ncaambb_app.js');
    const published = slug => fs.existsSync(
      path.join(UI, 'data', slug === app.BASKETBALL ? 'ncaambb' : slug,
        'manifest.json'));
    for (const entry of app.LEAGUES) {
      if (entry.slug === app.FOOTBALL) continue;  // football IS the site
      if (!entry.launched) continue;
      assert.ok(published(entry.slug),
        `${entry.slug} is flagged launched with no published export`);
    }
    // Football and the NFL are launched, and nothing here may quietly unship
    // them to make the rule above easier to satisfy.
    assert.equal(app.isLaunched('cfb'), true);
    assert.equal(app.isLaunched('nfl'), true);
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

/* ---- the Teams tab: a directory, and a club's own page ---------------- */

function clubLink(dom, label) {
  return [...dom.window.document.querySelectorAll('#team-body button.link')]
    .find(node => node.textContent === label);
}

test('the Teams tab lists every Division I club by conference', async () => {
  const dom = await open(undefined, undefined, '#league=ncaambb&section=teams');
  const doc = dom.window.document;
  await settle(dom, () => doc.querySelector('#team-body .directory'));
  const blocks = [...doc.querySelectorAll('#team-body .directory')];
  assert.deepEqual(blocks.map(node => node.querySelector('h3').textContent),
    ['ACC', 'Big 12', 'Big Ten']);
  assert.deepEqual(blocks.map(node => node.querySelector('li button.link')
    .textContent), ['Duke', 'BYU', 'Michigan']);
  // The rating beside a club is the board's, rounded for display.
  assert.equal(blocks[0].querySelector('.rating').textContent, '2211');
  dom.window.close();
});

test('a club in the directory opens its page, and says so in the URL',
  async () => {
    const dom = await open(undefined, undefined,
      '#league=ncaambb&section=teams');
    const doc = dom.window.document;
    await settle(dom, () => doc.querySelector('#team-body .directory'));
    clubLink(dom, 'Michigan').click();
    await settle(dom, () => doc.querySelector('#team-body .clubhead'));
    // The short id, not the whole key: an address is for a reader.
    assert.equal(dom.window.location.hash,
      '#league=ncaambb&section=teams&team=1');
    const head = doc.querySelector('#team-body .clubhead');
    assert.match(head.textContent, /Michigan/);
    assert.match(head.textContent, /Big Ten/);
    assert.match(head.textContent, /2274 · 30-4/);
    dom.window.close();
  });

test('a link to a club opens its page cold', async () => {
  const dom = await open(undefined, undefined,
    '#league=ncaambb&section=teams&team=2');
  const doc = dom.window.document;
  await settle(dom, () => doc.querySelector('#team-body .clubhead'));
  assert.match(doc.querySelector('#team-body .clubhead').textContent, /Duke/);
  dom.window.close();
});

test('a club page lists its season, with a neutral game called neither',
  async () => {
    const slate = [
      { id: 'g-home', at: DAY + 'T19:00:00Z', status: 'final',
        home: 'ncaambb:cbbd:team:1', away: 'ncaambb:cbbd:team:2',
        home_points: 70, away_points: 60 },
      { id: 'g-away', at: daysFromToday(1) + 'T19:00:00Z', status: 'final',
        home: 'ncaambb:cbbd:team:3', away: 'ncaambb:cbbd:team:1',
        home_points: 80, away_points: 71 },
      { id: 'g-neutral', at: daysFromToday(2) + 'T19:00:00Z', status: 'final',
        neutral: 1, home: 'ncaambb:cbbd:team:1', away: 'ncaambb:cbbd:team:3',
        home_points: 66, away_points: 64 },
      { id: 'g-later', at: daysFromToday(3) + 'T19:00:00Z',
        status: 'scheduled', home: 'ncaambb:cbbd:team:1',
        away: 'ncaambb:cbbd:team:2', home_points: null, away_points: null }
    ];
    const dom = await open(undefined, slate,
      '#league=ncaambb&section=teams&team=1');
    const doc = dom.window.document;
    await settle(dom, () => doc.querySelector('#team-body tbody tr'));
    const table = [...doc.querySelectorAll('#team-body tbody tr')]
      .map(tr => [...tr.children].map(td => td.textContent));
    assert.deepEqual(table.map(row => row[1]),
      ['vs Duke', '@ BYU', 'N BYU', 'vs Duke']);
    assert.deepEqual(table.map(row => row[2]),
      ['W 70\u201360', 'L 71\u201380', 'W 66\u201364', 'Scheduled']);
    dom.window.close();
  });

test('an opponent on a club page opens that club', async () => {
  const dom = await open(undefined, undefined,
    '#league=ncaambb&section=teams&team=1');
  const doc = dom.window.document;
  await settle(dom, () => doc.querySelector('#team-body tbody tr'));
  clubLink(dom, 'Duke').click();
  await settle(dom, () => /Duke/.test(
    doc.querySelector('#team-body .clubhead').textContent));
  assert.equal(dom.window.location.hash,
    '#league=ncaambb&section=teams&team=2');
  dom.window.close();
});

test('leaving the Teams tab takes the club out of the URL', async () => {
  const dom = await open(undefined, undefined,
    '#league=ncaambb&section=teams&team=1');
  const doc = dom.window.document;
  await settle(dom, () => doc.querySelector('#team-body .clubhead'));
  [...doc.querySelectorAll('#tabs button')]
    .find(node => node.textContent === 'Standings').click();
  await settle(dom, () => !doc.getElementById('standings').hidden);
  assert.equal(dom.window.location.hash,
    '#league=ncaambb&section=standings');
  dom.window.close();
});

test('a club id this season does not hold says so, with the way out',
  async () => {
    const dom = await open(undefined, undefined,
      '#league=ncaambb&section=teams&team=9999');
    const doc = dom.window.document;
    await settle(dom, () => doc.querySelector('#team-body .empty'));
    assert.match(doc.querySelector('#team-body .empty').textContent,
      /has that id/);
    assert.ok(clubLink(dom, '\u2190 All teams'),
      'a club that is not there left the reader with no way back');
    dom.window.close();
  });

test('a level score is reported without a verdict', async () => {
  // Basketball plays overtime until somebody wins, so a game the feed
  // reports as level is bad data. Calling it a loss would be the page
  // inventing a result; the score is shown and the verdict withheld.
  const slate = [
    { id: 'g-level', at: DAY + 'T19:00:00Z', status: 'final',
      home: 'ncaambb:cbbd:team:1', away: 'ncaambb:cbbd:team:2',
      home_points: 70, away_points: 70 }
  ];
  const dom = await open(undefined, slate,
    '#league=ncaambb&section=teams&team=1');
  const doc = dom.window.document;
  await settle(dom, () => doc.querySelector('#team-body tbody tr'));
  const cells = [...doc.querySelectorAll('#team-body tbody tr td')]
    .map(td => td.textContent);
  assert.equal(cells[2], '70\u201370');
  dom.window.close();
});

test('a club\u2019s season is ordered by date, not by whether a tip-off is set',
  async () => {
    // Codex, PR #153: this comparator was copied from the scoreboard, which
    // orders ONE day and rightly puts an undecided tip-off at the end of
    // it. Over a season that put a November game still to be scheduled
    // after a December game that has a time.
    const slate = [
      { id: 'g-dec', at: '2025-12-01T19:00:00Z', status: 'scheduled',
        home: 'ncaambb:cbbd:team:1', away: 'ncaambb:cbbd:team:2',
        home_points: null, away_points: null },
      { id: 'g-nov-tbd', at: '2025-11-01', time_tbd: 1, status: 'scheduled',
        home: 'ncaambb:cbbd:team:1', away: 'ncaambb:cbbd:team:3',
        home_points: null, away_points: null },
      // The ids sort the OTHER way from the tip-offs on purpose: with the
      // early game named 'a' the id fallback produces the right order by
      // itself, and the test passes with the tip-off comparison deleted.
      { id: 'g-nov-a', at: '2025-11-01T23:00:00Z', status: 'final',
        home: 'ncaambb:cbbd:team:1', away: 'ncaambb:cbbd:team:2',
        home_points: 81, away_points: 70 },
      { id: 'g-nov-z', at: '2025-11-01T17:00:00Z', status: 'final',
        home: 'ncaambb:cbbd:team:1', away: 'ncaambb:cbbd:team:3',
        home_points: 77, away_points: 60 }
    ];
    const dom = await open(undefined, slate,
      '#league=ncaambb&section=teams&team=1');
    const doc = dom.window.document;
    await settle(dom, () => doc.querySelector('#team-body tbody tr'));
    const dates = [...doc.querySelectorAll('#team-body tbody tr')]
      .map(tr => tr.children[0].textContent);
    assert.deepEqual(dates,
      ['2025-11-01', '2025-11-01', '2025-11-01', '2025-12-01']);
    // Within the one day, the known tip-offs come in order and the
    // undecided one is last -- which is the rule the scoreboard has.
    const against = [...doc.querySelectorAll('#team-body tbody tr')]
      .map(tr => tr.children[1].textContent);
    assert.deepEqual(against, ['vs BYU', 'vs Duke', 'vs BYU', 'vs Duke']);
    dom.window.close();
  });

/* ---- the header search box finds clubs ------------------------------- */

function type(dom, value) {
  const box = dom.window.document.getElementById('search');
  box.value = value;
  box.dispatchEvent(new dom.window.Event('input', { bubbles: true }));
}

test('typing in the header box finds clubs and opens the Teams tab',
  async () => {
    // The box took typing and did nothing at all before this: worse than no
    // box, because a reader cannot tell a control that found nothing from
    // one that was never wired.
    const dom = await open();
    const doc = dom.window.document;
    type(dom, 'duke');
    await settle(dom, () => doc.querySelector('#team-body .directory'));
    assert.equal(doc.getElementById('teams').hidden, false,
      'typing did not take the reader to the Teams tab');
    assert.equal(dom.window.location.hash, '#league=ncaambb&section=teams');
    const names = [...doc.querySelectorAll('#team-body li button.link')]
      .map(node => node.textContent);
    assert.deepEqual(names, ['Duke']);
    assert.match(doc.querySelector('#team-body .empty').textContent,
      /1 club matches/);
    dom.window.close();
  });

test('a search that matches nothing says so rather than showing everything',
  async () => {
    const dom = await open();
    const doc = dom.window.document;
    type(dom, 'notaschool');
    await settle(dom, () => /No club matches/.test(
      doc.getElementById('team-body').textContent));
    assert.equal(doc.querySelectorAll('#team-body li').length, 0,
      'a search with no match fell back to the whole directory');
    dom.window.close();
  });

test('clearing the box brings the whole directory back', async () => {
  const dom = await open();
  const doc = dom.window.document;
  type(dom, 'duke');
  await settle(dom, () => doc.querySelector('#team-body li'));
  assert.equal(doc.querySelectorAll('#team-body li').length, 1);
  type(dom, '');
  await settle(dom, () => doc.querySelectorAll('#team-body li').length === 3);
  assert.deepEqual([...doc.querySelectorAll('#team-body .directory h3')]
    .map(node => node.textContent), ['ACC', 'Big 12', 'Big Ten']);
  dom.window.close();
});

test('a club opened from a search offers the way back to the results',
  async () => {
    const dom = await open();
    const doc = dom.window.document;
    type(dom, 'duke');
    await settle(dom, () => doc.querySelector('#team-body li'));
    clubLink(dom, 'Duke').click();
    await settle(dom, () => doc.querySelector('#team-body .clubhead'));
    const back = clubLink(dom, '\u2190 Search results');
    assert.ok(back, 'the club page sent the reader back to the whole '
      + 'directory they never asked for');
    back.click();
    await settle(dom, () => doc.querySelector('#team-body li'));
    assert.equal(doc.querySelectorAll('#team-body li').length, 1);
    dom.window.close();
  });

test('typing while a club is open replaces it with the results', async () => {
  const dom = await open(undefined, undefined,
    '#league=ncaambb&section=teams&team=1');
  const doc = dom.window.document;
  await settle(dom, () => doc.querySelector('#team-body .clubhead'));
  type(dom, 'duke');
  await settle(dom, () => doc.querySelector('#team-body li'));
  assert.equal(doc.querySelectorAll('#team-body .clubhead').length, 0,
    'the club stayed drawn over its own search results');
  assert.equal(dom.window.location.hash, '#league=ncaambb&section=teams');
  dom.window.close();
});
