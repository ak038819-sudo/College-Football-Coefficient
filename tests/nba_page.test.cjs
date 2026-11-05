// The NBA page, driven as a page rather than as a module.
//
// A green build is not a rendered page: this repository has learned that
// several times. So these load ui/nba.html in jsdom with its real scripts, a
// real manifest and real payload files on disk, and then read the DOM. The
// refusal path is driven too, because an export that must not be shown has
// to produce no board at all rather than a board with a warning over it.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { JSDOM } = require('jsdom');

const UI = path.join(__dirname, '..', 'ui');
const loader = require(path.join(UI, 'nba_loader.js'));
const app = require(path.join(UI, 'nba_app.js'));

const NYK = 'nba:espn:team:18';
const SAS = 'nba:espn:team:24';
const OKC = 'nba:espn:team:25';

function names() {
  return {
    [NYK]: { abbr: 'NY', name: 'New York Knicks',
             conference: 'Eastern Conference' },
    [SAS]: { abbr: 'SA', name: 'San Antonio Spurs',
             conference: 'Western Conference' },
    [OKC]: { abbr: 'OKC', name: 'Oklahoma City Thunder',
             conference: 'Western Conference' }
  };
}

function teams() {
  const index = {};
  Object.keys(names()).forEach((key) => {
    index[key] = Object.assign({ id: Number(key.split(':').pop()) },
      names()[key]);
  });
  return index;
}

function games() {
  return [
    // A played game. Its LOCAL date is the 16th and it tips at 01:30Z on the
    // 17th, which is the trap this page exists to get right.
    { id: 'g1', date: '2025-12-16', at: '2025-12-17T01:30:00Z',
      type: 'cup_final', neutral: 1, home: NYK, away: SAS,
      home_points: 124, away_points: 113, rateable: 1,
      home_elo: 1633, away_elo: 1545.4, home_wp: 0.6235,
      notes: 'NBA Cup Championship', venue: 'T-Mobile Arena',
      city: 'Las Vegas' },
    { id: 'g2', date: '2025-12-18', at: '2025-12-19T00:00:00Z',
      type: 'regular', home: SAS, away: OKC, home_points: 101,
      away_points: 99, rateable: 1, home_elo: 1550, away_elo: 1600,
      home_wp: 0.44 },
    // Not played. The source writes 0-0 and the loader has already turned
    // that into nothing; the page must not put a number back.
    { id: 'g3', date: '2025-12-20', at: '2025-12-21T01:00:00Z',
      type: 'regular', home: OKC, away: NYK, home_points: null,
      away_points: null, rateable: 0, home_elo: 1600, away_elo: 1640,
      home_wp: 0.47, wp_estimated: 1 },
    // Not played and no tip-off time, which 44% of a new schedule has.
    { id: 'g4', date: '2025-12-22', time_tbd: 1, type: 'playin',
      home: NYK, away: OKC, home_points: null, away_points: null,
      rateable: 0 }
  ];
}

function board() {
  return [
    { team: NYK, abbr: 'NY', name: 'New York Knicks', rank: 1,
      conference: 'Eastern Conference', conference_from: 'season',
      rating: 1640.4, wins: 1, losses: 0, rated_games: 1 },
    { team: SAS, abbr: 'SA', name: 'San Antonio Spurs', rank: 2,
      conference: 'Western Conference', conference_from: 'season',
      rating: 1550.2, wins: 1, losses: 1, rated_games: 2 },
    { team: OKC, abbr: 'OKC', name: 'Oklahoma City Thunder', rank: 3,
      conference: 'Western Conference', conference_from: 'season',
      rating: 1500.9, wins: 0, losses: 1, rated_games: 1 }
  ];
}

function history() {
  return {
    conferences: [{
      season: 2026, season_label: '2025-26',
      conferences: [
        { conference: 'Eastern Conference', teams: 1, mean: 1640.4,
          median: 1640.4, best: NYK, best_rating: 1640.4 },
        { conference: 'Western Conference', teams: 2, mean: 1525.6,
          median: 1525.6, best: SAS, best_rating: 1550.2 }
      ]
    }],
    champions: { 2026: { champion: NYK, runner_up: SAS } }
  };
}

function manifest(overrides) {
  return Object.assign({
    schema_version: 1,
    sport: 'nba',
    sport_label: 'NBA',
    generated_at: new Date().toISOString().replace(/\.\d+Z$/, 'Z'),
    latest_season: 2026,
    model: {
      name: 'nba_elo_v1',
      config: { k: 20, home_advantage: 80, scale: 400 },
      config_sha256: 'a'.repeat(64),
      fitted_on: [2024, 2025],
      held_out: [2026],
      holdout_brier: 0.21809,
      holdout_games: 6605,
      built_at: '2026-11-04T00:00:00+00:00'
    },
    coverage: [{
      season: 2026, season_label: '2025-26', teams: 30, rated_teams: 30,
      games: 1320, finals: 1320, rateable: 1320, complete: true,
      played_teams: 30
    }],
    seasons: {},
    files: {}
  }, overrides || {});
}

function stamp(text) {
  return require('node:crypto').createHash('sha256')
    .update(text).digest('hex').slice(0, 10);
}

function site(overrides, schedule, standings, past) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'nba-page-'));
  const data = path.join(root, 'data', 'nba', 'seasons');
  fs.mkdirSync(data, { recursive: true });
  const payloadFor = (season, rows, slate) =>
    'window.NBA_SEASONS=(window.NBA_SEASONS||{});'
    + 'window.NBA_SEASONS[' + season + ']='
    + JSON.stringify({ season: season, season_label: app.seasonLabel(season),
      board: rows, games: slate, names: names() }) + ';\n';
  const seasonText = payloadFor(2026, standings || board(),
    schedule || games());
  const teamsText = 'window.NBA_TEAMS=' + JSON.stringify(teams()) + ';\n';
  const historyText = 'window.NBA_HISTORY=' + JSON.stringify(history()) + ';\n';
  fs.writeFileSync(path.join(data, '2026.js'), seasonText);
  fs.writeFileSync(path.join(root, 'data', 'nba', 'teams.js'), teamsText);
  fs.writeFileSync(path.join(root, 'data', 'nba', 'history.js'), historyText);
  const payload = manifest(overrides);
  payload.seasons = { 2026: 'seasons/2026.js?v=' + stamp(seasonText) };
  // A second season, so the picker has something to pick between and a deep
  // link naming a season can be told from the page's default.
  if (past) {
    const pastText = payloadFor(2025, standings || board(),
      schedule || games());
    fs.writeFileSync(path.join(data, '2025.js'), pastText);
    payload.seasons[2025] = 'seasons/2025.js?v=' + stamp(pastText);
    payload.coverage = payload.coverage.concat([Object.assign(
      {}, payload.coverage[0], { season: 2025, season_label: '2024-25' })]);
  }
  payload.files = { teams: 'teams.js?v=' + stamp(teamsText),
    history: 'history.js?v=' + stamp(historyText) };
  fs.writeFileSync(path.join(root, 'data', 'nba', 'manifest.json'),
    JSON.stringify(payload));
  for (const name of ['nba.html', 'nba_loader.js', 'nba_app.js',
    'ncaambb_app.js']) {
    fs.copyFileSync(path.join(UI, name), path.join(root, name));
  }
  return { root, manifest: payload };
}

// The budget is 5 SECONDS because the number is not a claim about speed, it
// is only how long to wait before saying "never". `node --test` runs every
// file in parallel, so a jsdom page loading its scripts from disk competes
// with several other files doing the same.
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

async function open(overrides, schedule, standings, hash, past) {
  const built = site(overrides, schedule, standings, past);
  const file = path.join(built.root, 'nba.html');
  const dom = new JSDOM(fs.readFileSync(file, 'utf8'), {
    // The hash is part of the URL the reader arrived with, so it is set here
    // rather than assigned afterwards: a tab the page opens on and a tab it
    // is moved to later are different claims, and this one is the link.
    url: 'file://' + file + (hash || ''),
    runScripts: 'dangerously',
    resources: 'usable',
    pretendToBeVisual: true,
    beforeParse(window) {
      // jsdom serves this page from file://, an opaque origin, so
      // window.localStorage is undefined -- the very case the page guards
      // against. Tests of what following a club DOES need a real store.
      const cells = new Map();
      Object.defineProperty(window, 'localStorage', {
        configurable: true,
        value: {
          getItem: key => (cells.has(key) ? cells.get(key) : null),
          setItem: (key, value) => { cells.set(key, String(value)); },
          removeItem: key => { cells.delete(key); },
          clear: () => cells.clear()
        }
      });
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
  // Rendered means the panel THIS route asks for has content in it. A page
  // opened on the Elo board draws rows and no cards at all, so waiting for a
  // card there would call a correct page "never rendered".
  await settle(dom, () => dom.window.document.querySelector(
    '#cards .card, #refusal-list li, #board tr, #groups tr,'
    + ' #teams-body *, #track-record *'));
  dom.store = built;
  return dom;
}

function click(dom, selector) {
  const node = dom.window.document.querySelector(selector);
  assert.ok(node, 'no node for ' + selector);
  node.dispatchEvent(new dom.window.Event('click', { bubbles: true }));
}

function textOf(dom, selector) {
  const node = dom.window.document.querySelector(selector);
  return node ? node.textContent : null;
}

/* Move the slate to a day of the fixture's season.
 *
 * The page opens on TODAY clamped into the season, which for a fixture
 * dated December 2025 is its last day. That is the right behaviour and not
 * what any of these tests are about, so each one says which night it means.
 */
async function goToDay(dom, dayOfMonth) {
  const buttons = [...dom.window.document.querySelectorAll('#days button')];
  const target = buttons.find(
    node => node.textContent.trim().endsWith(' ' + dayOfMonth));
  assert.ok(target, 'no button for day ' + dayOfMonth + ' among '
    + buttons.map(node => node.textContent).join(', '));
  target.dispatchEvent(new dom.window.Event('click', { bubbles: true }));
  await settle(dom, () => dom.window.document.querySelector('#cards .card'));
}

function allText(dom, selector) {
  return [...dom.window.document.querySelectorAll(selector)]
    .map(node => node.textContent);
}

// --- the page draws at all -------------------------------------------------

test('the page opens on a slate of cards, not a refusal', async () => {
  const dom = await open();
  assert.equal(dom.window.document.getElementById('refusal').hidden, true);
  assert.ok(dom.window.document.querySelectorAll('#cards .card').length > 0);
  dom.window.close();
});

test('the four tabs are the shared ones this export can fill', async () => {
  const dom = await open();
  const drawn = [...dom.window.document.querySelectorAll('#tabs button')]
    .map(node => [node.dataset.tab, node.textContent]);
  // No People: schema 1 carries no people layer, and an empty tab is worse
  // than an absent one.
  assert.deepEqual(drawn, [['home', 'Home'], ['stats', 'Stats'],
    ['standings', 'Standings'], ['teams', 'Teams']]);
  dom.window.close();
});

test('the league picker offers four, with the NBA pressed', async () => {
  const dom = await open();
  const buttons = [
    ...dom.window.document.querySelectorAll('#leagues button')];
  assert.deepEqual(buttons.map(b => b.textContent),
    ['NCAAF', 'NCAAM', 'NFL', 'NBA']);
  assert.deepEqual(buttons.map(b => b.getAttribute('aria-label')),
    ['College Football', "Men's College Basketball",
      'National Football League', 'National Basketball Association']);
  const pressed = buttons.filter(
    b => b.getAttribute('aria-pressed') === 'true');
  assert.equal(pressed.length, 1);
  assert.equal(pressed[0].textContent, 'NBA');
  dom.window.close();
});

test('the header says what the model scored on seasons it never saw',
  async () => {
    const dom = await open();
    const meta = textOf(dom, '#meta');
    assert.match(meta, /0\.21809/);
    assert.match(meta, /6,605 games/);
    dom.window.close();
  });

// --- the day a game belongs to --------------------------------------------

test('a game tipping after midnight UTC sits on its own local date',
  async () => {
    // g1 is at 01:30Z on the 17th and its local date is the 16th. A page
    // grouping by the timestamp would file it under the 17th, which is the
    // single most common shape of NBA game.
    const dom = await open();
    const days = allText(dom, '#days button');
    assert.ok(days.length >= 4, 'a strip of days: ' + days.join(', '));
    // The strip offers the 16th at all, which it would not if the game were
    // filed under its UTC timestamp.
    assert.ok(days.some(day => day.trim().endsWith(' 16')),
      'the 16th is a night of this season: ' + days.join(', '));
    await goToDay(dom, 16);
    const cards = allText(dom, '#cards .card');
    assert.equal(cards.length, 1);
    assert.match(cards[0], /New York Knicks/);
    assert.match(cards[0], /San Antonio Spurs/);
    // And the 17th, where a UTC grouping would have put it, has nothing.
    assert.ok(!days.some(day => day.trim().endsWith(' 17')),
      'nothing is filed under the UTC date: ' + days.join(', '));
    dom.window.close();
  });

// --- the 0-0 trap, at the last place it could come back --------------------

test('an unplayed game shows no score at all, never a zero', async () => {
  const dom = await open();
  await goToDay(dom, 20);
  const points = allText(dom, '#cards .card .pts');
  assert.deepEqual(points, ['', ''],
    'both score cells are empty, not "0"');
  dom.window.close();
});

test('a game with no tip-off time says so rather than reading midnight',
  async () => {
    const dom = await open();
    await goToDay(dom, 22);
    assert.equal(textOf(dom, '#cards .card .status'), 'Time TBD');
    dom.window.close();
  });

// --- the Cup final ---------------------------------------------------------

test('the Cup final is badged on the card, with the reason', async () => {
  const dom = await open();
  await goToDay(dom, 16);
  const tag = dom.window.document.querySelector('#cards .card .tag.cup');
  assert.ok(tag, 'the Cup final carries a badge');
  assert.equal(tag.textContent, 'NBA Cup final');
  assert.match(tag.getAttribute('title'),
    /does not count it in the standings/);
  dom.window.close();
});

test('the Cup final is in a club\'s schedule and not in its record',
  async () => {
    const dom = await open(null, null, null,
      '#league=nba&section=teams&team=' + encodeURIComponent(NYK));
    await settle(dom, () => dom.window.document.querySelector(
      '#teams-body tbody tr'));
    const rows = allText(dom, '#teams-body tbody tr');
    // Three games for the Knicks: the Cup final, the unplayed one and the
    // play-in. All three are listed.
    assert.equal(rows.length, 3);
    assert.ok(rows.some(row => /NBA Cup final/.test(row)),
      'the Cup final is in the schedule');
    // And the record above says 1-0, which is the board's -- the Cup final
    // win is not in it.
    assert.match(textOf(dom, '#teams-body p.note'), /^1–0 /);
    dom.window.close();
  });

test('a schedule row that does not count is dimmed and said to be', async () => {
  const dom = await open(null, null, null,
    '#league=nba&section=teams&team=' + encodeURIComponent(NYK));
  await settle(dom, () => dom.window.document.querySelector(
    '#teams-body tbody tr'));
  const dimmed = [...dom.window.document.querySelectorAll(
    '#teams-body tbody tr.uncounted')].map(row => row.textContent);
  // The Cup final and the play-in. Neither is a regular-season game, and a
  // regular-season record is what a standings page shows.
  assert.equal(dimmed.length, 2, dimmed.join(' | '));
  assert.ok(dimmed.some(row => /NBA Cup final/.test(row)));
  assert.ok(dimmed.some(row => /Play-In/.test(row)));
  // The one that was PLAYED is the Cup final, so the note counts one.
  const played = [...dom.window.document.querySelectorAll(
    '#teams-body tbody tr.uncounted')].filter(
    row => row.children[2].textContent);
  assert.equal(played.length, 1);
  // And the page says so above the table, rather than leaving a reader to
  // work out why the W column and the record disagree.
  const notes = allText(dom, '#teams-body p.note');
  assert.ok(notes.some(note => /1 game below does not/.test(note)),
    'the note explains the gap: ' + notes.join(' | '));
  assert.ok(notes.some(note => /^0–0 counts in the standings/.test(note)),
    'and the counted record excludes the Cup final win');
  dom.window.close();
});

test('scheduleRecord counts what the league counts and nothing else', () => {
  const rows = app.teamSchedule(games(), NYK);
  assert.equal(rows.length, 3);
  // The Knicks' one win in this fixture IS the Cup final, so a record that
  // counted it would read 1-0 and the league's would read 0-0.
  assert.deepEqual(app.scheduleRecord(rows), { wins: 0, losses: 0 });
  const everything = rows.map(row => Object.assign({}, row, { counts: true }));
  assert.deepEqual(app.scheduleRecord(everything), { wins: 1, losses: 0 });
});

// --- standings -------------------------------------------------------------

test('standings group by conference and never by a division', async () => {
  const dom = await open();
  click(dom, '#tabs button[data-tab="standings"]');
  await settle(dom, () => dom.window.document.querySelector('#groups tr'));
  assert.deepEqual(allText(dom, '#groups .group h3'),
    ['Eastern Conference', 'Western Conference']);
  // The note says why, because a reader who knows the NBA expects six.
  assert.match(textOf(dom, '#standings p.note'), /no divisions/);
  dom.window.close();
});

test('standings order by record, with the rating only as the tiebreak',
  async () => {
    // San Antonio is 1-1 at 1550.2 and Oklahoma City 0-1 at 1500.9, so the
    // West reads SA then OKC under either rule. The Knicks at 1640.4 sit in
    // the East, which is the point: a rating-ordered table would mix them.
    const dom = await open();
    click(dom, '#tabs button[data-tab="standings"]');
    await settle(dom, () => dom.window.document.querySelector('#groups tr'));
    const west = [...dom.window.document.querySelectorAll(
      '#groups .group')].pop();
    const rows = [...west.querySelectorAll('tbody tr')]
      .map(row => row.querySelector('td.teamcell').textContent);
    assert.deepEqual(rows,
      ['San Antonio Spurs', 'Oklahoma City Thunder']);
    dom.window.close();
  });

test('a rating is drawn as a whole number everywhere it is drawn',
  async () => {
    const dom = await open();
    // The rail on the scoreboard.
    const rail = allText(dom, '#rail li .num');
    assert.ok(rail.length, 'the rail has rows');
    rail.forEach((value) => {
      if (/^\d+$/.test(value)) return;             // a rating
      assert.match(value, /^\d+–\d+$/, 'or a record: ' + value);
    });
    assert.ok(rail.includes('1640'), '1640.4 is drawn 1640');
    assert.ok(!rail.some(v => v.indexOf('.') >= 0),
      'no rating keeps a decimal: ' + rail.join(', '));
    dom.window.close();
  });

// --- the Elo board ---------------------------------------------------------

test('the Elo view is its own route and ranks by rating', async () => {
  const dom = await open(null, null, null,
    '#league=nba&section=standings&view=elo');
  await settle(dom, () => dom.window.document.querySelector('#board tr'));
  assert.equal(dom.window.document.getElementById('rankings').hidden, false);
  const ranks = [...dom.window.document.querySelectorAll('#board tr')]
    .map(row => row.querySelector('td.rk').textContent);
  assert.deepEqual(ranks, ['1', '2', '3']);
  dom.window.close();
});

test('the legacy section name rankings still opens the Elo board',
  async () => {
    // Every other league in this site uses `section=rankings` for the same
    // board. A link is not ours to break.
    const dom = await open(null, null, null, '#league=nba&section=rankings');
    await settle(dom, () => dom.window.document.querySelector('#board tr'));
    assert.equal(dom.window.document.getElementById('rankings').hidden,
      false);
    dom.window.close();
  });

// --- a club page -----------------------------------------------------------

test('a club page shows the stored pregame rating, not a recomputed one',
  async () => {
    const dom = await open(null, null, null,
      '#league=nba&section=teams&team=' + encodeURIComponent(SAS));
    await settle(dom, () => dom.window.document.querySelector(
      '#teams-body tbody tr'));
    const first = dom.window.document.querySelector('#teams-body tbody tr');
    const cells = [...first.querySelectorAll('td')].map(td => td.textContent);
    // The Cup final, away, lost 113-124, pregame rating 1545.4 -> 1545.
    assert.equal(cells[2], 'L');
    assert.equal(cells[3], '113–124');
    assert.equal(cells[5], '1545');
    // The model gave San Antonio 1 - 0.6235 of it.
    assert.equal(cells[4], '38%');
    dom.window.close();
  });

test('an unplayed game on a club page is marked as an estimate', async () => {
  const dom = await open(null, null, null,
    '#league=nba&section=teams&team=' + encodeURIComponent(OKC));
  await settle(dom, () => dom.window.document.querySelector(
    '#teams-body tbody tr'));
  const rows = [...dom.window.document.querySelectorAll(
    '#teams-body tbody tr')].map(row => row.textContent);
  assert.ok(rows.some(row => /est\./.test(row)),
    'the unplayed game says its number is an estimate');
  dom.window.close();
});

test('following a club is stored under the NBA\'s own key', async () => {
  const dom = await open(null, null, null,
    '#league=nba&section=teams&team=' + encodeURIComponent(NYK));
  await settle(dom, () => dom.window.document.querySelector(
    '#teams-body tbody tr'));
  const follow = [...dom.window.document.querySelectorAll(
    '#teams-body button.expand')].find(n => n.textContent === 'Follow');
  assert.ok(follow, 'a Follow control');
  follow.dispatchEvent(new dom.window.Event('click', { bubbles: true }));
  const stored = dom.window.localStorage.getItem('elocast.favorites.nba.v1');
  assert.deepEqual(JSON.parse(stored), [NYK]);
  // And NOT in football's key, which is the whole reason it is per league.
  assert.equal(dom.window.localStorage.getItem('elocast.favorites.v1'), null);
  dom.window.close();
});

// --- the season picker -----------------------------------------------------

test('a deep link naming a season opens it, and the picker agrees',
  async () => {
    // The board redrew for the linked season while the control still read
    // the latest one, which is a page telling a reader two things at once.
    const dom = await open(null, null, null,
      '#league=nba&section=standings&view=elo&season=2025', true);
    await settle(dom, () => dom.window.document.querySelector('#board tr'));
    assert.equal(dom.window.document.getElementById('season').value, '2025');
    dom.window.close();
  });

test('a hash change moves the season picker with the board', async () => {
  const dom = await open(null, null, null, null, true);
  assert.equal(dom.window.document.getElementById('season').value, '2026');
  dom.window.location.hash = '#league=nba&section=standings&view=elo'
    + '&season=2025';
  await settle(dom, () => dom.window.document.getElementById(
    'season').value === '2025');
  assert.equal(dom.window.document.getElementById('season').value, '2025');
  dom.window.close();
});

test('Back off a season link returns to the season the bare URL means',
  async () => {
    // A URL must mean one thing. Reading a missing season parameter as
    // "keep what you have" made the bare URL show 2024-25 when it was
    // reached by Back and 2025-26 when it was reloaded.
    const dom = await open(null, null, null, null, true);
    assert.equal(dom.window.document.getElementById('season').value, '2026');
    dom.window.location.hash = '#league=nba&section=standings&view=elo'
      + '&season=2025';
    await settle(dom, () => dom.window.document.getElementById(
      'season').value === '2025');
    // Back, which is the bare URL this page opened on.
    dom.window.location.hash = '';
    await settle(dom, () => dom.window.document.getElementById(
      'season').value === '2026');
    assert.equal(dom.window.document.getElementById('season').value, '2026');
  });

// --- history ---------------------------------------------------------------

test('the Stats tab draws the track record, the conferences and the titles',
  async () => {
    const dom = await open();
    click(dom, '#tabs button[data-tab="stats"]');
    await settle(dom, () => dom.window.document.querySelector(
      '#champions tbody tr'));
    assert.match(textOf(dom, '#track-record'), /0\.24694/);
    assert.match(textOf(dom, '#conference-history'), /Eastern Conference/);
    const champ = dom.window.document.querySelector('#champions tbody tr');
    assert.match(champ.textContent, /New York Knicks/);
    dom.window.close();
  });

// --- the refusal path ------------------------------------------------------

test('another league\'s manifest draws no board at all', async () => {
  const dom = await open({ sport: 'nfl' });
  assert.equal(dom.window.document.getElementById('refusal').hidden, false);
  assert.match(textOf(dom, '#refusal-list'), /not the NBA/);
  // NO BOARD. A refusal with a board under it is not a refusal.
  assert.equal(dom.window.document.querySelectorAll('#cards .card').length, 0);
  assert.equal(dom.window.document.getElementById('scoreboard').hidden, true);
  dom.window.close();
});

test('a model no better than picking the home team is refused', async () => {
  const bad = manifest();
  bad.model.holdout_brier = 0.2461;
  const dom = await open(bad);
  assert.equal(dom.window.document.getElementById('refusal').hidden, false);
  assert.match(textOf(dom, '#refusal-list'), /picking the home team/);
  assert.equal(dom.window.document.querySelectorAll('#cards .card').length, 0);
  dom.window.close();
});

test('a season missing a franchise is refused', async () => {
  const bad = manifest();
  bad.coverage[0].teams = 29;
  const dom = await open(bad);
  assert.equal(dom.window.document.getElementById('refusal').hidden, false);
  assert.match(textOf(dom, '#refusal-list'), /29 franchises/);
  dom.window.close();
});

test('an export hours old is drawn, and said to be stale', async () => {
  const old = manifest();
  old.generated_at = new Date(Date.now() - 1000 * 60 * 60 * 6)
    .toISOString().replace(/\.\d+Z$/, 'Z');
  const dom = await open(old);
  // STALENESS IS NOT A REFUSAL. The last good snapshot is still the thing a
  // reader came for.
  assert.equal(dom.window.document.getElementById('refusal').hidden, true);
  assert.ok(dom.window.document.querySelectorAll('#cards .card').length > 0);
  assert.ok(dom.window.document.querySelector('#meta .stale'),
    'but it is marked');
  assert.match(textOf(dom, '#meta'), /6h ago/);
  dom.window.close();
});

// --- the loader, as a module ----------------------------------------------

test('the loader and the build gate agree on the league\'s size', () => {
  assert.equal(loader.expectedTeams(2004), 29);
  assert.equal(loader.expectedTeams(2005), 30);
  assert.equal(loader.EXPANSION_SEASON, 2005);
});

test('the manifest\'s season keys are sorted numerically, newest first',
  () => {
    // Object.keys on a JSON object is insertion order, which is no order at
    // all: a page trusting it opens on whichever season was written last.
    assert.deepEqual(
      loader.seasons({ seasons: { 2002: 'a', 2027: 'b', 2010: 'c' } }),
      [2027, 2010, 2002]);
  });

test('a season is labelled for the two years it spans', () => {
  assert.equal(loader.seasonLabel(2027), '2026-27');
  assert.equal(loader.seasonLabel(2010), '2009-10');
  assert.equal(app.seasonLabel(2027), loader.seasonLabel(2027));
});
