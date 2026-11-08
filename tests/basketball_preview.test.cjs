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
// A tip-off time is rendered in the TIMEZONE the browser is set to, so an
// assertion about one has to fix it. Pinned before the first Date is
// formatted, or these expectations change with the machine running them.
process.env.TZ = 'America/New_York';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { JSDOM } = require('jsdom');
const { copyAssets } = require('./page_assets.cjs');

const UI = path.join(__dirname, '..', 'ui');
// The fixture's games are TODAY's, because the page opens on today and the
// strip labels only the real today. A fixed past date would make every
// scoreboard assertion here test the out-of-season fallback instead of the
// case a reader meets. Out of season is tested on its own, below.
// In EASTERN time, which is the calendar the export states a game day in and
// therefore the one the page calls Today. Taking the UTC date here instead
// made this whole file's scoreboard assertions depend on the hour it ran: for
// the five hours after midnight UTC the fixture's games are dated tomorrow,
// and every "the strip opens on today" assertion would be checking the wrong
// day. See NCAAMBB_DAY_ZONE in ui/ncaambb_app.js.
const ZONE = 'America/New_York';

function easternDay(moment) {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: ZONE, year: 'numeric', month: '2-digit', day: '2-digit'
  }).format(moment);
}

const DAY = easternDay(new Date());

function daysFromToday(count) {
  return easternDay(new Date(Date.now() + count * 86400000));
}

const TEAM_KEYS = ['ncaambb:cbbd:team:1', 'ncaambb:cbbd:team:2',
                   'ncaambb:cbbd:team:3'];

// `logo` is the id the school's mark is filed under, as src/ncaambb/export.py
// writes it -- never a path. Michigan's two files are written to the fixture
// below and Duke's are NOT, which is how the fallback for a mark that has not
// been banked yet gets driven. BYU carries no id at all, standing in for a
// non-Division I opponent, which the provider sends no logo for.
function teams() {
  return {
    'ncaambb:cbbd:team:1': { name: 'Michigan', short: 'Michigan', logo: 130 },
    'ncaambb:cbbd:team:2': { name: 'Duke', short: 'Duke', logo: 150 },
    'ncaambb:cbbd:team:3': { name: 'BYU', short: 'BYU' }
  };
}

// The smallest real PNG, so an <img> in jsdom loads rather than erroring.
const PIXEL = Buffer.from(
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKm'
  + 'MIQAAAABJRU5ErkJggg==', 'base64');

// As the export writes a game: `at` is the day it is played on and `tip` the
// instant it starts, present only when the provider has decided one.
function games() {
  return [
    { id: 'ncaambb:cbbd:game:700', at: DAY, tip: DAY + 'T17:00:00Z',
      status: 'final', home: TEAM_KEYS[0], away: TEAM_KEYS[1],
      home_points: 78, away_points: 61, venue: 'Crisler Center',
      city: 'Ann Arbor', state: 'MI' },
    { id: 'ncaambb:cbbd:game:701', at: DAY, status: 'scheduled',
      time_tbd: 1, home: TEAM_KEYS[0], away: TEAM_KEYS[2],
      home_points: null, away_points: null },
    { id: 'ncaambb:cbbd:game:702', at: DAY, tip: DAY + 'T19:00:00Z',
      status: 'scheduled', home: TEAM_KEYS[1], away: TEAM_KEYS[2],
      home_points: null, away_points: null }
  ];
}

function board() {
  return [
    { team: 'ncaambb:cbbd:team:1', conference: 'Big Ten', rating: 2273.7,
      rank: 1, wins: 30, losses: 4, d1_wins: 29, d1_losses: 4,
      rated_games: 34 },
    { team: 'ncaambb:cbbd:team:2', conference: 'ACC', rating: 2211.3,
      rank: 2, wins: 29, losses: 5, d1_wins: 29, d1_losses: 5,
      rated_games: 34 },
    { team: 'ncaambb:cbbd:team:3', conference: 'Big 12', rating: 1788.4,
      rank: 3, wins: 18, losses: 14, d1_wins: 17, d1_losses: 14,
      rated_games: 32 }
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

// The people layer as the export writes it: an identity index, one roster per
// club, and the player-game lines split sixteen ways on the player's own id.
// Written with the same shapes src/ncaambb/export_people.py produces, and
// tests/test_ncaambb_export_people.py pins those on the Python side.
const SHARDS = 16;

function peopleShard(id) {
  return ((Number(id) % SHARDS) + SHARDS) % SHARDS;
}

const PLAYERS = {
  101: { n: 'Hal Guard', h: 'Provo', s: 'UT' },
  102: { n: 'Ivan Bench', h: 'Orem', s: 'UT' },
  // Deliberately in another shard from 101, so a player page that loaded the
  // wrong shard would find nothing.
  205: { n: 'Otto Center', h: 'Durham', s: 'NC' }
};

// [team index, jersey, position, height, weight], per player.
const MEMBERS = {
  101: [[0, '3', 'G', 75, 185]],
  102: [[0, '12', 'F', 79, 220]],
  205: [[1, '21', 'C', 83, 250]]
};

// [game id, team index, minutes, points, starter, day, opponent index, site],
// per player. The day and the opponent are IN the line because a career page
// spans seasons and loads one schedule; see LINE_FIELDS in
// src/ncaambb/export_people.py.
const LINES = {
  101: [[700, 0, 30, 22, 1, DAY, 1, 'H'],
        [701, 0, 28, 14, 1, DAY, 2, 'H']],
  // A line with no minutes recorded at all, which is not zero minutes.
  102: [[700, 0, null, 0, 0, DAY, 1, 'H']],
  205: [[700, 1, 33, 19, 1, DAY, 0, 'A']]
};

function writePeople(root, payload, overrides) {
  const lines = (overrides && overrides.lines) || LINES;
  const base = path.join(root, 'data', 'ncaambb', 'people');
  fs.mkdirSync(path.join(base, '2026'), { recursive: true });
  const indexText = 'window.NCAAMBB_PEOPLE=' + JSON.stringify({
    v: 1, seasons: [2026], shards: SHARDS, players: PLAYERS }) + ';\n';
  fs.writeFileSync(path.join(base, 'index.js'), indexText);

  // All three, because a line names its OPPONENT by index into this list
  // as well as its own club.
  const teams = [TEAM_KEYS[0], TEAM_KEYS[1], TEAM_KEYS[2]];
  const roster = {};
  Object.keys(MEMBERS).forEach(id => {
    MEMBERS[id].forEach(row => {
      const own = lines[id] || [];
      const minutes = own.reduce((total, line) => (
        line[2] === null ? total : total + line[2]), null);
      const points = own.reduce((total, line) => (
        line[3] === null ? total : (total === null ? 0 : total) + line[3]), null);
      roster[teams[row[0]]] = roster[teams[row[0]]] || [];
      roster[teams[row[0]]].push([Number(id), row[1], row[2], own.length,
        minutes, points]);
    });
  });
  Object.values(roster).forEach(rows => rows.sort(
    (a, b) => (b[5] || 0) - (a[5] || 0) || a[0] - b[0]));
  const rosterText = 'window.NCAAMBB_ROSTERS=(window.NCAAMBB_ROSTERS||{});'
    + 'window.NCAAMBB_ROSTERS[2026]=' + JSON.stringify({
      v: 1, season: 2026,
      fields: ['player', 'jersey', 'position', 'games', 'minutes', 'points'],
      teams: roster }) + ';\n';
  fs.writeFileSync(path.join(base, '2026', 'roster.js'), rosterText);

  const shards = [];
  for (let shard = 0; shard < SHARDS; shard += 1) {
    const m = {};
    const pl = {};
    Object.keys(MEMBERS).forEach(id => {
      if (peopleShard(id) === shard) m[id] = MEMBERS[id];
    });
    Object.keys(lines).forEach(id => {
      if (peopleShard(id) === shard) pl[id] = lines[id];
    });
    const text = '(window.NCAAMBB_PLAYER_GAMES='
      + 'window.NCAAMBB_PLAYER_GAMES||[]).push(' + JSON.stringify({
        v: 1, season: 2026, shard, t: teams,
        fields: ['game', 'team', 'minutes', 'points', 'starter'],
        member_fields: ['team', 'jersey', 'position', 'height', 'weight'],
        m, p: pl }) + ');\n';
    const file = path.join(base, '2026', shard + '.js');
    fs.writeFileSync(file, text);
    shards.push(`people/2026/${shard}.js?v=` + stamp(text));
  }
  // Listed newest-numbered first, deliberately. The order of this list means
  // nothing -- a reader looks up the shard it wants by NAME -- and writing it
  // in shard order would let a page that matched by array position pass.
  shards.reverse();
  const seasons = { 2026: {
    roster: 'people/2026/roster.js?v=' + stamp(rosterText), shards } };
  // Extra season KEYS, pointing at the same files. The page takes the list of
  // seasons it has people for from these keys, so this is how a test gets a
  // multi-season export without writing six seasons of fixtures.
  ((overrides && overrides.extraSeasons) || []).forEach(year => {
    seasons[year] = seasons[2026];
  });
  // Which seasons the manifest CLAIMS people for, when that has to differ
  // from the one whose files are on disk. The only way to describe a rated
  // season with no player files, which is the ordinary state of the season
  // being played: it is rated from its first game and its rosters are
  // fetched last.
  const claimed = overrides && overrides.peopleSeasonKeys;
  if (claimed) {
    const only = {};
    claimed.forEach(year => { only[year] = seasons[2026]; });
    Object.keys(seasons).forEach(key => { delete seasons[key]; });
    Object.assign(seasons, only);
  }
  payload.files.people = {
    version: (overrides && overrides.peopleVersion) || 2,
    shards: SHARDS,
    index: 'people/index.js?v=' + stamp(indexText),
    seasons
  };
}

function stamp(text) {
  return require('node:crypto').createHash('sha256')
    .update(text).digest('hex').slice(0, 10);
}

/* A whole export on disk, next to a copy of the page and its scripts. */
// `overrides` goes into the manifest, EXCEPT `board`, which is the season
// payload's rating board. It is pulled out here so a test can describe a
// season in progress without a fourth positional argument.
function site(overrides, schedule) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'cbb-preview-'));
  const data = path.join(root, 'data', 'ncaambb', 'seasons');
  fs.mkdirSync(data, { recursive: true });
  const rules = Object.assign({}, overrides || {});
  const boardRows = rules.board || board();
  // `board`, `lines`, `people` and `at` describe the EXPORT and the page's
  // clock, not the manifest, so they are taken out before the rest is merged
  // into it.
  const people = { lines: rules.lines, people: rules.people,
                   extraSeasons: rules.extraSeasons,
                   peopleSeasonKeys: rules.peopleSeasonKeys,
                   peopleVersion: rules.peopleVersion };
  // `at` freezes the PAGE's clock, so a test can put it at a named moment.
  // The export's age is measured against that same instant, or a page opened
  // at a fixed moment would badge a fresh export as months stale and draw a
  // refusal instead of a scoreboard.
  const at = rules.at ? Date.parse(rules.at) : null;
  delete rules.board;
  delete rules.lines;
  delete rules.extraSeasons;
  delete rules.peopleSeasonKeys;
  delete rules.peopleVersion;
  delete rules.at;
  overrides = rules;
  const seasonText = 'window.NCAAMBB_SEASONS=(window.NCAAMBB_SEASONS||{});'
    + 'window.NCAAMBB_SEASONS[2026]='
    + JSON.stringify({ season: 2026, board: boardRows,
      games: schedule || games() }) + ';\n';
  const teamsText = 'window.NCAAMBB_TEAMS=' + JSON.stringify(teams()) + ';\n';
  fs.writeFileSync(path.join(data, '2026.js'), seasonText);
  fs.writeFileSync(path.join(root, 'data', 'ncaambb', 'teams.js'), teamsText);
  const payload = manifest(overrides);
  if (at) payload.generated_at = new Date(at - 20 * 60000).toISOString();
  payload.seasons = { 2026: 'seasons/2026.js?v=' + stamp(seasonText) };
  payload.files = { teams: 'teams.js?v=' + stamp(teamsText) };
  if (people.people !== false) writePeople(root, payload, people);
  fs.writeFileSync(path.join(root, 'data', 'ncaambb', 'manifest.json'),
    JSON.stringify(payload));
  // Michigan's mark only. Duke's id points at a file that is not here.
  const marks = path.join(root, 'assets', 'ncaambb');
  fs.mkdirSync(marks, { recursive: true });
  fs.writeFileSync(path.join(marks, '130.png'), PIXEL);
  fs.writeFileSync(path.join(marks, '130-dark.png'), PIXEL);
  copyAssets(UI, 'basketball.html', root);
  return { root, manifest: payload, at };
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
      // A frozen clock, so a test can put the page at a named moment -- an
      // evening after midnight UTC, say, which is the case that showed the
      // page and the export disagreeing about which day it was.
      if (built.at) {
        const Real = window.Date;
        const Frozen = function (...args) {
          return args.length ? new Real(...args) : new Real(built.at);
        };
        Frozen.prototype = Real.prototype;
        Frozen.now = () => built.at;
        Frozen.parse = Real.parse;
        Frozen.UTC = Real.UTC;
        window.Date = Frozen;
      }
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
  // board row there would call a correct page "never rendered". Every panel a
  // route can open on has to be listed here, People included -- leaving it
  // out made every direct link to a player page fail in this helper, before
  // the test that was meant to check the page had run a single assertion.
  await settle(dom, () => dom.window.document.querySelector(
    '#board tr, #refusal-list li, #cards .card, #team-body .directory, '
    + '#team-body .clubhead, #team-body .empty, #people-body h2, '
    + '#people-body p, #people-body .directory, #people-body tbody tr, '
    // A link straight to a game hides the cards, so without the game page
    // listed here every such link timed out as "never rendered" before the
    // test that was meant to check it had asserted anything -- exactly what
    // leaving People off this list used to do.
    + '#game-page .gp-hero, #game-page .empty'));
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

test('a club yet to play shows no record, and the table says how many',
  async () => {
    // Two days into 2026-27 the chain knew twelve teams. The other 353 carry
    // last season's rating, and a ranked table that does not say so reads as
    // results nobody has played for.
    const early = board().map((row, index) => (
      index === 0 ? row : Object.assign({}, row, {
        rated_games: 0, wins: 0, losses: 0, d1_wins: 0, d1_losses: 0
      })));
    const dom = await open({ board: early });
    const table = rows(dom);
    assert.equal(table[0][4], '30-4', 'a club that has played shows its record');
    assert.equal(table[1][4], '\u2014', 'and one that has not shows none');
    assert.equal(table[1][3], '2211', 'its carried rating is still shown');
    const note = dom.window.document.getElementById('board-note');
    assert.equal(note.hidden, false);
    assert.match(note.textContent,
      /2 of these 3 clubs have no Division I result yet/);
    dom.window.close();
  });

test('a record against a non-Division I opponent is still a record',
  async () => {
    // `records()` counts every opponent; the rating counts Division I games
    // alone. So a club two days into November whose only final was against a
    // Division II visitor is 1-0 with no rated game, and keying the record
    // cell on rated_games put an em dash over it -- while the note under the
    // table said it had not played, on the same screen as its own 1-0.
    const early = board().map((row, index) => Object.assign({}, row, {
      rated_games: 0, wins: index === 1 ? 1 : 0, losses: 0,
      d1_wins: 0, d1_losses: 0
    }));
    const dom = await open({ board: early });
    const table = rows(dom);
    assert.equal(table[1][4], '1-0', 'the record it really has');
    assert.equal(table[0][4], '\u2014', 'and none where there is none');
    const note = dom.window.document.getElementById('board-note');
    assert.match(note.textContent, /No club has a Division I result yet/);
    assert.doesNotMatch(note.textContent, /has not played/);
    dom.window.close();
  });

test('an evening reader gets tonight’s games, not tomorrow’s', async () => {
  // 7:30pm on 14 November in New York is already 00:30 on the 15th in UTC, so
  // for the whole of every evening the UTC date is tomorrow. The export
  // states a game's day in Eastern time, and the page read today in UTC, so
  // it opened on the NEXT day's schedule while the games on the strip were
  // being played. The payload holds both days here, because a payload with
  // only one would be clamped onto it whatever the page believed.
  const dom = await open({ at: '2026-11-15T00:30:00Z' }, [
    { id: 'ncaambb:cbbd:game:800', at: '2026-11-14',
      tip: '2026-11-15T00:30:00Z', status: 'in_progress',
      home: 'ncaambb:cbbd:team:1', away: 'ncaambb:cbbd:team:2',
      home_points: 31, away_points: 28 },
    { id: 'ncaambb:cbbd:game:801', at: '2026-11-15',
      tip: '2026-11-16T00:00:00Z', status: 'scheduled',
      home: 'ncaambb:cbbd:team:2', away: 'ncaambb:cbbd:team:3',
      home_points: null, away_points: null }
  ]);
  const doc = await scoreboard(dom);
  // The strip labels only the real today, and the pressed day is the one the
  // page opened on. Both have to be the 14th: the label alone would pass on a
  // page that opened on the 15th and drew Today three buttons to the left.
  const strip = [...doc.querySelectorAll('#days button')];
  const marked = strip.filter(node => node.textContent === 'Today');
  assert.equal(marked.length, 1, 'exactly one day is Today');
  assert.equal(marked[0].getAttribute('aria-pressed'), 'true',
    'and it is the day the page opened on');
  assert.deepEqual(strip.filter(node => node.textContent !== 'Today')
    .map(node => node.textContent).slice(3, 4), ['11-15'],
    'the 15th is the day AFTER today on this strip');
  assert.equal(doc.querySelectorAll('#cards .card').length, 1);
  assert.match(doc.getElementById('cards').textContent, /Duke/,
    'tonight’s game, which is the one being played');
  dom.window.close();
});

test('a settled season says nothing about clubs yet to play', async () => {
  const dom = await open();
  const note = dom.window.document.getElementById('board-note');
  assert.equal(note.hidden, true);
  assert.equal(note.textContent, '');
  dom.window.close();
});

test('a board where nobody has played says exactly that', async () => {
  const none = board().map(row => Object.assign({}, row, {
    rated_games: 0, wins: 0, losses: 0
  }));
  const dom = await open({ board: none });
  const note = dom.window.document.getElementById('board-note');
  assert.match(note.textContent, /No club has a Division I result yet/);
  dom.window.close();
});

test('the league selector is there, with basketball selected', async () => {
  const dom = await open();
  const buttons = [...dom.window.document.querySelectorAll('#leagues button')];
  // The registry gained the NFL and then the NBA, so the selector offers
  // four leagues. The assertion is the full list rather than a membership
  // check: a league appearing here that has no page behind it is exactly
  // the kind of thing this file exists to catch.
  //
  // The tabs are abbreviations since 2026-11-04 (Austin). The spelled-out
  // league stays reachable as the accessible name, so both are asserted: a
  // bare "NCAAM" with no accessible name is the defect this guards.
  assert.deepEqual(buttons.map(b => b.textContent),
    ['NCAAF', 'NCAAM', 'NFL', 'NBA']);
  assert.deepEqual(buttons.map(b => b.getAttribute('aria-label')),
    ['College Football', "Men's College Basketball",
      'National Football League', 'National Basketball Association']);
  const pressed = buttons.filter(b => b.getAttribute('aria-pressed') === 'true');
  assert.equal(pressed.length, 1);
  assert.equal(pressed[0].textContent, 'NCAAM');
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
    // Teams and People are real basketball sections and both now have a
    // panel. Stats still does not, and is disabled for exactly the reason
    // People used to be: an enabled tab with no panel hid every panel and
    // left a blank page.
    const dom = await open();
    const document = dom.window.document;
    const tabs = [...document.querySelectorAll('#tabs button')];
    const drawn = () => !document.getElementById('standings').hidden
      || !document.getElementById('scoreboard').hidden
      || !document.getElementById('people').hidden
      || !document.getElementById('teams').hidden;
    assert.ok(drawn(), 'something is drawn to begin with');
    for (const button of tabs) {
      if (button.disabled) continue;
      button.click();
      assert.ok(drawn(), `${button.textContent} leaves the page empty`);
    }
    for (const [name, live] of [['Teams', true], ['People', true],
                                ['Stats', false]]) {
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
  // 2 is now a schema this build reads, so the unknown one has to be a
  // version nothing understands. A test pinned to the next version number
  // starts asserting the opposite of its name the day that version ships.
  const dom = await open({ schema_version: 99 });
  const list = dom.window.document.querySelectorAll('#refusal-list li');
  assert.equal(list.length, 1);
  assert.match(list[0].textContent, /does not understand export schema 99/);
  dom.window.close();
});

test('both published schemas are read', async () => {
  // An export already on disk is schema 1 until the next refresh rebuilds it,
  // and the page has to work in between.
  for (const version of [1, 2]) {
    const dom = await open({ schema_version: version });
    assert.equal(dom.window.document.querySelectorAll('#board tr').length, 3,
      `schema ${version} draws the board`);
    dom.window.close();
  }
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

function statuses(document) {
  return [...document.querySelectorAll('#cards .card')].map(card =>
    card.querySelector('.status').textContent);
}

function order(document, label) {
  return [...document.querySelectorAll('#sorts button')]
    .find(button => button.textContent === label);
}

test('a day of games renders, best first', async () => {
  // The page opens on the best games, because a Division I evening holds
  // three hundred of them. The fixture's best game is Michigan-Duke, which
  // is also the one that has already finished, so the ordering is visible
  // in the statuses: by tip-off it would be second.
  const dom = await open();
  const document = await scoreboard(dom);
  const cards = [...document.querySelectorAll('#cards .card')];
  assert.equal(cards.length, 3);
  // Michigan-BYU (TBD) beats Duke-BYU on the stronger side's credit.
  assert.deepEqual(statuses(document), ['Final', 'Time TBD', '2:00 PM']);
  dom.window.close();
});

test('tip-off order is one tap away, and undecided tip-offs go last',
  async () => {
    const dom = await open();
    const document = await scoreboard(dom);
    order(document, 'Tip-off').click();
    // 19:00Z is 2pm where this file pins the timezone. A game whose time is
    // undecided says so instead, and goes last.
    assert.deepEqual(statuses(document), ['Final', '2:00 PM', 'Time TBD']);
    assert.equal(order(document, 'Tip-off').getAttribute('aria-pressed'),
                 'true');
    assert.equal(order(document, 'Best games').getAttribute('aria-pressed'),
                 'false');
    dom.window.close();
  });

test('changing the order never changes which games are shown', async () => {
  // It is an ordering, not a filter. A reader who taps it to find a game
  // must not lose one.
  const dom = await open();
  const document = await scoreboard(dom);
  const ids = () => [...document.querySelectorAll('#cards .card')]
    .map(card => card.textContent).sort();
  const best = ids();
  order(document, 'Tip-off').click();
  assert.deepEqual(ids(), best);
  assert.equal(best.length, 3);
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

test('no card anywhere shows a game clock', async () => {
  // There is no basketball live feed. A 0:00 would read as a period
  // expiring. A tip-off TIME is a different thing and is now shown, so this
  // asserts the absence of a clock rather than of every colon.
  const dom = await open();
  const document = await scoreboard(dom);
  const statuses = [...document.querySelectorAll('#cards .status')]
    .map(node => node.textContent);
  assert.ok(statuses.includes('2:00 PM'), 'a tip-off time is shown');
  for (const status of statuses) {
    assert.doesNotMatch(status, /0:00/);
    assert.doesNotMatch(status, /\d(st|nd|rd|th)|OT|Half/);
  }
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
      { id: 'g-home', at: DAY, tip: DAY + 'T19:00:00Z', status: 'final',
        home: 'ncaambb:cbbd:team:1', away: 'ncaambb:cbbd:team:2',
        home_points: 70, away_points: 60 },
      { id: 'g-away', at: daysFromToday(1), tip: daysFromToday(1) + 'T19:00:00Z', status: 'final',
        home: 'ncaambb:cbbd:team:3', away: 'ncaambb:cbbd:team:1',
        home_points: 80, away_points: 71 },
      { id: 'g-neutral', at: daysFromToday(2), tip: daysFromToday(2) + 'T19:00:00Z', status: 'final',
        neutral: 1, home: 'ncaambb:cbbd:team:1', away: 'ncaambb:cbbd:team:3',
        home_points: 66, away_points: 64 },
      { id: 'g-later', at: daysFromToday(3), tip: daysFromToday(3) + 'T19:00:00Z',
        status: 'scheduled', home: 'ncaambb:cbbd:team:1',
        away: 'ncaambb:cbbd:team:2', home_points: null, away_points: null }
    ];
    const dom = await open(undefined, slate,
      '#league=ncaambb&section=teams&team=1');
    const doc = dom.window.document;
    await settle(dom, () => doc.querySelector('#team-body table:not(.roster) tbody tr'));
    const table = [...doc.querySelectorAll('#team-body table:not(.roster) tbody tr')]
      .map(tr => [...tr.children].map(td => td.textContent));
    assert.deepEqual(table.map(row => row[1]),
      ['vs Duke', '@ BYU', 'N BYU', 'vs Duke']);
    // A game still to be played says when, not "Scheduled": 19:00Z is 2pm
    // where this file pins the timezone.
    assert.deepEqual(table.map(row => row[2]),
      ['W 70\u201360', 'L 71\u201380', 'W 66\u201364', '2:00 PM']);
    dom.window.close();
  });

test('an opponent on a club page opens that club', async () => {
  const dom = await open(undefined, undefined,
    '#league=ncaambb&section=teams&team=1');
  const doc = dom.window.document;
  await settle(dom, () => doc.querySelector('#team-body table:not(.roster) tbody tr'));
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
    { id: 'g-level', at: DAY, tip: DAY + 'T19:00:00Z', status: 'final',
      home: 'ncaambb:cbbd:team:1', away: 'ncaambb:cbbd:team:2',
      home_points: 70, away_points: 70 }
  ];
  const dom = await open(undefined, slate,
    '#league=ncaambb&section=teams&team=1');
  const doc = dom.window.document;
  await settle(dom, () => doc.querySelector('#team-body table:not(.roster) tbody tr'));
  const cells = [...doc.querySelectorAll('#team-body table:not(.roster) tbody tr td')]
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
      { id: 'g-dec', at: '2025-12-01', tip: '2025-12-01T19:00:00Z',
        status: 'scheduled',
        home: 'ncaambb:cbbd:team:1', away: 'ncaambb:cbbd:team:2',
        home_points: null, away_points: null },
      { id: 'g-nov-tbd', at: '2025-11-01', time_tbd: 1, status: 'scheduled',
        home: 'ncaambb:cbbd:team:1', away: 'ncaambb:cbbd:team:3',
        home_points: null, away_points: null },
      // The ids sort the OTHER way from the tip-offs on purpose: with the
      // early game named 'a' the id fallback produces the right order by
      // itself, and the test passes with the tip-off comparison deleted.
      { id: 'g-nov-a', at: '2025-11-01', tip: '2025-11-01T23:00:00Z',
        status: 'final',
        home: 'ncaambb:cbbd:team:1', away: 'ncaambb:cbbd:team:2',
        home_points: 81, away_points: 70 },
      { id: 'g-nov-z', at: '2025-11-01', tip: '2025-11-01T17:00:00Z',
        status: 'final',
        home: 'ncaambb:cbbd:team:1', away: 'ncaambb:cbbd:team:3',
        home_points: 77, away_points: 60 }
    ];
    const dom = await open(undefined, slate,
      '#league=ncaambb&section=teams&team=1');
    const doc = dom.window.document;
    await settle(dom, () => doc.querySelector('#team-body table:not(.roster) tbody tr'));
    const dates = [...doc.querySelectorAll('#team-body table:not(.roster) tbody tr')]
      .map(tr => tr.children[0].textContent);
    assert.deepEqual(dates,
      ['2025-11-01', '2025-11-01', '2025-11-01', '2025-12-01']);
    // Within the one day, the known tip-offs come in order and the
    // undecided one is last -- which is the rule the scoreboard has.
    const against = [...doc.querySelectorAll('#team-body table:not(.roster) tbody tr')]
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

// --- people: the tab, a player's career, a club's roster -------------------
//
// The data layer for these was loaded nationally by B02 and never published,
// so the People tab was drawn disabled from the day the league launched.
// tests/test_ncaambb_export_people.py pins the files these read.

function peopleTab(dom, hash) {
  const doc = dom.window.document;
  const button = [...doc.querySelectorAll('#tabs button')]
    .find(b => b.textContent === 'People');
  button.click();
  return doc;
}

test('the People tab is enabled only when the export carries people',
  async () => {
    const withPeople = await open();
    const enabled = [...withPeople.window.document
      .querySelectorAll('#tabs button')].find(b => b.textContent === 'People');
    assert.equal(enabled.disabled, false);
    withPeople.window.close();

    // An export with no people at all -- which is every basketball export
    // published before this change. The tab must say so, not draw an empty
    // panel and not vanish.
    const without = await open({ people: false });
    const doc = peopleTab(without, '');
    await settle(without, () => doc.getElementById('people-body').textContent);
    assert.match(doc.getElementById('people-body').textContent,
      /no player data yet/);
    without.window.close();
  });

test('a people payload this page cannot read counts as no people',
  async () => {
    // The payload's shape is versioned apart from the manifest's. Version 1
    // carried no day and no opponent on a line, so decoding it with this
    // page's reader would draw a career log of em dashes and present it as
    // the player's record. Better to say there is no player data.
    const dom = await open({ peopleVersion: 1 });
    const doc = peopleTab(dom, '');
    await settle(dom, () => doc.getElementById('people-body').textContent);
    assert.match(doc.getElementById('people-body').textContent,
      /no player data yet/);
    // And nothing is half-drawn: no leaders board, no search box.
    assert.equal(doc.querySelector('#people-body tbody tr'), null);
    assert.equal(doc.getElementById('player-search'), null);
    dom.window.close();
  });

test('a run of seasons is named rather than listed with "and"', async () => {
  // Six seasons read "2021-22 and 2022-23 and 2023-24 and ..." on the real
  // export. One season and two are still listed, which is why this needs
  // three to say anything at all.
  const dom = await open({ extraSeasons: [2024, 2025] });
  const doc = peopleTab(dom, '');
  await settle(dom, () => doc.querySelector('#people-body .filters'));
  assert.match(doc.getElementById('people-body').textContent,
    /Players are loaded for 2023-24 through 2025-26\./);
  dom.window.close();
});

test('the People tab opens on the season’s leading scorers', async () => {
  const dom = await open();
  const doc = peopleTab(dom, '');
  await settle(dom, () => doc.querySelector('#people-body tbody tr'));
  const table = [...doc.querySelectorAll('#people-body tbody tr')]
    .map(tr => [...tr.children].map(td => td.textContent));
  // 101 scored 36 over two games, 205 scored 19 over one. Ordered by total
  // points: a per-game board this early in a season ranks players who have
  // played once.
  assert.deepEqual(table.map(row => row[1]),
    ['Hal Guard', 'Otto Center', 'Ivan Bench']);
  assert.deepEqual(table[0].slice(3), ['2', '36', '18']);
  dom.window.close();
});

test('a player with no recorded points is still listed, with no average',
  async () => {
    const dom = await open();
    const doc = peopleTab(dom, '');
    await settle(dom, () => doc.querySelector('#people-body tbody tr'));
    const row = [...doc.querySelectorAll('#people-body tbody tr')]
      .map(tr => [...tr.children].map(td => td.textContent))
      .find(cells => cells[1] === 'Ivan Bench');
    // One game, no minutes recorded at all, nil points. The minutes column
    // is an em dash and the nil is a measurement.
    assert.equal(row[3], '1');
    assert.equal(row[4], '0');
    dom.window.close();
  });

test('searching finds a player by name', async () => {
  const dom = await open();
  const doc = peopleTab(dom, '');
  await settle(dom, () => doc.getElementById('player-search'));
  const box = doc.getElementById('player-search');
  box.value = 'center';
  box.dispatchEvent(new dom.window.Event('input'));
  await settle(dom, () => doc.getElementById('people-body')
    .textContent.includes('matches'));
  const names = [...doc.querySelectorAll('#people-body .directory li button')]
    .map(node => node.textContent);
  assert.deepEqual(names, ['Otto Center']);
  box.value = 'nobody at all';
  box.dispatchEvent(new dom.window.Event('input'));
  await settle(dom, () => doc.getElementById('people-body')
    .textContent.includes('No player matches'));
  dom.window.close();
});

test('a player page lists every game of his season', async () => {
  const dom = await open(undefined, undefined,
    '#league=ncaambb&section=people&player=101');
  const doc = dom.window.document;
  await settle(dom, () => doc.querySelector('#people-body tbody tr'));
  assert.match(doc.querySelector('#people-body h2').textContent, /Hal Guard/);
  assert.match(doc.querySelector('#people-body .sub').textContent,
    /G · #3 · Provo, UT/);
  const table = [...doc.querySelectorAll('#people-body tbody tr')]
    .map(tr => [...tr.children].map(td => td.textContent));
  assert.equal(table.length, 2, 'both games, and only his');
  assert.deepEqual(table[0].slice(2), ['30', '22']);
  dom.window.close();
});

test('a player page lists his games oldest first, not by game id', async () => {
  // The archive's game ids do not run in date order -- CFBD numbers some
  // postseason games as though they were week one, which zeroed 1,353
  // football season Elo changes before anyone noticed -- so a career log
  // ordered by id puts games in the wrong order. Here game 703 was played
  // YESTERDAY and carries the higher id.
  const yesterday = daysFromToday(-1);
  const dom = await open({
    lines: { 101: [[700, 0, 30, 22, 1, DAY, 1, 'H'],
                   [703, 0, 24, 9, 0, yesterday, 1, 'H']] }
  }, [
    { id: 'ncaambb:cbbd:game:700', at: DAY, tip: DAY + 'T17:00:00Z',
      status: 'final', home: TEAM_KEYS[0], away: TEAM_KEYS[1],
      home_points: 78, away_points: 61 },
    { id: 'ncaambb:cbbd:game:703', at: yesterday,
      tip: yesterday + 'T17:00:00Z', status: 'final', home: TEAM_KEYS[0],
      away: TEAM_KEYS[2], home_points: 70, away_points: 66 }
  ], '#league=ncaambb&section=people&player=101');
  const doc = dom.window.document;
  await settle(dom, () => doc.querySelector('#people-body tbody tr'));
  const table = [...doc.querySelectorAll('#people-body tbody tr')]
    .map(tr => [...tr.children].map(td => td.textContent));
  assert.deepEqual(table.map(row => row[2]), ['24', '30'],
    'yesterday’s game first, though its id is higher');
  dom.window.close();
});

test('a player page says who he played and whether it was at home',
  async () => {
    const dom = await open(undefined, undefined,
      '#league=ncaambb&section=people&player=101');
    const doc = dom.window.document;
    await settle(dom, () => doc.querySelector('#people-body tbody tr'));
    const against = [...doc.querySelectorAll('#people-body tbody tr')]
      .map(tr => tr.children[1].textContent);
    // Game 700 is Michigan at home to Duke, 701 Michigan at home to BYU.
    assert.deepEqual(against, ['vs Duke', 'vs BYU']);
    dom.window.close();
  });

test('a season whose schedule is not loaded is still dated', async () => {
  // THE BUG THIS CARRIES ITS OWN DATE FOR. A career spans four or five
  // seasons and the page loads one schedule, so dating a game by looking it
  // up in the season payload dated one season and left the rest a table of
  // em dashes. Found in Chromium against the real export, not here.
  //
  // Game 650 is in no loaded season payload at all, which is what every game
  // of a previous season looks like to this page.
  const dom = await open({
    lines: { 101: [[650, 0, 26, 18, 1, '2025-12-19', 1, 'A']] }
  }, undefined, '#league=ncaambb&section=people&player=101');
  const doc = dom.window.document;
  await settle(dom, () => doc.querySelector('#people-body tbody tr'));
  const row = [...doc.querySelector('#people-body tbody tr').children]
    .map(td => td.textContent);
  assert.equal(row[0], '2025-12-19', 'the line’s own day');
  assert.equal(row[1], '@ Duke', 'and its own opponent, away');
  dom.window.close();
});

test('a player page sums only what was measured', async () => {
  const dom = await open(undefined, undefined,
    '#league=ncaambb&section=people&player=102');
  const doc = dom.window.document;
  await settle(dom, () => doc.querySelector('#people-body tbody tr'));
  const line = doc.querySelector('#people-body p.empty').textContent;
  assert.match(line, /1 game/);
  assert.match(line, /0 points/);
  // Nobody recorded his minutes, so no minute total is claimed.
  assert.doesNotMatch(line, /minutes/);
  assert.equal(doc.querySelector('#people-body tbody tr').children[2]
    .textContent, '—');
  dom.window.close();
});

test('a player id nobody published says so rather than drawing a blank',
  async () => {
    const dom = await open(undefined, undefined,
      '#league=ncaambb&section=people&player=999999');
    const doc = dom.window.document;
    await settle(dom, () => doc.getElementById('people-body')
      .textContent.includes('No player'));
    assert.match(doc.getElementById('people-body').textContent,
      /No player in this export has that id/);
    dom.window.close();
  });

test('a player page is a link: the URL names him and Back leaves him',
  async () => {
    const dom = await open();
    const doc = peopleTab(dom, '');
    await settle(dom, () => doc.querySelector('#people-body tbody tr'));
    doc.querySelector('#people-body tbody tr button').click();
    await settle(dom, () => doc.querySelector('#people-body h2'));
    assert.match(dom.window.location.hash, /player=101/);
    assert.match(dom.window.location.hash, /section=people/);
    doc.querySelector('#people-body .back button').click();
    await settle(dom, () => doc.getElementById('player-search'));
    assert.doesNotMatch(dom.window.location.hash, /player=/);
    dom.window.close();
  });

test('leaving People takes the player out of the URL with it', async () => {
  const dom = await open(undefined, undefined,
    '#league=ncaambb&section=people&player=101');
  const doc = dom.window.document;
  await settle(dom, () => doc.querySelector('#people-body h2'));
  [...doc.querySelectorAll('#tabs button')]
    .find(b => b.textContent === 'Standings').click();
  assert.doesNotMatch(dom.window.location.hash, /player=/);
  dom.window.close();
});

test('a club page carries its roster under its season', async () => {
  const dom = await open(undefined, undefined,
    '#league=ncaambb&section=teams&team=1');
  const doc = dom.window.document;
  await settle(dom, () => doc.querySelector('#team-body table.roster tbody tr'));
  const table = [...doc.querySelectorAll('#team-body table.roster tbody tr')]
    .map(tr => [...tr.children].map(td => td.textContent));
  assert.deepEqual(table.map(row => row[0]), ['#3 Hal Guard',
                                              '#12 Ivan Bench']);
  assert.deepEqual(table[0].slice(1), ['G', '2', '58', '36', '18']);
  dom.window.close();
});

test('a roster name opens that player’s page', async () => {
  const dom = await open(undefined, undefined,
    '#league=ncaambb&section=teams&team=1');
  const doc = dom.window.document;
  await settle(dom, () => doc.querySelector('#team-body table.roster tbody tr'));
  doc.querySelector('#team-body table.roster tbody tr button').click();
  await settle(dom, () => doc.querySelector('#people-body h2'));
  assert.match(dom.window.location.hash, /section=people/);
  assert.match(doc.querySelector('#people-body h2').textContent, /Hal Guard/);
  dom.window.close();
});

test('a club page in a season with no people shows no roster at all',
  async () => {
    const dom = await open({ people: false }, undefined,
      '#league=ncaambb&section=teams&team=1');
    const doc = dom.window.document;
    await settle(dom, () => doc.querySelector('#team-body tbody tr'));
    assert.equal(doc.querySelector('#team-body table.roster'), null);
    assert.doesNotMatch(doc.getElementById('team-body').textContent,
      /Roster/);
    dom.window.close();
  });

test('a club in the season being played says where its squad is', async () => {
  // Player files cover the most recent few seasons and the season in
  // progress is fetched last, so the season every link lands on is routinely
  // the one with no roster. Drawing nothing there left a reader with a
  // schedule, no squad, and no hint that a squad exists anywhere.
  // The rated season on disk is 2026; the manifest claims people for 2025
  // only, which is a rated season with no player files.
  // TWO claimed seasons, so "the most recent" is a different answer from
  // "the first one listed" -- with one, naming either passes.
  const dom = await open({ peopleSeasonKeys: [2024, 2025] }, undefined,
    '#league=ncaambb&section=teams&team=1');
  const doc = dom.window.document;
  await settle(dom, () => doc.querySelector('#team-body .clubhead'));
  const text = doc.querySelector('#team-body').textContent;
  assert.match(text, /Players are not published for 2025-26/);
  assert.match(text, /most recent squad on record is 2024-25/);
  assert.equal(doc.querySelector('#team-body table.roster'), null);
  dom.window.close();
});

// --- a school's mark beside its name ---------------------------------------

test('a club on the board carries its mark, in both cuts', async () => {
  const dom = await open();
  const cell = dom.window.document.querySelector('#board tr td:nth-child(2)');
  const img = cell.querySelector('img.logo-mark');
  // The <img> is the DARK cut, because this page's own theme is dark and a
  // browser that ignores the <source> falls back to the <img>.
  assert.equal(img.getAttribute('src'), 'assets/ncaambb/130-dark.png');
  const source = cell.querySelector('picture source');
  assert.equal(source.getAttribute('srcset'), 'assets/ncaambb/130.png');
  assert.equal(source.getAttribute('media'), '(prefers-color-scheme: light)');
  // The mark is decoration beside a name that is already there.
  assert.equal(img.getAttribute('alt'), '');
  dom.window.close();
});

test('the name is still the only text in the cell', async () => {
  // A mark drawn as initials would make this cell read "MMichigan" when it
  // is copied or spoken.
  const dom = await open();
  const cell = dom.window.document.querySelector('#board tr td:nth-child(2)');
  assert.equal(cell.textContent, 'Michigan');
  dom.window.close();
});

test('a club with no mark keeps its place in the row', async () => {
  const dom = await open();
  const cells = [...dom.window.document.querySelectorAll(
    '#board tr td:nth-child(2)')];
  const byu = cells[2];
  assert.equal(byu.textContent, 'BYU');
  assert.equal(byu.querySelector('img'), null);
  assert.ok(byu.querySelector('.logo-mark.blank'),
            'a blank box of the same size keeps the names aligned');
  dom.window.close();
});

test('a mark that has not been banked yet leaves no broken image', async () => {
  // The marks are fetched by a workflow of their own, so a deploy can carry
  // an export naming an id whose file is not there yet. 365 broken images
  // would be worse than the page as it was before marks existed.
  //
  // The event is dispatched rather than awaited: jsdom only fetches an <img>
  // when the optional canvas package is installed, so a missing file never
  // fails on its own here. A browser fires exactly this event, and the
  // Chromium pass over the real export is what proves it end to end.
  const dom = await open();
  const document = dom.window.document;
  const duke = () => [...document.querySelectorAll('#board tr')][1]
    .querySelector('td:nth-child(2)');
  const img = duke().querySelector('img.logo-mark');
  assert.equal(img.getAttribute('src'), 'assets/ncaambb/150-dark.png');
  img.dispatchEvent(new dom.window.Event('error'));
  assert.equal(duke().querySelector('img'), null);
  assert.ok(duke().querySelector('.logo-mark.blank'));
  assert.equal(duke().textContent, 'Duke');
  dom.window.close();
});

test('the marks are drawn on the cards and the club page too', async () => {
  // One helper draws every one of them, so a logo cannot appear on one
  // board and not on another.
  const dom = await open();
  const document = await scoreboard(dom);
  assert.ok(document.querySelector('#cards .card img.logo-mark'),
            'a game card names its clubs with their marks');
  const teams = [...document.querySelectorAll('#tabs button')]
    .find(button => button.textContent === 'Teams');
  teams.click();
  await settle(dom, () => document.querySelector('#team-body button.link'));
  assert.ok(document.querySelector('#team-body button.link .logo-mark'),
            'the club directory names its clubs with their marks');
  dom.window.close();
});

test('the tab bar is only fixed to the foot at phone width', () => {
  // This page shipped with `position: fixed; bottom: 0` and NO media query,
  // so a desktop window wore a row of five stranded across the foot of the
  // screen while college football and the NFL put theirs under the wordmark.
  //
  // A stylesheet assertion, because jsdom evaluates no media query: it reads
  // the rule that was wrong rather than the layout, and the Chromium pass at
  // 1400px and 430px is what proves the layout. Keeping it anyway, because
  // the rule is the thing that regressed.
  const page = fs.readFileSync(path.join(UI, 'basketball.html'), 'utf8');
  const start = page.indexOf('nav.tabs {');
  // From the tab rules onwards: the page carries an earlier 719px query of
  // its own, for the foot padding the fixed bar needs.
  const phoneAt = page.indexOf('@media (max-width: 719px)', start);
  assert.ok(start > 0 && phoneAt > start);
  assert.doesNotMatch(page.slice(start, phoneAt), /position:\s*fixed/);
  assert.match(page.slice(phoneAt, phoneAt + 400),
               /nav\.tabs\s*{[^}]*position:\s*fixed/);
});

test('the tabs sit inside the page wrap, where a desktop reads them', () => {
  // Under the wordmark, as both football pages have them. Outside the wrap
  // they can only ever be the phone's bottom bar.
  const page = fs.readFileSync(path.join(UI, 'basketball.html'), 'utf8');
  const wrap = page.slice(page.indexOf('<div class="wrap">'),
                          page.indexOf('</div>\n\n<script'));
  assert.match(wrap, /<\/header>\s*<nav class="tabs"/);
});

// --- a game is a place, with an address ------------------------------------

/* The scoreboard's cards carry Elo now, because a game page's matchup table
   reads the model's own chain off the game. The schedule in `games()` has
   none, so these tests describe their own. */
function ratedGames() {
  const [one, two, three] = games();
  return [
    // A FINAL game the model learned from: the whole chain.
    Object.assign({}, one, {
      home_elo: 2240.4, away_elo: 2100.1, home_wp: 0.6894,
      home_elo_post: 2254.2, away_elo_post: 2086.3, elo_change: 13.84,
      rateable: 1, d1: 1 }),
    // A game NOT YET PLAYED: today's ratings, said to be live.
    Object.assign({}, two, {
      home_elo: 2240.4, away_elo: 1788.4, home_wp: 0.9312, wp_live: 1,
      rateable: 0, d1: 1 }),
    // A game the model never rated at all.
    Object.assign({}, three, { rateable: 0, d1: 0 })
  ];
}

const GAME_ROUTE = '#league=ncaambb&section=games&game=700&season=2026';

function heroClubs(doc) {
  return [...doc.querySelectorAll('#game-page .gp-board .gp-side .gp-team')]
    .map(node => node.textContent);
}

function matchupRows(doc) {
  return [...doc.querySelectorAll('#game-page .gp-matchup tbody tr')]
    .map(tr => [...tr.children].map(cell => cell.textContent));
}

async function openGame(hash, schedule) {
  const dom = await open(undefined, schedule || ratedGames(), hash);
  await settle(dom, () => dom.window.document.querySelector(
    '#game-page .gp-hero, #game-page .empty'));
  return dom;
}

test('a card is a button and opens its game, with the season in the URL',
  async () => {
    const dom = await open(undefined, ratedGames(),
      '#league=ncaambb&section=games');
    const doc = dom.window.document;
    const card = doc.querySelector('#cards button.card.open');
    assert.ok(card, 'a card is not reachable by keyboard');
    card.dispatchEvent(new dom.window.Event('click', { bubbles: true }));
    await settle(dom, () => doc.querySelector('#game-page .gp-hero'));
    // The season is on the link because the page opens on the latest one: a
    // bare id would resolve against the wrong year, and would start finding
    // the WRONG game the day the provider reuses an id.
    assert.match(dom.window.location.hash, /&game=\d+/);
    assert.match(dom.window.location.hash, /&season=2026/);
    dom.window.close();
  });

test('opening a game hides the whole slate, not only the cards', async () => {
  // `hidden` is an attribute and any `display` rule beats it: `.cards` is a
  // grid and `.days` a flex row, so this asks whether they are visible
  // rather than which element carries the attribute.
  const dom = await openGame(GAME_ROUTE);
  const doc = dom.window.document;
  for (const id of ['days', 'filters', 'sorts', 'cards']) {
    assert.equal(doc.getElementById(id).hidden, true, id + ' is still shown');
  }
  assert.equal(doc.getElementById('game-page').hidden, false);
  dom.window.close();
});

test('a finished game names both clubs, scores them and dims the loser',
  async () => {
    const dom = await openGame(GAME_ROUTE);
    const doc = dom.window.document;
    assert.deepEqual(heroClubs(doc), ['Duke', 'Michigan']);
    assert.deepEqual(
      [...doc.querySelectorAll('#game-page .gp-score')]
        .map(node => node.textContent), ['61', '78']);
    const sides = [...doc.querySelectorAll('#game-page .gp-side')];
    assert.ok(sides[0].classList.contains('lost'), 'the loser is not dimmed');
    assert.ok(sides[1].classList.contains('won'), 'the winner is not marked');
    assert.match(doc.querySelector('#game-page .gl-top').textContent, /Final/);
    dom.window.close();
  });

test('a finished game shows the chain the model stored before tip-off',
  async () => {
    const dom = await openGame(GAME_ROUTE);
    const doc = dom.window.document;
    assert.deepEqual(
      [...doc.querySelectorAll('#game-page .gp-matchup thead tr')[1].children]
        .map(cell => cell.textContent),
      ['Team', 'Before', 'Win chance', 'After', 'Δ Elo']);
    assert.deepEqual(matchupRows(doc), [
      // Elo displays as a whole integer; a rating CHANGE keeps its decimal,
      // and the away side's is the home side's negated.
      ['Duke', '2100', '31.1%', '2086', '−13.8'],
      ['Michigan', '2240', '68.9%', '2254', '+13.8']
    ]);
    assert.match(doc.querySelector('#game-page .section-note').textContent,
      /Recorded before tip-off/);
    dom.window.close();
  });

test('a game not yet played says its numbers are today’s, not a forecast',
  async () => {
    const dom = await openGame(
      '#league=ncaambb&section=games&game=701&season=2026');
    const doc = dom.window.document;
    const head = [...doc.querySelectorAll(
      '#game-page .gp-matchup thead tr')[1].children]
      .map(cell => cell.textContent);
    // "Now", not "Before": giving a moving number and a measurement the same
    // heading is how a reader comes to believe the model predicted something
    // it never did. And no After or delta, because nothing has happened.
    assert.deepEqual(head, ['Team', 'Now', 'Win chance']);
    assert.match(doc.querySelector('#game-page .section-note').textContent,
      /stand today/);
    // The hero gives the space to the marks instead of to absent scores.
    assert.ok(doc.querySelector('#game-page .gp-pregame-hero'));
    dom.window.close();
  });

test('a game the model never rated says so instead of showing dashes',
  async () => {
    const dom = await openGame(
      '#league=ncaambb&section=games&game=702&season=2026');
    const doc = dom.window.document;
    assert.equal(doc.querySelectorAll('#game-page .gp-matchup').length, 0,
      'a table of dashes was drawn');
    assert.match(doc.querySelector('#game-page .empty').textContent,
      /not a Division I club/);
    // The hero is still there: the game happened, the rating did not.
    assert.deepEqual(heroClubs(doc), ['BYU', 'Duke']);
    dom.window.close();
  });

test('a game id this season does not hold is a dead end with a way out',
  async () => {
    const dom = await openGame(
      '#league=ncaambb&section=games&game=99999&season=2026');
    const doc = dom.window.document;
    assert.match(doc.querySelector('#game-page .empty').textContent,
      /No game in 2025-26 has that id/);
    assert.ok(doc.querySelector('#game-page .back button'),
      'the reader cannot walk back out');
    dom.window.close();
  });

test('pressing a tab closes the game, in the URL as well as on the page',
  async () => {
    const dom = await openGame(GAME_ROUTE);
    const doc = dom.window.document;
    const standings = [...doc.querySelectorAll('.tabs button')]
      .find(node => node.textContent === 'Standings');
    standings.dispatchEvent(new dom.window.Event('click', { bubbles: true }));
    await settle(dom, () => !doc.getElementById('standings').hidden);
    assert.doesNotMatch(dom.window.location.hash, /game=/,
      'the game is still in the URL');
    dom.window.close();
  });

test('the meetings panel says it covers this season only', async () => {
  // Only the open season is loaded, so this is not the all-time series and
  // must not read as one.
  const dom = await openGame(GAME_ROUTE);
  const panel = [...dom.window.document.querySelectorAll('#game-page .panel')]
    .find(node => /Meetings/.test(node.textContent));
  assert.ok(panel, 'no meetings panel');
  assert.match(panel.textContent, /This season only/);
  dom.window.close();
});
