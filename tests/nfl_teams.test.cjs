// The club directory and a franchise's own page.
//
// The pure helpers are tested as functions and the panel is tested by
// loading ui/nfl.html in jsdom with a real manifest, a real season payload
// and a real history file on disk, because a green build is not a rendered
// page.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { JSDOM } = require('jsdom');

const UI = path.join(__dirname, '..', 'ui');
const app = require(path.join(UI, 'nfl_app.js'));

const KC = 'nfl:nflverse:franchise:KC';
const BUF = 'nfl:nflverse:franchise:BUF';
const LA = 'nfl:nflverse:franchise:LA';
const HOU = 'nfl:nflverse:franchise:HOU';
const REID = 'nfl:nflverse:coach:Andy Reid';
const NAGY = 'nfl:nflverse:coach:Matt Nagy';

function teams() {
  return {
    [KC]: { code: 'KC', name: 'Kansas City Chiefs', conference: 'AFC',
      division: 'West' },
    [BUF]: { code: 'BUF', name: 'Buffalo Bills', conference: 'AFC',
      division: 'East' },
    [LA]: { code: 'LA', name: 'Los Angeles Rams', conference: 'NFC',
      division: 'West' },
    // In the export, with no row on this season's board: a franchise that
    // did not play still has a page.
    [HOU]: { code: 'HOU', name: 'Houston Texans', conference: 'AFC',
      division: 'South' }
  };
}

// Deliberately NOT in kickoff order, so an implementation that draws the
// export's order rather than sorting is visible.
function games() {
  return [
    { id: 'g2', date: '2026-09-20', at: '2026-09-20T17:00:00Z', week: 2,
      type: 'regular', home: LA, away: KC, home_points: 17, away_points: 17,
      rateable: 1, home_elo: 1500, away_elo: 1648.4, home_wp: 0.42,
      home_coach: 'Sean McVay', away_coach: 'Andy Reid' },
    { id: 'g3', date: '2026-09-27', at: '2026-09-27T17:00:00Z', week: 3,
      type: 'regular', home: KC, away: BUF, home_points: null,
      away_points: null, rateable: 0, home_elo: 1652.0, away_elo: 1588.0,
      home_wp: 0.63 },
    { id: 'g1', date: '2026-09-13', at: '2026-09-13T17:00:00Z', week: 1,
      type: 'regular', home: KC, away: BUF, home_points: 24,
      away_points: 20, rateable: 1, home_elo: 1630.0, away_elo: 1600.0,
      home_wp: 0.6, home_coach: 'Andy Reid', away_coach: 'Sean McDermott' }
  ];
}

function board() {
  return [
    { team: KC, code: 'KC', conference: 'AFC', division: 'West',
      rating: 1648.4, rank: 1, wins: 1, losses: 0, ties: 1, rated_games: 2 },
    { team: BUF, code: 'BUF', conference: 'AFC', division: 'East',
      rating: 1591.6, rank: 2, wins: 0, losses: 1, ties: 0, rated_games: 1 },
    { team: LA, code: 'LA', conference: 'NFC', division: 'West',
      rating: 1500.0, rank: 3, wins: 0, losses: 0, ties: 1, rated_games: 1 }
  ];
}

function coachSeasons() {
  return [
    // One season, TWO head coaches: one row on the page, both named.
    { coach_key: REID, coach_name: 'Andy Reid', season: 2026, team_key: KC,
      games: 6, wins: 4, losses: 1, ties: 1, postseason_games: 0,
      postseason_wins: 0, expected_wins: 3.4, wins_above: 0.6,
      elo_change: 15.4 },
    { coach_key: NAGY, coach_name: 'Matt Nagy', season: 2026, team_key: KC,
      games: 4, wins: 1, losses: 3, ties: 0, postseason_games: 0,
      postseason_wins: 0, expected_wins: 2.1, wins_above: -1.1,
      elo_change: -8.2 },
    { coach_key: REID, coach_name: 'Andy Reid', season: 2025, team_key: KC,
      games: 20, wins: 16, losses: 4, ties: 0, postseason_games: 4,
      postseason_wins: 3, expected_wins: 12.5, wins_above: 3.5,
      elo_change: 20.1 },
    // Another franchise's season, which must not appear on KC's page.
    { coach_key: 'nfl:nflverse:coach:Sean McDermott',
      coach_name: 'Sean McDermott', season: 2026, team_key: BUF, games: 10,
      wins: 6, losses: 4, ties: 0, postseason_games: 0, postseason_wins: 0,
      expected_wins: 5.5, wins_above: 0.5, elo_change: 4.0 }
  ];
}

function history() {
  return {
    coaches: [],
    coach_seasons: coachSeasons(),
    track_record: { seasons: [], totals: {} },
    conferences: []
  };
}

// --- the pure helpers ------------------------------------------------------

test('a schedule holds only that club\'s games, in the order played', () => {
  const rows = app.teamSchedule(games(), KC);
  assert.deepEqual(rows.map(row => row.game.id), ['g1', 'g2', 'g3']);
  const buf = app.teamSchedule(games(), BUF);
  assert.deepEqual(buf.map(row => row.game.id), ['g1', 'g3']);
});

test('a row is stated from the club\'s own side', () => {
  // g2 is KC AWAY at the Rams. Every number must flip with the side, and
  // the away win probability is the home one's complement rather than a
  // second rounding of the same fraction.
  const row = app.teamSchedule(games(), KC).find(r => r.game.id === 'g2');
  assert.equal(row.home, false);
  assert.equal(row.opponent, LA);
  assert.equal(row.elo, 1648.4);
  assert.equal(row.opponent_elo, 1500);
  assert.equal(row.points, 17);
  assert.equal(row.against, 17);
  assert.ok(Math.abs(row.win_probability - 0.58) < 1e-9);
  assert.equal(row.coach, 'Andy Reid');
  const home = app.teamSchedule(games(), KC).find(r => r.game.id === 'g1');
  assert.equal(home.home, true);
  assert.equal(home.opponent, BUF);
  assert.equal(home.points, 24);
  assert.equal(home.against, 20);
  assert.equal(home.win_probability, 0.6);
});

test('a tie is a result and an unplayed game is not', () => {
  const rows = app.teamSchedule(games(), KC);
  assert.deepEqual(rows.map(row => row.result), ['W', 'T', '']);
  assert.deepEqual(app.teamRecord(rows), { wins: 1, losses: 0, ties: 1 });
  assert.deepEqual(app.teamRecord(app.teamSchedule(games(), BUF)),
    { wins: 0, losses: 1, ties: 0 });
});

test('a club with no game in the season has an empty schedule', () => {
  assert.deepEqual(app.teamSchedule(games(), HOU), []);
  assert.deepEqual(app.teamRecord([]), { wins: 0, losses: 0, ties: 0 });
});

test('a week off is a row, not a gap in the weeks', () => {
  // KC plays weeks 1 and 2 here and the season runs to week 3, so the club
  // is idle in week 3 -- while BUF, which plays weeks 1 and 3, is idle in
  // week 2. A table that simply skipped the week would read as a hole in
  // the archive.
  const rows = app.withByeWeeks(app.teamSchedule(games(), BUF), games());
  assert.deepEqual(rows.map(row => row.bye ? 'bye ' + row.week
    : row.game.id), ['g1', 'bye 2', 'g3']);
  // The run of weeks comes from the season's own games, so a 17-week
  // season does not grow an 18th bye.
  const short = games().filter(game => Number(game.week) < 3);
  assert.deepEqual(
    app.withByeWeeks(app.teamSchedule(short, BUF), short)
      .map(row => row.bye ? 'bye ' + row.week : row.game.id),
    ['g1', 'bye 2']);
});

test('a club with no game gets no byes at all', () => {
  // An expansion team's missing season is not eighteen weeks off.
  assert.deepEqual(app.withByeWeeks(app.teamSchedule(games(), HOU), games()),
    []);
});

test('a season with two head coaches is one row, with both names', () => {
  const rows = app.franchiseSeasons(coachSeasons(), KC);
  assert.deepEqual(rows.map(row => row.season), [2026, 2025]);
  const split = rows[0];
  assert.deepEqual(split.coaches, ['Andy Reid', 'Matt Nagy']);
  assert.equal(split.games, 10);
  assert.equal(split.wins, 5);
  assert.equal(split.losses, 4);
  assert.equal(split.ties, 1);
  // The two tenures' Elo changes add up to the franchise's season.
  assert.ok(Math.abs(split.elo_change - 7.2) < 1e-9);
  assert.ok(Math.abs(split.wins_above - -0.5) < 1e-9);
});

test('another franchise\'s seasons are not on this one\'s page', () => {
  const rows = app.franchiseSeasons(coachSeasons(), KC);
  assert.equal(rows.length, 2);
  assert.deepEqual(app.franchiseSeasons(coachSeasons(), LA), []);
});

test('a missing Elo change stays missing rather than reading as zero', () => {
  const rows = app.franchiseSeasons([
    { coach_key: REID, coach_name: 'Andy Reid', season: 2026, team_key: KC,
      games: 10, wins: 7, losses: 3, ties: 0, postseason_games: 0,
      postseason_wins: 0 }
  ], KC);
  assert.equal(rows[0].elo_change, null);
  assert.equal(rows[0].wins_above, null);
  assert.equal(rows[0].wins, 7);
});

// --- the panel, driven as a page -------------------------------------------

function stamp(text) {
  return require('node:crypto').createHash('sha256')
    .update(text).digest('hex').slice(0, 10);
}

function site(options) {
  const settings = options || {};
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'nfl-teams-'));
  const data = path.join(root, 'data', 'nfl', 'seasons');
  fs.mkdirSync(data, { recursive: true });
  const payload = { season: 2026, board: board(), games: games(),
    coaches: coachSeasons().filter(row => row.season === 2026) };
  const seasonText = 'window.NFL_SEASONS=(window.NFL_SEASONS||{});'
    + 'window.NFL_SEASONS[2026]=' + JSON.stringify(payload) + ';\n';
  const teamsText = 'window.NFL_TEAMS=' + JSON.stringify(teams()) + ';\n';
  const historyText = 'window.NFL_HISTORY='
    + JSON.stringify(history()) + ';\n';
  fs.writeFileSync(path.join(data, '2026.js'), seasonText);
  fs.writeFileSync(path.join(root, 'data', 'nfl', 'teams.js'), teamsText);
  fs.writeFileSync(path.join(root, 'data', 'nfl', 'history.js'), historyText);
  const files = { teams: 'teams.js?v=' + stamp(teamsText) };
  if (!settings.withoutHistory) {
    files.history = 'history.js?v=' + stamp(historyText);
  }
  fs.writeFileSync(path.join(root, 'data', 'nfl', 'manifest.json'),
    JSON.stringify({
      schema_version: 2, sport: 'nfl',
      generated_at: new Date(Date.now() - 20 * 60000).toISOString(),
      latest_season: 2026,
      model: { name: 'nfl_elo_v1', fitted_on: [1999, 2020],
        held_out: [2021, 2025], holdout_brier: 0.22372,
        holdout_games: 1424 },
      coverage: [{ season: 2026, teams: 32, rated_teams: 3, games: 3,
        finals: 2, complete: false, played_teams: 3 }],
      seasons: { 2026: 'seasons/2026.js?v=' + stamp(seasonText) },
      files: files
    }));
  for (const name of ['nfl.html', 'nfl_loader.js', 'nfl_app.js',
    'ncaambb_app.js']) {
    fs.copyFileSync(path.join(UI, name), path.join(root, name));
  }
  return root;
}

// A club page asks for history.js on its first render, and that load
// resolves into a render() call. Closing the window before it lands leaves
// the page calling getElementById on a closed document, which node reports
// as asynchronous activity after the test ended. So every club-page test
// waits for the file to arrive before it closes.
function loaded(doc) {
  return settle(() => !/Loading/.test(
    doc.getElementById('teams-body').textContent));
}

function settle(ready, tries = 500) {
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

async function open(options, hash) {
  const root = site(options);
  const file = path.join(root, 'nfl.html');
  const dom = new JSDOM(fs.readFileSync(file, 'utf8'), {
    url: 'file://' + file + (hash || ''),
    runScripts: 'dangerously',
    resources: 'usable',
    pretendToBeVisual: true,
    beforeParse(window) {
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
        const target = path.join(root, String(url));
        return Promise.resolve({
          ok: fs.existsSync(target),
          json: () => Promise.resolve(
            JSON.parse(fs.readFileSync(target, 'utf8')))
        });
      };
    }
  });
  await settle(() => dom.window.document.querySelector(
    '#cards .card, #refusal-list li, .club, .clubhead, #teams-body .empty'));
  return dom;
}

function click(dom, selector) {
  const node = dom.window.document.querySelector(selector);
  assert.ok(node, 'no node for ' + selector);
  node.dispatchEvent(new dom.window.Event('click', { bubbles: true }));
}

async function openTeams(dom) {
  click(dom, '#tabs button[data-tab="teams"]');
  await settle(() => dom.window.document.querySelector('.club, .clubhead'));
  return dom.window.document;
}

test('the Teams tab draws a tile for every club, alphabetically', async () => {
  const dom = await open();
  const doc = await openTeams(dom);
  const names = [...doc.querySelectorAll('.club .nm b')]
    .map(node => node.textContent);
  assert.deepEqual(names,
    ['Buffalo Bills', 'Kansas City Chiefs', 'Los Angeles Rams']);
  // The tile carries the club's division, record and rating, so the
  // directory answers "how are they doing" without a second click.
  const kc = [...doc.querySelectorAll('.club')]
    .find(node => node.dataset.team === 'KC');
  assert.match(kc.textContent, /AFC West/);
  assert.match(kc.textContent, /1-0-1/);
  assert.match(kc.textContent, /1648/);
  dom.window.close();
});

test('pressing a club opens its page and puts it in the URL', async () => {
  const dom = await open();
  const doc = await openTeams(dom);
  click(dom, '.club[data-team="KC"]');
  await settle(() => doc.querySelector('.clubhead'));
  assert.equal(dom.window.location.hash,
    '#league=nfl&section=teams&team=KC');
  assert.match(doc.querySelector('.clubhead h3').textContent,
    /Kansas City Chiefs/);
  assert.match(doc.querySelector('.clubhead .sub').textContent,
    /AFC West · 2026 1-0-1 · Elo 1648 \(#1\)/);
  await loaded(doc);
  dom.window.close();
});

test('a link to a club opens that club, not the directory', async () => {
  const dom = await open(undefined, '#league=nfl&section=teams&team=BUF');
  const doc = dom.window.document;
  await settle(() => doc.querySelector('.clubhead'));
  assert.match(doc.querySelector('.clubhead h3').textContent,
    /Buffalo Bills/);
  assert.equal(doc.querySelectorAll('.club').length, 0,
    'the directory is drawn as well as the club');
  await loaded(doc);
  dom.window.close();
});

test('a club code the export does not carry says so', async () => {
  const dom = await open(undefined, '#league=nfl&section=teams&team=ZZZ');
  const doc = dom.window.document;
  await settle(() => doc.querySelector('#teams-body .empty'));
  assert.match(doc.querySelector('#teams-body .empty').textContent,
    /no club in this export has the code/i);
  dom.window.close();
});

test('the club page lists its season from its own side', async () => {
  const dom = await open(undefined, '#league=nfl&section=teams&team=KC');
  const doc = dom.window.document;
  await settle(() => doc.querySelector('.clubhead'));
  const rows = [...doc.querySelectorAll('#teams-body tbody tr')]
    .map(tr => [...tr.querySelectorAll('td')]
      .map(td => td.textContent.replace(/\s+/g, ' ').trim()));
  const schedule = rows.filter(cells => /^(Wk|Week)/.test(cells[0]));
  assert.equal(schedule.length, 3, 'one row per game, and only KC\'s games');
  assert.equal(schedule.filter(cells => cells[1] === 'Bye').length, 0,
    'KC plays every week of this fixture, so it has no bye');
  assert.match(schedule[0][1], /vs.*Buffalo Bills/);
  assert.equal(schedule[0][2], 'W 24–20');
  assert.equal(schedule[0][3], '1630');
  // The away game states KC's own side of it, tie and all.
  assert.match(schedule[1][1], /@.*Los Angeles Rams/);
  assert.equal(schedule[1][2], 'T 17–17');
  assert.equal(schedule[1][3], '1648');
  await loaded(doc);
  dom.window.close();
});

test('the club page names both coaches of a split season', async () => {
  const dom = await open(undefined, '#league=nfl&section=teams&team=KC');
  const doc = dom.window.document;
  await settle(() => doc.querySelector('.clubhead'));
  const headings = [...doc.querySelectorAll('#teams-body h3')]
    .map(node => node.textContent);
  assert.ok(headings.includes('Head coaches'),
    'two coaches in a season are not announced as two');
  const text = doc.getElementById('teams-body').textContent;
  assert.match(text, /Andy Reid/);
  assert.match(text, /Matt Nagy/);
  // And only this club's coaches: the Bills' head coach has a row in the
  // same payload, and a page that drew every row would show him here.
  assert.doesNotMatch(text, /McDermott/);
  await loaded(doc);
  dom.window.close();
});

test('the season-by-season table comes from the history file', async () => {
  const dom = await open(undefined, '#league=nfl&section=teams&team=KC');
  const doc = dom.window.document;
  await settle(() => /Season by season/.test(
    doc.getElementById('teams-body').textContent));
  await settle(() => !/Loading/.test(
    doc.getElementById('teams-body').textContent));
  const rows = [...doc.querySelectorAll('#teams-body tbody tr')]
    .map(tr => [...tr.querySelectorAll('td')]
      .map(td => td.textContent.replace(/\s+/g, ' ').trim()));
  const seasons = rows.filter(cells => /^\d{4}$/.test(cells[0]));
  assert.deepEqual(seasons.map(cells => cells[0]), ['2026', '2025']);
  assert.equal(seasons[0][1], 'Andy Reid, Matt Nagy');
  assert.equal(seasons[0][2], '5-4-1');
  assert.equal(seasons[1][3], '3–1', 'the postseason record is not shown');
  dom.window.close();
});

test('an export with no history file says so under the table', async () => {
  const dom = await open({ withoutHistory: true },
    '#league=nfl&section=teams&team=KC');
  const doc = dom.window.document;
  await settle(() => /Season by season/.test(
    doc.getElementById('teams-body').textContent));
  await settle(() => doc.querySelector('#teams-body .empty'));
  assert.match(doc.querySelector('#teams-body .empty').textContent,
    /does not carry the history file/);
  dom.window.close();
});

test('a club that did not play still has a page', async () => {
  const dom = await open(undefined, '#league=nfl&section=teams&team=HOU');
  const doc = dom.window.document;
  await settle(() => doc.querySelector('.clubhead'));
  assert.match(doc.querySelector('.clubhead h3').textContent,
    /Houston Texans/);
  assert.match(doc.querySelector('.clubhead .sub').textContent,
    /did not play in 2026/);
  assert.match(doc.getElementById('teams-body').textContent,
    /played no game in 2026/);
  await loaded(doc);
  dom.window.close();
});

test('going back to the directory takes the club out of the URL', async () => {
  const dom = await open(undefined, '#league=nfl&section=teams&team=KC');
  const doc = dom.window.document;
  await settle(() => doc.querySelector('.clubhead'));
  await loaded(doc);
  click(dom, '#teams-body button.expand');          // ← All clubs
  await settle(() => doc.querySelector('.club'));
  assert.equal(dom.window.location.hash, '#league=nfl&section=teams');
  assert.equal(doc.querySelectorAll('.clubhead').length, 0);
  dom.window.close();
});

test('leaving the Teams tab takes the club out of the URL', async () => {
  const dom = await open(undefined, '#league=nfl&section=teams&team=KC');
  const doc = dom.window.document;
  await settle(() => doc.querySelector('.clubhead'));
  await loaded(doc);
  click(dom, '#tabs button[data-tab="rankings"]');
  assert.equal(dom.window.location.hash, '#league=nfl&section=rankings',
    'the club rode along into a section it has no meaning in');
  dom.window.close();
});

test('the back button returns from a club to the directory', async () => {
  const dom = await open();
  const doc = await openTeams(dom);
  click(dom, '.club[data-team="BUF"]');
  await settle(() => doc.querySelector('.clubhead'));
  await loaded(doc);
  dom.window.history.back();
  await settle(() => doc.querySelector('.club'));
  assert.equal(doc.querySelectorAll('.clubhead').length, 0);
  assert.equal(dom.window.location.hash, '#league=nfl&section=teams');
  dom.window.close();
});
