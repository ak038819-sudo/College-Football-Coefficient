// The NFL game layer in the browser: the pure helpers that unpack the packed
// week files, and the page that draws one game from them, loaded in jsdom
// against a site with a real manifest naming a game layer.
'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { JSDOM } = require('jsdom');

const UI = path.join(__dirname, '..', 'ui');
const app = require(path.join(UI, 'nfl_app.js'));
const loader = require(path.join(UI, 'nfl_loader.js'));

const KC = 'nfl:nflverse:franchise:KC';
const BUF = 'nfl:nflverse:franchise:BUF';
const KINDS = ['TD', 'FG', 'XP', '2PT', 'SAFETY', 'D2PT'];
const RESULTS = ['Punt', 'Touchdown'];

// One drive, packed as src/nfl/game_export.py writes it. The Python test
// test_a_drive_is_packed_in_the_order_the_page_unpacks_it pins the other end.
function drive(over) {
  const row = [1, 1, 1, 3600, 'KC 25', 5, 20, '3:00', 0, 0, 0, 540];
  (over || []).forEach((pair) => { row[pair[0]] = pair[1]; });
  return row;
}

function score(over) {
  const row = [1, 1, '08:18', 0, 6, 'P.Mahomes', null, 6, 0, 'TOUCHDOWN.'];
  (over || []).forEach((pair) => { row[pair[0]] = pair[1]; });
  return row;
}

// --- unpacking -------------------------------------------------------------

test('a packed drive row is read back into the fields the page draws', () => {
  const got = app.unpackDrives({ d: [drive()] }, RESULTS)[0];
  assert.equal(got.drive, 1);
  assert.equal(got.side, 1);
  assert.equal(got.quarter, 1);
  assert.equal(got.seconds, 3600);
  assert.equal(got.start, 'KC 25');
  assert.equal(got.plays, 5);
  assert.equal(got.yards, 20);
  assert.equal(got.top, '3:00');
  assert.equal(got.result, 'Punt');
  assert.equal(got.homeScore, 0);
  assert.equal(got.awayScore, 0);
  assert.equal(got.wp, 540);
});

test('an interned result with no word behind it is left empty, not printed '
  + 'as a number', () => {
    const got = app.unpackDrives({ d: [drive([[8, 9]])] }, RESULTS)[0];
    assert.equal(got.result, null);
  });

test('a packed scoring row carries its kind and a readable label', () => {
  const got = app.unpackScoring({ s: [score()] }, KINDS)[0];
  assert.equal(got.side, 1);
  assert.equal(got.quarter, 1);
  assert.equal(got.clock, '08:18');
  assert.equal(got.kind, 'TD');
  assert.equal(got.label, 'Touchdown');
  assert.equal(got.points, 6);
  assert.equal(got.scorer, 'P.Mahomes');
  assert.equal(got.description, 'TOUCHDOWN.');
});

test('a safety is labelled as one rather than as a generic score', () => {
  const got = app.unpackScoring({ s: [score([[3, 4], [4, 2]])] }, KINDS)[0];
  assert.equal(got.kind, 'SAFETY');
  assert.equal(got.label, 'Safety');
});

// --- the probability a page prints -----------------------------------------

test('a chance the model did not call impossible is never printed as none',
  () => {
    // A model that has given a team one in a thousand has not given it none,
    // and 0% is a claim it did not make.
    assert.equal(app.probabilityText(1), '<1%');
    assert.equal(app.probabilityText(999), '>99%');
    // The endpoints are included: the exporter rounds to per-mille, so a
    // real 0.0004 arrives here as exactly 0, and an excluded endpoint would
    // print it as certainty.
    assert.equal(app.probabilityText(0), '<1%');
    assert.equal(app.probabilityText(1000), '>99%');
    assert.equal(app.probabilityText(540), '54%');
  });

test('a drive the export could not price prints nothing', () => {
  assert.equal(app.probabilityText(null), null);
  assert.equal(app.probabilityText(undefined), null);
});

// --- the trace -------------------------------------------------------------

const FINAL = { id: 'g1', home: KC, away: BUF, home_points: 27,
                away_points: 20, week: 1 };

test('the trace runs from kickoff through every priced drive to the result',
  () => {
    const entry = { wp: 580, d: [drive(), drive([[0, 2], [3, 1800],
      [11, 700]])] };
    const points = app.winProbabilityTrace(entry, FINAL);
    assert.equal(points.length, 4);
    assert.equal(points[0].at, 0);
    assert.equal(points[0].value, 0.58);
    assert.equal(points[2].at, 0.5);
    assert.equal(points[2].value, 0.7);
    // The line ends where the game did, not where the last drive started.
    assert.equal(points[3].at, 1);
    assert.equal(points[3].value, 1);
  });

test('a home loss ends the trace at zero and a tie at a half', () => {
  const lost = Object.assign({}, FINAL, { home_points: 20, away_points: 27 });
  const tied = Object.assign({}, FINAL, { home_points: 20, away_points: 20 });
  assert.equal(app.winProbabilityTrace({ wp: 500 }, lost).pop().value, 0);
  assert.equal(app.winProbabilityTrace({ wp: 500 }, tied).pop().value, 0.5);
});

test('an unpriced drive is dropped rather than drawn at an even chance',
  () => {
    const entry = { wp: 500, d: [drive([[11, null]]), drive([[0, 2]])] };
    const points = app.winProbabilityTrace(entry, null);
    assert.equal(points.length, 2);
    assert.equal(points[1].label, 'Drive 2');
  });

test('an unfinished game has no closing point', () => {
  const scheduled = { id: 'g2', home: KC, away: BUF, home_points: null,
                      away_points: null };
  const points = app.winProbabilityTrace({ wp: 500, d: [drive()] },
                                         scheduled);
  assert.equal(points[points.length - 1].label, 'Drive 1');
});

test('the path is straight segments through the points, in a box the caller '
  + 'scales', () => {
    const points = [{ at: 0, value: 1 }, { at: 0.5, value: 0.5 },
                    { at: 1, value: 0 }];
    assert.equal(app.tracePath(points, 100, 10), 'M0.00 0.00 L50.00 5.00 '
      + 'L100.00 10.00');
    // One point is not a line.
    assert.equal(app.tracePath([points[0]], 100, 10), '');
  });

// --- which file a game's detail is in --------------------------------------

test('a postseason game is in the post file whatever week it is numbered',
  () => {
    assert.equal(loader.gameWeekKey({ week: 19, postseason: 1 }), 'post');
    assert.equal(loader.gameWeekKey({ week: 5 }), '5');
    assert.equal(loader.gameWeekKey({ week: null }), 'post');
  });

test('a week the manifest does not list has no source, so no fetch is made',
  () => {
    const manifest = { files: { games: { seasons: {
      2024: { v: 'abc1234567', weeks: ['1', 'post'] } } } } };
    assert.equal(loader.gameWeekSource(manifest, 2024, '1', 'data/nfl/'),
      'data/nfl/games/2024/1.js?v=abc1234567');
    assert.equal(loader.gameWeekSource(manifest, 2024, '7', 'data/nfl/'),
      null);
    assert.equal(loader.gameWeekSource(manifest, 2023, '1', 'data/nfl/'),
      null);
    assert.equal(loader.gameWeekSource({ files: {} }, 2024, '1'), null);
  });

test('a season with no cache token has no source, because a url with no '
  + 'version stamp can be served from a cache of a previous export', () => {
    const manifest = { files: { games: { seasons: {
      2024: { weeks: ['1'] } } } } };
    assert.equal(loader.gameWeekSource(manifest, 2024, '1'), null);
  });

// --- the page --------------------------------------------------------------

function stamp(text) {
  return crypto.createHash('sha256').update(text).digest('hex').slice(0, 10);
}

function teams() {
  return {
    [KC]: { code: 'KC', name: 'Kansas City Chiefs', conference: 'AFC',
            division: 'West' },
    [BUF]: { code: 'BUF', name: 'Buffalo Bills', conference: 'AFC',
             division: 'East' }
  };
}

function board() {
  return [
    { team: KC, code: 'KC', conference: 'AFC', division: 'West',
      rating: 1648.4, rank: 1, wins: 1, losses: 0, ties: 0, rated_games: 1 },
    { team: BUF, code: 'BUF', conference: 'AFC', division: 'East',
      rating: 1591.6, rank: 2, wins: 0, losses: 1, ties: 0, rated_games: 1 }
  ];
}

function schedule() {
  return [{ id: 'g1', date: '2026-09-13', at: '2026-09-13T17:00:00Z',
            week: 1, type: 'regular', home: KC, away: BUF, home_points: 27,
            away_points: 20, rateable: 1, venue: 'Arrowhead Stadium' }];
}

function weekPayload(over) {
  return Object.assign({
    season: 2026, week: '1', kinds: KINDS, results: RESULTS,
    games: { g1: { wp: 580, ok: 1,
      d: [drive(), drive([[0, 2], [1, 0], [3, 1800], [8, 1], [11, 700]])],
      s: [score(), score([[0, 0], [3, 1], [4, 3], [9, 'FIELD GOAL.']])] } }
  }, over || {});
}

function site(withGames, payloadOver) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'nfl-game-'));
  const base = path.join(root, 'data', 'nfl');
  fs.mkdirSync(path.join(base, 'seasons'), { recursive: true });
  const seasonText = 'window.NFL_SEASONS=(window.NFL_SEASONS||{});'
    + 'window.NFL_SEASONS[2026]='
    + JSON.stringify({ season: 2026, board: board(), games: schedule() })
    + ';\n';
  const teamsText = 'window.NFL_TEAMS=' + JSON.stringify(teams()) + ';\n';
  fs.writeFileSync(path.join(base, 'seasons', '2026.js'), seasonText);
  fs.writeFileSync(path.join(base, 'teams.js'), teamsText);
  const payload = {
    schema_version: withGames ? 4 : 3,
    sport: 'nfl',
    generated_at: new Date(Date.now() - 20 * 60000).toISOString(),
    latest_season: 2026,
    model: { name: 'nfl_elo_v1', fitted_on: [1999, 2020],
      held_out: [2021, 2025], holdout_brier: 0.22372, holdout_games: 1424 },
    coverage: [{ season: 2026, teams: 32, rated_teams: 2, games: 1,
      finals: 1, complete: false, played_teams: 2 }],
    seasons: { 2026: 'seasons/2026.js?v=' + stamp(seasonText) },
    files: { teams: 'teams.js?v=' + stamp(teamsText) }
  };
  if (withGames) {
    const text = 'window.NFL_GAMEWEEKS=(window.NFL_GAMEWEEKS||{});'
      + 'window.NFL_GAMEWEEKS["2026/1"]='
      + JSON.stringify(weekPayload(payloadOver)) + ';\n';
    fs.mkdirSync(path.join(base, 'games', '2026'), { recursive: true });
    fs.writeFileSync(path.join(base, 'games', '2026', '1.js'), text);
    payload.files.games = { seasons: { 2026: { v: stamp(text),
      weeks: ['1'] } } };
  }
  fs.writeFileSync(path.join(base, 'manifest.json'), JSON.stringify(payload));
  for (const name of ['nfl.html', 'nfl_loader.js', 'nfl_app.js',
    'ncaambb_app.js']) {
    fs.copyFileSync(path.join(UI, name), path.join(root, name));
  }
  return root;
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

async function open(withGames, payloadOver, hash) {
  const root = site(withGames, payloadOver);
  const file = path.join(root, 'nfl.html');
  const dom = new JSDOM(fs.readFileSync(file, 'utf8'), {
    url: 'file://' + file + (hash || ''),
    runScripts: 'dangerously',
    resources: 'usable',
    pretendToBeVisual: true,
    beforeParse(window) {
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
  // A hash can open the page somewhere other than the scoreboard, so the
  // settle has to know every screen a link can land on.
  await settle(() => dom.window.document.querySelector(
    '#cards .card, #refusal-list li, #game table, .clubhead'));
  return dom;
}

function click(dom, selector) {
  const node = dom.window.document.querySelector(selector);
  assert.ok(node, 'no node for ' + selector);
  node.dispatchEvent(new dom.window.Event('click', { bubbles: true }));
}

/* Open a tab, and a view inside it where the fold put one.
 *
 * The page shows the five tabs every league shows, so what used to be the
 * Rankings, Coaches, History and Games tabs are views of Standings, People
 * and Stats, or Home. Driving the real controls -- the tab, then the strip
 * under it -- is the point: a test that reached straight for a panel would
 * pass with the nav broken.
 */
const FOLD = {
  games: ['home', null],
  rankings: ['standings', 'elo'],
  standings: ['standings', null],
  coaches: ['people', 'coaches'],
  history: ['stats', null],
  people: ['people', null],
  teams: ['teams', null],
  home: ['home', null],
  stats: ['stats', null]
};

function go(dom, name) {
  const [tab, view] = FOLD[name] || [name, null];
  click(dom, `#tabs button[data-tab="${tab}"]`);
  if (view) click(dom, `#subnav button[data-view="${view}"]`);
}

test('a card opens a game page with its scoring summary and its drives',
  async () => {
    const dom = await open(true);
    go(dom, 'games');
    click(dom, '#cards .card.open');
    await settle(() => dom.window.document.querySelector('#game table'));
    const panel = dom.window.document.getElementById('game');
    assert.match(panel.textContent, /Buffalo Bills at Kansas City Chiefs/);
    assert.match(panel.textContent, /TOUCHDOWN\./);
    assert.match(panel.textContent, /FIELD GOAL\./);
    assert.match(panel.textContent, /Drives/);
    // The week strip and the card grid give way to the game. The cards are
    // hidden by their container rather than in their own right, since the
    // Elo rail sits beside them and has to go too, so this asks whether
    // they are visible rather than which element carries the attribute.
    assert.ok(dom.window.document.getElementById('cards').closest('[hidden]'),
      'the card grid is not visible');
    assert.equal(dom.window.document.getElementById('weeks').hidden, true);
    dom.window.close();
  });

test('the drive table names the home team over the win-probability column, '
  + 'because every row of it is the home team’s', async () => {
    const dom = await open(true);
    go(dom, 'games');
    click(dom, '#cards .card.open');
    await settle(() => dom.window.document.querySelector('#game table'));
    const tables = dom.window.document.querySelectorAll('#game table');
    const header = tables[tables.length - 1].querySelector('tr').textContent;
    assert.match(header, /KC win/);
    assert.doesNotMatch(header, /BUF win/);
    dom.window.close();
  });

test('the win-probability line is drawn with a scale to read it by',
  async () => {
    const dom = await open(true);
    go(dom, 'games');
    click(dom, '#cards .card.open');
    await settle(() => dom.window.document.querySelector('#game svg'));
    const svg = dom.window.document.querySelector('#game svg');
    assert.ok(svg.querySelector('path.line').getAttribute('d').length > 10);
    const ticks = Array.from(svg.querySelectorAll('text'))
      .map((node) => node.textContent);
    assert.deepEqual(ticks, ['KC', '50%', 'BUF']);
    dom.window.close();
  });

test('a game whose scoring does not add up says so above the summary',
  async () => {
    const payload = weekPayload();
    payload.games.g1.ok = 0;
    const dom = await open(true, payload);
    go(dom, 'games');
    click(dom, '#cards .card.open');
    await settle(() => dom.window.document.querySelector('#game table'));
    assert.match(dom.window.document.getElementById('game').textContent,
      /does not add up to the final score/);
    dom.window.close();
  });

test('a game with an unpriced drive says why it carries no number',
  async () => {
    const payload = weekPayload();
    // An overtime drive: no probability at all, because the model's clock
    // runs to the end of regulation.
    payload.games.g1.d.push(drive([[0, 3], [2, 5], [3, 0], [11, null]]));
    const dom = await open(true, payload);
    go(dom, 'games');
    click(dom, '#cards .card.open');
    await settle(() => dom.window.document.querySelector('#game table'));
    assert.match(dom.window.document.querySelector('#game .wplegend').textContent,
      /Overtime drives carry no number/);
    dom.window.close();
  });

test('a game with every drive priced says nothing about overtime',
  async () => {
    const dom = await open(true);
    go(dom, 'games');
    click(dom, '#cards .card.open');
    await settle(() => dom.window.document.querySelector('#game table'));
    assert.doesNotMatch(
      dom.window.document.querySelector('#game .wplegend').textContent,
      /Overtime/);
    dom.window.close();
  });

test('an export built before the game layer offers no tap that opens onto '
  + 'nothing', async () => {
    const dom = await open(false);
    go(dom, 'games');
    assert.equal(
      dom.window.document.querySelector('#cards .card.open'), null);
    assert.ok(dom.window.document.querySelector('#cards .card'));
    dom.window.close();
  });

test('going back to the week restores the cards', async () => {
  const dom = await open(true);
  go(dom, 'games');
  click(dom, '#cards .card.open');
  await settle(() => dom.window.document.querySelector('#game table'));
  click(dom, '#game button.expand');
  assert.equal(dom.window.document.getElementById('cards').hidden, false);
  assert.equal(dom.window.document.getElementById('game').hidden, true);
  dom.window.close();
});

// --- the week's injury report ----------------------------------------------

const STATUSES = ['Out', 'Doubtful', 'Questionable', 'Probable'];

test('injury rows are read back and ordered by how likely he was to miss it',
  () => {
    const rows = app.unpackInjuries({ i: [
      [1, 2, 'T.Kelce', 'TE', 'Knee'],
      [0, 0, 'J.Allen', 'QB', 'Elbow'],
      [1, 1, 'I.Pacheco', 'RB', 'Ankle']
    ] }, STATUSES);
    assert.deepEqual(Array.from(rows.map((row) => row.name)),
      ['J.Allen', 'I.Pacheco', 'T.Kelce']);
    assert.equal(rows[0].status, 'Out');
    assert.equal(rows[0].side, 0);
    assert.equal(rows[0].position, 'QB');
    assert.equal(rows[0].injury, 'Elbow');
  });

test('a status index with no word behind it sorts last rather than first',
  () => {
    // Number(null) is 0, so a missing status read as a number would outrank
    // every player the club actually ruled out.
    const rows = app.unpackInjuries({ i: [
      [1, null, 'Nobody', 'TE', 'Knee'],
      [1, 0, 'T.Kelce', 'TE', 'Knee']
    ] }, STATUSES);
    assert.deepEqual(Array.from(rows.map((row) => row.name)),
      ['T.Kelce', 'Nobody']);
    assert.equal(rows[1].status, null);
  });

test('a game with no injury report unpacks to nothing', () => {
  assert.deepEqual(Array.from(app.unpackInjuries({}, STATUSES)), []);
});

test('a share prints as a whole percentage, and a measured zero prints', () => {
  assert.equal(app.shareText(0.8394), '84%');
  assert.equal(app.shareText(0), '0%');
  assert.equal(app.shareText(null), '');
  assert.equal(app.shareText(undefined), '');
});

test('the game page lists the week’s designated players and says who is '
  + 'missing from the list', async () => {
    const payload = weekPayload();
    payload.statuses = STATUSES;
    payload.games.g1.i = [[1, 0, 'T.Kelce', 'TE', 'Knee'],
                          [0, 2, 'J.Allen', 'QB', 'Elbow']];
    const dom = await open(true, payload);
    go(dom, 'games');
    click(dom, '#cards .card.open');
    await settle(() => dom.window.document.querySelector('#game table'));
    const panel = dom.window.document.getElementById('game');
    assert.match(panel.textContent, /Injury report/);
    assert.match(panel.textContent, /T\.Kelce/);
    assert.match(panel.textContent, /Elbow/);
    // The note is the point: the report itself changed in 2016, and a
    // shorter list is not a healthier squad.
    assert.match(panel.textContent, /stopped using “Probable”/);
    dom.window.close();
  });

test('a game with no injury report draws no empty table', async () => {
  const dom = await open(true);
  go(dom, 'games');
  click(dom, '#cards .card.open');
  await settle(() => dom.window.document.querySelector('#game table'));
  const panel = dom.window.document.getElementById('game');
  assert.doesNotMatch(panel.textContent, /Injury report/);
  dom.window.close();
});

test('opening a game hides the whole slate, rail and filters included',
  async () => {
    // The Elo rail sits BESIDE the cards inside `.slate`, and `#game`
    // follows it in the markup. Hiding only the cards would leave the
    // rankings list stranded above the game detail -- and the card handler
    // scrolls to the top, which on a phone puts the game's own heading
    // below the fold.
    const dom = await open(true);
    go(dom, 'games');
    const doc = dom.window.document;
    assert.equal(doc.getElementById('slate').hidden, false,
      'the slate is there before a game is opened');
    click(dom, '#cards .card.open');
    await settle(() => doc.querySelector('#game table'));
    assert.equal(doc.getElementById('slate').hidden, true);
    assert.equal(doc.getElementById('slate-filters').hidden, true);
    assert.equal(doc.getElementById('weeks').hidden, true);
    assert.equal(doc.getElementById('game').hidden, false);
    // and the rail is genuinely not painted, not merely display-none'd by a
    // rule a later stylesheet could beat
    assert.equal(doc.querySelector('#rail').closest('[hidden]').id, 'slate');
  });

// --- a club's name is a way to its page ------------------------------------
//
// The card is the most-seen instance of a club on this page, and it used to
// be a <button> -- which is why the names on it could not be links. It is a
// div with role="button" now, so these pin BOTH halves: the card still
// opens its game by click and by keyboard, and the names go to the club.

test('a card is reachable by keyboard and opens its game with Enter',
  async () => {
    const dom = await open(true);
    const doc = dom.window.document;
    const card = doc.querySelector('#cards .card.open');
    assert.equal(card.tagName, 'DIV',
      'a button cannot hold the club links inside it');
    assert.equal(card.getAttribute('role'), 'button');
    assert.equal(card.tabIndex, 0, 'the card cannot be reached by keyboard');
    card.dispatchEvent(new dom.window.KeyboardEvent('keydown',
      { key: 'Enter', bubbles: true }));
    await settle(() => doc.querySelector('#game table'));
    assert.equal(doc.getElementById('game').hidden, false);
    dom.window.close();
  });

test('a card names both clubs as links', async () => {
  const dom = await open(true);
  const doc = dom.window.document;
  const names = [...doc.querySelectorAll('#cards .card.open .nm b button')]
    .map(node => node.textContent);
  assert.deepEqual(names, ['Buffalo Bills', 'Kansas City Chiefs']);
  dom.window.close();
});

test('pressing a club on a card opens the club, not the game', async () => {
  const dom = await open(true);
  const doc = dom.window.document;
  const name = [...doc.querySelectorAll('#cards .card.open .nm b button')]
    .find(node => node.textContent === 'Kansas City Chiefs');
  name.dispatchEvent(new dom.window.Event('click', { bubbles: true }));
  await settle(() => doc.querySelector('.clubhead'));
  assert.equal(dom.window.location.hash, '#league=nfl&section=teams&team=KC');
  assert.equal(doc.querySelectorAll('#game .gamehead').length, 0,
    'the card opened its game as well as the club');
  dom.window.close();
});

test('pressing Enter on a club does not open the game under it', async () => {
  const dom = await open(true);
  const doc = dom.window.document;
  const name = [...doc.querySelectorAll('#cards .card.open .nm b button')]
    .find(node => node.textContent === 'Kansas City Chiefs');
  const event = new dom.window.KeyboardEvent('keydown',
    { key: 'Enter', bubbles: true, cancelable: true });
  name.dispatchEvent(event);
  // The card listens on itself and this keydown bubbles through it. If the
  // card answered, it would both swallow the button's own activation and
  // open the game, so the reader would land on neither club.
  assert.equal(event.defaultPrevented, false,
    'the card swallowed the club button\u2019s own activation');
  assert.equal(doc.getElementById('game').hidden, true,
    'a key meant for the club opened the game');
  dom.window.close();
});

test('the game heading links both clubs', async () => {
  const dom = await open(true);
  const doc = dom.window.document;
  click(dom, '#cards .card.open');
  await settle(() => doc.querySelector('#game table'));
  const names = [...doc.querySelectorAll('#game .gamehead button')]
    .map(node => node.textContent);
  assert.deepEqual(names, ['Buffalo Bills', 'Kansas City Chiefs'],
    'the heading names both clubs and links neither');
  dom.window.close();
});

/* ---- a game is a place, with an address ------------------------------- */

test('opening a game puts the game in the URL', async () => {
  const dom = await open(true);
  click(dom, '#cards .card.open');
  await settle(() => dom.window.document.querySelector('#game table'));
  // The short id, not the export's key: the address is for a reader.
  assert.equal(dom.window.location.hash, '#league=nfl&game=g1');
  dom.window.close();
});

test('a link to a game opens the game', async () => {
  const dom = await open(true, undefined, '#league=nfl&game=g1');
  const doc = dom.window.document;
  await settle(() => doc.querySelector('#game table'));
  assert.match(doc.getElementById('game').textContent,
    /Buffalo Bills at Kansas City Chiefs/);
  assert.equal(doc.getElementById('weeks').hidden, true,
    'the week strip is still over the game');
  dom.window.close();
});

test('a link naming a game this season does not hold opens the week',
  async () => {
    const dom = await open(true, undefined, '#league=nfl&game=no-such-game');
    const doc = dom.window.document;
    await settle(() => doc.querySelector('#cards .card'));
    assert.equal(doc.getElementById('game').hidden, true,
      'a game that is not there was drawn anyway');
    dom.window.close();
  });

test('going back out of a game takes the game out of the URL', async () => {
  const dom = await open(true);
  const doc = dom.window.document;
  click(dom, '#cards .card.open');
  await settle(() => doc.querySelector('#game table'));
  const back = [...doc.querySelectorAll('#game button')]
    .find(node => /Back to the week/.test(node.textContent));
  assert.ok(back, 'no way back out of the game');
  back.dispatchEvent(new dom.window.Event('click', { bubbles: true }));
  // The cards are rendered under the game rather than thrown away, so the
  // thing to wait for is the slate being visible again.
  await settle(() => !doc.getElementById('slate').hidden);
  assert.equal(dom.window.location.hash, '#league=nfl');
  dom.window.close();
});

test('pressing a week takes the game out of the URL too', async () => {
  const dom = await open(true);
  const doc = dom.window.document;
  click(dom, '#cards .card.open');
  await settle(() => doc.querySelector('#game table'));
  const week = [...doc.querySelectorAll('#weeks button')]
    .find(node => node.textContent === 'Wk 1');
  week.dispatchEvent(new dom.window.Event('click', { bubbles: true }));
  await settle(() => !doc.getElementById('slate').hidden);
  assert.equal(dom.window.location.hash, '#league=nfl');
  dom.window.close();
});

test('a result on a club\u2019s schedule opens that game', async () => {
  // A reader looking at a club's season should not have to find the week on
  // the scoreboard to read the game.
  const dom = await open(true, undefined, '#league=nfl&section=teams&team=KC');
  const doc = dom.window.document;
  await settle(() => doc.querySelector('.clubhead'));
  const result = [...doc.querySelectorAll('#teams tbody button')]
    .find(node => /^W 27\u201320$/.test(node.textContent));
  assert.ok(result, 'the schedule draws no way into the game');
  result.dispatchEvent(new dom.window.Event('click', { bubbles: true }));
  await settle(() => doc.querySelector('#game table'));
  assert.equal(dom.window.location.hash, '#league=nfl&game=g1');
  assert.match(doc.getElementById('game').textContent,
    /Buffalo Bills at Kansas City Chiefs/);
  assert.equal(doc.getElementById('teams').hidden, true,
    'the club page is still open under the game');
  dom.window.close();
});

test('a schedule result with no game page behind it is plain text',
  async () => {
    // withGames false: the export has no week files, so there is nothing to
    // open and the row must not offer a tap onto nothing.
    const dom = await open(false, undefined,
      '#league=nfl&section=teams&team=KC');
    const doc = dom.window.document;
    await settle(() => doc.querySelector('.clubhead'));
    const offered = [...doc.querySelectorAll('#teams tbody button')]
      .some(node => /^W 27\u201320$/.test(node.textContent));
    assert.equal(offered, false, 'the row offered a game page that is absent');
    assert.match(doc.getElementById('teams').textContent, /W 27\u201320/);
    dom.window.close();
  });
