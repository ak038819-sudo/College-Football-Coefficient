// Every club of a season against the closing line.
//
// The arithmetic is tested as functions and the view is tested by loading
// ui/nfl.html in jsdom, because a green build is not a rendered page.
//
// The sharpest test here is the BALANCE one: across a whole season the
// league's ATS wins must equal its ATS losses exactly, because one side of
// every priced game covers and the other does not. An implementation that
// forgets to negate the line for the visiting club gives BOTH sides the
// same letter and the two totals come apart at once.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const crypto = require('node:crypto');
const { JSDOM } = require('jsdom');

const UI = path.join(__dirname, '..', 'ui');
const app = require(path.join(UI, 'nfl_app.js'));

const KC = 'nfl:nflverse:franchise:KC';
const BUF = 'nfl:nflverse:franchise:BUF';
const LA = 'nfl:nflverse:franchise:LA';
// On the board but in no priced game, so "a club with nothing to measure"
// has something to be.
const HOU = 'nfl:nflverse:franchise:HOU';
// These two exist for ONE assertion: a club whose ATS record is over half
// and still short of the vig. 11-10 is 52.38%, and the break-even is 52.4%.
// Nothing smaller will do -- (l+1)/(2l+1) only drops below 0.524 at l = 10 --
// so the pair plays 21 games against each other and no one else.
const SEA = 'nfl:nflverse:franchise:SEA';
const DEN = 'nfl:nflverse:franchise:DEN';

function teams() {
  return {
    [KC]: { code: 'KC', name: 'Kansas City Chiefs', conference: 'AFC',
      division: 'West' },
    [BUF]: { code: 'BUF', name: 'Buffalo Bills', conference: 'AFC',
      division: 'East' },
    [LA]: { code: 'LA', name: 'Los Angeles Rams', conference: 'NFC',
      division: 'West' },
    [HOU]: { code: 'HOU', name: 'Houston Texans', conference: 'AFC',
      division: 'South' },
    [SEA]: { code: 'SEA', name: 'Seattle Seahawks', conference: 'NFC',
      division: 'West' },
    [DEN]: { code: 'DEN', name: 'Denver Broncos', conference: 'AFC',
      division: 'West' }
  };
}

/* Twenty-one pick'em games, eleven of them won by a point. Seattle finishes
 * 11-10 against the spread, which is 52.38%: over half, and still a losing
 * season for anyone who backed them every week. */
function vigGames() {
  const out = [];
  for (let i = 0; i < 21; i += 1) {
    const seattleWins = i < 11;
    const day = '2026-11-' + String((i % 28) + 1).padStart(2, '0');
    out.push({
      id: 'v' + i, date: day, at: day + 'T17:00:00Z', week: 6 + i,
      type: 'regular', home: SEA, away: DEN,
      home_points: seattleWins ? 21 : 20,
      away_points: seattleWins ? 20 : 21,
      spread: 0, total_line: 40, rateable: 1,
      home_elo: 1500, away_elo: 1500, home_wp: 0.5
    });
  }
  return out;
}

// One row of a schedule, as app.teamSchedule() hands it over.
function row(spread, total, points, against, home) {
  return {
    game: { id: 'g', spread: spread, total_line: total },
    home: home === undefined ? true : home,
    points: points,
    against: against
  };
}

// --- against the spread ----------------------------------------------------

test('the home side covers when its margin beats the line', () => {
  // Home favoured by 3 and winning by 7 covers; by 1 does not.
  assert.equal(app.atsResult(row(3, 44, 27, 20)), 'W');
  assert.equal(app.atsResult(row(3, 44, 21, 20)), 'L');
});

test('a margin equal to the line is a push, not a loss', () => {
  // Home favoured by 3, home wins by exactly 3. The stake comes back.
  assert.equal(app.atsResult(row(3, 44, 23, 20)), 'P');
  // And on a line of zero, a drawn game.
  assert.equal(app.atsResult(row(0, 44, 20, 20)), 'P');
});

test('the visiting side plays the line NEGATED', () => {
  // The SAME game from each side: home favoured by 3, away loses by 1.
  // The away club covered; the home club did not.
  assert.equal(app.atsResult(row(3, 44, 20, 21, true)), 'L');
  assert.equal(app.atsResult(row(3, 44, 20, 21, false)), 'W');
});

test('an underdog that wins outright always covers', () => {
  // Home a 7-point dog (spread -7) and wins by 1.
  assert.equal(app.atsResult(row(-7, 44, 21, 20, true)), 'W');
});

test('a game with no line, or none played, is in no record', () => {
  assert.equal(app.atsResult(row(null, 44, 27, 20)), '');
  assert.equal(app.atsResult(row(3, 44, null, null)), '');
  assert.equal(app.atsResult(row(3, 44, 27, null)), '');
  assert.equal(app.atsResult(null), '');
});

// --- the total -------------------------------------------------------------

test('the total is over, under, or a push', () => {
  assert.equal(app.totalResult(row(3, 44, 27, 20)), 'O');   // 47
  assert.equal(app.totalResult(row(3, 44, 24, 17)), 'U');   // 41
  assert.equal(app.totalResult(row(3, 44, 24, 20)), 'P');   // 44
  assert.equal(app.totalResult(row(3, null, 27, 20)), '');
  assert.equal(app.totalResult(row(3, 44, null, null)), '');
});

test('both clubs in one game get the SAME letter for the total', () => {
  // Unlike the spread, the total is a property of the game.
  assert.equal(app.totalResult(row(3, 44, 27, 20, true)), 'O');
  assert.equal(app.totalResult(row(3, 44, 20, 27, false)), 'O');
});

// --- the tally -------------------------------------------------------------

test('a tally counts each half and its own denominator', () => {
  const rows = [
    row(3, 44, 27, 20),      // W, O (47)
    row(3, 44, 21, 20),      // L, U (41)
    row(3, 44, 23, 20),      // P, U (43)
    row(null, 44, 30, 20),   // no line: not in ATS, but is in the total (O)
    row(3, null, 27, 10),    // W, no total
    row(3, 44, null, null)   // unplayed: in neither
  ];
  const tally = app.marketRecord(rows);
  assert.equal(tally.ats_wins, 2);
  assert.equal(tally.ats_losses, 1);
  assert.equal(tally.ats_pushes, 1);
  assert.equal(tally.priced, 4);
  assert.equal(tally.overs, 2);
  assert.equal(tally.unders, 2);
  assert.equal(tally.total_pushes, 0);
  assert.equal(tally.totalled, 4);
});

test('the existing atsText and atsRate read a tally as they read a track '
  + 'record row, because it is deliberately the same shape', () => {
  const tally = app.marketRecord([
    row(3, 44, 27, 20), row(3, 44, 21, 20), row(3, 44, 23, 20)
  ]);
  assert.equal(app.atsText(tally), '1-1-1');
  // The push is out of BOTH halves: one win from two decided bets.
  assert.equal(app.atsRate(tally), 0.5);
  assert.equal(app.atsBeatsVig(tally), false);
});

test('a club with no priced game has no rate at all, rather than zero', () => {
  const tally = app.marketRecord([row(null, null, 27, 20)]);
  assert.equal(tally.priced, 0);
  assert.equal(app.atsRate(tally), null);
  assert.equal(app.atsBeatsVig(tally), null);
});

// --- the balance invariant -------------------------------------------------

test('over a whole season the league covers exactly as often as it does not',
  () => {
    // One side of every priced game covers and the other does not, so the
    // two totals are equal by construction -- whatever the lines and
    // whatever the scores. Only the per-side negation makes that true.
    const all = games();
    let wins = 0;
    let losses = 0;
    let pushes = 0;
    for (const key of [KC, BUF, LA, HOU, SEA, DEN]) {
      const tally = app.marketRecord(app.teamSchedule(all, key));
      wins += tally.ats_wins;
      losses += tally.ats_losses;
      pushes += tally.ats_pushes;
    }
    // The fixture has to have something in each column, or this passes on
    // nothing at all.
    assert.ok(wins > 0 && pushes > 0, `fixture is empty: ${wins}/${pushes}`);
    assert.equal(wins, losses);
    // A push is a push for both clubs, so that total is even too.
    assert.equal(pushes % 2, 0);
  });

// --- the page --------------------------------------------------------------

function games() {
  return [
    // KC covers twice and pushes once; BUF is the other side each time.
    { id: 'g1', date: '2026-09-13', at: '2026-09-13T17:00:00Z', week: 1,
      type: 'regular', home: KC, away: BUF, home_points: 30, away_points: 20,
      spread: 3, total_line: 44, rateable: 1, home_elo: 1630, away_elo: 1600,
      home_wp: 0.6 },
    { id: 'g2', date: '2026-09-20', at: '2026-09-20T17:00:00Z', week: 2,
      type: 'regular', home: BUF, away: KC, home_points: 17, away_points: 27,
      spread: 1, total_line: 40, rateable: 1, home_elo: 1600, away_elo: 1640,
      home_wp: 0.48 },
    { id: 'g3', date: '2026-09-27', at: '2026-09-27T17:00:00Z', week: 3,
      type: 'regular', home: KC, away: LA, home_points: 23, away_points: 20,
      spread: 3, total_line: 44, rateable: 1, home_elo: 1650, away_elo: 1500,
      home_wp: 0.7 },
    // LA against BUF: LA loses by 10 as a 3-point home dog.
    { id: 'g4', date: '2026-10-04', at: '2026-10-04T17:00:00Z', week: 4,
      type: 'regular', home: LA, away: BUF, home_points: 10, away_points: 20,
      spread: -3, total_line: 38, rateable: 1, home_elo: 1490, away_elo: 1610,
      home_wp: 0.35 },
    // Played, but carries no line at all: in nobody's record.
    { id: 'g5', date: '2026-10-11', at: '2026-10-11T17:00:00Z', week: 5,
      type: 'regular', home: HOU, away: LA, home_points: 14, away_points: 7,
      spread: null, total_line: null, rateable: 1, home_elo: 1500,
      away_elo: 1480, home_wp: 0.5 }
  ].concat(vigGames());
}

function board() {
  return [
    { team: KC, code: 'KC', conference: 'AFC', division: 'West',
      rating: 1648.4, rank: 1, wins: 3, losses: 0, ties: 0, rated_games: 3 },
    { team: BUF, code: 'BUF', conference: 'AFC', division: 'East',
      rating: 1591.6, rank: 2, wins: 1, losses: 2, ties: 0, rated_games: 3 },
    { team: LA, code: 'LA', conference: 'NFC', division: 'West',
      rating: 1500.0, rank: 3, wins: 0, losses: 3, ties: 0, rated_games: 3 },
    { team: HOU, code: 'HOU', conference: 'AFC', division: 'South',
      rating: 1495.0, rank: 4, wins: 1, losses: 0, ties: 0, rated_games: 1 },
    { team: SEA, code: 'SEA', conference: 'NFC', division: 'West',
      rating: 1505.0, rank: 5, wins: 11, losses: 10, ties: 0,
      rated_games: 21 },
    { team: DEN, code: 'DEN', conference: 'AFC', division: 'West',
      rating: 1495.0, rank: 6, wins: 10, losses: 11, ties: 0,
      rated_games: 21 }
  ];
}

function stamp(text) {
  return crypto.createHash('sha256').update(text).digest('hex').slice(0, 12);
}

function site() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'nfl-market-'));
  const data = path.join(root, 'data', 'nfl', 'seasons');
  fs.mkdirSync(data, { recursive: true });
  const payload = { season: 2026, board: board(), games: games() };
  const seasonText = 'window.NFL_SEASONS=(window.NFL_SEASONS||{});'
    + 'window.NFL_SEASONS[2026]=' + JSON.stringify(payload) + ';\n';
  const teamsText = 'window.NFL_TEAMS=' + JSON.stringify(teams()) + ';\n';
  fs.writeFileSync(path.join(data, '2026.js'), seasonText);
  fs.writeFileSync(path.join(root, 'data', 'nfl', 'teams.js'), teamsText);
  fs.writeFileSync(path.join(root, 'data', 'nfl', 'manifest.json'),
    JSON.stringify({
      schema_version: 2, sport: 'nfl', latest_season: 2026,
      generated_at: new Date(Date.now() - 20 * 60000).toISOString(),
      model: { name: 'nfl_elo_v1', fitted_on: [1999, 2020],
        held_out: [2021, 2025], holdout_brier: 0.22372,
        holdout_games: 1424 },
      coverage: [{ season: 2026, teams: 32, rated_teams: 6, games: 26,
        finals: 26, complete: false, played_teams: 6 }],
      seasons: { 2026: 'seasons/2026.js?v=' + stamp(seasonText) },
      files: { teams: 'teams.js?v=' + stamp(teamsText) }
    }));
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

async function open(hash) {
  const root = site();
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
    '#cards .card, #refusal-list li, #market-body table'));
  return dom;
}

function click(dom, selector) {
  const node = dom.window.document.querySelector(selector);
  assert.ok(node, 'no ' + selector);
  node.dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true }));
}

async function openMarket(dom) {
  click(dom, '#tabs button[data-tab="stats"]');
  click(dom, '#subnav button[data-view="market"]');
  await settle(() => dom.window.document.querySelector(
    '#market-body table, #market-body .empty'));
  return dom.window.document;
}

function cells(tr) {
  return [...tr.querySelectorAll('td')].map(td => td.textContent.trim());
}

test('the Stats tab now offers a second view', async () => {
  const dom = await open();
  click(dom, '#tabs button[data-tab="stats"]');
  const views = [...dom.window.document.querySelectorAll('#subnav button')]
    .map(node => node.dataset.view);
  assert.deepEqual(views, ['history', 'market']);
  dom.window.close();
});

test('every club of the season gets a row, best ATS record first',
  async () => {
    const dom = await open();
    const doc = await openMarket(dom);
    const rows = [...doc.querySelectorAll('#market-body tbody tr')];
    const names = rows.map(tr => cells(tr)[0]);
    // KC is 2-0-1 against the spread, BUF 1-2, LA 0-1-1 -- and Houston,
    // whose only game carried no line, comes last with nothing to show.
    assert.deepEqual(names, ['Kansas City Chiefs', 'Seattle Seahawks',
      'Denver Broncos', 'Buffalo Bills', 'Los Angeles Rams',
      'Houston Texans']);
    assert.deepEqual(cells(rows[0]).slice(1),
      ['3-0', '2-0-1', '100.0%', '2-1', '3']);
    dom.window.close();
  });

test('a club with no priced game shows a dash, not a nil record', async () => {
  const dom = await open();
  const doc = await openMarket(dom);
  const last = [...doc.querySelectorAll('#market-body tbody tr')].pop();
  const row = cells(last);
  assert.equal(row[0], 'Houston Texans');
  assert.equal(row[2], '—');      // ATS
  assert.equal(row[3], '—');      // ATS%
  assert.equal(row[4], '—');      // over/under
  assert.equal(row[5], '0');
  dom.window.close();
});

test('only a club that CLEARED the vig is marked, not merely one over .500',
  async () => {
    const dom = await open();
    const doc = await openMarket(dom);
    const rows = [...doc.querySelectorAll('#market-body tbody tr')];
    const marked = rows.filter(tr => tr.querySelector('td.beat'))
      .map(tr => cells(tr)[0]);
    // KC at 100% clears it. Seattle is 11-10, which is 52.38% -- over half
    // and UNDER the 52.4% a -110 bet has to clear, so it is not marked.
    // Marking anything over .500 would put Seattle here.
    const seattle = rows.find(tr => cells(tr)[0] === 'Seattle Seahawks');
    assert.equal(cells(seattle)[2], '11-10');
    assert.equal(cells(seattle)[3], '52.4%');   // rounds to the break-even
    assert.deepEqual(marked, ['Kansas City Chiefs']);
    dom.window.close();
  });

test('the legend states the break-even, because a record over half is not '
  + 'an edge', async () => {
    const dom = await open();
    const doc = await openMarket(dom);
    const legend = doc.querySelector('#market-body .legend').textContent;
    assert.match(legend, /52\.4%/);
    assert.match(legend, /push/);
    dom.window.close();
  });

test('the view is a place: the hash alone opens it', async () => {
  const dom = await open('#league=nfl&section=stats&view=market');
  await settle(() => dom.window.document.querySelector('#market-body table'));
  const doc = dom.window.document;
  assert.ok(!doc.getElementById('market').hidden);
  assert.ok(doc.getElementById('history').hidden);
  dom.window.close();
});

test('pressing the view writes it into the URL', async () => {
  const dom = await open();
  await openMarket(dom);
  assert.match(dom.window.location.hash, /view=market/);
  dom.window.close();
});
