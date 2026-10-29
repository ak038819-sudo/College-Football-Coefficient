// A link to an NFL game names its season, and the page has to open THAT
// season. Every other route test runs against a one-season fixture, which
// cannot tell "loaded the right season" from "loaded the only season", so
// this file builds two.
'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { JSDOM } = require('jsdom');

const UI = path.join(__dirname, '..', 'ui');

const KC = 'nfl:nflverse:franchise:KC';
const BUF = 'nfl:nflverse:franchise:BUF';
const PREFIX = 'nfl:nflverse:game:';

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

/* One final a season, with the provider's own id shape -- the season is IN
   the id, which is the whole point of the rule under test. */
function games(season) {
  return [{ id: PREFIX + season + '_01_BUF_KC',
            date: season + '-09-13', at: season + '-09-13T17:00:00Z',
            week: 1, type: 'regular', home: KC, away: BUF,
            home_points: season === 2004 ? 31 : 24, away_points: 20,
            rateable: 1, venue: 'Arrowhead Stadium' }];
}

const SEASONS = [2004, 2026];

function site() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'nfl-routes-'));
  const base = path.join(root, 'data', 'nfl');
  fs.mkdirSync(path.join(base, 'seasons'), { recursive: true });
  const teamsText = 'window.NFL_TEAMS=' + JSON.stringify(teams()) + ';\n';
  fs.writeFileSync(path.join(base, 'teams.js'), teamsText);
  const payload = {
    schema_version: 4,
    sport: 'nfl',
    generated_at: new Date(Date.now() - 20 * 60000).toISOString(),
    latest_season: 2026,
    model: { name: 'nfl_elo_v1', fitted_on: [1999, 2020],
      held_out: [2021, 2025], holdout_brier: 0.22372, holdout_games: 1424 },
    // The gates are equalities, not floors: the league is a fixed size, so
    // a settled season has to state all 32 clubs rated. The season in
    // progress is the one allowed to be short.
    coverage: SEASONS.map(season => season === 2026
      ? { season, teams: 32, rated_teams: 2, games: 1, finals: 1,
          complete: false, played_teams: 2 }
      : { season, teams: 32, rated_teams: 32, games: 1, finals: 1,
          complete: true, played_teams: 32 }),
    seasons: {},
    files: { teams: 'teams.js?v=' + stamp(teamsText) }
  };
  for (const season of SEASONS) {
    const text = 'window.NFL_SEASONS=(window.NFL_SEASONS||{});'
      + 'window.NFL_SEASONS[' + season + ']='
      + JSON.stringify({ season, board: board(), games: games(season) })
      + ';\n';
    fs.writeFileSync(path.join(base, 'seasons', season + '.js'), text);
    payload.seasons[season] = 'seasons/' + season + '.js?v=' + stamp(text);
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

async function open(hash) {
  const root = site();
  const file = path.join(root, 'nfl.html');
  const dom = new JSDOM(fs.readFileSync(file, 'utf8'), {
    url: 'file://' + file + (hash || ''),
    runScripts: 'dangerously',
    resources: 'usable',
    pretendToBeVisual: true,
    beforeParse(window) {
      window.scrollTo = () => {};
      window.fetch = url => {
        const target = path.join(root, String(url));
        return Promise.resolve({ ok: fs.existsSync(target),
          json: () => Promise.resolve(
            JSON.parse(fs.readFileSync(target, 'utf8'))) });
      };
    }
  });
  await settle(() => dom.window.document.querySelector(
    '#cards .card, #refusal-list li, #game .gamehead'));
  return dom;
}

test('a link to a game loads the season the game is in, not the latest',
  async () => {
    const dom = await open('#league=nfl&game=2004_01_BUF_KC');
    const doc = dom.window.document;
    await settle(() => doc.querySelector('#game .gamehead'));
    assert.equal(doc.getElementById('season').value, '2004',
      'the page opened on the wrong season');
    // The 2004 game ended 20-31 and the 2026 one 20-24. Reading the score
    // is what tells the two apart -- both name the same two clubs.
    assert.match(doc.querySelector('#game .score').textContent,
      /20 – 31/);
    dom.window.close();
  });

test('with no game in the link the page still opens on the latest season',
  async () => {
    const dom = await open();
    const doc = dom.window.document;
    await settle(() => doc.querySelector('#cards .card'));
    assert.equal(doc.getElementById('season').value, '2026');
    dom.window.close();
  });

test('a link naming a season this export does not carry falls back',
  async () => {
    const dom = await open('#league=nfl&game=1975_01_BUF_KC');
    const doc = dom.window.document;
    await settle(() => doc.querySelector('#cards .card'));
    assert.equal(doc.getElementById('season').value, '2026',
      'the page asked for a season file that is not there');
    assert.equal(doc.getElementById('game').hidden, true);
    dom.window.close();
  });

test('changing season closes the game, in the URL as well as the page',
  async () => {
    const dom = await open('#league=nfl&game=2004_01_BUF_KC');
    const doc = dom.window.document;
    await settle(() => doc.querySelector('#game .gamehead'));
    const select = doc.getElementById('season');
    select.value = '2026';
    select.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    // The cards are rendered even while the game is over them, so waiting
    // for a card would be waiting for something that is already there.
    await settle(() => doc.getElementById('season').value === '2026'
      && !doc.getElementById('slate').hidden);
    assert.equal(dom.window.location.hash, '#league=nfl',
      'the URL still names a game from the season just left');
    assert.equal(doc.getElementById('game').hidden, true);
    dom.window.close();
  });

test('following a link to another season’s game loads that season',
  async () => {
    // Only the hash changes, so the page is not reloaded and start() does
    // not run again. The back and forward buttons reach this: open a 2004
    // game, leave it, change season, go forward.
    const dom = await open();
    const doc = dom.window.document;
    await settle(() => doc.querySelector('#cards .card'));
    assert.equal(doc.getElementById('season').value, '2026');
    dom.window.location.hash = '#league=nfl&game=2004_01_BUF_KC';
    await settle(() => !doc.getElementById('game').hidden
      && doc.querySelector('#game .score'));
    assert.equal(doc.getElementById('season').value, '2004');
    assert.match(doc.querySelector('#game .score').textContent, /20 – 31/);
    dom.window.close();
  });
