// The NFL people layer in the browser: the pure helpers that read the packed
// files, and the page that draws them, loaded in jsdom against a site with a
// real manifest naming a people layer.
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

// --- the pure helpers ------------------------------------------------------

const INDEX = {
  shards: 64,
  players: [
    ['00-1', 'Josh Allen', 'QB', 2018, 2026, 'BUF', 3],
    ['00-2', 'Josh Allen', 'OLB', 2019, 2024, 'JAX', 9],
    ['00-3', 'Keenan Allen', 'WR', 2013, 2025, 'LAC', 3],
    ['00-4', "Ja'Marr Chase", 'WR', 2021, 2026, 'CIN', 5],
    ['00-5', 'Kaʻimi Fairbairn', 'K', 2016, 2026, 'HOU', 5]
  ],
  tables: [
    { key: 'passing', label: 'Passing',
      stats: ['completions', 'attempts', 'passing_yards', 'passing_cpoe'] },
    { key: 'kicking', label: 'Kicking', stats: ['fg_made', 'fg_long'] },
    { key: 'defense', label: 'Defense', stats: ['def_sacks'] }
  ],
  mean_stats: ['passing_cpoe', 'target_share'],
  max_stats: ['fg_long']
};

test('a search ranks a name that starts with the query first, then the '
  + 'more recent career', () => {
  const found = app.searchPlayers(INDEX, 'josh allen');
  assert.deepEqual(found.map(row => row.id), ['00-1', '00-2']);
  // "allen" starts no name but begins a word in three of them; the most
  // recent careers come first.
  assert.deepEqual(app.searchPlayers(INDEX, 'allen').map(row => row.id),
                   ['00-1', '00-3', '00-2']);
});

test('a search ignores accents and punctuation', () => {
  assert.equal(app.searchPlayers(INDEX, 'jamarr')[0].id, '00-4');
  assert.equal(app.searchPlayers(INDEX, 'kaimi')[0].id, '00-5');
  assert.deepEqual(app.searchPlayers(INDEX, '   '), []);
});

const SHARD = {
  stats: ['attempts', 'completions', 'def_sacks', 'fg_long', 'passing_cpoe',
          'passing_yards'],
  players: {
    '00-1': {
      seasons: [
        [2019, 1, 'BUF', 1, 0, 30, 1, 20, 5, 200],
        [2018, 0, 'BUF', 12, 0, 300, 1, 170, 4, -6.5, 5, 2000],
        [2019, 0, 'BUF', 16, 0, 400, 1, 260, 4, 1.5, 5, 3000, 3, 20]
      ],
      roster: [[2018, 'BUF', 'QB', 17, 'ACT']]
    }
  }
};

test('packed season rows unpack in order, the regular season first', () => {
  const rows = app.unpackSeasons(SHARD, '00-1');
  assert.deepEqual(rows.map(row => [row.season, row.type]),
                   [[2018, 'reg'], [2019, 'reg'], [2019, 'post']]);
  assert.deepEqual(rows[0].stats, { attempts: 300, completions: 170,
                                    passing_cpoe: -6.5,
                                    passing_yards: 2000 });
  assert.deepEqual(app.unpackSeasons(SHARD, 'nobody'), []);
  assert.deepEqual(app.unpackRoster(SHARD, '00-1'),
                   [{ season: 2018, team: 'BUF', position: 'QB', jersey: 17,
                      status: 'ACT' }]);
});

test('career totals add counts, keep a best, and leave averages out', () => {
  const reg = app.unpackSeasons(SHARD, '00-1')
    .filter(row => row.type === 'reg');
  const totals = app.careerTotals(reg, INDEX.mean_stats, INDEX.max_stats);
  assert.equal(totals.games, 28);
  assert.equal(totals.stats.passing_yards, 5000);
  assert.equal(totals.stats.fg_long, 20);
  assert.equal('passing_cpoe' in totals.stats, false,
               'a mean of season CPOEs is not a career CPOE');
});

test('career totals refuse to add a postseason into a regular season', () => {
  assert.throws(() => app.careerTotals(app.unpackSeasons(SHARD, '00-1')),
                /mix regular season and postseason/);
});

test('a page draws only the tables and columns a player has numbers in',
  () => {
    const rows = app.unpackSeasons(SHARD, '00-1')
      .filter(row => row.type === 'reg');
    const tables = app.statTablesFor(rows, INDEX.tables);
    assert.deepEqual(tables.map(table => table.key), ['passing', 'kicking']);
    assert.deepEqual(tables[1].columns, ['fg_long']);
    assert.deepEqual(tables[1].rows.map(row => row.season), [2019]);
    // The order is the export's, never alphabetical.
    assert.deepEqual(tables[0].columns,
      ['completions', 'attempts', 'passing_yards', 'passing_cpoe']);
  });

test('statistics print as the sport writes them', () => {
  assert.equal(app.statText('def_sacks', 1.5), '1.5');
  assert.equal(app.statText('passing_yards', 5000), '5,000');
  assert.equal(app.statText('sack_yards_lost', -169), '−169');
  assert.equal(app.statText('passing_epa', 12.34), '+12.3');
  assert.equal(app.statText('target_share', 0.2345), '23.4%');
  assert.equal(app.statText('receptions', undefined), '—');
  assert.equal(app.heightText(75), '6-3');
  assert.equal(app.playerSpan(2018, 2018), '2018');
});

test('a draft file reads back as rounds of picks', () => {
  const file = { fields: ['round', 'pick', 'team', 'id', 'name'],
    seasons: { 2018: [[1, 7, 'BUF', '00-1', 'Josh Allen'],
                      [1, 8, 'CHI', null, 'Someone'],
                      [2, 40, 'BUF', null, 'Later']] } };
  const rounds = app.draftRounds(file, 2018);
  assert.deepEqual(rounds.map(round => [round.round, round.picks.length]),
                   [[1, 2], [2, 1]]);
  assert.equal(rounds[0].picks[1].id, null);
  assert.deepEqual(app.draftRounds(file, 1999), []);
});

test('the loader reads the people section and refuses a missing one', () => {
  const manifest = { files: { people: { index: 'people/index.js?v=1',
    careers: ['people/careers/0.js?v=2'] } } };
  assert.equal(loader.peopleSource(manifest, 'index', 'd/'),
               'd/people/index.js?v=1');
  assert.equal(loader.careerSource(manifest, 0, 'd/'),
               'd/people/careers/0.js?v=2');
  assert.equal(loader.careerSource(manifest, 1, 'd/'), null);
  assert.equal(loader.peopleSource({ files: {} }, 'index'), null);
});

// --- the page --------------------------------------------------------------

function stamp(text) {
  return crypto.createHash('sha256').update(text).digest('hex').slice(0, 10);
}

function write(root, name, text) {
  const target = path.join(root, 'data', 'nfl', name);
  fs.mkdirSync(path.dirname(target), { recursive: true });
  fs.writeFileSync(target, text);
  return name + '?v=' + stamp(text);
}

function site() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'nfl-people-'));
  const season = { season: 2026, board: [], games: [{
    id: 'g1', date: '2026-09-10', week: 1, type: 'REG',
    home: 'nfl:nflverse:franchise:BUF', away: 'nfl:nflverse:franchise:KC',
    home_points: 20, away_points: 17, rateable: 1 }],
    leaders: { passing_yards: [
      { id: '00-1', name: 'Josh Allen', position: 'QB', team: 'BUF',
        value: 1504 }] } };
  const careers = [];
  for (let shard = 0; shard < 64; shard += 1) {
    const body = shard === 3
      ? { stats: SHARD.stats, players: { '00-1': Object.assign(
        { name: 'Josh Allen', position: 'QB', height: 77, weight: 237,
          college: 'Wyoming',
          draft: { season: 2018, round: 1, pick: 7, team: 'BUF' } },
        SHARD.players['00-1']) } }
      : { stats: [], players: {} };
    careers.push(write(root, `people/careers/${shard}.js`,
      `window.NFL_PEOPLE_SHARDS=(window.NFL_PEOPLE_SHARDS||{});`
      + `window.NFL_PEOPLE_SHARDS[${shard}]=${JSON.stringify(body)};\n`));
  }
  const files = {
    teams: write(root, 'teams.js', 'window.NFL_TEAMS=' + JSON.stringify({
      'nfl:nflverse:franchise:BUF': { code: 'BUF', name: 'Buffalo Bills' },
      'nfl:nflverse:franchise:KC': { code: 'KC', name: 'Kansas City Chiefs' }
    }) + ';\n'),
    people: {
      index: write(root, 'people/index.js',
        'window.NFL_PEOPLE_INDEX=' + JSON.stringify(INDEX) + ';\n'),
      current: write(root, 'people/current.js',
        'window.NFL_PEOPLE_CURRENT=' + JSON.stringify({ season: 2026,
          stats: ['passing_yards'],
          players: { '00-1': { seasons: [[2026, 0, 'BUF', 6, 0, 1504]],
                               roster: [[2026, 'BUF', 'QB', 17, 'ACT']] } }
        }) + ';\n'),
      draft: write(root, 'people/draft.js', 'window.NFL_DRAFT='
        + JSON.stringify({ fields: ['round', 'pick', 'team', 'id', 'name',
          'position', 'college', 'hall_of_fame', 'all_pro', 'pro_bowls',
          'seasons_started', 'games', 'weighted_av', 'draft_av'],
        seasons: { 2026: [[1, 1, 'BUF', '00-1', 'Josh Allen', 'QB',
          'Wyoming', 0, 0, 0, 0, 0, 0, 0],
        [1, 2, 'KC', null, 'No Page', 'WR', 'Somewhere', 0, 0, 0, 0, 0, 0,
          0]] } }) + ';\n'),
      season: 2026, shards: 64, careers: careers
    }
  };
  const seasonText = 'window.NFL_SEASONS=(window.NFL_SEASONS||{});'
    + 'window.NFL_SEASONS[2026]=' + JSON.stringify(season) + ';\n';
  const seasons = { 2026: write(root, 'seasons/2026.js', seasonText) };
  fs.writeFileSync(path.join(root, 'data', 'nfl', 'manifest.json'),
    JSON.stringify({
      schema_version: 3, sport: 'nfl',
      generated_at: new Date(Date.now() - 20 * 60000).toISOString(),
      latest_season: 2026,
      model: { name: 'nfl_elo_v1', fitted_on: [1999, 2020],
        held_out: [2021, 2025], holdout_brier: 0.22372,
        holdout_games: 1424 },
      coverage: [{ season: 2026, teams: 32, rated_teams: 0, games: 1,
        finals: 1, complete: false, played_teams: 0 }],
      seasons: seasons, files: files }));
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

async function open() {
  const root = site();
  const file = path.join(root, 'nfl.html');
  const dom = new JSDOM(fs.readFileSync(file, 'utf8'), {
    url: 'file://' + file, runScripts: 'dangerously', resources: 'usable',
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
    '#cards .card, #refusal-list li'));
  return dom;
}

function button(document, scope, label) {
  return [...document.querySelectorAll(scope + ' button')]
    .find(node => node.textContent === label);
}

test('a leader opens his page, with a career row through the season in '
  + 'progress', async () => {
  const dom = await open();
  const document = dom.window.document;
  assert.equal(document.getElementById('refusal').hidden, true);
  button(document, '#tabs', 'Players').click();
  const body = document.getElementById('people-body');
  assert.match(body.textContent, /2026 regular-season leaders/);
  button(document, '#people-body', 'Josh Allen').click();
  await settle(() => /Passing/.test(body.textContent)
    && /Drafted 2018/.test(body.textContent));
  assert.match(body.textContent, /6-5, 237 lb/);
  const passing = [...body.querySelectorAll('table')][0];
  const rows = [...passing.querySelectorAll('tbody tr')]
    .map(tr => [...tr.children].map(td => td.textContent));
  assert.deepEqual(rows.map(row => row[0]),
                   ['2018', '2019', '2026', 'Career']);
  // 2,000 + 3,000 + the 1,504 of the season in progress, which lives in a
  // different file from the settled seasons.
  assert.equal(rows[3][rows[3].length - 1 - 1], '6,504');
  assert.equal(rows[3][rows[3].length - 1], '—',
               'a career CPOE is not printed');
  assert.match(body.textContent, /Passing · Postseason/);
  dom.window.close();
});

test('the header search finds a player and the draft links what it can',
  async () => {
    const dom = await open();
    const document = dom.window.document;
    const search = document.getElementById('search');
    search.value = 'allen';
    search.dispatchEvent(new dom.window.Event('input'));
    const body = document.getElementById('people-body');
    await settle(() => body.querySelectorAll('tbody tr').length === 3);
    assert.equal(document.getElementById('people').hidden, false);
    button(document, '#people-views', 'Draft').click();
    await settle(() => /2026 draft/.test(body.textContent));
    assert.match(body.textContent, /2 picks; 1 open a player page/);
    assert.ok(button(document, '#people-body', 'Josh Allen'));
    assert.equal(button(document, '#people-body', 'No Page'), undefined,
                 'a pick with no page is plain text');
    assert.match(body.textContent, /No Page/);
    dom.window.close();
  });
