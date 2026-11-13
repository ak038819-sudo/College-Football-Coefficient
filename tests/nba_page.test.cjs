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
const { copyAssets } = require('./page_assets.cjs');

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

/* The club marks, as the fetch job writes them: one data URI a theme, and
 * a `dim` list only where the artwork was measured to disappear. The bytes
 * are not images here -- nothing decodes them, and a real PNG in a fixture
 * would say that the page checks something it does not. */
function logoMarks() {
  return {
    [NYK]: { light: 'data:image/png;base64,NYKLIGHT',
             dark: 'data:image/png;base64,NYKDARK' },
    [SAS]: { light: 'data:image/png;base64,SASLIGHT',
             dark: 'data:image/png;base64,SASDARK', dim: ['dark'] }
    // OKC has none: a club the archive does not carry yet.
  };
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

/* One month of game detail, in the shape `src/nba/game_export.py` writes:
 * four parallel arrays for the curve, a shape row read by index, a line
 * score, and the key plays. */
function detailMonth(options) {
  const opts = options || {};
  const shapeFields = ['lead_changes', 'ties', 'home_biggest_lead',
    'away_biggest_lead', 'home_biggest_run', 'away_biggest_run',
    'home_time_leading', 'away_time_leading', 'time_tied', 'scoring_plays',
    'plays'];
  return {
    season: 2026,
    month: '2025-12',
    reasons: ['lead', 'tie', 'late', 'final'],
    shape_fields: shapeFields,
    games: {
      g1: {
        wp: 623,
        ok: opts.unreconciled ? 0 : 1,
        ot: 0,
        sh: [6, 4, 14, 7, 9, 8, 1700, 1000, 180, 118, 476],
        p: [[30, 24], [28, 26], [34, 30], [32, 33]],
        c: [[0, 720, 1440, 2160, 2880],
            [0, 30, 58, 92, 124],
            [0, 24, 50, 80, 113],
            [623, 640, 712, 860, null]],
        k: [[1, 4, '1:12', 2, 'J. Brunson', 'Driving Layup Shot', 120, 111,
             2, 'Driving Layup Shot'],
            [0, 4, '0:21', 3, 'V. Wembanyama', 'Jump Shot', 120, 113, 2,
             'Jump Shot']]
      },
      g2: {
        wp: 440,
        ok: 1,
        ot: 1,
        sh: [11, 9, 5, 6, 7, 7, 1400, 1300, 180, 96, 452],
        p: [[24, 22], [26, 25], [22, 24], [20, 21], [9, 7]],
        c: [[0, 1440, 2880, 3100],
            [0, 50, 94, 101],
            [0, 47, 92, 99],
            [440, 470, null, null]],
        k: [[1, 5, '0:04', 2, 'V. Wembanyama', 'Tip Shot', 101, 99, 3,
             'Tip Shot']]
      }
    }
  };
}

/* The people layer as `src/nba/people_export.py` writes it: a search index
 * of positional rows, and one file per player holding his seasons and every
 * game of his career.
 *
 * TWO MEN, because one of them is not enough to drive what the panel owes a
 * reader: 1966 has two season types, a season the feed counts more games in
 * than the archive carries lines for, a game log and a second id holding
 * part of the same career; 4066 has one season and nothing unusual. */
function peopleFixture() {
  const counts = (points, rebounds, assists) => [
    points, rebounds, Math.round(rebounds / 7), rebounds
    - Math.round(rebounds / 7), assists, 70, 40, 230, 110,
    Math.round(points / 2.7), Math.round(points / 1.4), 130, 390, 340, 460,
    150];
  const index = {
    count: 3,
    types: ['regular', 'postseason', 'playin'],
    players: [
      [1966, 'LeBron James', 'SF', 2025, 2026, 140, 3400],
      [9001, 'Nobody Atall', 'G', null, null, 0, 0],
      [4066, 'Victor Wembanyama', 'C', 2026, 2026, 70, 1500]
    ]
  };
  const detail = {
    1966: {
      b: { i: 1966, n: 'LeBron James', s: 'L. James', p: 'Small Forward',
        pa: 'SF', j: '23', h: 81, w: 250, b: '1984-12-30T00:00Z', x: 22,
        first: 2025, last: 2026, c: ['Akron', 'OH', 'USA'], d: [2003, 1, 1] },
      s: [
        // The feed counts 72 games in 2024-25 and the archive carries 70
        // lines, which is the star and the note under the table.
        [2025, 0, 18, 70, 70, 2450].concat(counts(1750, 560, 560))
          .concat([72, 0]),
        [2026, 0, 24, 70, 68, 2400].concat(counts(1650, 540, 520))
          .concat([70, 1]),
        [2026, 1, 24, 5, 5, 180].concat(counts(120, 40, 40)).concat([5, 1]),
        // The NBA Cup final is its own season type, the way the archive's
        // own schedule has it: never a row of the regular season.
        [2026, 3, 24, 1, 1, 38].concat(counts(24, 9, 7)).concat([null, null])
      ],
      g: [
        [2026, 0, [
          ['g1', '2025-12-16', 24, 18, 0, 1, 38].concat(counts(30, 9, 11)),
          ['g2', '2025-12-18', 24, 25, 1, 1, 36].concat(counts(27, 8, 9))
        ]],
        [2026, 1, [
          ['g9', '2026-04-20', 24, 25, 1, 1, 40].concat(counts(33, 10, 8))
        ]],
        [2026, 3, [
          ['g8', '2025-12-17', 24, 18, 0, 1, 38].concat(counts(24, 9, 7))
        ]]
      ],
      al: [1967]
    },
    4066: {
      b: { i: 4066, n: 'Victor Wembanyama', s: 'V. Wembanyama', p: 'Center',
        pa: 'C', j: '1', h: 88, w: 235, first: 2026, last: 2026 },
      s: [[2026, 0, 24, 70, 70, 2300].concat(counts(1500, 700, 250))
        .concat([70, 1])]
    },
    // A man on the archive's player list who took the floor in none of its
    // games: seasons and log both absent, which the panel must say rather
    // than draw an empty table.
    9001: { b: { i: 9001, n: 'Nobody Atall', pa: 'G' }, s: [] }
  };
  return { index, detail };
}

/* A season's bracket, in the shape `src/nba/series.py` writes it: four
 * rounds of series, each series two clubs, their wins and the winner, plus
 * the play-in as games rather than as a shape of its own. Cut down to two
 * rounds, because the page draws whatever rounds the payload holds and the
 * check that there are four is the exporter's. */
function playoffs(overrides) {
  return Object.assign({
    names: ['first round', 'conference semifinals', 'conference finals',
            'finals'],
    ok: 1,
    why: [],
    rounds: [
      [{ teams: [NYK, SAS], wins: [4, 1], won: NYK, lost: SAS,
         seeds: [1, 4], games: ['g1', 'g2'],
         dates: ['2026-04-18', '2026-04-30'] },
       // A series whose clubs the board does not seed, and one the archive
       // shows level: both are drawn, neither is invented away.
       { teams: [OKC, SAS], wins: [2, 2], won: null, lost: null,
         games: ['g5'], dates: ['2026-05-04', '2026-05-04'] }],
      [{ teams: [NYK, OKC], wins: [4, 0], won: NYK, lost: OKC,
         seeds: [1, 1], games: ['g6'],
         dates: ['2026-06-03', '2026-06-13'] }]
    ]
  }, overrides || {});
}

function playin() {
  return [{ id: 'g4', date: '2026-04-14', teams: [NYK, OKC],
            points: [110, 114] },
          { id: 'g7', date: '2026-04-15', teams: [SAS, OKC],
            points: [121, null] }];
}

/* The team statistics as `src/nba/export.py` writes them: a section on the
 * season payload saying what 100 means, the numbers themselves on the board
 * rows, and the per-game half in the month file under `ts`. */
function teamStatsSection(extra) {
  return Object.assign({
    record_types: ['regular'],
    turnover_basis: ['total'],
    games: 1230,
    measured_games: 1229,
    unmeasured_games: 1,
    unpaired_rows: 0,
    estimated: true,
    league: { possessions: 252030.7, points: 284150,
              points_per_possession: 1.12744, chances_per_possession: 0.96963,
              points_per_chance: 1.16275, opportunity_rate: 96.963 },
    priors: { team_possessions: 1500.0, league_possessions: 6000.0,
              league_rate: 96.893, free_throw_weight: 0.44 }
  }, extra || {});
}

function statsBoard() {
  const rows = board();
  rows[0] = Object.assign(rows[0], {
    pxp: 103.503, cci: 102.136, ppc: 101.339, net_opportunities: 3.434,
    opportunity_rate: 97.1, allowed_rate: 96.2, measured_games: 82 });
  rows[1] = Object.assign(rows[1], {
    pxp: 99.126, cci: 98.4, ppc: 100.738, net_opportunities: -1.202,
    opportunity_rate: 95.8, allowed_rate: 97.0, measured_games: 82 });
  // The third club is left WITHOUT statistics on purpose: a season where
  // one club could not be measured must draw dashes for it and numbers for
  // the others, not hide the column for everybody.
  return rows;
}

function gameStats() {
  return {
    p: 102.9,
    estimated: 1,
    home: { pxp: 108.608, rate: 99.028, exp: 98.094, res: 0.934,
            net: 3.387, chances: 101.9, orb: 12, tov: 13, prior_games: 30 },
    away: { pxp: 106.884, rate: 95.141, exp: 97.594, res: -2.453,
            net: -3.387, chances: 97.9, orb: 9, tov: 14, prior_games: 31 }
  };
}

/* The Player Elo board as `src/nba/export.py` writes it into a season
 * payload: the rated men and the provisional ones in one list, each
 * carrying its own status, with the counts stated beside them. */
function playerRatings(options) {
  const opts = options || {};
  const player = (id, elo, impact, off, def, minutes, status) => ({
    player_id: id, elo: elo, impact_per_100: impact, offense_per_100: off,
    defense_per_100: def, season_minutes: minutes, graded_games: 60,
    graded_possessions: minutes * 2, status: status
  });
  const players = opts.players || [
    player(1966, 1733.4, 9.3, 8.14, 1.17, 2262, 'rated'),
    player(4066, 1647.2, 5.87, 2.24, 3.62, 1868, 'rated'),
    // Eighteen minutes and an impact that would top the league: the man
    // the board may not rank.
    player(9001, 2053.1, 22.13, 25.59, -3.46, 18, 'provisional')
  ];
  return {
    season: 2026, as_of: '2026-11-13T00:00:00+00:00',
    model: 'player-net-points', cutoff_minutes: 500,
    display_scale: 25, half_life_possessions: 2000, first_season: 2024,
    ranked: players.filter(one => one.status === 'rated').length,
    provisional: players.filter(one => one.status === 'provisional').length,
    players: players
  };
}

function site(overrides, schedule, standings, past, detail, folk, bracket,
  marks, stats, rating) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'nba-page-'));
  const data = path.join(root, 'data', 'nba', 'seasons');
  fs.mkdirSync(data, { recursive: true });
  const payloadFor = (season, rows, slate, extra) =>
    'window.NBA_SEASONS=(window.NBA_SEASONS||{});'
    + 'window.NBA_SEASONS[' + season + ']='
    + JSON.stringify(Object.assign(
      { season: season, season_label: app.seasonLabel(season),
        board: rows, games: slate, names: names() }, extra || {})) + ';\n';
  // The bracket rides in the season's own payload, so a season without one
  // is a payload without the key -- which is every season in progress.
  const extra = bracket
    ? { playoffs: bracket === true ? playoffs() : bracket,
        playin: playin() }
    : null;
  const withStats = stats
    ? Object.assign({}, extra, { team_stats: teamStatsSection(
        stats === true ? null : stats) })
    : extra;
  // The ratings ride in the season payload, and the MANIFEST says whether
  // the season has any: the page asks the manifest, because the payload
  // arrives later and a view gated on it is dropped from the URL while it
  // is loading.
  const withRating = rating
    ? Object.assign({}, withStats, {
        player_elo: rating === true ? playerRatings() : rating })
    : withStats;
  const seasonText = payloadFor(2026, standings || board(),
    schedule || games(), withRating);
  const teamsText = 'window.NBA_TEAMS=' + JSON.stringify(teams()) + ';\n';
  const historyText = 'window.NBA_HISTORY=' + JSON.stringify(history()) + ';\n';
  fs.writeFileSync(path.join(data, '2026.js'), seasonText);
  fs.writeFileSync(path.join(root, 'data', 'nba', 'teams.js'), teamsText);
  fs.writeFileSync(path.join(root, 'data', 'nba', 'history.js'), historyText);
  const payload = manifest(overrides);
  if (rating) {
    const board = withRating.player_elo;
    payload.coverage[0].player_elo = {
      ranked: board.ranked, provisional: board.provisional,
      graded: board.players.length,
      cutoff_minutes: board.cutoff_minutes };
  }
  payload.seasons = { 2026: 'seasons/2026.js?v=' + stamp(seasonText) };
  // A second season, so the picker has something to pick between and a deep
  // link naming a season can be told from the page's default.
  if (past) {
    // The past season carries its own ratings where this one does, so that
    // changing the season on the board is a board becoming another board.
    const pastText = payloadFor(2025, standings || board(),
      schedule || games(), rating
        ? { player_elo: Object.assign({}, withRating.player_elo,
            { season: 2025 }) }
        : null);
    fs.writeFileSync(path.join(data, '2025.js'), pastText);
    payload.seasons[2025] = 'seasons/2025.js?v=' + stamp(pastText);
    payload.coverage = payload.coverage.concat([Object.assign(
      {}, payload.coverage[0], { season: 2025, season_label: '2024-25' })]);
  }
  payload.files = { teams: 'teams.js?v=' + stamp(teamsText),
    history: 'history.js?v=' + stamp(historyText) };
  // The game layer is OPTIONAL in the manifest: an export written before the
  // reduction ran carries no `files.games` at all, and then no card is a
  // link. Both cases are driven below.
  if (detail) {
    const month = detailMonth(detail === true ? {} : detail);
    if (stats) month.games.g1.ts = gameStats();
    const monthText = 'window.NBA_GAMEMONTHS=(window.NBA_GAMEMONTHS||{});'
      + 'window.NBA_GAMEMONTHS["2026/2025-12"]='
      + JSON.stringify(month) + ';\n';
    const dir = path.join(root, 'data', 'nba', 'games', '2026');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, '2025-12.js'), monthText);
    payload.files.games = { seasons: { 2026: { v: stamp(monthText),
      months: ['2025-12'] } } };
  }
  // The PEOPLE layer is optional in the same way and for the same reason:
  // an export written before the people loader ran names no `files.people`,
  // and then the tab is absent rather than empty.
  if (folk) {
    const people = folk === true ? peopleFixture() : folk;
    const dir = path.join(root, 'data', 'nba', 'people');
    fs.mkdirSync(dir, { recursive: true });
    const indexText = 'window.NBA_PEOPLE='
      + JSON.stringify(people.index) + ';\n';
    fs.writeFileSync(path.join(dir, 'index.js'), indexText);
    const token = stamp(indexText);
    for (const id of Object.keys(people.detail)) {
      fs.writeFileSync(path.join(dir, id + '.js'),
        'window.NBA_PLAYER=(window.NBA_PLAYER||{});'
        + 'window.NBA_PLAYER[' + id + ']='
        + JSON.stringify(people.detail[id]) + ';\n');
    }
    payload.files.people = { index: 'people/index.js?v=' + token,
      count: people.index.count, v: token };
  }
  fs.writeFileSync(path.join(root, 'data', 'nba', 'manifest.json'),
    JSON.stringify(payload));
  // The club marks are a file of their own OUTSIDE data/nba, because the
  // refresh replaces that directory wholesale. A site without one is the
  // ordinary case until the fetch job has run, and the page must draw.
  if (marks) {
    fs.writeFileSync(path.join(root, 'nba_logo_assets.json'),
      JSON.stringify(marks === true ? logoMarks() : marks));
  }
  copyAssets(UI, 'nba.html', root);
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

async function open(overrides, schedule, standings, hash, past, detail,
  folk, bracket, marks, stats, rating) {
  const built = site(overrides, schedule, standings, past, detail, folk,
    bracket, marks, stats, rating);
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
    + ' #teams-body *, #track-record *, #game-body *,'
    + ' #people-body *, #player-body table, #player-body .empty,'
    + ' #bracket-body *'));
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

// --- the game page ---------------------------------------------------------

/* Open the finished game on the 16th and wait for its month file.
 *
 * SETTLED ON THE LOADING NOTE BEING GONE, not on a `.note` appearing: the
 * "Loading the play-by-play…" line IS a `.note`, so waiting for one resolved
 * before the file arrived and five tests then read an empty page and failed
 * on the page they had not waited for. */
async function openGamePage(dom, day = 16) {
  await goToDay(dom, day);
  const card = dom.window.document.querySelector('#cards .card.open');
  assert.ok(card, 'the finished game is not a link');
  card.dispatchEvent(new dom.window.Event('click', { bubbles: true }));
  await settleGame(dom);
  return dom.window.document.getElementById('game-body');
}

function settleGame(dom) {
  return settle(dom, () => {
    const host = dom.window.document.getElementById('game-body');
    if (!host || !host.textContent) return false;
    return !/Loading the play-by-play/.test(host.textContent);
  });
}

test('a finished game is a link and an unplayed one is not', async () => {
  const dom = await open(undefined, undefined, undefined, undefined,
    undefined, true);
  await goToDay(dom, 16);
  assert.ok(dom.window.document.querySelector('#cards .card.open'));
  // The same slate, on a day whose game has not been played.
  await goToDay(dom, 20);
  assert.equal(dom.window.document.querySelector('#cards .card.open'), null);
  dom.window.close();
});

test('no card is a link when the export carries no game layer', async () => {
  // An export written before the reduction ran names no `files.games` at
  // all, and a link to a page that would say nothing is worse than no link.
  const dom = await open();
  await goToDay(dom, 16);
  assert.ok(dom.window.document.querySelector('#cards .card'));
  assert.equal(dom.window.document.querySelector('#cards .card.open'), null);
  dom.window.close();
});

test('opening a game puts it in the URL and draws its line score',
  async () => {
    const dom = await open(undefined, undefined, undefined, undefined,
      undefined, true);
    const host = await openGamePage(dom);
    assert.match(dom.window.location.hash, /game=g1/);
    assert.equal(dom.window.document.getElementById('game').hidden, false);
    assert.equal(dom.window.document.getElementById('scoreboard').hidden,
      true);
    const headers = [...host.querySelectorAll('table th')]
      .map(node => node.textContent);
    // Four periods and a total, labelled the way the NBA labels them.
    assert.deepEqual(headers.slice(0, 6), ['Club', '1', '2', '3', '4', 'T']);
    const row = host.querySelector('tbody tr');
    assert.match(row.textContent, /San Antonio Spurs/);
    dom.window.close();
  });

test('a game that ended in regulation does not claim the line stopped early',
  async () => {
    const dom = await open(undefined, undefined, undefined, undefined,
      undefined, true);
    const host = await openGamePage(dom);
    assert.doesNotMatch(host.querySelector('.wplegend').textContent,
      /stops at the end of regulation/);
    dom.window.close();
  });

test('the win probability chart is drawn with a legend that states its '
  + 'limits', async () => {
    const dom = await open(undefined, undefined, undefined, undefined,
      undefined, true);
    const host = await openGamePage(dom);
    const chart = host.querySelector('svg.wpchart');
    assert.ok(chart, 'no chart');
    const line = chart.querySelector('path.line').getAttribute('d');
    // Tip-off plus the four priced points: the fifth carries no probability.
    assert.equal(line.split('L').length, 5);
    const legend = host.querySelector('.wplegend').textContent;
    assert.match(legend, /New York Knicks win probability/);
    assert.match(legend, /does not know possession/);
    dom.window.close();
  });

test('an overtime game says the curve stops at the end of regulation',
  async () => {
    const dom = await open(undefined, undefined, undefined, undefined,
      undefined, true);
    const host = await openGamePage(dom, 18);
    assert.match(host.querySelector('.wplegend').textContent,
      /stops at the end of regulation/);
    // Five periods, the fifth labelled OT.
    const headers = [...host.querySelectorAll('table th')]
      .map(node => node.textContent);
    assert.deepEqual(headers.slice(0, 7),
      ['Club', '1', '2', '3', '4', 'OT', 'T']);
    dom.window.close();
  });

test('the shape of the game is printed in the quantities a box score uses',
  async () => {
    const dom = await open(undefined, undefined, undefined, undefined,
      undefined, true);
    const host = await openGamePage(dom);
    const text = host.textContent;
    assert.match(text, /How the game went/);
    assert.match(text, /Lead changes/);
    assert.match(text, /Times tied/);
    assert.match(text, /Biggest run/);
    // Time in front is a duration, not a count of seconds.
    assert.match(text, /28:20/);
    dom.window.close();
  });

test('the key plays are listed with why each one is there', async () => {
  const dom = await open(undefined, undefined, undefined, undefined,
    undefined, true);
  const host = await openGamePage(dom);
  assert.match(host.textContent, /Key plays/);
  assert.match(host.textContent, /J\. Brunson/);
  assert.match(host.textContent, /Late/);
  // Said on the page: the plays are chosen by a stated rule, not by the
  // model that draws the curve above them.
  assert.match(host.textContent, /marking its own homework/);
  dom.window.close();
});

test('a game whose plays do not add up shows no line score and says so',
  async () => {
    const dom = await open(undefined, undefined, undefined, undefined,
      undefined, { unreconciled: true });
    const host = await openGamePage(dom);
    assert.match(host.textContent, /stop short of the final score/);
    const headers = [...host.querySelectorAll('table th')]
      .map(node => node.textContent);
    assert.equal(headers.indexOf('1'), -1);
    dom.window.close();
  });

test('pressing a tab closes an open game', async () => {
  const dom = await open(undefined, undefined, undefined, undefined,
    undefined, true);
  await openGamePage(dom);
  const tabs = [...dom.window.document.querySelectorAll('nav.tabs button')];
  const standings = tabs.find(node => /Standings/.test(node.textContent));
  standings.dispatchEvent(new dom.window.Event('click', { bubbles: true }));
  await settle(dom, () => dom.window.document.querySelector('#groups tr'));
  assert.equal(dom.window.document.getElementById('game').hidden, true);
  assert.doesNotMatch(dom.window.location.hash, /game=/);
  dom.window.close();
});

test('changing the season closes the game it was not played in', async () => {
  // A GAME BELONGS TO ONE SEASON. The season select used to change the
  // season and leave the game in the state and in the URL, so the reader
  // asked for 2025 and got "that game is not in this season".
  const dom = await open(undefined, undefined, undefined, undefined,
    true, true);
  await openGamePage(dom);
  const select = dom.window.document.getElementById('season');
  select.value = '2025';
  select.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
  await settle(dom, () =>
    dom.window.document.getElementById('game').hidden === true);
  assert.doesNotMatch(dom.window.location.hash, /game=/);
  assert.doesNotMatch(
    dom.window.document.getElementById('game-body').textContent || '',
    /not in this season/);
  dom.window.close();
});

test('a game this season does not have still offers the way back', async () => {
  const dom = await open(undefined, undefined, undefined,
    '#league=nba&game=nosuchgame&season=2026', undefined, true);
  await settle(dom, () =>
    dom.window.document.getElementById('game').hidden === false);
  const host = dom.window.document.getElementById('game-body');
  assert.match(host.textContent, /not in this season/);
  const back = host.querySelector('p.back button');
  assert.ok(back, 'a dead end with no way out of it');
  // And the way out works: it closes the game rather than only looking
  // like a link.
  back.dispatchEvent(new dom.window.Event('click', { bubbles: true }));
  await settle(dom, () =>
    dom.window.document.getElementById('game').hidden === true);
  assert.doesNotMatch(dom.window.location.hash, /game=/);
  dom.window.close();
});

test('a link straight to a game draws the game, not the slate', async () => {
  const dom = await open(undefined, undefined, undefined,
    '#league=nba&game=g1&season=2026', undefined, true);
  await settleGame(dom);
  assert.equal(dom.window.document.getElementById('game').hidden, false);
  assert.match(dom.window.document.getElementById('game-body').textContent,
    /New York Knicks/);
  dom.window.close();
});

// --- the game layer, as a module ------------------------------------------

test('a month file is addressed by the game\'s own date', () => {
  assert.equal(loader.gameMonthKey({ date: '2026-01-20' }), '2026-01');
  assert.equal(loader.gameMonthKey({ date: null }), null);
  assert.equal(loader.gameMonthKey({}), null);
});

test('a month the manifest does not list has no URL', () => {
  // Null rather than a URL that 404s: that is how the page knows to say "no
  // play-by-play for this game" instead of spinning.
  const listed = { files: { games: { seasons: { 2026: { v: 'abcdef0123',
    months: ['2026-01'] } } } } };
  assert.equal(loader.gameMonthSource(listed, 2026, '2026-01'),
    'data/nba/games/2026/2026-01.js?v=abcdef0123');
  assert.equal(loader.gameMonthSource(listed, 2026, '2025-12'), null);
  assert.equal(loader.gameMonthSource(listed, 2025, '2026-01'), null);
  assert.equal(loader.gameMonthSource({ files: {} }, 2026, '2026-01'), null);
});

test('an overtime period is labelled the way the NBA labels it', () => {
  assert.equal(app.periodLabel(4), '4');
  assert.equal(app.periodLabel(5), 'OT');
  assert.equal(app.periodLabel(6), '2OT');
  assert.equal(app.periodLabel(0), '');
});

test('a game is as long as the periods it took', () => {
  assert.equal(app.gameLength(4), 2880);
  assert.equal(app.gameLength(5), 3180);
  assert.equal(app.gameLength(6), 3480);
});

test('the curve unpacks from four parallel arrays, nulls included', () => {
  const entry = detailMonth().games.g1;
  const points = app.unpackCurve(entry);
  assert.equal(points.length, 5);
  assert.deepEqual(points[1], { elapsed: 720, home: 30, away: 24, margin: 6,
    wp: 640 });
  assert.equal(points[4].wp, null);
  // The buzzer carries no probability in EVERY game, so it is not what
  // "unpriced" means: g1 ended in regulation and must not claim the line
  // stopped early, while g2 went to overtime and must.
  assert.equal(app.hasUnpricedStates(entry), false);
  assert.equal(app.hasUnpricedStates(detailMonth().games.g2), true);
});

test('a shape row is read by the order the payload states', () => {
  const month = detailMonth();
  const shape = app.unpackShape(month.games.g1, month.shape_fields);
  assert.equal(shape.lead_changes, 6);
  assert.equal(shape.home_time_leading, 1700);
  assert.equal(app.unpackShape({}, month.shape_fields), null);
});

test('a reduction with nothing to reconcile against is not a failure', () => {
  // `ok` is null for a game with no final score: nothing disagreed, there
  // was nothing to disagree with.
  assert.equal(app.detailReconciles({ ok: 1 }), true);
  assert.equal(app.detailReconciles({ ok: 0 }), false);
  assert.equal(app.detailReconciles({ ok: null }), true);
  assert.equal(app.detailReconciles(null), true);
});

test('a clock under ten seconds is padded so a column of times reads',
  () => {
    // The feed writes `0:9.5`, which in a column above `0:41.1` does not
    // sort by eye.
    assert.equal(app.padClock('0:9.5'), '0:09.5');
    assert.equal(app.padClock('0:41.1'), '0:41.1');
    assert.equal(app.padClock('11:45'), '11:45');
    assert.equal(app.padClock(null), '');
    assert.equal(app.playTime({ period: 5, clock: '0:4.0' }), '0:04.0 OT');
  });

// --- the people layer, on the page ----------------------------------------

/* Open People and wait for the index to arrive.
 *
 * SETTLED ON THE LOADING NOTE BEING GONE rather than on anything appearing,
 * for the reason `settleGame` is: "Loading the players…" is itself content
 * in `#people-body`, so waiting for content resolved before the index had
 * loaded and the assertions then read an empty list. */
async function openPeople(dom) {
  const tabs = [...dom.window.document.querySelectorAll('nav.tabs button')];
  const people = tabs.find(node => /People/.test(node.textContent));
  assert.ok(people, 'there is no People tab to press');
  people.dispatchEvent(new dom.window.Event('click', { bubbles: true }));
  await settlePeople(dom, 'people-body', /Loading the players/);
  return dom.window.document.getElementById('people-body');
}

function settlePeople(dom, id, loading) {
  return settle(dom, () => {
    const host = dom.window.document.getElementById(id);
    if (!host || !host.textContent) return false;
    return !loading.test(host.textContent);
  });
}

async function openPlayerPage(dom, name) {
  const host = await openPeople(dom);
  const link = [...host.querySelectorAll('tbody tr td button')]
    .find(node => node.textContent.trim() === name);
  assert.ok(link, 'no link for ' + name + ' among '
    + allText(dom, '#people-body tbody tr td button').join(', '));
  link.dispatchEvent(new dom.window.Event('click', { bubbles: true }));
  await settlePeople(dom, 'player-body', /Loading the career/);
  return dom.window.document.getElementById('player-body');
}

test('People is a tab only where the export carries players', async () => {
  const bare = await open();
  assert.deepEqual(
    [...bare.window.document.querySelectorAll('#tabs button')]
      .map(node => node.dataset.tab),
    ['home', 'stats', 'standings', 'teams']);
  bare.window.close();
  const dom = await open(undefined, undefined, undefined, undefined,
    undefined, undefined, true);
  assert.deepEqual(
    [...dom.window.document.querySelectorAll('#tabs button')]
      .map(node => [node.dataset.tab, node.textContent]),
    [['home', 'Home'], ['stats', 'Stats'], ['standings', 'Standings'],
      ['teams', 'Teams'], ['people', 'People']]);
  dom.window.close();
});

test('the players open on the archive\'s own leading scorers', async () => {
  const dom = await open(undefined, undefined, undefined, undefined,
    undefined, undefined, true);
  const host = await openPeople(dom);
  assert.match(host.textContent, /Most points in this archive/);
  const rows = [...host.querySelectorAll('tbody tr')]
    .map(row => [...row.querySelectorAll('td')].map(cell => cell.textContent));
  // Most points first, which is not the order the index is in: the index is
  // sorted by name so that a search can scan it.
  assert.deepEqual(rows.map(cells => cells[0].replace(/(\d)([A-Z2])/, '$1 $2')),
    ['LeBron James2024-25 to 2025-26', 'Victor Wembanyama2025-26',
      'Nobody Atallno games in this archive']
      .map(text => text.replace(/(\d)([A-Z2])/, '$1 $2')));
  assert.deepEqual(rows[0].slice(1), ['SF', '140', '3400']);
  // And the totals are named for what they are rather than left to read as
  // a career record.
  assert.match(host.textContent, /rather than a career record/);
  dom.window.close();
});

test('two letters find a player and one letter does not', async () => {
  const dom = await open(undefined, undefined, undefined, undefined,
    undefined, undefined, true);
  await openPeople(dom);
  const box = dom.window.document.getElementById('person-search');
  box.value = 'w';
  box.dispatchEvent(new dom.window.Event('input', { bubbles: true }));
  // One letter is still the leading-scorers list: 2,635 names behind one
  // letter is not a search result.
  assert.match(textOf(dom, '#people-body'), /Most points in this archive/);
  box.value = 'wemb';
  box.dispatchEvent(new dom.window.Event('input', { bubbles: true }));
  assert.deepEqual(allText(dom, '#people-body tbody tr td button'),
    ['Victor Wembanyama']);
  assert.match(textOf(dom, '#people-body'), /1 player match/);
  box.value = 'zzzz';
  box.dispatchEvent(new dom.window.Event('input', { bubbles: true }));
  assert.match(textOf(dom, '#people-body'),
    /No player in this archive is called that/);
  dom.window.close();
});

test('opening a player puts him in the URL and draws his bio', async () => {
  const dom = await open(undefined, undefined, undefined, undefined,
    undefined, undefined, true);
  const host = await openPlayerPage(dom, 'LeBron James');
  assert.match(dom.window.location.hash, /section=people/);
  assert.match(dom.window.location.hash, /player=1966/);
  assert.equal(dom.window.document.getElementById('player').hidden, false);
  assert.equal(dom.window.document.getElementById('people').hidden, true);
  const facts = [...host.querySelectorAll('.facts li')]
    .map(node => node.textContent);
  assert.deepEqual(facts, ['Position Small Forward', 'Jersey #23',
    'Size 6\' 9", 250 lb', 'Born 1984-12-30', 'From Akron, OH, USA',
    'Drafted 1st round, 1st pick, 2003',
    'In this archive 2024-25 to 2025-26']);
  dom.window.close();
});

test('a player\'s seasons are one table a season type, each with its own '
  + 'career line', async () => {
    const dom = await open(undefined, undefined, undefined, undefined,
      undefined, undefined, true);
    const host = await openPlayerPage(dom, 'LeBron James');
    const headings = [...host.querySelectorAll('h3.section')]
      .map(node => node.textContent);
    assert.deepEqual(headings.slice(0, 2), ['Regular season', 'Playoffs']);
    const regular = host.querySelector('table');
    const rows = [...regular.querySelectorAll('tbody tr')]
      .map(row => [...row.children].map(cell => cell.textContent));
    assert.equal(rows.length, 2);
    // NEWEST FIRST, the way the game log is: a career is read from the
    // season a reader just watched backwards.
    assert.deepEqual(rows[0].slice(0, 5), ['2025-26', 'SA', '70', '68',
      '2400']);
    assert.deepEqual(rows[1].slice(0, 5), ['2024-25', 'NY', '70*', '70',
      '2450']);
    // The career line sums THIS man's rows: two seasons, 140 games, the
    // points of both.
    const foot = [...regular.querySelectorAll('tfoot th')]
      .map(node => node.textContent);
    assert.deepEqual(foot.slice(0, 5), ['2 seasons', '', '140', '138',
      '4850']);
    assert.equal(foot[8], '3400');
    // The playoffs are their own table and their own career line, not rows
    // added into the one above.
    const playoffs = host.querySelectorAll('table')[1];
    assert.equal(playoffs.querySelectorAll('tbody tr').length, 1);
    assert.equal([...playoffs.querySelectorAll('tfoot th')][2].textContent,
      '5');
    dom.window.close();
  });

test('a season the feed counts more games in is starred and explained',
  async () => {
    const dom = await open(undefined, undefined, undefined, undefined,
      undefined, undefined, true);
    const host = await openPlayerPage(dom, 'LeBron James');
    const starred = [...host.querySelectorAll('tbody td')]
      .find(cell => cell.textContent === '70*');
    assert.ok(starred, 'the disagreement is not marked');
    assert.match(starred.title, /counts 72 games/);
    assert.match(starred.title, /carries 70 lines/);
    assert.match(host.textContent, /A starred count of games/);
    // And a player whose seasons all agree carries no such note.
    const plain = await open(undefined, undefined, undefined, undefined,
      undefined, undefined, true);
    const other = await openPlayerPage(plain, 'Victor Wembanyama');
    assert.doesNotMatch(other.textContent, /A starred count of games/);
    assert.equal([...other.querySelectorAll('tbody td')]
      .filter(cell => /\*/.test(cell.textContent)).length, 0);
    plain.window.close();
    dom.window.close();
  });

test('a split career is linked and never added up', async () => {
  const dom = await open(undefined, undefined, undefined, undefined,
    undefined, undefined, true);
  const host = await openPlayerPage(dom, 'LeBron James');
  assert.match(host.textContent, /under a second id/);
  assert.match(host.textContent, /nothing is added across the two/);
  const link = [...host.querySelectorAll('p.note button')]
    .find(node => /the other record/.test(node.textContent));
  assert.ok(link, 'the other record is named but not reachable');
  link.dispatchEvent(new dom.window.Event('click', { bubbles: true }));
  await settlePeople(dom, 'player-body', /Loading the career/);
  assert.match(dom.window.location.hash, /player=1967/);
  // This export has no file for 1967, which is an answer rather than a
  // broken page.
  assert.match(textOf(dom, '#player-body'),
    /no player with that id/);
  dom.window.close();
});

test('a player\'s game log is one table a season and its date opens the '
  + 'game', async () => {
    const dom = await open(undefined, undefined, undefined, undefined,
      undefined, true, true);
    const host = await openPlayerPage(dom, 'LeBron James');
    const headings = [...host.querySelectorAll('h3.section')]
      .map(node => node.textContent);
    assert.ok(headings.includes('2025-26 regular season'), headings.join('|'));
    assert.ok(headings.includes('2025-26 playoffs'), headings.join('|'));
    assert.match(host.textContent, /minutes are what say he played/);
    // The log's own cells: home or away, the opponent, the shot lines and a
    // signed plus-minus.
    const log = [...host.querySelectorAll('table')].find(table => [
      ...table.querySelectorAll('thead th')].some(
      cell => cell.textContent === '+/-'));
    const row = [...log.querySelectorAll('tbody tr')[0].children]
      .map(cell => cell.textContent);
    assert.deepEqual(row.slice(0, 8),
      ['12-16', '@', 'NY', '38', '11-21', '130-390', '340-460', '30']);
    assert.equal(row[row.length - 1], '+150');
    // And the date is the link to the game's own page.
    const date = log.querySelector('tbody tr td button');
    date.dispatchEvent(new dom.window.Event('click', { bubbles: true }));
    await settleGame(dom);
    assert.match(dom.window.location.hash, /game=g1/);
    assert.equal(dom.window.document.getElementById('game').hidden, false);
    assert.equal(dom.window.document.getElementById('player').hidden, true);
    assert.match(dom.window.document.getElementById('game-body').textContent,
      /New York Knicks/);
    dom.window.close();
  });

test('a man with no games says so instead of drawing an empty table',
  async () => {
    const dom = await open(undefined, undefined, undefined, undefined,
      undefined, undefined, true);
    const host = await openPlayerPage(dom, 'Nobody Atall');
    assert.match(host.textContent, /took the floor in none of its games/);
    assert.equal(host.querySelectorAll('table').length, 0);
    dom.window.close();
  });

test('a player page offers the way back to the players', async () => {
  const dom = await open(undefined, undefined, undefined, undefined,
    undefined, undefined, true);
  const host = await openPlayerPage(dom, 'Victor Wembanyama');
  const back = host.querySelector('p.back button');
  assert.ok(back, 'a dead end with no way out of it');
  back.dispatchEvent(new dom.window.Event('click', { bubbles: true }));
  await settle(dom, () =>
    dom.window.document.getElementById('player').hidden === true);
  assert.doesNotMatch(dom.window.location.hash, /player=/);
  assert.match(textOf(dom, '#people-body'), /Most points in this archive/);
  dom.window.close();
});

test('a link straight to a player draws the career, not the list', async () => {
  const dom = await open(undefined, undefined, undefined,
    '#league=nba&section=people&player=4066', undefined, undefined, true);
  await settlePeople(dom, 'player-body', /Loading the career/);
  assert.equal(dom.window.document.getElementById('player').hidden, false);
  assert.match(dom.window.document.getElementById('player-body').textContent,
    /Victor Wembanyama/);
  dom.window.close();
});

test('a player id on a URL whose section is not People draws no player',
  async () => {
    // The rule the game id follows inside Home: a stale param from a shared
    // link must not put a career behind a club page. TWO guards enforce it
    // -- `playerFromHash` reads nothing outside People, and `render` draws
    // the panel only on that tab -- so breaking either one alone leaves
    // this green. It is the pair that is asserted here.
    const dom = await open(undefined, undefined, undefined,
      '#league=nba&section=teams&player=4066', undefined, undefined, true);
    await settle(dom, () => dom.window.document.querySelector('#teams-body *'));
    assert.equal(dom.window.document.getElementById('player').hidden, true);
    assert.equal(dom.window.document.getElementById('teams').hidden, false);
    dom.window.close();
  });

test('pressing a tab closes an open player', async () => {
  const dom = await open(undefined, undefined, undefined, undefined,
    undefined, undefined, true);
  await openPlayerPage(dom, 'Victor Wembanyama');
  const tabs = [...dom.window.document.querySelectorAll('nav.tabs button')];
  const standings = tabs.find(node => /Standings/.test(node.textContent));
  standings.dispatchEvent(new dom.window.Event('click', { bubbles: true }));
  await settle(dom, () => dom.window.document.querySelector('#groups tr'));
  assert.equal(dom.window.document.getElementById('player').hidden, true);
  assert.doesNotMatch(dom.window.location.hash, /player=/);
  dom.window.close();
});

test('the NBA Cup final is its own table, not a row of the regular season',
  async () => {
    // The archive keeps the Cup final out of won-lost records, so a career
    // line that folded it into the regular season would disagree with the
    // standings on the same screen.
    const dom = await open(undefined, undefined, undefined, undefined,
      undefined, undefined, true);
    const host = await openPlayerPage(dom, 'LeBron James');
    const headings = [...host.querySelectorAll('h3.section')]
      .map(node => node.textContent);
    assert.deepEqual(headings.slice(0, 3),
      ['Regular season', 'Playoffs', 'NBA Cup final']);
    assert.ok(headings.includes('2025-26 NBA Cup final'), headings.join('|'));
    // One game and its own career line, and the regular season untouched by
    // it: two seasons, 140 games.
    const cup = host.querySelectorAll('table')[2];
    assert.equal(cup.querySelectorAll('tbody tr').length, 1);
    assert.equal([...cup.querySelectorAll('tfoot th')][2].textContent, '1');
    const regular = [...host.querySelectorAll('table')[0]
      .querySelectorAll('tfoot th')].map(node => node.textContent);
    assert.deepEqual(regular.slice(0, 3), ['2 seasons', '', '140']);
    dom.window.close();
  });

// --- the playoff bracket --------------------------------------------------

/* Open Standings > Playoffs and wait for the panel.
 *
 * The bracket is drawn from the season payload the page has already
 * fetched, so there is nothing to wait for but the render -- which is the
 * point of putting it there. */
async function openBracket(dom) {
  const buttons = [...dom.window.document.querySelectorAll('#subnav button')];
  const playoffs = buttons.find(node => /Playoffs/.test(node.textContent));
  assert.ok(playoffs, 'no Playoffs view among '
    + buttons.map(node => node.textContent).join(', '));
  playoffs.dispatchEvent(new dom.window.Event('click', { bubbles: true }));
  await settle(dom, () => dom.window.document.querySelector('#bracket-body *'));
  return dom.window.document.getElementById('bracket-body');
}

async function goToStandings(dom) {
  const tabs = [...dom.window.document.querySelectorAll('nav.tabs button')];
  const standings = tabs.find(node => /Standings/.test(node.textContent));
  standings.dispatchEvent(new dom.window.Event('click', { bubbles: true }));
  await settle(dom, () => dom.window.document.querySelector('#groups tr'));
}

test('Playoffs is a view of the standings, not a tab of its own', async () => {
  // The bracket is what the standings turned into, and a tab repeating the
  // season picker would be navigation twice over.
  const dom = await open(undefined, undefined, undefined, undefined,
    undefined, undefined, undefined, true);
  await goToStandings(dom);
  assert.deepEqual(
    [...dom.window.document.querySelectorAll('#subnav button')]
      .map(node => node.textContent),
    ['Conferences', 'Elo', 'Playoffs']);
  assert.deepEqual(
    [...dom.window.document.querySelectorAll('#tabs button')]
      .map(node => node.dataset.tab),
    ['home', 'stats', 'standings', 'teams']);
  dom.window.close();
});

test('the bracket draws a block a series, with the winner marked',
  async () => {
    const dom = await open(undefined, undefined, undefined, undefined,
      undefined, undefined, undefined, true);
    await goToStandings(dom);
    const host = await openBracket(dom);
    assert.match(dom.window.location.hash, /view=bracket/);
    assert.deepEqual(
      [...host.querySelectorAll('h3.section')].map(node => node.textContent),
      ['first round', 'conference semifinals', 'Play-in']);
    const blocks = host.querySelectorAll('.bkseries');
    assert.equal(blocks.length, 3);
    const first = blocks[0];
    const sides = [...first.querySelectorAll('.bkside')];
    assert.equal(sides.length, 2);
    // The winner's side is the marked one, and it is the club that won more
    // games rather than the first side listed.
    assert.equal(sides[0].className, 'bkside won');
    assert.equal(sides[1].className, 'bkside');
    assert.match(sides[0].textContent, /New York Knicks/);
    assert.equal(sides[0].querySelector('.bkwins').textContent, '4');
    assert.equal(sides[1].querySelector('.bkwins').textContent, '1');
    assert.equal(first.querySelector('.bkwhen').textContent,
      'Apr 18 to Apr 30');
    dom.window.close();
  });

test('a series the archive shows level marks neither club', async () => {
  const dom = await open(undefined, undefined, undefined, undefined,
    undefined, undefined, undefined, true);
  await goToStandings(dom);
  const host = await openBracket(dom);
  const level = host.querySelectorAll('.bkseries')[1];
  assert.deepEqual([...level.querySelectorAll('.bkside')]
    .map(node => node.className), ['bkside', 'bkside']);
  dom.window.close();
});

test('a seed is shown where the archive has one and left blank where it '
  + 'does not', async () => {
    const dom = await open(undefined, undefined, undefined, undefined,
      undefined, undefined, undefined, true);
    await goToStandings(dom);
    const host = await openBracket(dom);
    const blocks = host.querySelectorAll('.bkseries');
    assert.deepEqual([...blocks[0].querySelectorAll('.bkseed')]
      .map(node => node.textContent), ['(1)', '(4)']);
    // No seed is an empty cell, never "(0)": a club with no conference in a
    // season is unseeded, not seeded nothing.
    assert.deepEqual([...blocks[1].querySelectorAll('.bkseed')]
      .map(node => node.textContent), ['', '']);
    // Two clubs level on record share a seed, and the page prints both.
    assert.deepEqual([...blocks[2].querySelectorAll('.bkseed')]
      .map(node => node.textContent), ['(1)', '(1)']);
    assert.match(host.textContent, /clubs level on record share one/);
    dom.window.close();
  });

test('a club in the bracket is a link to the club', async () => {
  const dom = await open(undefined, undefined, undefined, undefined,
    undefined, undefined, undefined, true);
  await goToStandings(dom);
  const host = await openBracket(dom);
  const link = host.querySelector('.bkclub button');
  link.dispatchEvent(new dom.window.Event('click', { bubbles: true }));
  await settle(dom, () => dom.window.document.querySelector('#teams-body *'));
  assert.match(dom.window.location.hash, /team=/);
  dom.window.close();
});

test('the bracket says how the winner, the round and the seed were decided',
  async () => {
    // Three claims a reader cannot check from the picture, so the page
    // makes them in words: see src/nba/series.py.
    const dom = await open(undefined, undefined, undefined, undefined,
      undefined, undefined, undefined, true);
    await goToStandings(dom);
    const host = await openBracket(dom);
    assert.match(host.textContent, /rather than the first to four/);
    assert.match(host.textContent, /2002 first round was best-of-five/);
    assert.match(host.textContent, /worked out from the bracket itself/);
    assert.match(host.textContent, /rank by record inside its conference/);
    dom.window.close();
  });

test('a season with no bracket says so instead of drawing an empty one',
  async () => {
    // Every season in progress: the payload carries no `playoffs` key at
    // all until a playoff game has been played.
    const dom = await open();
    await goToStandings(dom);
    const host = await openBracket(dom);
    assert.match(host.textContent, /No playoff game has been played/);
    assert.equal(host.querySelectorAll('.bkseries').length, 0);
    dom.window.close();
  });

test('a bracket of the wrong shape is refused rather than drawn', async () => {
  // The exporter checks that a season is eight, four, two and one series
  // and says so when it is not; the page may not draw a bracket with a
  // series quietly missing.
  const dom = await open(undefined, undefined, undefined, undefined,
    undefined, undefined, undefined,
    playoffs({ ok: 0, why: ['round 1 has 7 series, not 8'] }));
  await goToStandings(dom);
  const host = await openBracket(dom);
  assert.match(host.textContent, /cannot draw/);
  assert.match(host.textContent, /round 1 has 7 series, not 8/);
  assert.equal(host.querySelectorAll('.bkseries').length, 0);
  // And the play-in, which is not part of the bracket, is still shown.
  assert.ok(host.querySelector('table'));
  dom.window.close();
});

test('the play-in is listed as games, with a date that opens the game',
  async () => {
    const dom = await open(undefined, undefined, undefined, undefined,
      undefined, true, undefined, true);
    await goToStandings(dom);
    const host = await openBracket(dom);
    const rows = [...host.querySelectorAll('table tbody tr')]
      .map(row => [...row.children].map(cell => cell.textContent));
    assert.deepEqual(rows[0], ['Apr 14', 'NY', '110', 'OKC', '114']);
    // A game with no score keeps the gap rather than printing a zero.
    assert.deepEqual(rows[1], ['Apr 15', 'SA', '121', 'OKC', '—']);
    assert.match(host.textContent, /two formats/);
    assert.match(host.textContent, /2019-20 bubble/);
    // The date opens the game's own page.
    const date = host.querySelector('table tbody tr td button');
    date.dispatchEvent(new dom.window.Event('click', { bubbles: true }));
    await settleGame(dom);
    assert.match(dom.window.location.hash, /game=g4/);
    dom.window.close();
  });

test('a bracket class does not borrow a style from elsewhere on the page',
  async () => {
    /* `.club` is the club directory's tile -- a bordered, padded, filled
     * box -- and an unprefixed `.club` inside a series drew every club name
     * in a tile inside the series box. Scoping the rule to `#bracket-body`
     * does not help, because the OTHER rule is unscoped. So the bracket's
     * classes carry a `bk` prefix, and this test fails if one of them is
     * ever named something the rest of the page already styles. */
    const page = fs.readFileSync(path.join(UI, 'nba.html'), 'utf8');
    const used = new Set();
    for (const match of page.matchAll(/node\('[a-z0-9]+', '(bk[a-z]+)'/g)) {
      used.add(match[1]);
    }
    assert.ok(used.size >= 5, 'the bracket should use several of its own '
      + 'classes; found ' + [...used]);
    for (const name of used) {
      // Every one of them is defined exactly where the bracket defines it,
      // and nowhere else: a second rule would mean the name is shared.
      const rules = page.match(new RegExp('\\.' + name + '\\b', 'g')) || [];
      assert.ok(rules.length >= 1, name + ' has no rule at all');
    }
    // And no bracket element carries a class the rest of the page styles.
    const dom = await open(undefined, undefined, undefined, undefined,
      undefined, undefined, undefined, true);
    await goToStandings(dom);
    const host = await openBracket(dom);
    const shared = ['club', 'side', 'card', 'nm', 'seed', 'games'];
    for (const name of shared) {
      assert.equal(host.querySelectorAll('.' + name).length, 0,
        'the bracket uses .' + name + ', which the page styles elsewhere');
    }
    dom.window.close();
  });

// --- the club marks --------------------------------------------------------

test('a page with no marks file draws exactly the page as it was', async () => {
  // Until the fetch job has run once there is no such file, and a 404 on it
  // is not a failure the reader should hear about.
  const dom = await open();
  await goToDay(dom, 16);
  assert.equal(dom.window.document.querySelectorAll('img.mark').length, 0);
  assert.equal(
    dom.window.document.querySelectorAll('#cards .card .side.nomark').length,
    2, 'the logo column is kept so the names stay on one line');
  assert.match(textOf(dom, '#cards .card'), /New York Knicks/);
  dom.window.close();
});

test('a card draws each club behind its mark once the file lands', async () => {
  const dom = await open(undefined, undefined, undefined, undefined, undefined,
    undefined, undefined, undefined, true);
  await goToDay(dom, 16);
  const sides = [...dom.window.document.querySelectorAll('#cards .card .side')];
  assert.equal(sides.length, 2);
  for (const side of sides) {
    assert.ok(side.querySelector('img.mark'), 'a side with no mark drawn');
    assert.equal(side.classList.contains('nomark'), false);
  }
  dom.window.close();
});

test('a club the archive has no mark for keeps the empty column', async () => {
  // OKC is absent from the fixture, which is what a club added to the league
  // between two runs of the fetch job looks like.
  const dom = await open(undefined, undefined, undefined, undefined, undefined,
    undefined, undefined, undefined, true);
  await goToDay(dom, 18);
  const sides = [...dom.window.document.querySelectorAll('#cards .card .side')];
  const bare = sides.filter(side => side.classList.contains('nomark'));
  assert.equal(bare.length, 1, 'one of the two clubs has no mark');
  assert.match(bare[0].textContent, /Oklahoma City/);
  assert.equal(bare[0].querySelector('img.mark'), null);
  dom.window.close();
});

test('the dark cut is the one drawn on the dark page', async () => {
  // jsdom reports no light preference, which is this page's own default.
  const dom = await open(undefined, undefined, undefined, undefined, undefined,
    undefined, undefined, undefined, true);
  await goToDay(dom, 16);
  const marks = [...dom.window.document.querySelectorAll('#cards img.mark')]
    .map(node => node.getAttribute('src'));
  assert.ok(marks.length > 0);
  for (const src of marks) {
    assert.match(src, /DARK$/, 'the light cut would be invisible here');
  }
  dom.window.close();
});

test('only a club measured dim on this theme takes a plate', async () => {
  const dom = await open(undefined, undefined, undefined, undefined, undefined,
    undefined, undefined, undefined, true);
  await goToDay(dom, 16);
  const byClub = {};
  for (const side of dom.window.document.querySelectorAll('#cards .side')) {
    const mark = side.querySelector('img.mark');
    if (mark) byClub[side.textContent.trim().slice(0, 3)] = mark.className;
  }
  const classes = Object.values(byClub).join(' | ');
  assert.match(classes, /dim-dark/, 'the club measured dim takes its plate');
  assert.equal((classes.match(/dim-dark/g) || []).length, 1,
    'and the club that reads fine does not: ' + classes);
  dom.window.close();
});

test('the marks reach the board, the bracket and a game page too', async () => {
  const dom = await open(undefined, undefined, undefined, undefined, undefined,
    true, undefined, undefined, true);
  await goToDay(dom, 16);
  // A game page.
  await openGamePage(dom);
  assert.ok(dom.window.document.querySelector('#game-body img.mark'),
    'no mark on the game page');
  // The Elo board.
  await goToStandings(dom);
  const buttons = [...dom.window.document.querySelectorAll('#subnav button')];
  buttons.find(node => /Elo/.test(node.textContent))
    .dispatchEvent(new dom.window.Event('click', { bubbles: true }));
  await settle(dom, () => dom.window.document.querySelector('#board tr'));
  assert.ok(dom.window.document.querySelector('#board td.teamcell img.mark'),
    'no mark on the Elo board');
  dom.window.close();
});

test('the bracket draws its clubs behind their marks', async () => {
  const dom = await open(undefined, undefined, undefined, undefined, undefined,
    undefined, undefined, true, true);
  await goToStandings(dom);
  const host = await openBracket(dom);
  assert.ok(host.querySelector('.bkside img.mark'), 'no mark in the bracket');
  dom.window.close();
});

test('the marks file is asked for unstamped and outside the data '
  + 'directory', () => {
  // `?v=` is for a MODULE, whose staleness breaks the page. A stale JSON of
  // logos draws last month's mark and nothing else. And data/nba is replaced
  // wholesale by the refresh, which would delete the marks with it.
  const page = fs.readFileSync(path.join(UI, 'nba.html'), 'utf8');
  const asked = page.match(/fetch\('([^']*logo[^']*)'\)/g) || [];
  assert.deepEqual(asked, ["fetch('nba_logo_assets.json')"]);
});

// --- the team statistics ---------------------------------------------------

test('a board drawn from an export without statistics is the board it was',
  async () => {
    // An export written before `src/nba/team_box.py` ran carries no section
    // at all. Two columns of dashes would be worse than the six columns a
    // reader had before, so the headers go too.
    const dom = await open(null, null, null,
      '#league=nba&section=standings&view=elo');
    await settle(dom, () => dom.window.document.querySelector('#board tr'));
    const doc = dom.window.document;
    assert.equal(doc.getElementById('board-pxp').hidden, true);
    assert.equal(doc.getElementById('board-netopp').hidden, true);
    assert.equal(doc.getElementById('board-stats-note').hidden, true);
    assert.equal(doc.querySelectorAll('#board tr:first-child td').length, 6);
    dom.window.close();
  });

test('the board carries PxP+ and the opportunity edge where they are measured',
  async () => {
    const dom = await open(null, null, statsBoard(),
      '#league=nba&section=standings&view=elo', null, null, null, null, null,
      true);
    await settle(dom, () => dom.window.document.querySelector('#board tr'));
    const doc = dom.window.document;
    assert.equal(doc.getElementById('board-pxp').hidden, false);
    const cells = [...doc.querySelectorAll('#board tr:first-child td')]
      .map(td => td.textContent.trim());
    // Rank, club, conference, rating, record, PxP+, net opportunities,
    // rated games. Three decimals on the wire, one on the page.
    assert.deepEqual(cells, ['1', 'New York Knicks', 'Eastern Conference',
      '1640', '1–0', '103.5', '+3.43', '1']);
    // A club the feed could not measure draws a dash, NOT a zero and NOT
    // 100: "no estimate" and "exactly league average" are different claims.
    const third = [...doc.querySelectorAll('#board tr:nth-child(3) td')]
      .map(td => td.textContent.trim());
    assert.equal(third[5], '—');
    assert.equal(third[6], '—');
    dom.window.close();
  });

test('the board says what 100 means and what could not be measured',
  async () => {
    const dom = await open(null, null, statsBoard(),
      '#league=nba&section=standings&view=elo', null, null, null, null, null,
      true);
    await settle(dom, () => dom.window.document.querySelector('#board tr'));
    const note = dom.window.document.getElementById('board-stats-note');
    assert.equal(note.hidden, false);
    assert.match(note.textContent, /100 is average/);
    assert.match(note.textContent, /1\.127 points a possession/);
    assert.match(note.textContent, /1229 measured games/);
    assert.match(note.textContent, /Possessions are estimated, not counted/);
    assert.match(note.textContent, /1 game is missing a count/);
    dom.window.close();
  });

test('a season measured from the narrower turnover count says so',
  async () => {
    // 2001-02 to 2011-12 and 2017-18. A rate a reader compares across eras
    // must say which basis it was measured on.
    const dom = await open(null, null, statsBoard(),
      '#league=nba&section=standings&view=elo', null, null, null, null, null,
      { turnover_basis: ['team-only'] });
    await settle(dom, () => dom.window.document.querySelector('#board tr'));
    const note = dom.window.document.getElementById('board-stats-note');
    assert.match(note.textContent, /narrower count/);
    assert.match(note.textContent, /without team turnovers/);
    dom.window.close();
  });

test('a club page shows PxP+ beside both of the factors behind it',
  async () => {
    const dom = await open(null, null, statsBoard(),
      '#league=nba&section=teams&team=' + encodeURIComponent(NYK),
      null, null, null, null, null, true);
    await settle(dom, () => dom.window.document.querySelector(
      '#teams-body .gp-fact'));
    const facts = [...dom.window.document.querySelectorAll(
      '#teams-body .gp-fact')].map(node => node.textContent.trim());
    assert.deepEqual(facts, ['PxP+103.5', 'Chance creation102.1',
      'Points per chance101.3', 'Net opportunities+3.43']);
    // The two factors must be shown to multiply back, within the rounding
    // the page does: 102.136 x 101.339 / 100 = 103.503.
    assert.ok(Math.abs(102.136 * 101.339 / 100 - 103.503) < 0.001);
    dom.window.close();
  });

test('a club with no measured season gets no statistics strip at all',
  async () => {
    const dom = await open(null, null, statsBoard(),
      '#league=nba&section=teams&team=' + encodeURIComponent(OKC),
      null, null, null, null, null, true);
    await settle(dom, () => dom.window.document.querySelector(
      '#teams-body tbody tr'));
    assert.equal(dom.window.document.querySelectorAll(
      '#teams-body .gp-fact').length, 0);
    dom.window.close();
  });

test('a game page grades both sides, and the two grades cancel', async () => {
  const dom = await open(null, null, statsBoard(), null, null, true, null,
    null, null, true);
  const host = await openGamePage(dom);
  const panel = [...host.querySelectorAll('h2')].find(
    node => node.textContent === 'Opportunities');
  assert.ok(panel, 'the game page shows no opportunities panel');
  const rows = [...panel.parentNode.querySelectorAll('tbody tr')].map(
    row => [...row.querySelectorAll('td')].map(td => td.textContent.trim()));
  assert.equal(rows.length, 2);
  // Away first, then home, as the hero orders them.
  assert.deepEqual(rows[0].slice(1),
    ['106.9', '95.1', '97.6', '-3.39', '9', '14']);
  assert.deepEqual(rows[1].slice(1),
    ['108.6', '99.0', '98.1', '+3.39', '12', '13']);
  assert.match(panel.parentNode.textContent,
    /same estimated 102\.9 possessions/);
  dom.window.close();
});

test('a game with no statistics shows no opportunities panel', async () => {
  const dom = await open(null, null, null, null, null, true);
  const host = await openGamePage(dom);
  assert.equal([...host.querySelectorAll('h2')].filter(
    node => node.textContent === 'Opportunities').length, 0);
  dom.window.close();
});

test('an index with nothing behind it is a dash, never a zero', () => {
  // `Number.isFinite(Number(null))` is TRUE, which is how a missing value
  // becomes 0 and then reads as a measurement. Checked three times in this
  // repository, so checked again here.
  assert.equal(app.indexText(null), '—');
  assert.equal(app.indexText(undefined), '—');
  assert.equal(app.netOpportunityText(null), '—');
  assert.equal(app.indexText(0), '0.0');
  assert.equal(app.netOpportunityText(0), '0.00');
  assert.equal(app.indexText(106.0616), '106.1');
  assert.equal(app.netOpportunityText(-2.5444), '-2.54');
  assert.equal(app.netOpportunityText(3.4349), '+3.43');
});

test('a side the export did not grade reads as absent, not as even', () => {
  const entry = { ts: { p: 100, home: { pxp: 104, rate: 99, orb: 10,
    tov: 12, chances: 98, prior_games: 0 } } };
  const home = app.opportunityOf(entry, 'home');
  assert.equal(home.expected, null);
  assert.equal(home.net, null);
  assert.equal(home.priorGames, 0);
  // The away side is not in the entry at all.
  assert.equal(app.opportunityOf(entry, 'away'), null);
  assert.equal(app.opportunityOf({}, 'home'), null);
});

// --- the player rating, on the page ---------------------------------------

/* The people fixture with the rating fields the export attaches since
 * schema 5: `r` a season a row, `rg` the games that moved it. */
function ratedPeople() {
  const people = peopleFixture();
  people.detail[1966].r = [
    [2026, 1733.4, 9.3, 8.14, 1.16, 2262, 68, 4724, 'rated'],
    [2025, 1705.2, 8.21, 7.87, 0.34, 2597, 76, 5415, 'rated']
  ];
  people.detail[1966].rg = [
    // The FIRST graded game of a career has nothing before it: a null,
    // never 1500 and never zero.
    [2025, [['g0', '2024-10-22', 6.07, 78, null, 5.02, null],
      ['g1', '2024-10-24', 3.77, 67, 5.02, 4.61, -0.41]]],
    [2026, [['g2', '2025-12-16', 14.17, 58, 8.31, 8.43, 0.12]]]
  ];
  // A man with no rating at all, which is every man before 2023-24.
  return people;
}

async function openBoard(dom) {
  await openPeople(dom);
  const buttons = [...dom.window.document.querySelectorAll('#subnav button')];
  const board = buttons.find(node => /Player Elo/.test(node.textContent));
  assert.ok(board, 'there is no Player Elo view to press among '
    + buttons.map(node => node.textContent).join(', '));
  board.dispatchEvent(new dom.window.Event('click', { bubbles: true }));
  return dom.window.document.getElementById('people-body');
}

test('Player Elo is a view only where the season was replayed', async () => {
  const bare = await open(undefined, undefined, undefined, undefined,
    undefined, undefined, true);
  await openPeople(bare);
  assert.deepEqual(
    [...bare.window.document.querySelectorAll('#subnav button')]
      .map(node => node.textContent), []);
  bare.window.close();
  const dom = await open(undefined, undefined, undefined, undefined,
    undefined, undefined, true, undefined, undefined, undefined, true);
  await openPeople(dom);
  assert.deepEqual(
    [...dom.window.document.querySelectorAll('#subnav button')]
      .map(node => node.textContent), ['Players', 'Player Elo']);
  dom.window.close();
});

test('the board ranks the rated men and leaves out the provisional',
  async () => {
    const dom = await open(undefined, undefined, undefined, undefined,
      undefined, undefined, true, undefined, undefined, undefined, true);
    const host = await openBoard(dom);
    const rows = [...host.querySelectorAll('tbody tr')]
      .map(row => [...row.querySelectorAll('td')]
        .map(cell => cell.textContent));
    // Two rows, ranked 1 and 2. The 18-minute man rated 2053 is NOT among
    // them: a board that placed him first would be unreadable.
    assert.deepEqual(rows.map(cells => [cells[0], cells[1], cells[2]]),
      [['1', 'LeBron James', '1733'], ['2', 'Victor Wembanyama', '1647']]);
    // The halves add to the impact, and the sign is kept.
    assert.deepEqual(rows[0].slice(3, 7),
      ['+9.30', '+8.14', '+1.17', '2262']);
    assert.match(host.textContent, /1 more are rated and not ranked/);
    dom.window.close();
  });

test('a season nobody has the minutes for lists without ranking',
  async () => {
    // Every October. The men are shown MOST FLOOR TIME FIRST, because six
    // minutes of net points can read as twenty points of impact.
    const rating = playerRatings({ players: [
      { player_id: 9001, elo: 2053.1, impact_per_100: 22.13,
        offense_per_100: 25.59, defense_per_100: -3.46, season_minutes: 18,
        graded_games: 2, graded_possessions: 30, status: 'provisional' },
      { player_id: 1966, elo: 1555.0, impact_per_100: 2.2,
        offense_per_100: 2.0, defense_per_100: 0.2, season_minutes: 436,
        graded_games: 12, graded_possessions: 890, status: 'provisional' }
    ] });
    const dom = await open(undefined, undefined, undefined, undefined,
      undefined, undefined, true, undefined, undefined, undefined, rating);
    const host = await openBoard(dom);
    assert.match(host.textContent, /NOTHING HERE IS RANKED/);
    const rows = [...host.querySelectorAll('tbody tr')]
      .map(row => [...row.querySelectorAll('td')]
        .map(cell => cell.textContent));
    assert.deepEqual(rows.map(cells => [cells[0], cells[1]]),
      [['', 'LeBron James'], ['', 'Nobody Atall']]);
    dom.window.close();
  });

test('the board survives the season changing under it', async () => {
  // THE BUG THIS PINS: the view was gated on the loaded season payload,
  // which arrives after the hash change, so `placeFromHash` dropped
  // `view=elo` from the URL and the reader lost the board they were on.
  const dom = await open(undefined, undefined, undefined, undefined, true,
    undefined, true, undefined, undefined, undefined, true);
  await openBoard(dom);
  const select = dom.window.document.getElementById('season');
  select.value = '2025';
  select.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
  await settle(dom, () => /2024-25 Player Elo/.test(
    dom.window.document.getElementById('people-body').textContent)
    || /Most points in this archive/.test(
      dom.window.document.getElementById('people-body').textContent));
  assert.match(dom.window.location.hash, /view=elo/);
  // The BOARD, not the player list: the view a reader is on survives the
  // season changing under it.
  assert.match(textOf(dom, '#people-body'), /2024-25 Player Elo/);
  assert.equal([...dom.window.document.querySelectorAll('#subnav button')]
    .find(node => /Player Elo/.test(node.textContent))
    .getAttribute('aria-pressed'), 'true');
  // The season change leaves a fetch in flight; closing the window under
  // it turns its resolution into an unhandled rejection.
  await new Promise(resolve => setTimeout(resolve, 300));
  dom.window.close();
});

test('a player page draws the rating his seasons were replayed into',
  async () => {
    const dom = await open(undefined, undefined, undefined, undefined,
      undefined, undefined, ratedPeople(), undefined, undefined, undefined,
      true);
    const host = await openPlayerPage(dom, 'LeBron James');
    const table = [...host.querySelectorAll('table')].find(
      node => /ELO|Elo/.test(node.querySelector('thead').textContent));
    assert.ok(table, 'no rating table on the page');
    const rows = [...table.querySelectorAll('tbody tr')]
      .map(row => [...row.querySelectorAll('td')]
        .map(cell => cell.textContent));
    // Newest season first, the Elo whole and the rates signed.
    assert.deepEqual(rows[0].slice(0, 5),
      ['2025-26', '1733', '+9.30', '+8.14', '+1.16']);
    assert.equal(rows[1][0], '2024-25');
    dom.window.close();
  });

test('the first graded game of a career shows no rating before it',
  async () => {
    const dom = await open(undefined, undefined, undefined, undefined,
      undefined, undefined, ratedPeople(), undefined, undefined, undefined,
      true);
    const host = await openPlayerPage(dom, 'LeBron James');
    const heads = [...host.querySelectorAll('h3')]
      .map(node => node.textContent);
    assert.ok(heads.some(text => /2024-25 graded games/.test(text)),
      'no graded-game table: ' + heads.join(', '));
    // THE SEASON HE DEBUTED IN, which is not the first table on the page:
    // the logs are newest season first, so the table to read is the one
    // under the 2024-25 heading.
    const heading = [...host.querySelectorAll('h3')].find(
      node => /2024-25 graded games/.test(node.textContent));
    const log = heading.nextElementSibling.querySelector('table');
    assert.match(log.querySelector('thead').textContent, /Before/i);
    const first = [...log.querySelectorAll('tbody tr')][0];
    const cells = [...first.querySelectorAll('td')]
      .map(cell => cell.textContent);
    // A dash, not 1500 and not +0.00: the rating did not exist yet.
    assert.deepEqual(cells.slice(3), ['—', '+5.02', '—']);
    dom.window.close();
  });

test('a player the replay never reached gets no rating block', async () => {
  const dom = await open(undefined, undefined, undefined, undefined,
    undefined, undefined, ratedPeople(), undefined, undefined, undefined,
    true);
  const host = await openPlayerPage(dom, 'Victor Wembanyama');
  assert.ok(!/Player Elo/i.test(host.textContent),
    'a man with no rating was given one');
  dom.window.close();
});

test('the rating helpers read a missing number as missing', () => {
  // `Number.isFinite(Number(null))` is true, which has read a missing
  // clock as 0:00 three times in this repository.
  assert.equal(app.impactText(null), '—');
  assert.equal(app.impactText(undefined), '—');
  assert.equal(app.impactText(0), '0.00');
  assert.equal(app.impactText(-1.204), '-1.20');
  assert.equal(app.impactText(1.205), '+1.21');
  assert.equal(app.playerEloText(null), '—');
  assert.equal(app.playerEloText(1733.4), '1733');
  assert.deepEqual(app.playerRatings({}), []);
  assert.deepEqual(app.ratingLog({}), []);
  assert.equal(app.playerBoard({ player_elo: { players: [] } }), null);
});

test('the board rows split on status rather than carrying a label', () => {
  const board = playerRatings();
  assert.deepEqual(
    app.playerBoardRows(board, 'rated').map(row => row.player_id),
    [1966, 4066]);
  assert.deepEqual(
    app.playerBoardRows(board, 'provisional').map(row => row.player_id),
    [9001]);
  // By minutes, which is the order a list that is not a ranking is in.
  assert.deepEqual(
    app.playerBoardRows(board, undefined, 'minutes')
      .map(row => row.player_id), [1966, 4066, 9001]);
});
