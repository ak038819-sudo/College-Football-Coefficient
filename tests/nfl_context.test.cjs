// What phase 1 added to the NFL page, driven as a page.
//
// The pure helpers are tested as functions and the panels are tested by
// loading ui/nfl.html in jsdom with a real manifest, a real season payload
// and a real history file on disk. A green build is not a rendered page, and
// three of the four panels here did not exist an hour before this file did.
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
const REID = 'nfl:nflverse:coach:Andy Reid';

// --- the pure helpers ------------------------------------------------------

test('a spread names the favoured side, and zero is a pick-em', () => {
  // `spread` is the HOME team's expected margin, positive when the home team
  // is favoured: the convention measured against the archive, not assumed.
  assert.equal(app.spreadText({ spread: 3, home: KC, away: BUF }),
    'KC −3');
  assert.equal(app.spreadText({ spread: -6.5, home: KC, away: BUF }),
    'BUF −6.5');
  assert.equal(app.spreadText({ spread: 0, home: KC, away: BUF }),
    'Pick’em');
  assert.equal(app.spreadText({ home: KC, away: BUF }), null);
});

test('a missing reading is absent, and a measured zero is a measurement',
  () => {
    // Number(null) is 0 and Number.isFinite(0) is true, which is how a
    // missing clock once read as 0:00 and a missing score as 0-0.
    assert.equal(app.weatherText({ roof: 'dome' }), 'Dome');
    assert.equal(app.weatherText({ roof: 'outdoors', temp: null,
      wind: null }), 'Outdoors');
    assert.equal(app.weatherText({ roof: 'outdoors', temp: 0, wind: 0 }),
      'Outdoors · 0°F · 0 mph wind');
    assert.equal(app.weatherText({}), null);
  });

test('rest days are shown for both sides, or not at all', () => {
  assert.equal(app.restText({ home_rest: 7, away_rest: 7 }),
    "Both on 7 days' rest");
  assert.equal(app.restText({ home_rest: 14, away_rest: 6 }),
    'Rest 6 away, 14 home');
  // One side alone says nothing about the other, so it says nothing.
  assert.equal(app.restText({ home_rest: 7 }), null);
});

test('a moneyline pair becomes a probability with the overround removed',
  () => {
    // -148/+124 is raw 0.5968 + 0.4464 = 1.0432, a 4.3% book.
    const home = app.marketProbability({ home_ml: -148, away_ml: 124 });
    assert.ok(Math.abs(home - 0.5721) < 5e-5, String(home));
    const away = app.marketProbability({ home_ml: 124, away_ml: -148 });
    assert.ok(Math.abs(home + away - 1) < 1e-12);
    // The same arithmetic as devig() in src/nfl/market.py.
    assert.equal(app.impliedProbability(-150), 0.6);
    assert.equal(app.impliedProbability(150), 0.4);
  });

test('one price alone is not a probability', () => {
  // A single price carries the vig with it and there is nothing to divide it
  // out by, so this is null rather than a number that looks like 59.7%.
  assert.equal(app.marketProbability({ home_ml: -148 }), null);
  assert.equal(app.marketProbability({ away_ml: 124 }), null);
  assert.equal(app.impliedProbability(40), null);
});

test('an against-the-spread record is measured against the vig, not 50%',
  () => {
    // The archive's own number: 3655-3513-201 is 51.0%, and a bettor needs
    // about 52.4% to clear a standard -110. Calling that a winning record
    // would be the single most misleading thing this page could say.
    const whole = { ats_wins: 3655, ats_losses: 3513, ats_pushes: 201 };
    assert.equal(app.atsText(whole), '3655-3513-201');
    assert.ok(Math.abs(app.atsRate(whole) - 0.50990) < 1e-4);
    assert.equal(app.atsBeatsVig(whole), false);
    assert.equal(app.atsBeatsVig({ ats_wins: 60, ats_losses: 40 }), true);
    assert.equal(app.ATS_BREAK_EVEN, 0.524);
  });

test('the market verdict compares the two scores measured on the same games',
  () => {
    const verdict = app.marketVerdict({ model_brier_priced: 0.21893,
      market_brier: 0.21101, priced_games: 5388 });
    assert.equal(verdict.better, 'market');
    assert.equal(verdict.games, 5388);
    assert.equal(app.marketVerdict({ model_brier_priced: 0.20,
      market_brier: 0.21, priced_games: 10 }).better, 'model');
    // No market score, no verdict — never a default of "the model won".
    assert.equal(app.marketVerdict({ model_brier_priced: 0.2 }), null);
  });

test('a seed in October leads its division rather than having won it', () => {
  const seed = { division_winner: true, bye: true };
  assert.equal(app.seedNote(seed, true), 'division winner · bye');
  assert.equal(app.seedNote(seed, false), 'division lead · bye');
  assert.equal(app.seedNote({ division_winner: false, bye: false }, true),
    '');
});

test('wins above expectation is signed, with a real minus sign', () => {
  assert.equal(app.signedWins(4.66), '+4.7');
  assert.equal(app.signedWins(-1.24), '−1.2');
  assert.equal(app.signedWins(0), '0.0');
  assert.equal(app.signedWins(null), '—');
});

// --- the page --------------------------------------------------------------

function teams() {
  return {
    [KC]: { code: 'KC', name: 'Kansas City Chiefs' },
    [BUF]: { code: 'BUF', name: 'Buffalo Bills' },
    [LA]: { code: 'LA', name: 'Los Angeles Rams' }
  };
}

function games() {
  return [
    { id: 'g1', date: '2026-09-13', at: '2026-09-13T17:00:00Z', week: 1,
      type: 'regular', home: KC, away: BUF, home_points: 24,
      away_points: 20, rateable: 1, venue: 'Arrowhead Stadium',
      roof: 'outdoors', surface: 'grass', temp: 67, wind: 8, home_rest: 7,
      away_rest: 7, spread: 3, total_line: 46, home_ml: -148,
      away_ml: 124, home_coach: 'Andy Reid', away_coach: 'Sean McDermott' },
    // Indoors, with no reading at all: the card must show no temperature
    // rather than a zero.
    { id: 'g2', date: '2026-09-20', at: '2026-09-20T17:00:00Z', week: 2,
      type: 'regular', home: LA, away: KC, home_points: null,
      away_points: null, rateable: 0, roof: 'dome', home_rest: 7,
      away_rest: 7 }
  ];
}

function board() {
  return [
    { team: KC, code: 'KC', conference: 'AFC', division: 'West',
      rating: 1648.4, rank: 1, wins: 1, losses: 0, ties: 0, rated_games: 1 },
    { team: BUF, code: 'BUF', conference: 'AFC', division: 'East',
      rating: 1591.6, rank: 2, wins: 0, losses: 1, ties: 0, rated_games: 1 },
    { team: LA, code: 'LA', conference: 'NFC', division: 'West',
      rating: 1500.0, rank: 3, wins: 0, losses: 0, ties: 0, rated_games: 0 }
  ];
}

function playoffField(overrides) {
  return Object.assign({
    season: 2026, format: '7 per conference, one bye', seeds: 7, byes: 1,
    division_winners: 4, complete: false, games_played: 93,
    games_scheduled: 272,
    conferences: [{
      conference: 'AFC',
      seeds: [
        { seed: 1, team: KC, division_winner: true, bye: true, wins: 1,
          games: 1, win_pct: 1, conference_win_pct: 1 },
        { seed: 2, team: BUF, division_winner: true, bye: false, wins: 0,
          games: 1, win_pct: 0, conference_win_pct: 0 }
      ],
      first_out: [{ team: LA, wins: 0, games: 0, win_pct: 0 }]
    }]
  }, overrides || {});
}

function history() {
  return {
    coaches: [
      { coach: REID, name: 'Andy Reid', seasons: 2, teams: [KC],
        first_season: 2025, last_season: 2026, games: 20, wins: 14,
        losses: 5, ties: 1, postseason_games: 3, postseason_wins: 2,
        expected_wins: 12.5, wins_above: 2 }
    ],
    coach_seasons: [
      { coach_key: REID, coach_name: 'Andy Reid', season: 2025,
        team_key: KC, games: 10, wins: 7, losses: 3, ties: 0,
        postseason_games: 1, postseason_wins: 1, expected_wins: 6.2,
        wins_above: 0.8, elo_change: 20.1 },
      { coach_key: REID, coach_name: 'Andy Reid', season: 2026,
        team_key: KC, games: 10, wins: 7, losses: 2, ties: 1,
        postseason_games: 2, postseason_wins: 1, expected_wins: 6.3,
        wins_above: 1.2, elo_change: 15.4 }
    ],
    track_record: {
      seasons: [
        { season: 2025, rated_games: 285, model_brier: 0.22117,
          model_accuracy: 0.64, priced_games: 285,
          model_brier_priced: 0.22117, market_brier: 0.21038,
          market_accuracy: 0.66, spread_games: 285, ats_wins: 139,
          ats_losses: 145, ats_pushes: 1, model_margin_mae: 10.3,
          market_margin_mae: 10.1 }
      ],
      totals: { seasons: 1, rated_games: 285, model_brier: 0.22117,
        model_accuracy: 0.64, priced_games: 285,
        model_brier_priced: 0.22117, market_brier: 0.21038,
        market_accuracy: 0.66, spread_games: 285, ats_wins: 139,
        ats_losses: 145, ats_pushes: 1, model_margin_mae: 10.3,
        market_margin_mae: 10.1 }
    },
    conferences: [{
      season: 2026, champion: KC, runner_up: LA,
      conference_champions: [KC, LA],
      conferences: [
        { conference: 'AFC', teams: 2, mean_rating: 1620, median_rating: 1620,
          best: KC, best_rating: 1648.4, divisions: [] },
        { conference: 'NFC', teams: 1, mean_rating: 1500, median_rating: 1500,
          best: LA, best_rating: 1500, divisions: [] }
      ]
    }]
  };
}

function stamp(text) {
  return require('node:crypto').createHash('sha256')
    .update(text).digest('hex').slice(0, 10);
}

function site(options) {
  const settings = options || {};
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'nfl-ctx-'));
  const data = path.join(root, 'data', 'nfl', 'seasons');
  fs.mkdirSync(data, { recursive: true });
  const payload = { season: 2026, board: board(), games: games() };
  if (settings.playoff !== null) {
    payload.playoff = settings.playoff || playoffField();
  }
  const seasonText = 'window.NFL_SEASONS=(window.NFL_SEASONS||{});'
    + 'window.NFL_SEASONS[2026]=' + JSON.stringify(payload) + ';\n';
  const teamsText = 'window.NFL_TEAMS=' + JSON.stringify(teams()) + ';\n';
  const historyText = 'window.NFL_HISTORY='
    + JSON.stringify(settings.history || history()) + ';\n';
  fs.writeFileSync(path.join(data, '2026.js'), seasonText);
  fs.writeFileSync(path.join(root, 'data', 'nfl', 'teams.js'), teamsText);
  fs.writeFileSync(path.join(root, 'data', 'nfl', 'history.js'), historyText);
  const files = { teams: 'teams.js?v=' + stamp(teamsText) };
  // A schema 1 export names no history file. The page still accepts it, so
  // the panels must say so rather than showing an empty table.
  if (!settings.withoutHistory) {
    files.history = 'history.js?v=' + stamp(historyText);
  }
  fs.writeFileSync(path.join(root, 'data', 'nfl', 'manifest.json'),
    JSON.stringify({
      schema_version: settings.withoutHistory ? 1 : 2,
      sport: 'nfl',
      generated_at: new Date(Date.now() - 20 * 60000).toISOString(),
      latest_season: 2026,
      model: { name: 'nfl_elo_v1', fitted_on: [1999, 2020],
        held_out: [2021, 2025], holdout_brier: 0.22372,
        holdout_games: 1424 },
      coverage: [{ season: 2026, teams: 32, rated_teams: 3, games: 2,
        finals: 1, complete: false, played_teams: 3 }],
      seasons: { 2026: 'seasons/2026.js?v=' + stamp(seasonText) },
      files: files
    }));
  for (const name of ['nfl.html', 'nfl_loader.js', 'nfl_app.js',
    'ncaambb_app.js']) {
    fs.copyFileSync(path.join(UI, name), path.join(root, name));
  }
  return root;
}

// 5 seconds, for the reason tests/nfl_page.test.cjs states at length: the
// number is not a claim about speed, only how long to wait before saying
// "never", and a sub-second budget failed on a busy CI runner against a
// page that was perfectly correct.
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

async function open(options) {
  const root = site(options);
  const file = path.join(root, 'nfl.html');
  const dom = new JSDOM(fs.readFileSync(file, 'utf8'), {
    url: 'file://' + file,
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
  await settle(() => dom.window.document.querySelector(
    '#cards .card, #refusal-list li'));
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

async function openTab(dom, tab, ready) {
  go(dom, tab);
  await settle(() => ready(dom.window.document));
  return dom.window.document;
}

test('a card carries the weather, the rest and the posted line', async () => {
  const dom = await open();
  click(dom, '#weeks button');            // week 1
  const card = dom.window.document.querySelector('#cards .card');
  const context = card.querySelector('.ctx').textContent;
  assert.match(context, /Outdoors/);
  assert.match(context, /67°F/);
  assert.match(context, /8 mph wind/);
  assert.match(context, /Both on 7 days' rest/);
  assert.match(context, /KC −3/);
  assert.match(context, /O\/U 46/);
  dom.window.close();
});

test('an indoor game with no reading shows no temperature at all',
  async () => {
    const dom = await open();
    const cards = [...dom.window.document.querySelectorAll('#cards .card')];
    const context = cards.map(c => c.textContent).join(' ');
    assert.match(context, /Dome/);
    // Not "0°F". A roof is the absence of a measurement, not a cold day.
    assert.doesNotMatch(context, /0°F/);
    dom.window.close();
  });

test('the standings tab shows the real field, seeded and with a bye marked',
  async () => {
    const dom = await open();
    const doc = await openTab(dom, 'standings',
      d => d.querySelector('#playoff .seeds tbody tr'));
    const text = doc.querySelector('#playoff').textContent;
    assert.match(text, /7 per conference, one bye/);
    const rows = [...doc.querySelectorAll('#playoff .seeds tbody tr')];
    assert.equal(rows[0].querySelector('.seed').textContent, '1');
    assert.match(rows[0].textContent, /Kansas City Chiefs/);
    assert.ok(rows[0].classList.contains('bye'));
    assert.ok(!rows[1].classList.contains('bye'));
    // The first team out is the number a reader actually wants, and it
    // carries a record rather than an empty cell.
    const out = rows.find(tr => tr.classList.contains('out'));
    assert.match(out.textContent, /Los Angeles Rams/);
    dom.window.close();
  });

test('a field computed mid-season says nobody has won anything yet',
  async () => {
    const dom = await open();
    const doc = await openTab(dom, 'standings',
      d => d.querySelector('#playoff .seeds tbody tr'));
    const text = doc.querySelector('#playoff').textContent;
    assert.match(text, /as things stand/);
    assert.match(text, /division lead/);
    assert.doesNotMatch(text, /division winner/);
    assert.match(text, /93 games into 272/);
    dom.window.close();
  });

test('a finished season calls its division winners winners', async () => {
  const dom = await open({ playoff: playoffField({ complete: true }) });
  const doc = await openTab(dom, 'standings',
    d => d.querySelector('#playoff .seeds tbody tr'));
  const text = doc.querySelector('#playoff').textContent;
  assert.match(text, /division winner/);
  assert.doesNotMatch(text, /as things stand/);
  dom.window.close();
});

test('a season the alignment cannot speak for says so, and draws no field',
  async () => {
    const dom = await open({ playoff: null });
    const doc = await openTab(dom, 'standings',
      d => d.querySelector('#playoff p'));
    assert.match(doc.querySelector('#playoff').textContent,
      /not computed for this season/);
    assert.equal(doc.querySelector('#playoff .seeds'), null);
    dom.window.close();
  });

test('the coaches tab ranks careers by wins above expectation', async () => {
  const dom = await open();
  const doc = await openTab(dom, 'coaches',
    d => d.querySelector('#coach-career tbody tr'));
  const row = doc.querySelector('#coach-career tbody tr');
  assert.match(row.textContent, /Andy Reid/);
  assert.match(row.textContent, /14-5-1/);     // a tie is not dropped
  assert.match(row.textContent, /\+2\.0/);
  // And the sentence that stops the number being read as a verdict.
  assert.match(doc.querySelector('#coaches').textContent,
    /not a measure of coaching/);
  dom.window.close();
});

test('a coach opens to his whole career, not only the loaded seasons',
  async () => {
    // The first version read the loaded season payloads, so Belichick's 24
    // years came out blank in October 2026. These rows come from the history
    // file, which carries every coach-season.
    const dom = await open();
    const doc = await openTab(dom, 'coaches',
      d => d.querySelector('#coach-career tbody tr'));
    click(dom, '#coach-career button.expand');
    const rows = [...doc.querySelectorAll('#coach-career tbody tr')];
    const text = rows.map(tr => tr.textContent).join('\n');
    assert.match(text, /2025 · KC/);
    assert.match(text, /2026 · KC/);
    assert.equal(doc.querySelector('#coach-career button.expand')
      .getAttribute('aria-expanded'), 'true');
    dom.window.close();
  });

test('the history tab states the market is ahead when the market is ahead',
  async () => {
    const dom = await open();
    const doc = await openTab(dom, 'history',
      d => d.querySelector('#track-record tbody tr'));
    const text = doc.querySelector('#track-record').textContent;
    assert.match(text, /the market is ahead/);
    assert.match(text, /0\.22117/);
    assert.match(text, /0\.21038/);
    // 139-145-1 is 48.9%, below the 52.4% that clears the vig.
    assert.match(text, /139-145-1/);
    assert.match(text, /below the 52\.4%/);
    dom.window.close();
  });

test('the conference history names each season champion', async () => {
  const dom = await open();
  const doc = await openTab(dom, 'history',
    d => d.querySelector('#conference-history tbody tr'));
  const row = doc.querySelector('#conference-history tbody tr');
  assert.match(row.textContent, /2026/);
  assert.match(row.textContent, /Kansas City Chiefs/);
  dom.window.close();
});

test('an export with no history file says so rather than showing nothing',
  async () => {
    // A schema 1 export, which this page still accepts. The panels must
    // explain the gap instead of rendering an empty table that reads as
    // "this coach has no record".
    const dom = await open({ withoutHistory: true });
    const doc = await openTab(dom, 'coaches',
      d => d.querySelector('#coach-career p'));
    assert.match(doc.querySelector('#coach-career').textContent,
      /does not carry the history file/);
    dom.window.close();
  });

test('a wide table scrolls itself rather than pushing the page sideways',
  async () => {
    // Nine columns do not fit a 430px phone, and a table that does not fit
    // takes the whole page with it unless it is inside a scroller.
    const dom = await open();
    const doc = await openTab(dom, 'history',
      d => d.querySelector('#track-record tbody tr'));
    const table = doc.querySelector('#track-record table');
    assert.ok(table.closest('.scroll'),
      'the track record table is not inside a horizontal scroller');
    // The legend explains the columns and must wrap, so it stays OUTSIDE.
    const legend = doc.querySelector('#track-record .legend');
    assert.ok(legend && !legend.closest('.scroll'));
    dom.window.close();
  });

test('the new sections are registered for the NFL and not for the others',
  () => {
    const leagues = require(path.join(UI, 'ncaambb_app.js'));
    assert.ok(leagues.hasSection('nfl', 'coaches'));
    assert.ok(leagues.hasSection('nfl', 'history'));
    // The people layer (export schema 3) gives the NFL the same `people`
    // section the other leagues use; coaches stay their own section.
    assert.ok(leagues.hasSection('nfl', 'people'));
    assert.ok(!leagues.hasSection('ncaambb', 'coaches'));
  });

test('a club in the playoff field opens its page', async () => {
  // Every panel that names a club links it (Austin, 2026-10-27). The seeds
  // table is the one in this file; the others are pinned where they live.
  const dom = await open();
  const doc = await openTab(dom, 'standings',
    d => d.querySelector('#playoff .seeds tbody tr'));
  const link = [...doc.querySelectorAll('#playoff .seeds button')]
    .find(node => node.textContent === 'Kansas City Chiefs');
  assert.ok(link, 'the name in the playoff field is not a link');
  link.dispatchEvent(new dom.window.Event('click', { bubbles: true }));
  await settle(() => doc.querySelector('.clubhead'));
  assert.match(doc.querySelector('.clubhead h3').textContent,
    /Kansas City Chiefs/);
  assert.equal(dom.window.location.hash, '#league=nfl&section=teams&team=KC');
  // The club page asks for history.js on its first render, and that load
  // resolves into a render(). Closing before it lands leaves the page
  // calling getElementById on a closed document.
  await settle(() => !/Loading/.test(
    doc.getElementById('teams-body').textContent));
  dom.window.close();
});
