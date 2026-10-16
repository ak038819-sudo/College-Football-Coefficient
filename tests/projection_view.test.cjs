// The two pages added for the rest-of-season projection and the track record.
//
// Both render straight from an export, so what can go wrong is presentation:
// a zero printed as a real chance, a small chance rounded away to nothing, the
// live projection shown under a finished season's year, or a page that renders
// at all when its export is absent. Each of those is a test here.
//
// Every helper these views call is lifted in for real rather than stubbed.
// Stubbing statCell() is how #71 silently replaced #72's and put an em dash in
// every player stat on the live site; a test that stubs the helper it depends
// on proves nothing about the page.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const shell = fs.readFileSync('ui/dashboard_shell.html', 'utf8');
function sourceOf(name) {
  const match = shell.match(new RegExp(`function ${name}\\([^)]*\\) \\{[\\s\\S]*?\\n\\}`));
  assert.ok(match, `missing ${name}`);
  return match[0];
}

const TEAMS = [
  {team: 'Ohio State', conference: 'Big Ten', wins: 5, losses: 1, ties: 0, projected_wins: 9.4,
   projected_elo: 1893,
   conference_title_pct: 15.4, playoff_pct: 100.0, bye_pct: 100.0, semifinal_pct: 74.0,
   final_pct: 55.3, champion_pct: 37.6},
  {team: 'Oregon', conference: 'Big Ten', wins: 2, losses: 2, ties: 0, projected_wins: 5.7,
   projected_elo: 1744,
   conference_title_pct: 0, playoff_pct: 100.0, bye_pct: 60.8, semifinal_pct: 40.0,
   final_pct: 19.7, champion_pct: 8.6},
  {team: 'Liberty', conference: 'Conference USA', wins: 4, losses: 1, ties: 0, projected_wins: 9.1,
   projected_elo: 1602,
   conference_title_pct: 73.8, playoff_pct: 0, bye_pct: 0, semifinal_pct: 0,
   final_pct: 0, champion_pct: 0},
  {team: 'Houston', conference: 'Big 12', wins: 4, losses: 1, ties: 0, projected_wins: 8.1,
   projected_elo: 1655,
   conference_title_pct: 0.04, playoff_pct: 0.02, bye_pct: 0, semifinal_pct: 0,
   final_pct: 0, champion_pct: 0},
];

function projectionContext(projection, year) {
  const ctx = {
    DATA: {projection},
    STATIC_MANIFEST: {},
    state: {year: year == null ? 2026 : year, section: 'playoff', subview: 'projection'},
    esc: s => String(s),
    teamLink: name => `<a data-team="${name}">${name}</a>`,
    logoMarkup: name => `<i data-logo="${name}"></i>`,
    routeHref: changes => '#' + JSON.stringify(changes),
  };
  vm.runInNewContext(
    'const PROJECTION = DATA.projection || null;\n' +
    // eloValue is lifted for real, not stubbed: a stubbed formatter is how
    // #71 silently replaced #72's and put an em dash in every player stat.
    sourceOf('eloValue') + '\n' +
    sourceOf('oddsPct') + '\n' + sourceOf('projectionRecord') + '\n' +
    sourceOf('renderConferenceRaces') + '\n' + sourceOf('renderProjection') + '\n' +
    'this.render = renderProjection; this.oddsPct = oddsPct; this.record = projectionRecord;',
    ctx);
  return ctx;
}

const PROJECTION = {season: 2026, n_sims: 10000, unplayed: 432, as_of: '2026-10-11',
                    ranking: 'Elo', teams: TEAMS};

test('a zero chance is blank and a tiny one is not rounded away', () => {
  const {oddsPct} = projectionContext(PROJECTION);
  // An exact zero is nothing to report, so it reads as nothing.
  assert.equal(oddsPct(0), '—');
  assert.equal(oddsPct(null), '—');
  assert.equal(oddsPct(undefined), '—');
  // A real but small chance must not render as "0.0%", which says the opposite.
  assert.equal(oddsPct(0.04), '<0.1%');
  assert.equal(oddsPct(0.09), '<0.1%');
  assert.equal(oddsPct(0.1), '0.1%');
  assert.equal(oddsPct(37.64), '37.6%');
  assert.equal(oddsPct(100), '100.0%');
});

test('a record shows ties only when there are ties', () => {
  const {record} = projectionContext(PROJECTION);
  assert.equal(record({wins: 5, losses: 1, ties: 0}), '5–1');
  assert.equal(record({wins: 5, losses: 1, ties: 2}), '5–1–2');
});

test('the projection table lists every team with a playoff chance, best first', () => {
  const html = projectionContext(PROJECTION).render();
  const order = [...html.matchAll(/data-team="([^"]+)"/g)].map(m => m[1]);
  // Ohio State and Oregon are both 100% to make it; the title chance breaks the
  // tie. Liberty has no playoff chance and belongs only to the races below.
  assert.deepEqual(order.slice(0, 3), ['Ohio State', 'Oregon', 'Houston']);
  assert.ok(html.includes('432 games'));
  assert.ok(html.includes('10,000'));
  assert.ok(html.includes('2026-10-11'));
  // Houston's 0.02% playoff chance survives as a chance, not as a zero.
  assert.ok(html.includes('&lt;0.1%') || html.includes('<0.1%'));
});

test('a team with no playoff chance still appears in its conference race', () => {
  const html = projectionContext(PROJECTION).render();
  const races = html.slice(html.indexOf('Conference Races'));
  assert.ok(races.includes('data-team="Liberty"'), 'Liberty leads Conference USA and is missing');
  assert.ok(races.includes('73.8%'));
  // Oregon has no chance of winning the Big Ten, so it is not in that race.
  assert.ok(!races.includes('data-team="Oregon"'));
});

test('conference races are grouped by conference and ordered within each', () => {
  const html = projectionContext(PROJECTION).render();
  const races = html.slice(html.indexOf('Conference Races'));
  const confs = [...races.matchAll(/data-logo="([^"]+)"/g)].map(m => m[1]);
  assert.deepEqual(confs, ['Big 12', 'Big Ten', 'Conference USA'], 'alphabetical, one each');
});

test('a finished season does not show the live projection', () => {
  // The bug this test exists for: the view read DATA.projection without
  // checking its season, so #season=2024 rendered 2026's numbers under a
  // paragraph that said "the 2026 season" on a page the reader had asked
  // about 2024.
  const html = projectionContext(PROJECTION, 2024).render();
  assert.ok(html.includes('The 2024 season has no games left to play'));
  assert.ok(!html.includes('data-team="Ohio State"'));
  assert.ok(html.includes('2026'), 'it should point at the season that is still being played');
});

test('no projection at all renders a page, not a crash or an empty table', () => {
  const html = projectionContext(null, 2025).render();
  assert.ok(html.includes('The 2025 season has no games left to play'));
  assert.ok(!html.includes('still being played'), 'there is no live season to point at');
});

// ---------------------------------------------------------------- track record

const ACCURACY = {
  headline: 'elo',
  fingerprint: 'a5286e1f384342f5',
  bins: 10,
  models: [
    {key: 'elo', label: 'Elo', blurb: 'The live engine.', headline: true, baseline: false,
     first_season: 1980, last_season: 2026, accuracy: 0.7253847, brier: 0.1793263,
     brier_skill: 0.2500149, log_loss: 0.5362875, calibration_error: 0.0249199,
     n: 30408, base_rate: 0.5964713},
    {key: 'baseline_home_rate', label: 'Always the home team', blurb: 'A constant.',
     headline: false, baseline: true, first_season: 1980, last_season: 2026,
     accuracy: 0.5964713, brier: 0.2391065, brier_skill: 0.0, log_loss: 0.6744165,
     calibration_error: 0.0, n: 30408, base_rate: 0.5964713},
  ],
  by_season: [
    {season: 2025, accuracy: 0.7252475, brier: 0.1839033, brier_skill: 0.2366573,
     base_rate: 0.5952970, n: 808},
    {season: 2026, accuracy: 0.7355623, brier: 0.1760055, brier_skill: 0.2430459,
     base_rate: 0.6322188, n: 329},
  ],
  calibration: [
    {lower: 0.0, upper: 0.1, predicted: 0.0657661, observed: 0.0796178, n: 942},
    {lower: 0.9, upper: 1.0, predicted: 0.9400000, observed: 0.9550000, n: 3071},
  ],
};

// The market comparison is lifted from the shell rather than stubbed, and the
// manifest carries it or not: the page has to render with a line archive and
// without one, since most checkouts have no lines loaded.
function accuracyContext(manifestAccuracy, manifestMarket) {
  const ctx = {STATIC_MANIFEST: {accuracy: manifestAccuracy, market: manifestMarket},
    esc: s => String(s)};
  vm.runInNewContext(
    'const ACCURACY = STATIC_MANIFEST.accuracy || null;\n' +
    sourceOf('accuracyPct') + '\n' + sourceOf('accuracyNum') + '\n' +
    sourceOf('renderCalibration') + '\n' + sourceOf('renderAccuracyBySeason') + '\n' +
    sourceOfConst('MARKET') + '\n' + sourceOf('marketVerdict') + '\n' +
    sourceOf('renderMarketSeasons') + '\n' + sourceOf('renderMarketComparison') + '\n' +
    sourceOf('renderAccuracy') + '\n' +
    'this.render = renderAccuracy; this.pct = accuracyPct; this.num = accuracyNum;' +
    'this.market = renderMarketComparison; this.verdict = marketVerdict;',
    ctx);
  return ctx;
}

test('track-record numbers are formatted as percentages and scores, never as raw floats', () => {
  const {pct, num} = accuracyContext(ACCURACY);
  assert.equal(pct(0.7253847), '72.5%');
  assert.equal(pct(0.0, 0), '0%');
  assert.equal(pct(null), '—');
  assert.equal(num(0.1793263), '0.1793');
  assert.equal(num(0.0249199, 3), '0.025');
  assert.equal(num(undefined), '—');
});

test('the headline states the model, the baseline and the Brier score together', () => {
  const html = accuracyContext(ACCURACY).render();
  assert.ok(html.includes('30,408'));
  assert.ok(html.includes('72.5%'), 'the accuracy');
  assert.ok(html.includes('59.6%'), 'the home-team baseline it has to beat');
  assert.ok(html.includes('0.1793'), 'the Brier score');
  assert.ok(html.includes('0.2391'), "the baseline's Brier score");
  assert.ok(html.includes('a5286e1f384342f5'), 'which run produced these');
});

test('the published model is marked and the baselines are dimmed', () => {
  const html = accuracyContext(ACCURACY).render();
  const published = html.slice(html.indexOf('>Elo'), html.indexOf('Always the home team'));
  assert.ok(published.includes('Published'));
  assert.ok(html.includes('class="row-dim"'), 'baselines are not presented as rival models');
});

test('the calibration table shows the gap between stated and observed', () => {
  const html = accuracyContext(ACCURACY).render();
  const table = html.slice(html.indexOf('When it says 70%'));
  assert.ok(table.includes('0%–10%'), 'bins are labelled by their edges');
  assert.ok(table.includes('6.6%') && table.includes('8.0%'));
  assert.ok(table.includes('+1.4'), 'observed minus predicted, signed');
});

test('seasons are listed newest first', () => {
  const html = accuracyContext(ACCURACY).render();
  const table = html.slice(html.indexOf('Season by season'));
  assert.ok(table.indexOf('2026') < table.indexOf('2025'));
});

test('no validation report renders a page that says so', () => {
  const html = accuracyContext(null).render();
  assert.ok(html.includes('No validation report'));
  assert.ok(!html.includes('72.5%'));
});

// ------------------------------------------------------------ section labels

// Every route the navigation module registers reaches three places that read
// SECTION_LABELS by the section's own name: the browser title, the aria-label
// on every scrollable table, and the "Back to ..." link. A section with a
// route and no label does not fail -- it renders the word "undefined" in all
// three. #section=accuracy shipped that way and the review caught it.
test('every registered section has a label', () => {
  const nav = require('../ui/navigation.js');
  const labels = {};
  vm.runInNewContext(sourceOfConst('SECTION_LABELS') + '\nthis.l = SECTION_LABELS;',
                     labels);
  const sections = nav.sections;
  assert.ok(sections.includes('accuracy'), 'the track-record route should be registered');
  const missing = sections.filter(s => !labels.l[s]);
  assert.deepEqual(missing, [], 'these sections would render "undefined" as their title');
  // And nothing is labelled that cannot be reached, which would be a label for
  // a page that does not exist.
  const orphan = Object.keys(labels.l).filter(s => !sections.includes(s));
  assert.deepEqual(orphan, []);
});

// The market comparison: the Track Record page's hardest claim, so the tests
// pin the direction of it. "Lower is better" twice over -- a verdict that read
// the comparison backwards would be the most confident wrong sentence on the
// site.
const MARKET = {
  overall: {
    games: 15234, first_season: 2013, last_season: 2026,
    model_mae: 11.42, market_mae: 10.88, model_rmse: 14.9, market_rmse: 14.1,
    model_bias: 0.31, market_bias: -0.12,
    model_brier: 0.1871, market_brier: 0.1802, scored: 15234,
    ats: {wins: 7400, losses: 7600, pushes: 234, decided: 15000, rate: 7400 / 15000,
          break_even_with_vig: 110 / 210},
    disagreements: {games: 1200, model_right: 560, market_right: 640,
                    model_rate: 560 / 1200},
  },
  by_season: [
    {season: 2013, games: 700, model_mae: 11.8, market_mae: 11.1,
     model_brier: 0.19, market_brier: 0.18,
     ats: {wins: 340, losses: 350, pushes: 10, decided: 690, rate: 340 / 690}},
    {season: 2014, games: 720, model_mae: 11.2, market_mae: 10.7,
     model_brier: 0.185, market_brier: 0.179,
     ats: {wins: 360, losses: 350, pushes: 10, decided: 710, rate: 360 / 710}},
  ],
  coverage: [{season: 2013, priced: 700, games: 760, share: 700 / 760},
             {season: 2014, priced: 720, games: 760, share: 720 / 760}],
  market_sigma: 13.6,
};

test('the market verdict names whichever side is CLOSER, not whichever number is bigger', () => {
  const {verdict} = accuracyContext(ACCURACY, MARKET);
  // Lower error is better, so a smaller model number means the model wins.
  assert.match(verdict(10.0, 11.0), /ratings are closer, by 1\.00/);
  assert.match(verdict(11.0, 10.0), /market is closer, by 1\.00/);
  assert.match(verdict(11.0, 11.02), /level/);
  assert.equal(verdict(null, 11.0), '');
});

test('the market panel states both margin errors, the spread record and the vig break-even', () => {
  const html = accuracyContext(ACCURACY, MARKET).render();
  assert.match(html, /Against the closing line/);
  assert.match(html, /15,234<\/b> games from 2013 to 2026/);
  assert.match(html, /ratings miss by <b>11\.42<\/b> points/);
  assert.match(html, /line misses by <b>10\.88<\/b>/);
  assert.match(html, /market is closer, by 0\.54/);
  assert.match(html, /7400–7600–234/);
  // 49.3% is a losing ticket, and the page has to say so rather than leaving
  // a number under 50% looking like a near miss.
  assert.match(html, /52\.4% to break even/);
  assert.match(html, /losing ticket/);
  assert.match(html, /test of a forecast rather than of a betting strategy/);
});

test('the market panel reports the straight-up disagreements and the curve it used', () => {
  const html = accuracyContext(ACCURACY, MARKET).render();
  assert.match(html, /1,200<\/b> games where the two disagreed/);
  assert.match(html, /46\.7%<\/b> of them correctly/);
  assert.match(html, /13\.6 points wide/);
});

test('the market season table carries how much of each season was priced', () => {
  const html = accuracyContext(ACCURACY, MARKET).render();
  const seasons = html.slice(html.indexOf('season by season'));
  // Newest first, and the coverage share beside the count.
  assert.ok(seasons.indexOf('2014') < seasons.indexOf('2013'));
  assert.match(seasons, /92%/);     // 700 of 760
});

test('the season note says whether a thinly priced season is in the table', () => {
  // The fixture's seasons are priced 92%, so the caution has to appear...
  const thin = accuracyContext(ACCURACY, MARKET).render();
  assert.match(thin, /Treat the 2 seasons under 95% as indicative/);
  assert.doesNotMatch(thin, /a number on every game/);
  // ...and must NOT appear when the archive prices every game, which is what
  // 2013-2026 actually looks like. A hardcoded sentence either way would be
  // wrong for one of these two archives.
  const full = {
    ...MARKET,
    coverage: MARKET.coverage.map(c => ({...c, priced: c.games, share: 1})),
  };
  const whole = accuracyContext(ACCURACY, full).render();
  assert.match(whole, /a number on every game of every season/);
  assert.doesNotMatch(whole, /as indicative/);
  assert.match(whole, /100%/);
});

test('no line archive means no market section at all, not an empty one', () => {
  const ctx = accuracyContext(ACCURACY, undefined);
  assert.equal(ctx.market(), '');
  const html = ctx.render();
  assert.doesNotMatch(html, /Against the closing line/);
  assert.match(html, /How often the model is right/);
});

test('a line archive with no priced games is still no section', () => {
  assert.equal(accuracyContext(ACCURACY, {overall: {games: 0}}).market(), '');
});

test('the market section survives a build with no validation report', () => {
  const html = accuracyContext(null, MARKET).render();
  assert.match(html, /No validation report/);
  assert.match(html, /Against the closing line/);
});

function sourceOfConst(name) {
  const match = shell.match(new RegExp(`^const ${name} = .*;$`, 'm'));
  assert.ok(match, `missing ${name}`);
  return match[0];
}

test('the projection table shows the rating the field is seeded on', () => {
  const html = projectionContext(PROJECTION).render();
  // Whole integers, like every other Elo on the site, and the header names
  // the metric from the payload rather than hardcoding it.
  assert.ok(html.includes('Proj. Elo'));
  assert.ok(html.includes('1893') && html.includes('1744'));
  assert.ok(!html.includes('1893.0'), 'an Elo rating is displayed as a whole number');
});
