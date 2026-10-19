// The page's live win probability must be the same model as src/win_probability.py.
//
// Two models of the same thing in two languages is how a scorebug ends up
// disagreeing with the archive, so these tests pin the page's copy against
// numbers produced by the Python one (recorded below, not recomputed here) and
// against the properties a viewer would notice: a lead that gets safer, a
// kickoff that matches the pregame chance already on screen, and no number at
// all when the feed cannot support one.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const shell = fs.readFileSync('ui/dashboard_shell.html', 'utf8');
const NAMES = ['liveWinParams', 'liveOddsText', 'erf', 'normalCdf', 'liveSecondsRemaining', 'livePeriodClock',
  'liveEloGap', 'liveWinProbability', 'liveChanceFor', 'probPct'];
// The real helpers, lifted by name. Stubbing one of these would test the stub:
// `liveWinProbability` is almost entirely its calls into the others.
const sources = NAMES.map(name => {
  const source = shell.match(new RegExp('^function ' + name + '\\([^)]*\\) \\{[\\s\\S]*?\\n\\}', 'm'))?.[0];
  assert.ok(source, 'missing ' + name + ' in the shell');
  return source;
});
const constants = shell.match(/^const LIVE_PERIOD_SECONDS[\s\S]*?^const LIVE_REGULATION_SECONDS[^\n]*\n/m)?.[0];
assert.ok(constants, 'missing the live regulation constants');

// The production fit (config/model_config.json -> model_params.live_win_probability).
const PARAMS = {
  points_per_elo: 0.052627, sigma: 16.651, time_exponent: 0.5,
  overtime_home_win: 0.5099, tie_half_width: 1.142
};

function load(params = PARAMS, scale = 400) {
  const ctx = {STATIC_MANIFEST: {model_params: {elo: {scale}, live_win_probability: params}}};
  vm.runInNewContext(constants + sources.join('\n') +
    '\nthis.api = {liveWinProbability, liveChanceFor, liveSecondsRemaining, liveEloGap, normalCdf};', ctx);
  return ctx.api;
}

const game = (fields) => ({status: 'in_progress', period: 2, clock: '7:30',
  home: {points: 14}, away: {points: 14}, ...fields});

test('at kickoff the live model lands on the pregame chance beside it', () => {
  // Not identical, and it cannot be: the rating engine's pregame chance is a
  // logistic on the Elo gap, while this is a normal on the margin that gap
  // implies. The two shapes separate slightly in the tails. What matters on a
  // scorebug is that a game with no score on the board does not show a live
  // number that argues with the pregame one.
  const {liveWinProbability} = load();
  for (const pregame of [0.2, 0.5, 0.62, 0.87]) {
    const atKickoff = liveWinProbability(pregame, 0, 3600);
    assert.ok(Math.abs(atKickoff - pregame) < 0.03,
      `pregame ${pregame} became ${atKickoff} at kickoff`);
  }
});

test('the page agrees with src/win_probability.py on states it has computed', () => {
  // Produced by the Python model at the production parameters, with the Elo gap
  // recovered from the pregame probability exactly as the page does it.
  // pregame p_home, margin, seconds left, expected probability
  const cases = [
    [0.5, 0, 3600, 0.5005],
    [0.5, 7, 1800, 0.7236],
    [0.5, -7, 900, 0.2032],
    [0.5, 3, 120, 0.8233],
    [0.7363, -10, 900, 0.1819],
    [0.7363, 0, 60, 0.5292],
    [0.5, 0, 0, 0.5099],
    [0.5, 1, 0, 1],
    [0.5, -1, 0, 0],
  ];
  const {liveWinProbability} = load();
  for (const [pregame, margin, seconds, expected] of cases) {
    const value = liveWinProbability(pregame, margin, seconds);
    assert.ok(Math.abs(value - expected) < 0.002,
      `p=${pregame} margin=${margin} left=${seconds}: page ${value}, python ${expected}`);
  }
});

test('a lead gets safer as the clock runs down and a deficit gets worse', () => {
  const {liveWinProbability} = load();
  const leading = [2700, 1800, 900, 120].map(left => liveWinProbability(0.5, 7, left));
  assert.deepEqual(Array.from(leading), Array.from(leading).sort((a, b) => a - b));
  const trailing = [2700, 1800, 900, 120].map(left => liveWinProbability(0.5, -7, left));
  assert.deepEqual(Array.from(trailing), Array.from(trailing).sort((a, b) => b - a));
});

test('the pregame rating fades out as the game goes on', () => {
  const {liveWinProbability} = load();
  const favourite = [2700, 900, 60].map(left => liveWinProbability(0.85, -3, left));
  assert.ok(favourite[0] > favourite[1] && favourite[1] > favourite[2]);
  assert.equal(liveWinProbability(0.85, -3, 0), 0);
});

test('the clock is read in the shapes the scoreboard publishes', () => {
  const {liveSecondsRemaining} = load();
  assert.equal(liveSecondsRemaining(1, '15:00'), 3600);
  assert.equal(liveSecondsRemaining(3, '7:30'), 1350);
  assert.equal(liveSecondsRemaining(4, '0:00'), 0);
  assert.equal(liveSecondsRemaining(4, 450), 450);
  assert.equal(liveSecondsRemaining(5, '15:00'), 0, 'overtime has no regulation left');
  // Not only the first overtime, and not only a full clock: the period test has
  // to short-circuit, because the arithmetic below it goes negative past the
  // fourth quarter and happens to land on zero for one of these.
  assert.equal(liveSecondsRemaining(5, '3:00'), 0);
  assert.equal(liveSecondsRemaining(6, '10:00'), 0);
  assert.equal(liveSecondsRemaining(1, '20:00'), 3600, 'a clock past the period cannot widen it');
  for (const [period, clock] of [[null, '15:00'], [0, '15:00'], [1, null], [1, 'half'],
    [1, '15:75'], [1, ''], ['Q1', '15:00'], [1.5, '15:00']]) {
    assert.equal(liveSecondsRemaining(period, clock), null,
      `period ${period} clock ${clock} should give no time`);
  }
});

test('a scheduled game, a pregame 0-0 row and a clockless game get no live number', () => {
  const {liveChanceFor} = load();
  assert.equal(liveChanceFor(game({status: 'scheduled', home: {points: null}, away: {points: null}}), 0.6), null);
  assert.equal(liveChanceFor(game({status: 'scheduled', home: {points: 0}, away: {points: 0}}), 0.6), null);
  assert.equal(liveChanceFor(game({period: null, clock: null}), 0.6), null);
  assert.equal(liveChanceFor(game({clock: 'halftime'}), 0.6), null);
  assert.equal(liveChanceFor(game({home: {points: '14'}}), 0.6), null);
  assert.equal(liveChanceFor(null, 0.6), null);
});

test('a game in overtime gets no live number, so the card keeps the pregame one', () => {
  // Regulation seconds remaining is zero all through overtime, which reads as a
  // finished game: a team that has just scored in the first overtime would show
  // as certain while its opponent still has a possession coming.
  const {liveChanceFor} = load();
  for (const period of [5, 6, '5']) {
    assert.equal(liveChanceFor(game({period, clock: '0:00', home: {points: 31}, away: {points: 24}}), 0.5), null);
    assert.equal(liveChanceFor(game({period, clock: null, home: {points: 31}, away: {points: 24}}), 0.5), null);
  }
  // The fourth quarter running out is not overtime: a lead there is a win and a
  // tie is worth the overtime term.
  assert.equal(liveChanceFor(game({period: 4, clock: '0:00', home: {points: 31}, away: {points: 24}}), 0.5).home, 100);
  assert.equal(liveChanceFor(game({period: 4, clock: '0:00', home: {points: 24}, away: {points: 24}}), 0.5).home, 51);
  // A final is decided by its score however many overtimes it took.
  assert.equal(liveChanceFor(game({status: 'completed', period: 5, clock: '0:00',
    home: {points: 31}, away: {points: 24}}), 0.5).home, 100);
});

test('a live game gets a whole-percent pair that adds to a hundred', () => {
  const {liveChanceFor} = load();
  const chance = liveChanceFor(game({home: {points: 21}, away: {points: 7}, period: 3, clock: '5:00'}), 0.5);
  assert.ok(chance.home > 85 && chance.home < 100);
  assert.equal(chance.home + chance.away, 100);
  assert.equal(chance.home, Math.round(chance.home));
});

test('a completed game is decided by its score, whatever the ratings said', () => {
  const {liveChanceFor} = load();
  const final = fields => liveChanceFor(game({status: 'completed', period: null, clock: null, ...fields}), 0.9);
  assert.equal(final({home: {points: 10}, away: {points: 31}}).home, 0);
  assert.equal(final({home: {points: 31}, away: {points: 10}}).home, 100);
  assert.equal(final({home: {points: 24}, away: {points: 24}}).home, 51, 'a level final goes to overtime');
});

test('an incomplete or impossible parameter payload produces no number at all', () => {
  const broken = [
    null, {}, {...PARAMS, sigma: 0}, {...PARAMS, sigma: -4}, {...PARAMS, time_exponent: 0},
    {...PARAMS, overtime_home_win: 1.4}, {...PARAMS, tie_half_width: -1},
    {...PARAMS, points_per_elo: null}, {...PARAMS, sigma: 'wide'},
  ];
  for (const params of broken) {
    assert.equal(load(params).liveWinProbability(0.6, 7, 900), null,
      `params ${JSON.stringify(params)} should give no live number`);
  }
  // A manifest with no Elo scale cannot place the pregame chance on the Elo
  // axis, so there is nothing to tilt the walk with.
  assert.equal(load(PARAMS, 0).liveWinProbability(0.6, 7, 900), null);
  // A certainty cannot be inverted into a finite rating gap.
  assert.equal(load().liveWinProbability(1, 7, 900), null);
  assert.equal(load().liveWinProbability(0, 7, 900), null);
});

test('the normal CDF is accurate enough for a percentage', () => {
  const {normalCdf} = load();
  const known = [[-3, 0.0013499], [-1.96, 0.0249979], [-1, 0.1586553], [0, 0.5],
    [1, 0.8413447], [1.96, 0.9750021], [3, 0.9986501]];
  for (const [z, expected] of known) {
    assert.ok(Math.abs(normalCdf(z) - expected) < 1e-6, `normalCdf(${z}) = ${normalCdf(z)}`);
  }
});

test('the Elo gap is recovered from the probability the page already shows', () => {
  const {liveEloGap} = load();
  // The rating engine's own logistic, inverted: 400 points of scale is a
  // ten-to-one gap at p = 10/11.
  assert.ok(Math.abs(liveEloGap(10 / 11) - 400) < 1e-6);
  assert.ok(Math.abs(liveEloGap(0.5)) < 1e-9);
  assert.ok(Math.abs(liveEloGap(1 / 11) + 400) < 1e-6);
});

test('the scorebug prints the live chance while a game is on, and the pregame one otherwise', () => {
  const liveTeam = shell.match(/^function liveTeam\([^)]*\) \{[\s\S]*?\n\}/m)?.[0];
  assert.ok(liveTeam, 'missing liveTeam');
  const render = (status, live) => {
    const ctx = {
      liveTeamId: () => ({id: 1, name: 'Alpha'}), CURRENT_ELO: new Map(),
      livePrediction: () => ({homeElo: 1600, awayElo: 1500, pHome: 0.64}),
      pickDatedLogoEntry: () => null, TEAM_BRAND_COLORS: {}, logoMarkup: () => '',
      teamAnchor: (name, inner) => inner, esc: value => String(value),
      seasonRecord: () => '', liveSeason: () => 2026, liveChange: () => '',
      DATED_LOGOS: {}, NON_FBS_BY_ESPN_ID: new Map()
    };
    // The real formatter, not a stub: what the two ends of the scale print is
    // part of what this test is checking.
    const oddsText = shell.match(/^function liveOddsText\([^)]*\) \{[\s\S]*?\n\}/m)?.[0];
    assert.ok(oddsText, 'missing liveOddsText');
    // The real artwork registry and its style helper, which liveTeam reads for
    // every row it draws.
    const panels = shell.match(/^const TEAM_PANELS = Object\.freeze\(\{[\s\S]*?^\}\);/m)?.[0];
    assert.ok(panels, 'missing TEAM_PANELS');
    const panelStyle = shell.match(/^function teamPanelStyle\([^)]*\) \{[\s\S]*?\n\}/m)?.[0];
    assert.ok(panelStyle, 'missing teamPanelStyle');
    // liveLogoName is lifted, not stubbed: liveTeam calls it, and a stub here
    // would let this file pass while the page threw.
    const logoName = shell.match(/^function liveLogoName\([^)]*\) \{[\s\S]*?\n\}/m)?.[0];
    assert.ok(logoName, 'missing liveLogoName');
    vm.runInNewContext(panels + '\n' + panelStyle + '\n' + oddsText + '\n' + logoName + '\n' + liveTeam +
      '\nthis.render = liveTeam;', ctx);
    const g = {status, home: {points: 21, name: 'Alpha'}, away: {points: 17, name: 'Bravo'}};
    return ctx.render(g.home, g, {home: 64, away: 36}, live);
  };
  const playing = render('in_progress', {home: 88, away: 12});
  assert.match(playing, /88% win/);
  // A game still being played is never certain, at either end.
  assert.match(render('in_progress', {home: 100, away: 0}), />99% win/);
  assert.match(render('in_progress', {home: 0, away: 100}), /&lt;1% win|<1% win/);
  assert.doesNotMatch(playing, /pregame<\/span>/, 'the pregame line is replaced, not stacked');
  assert.match(playing, /64% pregame">/, 'the pregame chance stays in the tooltip');
  // A final score needs no probability, and a feed with no clock has no live
  // number to show, so both keep the pregame line.
  assert.match(render('completed', {home: 100, away: 0}), /64% pregame<\/span>/);
  assert.match(render('in_progress', null), /64% pregame<\/span>/);
});
