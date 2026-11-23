/* The Matchup table's Efficiency group: CFBD's measurement of each offence.
 *
 * Lifted out of the production shell rather than reimplemented, which is the
 * only version of this test worth having -- a copy of the helper would keep
 * passing after the shell's own copy broke.
 *
 * The export side is tests/test_game_efficiency.py.
 */
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const shell = fs.readFileSync('ui/dashboard_shell.html', 'utf8');

// Two declarations of one name silently replace each other in this file and a
// test that lifts BY NAME would never notice, so count the matches first.
const declarations = shell.match(/function gameEfficiencyGroup\(/g) || [];
assert.equal(declarations.length, 1,
  'exactly one gameEfficiencyGroup must be declared in the shell');

const source = shell.match(/function gameEfficiencyGroup\(detail\) \{[^]*?\n\}/)?.[0];
assert.ok(source, 'the Efficiency group helper must exist in the production shell');
const context = {};
vm.runInNewContext(source + '\nthis.group = gameEfficiencyGroup;', context);
const group = context.group;

const detail = (over = {}) => Object.assign({
  home_plays: 64, home_ppa: 0.412, home_explosiveness: 1.771, home_success_rate: 0.4762,
  away_plays: 51, away_ppa: -0.108, away_explosiveness: 1.204, away_success_rate: 0.3750
}, over);

// node's deepStrictEqual rejects a vm-context array against an identical host
// array -- different realms, same structure -- so everything crossing back out
// of the context is copied into a host array first.
const cellsFor = (d, side) => Array.from(group(d)[2](side));
const labelsOf = d => Array.from(group(d)[1]).map(c => c[0]);

test('a game with no efficiency gets no group at all, not a row of dashes', () => {
  assert.equal(group(null), null);
  assert.equal(group({}), null);
  assert.equal(group({ home_ppa: null, away_ppa: null, home_plays: null, away_plays: null,
    home_success_rate: null, away_success_rate: null, home_explosiveness: null,
    away_explosiveness: null }), null);
  // One side alone is still a measurement and still earns the group.
  assert.ok(group({ home_ppa: 0.4 }));
  assert.ok(group({ away_plays: 51 }));
});

test('four columns, in the order a reader needs them', () => {
  const [name, columns] = group(detail());
  assert.equal(name, 'Efficiency');
  assert.equal(Array.from(columns).length, 4);
  assert.deepEqual(labelsOf(detail()), ['Plays', 'Success Rate', 'PPA', 'Explosiveness']);
  // Plays comes first because it is the sample the three averages rest on.
  assert.match(columns[0][1], /garbage time excluded/i);
  for (const [label, tip] of columns) assert.ok(tip && tip.length > 20, label);
});

test('each side reads its own offence', () => {
  const d = detail();
  assert.deepEqual(cellsFor(d, 'home'), ['64', '47.6%', '0.41', '1.77']);
  // A real minus sign (U+2212), not a hyphen: the xSRDiff columns beside it
  // use one, and the two side by side read as a typo.
  assert.deepEqual(cellsFor(d, 'away'), ['51', '37.5%', '\u22120.11', '1.20']);
  assert.ok(!cellsFor(d, 'away')[2].includes('-'), 'no ASCII hyphen');
});

test('PPA keeps two decimals, because one would merge two different offences', () => {
  // 0.44 and 0.51 are a good offence and a very good one; both are "0.5" at a
  // single decimal, and PPA's whole range is about -0.5 to 1.5 a play.
  const a = cellsFor(detail({ home_ppa: 0.4449 }), 'home')[2];
  const b = cellsFor(detail({ home_ppa: 0.5051 }), 'home')[2];
  assert.equal(a, '0.44');
  assert.equal(b, '0.51');
  assert.notEqual(a, b);
});

test('a missing number is N/A and never a zero', () => {
  // `Number(null)` is 0, which this project has shipped as a real measurement
  // three times. An offence that the provider did not report is not an offence
  // that ran no plays and gained nothing.
  const cells = cellsFor(detail({
    away_plays: null, away_ppa: null, away_explosiveness: null, away_success_rate: null
  }), 'away');
  assert.deepEqual(cells, ['N/A', 'N/A', 'N/A', 'N/A']);
  // And the other side is untouched.
  assert.deepEqual(cellsFor(detail({ away_ppa: null }), 'home'),
    ['64', '47.6%', '0.41', '1.77']);
});

test('a metric neither side has is left out, not shown as a column of N/A', () => {
  // A season the provider measured Success Rate for and nothing else used to
  // widen this table by three columns saying nothing. The group already drops
  // itself when a game has no efficiency at all; each column follows the rule.
  const d = detail({
    home_plays: null, away_plays: null, home_ppa: null, away_ppa: null,
    home_explosiveness: null, away_explosiveness: null
  });
  assert.deepEqual(labelsOf(d), ['Success Rate']);
  assert.deepEqual(cellsFor(d, 'home'), ['47.6%']);
  assert.ok(!cellsFor(d, 'home').includes('N/A'));
  // One side measured and the other not IS worth an N/A: the column is real
  // and this game's other offence is the gap.
  const half = detail({ away_ppa: null });
  assert.deepEqual(labelsOf(half), ['Plays', 'Success Rate', 'PPA', 'Explosiveness']);
  assert.equal(cellsFor(half, 'away')[2], 'N/A');
});

test('a real zero prints as a zero', () => {
  assert.deepEqual(cellsFor(detail({
    home_plays: 0, home_ppa: 0, home_explosiveness: 0, home_success_rate: 0
  }), 'home'), ['0', '0.0%', '0.00', '0.00']);
});

test('a four-figure play count is grouped, like every other count on the site', () => {
  // Not expected per game, but the same helper is the obvious place to put a
  // season total later, and '1234' would be the one unformatted number here.
  assert.equal(cellsFor(detail({ home_plays: 1234 }), 'home')[0],
    Number(1234).toLocaleString());
});

test('the model layer is NOT in this group', () => {
  // SRDiff, xSRDiff and SR+ are derived by the Elo engine from these numbers.
  // Printing them alongside as if the provider reported them is the confusion
  // this split exists to prevent.
  assert.equal(group(detail({ home_sr_diff: 0.12 }))[0], 'Efficiency');
  const labels = labelsOf(detail({
    home_sr_diff: 0.12, home_xsr_diff: 0.03, home_sr_plus: 0.09
  })).join(' ');
  assert.doesNotMatch(labels, /SRDiff|SR\+|xSR/);
});

test('the xSRDiff layer keeps a group of its own in the shell', () => {
  // The companion half of the same change: the three model columns moved out of
  // "Efficiency" into a group named for the model that produces them.
  assert.match(shell, /groups\.push\(\['xSRDiff layer'/);
  assert.match(shell, /\['SRDiff',/);
  assert.match(shell, /\['SR\+',/);
});
