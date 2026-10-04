// A coach's career against the Elo expectation, as the page computes it. The
// functions are pulled out of the shell and driven, rather than the shell's
// source text being asserted on.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const shell = fs.readFileSync('ui/dashboard_shell.html', 'utf8');

function load(names) {
  const source = names.map(name =>
    shell.match(new RegExp('function ' + name + '\\([^]*?\\n\\}'))[0]).join('\n');
  const context = {
    esc: value => String(value).replaceAll('&', '&amp;').replaceAll('<', '&lt;'),
    teamCellById: id => 'TEAM' + id,
  };
  vm.runInNewContext(source + '\nthis.__fns = {' + names.join(',') + '};', context);
  return context.__fns;
}

test('a career total counts the measured seasons and says how many there were', () => {
  const {coachExpectation} = load(['coachExpectation']);
  const exp = coachExpectation([
    {season_year: 2019, expected_wins: 9.2, wins_above_expected: 1.8},
    {season_year: 2020, expected_wins: 10.4, wins_above_expected: 2.6},
  ]);
  assert.equal(exp.seasons, 2);
  assert.equal(exp.unmeasured, 0);
  assert.ok(Math.abs(exp.total - 4.4) < 1e-9);
  assert.ok(Math.abs(exp.perSeason - 2.2) < 1e-9);
  assert.ok(Math.abs(exp.expected - 19.6) < 1e-9);
});

test('a season nobody can attribute is left out of the total, not counted as zero', () => {
  const {coachExpectation} = load(['coachExpectation']);
  const exp = coachExpectation([
    {season_year: 2019, expected_wins: 9.2, wins_above_expected: 1.8},
    {season_year: 2020, unmeasured: 'two head coaches this season'},
  ]);
  // Counting the shared season as zero would drag the average toward nothing
  // for a reason that has nothing to do with the coach.
  assert.equal(exp.seasons, 1);
  assert.equal(exp.unmeasured, 1);
  assert.ok(Math.abs(exp.perSeason - 1.8) < 1e-9);
});

test('a coach with nothing measured gets no number at all', () => {
  const {coachExpectation} = load(['coachExpectation']);
  // Greg Knox: three interim seasons, every one shared with another head coach.
  assert.equal(coachExpectation([{unmeasured: 'two head coaches this season'},
                                 {unmeasured: 'two head coaches this season'}]), null);
  assert.equal(coachExpectation([]), null);
  assert.equal(coachExpectation(undefined), null);
});

test('the shell has exactly one signed(), the one the team pages already use', () => {
  // A second definition of it, added here for the coach page, silently replaced
  // the existing helper for all 18 of its call sites -- the team page's own
  // "Wins vs. expectation" among them -- because a later function declaration
  // wins. The measure is the same one; it must use the same helper.
  const definitions = shell.match(/function signed\(/g) || [];
  assert.equal(definitions.length, 1);
  const {signed} = load(['signed']);
  assert.equal(signed(1.84, 1), '+1.8');
  assert.equal(signed(-0.62, 1), '\u22120.6');
  assert.equal(signed(0, 1), '\u00b10.0');
});

test('the season table shows the expectation, and names a season it cannot measure', () => {
  const {coachPanels} = load(['coachPanels', 'coachRecord', 'coachExpectation', 'signed',
                              'teamStat']);
  const html = coachPanels({display_name: 'The Coach', tenures: [
    {season_year: 2019, team_id: 7, wins: 11, losses: 2, ties: 0,
     expected_wins: 9.24, wins_above_expected: 1.76},
    {season_year: 2020, team_id: 7, wins: 4, losses: 8, ties: 0,
     unmeasured: 'two head coaches this season, and the feed does not say which games'},
  ]}, 1);
  assert.match(html, /Wins vs expectation/);
  assert.match(html, /\+1\.8/);
  assert.match(html, /9\.2/);
  assert.match(html, /not measured/);
  assert.match(html, /two head coaches this season/, 'the reason is on the page, not implied');
  // The career line counts one season, not two.
  assert.match(html, /1 measured season/);
});
