/* The game page every league shares.
 *
 * These drive ui/game_page.js directly, which is the point of the module
 * being pure: four leagues answer the same questions -- what the matchup
 * table says, when a line score may be drawn, how a probability becomes two
 * percentages -- and each one used to answer them in its own page.
 */
const test = require('node:test');
const assert = require('node:assert/strict');
const gp = require('../ui/game_page.js');

/* ---- a missing number is not a zero ---------------------------------- */

test('null is read as missing, not coerced to zero', () => {
  // `Number(null)` is 0 and `Number('')` is 0. This project has shipped that
  // bug three times -- a missing clock reading 0:00, a missing score 0-0 --
  // so the null check has to come before the coercion, every time.
  for (const empty of [null, undefined, '']) {
    assert.equal(gp.num(empty), null, String(empty));
    assert.equal(gp.eloValue(empty), '—');
    assert.equal(gp.signed(empty), '—');
    assert.equal(gp.percents(empty), null);
  }
  // A real zero is still a real zero.
  assert.equal(gp.num(0), 0);
  assert.equal(gp.eloValue(0), '0');
  assert.equal(gp.signed(0), '+0.0');
});

test('an Elo rating rounds to a whole number and a change keeps a decimal',
  () => {
    assert.equal(gp.eloValue(1577.8), '1578');
    assert.equal(gp.eloValue(1577.4), '1577');
    // Math.round breaks a .5 tie upward, which for a rating is arbitrary and
    // harmless -- it is a display rule, and nothing downstream reads it.
    assert.equal(gp.eloValue(1577.5), '1578');
    // A 0.4 move rounded to zero would read as a game that did not count.
    assert.equal(gp.signed(0.4), '+0.4');
    assert.equal(gp.signed(-11.02), '−11.0');
    assert.equal(gp.signed(-11.02, 2), '−11.02');
  });

test('the two win chances add to a hundred', () => {
  // Rounding each side on its own showed 51% against 48% on a game page,
  // which reads as a third outcome nobody modelled.
  for (const p of [0.2925, 0.505, 0.5049, 1 / 3, 0.666, 0.9657]) {
    const pair = gp.percents(p);
    assert.equal(Number(pair.home) + Number(pair.away), 100, String(p));
  }
  assert.deepEqual(gp.percents(0.2925), { home: '29.3', away: '70.7' });
  assert.deepEqual(gp.percents(0), { home: '0.0', away: '100.0' });
  assert.deepEqual(gp.percents(1), { home: '100.0', away: '0.0' });
  // Not a probability at all.
  assert.equal(gp.percents(1.4), null);
  assert.equal(gp.percents(-0.1), null);
  assert.equal(gp.percents('nonsense'), null);
});

/* ---- the Elo group --------------------------------------------------- */

const played = {
  home_elo: 1370.2, away_elo: 1270.1, home_wp: 0.64,
  home_elo_post: 1374.2, away_elo_post: 1266.1, elo_change: 4.0
};

test('a game the model learned from carries four columns', () => {
  const group = gp.eloGroup(played, { completed: true });
  assert.deepEqual(group.columns.map(pair => pair[0]),
    ['Before', 'Win chance', 'After', 'Δ Elo']);
  assert.deepEqual(group.cells('home'), ['1370', '64.0%', '1374', '+4.0']);
  // The stored change is the HOME side's; the away side moved by exactly as
  // much the other way, which is what makes the chain add up.
  assert.deepEqual(group.cells('away'), ['1270', '36.0%', '1266', '−4.0']);
});

test('a game not yet played carries two, and no postgame rating', () => {
  const group = gp.eloGroup({ home_elo: 1600, away_elo: 1500, home_wp: 0.7 },
    { completed: false });
  assert.deepEqual(group.columns.map(pair => pair[0]),
    ['Before', 'Win chance']);
  assert.deepEqual(group.cells('home'), ['1600', '70.0%']);
});

test('a live number is named "Now", not "Before"', () => {
  // Giving a moving number and a measurement the same heading is how a
  // reader comes to believe the model predicted something it never did.
  const live = gp.eloGroup(
    { home_elo: 1839.1, away_elo: 1359.1, home_wp: 0.9657, wp_live: 1 },
    { completed: false });
  assert.equal(live.columns[0][0], 'Now');
  assert.match(live.columns[0][1], /as it stands today/);
  // `wp_live` on a FINAL game is a contradiction; the stored chain wins.
  const done = gp.eloGroup(Object.assign({ wp_live: 1 }, played),
    { completed: true });
  assert.equal(done.columns[0][0], 'Before');
});

test('a game the model never rated gets no group at all', () => {
  // Null, not a group of dashes: a table that widens to say nothing is the
  // sparseness this layout exists to remove.
  assert.equal(gp.eloGroup({}, { completed: true }), null);
  assert.equal(gp.eloGroup(null, { completed: true }), null);
  assert.equal(gp.eloGroup({ venue: 'Somewhere' }, {}), null);
  // A win chance with no ratings is still something to show.
  assert.ok(gp.eloGroup({ home_wp: 0.5 }, {}));
});

/* ---- the matchup table ----------------------------------------------- */

const group = name => ({
  name: name,
  columns: [['A', 'tip a'], ['B', 'tip b']],
  cells: side => [side + '-a', side + '-b']
});

test('the matchup table is a row per club and a column group per model',
  () => {
    const html = gp.matchupTable([group('Elo'), group('Merits')],
      { away: 'Away FC', home: 'Home FC' });
    assert.match(html, /<th scope="colgroup" colspan="2" class="gp-group">Elo</);
    assert.match(html, /<th scope="colgroup" colspan="2" class="gp-group">Merits</);
    // The away club is the first row, as it is in the hero above it.
    assert.ok(html.indexOf('Away FC') < html.indexOf('Home FC'));
    assert.match(html, /<td>away-a<\/td><td>away-b<\/td><td>away-a<\/td>/);
  });

test('the table fills the panel only once it has the columns for it', () => {
  // Below the threshold a game simply has less to show, and five columns
  // stretched across a panel is the sparseness this exists to remove.
  const columns = n => 1 + n * 2;        // a club column plus two per group
  const groups = n => Array.from({ length: n }, () => group('G'));
  // The largest number of two-column groups that still fits UNDER the
  // threshold, and one more.
  let under = 0;
  while (columns(under + 1) < gp.FILL_COLUMNS) under += 1;
  assert.ok(columns(under) < gp.FILL_COLUMNS, 'the fixture is not under it');
  assert.ok(columns(under + 1) >= gp.FILL_COLUMNS, 'nor over it');
  assert.doesNotMatch(gp.matchupTable(groups(under), {}), /is-wide/);
  assert.match(gp.matchupTable(groups(under + 1), {}), /is-wide/);
});

test('a group that is null is left out, and all-null draws no table', () => {
  assert.match(gp.matchupTable([null, group('Elo')], {}), /Elo/);
  assert.equal(gp.matchupTable([null, undefined], {}), '');
  assert.equal(gp.matchupTable([], {}), '');
  assert.equal(gp.matchupTable(null, {}), '');
});

test('a club name is escaped, not injected', () => {
  const html = gp.matchupTable([group('Elo')],
    { away: '<script>x</script>', home: 'A & B' });
  assert.doesNotMatch(html, /<script>/);
  assert.match(html, /A &amp; B/);
});

/* ---- the line score -------------------------------------------------- */

const line = over => Object.assign({
  home: [7, 7, 0, 7], away: [3, 7, 7, 0], homeScore: 21, awayScore: 17,
  regulation: 4
}, over);

test('a regulation line score is numbered, and overtime is named', () => {
  assert.deepEqual(gp.lineScore(line()).labels, ['1', '2', '3', '4']);
  assert.deepEqual(gp.lineScore(line({
    home: [7, 7, 0, 7, 6, 3], away: [3, 7, 7, 0, 6, 0],
    homeScore: 30, awayScore: 23
  })).labels, ['1', '2', '3', '4', 'OT', '2OT']);
  // Two halves, not four quarters: the league states its own regulation.
  assert.deepEqual(gp.lineScore({
    home: [40, 42], away: [38, 39], homeScore: 82, awayScore: 77,
    regulation: 2
  }).labels, ['1', '2']);
});

test('periods that disagree with the scoreboard draw nothing', () => {
  // A table that disagrees with the score above it is worse than no table,
  // so the whole thing is withheld rather than shown wrong.
  assert.equal(gp.lineScore(line({ homeScore: 22 })), null);
  assert.equal(gp.lineScore(line({ awayScore: 16 })), null);
  assert.equal(gp.lineScore(line({ homeScore: null })), null);
});

test('a malformed or one-sided line score draws nothing', () => {
  assert.equal(gp.lineScore(line({ away: null })), null);
  assert.equal(gp.lineScore(line({ home: [] })), null);
  assert.equal(gp.lineScore(line({ away: [3, 7, 7] })), null,
    'the two sides have different lengths');
  assert.equal(gp.lineScore(line({
    home: [10, 11], away: [8, 9], homeScore: 21, awayScore: 17
  })), null, 'fewer periods than regulation');
  assert.equal(gp.lineScore(line({ home: [7, 7, 0, '7'] })), null,
    'a period that is not a number');
  assert.equal(gp.lineScore(line({ home: [7, 7, 0.5, 6.5] })), null,
    'a fractional period');
  assert.equal(gp.lineScore(line({ home: [7, 7, -7, 14] })), null,
    'a negative period');
  assert.equal(gp.lineScore(null), null);
});

test('the line score table names both clubs and marks the total', () => {
  const html = gp.lineScoreTable(gp.lineScore(line()),
    { away: 'Away FC', home: 'Home FC' }, { away: 17, home: 21 });
  assert.match(html, /class="gp-table gp-line"/);
  assert.match(html, /<th scope="col" class="gp-line-total">T<\/th>/);
  assert.match(html, /<th scope="row">Away FC<\/th><td>3<\/td>/);
  assert.match(html, /<td class="gp-line-total">21<\/td>/);
  assert.equal(gp.lineScoreTable(null, {}, {}), '');
});

/* ---- badges and facts ------------------------------------------------ */

test('a badge carries its kind, and a falsy badge is dropped', () => {
  assert.equal(gp.badges([{ text: 'Final', kind: 'final' }, null, 'Neutral']),
    '<span class="gbadge final">Final</span>'
    + '<span class="gbadge">Neutral</span>');
  assert.equal(gp.badges([]), '');
  assert.equal(gp.badges(null), '');
  assert.doesNotMatch(gp.badges([{ text: '<b>x</b>' }]), /<b>/);
});

test('a fact with nothing to say is dropped rather than shown empty', () => {
  const html = gp.facts([
    { label: 'Venue', value: 'Somewhere' },
    { label: 'Tip-off', value: '' },
    { label: 'Nothing', value: null },
    { label: '', value: 'orphan' }
  ]);
  assert.match(html, /<b>Venue<\/b>Somewhere/);
  assert.doesNotMatch(html, /Tip-off/);
  assert.doesNotMatch(html, /orphan/);
  // Every fact empty means no grid at all, not an empty one.
  assert.equal(gp.facts([{ label: 'A', value: '' }]), '');
  assert.equal(gp.facts([]), '');
});

test('a fact escapes its value unless it is explicitly markup', () => {
  assert.doesNotMatch(gp.facts([{ label: 'L', value: '<i>x</i>' }]), /<i>/);
  assert.match(gp.facts([{ label: 'L', html: '<a href="#">x</a>' }]),
    /<a href="#">x<\/a>/);
});
