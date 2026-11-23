/* The returning-production board, lifted out of the production shell.
 *
 * The export side is tests/test_returning_production.py.
 */
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const shell = fs.readFileSync('ui/dashboard_shell.html', 'utf8');

for (const name of ['returningPct', 'returningTableHtml', 'renderReturning',
  'loadReturningProduction', 'returningEntry', 'hydrateReturning']) {
  // Two declarations of one name silently replace each other here, and a test
  // that lifts BY NAME never notices which one it got.
  const found = shell.match(new RegExp('function ' + name + '\\(', 'g')) || [];
  assert.equal(found.length, 1, name + ' must be declared exactly once in the shell');
}

const source = [
  shell.match(/function returningPct\([^]*?\n\}/)[0],
  shell.match(/const RETURNING_COLUMNS = \[[^]*?\n\];/)[0],
  shell.match(/function returningTableHtml\([^]*?\n\}\n/)[0]
].join('\n');

const context = {
  esc: v => String(v).replaceAll('&', '&amp;').replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;').replaceAll('"', '&quot;'),
  TEAM_BY_ID: new Map([['1', { name: 'Alabama' }], ['2', { name: 'Auburn' }]]),
  teamLink: name => '<a href="#">' + context.esc(name) + '</a>'
};
vm.runInNewContext(
  source + '\nthis.table = returningTableHtml; this.pct = returningPct;' +
  '\nthis.columns = RETURNING_COLUMNS;', context);
const { table, pct, columns } = context;

const team = (over = {}) => Object.assign({
  team_id: 1, percent_ppa: 0.6, percent_passing_ppa: 0.5, percent_rushing_ppa: 0.4,
  percent_receiving_ppa: 0.3, usage: 0.55, total_ppa: 120.5
}, over);
const text = html => html.replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();

/* ---- a share, and the gap that is not a zero -------------------------- */

test('a share prints as a percentage with one decimal', () => {
  assert.equal(pct(0.6), '60.0%');
  assert.equal(pct(0), '0.0%', 'a team that returned nothing returned nothing');
  assert.equal(pct(1), '100.0%');
});

test('a share the provider never reported is a gap, not a zero', () => {
  // `Number(null)` is 0, which this project has shipped three times.
  assert.equal(pct(null), '—');
  assert.equal(pct(undefined), '—');
  assert.equal(pct('nonsense'), '—');
});

test('a share above 100% is printed as served, not clipped', () => {
  // 432 of the archive's values exceed 1, up to 7666.7%: the ratio is against
  // last season's category total, which can be near zero or negative. Capping
  // would hide which teams those are.
  assert.equal(pct(2.104), '210.4%');
  assert.equal(pct(76.667), '7666.7%');
});

/* ---- the board -------------------------------------------------------- */

test('the board is ordered by overall share, best first', () => {
  const html = table([team({ team_id: 1, percent_ppa: 0.4 }),
    team({ team_id: 2, percent_ppa: 0.9 })]);
  const names = [...html.matchAll(/<a href="#">([^<]+)<\/a>/g)].map(m => m[1]);
  assert.deepEqual(names, ['Auburn', 'Alabama']);
  assert.match(html, /<td class="num rank-col">1<\/td>/);
});

test('a team with no overall share is left off rather than ranked last', () => {
  // Sorting on null would park it at an arbitrary end and read as a measured 0.
  const html = table([team({ team_id: 1 }), team({ team_id: 2, percent_ppa: null })]);
  assert.match(html, /Alabama/);
  assert.doesNotMatch(html, /Auburn/);
});

test('an empty season says so instead of drawing an empty table', () => {
  assert.match(table([]), /No returning production/);
  assert.match(table(null), /No returning production/);
  assert.match(table([team({ percent_ppa: null })]), /No returning production/);
});

test('the absolute figure is on the row, because a share alone hides scale', () => {
  // 80% of very little and 80% of a great deal are the same percentage.
  assert.match(text(table([team()])), /120\.5/);
  assert.match(table([team({ total_ppa: null })]), /—/);
});

test('every column sorts on its own number, not on the printed percentage', () => {
  // ui/stats.js keys a cell on data-sort; '60.0%' would send the column to a
  // text sort, where 9% outranks 60%.
  const html = table([team({ usage: null })]);
  assert.match(html, /data-sort="0\.6"/);
  assert.match(html, /data-sort="120\.5"/);
  assert.match(html, /data-sort=""/, 'a gap carries no sort key at all');
});

test('the columns are the five shares plus usage, each with its own tooltip', () => {
  assert.deepEqual(Array.from(columns, c => c[0]),
    ['percent_ppa', 'percent_passing_ppa', 'percent_rushing_ppa',
      'percent_receiving_ppa', 'usage']);
  for (const [, label, tip] of columns) {
    assert.ok(tip && tip.length > 10, label + ' needs a tooltip saying what it measures');
  }
});

test('a team the database cannot name does not draw "undefined"', () => {
  const html = table([team({ team_id: 999 })]);
  assert.doesNotMatch(html, /undefined/);
});

/* ---- the claim the board must never lose ------------------------------ */

test('the page says no rating reads this, and explains a share over 100%', () => {
  const render = shell.match(/function renderReturning\([^]*?\n\}\n/)[0];
  assert.match(render, /no rating on this site reads it/i);
  assert.match(render, /above 100%/,
    'an uncapped 7666.7% looks broken unless the page explains the ratio');
  assert.match(render, /negative/, 'the mechanism is the negative-PPA denominator');
});
