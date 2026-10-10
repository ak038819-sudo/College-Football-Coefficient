// The Stats tables lead with a rank. It is the position in the CURRENT
// ordering, not a stored rank, so these tests drive the real SortableTable out
// of the shell and pin the three ways it can be wrong: ties, paging, and a
// sort on a column where a rank means nothing.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const shell = fs.readFileSync(path.join(__dirname, '../ui/dashboard_shell.html'), 'utf8');
const CfbStats = require('../ui/stats.js');

function sourceOf(name) {
  const match = shell.match(new RegExp(`function ${name}\\([^)]*\\) \\{[\\s\\S]*?\\n\\}`));
  assert.ok(match, `missing ${name}`);
  return match[0];
}

function render(rows, columns, state, options = {}) {
  const ctx = {
    CfbStats,
    state: Object.assign({ sort: '', dir: 'desc', page: 1, subview: 'players' }, state),
    esc: x => String(x),
    statsHref: () => '#',
  };
  vm.createContext(ctx);
  vm.runInContext(sourceOf('statValue') + '\n' + sourceOf('SortableTable') +
    '\nthis.render = SortableTable;', ctx);
  return ctx.render(rows, columns, options);
}

const COLUMNS = [{ key: 'name', label: 'Player', type: 'string' }, { key: 'yards', label: 'YDS' }];
const ranksIn = html => Array.from(html.matchAll(/<td class="num rank-col">(\d+)<\/td>/g)).map(m => Number(m[1]));
const namesIn = html => Array.from(html.matchAll(/<td>([A-Z][a-z]+)<\/td>/g)).map(m => m[1]);

test('the rank is the position in the sorted order, not the row order', () => {
  const rows = [{ name: 'Carl', yards: 100 }, { name: 'Ann', yards: 900 }, { name: 'Bob', yards: 500 }];
  const html = render(rows, COLUMNS, { sort: 'yards', dir: 'desc' });
  assert.deepEqual(Array.from(namesIn(html)), ['Ann', 'Bob', 'Carl']);
  assert.deepEqual(Array.from(ranksIn(html)), [1, 2, 3]);
});

test('tied values share a rank and the next rank is skipped', () => {
  const rows = [{ name: 'Ann', yards: 900 }, { name: 'Bob', yards: 500 },
                { name: 'Cal', yards: 500 }, { name: 'Dee', yards: 100 }];
  const html = render(rows, COLUMNS, { sort: 'yards', dir: 'desc' });
  assert.deepEqual(Array.from(ranksIn(html)), [1, 2, 2, 4]);
});

test('reversing the sort renumbers from the other end', () => {
  const rows = [{ name: 'Ann', yards: 900 }, { name: 'Bob', yards: 500 }, { name: 'Cal', yards: 100 }];
  const html = render(rows, COLUMNS, { sort: 'yards', dir: 'asc' });
  assert.deepEqual(Array.from(namesIn(html)), ['Cal', 'Bob', 'Ann']);
  assert.deepEqual(Array.from(ranksIn(html)), [1, 2, 3]);
});

test('page two continues the ranking rather than restarting at one', () => {
  const rows = Array.from({ length: 60 }, (_, i) => ({ name: 'P' + i, yards: 1000 - i }));
  const page2 = render(rows, COLUMNS, { sort: 'yards', dir: 'desc', page: 2 });
  const got = ranksIn(page2);
  assert.equal(got.length, 10);
  assert.equal(got[0], 51);
  assert.equal(got[9], 60);
});

test('rows with no value for the sorted column still get a number, at the end', () => {
  const rows = [{ name: 'Ann', yards: 900 }, { name: 'Bob' }, { name: 'Cal', yards: 100 }];
  const html = render(rows, COLUMNS, { sort: 'yards', dir: 'desc' });
  assert.deepEqual(Array.from(namesIn(html)), ['Ann', 'Cal', 'Bob']);
  // Two nulls would tie on "no value"; one null is simply last.
  assert.deepEqual(Array.from(ranksIn(html)), [1, 2, 3]);
});

test('sorting by a name column numbers the rows in that order, without tying them', () => {
  const rows = [{ name: 'Cal', yards: 100 }, { name: 'Ann', yards: 100 }, { name: 'Bob', yards: 100 }];
  const html = render(rows, COLUMNS, { sort: 'name', dir: 'asc' });
  assert.deepEqual(Array.from(namesIn(html)), ['Ann', 'Bob', 'Cal']);
  // Identical yardage must NOT make three alphabetical rows all rank 1.
  assert.deepEqual(Array.from(ranksIn(html)), [1, 2, 3]);
});

test('the rank header is a plain column, not a sortable one', () => {
  const html = render([{ name: 'Ann', yards: 1 }], COLUMNS, { sort: 'yards' });
  assert.match(html, /<th scope="col" class="rank-col">#<\/th>/);
  // Sorting by "#" would mean sorting by the sort order, which is circular.
  assert.doesNotMatch(html, /data-stat-sort="#"/);
});
