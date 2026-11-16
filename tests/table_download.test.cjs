/* Taking a table away with you.
 *
 * Every board on the site renders through enhanceTable or SortableTable and
 * ends up inside a .table-scroll, so one reader of the finished DOM exports
 * all of them. These drive that reader -- ui/stats.js's tableCsv -- against
 * real markup, plus the shell's own glue that decides where a control appears.
 */
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const { JSDOM } = require('jsdom');
const stats = require('../ui/stats.js');

const dom = html => new JSDOM('<!doctype html><body>' + html + '</body>').window.document;
const table = html => dom('<table>' + html + '</table>').querySelector('table');
const rows = csv => csv.trimEnd().split('\r\n');

/* ---- one cell ------------------------------------------------------- */

test('a dash is an empty cell, not the character', () => {
  // The site draws "no value" as an em dash. A spreadsheet column holding
  // '—' is a text column, and every number in it stops being a number.
  for (const nothing of ['—', '–', '-', '  —  ']) {
    assert.equal(stats.csvCell(nothing), '', JSON.stringify(nothing));
  }
  assert.equal(stats.csvCell(''), '');
  assert.equal(stats.csvCell(null), '');
  assert.equal(stats.csvCell(undefined), '');
});

test('a real zero is not a dash', () => {
  assert.equal(stats.csvCell('0'), '0');
  assert.equal(stats.csvCell('0.0%'), '0.0%');
  assert.equal(stats.csvCell(0), '0');
});

test('typography becomes something a parser can read', () => {
  // U+2212 is the minus sign this site prints; no parser treats it as one.
  assert.equal(stats.csvCell('−92.9'), '-92.9');
  // A triangle is a sign too -- dropping it would turn a fall into a rise.
  assert.equal(stats.csvCell('▲65.0'), '+65.0');
  assert.equal(stats.csvCell('▼76.5'), '-76.5');
  assert.equal(stats.csvCell('1928 (▼1.7)'), '1928 (-1.7)');
});

test('a thousands separator is removed from a number and from nothing else', () => {
  assert.equal(stats.csvCell('1,048,998'), '1048998');
  assert.equal(stats.csvCell('-31.6'), '-31.6');
  assert.equal(stats.csvCell('73.03%'), '73.03%');
  // Not a bare number: the comma is part of what the cell says.
  assert.equal(stats.csvCell('Miami, FL'), 'Miami, FL');
  assert.equal(stats.csvCell('Texas A&M'), 'Texas A&M');
});

/* ---- a whole table --------------------------------------------------- */

test('header first, then every body row', () => {
  const csv = stats.tableCsv(table(
    '<thead><tr><th>Team</th><th>Elo</th></tr></thead>' +
    '<tbody><tr><td>Georgia</td><td>1928</td></tr>' +
    '<tr><td>Indiana</td><td>1916</td></tr></tbody>'));
  assert.deepEqual(rows(csv), ['Team,Elo', 'Georgia,1928', 'Indiana,1916']);
  assert.ok(csv.endsWith('\r\n'), 'a CSV ends its last record');
});

test('the file is what the cell SAYS, not what it sorts by', () => {
  // data-sort is a sort KEY and the two differ exactly where it exists. The
  // Elo board sorts Record on wins and Last 5 on an unrounded float, so a
  // data-sort-first export published '8' for '8-1' and '-31.609999999999996'.
  const csv = stats.tableCsv(table(
    '<thead><tr><th>Record</th><th>Last 5</th></tr></thead>' +
    '<tbody><tr><td data-sort="8">8–1</td>' +
    '<td data-sort="-31.609999999999996">−31.6</td></tr></tbody>'));
  assert.deepEqual(rows(csv), ['Record,Last 5', '8–1,-31.6']);
});

test('data-sort fills a cell that says nothing, and an empty one does not', () => {
  const csv = stats.tableCsv(table(
    '<thead><tr><th>A</th><th>B</th></tr></thead>' +
    '<tbody><tr><td data-sort="1645"></td><td data-sort="   "></td></tr>' +
    '<tr><td data-sort="1500"></td><td></td></tr></tbody>'));
  assert.deepEqual(rows(csv), ['A,B', '1645,', '1500,']);
});

test('a field holding a comma, a quote or a newline is quoted RFC 4180 style', () => {
  const csv = stats.tableCsv(table(
    '<thead><tr><th>Where it comes from</th></tr></thead>' +
    '<tbody><tr><td>CFBD, and nowhere else</td></tr>' +
    '<tr><td>he said "no"</td></tr></tbody>'));
  assert.deepEqual(rows(csv), ['Where it comes from',
    '"CFBD, and nowhere else"', '"he said ""no"""']);
});

test('the column names come from the LAST head row, without the sort arrow', () => {
  // The game page's Matchup groups its columns, so the first head row holds
  // group names that each span several columns; taking it would misalign
  // every cell beneath. The arrow on a sorted header is display, not a name.
  const csv = stats.tableCsv(table(
    '<thead><tr><th></th><th colspan="2">Elo</th></tr>' +
    '<tr><th>Team</th><th>Before</th><th>After ▼</th></tr></thead>' +
    '<tbody><tr><td>Miami</td><td>1506</td><td>1599</td></tr></tbody>'));
  assert.deepEqual(rows(csv), ['Team,Before,After', 'Miami,1506,1599']);
});

test('nothing to take is an empty string, never a header on its own', () => {
  assert.equal(stats.tableCsv(null), '');
  assert.equal(stats.tableCsv(table('<tbody><tr><td>x</td></tr></tbody>')), '',
    'no head row means no column names, so no file');
  assert.equal(stats.tableCsv(table(
    '<thead><tr><th>Team</th></tr></thead><tbody></tbody>')), '',
    'a header with no rows is not a table worth downloading');
});

/* ---- where a control appears ----------------------------------------- */

const shell = fs.readFileSync('ui/dashboard_shell.html', 'utf8');

function lift(name) {
  const matches = shell.match(new RegExp('function ' + name + '\\(', 'g')) || [];
  assert.equal(matches.length, 1, name + ' must be declared exactly once');
  const source = shell.match(new RegExp('function ' + name + '\\([^]*?\\n\\}'))?.[0];
  assert.ok(source, name + ' must exist in the production shell');
  return source;
}

test('a one-row table gets no control, a real one does', () => {
  const source = lift('addTableDownload') +
    '\n' + (shell.match(/const DOWNLOAD_MIN_ROWS = \d+;/) || [''])[0];
  const context = { document: null };
  vm.runInNewContext(source + '\nthis.add = addTableDownload;', context);
  const make = bodyRows => {
    const d = dom('<div class="table-scroll"><table><thead><tr><th>A</th></tr></thead>' +
      '<tbody>' + '<tr><td>1</td></tr>'.repeat(bodyRows) + '</tbody></table></div>');
    context.document = d;
    context.add(d.querySelector('table'));
    return d.querySelectorAll('.table-download').length;
  };
  assert.equal(make(1), 0, 'a single row is read, not analysed');
  assert.equal(make(2), 1);
  assert.equal(make(40), 1);
});

test('a control is added once, however often the page is re-measured', () => {
  // wrapWideTables runs on every render and on some interactions; a second
  // control under the same table is the obvious way for this to go wrong.
  const source = lift('addTableDownload') +
    '\n' + (shell.match(/const DOWNLOAD_MIN_ROWS = \d+;/) || [''])[0];
  const context = { document: null };
  vm.runInNewContext(source + '\nthis.add = addTableDownload;', context);
  const d = dom('<div class="table-scroll"><table><thead><tr><th>A</th></tr></thead>' +
    '<tbody><tr><td>1</td></tr><tr><td>2</td></tr></tbody></table></div>');
  context.document = d;
  for (let i = 0; i < 4; i++) context.add(d.querySelector('table'));
  assert.equal(d.querySelectorAll('.table-download').length, 1);
});

test('a paginated board says the file is one page', () => {
  // SortableTable shows 50 rows of a longer list. A file that silently held
  // one page of it would be worse than no file.
  const source = lift('addTableDownload') +
    '\n' + (shell.match(/const DOWNLOAD_MIN_ROWS = \d+;/) || [''])[0];
  const context = { document: null };
  vm.runInNewContext(source + '\nthis.add = addTableDownload;', context);
  const d = dom('<div><div class="table-scroll"><table><thead><tr><th>A</th></tr></thead>' +
    '<tbody><tr><td>1</td></tr><tr><td>2</td></tr></tbody></table></div>' +
    '<nav class="table-pagination"><a href="#">Next</a></nav></div>');
  context.document = d;
  context.add(d.querySelector('table'));
  assert.match(d.querySelector('.download-button').textContent, /this page/i);
});

test('every board on the site is reachable, because wrapWideTables offers them all', () => {
  // The claim this feature rests on: one call site covers the whole site.
  assert.match(shell, /document\.querySelectorAll\('#content table'\)/);
  assert.match(shell, /addTableDownload\(table\);/);
});
