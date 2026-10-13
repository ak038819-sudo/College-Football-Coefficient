// Every sortable table on the site shares one comparator. These tests drive the
// real one out of ui/stats.js with the values the pages actually render -- the
// '#12' rank that started this, a U+2212 signed number, a '<0.1%' probability,
// a 'bye' in a column of Elo changes, a record and a calendar date.
const test = require('node:test');
const assert = require('node:assert/strict');
const { JSDOM } = require('jsdom');
const CfbStats = require('../ui/stats.js');

// Sorts a column the way a click does: through sortCells, not through a copy of it.
function order(values, dir = 'asc') {
  return CfbStats.sortCells(values.map(value => ({ value })), dir).map(e => e.value);
}

test('a rank column sorts numerically, whether or not it carries a #', () => {
  const ranks = ['#9', '#1', '#18', '#2', '#10', '#11'];
  assert.deepEqual(order(ranks), ['#1', '#2', '#9', '#10', '#11', '#18']);
  assert.deepEqual(order(['9', '1', '18', '2', '10', '11']),
    ['1', '2', '9', '10', '11', '18']);
});

test('a minus sign (U+2212) is a negative number, not a word', () => {
  assert.deepEqual(order(['−0.5', '+4.6', '−1.7', '+1.4']),
    ['−1.7', '−0.5', '+1.4', '+4.6']);
});

test('a bounded probability sorts just below its bound', () => {
  assert.deepEqual(order(['<0.1%', '9.8%', '84.9%', '0.1%', '100.0%'], 'desc'),
    ['100.0%', '84.9%', '9.8%', '0.1%', '<0.1%']);
});

test('one word in a numeric column sorts last and decides nothing', () => {
  assert.deepEqual(order(['bye', '−15.4', '+2.0', 'bye', '−6.2'], 'desc'),
    ['+2.0', '−6.2', '−15.4', 'bye', 'bye']);
  assert.deepEqual(order(['bye', '−15.4', '+2.0'], 'asc'),
    ['−15.4', '+2.0', 'bye']);
});

test('a blank sorts last in both directions', () => {
  assert.deepEqual(order(['—', '3', '1'], 'asc'), ['1', '3', '—']);
  assert.deepEqual(order(['—', '3', '1'], 'desc'), ['3', '1', '—']);
  assert.deepEqual(order(['…', '2', '10'], 'asc'), ['2', '10', '…']);
});

test('a record sorts by wins, then by the fewest losses', () => {
  assert.deepEqual(order(['9–3', '10–2', '10–3', '2–10'], 'desc'),
    ['10–2', '10–3', '9–3', '2–10']);
});

test('a record followed by what it works out to still sorts on the record', () => {
  // The conference history columns render a record beside its percentage or its
  // bid count, and a W-L-T record carries a third number of its own.
  assert.deepEqual(CfbStats.cellKey('30\u201311 73.2%'), { nums: [30, -11, 73.2] });
  assert.deepEqual(order(['10\u20132 83.3%', '10\u20133 76.9%', '9\u20133 75.0%'], 'desc'),
    ['10\u20132 83.3%', '10\u20133 76.9%', '9\u20133 75.0%']);
  assert.deepEqual(order(['6\u20133 9 bids', '6\u20132 9 bids', '10\u20132 3 bids'], 'desc'),
    ['10\u20132 3 bids', '6\u20132 9 bids', '6\u20133 9 bids']);
  assert.deepEqual(order(['76\u2013103\u20132', '131\u2013121\u20134', '131\u2013120\u20131'], 'desc'),
    ['131\u2013120\u20131', '131\u2013121\u20134', '76\u2013103\u20132']);
});

test('a stored date is a date, not a record whose month is a loss', () => {
  assert.deepEqual(CfbStats.cellKey('2024-09-07'), { nums: [2024, 9, 7] });
  assert.deepEqual(order(['2024-11-30', '2024-09-07', '2024-10-01']),
    ['2024-09-07', '2024-10-01', '2024-11-30']);
  assert.deepEqual(order(['2024-09-07T19:30:00.000Z', '2024-09-06T23:00:00.000Z']),
    ['2024-09-06T23:00:00.000Z', '2024-09-07T19:30:00.000Z']);
});

test('a made/attempted pair sorts by what was made', () => {
  assert.deepEqual(order(['21/26', '31/48', '21/30'], 'desc'), ['31/48', '21/26', '21/30']);
});

test('a thousands separator is not a decimal point', () => {
  // Pinned on the key as well as the order: a grouped number that fell back to a
  // word key still happened to land in the right place in a column of its own.
  assert.deepEqual(CfbStats.cellKey('3,688'), { nums: [3688] });
  assert.deepEqual(order(['50000', '9,998', '1,002', '20'], 'desc'),
    ['50000', '9,998', '1,002', '20']);
});

test('a rank reads as a number, so a word beside it still sorts last', () => {
  assert.deepEqual(CfbStats.cellKey('#12'), { nums: [12] });
  assert.deepEqual(order(['#9', 'NR', '#12', '#1'], 'asc'), ['#1', '#9', '#12', 'NR']);
});

test('a calendar date sorts by day, not by the spelling of the month', () => {
  assert.deepEqual(order(['Sep 24, 2022', 'Sep 3, 2022', 'Oct 8, 2022', 'Sep 10, 2022']),
    ['Sep 3, 2022', 'Sep 10, 2022', 'Sep 24, 2022', 'Oct 8, 2022']);
});

test('a word column still sorts as words, numerically within them', () => {
  assert.deepEqual(order(['Ohio State', 'Miami (FL)', 'Indiana']),
    ['Indiana', 'Miami (FL)', 'Ohio State']);
  assert.deepEqual(order(['Week 10', 'Week 2', 'Week 1']), ['Week 1', 'Week 2', 'Week 10']);
});

test('a rating with its rank beside it sorts on the rating', () => {
  assert.deepEqual(order(['1498 #76', '1633 #9', '1496 #77'], 'desc'),
    ['1633 #9', '1498 #76', '1496 #77']);
});

test('ties keep the order the page rendered them in', () => {
  const rows = [{ value: '5', id: 'a' }, { value: '5', id: 'b' }, { value: '5', id: 'c' }];
  assert.deepEqual(CfbStats.sortCells(rows, 'desc').map(r => r.id), ['a', 'b', 'c']);
  assert.deepEqual(CfbStats.sortCells(rows, 'asc').map(r => r.id), ['a', 'b', 'c']);
});

// ---- The click path, through a real DOM -------------------------------------
function table(head, rows, attrs = '') {
  const dom = new JSDOM('<table id="t" ' + attrs + '><thead><tr>' +
    head.map(h => '<th>' + h + '</th>').join('') + '</tr></thead><tbody>' +
    rows.map(r => '<tr>' + r.map(c => '<td ' + (c.key == null ? '' :
      'data-sort="' + c.key + '"') + '>' + (c.text ?? c) + '</td>').join('') + '</tr>').join('') +
    '</tbody></table>');
  return dom.window.document.getElementById('t');
}
function clickSort(t, index, times = 1) {
  const button = t.tHead.rows[0].cells[index].querySelector('button');
  const win = t.ownerDocument.defaultView;
  for (let i = 0; i < times; i++) button.dispatchEvent(new win.MouseEvent('click', { bubbles: true }));
  return [...t.tBodies[0].rows].map(r => r.cells[index].textContent.trim());
}

test('clicking a rank header orders the rows numerically', () => {
  const t = table(['Standing', 'Team'], [['#9', 'A'], ['#1', 'B'], ['#18', 'C'], ['#10', 'D']]);
  CfbStats.enhanceTable(t);
  assert.deepEqual(clickSort(t, 0), ['#18', '#10', '#9', '#1']);
  assert.deepEqual(clickSort(t, 0), ['#1', '#9', '#10', '#18']);
});

test('a column filled in after enhancement sorts on its new values', () => {
  const t = table(['#', 'SOS'], [['1', { text: '…', key: '' }], ['2', { text: '…', key: '' }]]);
  CfbStats.enhanceTable(t);
  const cells = [...t.tBodies[0].rows].map(r => r.cells[1]);
  cells[0].textContent = '1498';
  cells[1].textContent = '1633';
  cells[0].removeAttribute('data-sort');
  cells[1].removeAttribute('data-sort');
  assert.deepEqual(clickSort(t, 1), ['1633', '1498']);
});

test('data-sort decides the order where it is set, and the text where it is not', () => {
  const t = table(['Elo'], [[{ text: '1891(▲3.6)', key: '1891.2' }],
    [{ text: '1952(▲2.0)', key: '1952.4' }]]);
  CfbStats.enhanceTable(t);
  assert.deepEqual(clickSort(t, 0), ['1952(▲2.0)', '1891(▲3.6)']);
});

test('the header keeps its label and reports the direction', () => {
  const t = table(['Standing'], [['#9'], ['#1']]);
  CfbStats.enhanceTable(t);
  clickSort(t, 0);
  assert.equal(t.tHead.rows[0].cells[0].getAttribute('aria-sort'), 'descending');
  assert.equal(t.tHead.rows[0].cells[0].querySelector('button').textContent, 'Standing ▼');
  clickSort(t, 0);
  assert.equal(t.tHead.rows[0].cells[0].getAttribute('aria-sort'), 'ascending');
  assert.equal(t.tHead.rows[0].cells[0].querySelector('button').textContent, 'Standing ▲');
});
