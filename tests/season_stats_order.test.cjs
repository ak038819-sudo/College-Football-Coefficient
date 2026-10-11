// A player's season statistics: the order the columns come in, and the career
// line under them. The exporter emits stat types ordered by name, so before
// this a quarterback's passing table read ATT, COMPLETIONS, INT, PCT, TD, YDS,
// YPA -- interceptions before yards, the attempt before the completion. The
// career row is the other half: adding seven seasons of passing yards by hand
// is not reading, and averaging seven completion percentages is not a career
// completion percentage.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const shell = fs.readFileSync('ui/dashboard_shell.html', 'utf8');

function lift(names, extra = {}) {
  const constants = shell.match(
    /const SEASON_STAT_ORDER = \{[^]*?const SEASON_STAT_MAX = \{[^}]*\};/);
  assert.ok(constants, 'the shell must declare the SEASON_STAT_* constants');
  const decimals = shell.match(/^const DECIMAL_STATS = \{[^}]*\};/m);
  assert.ok(decimals, 'the shell must declare DECIMAL_STATS');
  const source = constants[0] + '\n' + decimals[0] + '\n' + names.map(name => {
    const m = shell.match(new RegExp('^function ' + name + '\\([^]*?\\n\\}', 'm'));
    assert.ok(m, 'the shell must declare ' + name);
    return m[0];
  }).join('\n');
  const context = Object.assign({
    FRACTION_STATS: { PCT: true },
    esc: v => String(v).replace(/[&<>]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c])),
    teamCellById: id => 'TEAM' + id,
    personStatsNote: () => '<p>NO STATS</p>',
    STAT_CATEGORY_LABELS: { passing: 'Passing', rushing: 'Rushing', kicking: 'Kicking',
                            punting: 'Punting' },
    STAT_CATEGORY_ORDER: ['passing', 'rushing', 'kicking', 'punting'],
  }, extra);
  vm.runInNewContext(source + '\nthis.__out = {' + names.join(',') + '};', context);
  return context.__out;
}

const { orderSeasonStatTypes, careerStatRow } = lift(['orderSeasonStatTypes', 'careerStatRow']);
const { playerStatsPanel } = lift(['statCell', 'orderSeasonStatTypes', 'careerStatRow',
                                   'playerStatsPanel']);

// ---- the order ---------------------------------------------------------------

test('a passing line reads as a passing line, not alphabetically', () => {
  const alphabetical = ['ATT', 'COMPLETIONS', 'INT', 'PCT', 'TD', 'YDS', 'YPA'];
  assert.deepEqual(Array.from(orderSeasonStatTypes('passing', alphabetical)),
                   ['COMPLETIONS', 'ATT', 'PCT', 'YDS', 'YPA', 'TD', 'INT']);
});

test('every category the feed sends has an order of its own', () => {
  // Alphabetically a rusher's line opens on CAR by luck and a kicker's opens
  // on FGA before FGM. Neither should depend on luck.
  assert.deepEqual(Array.from(orderSeasonStatTypes('rushing', ['CAR', 'LONG', 'TD', 'YDS', 'YPC'])),
                   ['CAR', 'YDS', 'YPC', 'LONG', 'TD']);
  assert.deepEqual(Array.from(orderSeasonStatTypes('kicking',
                     ['FGA', 'FGM', 'LONG', 'PCT', 'PTS', 'XPA', 'XPM'])),
                   ['FGM', 'FGA', 'PCT', 'LONG', 'XPM', 'XPA', 'PTS']);
  assert.deepEqual(Array.from(orderSeasonStatTypes('punting',
                     ['In 20', 'LONG', 'NO', 'TB', 'YDS', 'YPP'])),
                   ['NO', 'YDS', 'YPP', 'LONG', 'In 20', 'TB']);
});

test('a type the feed adds later keeps a stable place rather than vanishing', () => {
  const out = Array.from(orderSeasonStatTypes('rushing', ['SOMETHING', 'CAR', 'ANOTHER', 'YDS']));
  assert.deepEqual(out, ['CAR', 'YDS', 'ANOTHER', 'SOMETHING']);
});

test('a category with no declared order is left alphabetical, not shuffled', () => {
  assert.deepEqual(Array.from(orderSeasonStatTypes('somethingNew', ['B', 'C', 'A'])),
                   ['A', 'B', 'C']);
});

test('ordering does not mutate the list it was handed', () => {
  const types = ['ATT', 'COMPLETIONS'];
  orderSeasonStatTypes('passing', types);
  assert.deepEqual(Array.from(types), ['ATT', 'COMPLETIONS']);
});

// ---- the career line ----------------------------------------------------------

test('counting statistics add up', () => {
  const career = careerStatRow('rushing', ['CAR', 'YDS', 'TD'],
    [{ CAR: 100, YDS: 500, TD: 4 }, { CAR: 50, YDS: 300, TD: 2 }]);
  assert.equal(career.CAR, 150);
  assert.equal(career.YDS, 800);
  assert.equal(career.TD, 6);
});

test('a rate is recomputed from the totals, never averaged', () => {
  // 1 of 1 one season and 50 of 100 the next is 51 of 101, not 75%.
  const career = careerStatRow('passing', ['COMPLETIONS', 'ATT', 'PCT'],
    [{ COMPLETIONS: 1, ATT: 1, PCT: 1 }, { COMPLETIONS: 50, ATT: 100, PCT: 0.5 }]);
  assert.ok(Math.abs(career.PCT - 51 / 101) < 1e-12, 'got ' + career.PCT);
});

test('yards per attempt and per carry come from the totals too', () => {
  const passing = careerStatRow('passing', ['YDS', 'ATT', 'YPA'],
    [{ YDS: 100, ATT: 10, YPA: 10 }, { YDS: 200, ATT: 90, YPA: 2.2 }]);
  assert.equal(passing.YPA, 3);
  const rushing = careerStatRow('rushing', ['YDS', 'CAR', 'YPC'],
    [{ YDS: 100, CAR: 10, YPC: 10 }, { YDS: 200, CAR: 90, YPC: 2.2 }]);
  assert.equal(rushing.YPC, 3);
});

test('a career rate carries the one decimal the feed gives its seasons', () => {
  // 7,590 over 1,076 is 7.0539. Printed in full it sits under seasons of 7.1
  // and 6.8 reading 7.05, which looks like a different kind of number.
  const career = careerStatRow('passing', ['YDS', 'ATT', 'YPA'],
    [{ YDS: 7000, ATT: 1000 }, { YDS: 590, ATT: 76 }]);
  assert.equal(career.YPA, 7.1);
});

test('a longest run is the longest, not the sum of them', () => {
  const career = careerStatRow('rushing', ['LONG'], [{ LONG: 42 }, { LONG: 71 }, { LONG: 12 }]);
  assert.equal(career.LONG, 71);
});

test('a rate with no denominator is blank rather than a division by zero', () => {
  const career = careerStatRow('passing', ['COMPLETIONS', 'ATT', 'PCT'],
    [{ COMPLETIONS: 0, ATT: 0, PCT: 0 }, { COMPLETIONS: 0, ATT: 0, PCT: 0 }]);
  assert.equal(career.PCT, null);
});

test('a season missing a statistic does not poison the total', () => {
  const career = careerStatRow('rushing', ['CAR', 'YDS'],
    [{ CAR: 100, YDS: 500 }, { YDS: 300 }, { CAR: null, YDS: 'n/a' }]);
  assert.equal(career.CAR, 100);
  assert.equal(career.YDS, 800);
});

test('one season gets no career row, because it is already the career', () => {
  assert.equal(careerStatRow('rushing', ['CAR'], [{ CAR: 100 }]), null);
  assert.equal(careerStatRow('rushing', ['CAR'], []), null);
});

test('a rate column carries one decimal on every row of it', () => {
  // The feed sends 5 for one season and 7.6 for the next; read down the
  // column, the whole number looks like a different kind of measurement.
  const html = playerStatsPanel({
    2025: { passing: { team_id: 7, YDS: 435, ATT: 87, YPA: 5 } },
    2024: { passing: { team_id: 7, YDS: 1452, ATT: 190, YPA: 7.6 } },
  });
  assert.match(html, />5\.0</);
  assert.match(html, />7\.6</);
});

test('a rate is shown to one decimal however many the feed sends', () => {
  // Today the archive never sends more than one. The column's job is to be
  // readable down its length, which stops being true the day it does.
  const html = playerStatsPanel({ 2025: { punting: { team_id: 7, NO: 3, YDS: 130, YPP: 43.333 } } });
  assert.match(html, />43\.3</);
  assert.doesNotMatch(html, />43\.33</);
});

// ---- the table ----------------------------------------------------------------

test('the table carries the career row and labels it', () => {
  const html = playerStatsPanel({
    2025: { passing: { team_id: 7, ATT: 100, COMPLETIONS: 70, PCT: 0.7, YDS: 1000 } },
    2024: { passing: { team_id: 7, ATT: 100, COMPLETIONS: 50, PCT: 0.5, YDS: 500 } },
  });
  assert.match(html, /<tfoot>/);
  assert.match(html, /Career<\/th><td>2 seasons<\/td>/);
  assert.match(html, />1,500</, 'career yards');
  assert.match(html, />60\.0%</, 'career completion percentage, from the totals');
  assert.match(html, />120</, 'career completions');
  assert.doesNotMatch(html, />120\.0%</, 'the percentages must not be summed');
});

test('a single-season player gets no career row', () => {
  const html = playerStatsPanel({ 2025: { passing: { team_id: 7, YDS: 1000 } } });
  assert.doesNotMatch(html, /<tfoot>/);
});

test('the header shows CMP, not the feed\'s COMPLETIONS', () => {
  const html = playerStatsPanel({ 2025: { passing: { team_id: 7, COMPLETIONS: 70, ATT: 100 } } });
  assert.match(html, /<th scope="col">CMP<\/th><th scope="col">ATT<\/th>/);
  assert.doesNotMatch(html, /COMPLETIONS/);
});

test('the columns are in reading order in the rendered table', () => {
  const html = playerStatsPanel({
    2025: { passing: { team_id: 7, ATT: 100, COMPLETIONS: 70, INT: 3, PCT: 0.7, TD: 9, YDS: 1000, YPA: 10 } },
  });
  const headers = [...html.matchAll(/<th scope="col">([^<]+)<\/th>/g)].map(m => m[1]);
  assert.deepEqual(headers.slice(2), ['CMP', 'ATT', 'PCT', 'YDS', 'YPA', 'TD', 'INT']);
});

test('the note says how a career rate was arrived at', () => {
  const html = playerStatsPanel({
    2025: { passing: { team_id: 7, YDS: 1 } }, 2024: { passing: { team_id: 7, YDS: 1 } },
  });
  assert.match(html, /every completion over every attempt/);
});
