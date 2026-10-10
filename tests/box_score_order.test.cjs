// A box score should read the same way every game. CFBD assembles the player
// stats in whatever order it happens to build them -- across a sample of the
// archive the interceptions block lands anywhere from fourth to seventh, one
// game opens on rushing, and a receiving block arrives LONG-first -- so the
// panel imposes its own order. These lift the real ordering functions out of
// the shell rather than restating the rule.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const shell = fs.readFileSync('ui/dashboard_shell.html', 'utf8');

function load() {
  const consts = ['STAT_CATEGORY_ORDER', 'STAT_TYPE_ORDER', 'SHADE_FULL', 'SHADE_MIN_PCT'];
  const fns = ['statGroupRank', 'orderStatGroups', 'eloShade'];
  const source =
    consts.map(name => {
      const m = shell.match(new RegExp('^const ' + name + ' = [^]*?;$', 'm'));
      assert.ok(m, 'the shell must declare ' + name);
      return m[0];
    }).join('\n') + '\n' +
    fns.map(name => {
      const m = shell.match(new RegExp('^function ' + name + '\\([^]*?\\n\\}', 'm'));
      assert.ok(m, 'the shell must declare ' + name);
      return m[0];
    }).join('\n');
  const context = {};
  vm.runInNewContext(source + '\nthis.__x = {STAT_CATEGORY_ORDER, orderStatGroups, eloShade};', context);
  return context.__x;
}

const { STAT_CATEGORY_ORDER, orderStatGroups, eloShade } = load();
const group = (name, type) => ({ name, type });
const names = out => Array.from(out, g => g.name + '/' + g.type);

// The real shape of one team's block, copied from ui/data/players/2024/8.js
// (Notre Dame, 2024 CFP final). It opens on receiving, numbers that block
// LONG-first, and puts fumbles before defense.
const NOTRE_DAME_2024 = [
  ['receiving', 'LONG'], ['receiving', 'REC'], ['receiving', 'YDS'], ['receiving', 'AVG'], ['receiving', 'TD'],
  ['fumbles', 'FUM'], ['fumbles', 'LOST'], ['fumbles', 'REC'],
  ['defensive', 'TOT'], ['defensive', 'SOLO'], ['defensive', 'SACKS'], ['defensive', 'TFL'],
  ['defensive', 'PD'], ['defensive', 'QB HUR'], ['defensive', 'TD'],
  ['puntReturns', 'NO'], ['puntReturns', 'YDS'], ['puntReturns', 'AVG'], ['puntReturns', 'LONG'], ['puntReturns', 'TD'],
  ['kicking', 'FG'], ['kicking', 'PCT'], ['kicking', 'LONG'], ['kicking', 'XP'],
].map(pair => group(pair[0], pair[1]));

test('a real feed block comes out in the panel order, not the feed order', () => {
  const out = names(orderStatGroups(NOTRE_DAME_2024));
  assert.deepEqual(out.slice(0, 5),
    ['receiving/REC', 'receiving/YDS', 'receiving/AVG', 'receiving/TD', 'receiving/LONG']);
  // Defense before fumbles, and kicking before punt returns, both of which the
  // feed had the other way round.
  assert.ok(out.indexOf('defensive/TOT') < out.indexOf('fumbles/FUM'));
  assert.ok(out.indexOf('kicking/FG') < out.indexOf('puntReturns/NO'));
});

// Pinned as literals, not read back out of STAT_CATEGORY_ORDER: comparing the
// code against itself would pass however the order were rewritten, which is the
// one thing this test exists to catch.
const EXPECTED_CATEGORIES = ['passing', 'rushing', 'receiving', 'defensive', 'interceptions',
  'fumbles', 'kicking', 'punting', 'kickReturns', 'puntReturns'];

test('the category order is the declared one whatever order the feed sends', () => {
  assert.deepEqual(Array.from(STAT_CATEGORY_ORDER), EXPECTED_CATEGORIES);
  const sent = EXPECTED_CATEGORIES.slice().reverse().map(c => group(c, 'YDS'));
  assert.deepEqual(Array.from(orderStatGroups(sent), g => g.name), EXPECTED_CATEGORIES);
});

test('nothing is dropped, duplicated or invented', () => {
  const out = orderStatGroups(NOTRE_DAME_2024);
  assert.equal(out.length, NOTRE_DAME_2024.length);
  assert.deepEqual(names(out).slice().sort(), names(NOTRE_DAME_2024).slice().sort());
  // Array.from: an array built inside the vm context is not reference-equal to
  // a host array of the same contents, so deepEqual rejects it outright.
  assert.deepEqual(Array.from(orderStatGroups([])), []);
  assert.deepEqual(Array.from(orderStatGroups(undefined)), []);
});

// A stat type this project has never seen must still render. Guessing a place
// for it would move it around between games, which is the bug being fixed.
test('an unlisted category or type keeps its feed order, after the listed ones', () => {
  const sent = [
    group('twoPointConversions', 'ATT'), group('twoPointConversions', 'MADE'),
    group('passing', 'TD'), group('passing', 'C/ATT'),
    group('rushing', 'NEW STAT'), group('rushing', 'CAR'),
  ];
  assert.deepEqual(names(orderStatGroups(sent)), [
    'passing/C/ATT', 'passing/TD',
    'rushing/CAR', 'rushing/NEW STAT',
    'twoPointConversions/ATT', 'twoPointConversions/MADE',
  ]);
});

test('the order does not depend on the order it was given in', () => {
  const shuffled = NOTRE_DAME_2024.slice().reverse();
  assert.deepEqual(names(orderStatGroups(shuffled)), names(orderStatGroups(NOTRE_DAME_2024)));
});

// ---- Schedule shading ----
const pct = attr => Number(/ ([\d.]+)%, transparent/.exec(attr)[1]);

test('a win shades green and a loss red', () => {
  assert.match(eloShade(12), /class="elo-shade"/);
  assert.match(eloShade(12), /var\(--green-hl\)/);
  assert.match(eloShade(-12), /var\(--red\)/);
});

test('the shade deepens with the size of the Elo move, and saturates', () => {
  assert.ok(pct(eloShade(2)) < pct(eloShade(10)));
  assert.ok(pct(eloShade(10)) < pct(eloShade(24)));
  // Past the saturation point every row is already as dark as the table goes,
  // so a 40-point swing and a 400-point one look the same rather than one of
  // them turning the row into a solid block.
  assert.equal(pct(eloShade(40)), pct(eloShade(400)));
  assert.equal(pct(eloShade(-40)), pct(eloShade(40)));
  // Loud enough to see, quiet enough to read the numbers through.
  assert.ok(pct(eloShade(0.1)) >= 4 && pct(eloShade(400)) <= 25);
});

test('a row the model has not rated gets no shade at all', () => {
  for (const value of [null, undefined, NaN, 0, 'W']) assert.equal(eloShade(value), '');
});
