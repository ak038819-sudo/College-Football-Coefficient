const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const shell = fs.readFileSync('ui/dashboard_shell.html', 'utf8');
const source = shell.match(/function visibleLiveGames\([^)]*\) \{[\s\S]*?\n\}/)?.[0];
assert.ok(source, 'missing live game selector');

test('status filter and kickoff order are independent of the featured order', () => {
  const ctx = {compareHype: (a, b) => b.hype - a.hype};
  vm.runInNewContext(source + '\nthis.select = visibleLiveGames;', ctx);
  const games = [
    {id: 3, status: 'completed', start_date: '2026-09-26T20:00:00Z', hype: 20},
    {id: 2, status: 'scheduled', start_date: '2026-10-03T19:00:00Z', hype: 90},
    {id: 1, status: 'scheduled', start_date: '2026-10-03T16:00:00Z', hype: 5},
    {id: 4, status: 'in_progress', start_date: '2026-09-26T16:00:00Z', hype: 50},
  ];
  const ids = (filter, order) => Array.from(ctx.select(games, filter, order), g => g.id);
  assert.deepEqual(ids('all', 'hype'), [2, 4, 3, 1]);
  assert.deepEqual(ids('scheduled', 'time'), [1, 2]);
  assert.deepEqual(ids('completed', 'hype'), [3]);
  assert.deepEqual(ids('in_progress', 'time'), [4]);
  assert.deepEqual(ids('all', 'time'), [4, 3, 1, 2]);
  assert.deepEqual(games.map(g => g.id), [3, 2, 1, 4], 'the original slate remains in feed order');
});
