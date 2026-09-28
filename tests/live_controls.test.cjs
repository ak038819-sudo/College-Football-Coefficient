const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const shell = fs.readFileSync('ui/dashboard_shell.html', 'utf8');
const source = shell.match(/function visibleLiveGames\([^)]*\) \{[\s\S]*?\n\}/)?.[0];
const sectionsSource = shell.match(/function liveSections\([^)]*\) \{[\s\S]*?\n\}/)?.[0];
const homeSource = shell.match(/function renderLiveScores\([^)]*\) \{[\s\S]*?\n\}/)?.[0];
assert.ok(source, 'missing live game selector');
assert.ok(sectionsSource, 'missing collapsible slate selector');
assert.ok(homeSource, 'missing Home scoreboard');

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

test('rankings start folded on narrow screens and open on desktop', () => {
  const render = mobile => {
    const ctx = {window: {matchMedia: () => ({matches: mobile})},
      renderEloTop25: () => 'Elo board', renderConferenceBoard: () => 'Conference board',
      renderPollsPanel: () => 'Polls'};
    vm.runInNewContext("let liveFilter = 'all'; let liveOrder = 'hype';\n" + homeSource +
      '\nthis.renderHome = renderLiveScores;', ctx);
    return ctx.renderHome();
  };
  assert.match(render(true), /<details class="home-side-fold"><summary>/);
  assert.match(render(false), /<details class="home-side-fold" open><summary>/);
});

test('Home shows a short current slate and folds the rest, including finals', () => {
  const ctx = {};
  vm.runInNewContext(sectionsSource + '\nthis.sections = liveSections;', ctx);
  const live = Array.from({length: 8}, (_, i) => ({id: i + 1, status: 'in_progress'}));
  const upcoming = [{id: 9, status: 'scheduled'}];
  const finals = [{id: 10, status: 'completed'}, {id: 11, status: 'completed'}];
  const slate = ctx.sections([...live, ...upcoming, ...finals], 'all');
  assert.deepEqual(Array.from(slate.lead, g => g.id), [1, 2, 3, 4, 5, 6]);
  assert.deepEqual(Array.from(slate.more, g => g.id), [7, 8, 9]);
  assert.deepEqual(Array.from(slate.finals, g => g.id), [10, 11]);
  const finished = ctx.sections(finals, 'all');
  assert.deepEqual(Array.from(finished.lead, g => g.id), [10, 11]);
  assert.equal(finished.finals.length, 0);
  assert.deepEqual(Array.from(ctx.sections(live, 'in_progress').more, g => g.id), [7, 8]);
});
