const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const shell = fs.readFileSync('ui/dashboard_shell.html', 'utf8');
function sourceOf(name) {
  const match = shell.match(new RegExp(`function ${name}\\([^)]*\\) \\{[\\s\\S]*?\\n\\}`));
  assert.ok(match, `missing ${name}`);
  return match[0];
}

test('published game Elo movement is green/up or red/down; missing and zero stay blank', () => {
  const ctx = {};
  vm.runInNewContext(sourceOf('eloChangeMarkup') + '\nthis.delta = eloChangeMarkup;', ctx);
  assert.match(ctx.delta(14.5), /class="elo-change up"[^>]*>\(▲14\.5\)/);
  assert.match(ctx.delta(-8.2), /class="elo-change down"[^>]*>\(▼8\.2\)/);
  assert.equal(ctx.delta(null), '');
  assert.equal(ctx.delta(0), '');
});

test('Elo rankings pass each audited change to the table', () => {
  const ctx = {
    DATA: {team_elo_by_year: {2026: [{team: 'Florida', elo: 1551.4, change: -8.2}]}},
    state: {year: 2026}, UPCOMING: {season: 2026, ratings_as_of: '2026-09-20'},
    esc: x => x, rankTable: rows => JSON.stringify(rows),
  };
  vm.runInNewContext(sourceOf('renderElo') + '\nthis.render = renderElo;', ctx);
  assert.match(ctx.render(), /"change":-8\.2/);
});

test('a newer final marks only its teams as waiting for a model rebuild', () => {
  const ctx = {liveTeamId: t => ({name: t.name})};
  vm.runInNewContext(sourceOf('newerFinalTeams') + '\nthis.newer = newerFinalTeams;', ctx);
  const games = [
    {status: 'completed', start_date: '2026-09-26T19:30:00Z', away: {name: 'Ole Miss'}, home: {name: 'Florida'}},
    {status: 'scheduled', start_date: '2026-10-01T19:30:00Z', away: {name: 'BYU'}, home: {name: 'Utah'}},
    {status: 'completed', start_date: '2026-09-20T19:30:00Z', away: {name: 'Texas'}, home: {name: 'Georgia'}},
  ];
  assert.deepEqual([...ctx.newer(games, '2026-09-20')].sort(), ['Florida', 'Ole Miss']);
  assert.equal(ctx.newer(games, '2026-09-26').size, 0);
});
