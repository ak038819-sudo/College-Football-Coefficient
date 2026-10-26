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

test('a rating with no value reads as unavailable, not as a zero Elo', () => {
  // A completed game can have no Elo history on either side, and the export
  // emits null for it. Number(null) is 0, so the guard is what keeps the game
  // page from presenting a missing rating as a real 0.
  const ctx = {};
  vm.runInNewContext(sourceOf('eloValue') + '\nthis.elo = eloValue;', ctx);
  assert.equal(ctx.elo(null), '\u2014');
  assert.equal(ctx.elo(undefined), '\u2014');
  assert.equal(ctx.elo(1551.4), '1551');
  assert.equal(ctx.elo(1551.5), '1552');
});

test('Elo rankings pass each audited change to the table', () => {
  const ctx = {
    DATA: {team_elo_by_year: {2026: [{team: 'Florida', elo: 1551.4, change: -8.2}]},
      team_records_by_year: {2026: {5: [3, 2, 0]}}, non_fbs_records_by_year: {2026: {5: [1, 0, 0]}}},
    state: {year: 2026}, LATEST_SEASON: 2026, UPCOMING: {season: 2026, ratings_as_of: '2026-09-20'},
    TEAM_BY_NAME: new Map([['Florida', {id: 5, name: 'Florida', conference: 'SEC'}]]),
    esc: x => x, teamLink: name => '<a>' + name + '</a>', conferenceLink: name => '<a>' + name + '</a>',
  };
  // eloValue, eloChangeMarkup and seasonRecord are the real helpers, lifted in:
  // the row is what renders a rating, its audited change and the record, so a
  // stub here would stop proving any of the three.
  vm.runInNewContext([sourceOf('eloValue'), sourceOf('eloChangeMarkup'), sourceOf('seasonRecord'),
    sourceOf('eloHead'), sourceOf('rankingStageMount'), sourceOf('renderElo'),
    "const ELO_COLUMNS = ['Team', 'Conf', 'Record', 'Elo', 'Last 5', 'SOS', 'vs Exp'];",
    'this.render = renderElo;'].join('\n'), ctx);
  const html = ctx.render();
  // The rating rounds to a whole number; the audited change keeps its decimal.
  assert.match(html, />1551<span class="elo-change down"[^>]*>\(▼8\.2\)</);
  // The record is the whole season's, FCS win included.
  assert.match(html, />4–2</);
  assert.match(html, /<a>SEC<\/a>/);
});

test('a newer final marks only its teams as waiting for a model rebuild', () => {
  // "Newer" means the scoreboard has a final the published model still lists as
  // upcoming. The Ole Miss game is the one the export has not absorbed; the
  // Georgia game is already rated, so its teams are not pending.
  const ctx = {
    liveTeamId: t => ({name: t.name}),
    UPCOMING_BY_ID: new Map([[11, {}], [22, {}]]),
  };
  vm.runInNewContext(sourceOf('pendingFinals') + '\n' + sourceOf('newerFinalTeams') +
    '\nthis.newer = newerFinalTeams;', ctx);
  const games = [
    {id: 11, status: 'completed', start_date: '2026-09-26T19:30:00Z', away: {name: 'Ole Miss'}, home: {name: 'Florida'}},
    {id: 22, status: 'scheduled', start_date: '2026-10-01T19:30:00Z', away: {name: 'BYU'}, home: {name: 'Utah'}},
    {id: 33, status: 'completed', start_date: '2026-09-20T19:30:00Z', away: {name: 'Texas'}, home: {name: 'Georgia'}},
  ];
  assert.deepEqual([...ctx.newer(games)].sort(), ['Florida', 'Ole Miss']);
  assert.equal(ctx.newer(games.filter(g => g.id !== 11)).size, 0);
});
