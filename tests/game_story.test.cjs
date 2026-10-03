const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const shell = fs.readFileSync('ui/dashboard_shell.html', 'utf8');
function sourceOf(name) {
  const source = shell.match(new RegExp(`function ${name}\\([^)]*\\) \\{[\\s\\S]*?\\n\\}`))?.[0];
  assert.ok(source, `missing ${name}`);
  return source;
}
const ctx = {esc: value => String(value).replaceAll('&', '&amp;').replaceAll('<', '&lt;'),
  stadiumHref: id => `#section=teams&view=stadiums&stadium=${id}`};
vm.runInNewContext(sourceOf('gameStory') + '\n' + sourceOf('storyPlayerLeaders') +
  '\nthis.story = gameStory; this.leaders = storyPlayerLeaders;', ctx);

test('completed game story uses stored outcome, venue, SR+ and Elo change', () => {
  const g = {completed: true, home_score: 20, away_score: 27, p_home: 0.8,
    stadium_id: 42, stadium: 'The Ground', home_elo_change: -18.2};
  const html = ctx.story(g, {home_sr_plus: -0.08, away_sr_plus: 0.08}, 2025, 'Visitors', 'Hosts');
  assert.match(html, /Visitors beat Hosts, 27–20.*20% pregame win chance/);
  assert.match(html, /The Ground<\/a>/);
  assert.match(html, /Visitors \+8\.0 pp/);
  assert.match(html, /Visitors \+18\.2/);
  assert.match(html, /#gp-efficiency/);
});

test('missing stats stay absent, and upcoming game shows stored win chance', () => {
  const g = {completed: false, neutral: true, p_home: 0.65, stadium: 'Unverified Park', home_elo_change: null};
  const html = ctx.story(g, null, 2026, 'Visitors', 'Hosts');
  assert.match(html, /Hosts 65%/);
  assert.match(html, /Unverified Park \(venue unverified\)/);
  assert.doesNotMatch(html, /#gp-efficiency|#gp-players|Explore the stadium/);
  assert.match(html, /Efficiency edge<\/small><strong>Unavailable/);
  assert.match(ctx.story({...g, completed: true, home_score: 7, away_score: 7},
    {home_sr_plus: 0, away_sr_plus: 0}, 2026, 'Visitors', 'Hosts'),
    /Efficiency edge<\/small><strong>Even/);
});

test('player leaders use only numeric yardage from published categories', () => {
  const html = ctx.leaders([
    {name: 'Hosts', categories: [{name: 'Passing', type: 'YDS', lines: [
      {name: 'A', stat: '230'}, {name: 'B', stat: '180'}]},
    {name: 'Passing', type: 'TD', lines: [{name: 'C', stat: '4'}]}]},
    {name: 'Visitors', categories: [{name: 'Passing', type: 'YDS', lines: [{name: 'D', stat: '301'}]},
      {name: 'Rushing', type: 'YDS', lines: [{name: '<Runner>', stat: '115'}]}]},
  ]);
  assert.match(html, /D<\/strong><p>Visitors · 301 yards/);
  assert.match(html, /&lt;Runner>/);
  assert.doesNotMatch(html, /C<\/strong>|Receiving<\/small>/);
  assert.match(ctx.leaders([]), /not available/);
});

test('active game story waits for the official model update', () => {
  const live = {esc: ctx.esc, stadiumHref: ctx.stadiumHref, storyPlayerLeaders: ctx.leaders,
    liveSnapshot: {player_boxscores: {}}, liveSeasonData: {byId: new Map()},
    UPCOMING_BY_ID: new Map(), livePrediction: () => ({pHome: 0.7}),
    probPct: () => ({home: 70, away: 30}), liveTeamId: t => ({name: t.name}),
    liveSeason: () => 2026, liveLabel: () => 'Final', teamLink: name => name,
    liveEloPanel: () => 'Elo table', livePlayerPanel: () => 'Player box',
  };
  vm.runInNewContext(sourceOf('liveDetail') + '\nthis.detail = liveDetail;', live);
  const html = live.detail({id: 12, status: 'completed', venue: 'Sample Field',
    away: {name: 'Visitors', points: 27}, home: {name: 'Hosts', points: 20}});
  assert.match(html, /Game Story/);
  assert.match(html, /Visitors beat Hosts, 27–20/);
  assert.match(html, /Model update<\/small><strong>Pending/);
  assert.match(html, /Sample Field \(venue unverified\)/);
});
