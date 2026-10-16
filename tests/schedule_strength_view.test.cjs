// The Schedule Strength view.
//
// The measurement is tested in tests/test_schedule_strength.py. What can go
// wrong here is presentation, and three of those are specific to this table:
//
//   * a missing remaining number rendering as a rating rather than a dash,
//     which would read as "this team has an easy run left" when the truth is
//     "there is nothing left";
//   * a column failing to sort, which is the whole point of a league-wide
//     table and the failure PR #85 was about: one non-numeric cell sends the
//     entire column to a text sort, so every cell carries its own data-sort;
//   * the copy claiming a remaining half on a season that has none.
//
// Every helper is lifted from the shell for real rather than stubbed. Stubbing
// the helper a view depends on is how #71 silently replaced #72's statCell()
// and put an em dash in every player stat on the live site.
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

const SEASON = {
  season: 2026,
  remaining_games: 432,
  remaining_team_games: 864,
  teams: [
    {team_id: 1, team: 'Missouri', conference: 'SEC', played_games: 6, played_sos: 1582.2,
     played_rank: 24, remaining_games: 6, remaining_sos: 1733.8, remaining_rank: 1,
     remaining_unrated: 0, full_sos: 1664.9, full_rank: 1},
    {team_id: 2, team: 'Texas', conference: 'SEC', played_games: 6, played_sos: 1669.3,
     played_rank: 2, remaining_games: 6, remaining_sos: 1645.0, remaining_rank: 16,
     remaining_unrated: 0, full_sos: 1655.1, full_rank: 2},
    // Nothing left to play: the remaining columns must read as absent.
    {team_id: 3, team: 'Kennesaw State', conference: 'Conference USA', played_games: 12,
     played_sos: 1402.4, played_rank: 130, remaining_games: 0, remaining_sos: null,
     remaining_rank: null, remaining_unrated: 2, full_sos: 1402.4, full_rank: 130},
  ],
  conferences: [
    {conference: 'SEC', teams: 16, played_sos: 1597.1, remaining_sos: 1672.3, full_sos: 1637.0},
    {conference: 'Conference USA', teams: 10, played_sos: 1402.4, remaining_sos: null,
     full_sos: 1402.4},
  ],
};

function view(data, manifestSeasons) {
  const ctx = {
    STATIC_MANIFEST: {schedule_strength: manifestSeasons === undefined
      ? {2026: 'data/schedule_strength/2026.js?v=abc'} : manifestSeasons},
    state: {year: 2026, section: 'rankings', subview: 'schedule'},
    esc: s => String(s),
    teamLink: name => '<a class="t">' + name + '</a>',
    conferenceLink: name => '<a class="c">' + name + '</a>',
    logoMarkup: () => '',
  };
  vm.runInNewContext(
    sourceOf('eloValue') + '\n' + sourceOf('scheduleCell') + '\n' +
    sourceOf('scheduleStrengthConferences') + '\n' + sourceOf('scheduleStrengthPanels') + '\n' +
    sourceOf('renderScheduleStrength') + '\n' +
    'this.panels = scheduleStrengthPanels; this.render = renderScheduleStrength;' +
    'this.cell = scheduleCell;',
    ctx);
  return {html: data ? ctx.panels(data) : null, ctx};
}

test('a team with nothing left to play shows a dash, never a rating', () => {
  const {html} = view(SEASON);
  const rows = html.split('<tr>').filter(r => r.includes('Kennesaw State'));
  assert.equal(rows.length, 1);
  // Three value columns: played, remaining, full. The middle one is absent.
  const cells = rows[0].match(/<td class="num"[^>]*>([^<]*)/g);
  assert.ok(cells[1].includes('—'), 'remaining should be a dash, got ' + cells[1]);
  // And it is NOT the full-schedule number copied across, which is what a
  // missing-value fallback would most plausibly do.
  assert.ok(!cells[1].includes('1402'));
});

test('every value cell carries its own sort key, so one dash cannot break the column', () => {
  // ui/stats.js reads data-sort per cell on the click; a column where one cell
  // has no key falls back to that cell's text and sorts the whole column as
  // text (ranks as 1, 10, 100). This is PR #85's bug.
  const {html} = view(SEASON);
  // The rating cells: three per team row and three per conference row. The
  // conference "Teams" count is excluded deliberately -- its text IS its
  // value, so it needs no key, and requiring one there would be noise.
  const valueCells = html.match(/<td class="num" data-sort=|<td class="num">/g) || [];
  const unkeyed = valueCells.filter(c => !c.includes('data-sort'));
  assert.equal(unkeyed.length, 2, 'only the two conference team-counts may go unkeyed');
  assert.equal(valueCells.length - unkeyed.length, 3 * 3 + 2 * 3);
  // The key is the raw number, not the rendered text with its rank beside it.
  assert.match(html, /data-sort="1582\.2"/);
  assert.match(html, /data-sort="1733\.8"/);
});

test('an absent value sorts as absent rather than as zero', () => {
  // data-sort="0" would rank "nothing left to play" as the easiest possible
  // remaining schedule instead of as no schedule.
  const {ctx} = view(SEASON);
  const absent = ctx.cell(null, null);
  assert.ok(absent.includes('data-sort=""'), absent);
  assert.ok(!absent.includes('data-sort="0"'));
});

test('the rating is rounded to a whole number, as every Elo on the site is', () => {
  const {html} = view(SEASON);
  assert.match(html, />1582 </);         // 1582.2, with its rank after it
  assert.match(html, />1734 </);         // 1733.8
  // The decimal survives in the sort key, where it belongs, and nowhere a
  // reader sees it.
  assert.match(html, /data-sort="1582\.2"/);
  const rendered = html.replace(/data-sort="[^"]*"/g, '');
  assert.doesNotMatch(rendered, /1582\.2/);
  assert.doesNotMatch(rendered, /1733\.8/);
});

test('a rank is shown beside the value but is not what sorts the column', () => {
  const {html} = view(SEASON);
  assert.match(html, /#24</);
  assert.match(html, /#1</);
  // The rank must not be the sort key, or the column would sort backwards.
  assert.doesNotMatch(html, /data-sort="24"/);
});

test('the note counts the games left and says what the ranks are out of', () => {
  const {html} = view(SEASON);
  assert.match(html, /432 games still to be played/);
  // Three teams in the fixture, all rated.
  assert.match(html, /out of the 3 rated teams, 1 = toughest/);
  assert.match(html, /rank within 2026 and do not compare across eras/);
});

test('a finished season says so instead of claiming a remaining half', () => {
  const done = {...SEASON, remaining_games: 0, remaining_team_games: 0,
    teams: SEASON.teams.map(t => ({...t, remaining_games: 0, remaining_sos: null,
      remaining_rank: null, full_sos: t.played_sos, full_rank: t.played_rank}))};
  const {html} = view(done);
  assert.match(html, /season is over, so nothing remains/);
  assert.doesNotMatch(html, /still to be played/);
  // The "the split is the point" paragraph is about a live season only.
  assert.doesNotMatch(html, /hard games and one that still has them/);
});

test('a live season explains why the two columns are worth reading together', () => {
  const {html} = view(SEASON);
  assert.match(html, /hard games and one that still has them/);
});

test('unrated remaining opponents are reported rather than silently dropped', () => {
  const {html} = view(SEASON);
  assert.match(html, /2 remaining games against an opponent this project does not rate/);
  assert.match(html, /1500 prior/);
});

test('no unrated remaining opponents means no note about them', () => {
  const clean = {...SEASON, teams: SEASON.teams.map(t => ({...t, remaining_unrated: 0}))};
  const {html} = view(clean);
  assert.doesNotMatch(html, /1500 prior/);
});

test('the conference table averages members and tolerates a missing half', () => {
  const {html} = view(SEASON);
  const section = html.slice(html.indexOf('By conference'));
  assert.match(section, />1597</);                 // SEC played
  assert.match(section, />1672</);                 // SEC remaining
  const usa = section.split('<tr>').find(r => r.includes('Conference USA'));
  assert.ok(usa.includes('—'), 'a conference with nothing left shows a dash');
});

test('a season with no export renders a reason, not an empty table', () => {
  const {ctx} = view(null, {});
  const html = ctx.render();
  assert.match(html, /2026 has no rated games/);
  assert.doesNotMatch(html, /<table/);
});

test('a season with an export renders a loading placeholder for the fetch to fill', () => {
  const {ctx} = view(null);
  const html = ctx.render();
  assert.match(html, /id="ss-body"/);
  assert.match(html, /Loading schedules/);
});

test('no rated teams in the export is reported, not drawn as an empty table', () => {
  const {html} = view({...SEASON, teams: [], conferences: []});
  assert.match(html, /No rated teams in 2026/);
  assert.doesNotMatch(html, /<table/);
});

test('the view is a registered rankings route, or the tab would fall back to Top 25', () => {
  // navigation.js validates the subview against its own list and silently
  // substitutes the first entry when it does not match. The view rendered
  // fine while this was missing and the tab simply never opened.
  const nav = fs.readFileSync('ui/navigation.js', 'utf8');
  const rankings = nav.match(/rankings: \[([^\]]*)\]/);
  assert.ok(rankings, 'rankings route list missing');
  assert.match(rankings[1], /'schedule'/);
  assert.match(shell, /\['schedule', 'Schedule Strength'\]/);
  assert.match(shell, /schedule: renderScheduleStrength/);
  assert.match(shell, /subview === 'schedule'\) hydrateScheduleStrength/);
});
