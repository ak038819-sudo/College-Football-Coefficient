/* The Top 25 poll boards for a season that is not the current one.
 *
 * `poll_rankings` held 325 rows, all of 2026, so every past season's board
 * read "not in this export". With the AP archive back to 1936 the data is
 * there, in that season's Elo timeline file -- these drive the bridge between
 * the two shapes.
 *
 * The fetcher side is tests/test_rankings_backfill.py.
 */
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const shell = fs.readFileSync('ui/dashboard_shell.html', 'utf8');

for (const name of ['pollRelease', 'pollReleaseFromTimeline', 'top25PollBoard',
  'top25BoardsHtml', 'renderTop25Boards', 'hydrateTop25Polls', 'pollTitle']) {
  // Two declarations of one name silently replace each other here, and a test
  // that lifts BY NAME never notices which one it got.
  const found = shell.match(new RegExp('function ' + name + '\\(', 'g')) || [];
  assert.equal(found.length, 1, name + ' must be declared exactly once in the shell');
}

const lift = name => shell.match(new RegExp('function ' + name + '\\([^]*?\\n\\}'))[0];

const context = {
  esc: v => String(v).replaceAll('&', '&amp;').replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;').replaceAll('"', '&quot;'),
  pollTitle: key => (key === 'cfp' ? 'CFP Rankings' : 'AP Top 25'),
  state: { year: 1994 },
  top25EloBoard: () => '<div class="panel board-col"><h2>Elo Top 25</h2></div>',
  top25PollBoard: (key, release) => release && release.rows && release.rows.length
    ? '<div class="panel board-col"><h2>' + context.pollTitle(key) + '</h2></div>' : ''
};
vm.runInNewContext(lift('pollReleaseFromTimeline') + '\n' + lift('top25BoardsHtml') +
  '\nthis.fromTimeline = pollReleaseFromTimeline; this.boards = top25BoardsHtml;', context);
const { fromTimeline, boards } = context;

// A timeline track as loadEloTimeline hands it over: the payload's flat arrays
// have already been unpacked against poll_row_fields, so a row is an object
// keyed {team, school, rank, d_rank, votes} and `team` indexes timeline.teams.
const row = (team, school, rank, d_rank, votes) =>
  ({ team, school, rank, d_rank, votes });
const release = (key, label, season_type, week) =>
  ({ key, label, season_type, week });
const timeline = (over = {}) => Object.assign({
  teams: [{ id: 10, name: 'Alabama' }, { id: 20, name: 'Auburn' }],
  polls: {
    ap: {
      releases: [release('r1', 'Week 1', 'regular', 1),
                 release('p1', 'Final', 'postseason', 1)],
      rows: [
        [row(0, 'Alabama', 1, null, 40), row(1, 'Auburn', 2, null, 12)],
        [row(1, 'Auburn', 1, 1, 55), row(0, 'Alabama', 2, -1, 6)]
      ]
    }
  }
}, over);
const text = html => html.replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();

/* ---- reading a release out of a timeline ------------------------------- */

test('the board shows the LAST ballot of the season, not the first', () => {
  // CFBD restarts the week count in the postseason, so a final poll can be
  // numbered 1. The timeline already orders its releases; taking rows[0]
  // would show the preseason ballot as the season's result.
  const release = fromTimeline(timeline(), 'ap');
  assert.equal(release.season_type, 'postseason');
  // Array.from: a vm-context array is not deepEqual to a host array.
  assert.deepEqual(Array.from(release.rows[0]), [1, 20, 'Auburn', 55]);
});

test('a release is shaped exactly like the current season’s', () => {
  // DATA.polls rows are [rank, team_id, school, votes], and one renderer draws
  // both. A different order here would silently swap rank and id.
  const [rank, id, school, votes] = fromTimeline(timeline(), 'ap').rows[0];
  assert.equal(rank, 1);
  assert.equal(id, 20);
  assert.equal(school, 'Auburn');
  assert.equal(votes, 55);
});

test('first-place votes survive the trip, and a blank stays blank', () => {
  const tl = timeline();
  tl.polls.ap.rows[1] = [row(0, 'Alabama', 1, null, null)];
  assert.equal(fromTimeline(tl, 'ap').rows[0][3], null, 'the CFP poll reports none at all');
});

test('a school the database cannot place keeps its name and carries no id', () => {
  const tl = timeline();
  tl.polls.ap.rows[1] = [row(null, 'Centre College', 1, null, 3)];
  assert.deepEqual(Array.from(fromTimeline(tl, 'ap').rows[0]),
                   [1, null, 'Centre College', 3]);
});

test('a poll the season never had is null, not an empty board', () => {
  assert.equal(fromTimeline(timeline(), 'cfp'), null, 'the CFP poll begins in 2014');
  assert.equal(fromTimeline({ teams: [], polls: {} }, 'ap'), null);
  assert.equal(fromTimeline({ teams: [] }, 'ap'), null);
  assert.equal(fromTimeline({ teams: [], polls: { ap: { releases: [], rows: [] } } }, 'ap'), null);
});

/* ---- the note under the boards ----------------------------------------- */

test('a poll missing from a past season was never archived, not "not yet released"', () => {
  // The distinction the old wording could not make, because every past season
  // was missing: 1994 has no CFP poll because the CFP did not exist.
  const tl = timeline();
  const html = boards(key => fromTimeline(tl, key), false);
  assert.match(text(html), /CFP Rankings for 1994 is not in this export/);
  assert.doesNotMatch(text(html), /has not been released/);
});

test('a poll missing from the current season has simply not been released', () => {
  const html = boards(() => null, true);
  assert.match(text(html), /AP Top 25 and CFP Rankings have not been released for 1994/);
});

test('a season with every poll gets no note at all', () => {
  const both = key => ({ rows: [[1, 10, 'Alabama', 40]], season_type: 'regular', week: 1 });
  assert.doesNotMatch(text(boards(both, false)), /not in this export|not been released/);
});

test('the column count counts the boards that drew, so the widths are right', () => {
  assert.match(boards(() => null, true), /data-boards="1"/, 'the Elo board alone');
  assert.match(boards(key => fromTimeline(timeline(), key), false), /data-boards="2"/);
  const both = () => ({ rows: [[1, 10, 'A', 1]], season_type: 'regular', week: 1 });
  assert.match(boards(both, false), /data-boards="3"/);
});

/* ---- what the page does while it waits --------------------------------- */

test('the Elo board draws at once and the ballots fill in after', () => {
  // A reader who asked for 1994 should not watch three empty columns while a
  // timeline file loads.
  const render = lift('renderTop25Boards');
  assert.match(render, /top25-polls/);
  assert.match(render, /top25EloBoard\(\)/,
    'the Elo board must be in the mount, not only in the hydrate');
});

test('a timeline that will not load leaves the Elo board standing', () => {
  const hydrate = lift('hydrateTop25Polls');
  assert.match(hydrate, /\.catch/);
  assert.match(hydrate, /top25EloBoard\(\)/,
    'the ballots are unknown; the Elo board is not');
  assert.match(hydrate, /version !== renderVersion/,
    'and a stale load must not overwrite the season now on screen');
});
