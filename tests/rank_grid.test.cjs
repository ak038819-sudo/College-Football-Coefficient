// Every team's rank, week by week.
//
// The grid is one row per team and one column per week, built from the payload
// src/elo_timeline.py already writes. What can go wrong is reading it: lining a
// poll's releases up with the Elo weeks (they are not the same moments), losing
// a team that was unranked early, sorting a column of ranks as text because one
// cell is empty, and claiming movement where the previous column has none.
//
// Helpers are lifted from the shell for real rather than stubbed -- stubbing the
// helper a view depends on is how #71 silently replaced #72's statCell().
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
function constOf(name) {
  const match = shell.match(new RegExp(`const ${name} = \\[[\\s\\S]*?\\];`));
  assert.ok(match, `missing ${name}`);
  return match[0];
}

// Three Elo stages and two AP releases, deliberately a different number of
// each: a grid that silently reuses one track's columns for the other would
// read a rank out of a column that does not exist.
//
// Rising enters the AP poll only in the second release (so it has no movement
// to claim), Falling drops out of it (so its latest standing is the release it
// was last in), and Nowhere is rated by Elo and never ranked by a ballot.
const DATA = {
  season: 2026,
  teams: [
    {id: 1, name: 'Rising', conference: 'Big Ten'},
    {id: 2, name: 'Falling', conference: 'SEC'},
    {id: 3, name: 'Nowhere', conference: 'Big Ten'},
  ],
  stages: [
    {key: 'pre', label: 'Preseason', week: null, games: 0},
    {key: 'w1', label: 'Week 1', week: 1, games: 2},
    {key: 'post', label: 'Postseason', week: null, games: 1},
  ],
  // `team` is the team's INDEX into the teams list above, not its id -- which
  // is what the payload src/elo_timeline.py writes carries.
  rows: [
    [{team: 0, elo: 1600, rank: 1, d_elo: null, d_rank: null, stage_games: 0},
     {team: 1, elo: 1550, rank: 2, d_elo: null, d_rank: null, stage_games: 0},
     {team: 2, elo: 1400, rank: 3, d_elo: null, d_rank: null, stage_games: 0}],
    [{team: 1, elo: 1580, rank: 1, d_elo: 30, d_rank: 1, stage_games: 1},
     {team: 0, elo: 1570, rank: 2, d_elo: -30, d_rank: -1, stage_games: 1},
     {team: 2, elo: 1400, rank: 3, d_elo: 0, d_rank: 0, stage_games: 0}],
    [{team: 0, elo: 1610, rank: 1, d_elo: 40, d_rank: 1, stage_games: 1},
     {team: 1, elo: 1580, rank: 2, d_elo: 0, d_rank: -1, stage_games: 0},
     {team: 2, elo: 1400, rank: 3, d_elo: 0, d_rank: 0, stage_games: 0}],
  ],
  polls: {
    ap: {
      releases: [
        {key: 'r1', label: 'Week 1', season_type: 'regular', week: 1},
        {key: 'p1', label: 'Final', season_type: 'postseason', week: 1},
      ],
      rows: [
        [{team: 1, school: 'Falling', rank: 5, d_rank: null},
         {team: null, school: 'Unmatched State', rank: 24, d_rank: null}],
        [{team: 0, school: 'Rising', rank: 13, d_rank: null},
         {team: null, school: 'Unmatched State', rank: 20, d_rank: 4}],
      ],
    },
  },
};

function load(extra = {}) {
  const ctx = {
    esc: s => String(s),
    teamLink: name => '<a class="t">' + name + '</a>',
    ...extra,
  };
  vm.runInNewContext(
    constOf('WEEKLY_TRACKS') + '\n' + sourceOf('weeklyTrackLabel') + '\n' +
    sourceOf('eloValue') + '\n' + sourceOf('signed') + '\n' +
    sourceOf('rankBand') + '\n' + sourceOf('weeklyColumns') + '\n' +
    sourceOf('weeklyGridRows') + '\n' + sourceOf('weeklyGridCell') + '\n' +
    sourceOf('weeklyGridTable') + '\n' + sourceOf('weeklyGridNote'), ctx);
  return ctx;
}

const ctx = load();
const byName = grid => Object.fromEntries(grid.rows.map(r => [r.name, r]));

// --------------------------------------------------------------------------
// Columns: each track brings its own
// --------------------------------------------------------------------------

test('the Elo track is one column per stage, preseason and postseason named', () => {
  const columns = ctx.weeklyColumns(DATA, 'elo');
  assert.deepEqual(Array.from(columns.map(c => c.label)), ['Pre', 'W1', 'Post']);
  assert.deepEqual(Array.from(columns.map(c => c.key)), ['pre', 'w1', 'post']);
});

test('a poll track is one column per RELEASE, not per Elo stage', () => {
  const columns = ctx.weeklyColumns(DATA, 'ap');
  // Two releases against three Elo stages: the counts must not be confused.
  assert.equal(columns.length, 2);
  assert.notEqual(columns.length, ctx.weeklyColumns(DATA, 'elo').length);
  assert.deepEqual(Array.from(columns.map(c => c.label)), ['W1', 'Final']);
  assert.ok(columns[0].title.includes('AP Poll'));
});

test('a track the season never released has no columns rather than throwing', () => {
  assert.deepEqual(Array.from(ctx.weeklyColumns(DATA, 'cfp')), []);
  assert.deepEqual(Array.from(ctx.weeklyColumns({...DATA, polls: {}}, 'ap')), []);
});

// --------------------------------------------------------------------------
// Rows
// --------------------------------------------------------------------------

test('every rated team gets a row with one cell per column, in column order', () => {
  const grid = ctx.weeklyGridRows(DATA, 'elo');
  assert.equal(grid.rows.length, 3);
  for (const row of grid.rows) assert.equal(row.cells.length, 3);
  assert.deepEqual(Array.from(byName(grid)['Rising'].cells.map(c => c.rank)), [1, 2, 1]);
  assert.deepEqual(Array.from(byName(grid)['Falling'].cells.map(c => c.rank)), [2, 1, 2]);
});

test('rows are ordered by where the team stands in the last column it has', () => {
  const grid = ctx.weeklyGridRows(DATA, 'elo');
  assert.deepEqual(Array.from(grid.rows.map(r => r.name)), ['Rising', 'Falling', 'Nowhere']);
});

test('best and worst are the extremes of the ranks the team actually held', () => {
  const rising = byName(ctx.weeklyGridRows(DATA, 'elo'))['Rising'];
  assert.equal(rising.best, 1);
  assert.equal(rising.worst, 2);
  assert.equal(rising.weeks, 3);
});

test('a team unranked in a poll release gets an empty cell, not a missing row', () => {
  const grid = ctx.weeklyGridRows(DATA, 'ap');
  const rows = byName(grid);
  assert.deepEqual(Array.from(rows['Rising'].cells.map(c => c && c.rank)), [null, 13]);
  assert.deepEqual(Array.from(rows['Falling'].cells.map(c => c && c.rank)), [5, null]);
  // Rated by Elo but never on a ballot: no row at all in a poll track.
  assert.equal(rows['Nowhere'], undefined);
});

test('a poll row drops out and still sorts by where it last stood', () => {
  const rows = byName(ctx.weeklyGridRows(DATA, 'ap'));
  assert.equal(rows['Falling'].latest, 5);        // its last release, not an empty cell
  assert.equal(rows['Rising'].latest, 13);
  assert.deepEqual(Array.from(ctx.weeklyGridRows(DATA, 'ap').rows.map(r => r.name)),
    ['Falling', 'Rising', 'Unmatched State']);
});

test('a school with no canonical team keeps its own row and its raw name', () => {
  const row = byName(ctx.weeklyGridRows(DATA, 'ap'))['Unmatched State'];
  assert.equal(row.team, null);
  assert.deepEqual(Array.from(row.cells.map(c => c.rank)), [24, 20]);
});

test('a row carries the season conference, so a filter can cut the grid', () => {
  const rows = byName(ctx.weeklyGridRows(DATA, 'elo'));
  assert.equal(rows['Rising'].conference, 'Big Ten');
  assert.equal(rows['Falling'].conference, 'SEC');
});

// --------------------------------------------------------------------------
// Cells
// --------------------------------------------------------------------------

test('the bands are the top four, the playoff field and the top 25', () => {
  assert.deepEqual([1, 4, 5, 12, 13, 25, 26, null].map(ctx.rankBand),
    ['band-bye', 'band-bye', 'band-field', 'band-field',
     'band-ranked', 'band-ranked', '', '']);
});

test('a rank cell carries its own numeric sort key', () => {
  const column = ctx.weeklyColumns(DATA, 'elo')[1];
  assert.match(ctx.weeklyGridCell({rank: 7, elo: 1600, d_rank: 2}, column, 'Rising'), /data-sort="7"/);
  // Never 0: that is a rank no team ever held, and it would sort as the best.
  assert.doesNotMatch(ctx.weeklyGridCell(null, column, 'Rising'), /data-sort="0"/);
});

// A poll ranks 25 teams out of every team that has been ranked all season, so
// most of a poll column is empty -- below the density at which CfbStats keeps a
// column on a numeric sort. The whole path matters here, not the attribute:
// enhanceTable falls back to the cell's own TEXT when data-sort is empty, so an
// empty key reads as "no value" to the eye and as the character '\u00b7' to the
// sorter, and sorting the column descending put every unranked team on top.
const SPARSE = {
  season: 2026,
  teams: [0, 1, 2, 3, 4, 5].map(i => ({id: i + 1, name: 'Team ' + i, conference: 'Big Ten'})),
  stages: [], rows: [],
  polls: {
    ap: {
      releases: [{key: 'r1', label: 'Week 1', season_type: 'regular', week: 1},
        {key: 'r2', label: 'Week 2', season_type: 'regular', week: 2}],
      rows: [
        [{team: 0, school: 'Team 0', rank: 9, d_rank: null},
         {team: 1, school: 'Team 1', rank: 14, d_rank: null}],
        [{team: 2, school: 'Team 2', rank: 1, d_rank: null},
         {team: 3, school: 'Team 3', rank: 2, d_rank: null},
         {team: 4, school: 'Team 4', rank: 3, d_rank: null},
         {team: 5, school: 'Team 5', rank: 4, d_rank: null}],
      ],
    },
  },
};

function sortedColumn(columnIndex, clicks) {
  const { JSDOM } = require('jsdom');
  const stats = require('../ui/stats.js');
  const grid = ctx.weeklyGridRows(SPARSE, 'ap');
  const dom = new JSDOM('<main>' + ctx.weeklyGridTable(grid) + '</main>');
  const table = dom.window.document.querySelector('table');
  stats.enhanceTable(table);          // the real sort wiring, not a stand-in
  const button = table.tHead.rows[0].cells[columnIndex].querySelector('.sort-button');
  assert.ok(button, 'the column must be sortable at all');
  for (let i = 0; i < clicks; i++) button.click();
  return [...table.tBodies[0].rows].map(row => row.cells[columnIndex].textContent.trim());
}

test('a sparse poll column sorts its ranked teams above the unranked, either way', () => {
  // One click is descending, two is ascending (ui/stats.js starts at ascending
  // and flips on each click).
  assert.deepEqual(Array.from(sortedColumn(1, 1)).slice(0, 2), ['14', '9']);
  assert.deepEqual(Array.from(sortedColumn(1, 2)).slice(0, 2), ['9', '14']);
  for (const clicks of [1, 2]) {
    assert.deepEqual(Array.from(sortedColumn(1, clicks)).slice(2), ['\u00b7', '\u00b7', '\u00b7', '\u00b7'],
      'four unranked teams, last, after ' + clicks + ' click(s)');
  }
});

test('a cell marks which way the team moved, and says nothing when it cannot', () => {
  const column = ctx.weeklyColumns(DATA, 'elo')[1];
  assert.match(ctx.weeklyGridCell({rank: 3, elo: 1600, d_rank: 4}, column, 'T'), /grid-rank [a-z-]* up"/);
  assert.match(ctx.weeklyGridCell({rank: 3, elo: 1600, d_rank: -4}, column, 'T'), /grid-rank [a-z-]* down"/);
  const flat = ctx.weeklyGridCell({rank: 3, elo: 1600, d_rank: 0}, column, 'T');
  assert.doesNotMatch(flat, / (up|down)"/);
  assert.match(flat, /no move/);
  // No previous column to measure against is not "stood still".
  const first = ctx.weeklyGridCell({rank: 3, elo: 1600, d_rank: null}, column, 'T');
  assert.doesNotMatch(first, / (up|down)"/);
  assert.doesNotMatch(first, /no move/);
});

test('an Elo cell names the rating in its tooltip and a poll cell does not', () => {
  const column = ctx.weeklyColumns(DATA, 'elo')[1];
  assert.match(ctx.weeklyGridCell({rank: 3, elo: 1612.4, d_rank: 1}, column, 'T'), /1612 Elo/);
  assert.doesNotMatch(ctx.weeklyGridCell({rank: 3, d_rank: 1}, column, 'T'), /Elo/);
});

// --------------------------------------------------------------------------
// The table and what it says about itself
// --------------------------------------------------------------------------

test('the table has a header cell for every column plus team, best and worst', () => {
  const grid = ctx.weeklyGridRows(DATA, 'elo');
  const html = ctx.weeklyGridTable(grid);
  assert.equal((html.match(/<th /g) || []).length, grid.columns.length + 3);
  assert.equal((html.match(/<tr>/g) || []).length, grid.rows.length + 1);
  // One row per team and one cell per column, or the grid is misaligned.
  for (const row of html.split('<tbody>')[1].split('<tr>').slice(1)) {
    assert.equal((row.match(/<td/g) || []).length, grid.columns.length + 3);
  }
});

test('a team with a page is linked and an unmatched school is plain text', () => {
  const html = ctx.weeklyGridTable(ctx.weeklyGridRows(DATA, 'ap'));
  assert.match(html, /<a class="t">Rising<\/a>/);
  assert.match(html, /<div class="name-cell">Unmatched State<\/div>/);
});

test('the poll note says an empty cell is unranked and the columns are its own', () => {
  const note = ctx.weeklyGridNote('ap', 2026, 3, 2);
  assert.match(note, /unranked/);
  assert.match(note, /OWN releases/);
  // The Elo note must NOT carry the poll caveat: its columns ARE the weeks.
  assert.doesNotMatch(ctx.weeklyGridNote('elo', 2026, 138, 17), /unranked|OWN releases/);
  assert.match(ctx.weeklyGridNote('elo', 2026, 138, 17), /Preseason/);
});

test('the note counts the rows and columns it was handed, singular included', () => {
  assert.match(ctx.weeklyGridNote('ap', 2026, 3, 1), /1 release\./);
  assert.match(ctx.weeklyGridNote('ap', 2026, 3, 2), /2 releases\./);
  assert.match(ctx.weeklyGridNote('elo', 2026, 138, 17), /138 teams across 17/);
});
