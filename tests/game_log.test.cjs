// The game-by-game panel on a player page. The lines have been in the archive
// since 2005; what was missing was a way to read one player's career out of
// it. The risks are in the reading, not the rendering: a line of zeroes the
// feed emits for every punter in every game, and a result taken from the
// wrong side of the score.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const shell = fs.readFileSync('ui/dashboard_shell.html', 'utf8');

function lift(names, extra = {}) {
  const constants = ['STAT_CATEGORY_LABELS', 'STAT_CATEGORY_ORDER', 'STAT_TYPE_ORDER',
                     'FRACTION_STATS', 'DECIMAL_STATS'].map(name => {
    // The single-line form first: a greedy multi-line match would swallow the
    // declarations after it and declare them twice in the context.
    const m = shell.match(new RegExp('^const ' + name + ' = \\{[^{}]*\\};', 'm'))
      || shell.match(new RegExp('^const ' + name + ' = \\[[^\\]]*\\];', 'm'))
      || shell.match(new RegExp('^const ' + name + ' = \\{[^]*?^\\};', 'm'));
    assert.ok(m, 'the shell must declare ' + name);
    return m[0];
  }).join('\n');
  const source = constants + '\n' + names.map(name => {
    const m = shell.match(new RegExp('^function ' + name + '\\([^]*?\\n\\}', 'm'));
    assert.ok(m, 'the shell must declare ' + name);
    return m[0];
  }).join('\n');
  const context = Object.assign({
    esc: v => String(v).replace(/[&<>]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c])),
    teamCellById: id => 'TEAM' + id,
    gameHref: (id, season) => '#game=' + id + '&season=' + season,
  }, extra);
  vm.runInNewContext(source + '\nthis.__out = {' + names.join(',') + '};', context);
  return context.__out;
}

const { gameLineSummary, gameLogPanel, playerLogSeasons } =
  lift(['statCell', 'gameLineSummary', 'gameLogPanel', 'playerLogSeasons']);

const row = (over = {}) => Object.assign({
  id: 401, date: '2025-09-06', opp: 2, home: 1, pf: 31, pa: 24,
  stats: { passing: { YDS: '233', TD: '3' } },
}, over);
const log = (...rows) => ({ 2025: rows.map(r => [r.id, r.date, r.opp, r.home, r.pf, r.pa, r.stats]) });

// ---- which seasons to fetch ---------------------------------------------------

test('the seasons asked for are the roster seasons and the stat seasons both', () => {
  // They are not the same set: a box-score line can exist in a season no
  // roster snapshot covers, and a roster season may hold no statistics.
  const seasons = playerLogSeasons({
    seasons: [{ season_year: 2023 }, { season_year: 2024 }],
    stats: { 2024: {}, 2025: {} },
  });
  assert.deepEqual(Array.from(seasons), [2025, 2024, 2023]);
});

test('a player with neither asks for nothing', () => {
  assert.deepEqual(Array.from(playerLogSeasons({})), []);
});

// ---- the line ------------------------------------------------------------------

test('a line is read in the order a box score is read in', () => {
  const out = gameLineSummary({ rushing: { TD: '1', CAR: '12', YDS: '64' },
                                passing: { YDS: '233', 'C/ATT': '18/29' } });
  assert.ok(out.indexOf('Passing') < out.indexOf('Rushing'), out);
  assert.ok(out.indexOf('C/ATT') < out.indexOf('YDS'), out);
  assert.ok(out.indexOf('CAR') < out.indexOf('TD'), out);
});

test('a category of nothing but zeroes is not printed', () => {
  // Every punter appears under punting TB and In 20 in every game with a 0.
  // Printing it says he did something; he did not.
  assert.equal(gameLineSummary({ punting: { TB: '0', 'In 20': '0' } }), '');
  assert.match(gameLineSummary({ punting: { TB: '0', 'In 20': '2' } }), /In 20/);
});

test('a real zero beside a real number stays', () => {
  const out = gameLineSummary({ passing: { YDS: '233', INT: '0' } });
  assert.match(out, /INT/);
});

test('nothing at all is an empty summary, not a crash', () => {
  assert.equal(gameLineSummary(null), '');
  assert.equal(gameLineSummary({}), '');
});

// ---- the panel -------------------------------------------------------------------

test('a player with no log gets no panel at all', () => {
  assert.equal(gameLogPanel({}), '');
  assert.equal(gameLogPanel(null), '');
});

test('a row links the game, names the opponent and reads the result', () => {
  const html = gameLogPanel(log(row()));
  assert.match(html, /href="#game=401&amp;season=2025"/);
  assert.match(html, /TEAM2/);
  assert.match(html, /<b class="win">W<\/b> 31–24/);
});

test('an away game says so and a home game does not', () => {
  assert.match(gameLogPanel(log(row({ home: 0 }))), />at TEAM2/);
  assert.doesNotMatch(gameLogPanel(log(row({ home: 1 }))), />at TEAM2/);
});

test('the result is read from the player\'s own side of the score', () => {
  assert.match(gameLogPanel(log(row({ pf: 24, pa: 31 }))), /<b class="loss">L<\/b> 24–31/);
  assert.match(gameLogPanel(log(row({ pf: 21, pa: 21 }))), /T 21–21/);
});

test('a game with no score shows a dash rather than a wrong verdict', () => {
  // Asserting only that no win/loss class appears is not enough: with both
  // sides null, null > null and null < null are both false, so dropping the
  // guard falls through to the tie branch and renders "T null-null" -- a
  // verdict, with no class on it.
  const html = gameLogPanel(log(row({ pf: null, pa: null })));
  assert.match(html, /<td>&mdash;<\/td>/);
  assert.doesNotMatch(html, /class="win"|class="loss"|>T /);
  assert.doesNotMatch(html, /null/);
});

test('a game with a line of nothing shows a dash, not an empty cell', () => {
  assert.match(gameLogPanel(log(row({ stats: {} }))), /<td class="game-line">&mdash;<\/td>/);
});

test('seasons are newest first and each is its own table', () => {
  const html = gameLogPanel({
    2024: [[1, '2024-09-06', 2, 1, 7, 3, {}]],
    2025: [[2, '2025-09-06', 2, 1, 7, 3, {}]],
  });
  assert.ok(html.indexOf('<h3>2025</h3>') < html.indexOf('<h3>2024</h3>'));
  assert.equal((html.match(/<table/g) || []).length, 2);
});

test('the note counts every game across every season', () => {
  const html = gameLogPanel({
    2024: [[1, '2024-09-06', 2, 1, 7, 3, {}], [2, '2024-09-13', 2, 1, 7, 3, {}]],
    2025: [[3, '2025-09-06', 2, 1, 7, 3, {}]],
  });
  assert.match(html, /3 games with a box-score line/);
});

test('the note says what a missing game does not mean', () => {
  // The archive starts in 2005 and the source does not publish every box
  // score. A blank there is not a zero.
  assert.match(gameLogPanel(log(row())), /does not mean the player recorded nothing/);
});
