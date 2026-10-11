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

function liftFn(name) {
  const m = shell.match(new RegExp('^function ' + name + '\\([^]*?\\n\\}', 'm'));
  assert.ok(m, 'the shell must declare ' + name);
  const context = { fmtDate: d => String(d) };
  vm.runInNewContext(m[0] + '\nthis.__f = ' + name + ';', context);
  return context.__f;
}

function lift(names, extra = {}) {
  const constants = ['STAT_CATEGORY_LABELS', 'STAT_CATEGORY_ORDER', 'STAT_TYPE_ORDER',
                     'FRACTION_STATS', 'DECIMAL_STATS', 'GAME_PERCENT_STATS'].map(name => {
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

const { gameLineSummary, gameLogPanel, statLineHasActivity, gameLogShard, gameStatCell } =
  lift(['playerStatCell', 'gameStatCell', 'statLineHasActivity', 'gameLineSummary',
        'gameLogPanel', 'gameLogShard'],
       // The real one, so a change to how the site dates a game shows up here.
       { fmtGameDate: liftFn('fmtGameDate'), fmtDate: d => String(d) });

const row = (over = {}) => Object.assign({
  id: 401, date: '2025-09-07', kickoff: '2025-09-07T00:30:00.000Z', neutral: 0,
  opp: 2, home: 1, pf: 31, pa: 24,
  stats: { passing: { YDS: '233', TD: '3' } },
}, over);
const log = (...rows) => ({ 2025: rows.map(r =>
  [r.id, r.date, r.opp, r.home, r.pf, r.pa, r.stats, r.kickoff, r.neutral]) });

// ---- the shard key ------------------------------------------------------------

test('a negative athlete id lands in a real shard, the same one Python picks', () => {
  // 29,162 ids in this archive are negative and JavaScript's % keeps the sign
  // of the dividend, so the bare modulo would ask for shard -17.
  assert.equal(gameLogShard(-1105, 64), ((-1105 % 64) + 64) % 64);
  assert.ok(gameLogShard(-1105, 64) >= 0);
  assert.equal(gameLogShard(4685740, 64), 4685740 % 64);
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

test('a composite of zeroes is a category he did not register in', () => {
  // Number("0/0") is NaN and NaN !== 0, so reading the whole value kept every
  // kicker who attempted nothing: 503 all-zero FG lines and 125 all-zero XP
  // lines in three seasons of this archive alone.
  assert.equal(gameLineSummary({ kicking: { FG: '0/0', XP: '0/0', PTS: '0' } }), '');
  assert.match(gameLineSummary({ kicking: { FG: '1/2', XP: '0/0', PTS: '3' } }), /FG/);
  assert.equal(statLineHasActivity({ 'C/ATT': '0/0' }, ['C/ATT']), false);
  assert.equal(statLineHasActivity({ 'C/ATT': '18/29' }, ['C/ATT']), true);
});

test('a value the feed left empty is not activity', () => {
  assert.equal(statLineHasActivity({ TB: '', 'In 20': null }, ['TB', 'In 20']), false);
  assert.equal(statLineHasActivity({ TB: '  ' }, ['TB']), false);
  assert.equal(gameLineSummary({ punting: { TB: '', 'In 20': '0' } }), '');
});

test('a value that is not a number at all counts as something, not nothing', () => {
  // Hiding a line is the worse mistake of the two: a type the feed starts
  // sending in some new shape should appear, not vanish.
  assert.equal(statLineHasActivity({ NOTE: 'DNP' }, ['NOTE']), true);
});

test('a box-score percentage is already a percentage', () => {
  // The season feed sends a completion percentage as the fraction 0.656; the
  // box score sends 65.6. Formatting the latter with the former's rule made a
  // kicker who made his only attempt read "PCT 10000.0%", which is what the
  // page showed until a browser check caught it.
  assert.equal(gameStatCell('100', 'PCT'), '100.0%');
  assert.equal(gameStatCell('50', 'PCT'), '50.0%');
  assert.equal(gameStatCell('0', 'PCT'), '0.0%');
  assert.match(gameLineSummary({ kicking: { FG: '1/1', PCT: '100', PTS: '3' } }), /100\.0%/);
});

test('a composite value is printed as it came, not as a number', () => {
  assert.equal(gameStatCell('18/29', 'C/ATT'), '18/29');
});

test('a rate keeps one decimal, as it does in the season tables', () => {
  assert.equal(gameStatCell('8', 'AVG'), '8.0');
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

test('a neutral-site game is not played "at" the opponent', () => {
  // 1,128 games since 2005 are at a neutral site, and CFBD still designates
  // one side home: the away side in a bowl game was not visiting anyone. The
  // site's own schedule tables say "vs" with an (N), so this does too.
  const away = gameLogPanel(log(row({ home: 0, neutral: 1 })));
  assert.doesNotMatch(away, /at TEAM2/);
  assert.match(away, /\(N\)/);
  const host = gameLogPanel(log(row({ home: 1, neutral: 1 })));
  assert.doesNotMatch(host, /at TEAM2/);
  assert.match(host, /\(N\)/);
  assert.doesNotMatch(gameLogPanel(log(row({ home: 0, neutral: 0 }))), /\(N\)/);
});

test('a late kickoff is dated the day it was played, as the game page dates it', () => {
  // The stored date is UTC, so an 8pm Eastern Saturday game is a Sunday in
  // the database. 3,802 games since 2005 differ, and every one of them would
  // have disagreed with the game page this row links to.
  const html = gameLogPanel(log(row({ date: '2025-09-07',
                                      kickoff: '2025-09-07T00:30:00.000Z' })));
  assert.match(html, /Sep 6, 2025/);
  assert.doesNotMatch(html, /Sep 7, 2025|2025-09-07/);
});

test('a game with no kickoff time keeps the stored date', () => {
  // Seasons before CFBD published start times have a date and nothing else.
  const html = gameLogPanel(log(row({ date: '2006-09-09', kickoff: null })));
  assert.match(html, /2006-09-09/);
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
