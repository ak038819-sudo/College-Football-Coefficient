// The quarter-by-quarter table on a game page. The page draws it only when the
// periods agree with the scoreboard, so these drive the real helpers.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const shell = fs.readFileSync('ui/dashboard_shell.html', 'utf8');
const fn = name => {
  const match = shell.match(new RegExp('function ' + name + '\\([^)]*\\) \\{[\\s\\S]*?\\n\\}'));
  assert.ok(match, 'missing ' + name);
  return match[0];
};
const ctx = {esc: v => String(v)};
vm.runInNewContext([fn('lineScoreRows'), fn('lineScoreTable'),
  'this.rows = lineScoreRows; this.table = lineScoreTable;'].join('\n'), ctx);

const game = (home, away) => ({completed: 1, home_score: home, away_score: away});

test('a regulation game reads as four quarters and a total', () => {
  const rows = ctx.rows({home_line_scores: '7-7-0-7', away_line_scores: '3-7-7-0'}, game(21, 17));
  assert.deepEqual(Array.from(rows.labels), ['1', '2', '3', '4']);
  assert.deepEqual(Array.from(rows.home), [7, 7, 0, 7]);
  const html = ctx.table({home_line_scores: '7-7-0-7', away_line_scores: '3-7-7-0'}, game(21, 17), 'Away', 'Home');
  assert.match(html, /<th scope="row">Away<\/th><td>3<\/td><td>7<\/td><td>7<\/td><td>0<\/td>/);
  assert.match(html, /gp-line-total">17</, 'the total column is the final score');
});

test('overtime periods are named, not numbered as quarters', () => {
  const rows = ctx.rows({home_line_scores: '7-7-0-7-3-7', away_line_scores: '7-7-7-0-3-0'}, game(31, 24));
  assert.deepEqual(Array.from(rows.labels), ['1', '2', '3', '4', 'OT', '2OT']);
});

test('periods that disagree with the scoreboard are not shown', () => {
  // A stale or partial line would otherwise print a score the page contradicts
  // two inches above.
  assert.equal(ctx.rows({home_line_scores: '7-7-0-7', away_line_scores: '3-7-7-0'}, game(28, 17)), null);
  assert.equal(ctx.table({home_line_scores: '7-7-0-7', away_line_scores: '3-7-7-0'}, game(28, 17), 'A', 'H'), '');
});

test('a game with no periods, or only one side, draws nothing', () => {
  for (const detail of [null, {}, {home_line_scores: '7-7-0-7'},
    {home_line_scores: '7-7-0-7', away_line_scores: null},
    {home_line_scores: '21', away_line_scores: '17'},
    {home_line_scores: '7-7-0-7', away_line_scores: '3-7-7'}]) {
    assert.equal(ctx.table(detail, game(21, 17), 'A', 'H'), '', JSON.stringify(detail));
  }
});

test('an unplayed game never shows a line score', () => {
  assert.equal(ctx.table({home_line_scores: '7-7-0-7', away_line_scores: '3-7-7-0'},
    {completed: 0, home_score: null, away_score: null}, 'A', 'H'), '');
});
