/* The market panel on a game page, lifted out of the production shell.
 *
 * The export side is tests/test_market_lines.py.
 */
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const shell = fs.readFileSync('ui/dashboard_shell.html', 'utf8');

for (const name of ['gameMarketPanel', 'marketSpreadText', 'loadMarketLine', 'marketLineEntry',
  'hydrateMarketLine']) {
  // Two declarations of one name silently replace each other here, and a test
  // that lifts BY NAME never notices which one it got.
  const found = shell.match(new RegExp('function ' + name + '\\(', 'g')) || [];
  assert.equal(found.length, 1, name + ' must be declared exactly once in the shell');
}

const source = [
  shell.match(/function marketSpreadText\([^]*?\n\}/)[0],
  shell.match(/function gameMarketPanel\([^]*?\n\}\n/)[0]
].join('\n');

const context = {
  esc: v => String(v).replaceAll('&', '&amp;').replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;').replaceAll('"', '&quot;'),
  STATIC_MANIFEST: { market: { overall: { games: 10392 } } }
};
vm.runInNewContext(source + '\nthis.panel = gameMarketPanel; this.spread = marketSpreadText;', context);
const { panel, spread } = context;

const line = (over = {}) => Object.assign({
  game_id: 7, market_home_margin: -2.5, over_under: 54.5, provider: 'Bovada', closing: 1
}, over);
const game = (over = {}) => Object.assign({ completed: 1, home_score: 17, away_score: 41 }, over);
const text = html => html.replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();

/* ---- which side is favoured ------------------------------------------ */

test('the spread names a side, because "-2.5" alone names nobody', () => {
  // CFBD's own sign is the opposite of this database's. What is stored is the
  // market's expected HOME margin, so a NEGATIVE number favours the away team.
  assert.equal(spread(-2.5, 'Miami (FL)', 'Florida'), 'Miami (FL) by 2.5');
  assert.equal(spread(7, 'Miami (FL)', 'Florida'), 'Florida by 7');
  assert.equal(spread(34, 'Western Kentucky', 'Alabama'), 'Alabama by 34');
});

test('a pick’em is a real line at zero, not a missing one', () => {
  assert.equal(spread(0, 'A', 'B'), 'Pick’em');
  assert.equal(spread(null, 'A', 'B'), null);
  assert.equal(spread(undefined, 'A', 'B'), null);
  assert.equal(spread('nonsense', 'A', 'B'), null);
});

/* ---- the panel -------------------------------------------------------- */

test('no line means no panel, not an empty one', () => {
  assert.equal(panel(null, game(), 'Miami (FL)', 'Florida'), '');
  assert.equal(panel({}, game(), 'Miami (FL)', 'Florida'), '');
  assert.equal(panel(line({ market_home_margin: null }), game(), 'A', 'B'), '');
});

test('a completed game is scored against the number', () => {
  // Miami won 41-17 as a 2.5-point favourite: it beat the line by 21.5, and
  // the 58 points went 3.5 over a total of 54.5.
  const html = panel(line(), game(), 'Miami (FL)', 'Florida');
  const body = text(html);
  assert.match(body, /Spread Miami \(FL\) by 2\.5/);
  assert.match(body, /Total 54\.5/);
  assert.match(body, /Against the spread Miami \(FL\) covered by 21\.5/);
  assert.match(body, /Against the total 58 points, 3\.5 over/);
});

test('the home team covering is credited to the home team', () => {
  // The sign of `actual - margin` decides, and getting it backwards would
  // still produce a full, plausible-looking panel.
  const html = panel(line({ market_home_margin: -2.5 }),
    game({ home_score: 41, away_score: 17 }), 'Miami (FL)', 'Florida');
  assert.match(text(html), /Against the spread Florida covered by 26\.5/);
});

test('landing exactly on the number is a push, and on the total is neither', () => {
  const html = panel(line({ market_home_margin: -7, over_under: 41 }),
    game({ home_score: 17, away_score: 24 }), 'Away', 'Home');
  assert.match(text(html), /Against the spread Push/);
  assert.match(text(html), /Against the total 41 points, exactly the number/);
});

test('an unplayed game is not scored against anything', () => {
  const html = panel(line({ closing: 0 }),
    game({ completed: 0, home_score: null, away_score: null }), 'Miami (FL)', 'Florida');
  const body = text(html);
  assert.match(body, /Spread Miami \(FL\) by 2\.5/);
  assert.doesNotMatch(body, /Against the spread/);
  assert.doesNotMatch(body, /Against the total/);
});

test('a missing score is not a nil-nil draw', () => {
  // `Number(null)` is 0. This project has shipped that three times, so a game
  // flagged completed with no score must still not be scored against the line.
  const html = panel(line(), game({ completed: 1, home_score: null, away_score: null }),
    'Miami (FL)', 'Florida');
  assert.doesNotMatch(text(html), /Against the spread/);
});

test('a line captured before the end is not called a closing line', () => {
  assert.match(text(panel(line({ closing: 1 }), game(), 'A', 'B')),
    /The closing line from Bovada/);
  const open = text(panel(line({ closing: 0 }), game(), 'A', 'B'));
  assert.match(open, /The line from Bovada/);
  assert.doesNotMatch(open, /The closing line/);
  assert.match(open, /captured before this game ended and so not a closing number/);
});

test('a missing total is left out, not printed as zero', () => {
  const body = text(panel(line({ over_under: null }), game(), 'Miami (FL)', 'Florida'));
  assert.doesNotMatch(body, /Total/);
  assert.match(body, /Against the spread/, 'the spread still stands on its own');
});

test('the panel says the model cannot see this number', () => {
  // The whole value of the benchmark, and the one claim this panel must never
  // lose: no rating on the site reads game_lines.
  const body = text(panel(line(), game(), 'A', 'B'));
  assert.match(body, /No rating on this site reads this number/);
  assert.match(body, /the market has been the better forecaster/);
  assert.match(body, /10,392 games/, 'the count comes from the manifest, not from prose');
});

test('a team name cannot inject markup', () => {
  const html = panel(line(), game(), '<script>x</script>', 'Florida');
  assert.doesNotMatch(html, /<script>/);
  assert.match(html, /&lt;script&gt;/);
});

/* ---- filling the slot, and the render that has moved on ---------------- */

const { JSDOM } = require('jsdom');

// hydrateMarketLine is lifted on its own: the two callbacks it holds are the
// only place the page decides whether an in-flight request still speaks for
// what is on screen.
function hydrator({ resolve, reject, version, current }) {
  const doc = new JSDOM('<!doctype html><body><div id="gp-market-slot"></div>' +
    '</body>').window.document;
  const context = {
    document: doc,
    renderVersion: current,
    loadMarketLine: () => reject ? Promise.reject(new Error('no file')) : Promise.resolve(resolve),
    gameMarketPanel: () => '<div class="panel" id="drawn">panel</div>'
  };
  vm.runInNewContext(shell.match(/function hydrateMarketLine\([^]*?\n\}/)[0] +
    '\nthis.run = hydrateMarketLine;', context);
  return context.run(2024, 7, game(), 'A', 'B', version).then(() => doc);
}

test('a line that arrives replaces the slot with the panel', async () => {
  const doc = await hydrator({ resolve: line(), version: 3, current: 3 });
  assert.ok(doc.getElementById('drawn'), 'the panel is drawn');
  assert.equal(doc.getElementById('gp-market-slot'), null, 'the slot is consumed');
});

test('a season with no file takes its own slot away', async () => {
  const doc = await hydrator({ reject: true, version: 3, current: 3 });
  assert.equal(doc.getElementById('gp-market-slot'), null);
});

test('a request whose render has moved on touches nothing', async () => {
  // The failure side is the one that bites: a reader opens a 2024 game, moves
  // to a 1998 one before the file lands, and the old request's failure would
  // remove the NEW page's slot -- after which the new request, even when it
  // succeeds, finds no slot and the panel never appears at all.
  const failed = await hydrator({ reject: true, version: 3, current: 4 });
  assert.ok(failed.getElementById('gp-market-slot'),
    "a stale failure must not remove the current page's slot");
  const stale = await hydrator({ resolve: line(), version: 3, current: 4 });
  assert.equal(stale.getElementById('drawn'), null, 'nor may a stale success draw');
  assert.ok(stale.getElementById('gp-market-slot'));
});
