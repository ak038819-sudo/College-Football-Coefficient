/* The recruiting board and the team tab, lifted out of the production shell.
 *
 * The export side is tests/test_recruiting_export.py.
 */
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const shell = fs.readFileSync('ui/dashboard_shell.html', 'utf8');

for (const name of ['recruitingBoardHtml', 'signeeTableHtml', 'classBoardRow', 'recruitHometown',
  'recruitStars', 'teamClassHistory', 'teamRecruitingBody', 'renderRecruiting',
  'hydrateRecruiting', 'fillRecruitingPanel', 'loadRecruitingClasses', 'loadSignees']) {
  // Two declarations of one name silently replace each other here, and a test
  // that lifts BY NAME never notices which one it got.
  const found = shell.match(new RegExp('function ' + name + '\\(', 'g')) || [];
  assert.equal(found.length, 1, name + ' must be declared exactly once in the shell');
}

const lift = name => shell.match(new RegExp('function ' + name + '\\([^]*?\\n\\}'))[0];
const source = ['recruitHometown', 'recruitStars', 'signeeTableHtml', 'classBoardRow',
  'recruitingBoardHtml', 'teamClassHistory'].map(lift).join('\n');

const context = {
  esc: v => String(v).replaceAll('&', '&amp;').replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;').replaceAll('"', '&quot;'),
  TEAM_BY_ID: new Map([['10', { name: 'Alabama' }], ['20', { name: 'Auburn' }]]),
  teamLink: name => '<a class="team" href="#">' + context.esc(name) + '</a>',
  playerHref: id => '#player=' + id
};
vm.runInNewContext(source + '\nthis.board = recruitingBoardHtml; this.signees = signeeTableHtml;' +
  '\nthis.row = classBoardRow; this.history = teamClassHistory; this.stars = recruitStars;' +
  '\nthis.home = recruitHometown;', context);
const { board, signees, row, history, stars, home } = context;

const signee = (over = {}) => Object.assign({
  name: 'A Player', position: 'QB', stars: 4, rating: 0.9208, national_rank: 12,
  athlete_id: 500, city: 'Mobile', state_province: 'AL', high_school: 'Central'
}, over);
const cls = (over = {}) => Object.assign({
  team_id: 10, season_year: 2026, class_rank: 1, points: 310.67 }, over);
const text = html => html.replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();

/* ---- a signee ---------------------------------------------------------- */

test('a signee links to his player page, and only when he has one', () => {
  // recruits.athlete_id is the join that had been stored and never used. The
  // exporter nulls it when no player page exists, so a link here never dies.
  const html = signees([signee(), signee({ name: 'No Page', athlete_id: null })]);
  assert.match(html, /<a href="#player=500">A Player<\/a>/);
  assert.match(html, />No Page</);
  assert.equal((html.match(/#player=/g) || []).length, 1);
});

test('an unrated signee is a gap, never a nought-star recruit', () => {
  assert.equal(stars(null), '&mdash;');
  assert.equal(stars(0), '0', 'a real zero, if the source ever sends one, stays');
  assert.equal(stars(5), '5');
  const html = signees([signee({ stars: null, rating: null, national_rank: null })]);
  assert.doesNotMatch(html, /0\.0000/, 'a missing rating must not print as a rating of zero');
});

test('a hometown with only one half is not a stray comma', () => {
  assert.equal(home({ city: 'Mobile', state_province: 'AL' }), 'Mobile, AL');
  assert.equal(home({ city: 'Mobile', state_province: null }), 'Mobile');
  assert.equal(home({ city: null, state_province: 'AL' }), 'AL');
  assert.equal(home({ city: null, state_province: null }), '');
  assert.match(signees([signee({ city: null, state_province: null })]), /&mdash;/);
});

test('every column sorts on its own number, not on the printed text', () => {
  const html = signees([signee()]);
  assert.match(html, /data-sort="4"/);          // stars
  assert.match(html, /data-sort="0\.9208"/);    // rating
  assert.match(html, /data-sort="12"/);         // national rank
  assert.match(signees([signee({ stars: null })]), /data-sort=""/);
});

test('a signee name cannot inject markup', () => {
  const html = signees([signee({ name: '<script>x</script>', athlete_id: null })]);
  assert.doesNotMatch(html, /<script>/);
  assert.match(html, /&lt;script&gt;/);
});

/* ---- the board --------------------------------------------------------- */

test('a class averages over the signees it has, and reports none when it has none', () => {
  const by = new Map([[10, [signee({ rating: 0.9, stars: 4 }), signee({ rating: 0.8, stars: 3 })]]]);
  const out = row(cls(), by);
  assert.equal(out.signees, 2);
  assert.ok(Math.abs(out.avg_rating - 0.85) < 1e-9);
  assert.equal(out.avg_stars, 3.5);
  // A class whose signees are all unrated must not average to 0 and rank last
  // on a scale it was never measured on.
  const unrated = row(cls(), new Map([[10, [signee({ rating: null, stars: null })]]]));
  assert.equal(unrated.avg_rating, null);
  assert.equal(unrated.avg_stars, null);
  assert.equal(unrated.signees, 1, 'he still signed');
});

test('a season whose signees did not load says nothing rather than zero', () => {
  // `null` is "the file is not here"; an empty Map is "it is here and this
  // team is not in it". Printing 0 signees for the first would be a lie.
  const out = row(cls(), null);
  assert.equal(out.signees, null);
  assert.match(board([cls()], null, 0), /&mdash;/);
});

test('the board is ordered by national rank and names the teams it cannot show', () => {
  const html = board([cls({ team_id: 20, class_rank: 4 }), cls({ team_id: 10, class_rank: 1 })],
    new Map(), 82);
  const names = [...html.matchAll(/class="team" href="#">([^<]+)</g)].map(m => m[1]);
  assert.deepEqual(names, ['Alabama', 'Auburn']);
  // 1,148 classes belong to schools this database does not carry, so a board
  // of 139 rows runs past rank 139 and has to say why.
  assert.match(text(html), /82 more in 2026 went to schools it does not/);
});

test('with every class placed, the board makes no excuse for itself', () => {
  assert.doesNotMatch(text(board([cls()], new Map(), 0)), /went to schools it does not/);
});

test('an unranked class is shown, not sorted to the top as rank zero', () => {
  const html = board([cls({ team_id: 20, class_rank: null }), cls({ team_id: 10, class_rank: 1 })],
    new Map(), 0);
  const names = [...html.matchAll(/class="team" href="#">([^<]+)</g)].map(m => m[1]);
  assert.deepEqual(names, ['Alabama', 'Auburn'], 'the unranked class goes last');
});

test('an empty season says so instead of drawing an empty table', () => {
  assert.match(board([], new Map(), 0), /No classes for this season/);
});

/* ---- the team tab ------------------------------------------------------ */

test('a class history needs more than one season to be a history', () => {
  const archive = new Map([[2026, [cls()]]]);
  assert.equal(history({ id: 10 }, { bySeason: archive }), '');
  archive.set(2025, [cls({ season_year: 2025, class_rank: 3 })]);
  const html = history({ id: 10 }, { bySeason: archive });
  assert.match(text(html), /Class by class/);
  assert.match(text(html), /2026 #1/);
  assert.match(text(html), /2025 #3/, 'newest season first');
});

test('the team tab and the board both say no rating reads this', () => {
  // The one claim these panels must never lose: Elo and Merits are built from
  // results, and a class rank describes what was signed, not what happened.
  for (const fn of ['teamRecruitingBody', 'renderRecruiting']) {
    assert.match(lift(fn), /no rating on this site reads/i, fn);
  }
});

/* ---- the shared table enhancer must be allowed to run ------------------- */

test('the board really does sort, driven through the real ui/stats.js', () => {
  // The structural test below says the marker is gone. This one says the
  // consequence: the enhancer installs buttons and a click reorders the rows.
  const { JSDOM } = require('jsdom');
  const html = board([cls(), cls({ team_id: 20, class_rank: 4, points: 120.2 })],
                     new Map(), 0);
  const dom = new JSDOM('<body>' + html + '</body>', { runScripts: 'outside-only' });
  const w = dom.window;
  w.eval(fs.readFileSync('ui/stats.js', 'utf8'));
  const table = w.document.querySelector('table');
  w.CfbStats.enhanceTable(table);
  const points = () => [...table.tBodies[0].rows].map(r => r.cells[2].textContent.trim());
  assert.ok(table.querySelectorAll('.sort-button').length > 0,
    'the enhancer must have been allowed to run');
  const button = [...table.querySelectorAll('.sort-button')]
    .find(b => /Points/.test(b.textContent));
  button.dispatchEvent(new w.Event('click'));   // first click sorts descending
  button.dispatchEvent(new w.Event('click'));
  assert.deepEqual(points(), ['120.20', '310.67'], 'ascending by points');
});

test('a recruiting table does not pre-set data-sortable', () => {
  // `CfbStats.enhanceTable()` uses that attribute as its own "already done"
  // marker and returns immediately when it is present (ui/stats.js). A table
  // that sets it in its own markup therefore never gets sort buttons or click
  // handlers, and the sort keys beside every number go unread. Only the
  // roster table may carry it, because that one installs its own handlers.
  for (const html of [
    signees([signee()]),
    board([cls()], new Map(), 0),
    history({ id: 10 }, { bySeason: new Map([
      [2026, [cls()]], [2025, [cls({ season_year: 2025, class_rank: 3 })]]]) })
  ]) {
    assert.doesNotMatch(html, /data-sortable/,
      'leave the attribute to the enhancer, which sets it once it has run');
  }
});
