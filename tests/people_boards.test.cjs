/* The People boards, lifted out of the production shell.
 *
 * The export side is tests/test_people_boards.py.
 */
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const shell = fs.readFileSync('ui/dashboard_shell.html', 'utf8');

for (const name of ['boardTotal', 'careerBoardHtml', 'coachBoardHtml', 'activeBoard',
  'boardPicker', 'peopleBoardsHtml', 'hydratePeopleBoards', 'loadPeopleBoards',
  'peopleBoardsEntry', 'boardKeys']) {
  // Two declarations of one name silently replace each other here, and a test
  // that lifts BY NAME never notices which one it got.
  const found = shell.match(new RegExp('function ' + name + '\\(', 'g')) || [];
  assert.equal(found.length, 1, name + ' must be declared exactly once in the shell');
}

const lift = name => shell.match(new RegExp('function ' + name + '\\([^]*?\\n\\}'))[0];
const source = ['boardTotal', 'careerBoardHtml', 'coachBoardHtml', 'boardKeys', 'activeBoard',
  'boardPicker', 'peopleBoardsHtml'].map(lift).join('\n') +
  '\n' + shell.match(/let peopleBoardKey = null;/)[0];

const context = {
  esc: v => String(v).replaceAll('&', '&amp;').replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;').replaceAll('"', '&quot;'),
  playerHref: id => '#player=' + id,
  coachHref: id => '#coach=' + id
};
vm.runInNewContext(source +
  '\nthis.total = boardTotal; this.career = careerBoardHtml; this.coachBoard = coachBoardHtml;' +
  '\nthis.active = activeBoard; this.picker = boardPicker; this.all = peopleBoardsHtml;' +
  '\nthis.setKey = k => { peopleBoardKey = k; };', context);
const { total, career, coachBoard, active, picker, all, setKey } = context;

const COACH_FIELDS = ['coach_id', 'name', 'wins_above_expected', 'seasons_measured',
  'seasons_unmeasured', 'games', 'wins', 'first_season', 'last_season'];
const coachPayload = (coaches, over = {}) => Object.assign({
  fields: COACH_FIELDS, coaches, measured_coaches: 715, unmeasured_seasons: 370 }, over);
const careerBoard = (leaders, over = {}) => Object.assign({
  label: 'Rushing yards', category: 'rushing', first_season: 2009, leaders }, over);
const text = html => html.replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();

/* ---- a total ----------------------------------------------------------- */

test('a half sack keeps its half, and a whole number keeps none', () => {
  // Sacks and tackles for loss are halves in this feed. Formatting them as
  // integers would lose a half-sack from every career that has one.
  assert.equal(total(34.5), '34.5');
  assert.equal(total(11), '11');
  assert.equal(total(6405), '6,405', 'a career total is read, so it is grouped');
});

test('a missing total is a gap, not a zero', () => {
  assert.equal(total(null), '—');
  assert.equal(total(undefined), '—');
  assert.equal(total('nonsense'), '—');
  assert.equal(total(0), '0', 'a real zero is a real zero');
});

/* ---- a career board ---------------------------------------------------- */

test('a career board links every row to its player page', () => {
  const html = career(careerBoard([[1, 'A Back', 6405, 4, 2013]]));
  assert.match(html, /<a href="#player=1">A Back<\/a>/);
  assert.match(text(html), /1 A Back 6,405 4 2013/);
});

test('a career board says which season the archive starts, not "all-time"', () => {
  // The archive begins in 2009, and defensive statistics in 2016. A board
  // that said "career" for both would be wrong about one of them.
  assert.match(text(career(careerBoard([[1, 'A', 100, 1, 2009]]))), /begins in 2009/);
  assert.match(text(career(careerBoard([[1, 'A', 34.5, 6, 2020]], { first_season: 2016 }))),
    /begins in 2016/);
  assert.match(text(career(careerBoard([[1, 'A', 100, 1, 2009]]))), /not an all-time list/);
});

test('an empty board draws nothing rather than a heading over nothing', () => {
  assert.equal(career(careerBoard([])), '');
  assert.equal(career(null), '');
});

test('every column sorts on its own number', () => {
  const html = career(careerBoard([[1, 'A Back', 6405, 4, 2013]]));
  assert.match(html, /data-sort="6405"/, 'not on the grouped "6,405"');
  assert.match(html, /data-sort="2013"/);
});

test('a player name cannot inject markup', () => {
  const html = career(careerBoard([[1, '<script>x</script>', 10, 1, 2009]]));
  assert.doesNotMatch(html, /<script>/);
  assert.match(html, /&lt;script&gt;/);
});

/* ---- the coach board --------------------------------------------------- */

test('a season two coaches shared is shown beside the total, never inside it', () => {
  // The source never says which games were whose, so the measure refuses
  // them. A row reading "28 seasons" when two were not counted would be a
  // claim this project cannot make.
  const html = coachBoard(coachPayload([[1, 'A Coach', 4.0, 26, 2, 312, 240, 1990, 2019]]));
  assert.match(text(html), /26 \(\+2 not measured\)/);
  assert.match(text(html), /370 seasons are unmeasured/);
  assert.match(text(html), /never folded into it/);
});

test('a coach with nothing unmeasured gets no parenthetical', () => {
  const html = coachBoard(coachPayload([[1, 'A Coach', 4.0, 26, 0, 312, 240, 1990, 2019]]));
  assert.doesNotMatch(text(html), /not measured\)/);
});

test('the sign is on the number, because "4.00 above expected" reads as neither', () => {
  const html = coachBoard(coachPayload([[1, 'Over', 4.0, 5, 0, 60, 40, 2015, 2019],
    [2, 'Under', -2.75, 5, 0, 60, 20, 2015, 2019],
    [3, 'Level', 0, 5, 0, 60, 30, 2015, 2019]]));
  assert.match(text(html), /\+4\.00/);
  assert.match(text(html), /-2\.75/, 'a coach below expectation must not be clipped at zero');
  assert.match(html, /class="num pos"/);
  assert.match(html, /class="num neg"/);
  assert.match(html, /class="num flat"/);
});

test('the board says what it is the top of', () => {
  const html = coachBoard(coachPayload([[1, 'A', 1, 1, 0, 12, 7, 2019, 2019]]));
  assert.match(text(html), /the top 1 of 715 measured coaches/);
});

test('a coach row links to his coach page, not a player page', () => {
  const html = coachBoard(coachPayload([[7, 'A Coach', 1, 1, 0, 12, 7, 2019, 2019]]));
  assert.match(html, /href="#coach=7"/);
  assert.doesNotMatch(html, /#player=/);
});

/* ---- choosing a board -------------------------------------------------- */

test('coaches is the board a reader lands on, and an unknown name falls back to it', () => {
  const boards = { leaders: { rushing_YDS: careerBoard([]) }, coaches: coachPayload([]) };
  setKey(null);
  assert.equal(active(boards), 'coaches');
  setKey('not_a_board');
  assert.equal(active(boards), 'coaches', 'an unknown key must not render nothing');
  setKey('rushing_YDS');
  assert.equal(active(boards), 'rushing_YDS');
  setKey(null);
});

test('the picker offers every board and marks the open one', () => {
  const boards = {
    leaders: { rushing_YDS: careerBoard([]), passing_YDS: careerBoard([], { label: 'Passing yards' }) },
    coaches: coachPayload([])
  };
  const html = picker(boards, 'passing_YDS');
  assert.equal((html.match(/<option/g) || []).length, 3);
  assert.match(html, /value="passing_YDS" selected/);
  assert.match(html, />Passing yards</);
  assert.match(html, />Coaches: wins above expected</);
});

test('the open board is the one that renders', () => {
  const boards = {
    leaders: { rushing_YDS: careerBoard([[1, 'A Back', 6405, 4, 2013]]) },
    coaches: coachPayload([[7, 'A Coach', 1, 1, 0, 12, 7, 2019, 2019]])
  };
  setKey('coaches');
  assert.match(text(all(boards)), /A Coach/);
  assert.doesNotMatch(text(all(boards)), /A Back/);
  setKey('rushing_YDS');
  assert.match(text(all(boards)), /A Back/);
  assert.doesNotMatch(text(all(boards)), /A Coach/);
  setKey(null);
});

test('the board choice is page state, not a URL the router would strip', () => {
  // CfbNavigation normalises the hash and drops anything outside its
  // contract, so a `board=` parameter springs back on the next render.
  const fn = lift('activeBoard') + lift('hydratePeopleBoards');
  assert.doesNotMatch(fn, /location\.hash/,
    'the picker must not try to keep its state in the URL');
});
