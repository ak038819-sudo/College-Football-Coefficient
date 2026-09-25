// Dependency-free search regressions: node --test tests/search.test.cjs
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { buildSearcher, words } = require('../ui/search.js');

const teams = [
  { id: 1, name: 'Texas', slug: 'texas', aliases: [] },
  { id: 2, name: 'Texas A&M', slug: 'texas-am', aliases: [] },
  { id: 3, name: 'LSU', slug: 'lsu', aliases: [] },
  { id: 4, name: 'Miami (FL)', slug: 'miami-fl', aliases: ['Miami'] },
  { id: 5, name: 'Miami (OH)', slug: 'miami-oh', aliases: [] },
  { id: 6, name: 'San José State', slug: 'san-jose-state', aliases: ['San Jose State'] },
  { id: 7, name: 'Texas Tech', slug: 'texas-tech', aliases: [] }
];
const games = [
  [100, 2018, 3, 2], [101, 2019, 2, 3], [102, 2019, 3, 1], [103, 2020, 2, 1], [104, 2020, 3, 2], [105, 2019, 1, 7]
];
const s = buildSearcher({ teams, seasons: [2018, 2019, 2020], games });
const names = r => r.teams.map(t => t.name);

test('words split and normalize like the directory search', () => {
  assert.deepEqual(words('Texas A&M'), ['texas', 'am']);
  assert.deepEqual(words('miami-fl'), ['miami', 'fl']);
  assert.deepEqual(words('  San José   State '), ['san', 'jose', 'state']);
});

test('whole team names win: "texas a&m" is Texas A&M, not Texas', () => {
  const r = s.search('texas a&m');
  assert.deepEqual(r.mentioned, [2]);
  assert.equal(names(r)[0], 'Texas A&M');
});

test('aliases resolve to the canonical team, which is listed first', () => {
  const r = s.search('miami');
  assert.deepEqual(r.mentioned, [4]);
  assert.deepEqual(names(r), ['Miami (FL)', 'Miami (OH)']);
  assert.deepEqual(s.search('San Jose State').mentioned, [6]);
  assert.deepEqual(s.search('san josé state').mentioned, [6]);
});

test('two teams + a year find that meeting; without a year, every meeting newest first', () => {
  const r = s.search('texas a&m lsu 2019');
  assert.deepEqual(r.games.map(g => g.game_id), [101]);
  assert.equal(r.year, 2019);
  assert.deepEqual(r.seasons, [2019]);
  const all = s.search('lsu texas a&m');
  assert.deepEqual(all.games.map(g => g.game_id), [104, 101, 100]);
  assert.equal(all.gamesTotal, 3);
});

test('one team + a year lists that team\'s season in order', () => {
  assert.deepEqual(s.search('texas 2019').games.map(g => g.game_id), [102, 105]);
  assert.deepEqual(s.search('texas').games, []);               // no year: team result, not every game ever
});

test('partial typing suggests teams, prefix matches first', () => {
  assert.deepEqual(names(s.search('tex')), ['Texas', 'Texas A&M', 'Texas Tech']);
  assert.deepEqual(names(s.search('a&m')), ['Texas A&M']);    // later-word match found...
  assert.deepEqual(names(s.search('ami')), []);                // ...but never mid-word ("mi-ami")
  assert.deepEqual(names(s.search('tech')), ['Texas Tech']);
  assert.deepEqual(names(s.search('t')), []);                  // one character is too little to suggest
});

test('years only count when they are real seasons; nonsense returns nothing', () => {
  assert.equal(s.search('1850').year, null);
  assert.deepEqual(s.search('1850').seasons, []);
  const none = s.search('zzzz qqq');
  assert.equal(none.teams.length + none.games.length + none.seasons.length, 0);
  assert.deepEqual(s.search('').teams, []);
});

test('limits cap results but report totals', () => {
  const r = s.search('tex', { teams: 1 });
  assert.equal(r.teams.length, 1);
  assert.equal(r.teamsTotal, 3);
});

test('an empty or missing index never throws', () => {
  const empty = buildSearcher({});
  assert.deepEqual(empty.search('texas').teams, []);
});
