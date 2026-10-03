// Dependency-free search regressions: node --test tests/search.test.cjs
const { test } = require('node:test');
const assert = require('node:assert/strict');
const CfbSearch = require('../ui/search.js');
const { buildSearcher, words } = CfbSearch;

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
const conferences = [
  ['sec', 'SEC', []],
  ['big-ten', 'Big Ten', ['B1G', 'Big 10']],
  ['mid-american', 'Mid-American', ['MAC']],
  ['american-athletic', 'American Athletic', ['AAC', 'The American', 'American']],
  ['pac-12', 'Pac-12', ['Pacific-12', 'PAC 12']]
];
const s = buildSearcher({ teams, seasons: [2018, 2019, 2020], games, conferences });
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


// ---------------- Conferences (Milestone E pages) ----------------

const confNames = r => (r.conferences || []).map(c => c.name);

test('a conference is found by name, slug or shorthand', () => {
  assert.deepEqual(confNames(s.search('sec')), ['SEC']);
  assert.deepEqual(confNames(s.search('big ten')), ['Big Ten']);
  assert.deepEqual(confNames(s.search('big-ten')), ['Big Ten']);
  assert.deepEqual(confNames(s.search('b1g')), ['Big Ten']);
  assert.deepEqual(confNames(s.search('mac')), ['Mid-American']);
  assert.deepEqual(confNames(s.search('pac 12')), ['Pac-12']);
  assert.deepEqual(confNames(s.search('pac-12')), ['Pac-12']);
  // A prefix matches while typing, and the whole-name match sorts ahead of a later word.
  assert.deepEqual(confNames(s.search('american')), ['American Athletic', 'Mid-American']);
});

test('a conference hit carries the slug the conference route needs', () => {
  assert.deepEqual(s.search('sec').conferences, [{ slug: 'sec', name: 'SEC' }]);
});

test('conferences never consume a word from a team query', () => {
  // "american" is a conference shorthand; it must not stop Texas from being matched.
  const r = s.search('texas lsu');
  assert.deepEqual(r.mentioned, [1, 3]);
  assert.equal(r.gamesTotal, 1);
  assert.deepEqual(confNames(r), []);
  // Two real teams still produce the head-to-head list with no conference noise.
  assert.deepEqual(confNames(s.search('texas a&m lsu')), []);
});

test('conference matching does not disturb team results', () => {
  // "miami" is a team, not a conference: the team groups are unchanged.
  const r = s.search('miami');
  assert.deepEqual(names(r), ['Miami (FL)', 'Miami (OH)']);
  assert.deepEqual(confNames(r), []);
});

test('conference results are capped and counted', () => {
  const many = Array.from({ length: 9 }, (_, i) => ['c' + i, 'Coastal ' + i, []]);
  const big = buildSearcher({ teams: [], seasons: [], games: [], conferences: many });
  const r = big.search('coastal');
  assert.equal(r.conferences.length, 5);
  assert.equal(r.conferencesTotal, 9);
  assert.equal(big.search('coastal', { conferences: 2 }).conferences.length, 2);
});

test('an index with no conferences still searches teams', () => {
  const none = buildSearcher({ teams, seasons: [2019], games });
  assert.deepEqual(none.search('texas a&m').mentioned, [2]);
  assert.deepEqual(none.search('sec').conferences, []);
  assert.equal(none.search('sec').conferencesTotal, 0);
});

// ---- People in search (v0.1.1) ----------------------------------------------
// People are not in the search index: 15,909 for one synced season, with a
// backfill to 2009 ahead. They live in per-letter shards, and these cover the
// matching the page runs over whichever shard it loaded.

const peopleRows = [
  ['p', 1, 'Cade Klubnik', 10, 'QB'],
  ['p', 2, 'Klub Jones', 20, 'RB'],
  ['p', 3, 'Jaylen Klubnik', 30, 'WR'],
  ['c', 4, 'Dan Lanning', 10, 'head coach'],
  ['p', 5, "Na'eem Offord", 20, 'DB'],
  ['p', 6, 'Aaron Karas', null, null]
];

test('the shard a query needs is the first letter of the word being typed', () => {
  // "cade klub" is a surname in progress, so it needs k, not c.
  assert.equal(CfbSearch.peopleShardKey('klubnik'), 'k');
  assert.equal(CfbSearch.peopleShardKey('cade klub'), 'k');
  assert.equal(CfbSearch.peopleShardKey('  Lanning '), 'l');
  assert.equal(CfbSearch.peopleShardKey("Na'eem"), 'n');
  assert.equal(CfbSearch.peopleShardKey('2026'), '0');
  for (const none of ['', '   ', '!!', '#']) assert.equal(CfbSearch.peopleShardKey(none), null);
});

test('a person is found by surname, which is how anyone searches', () => {
  const r = CfbSearch.matchPeople(peopleRows, 'klubnik', 8);
  assert.deepEqual(r.people.map(p => p.name), ['Cade Klubnik', 'Jaylen Klubnik']);
  assert.equal(r.total, 2);
  assert.deepEqual(r.people[0], { kind: 'player', id: 1, name: 'Cade Klubnik', teamId: 10, role: 'QB' });
});

test('a whole-name prefix outranks a later word, and a coach is labelled as one', () => {
  // "klub" starts Klub Jones' whole name, and Klubnik's second word.
  assert.deepEqual(CfbSearch.matchPeople(peopleRows, 'klub', 8).people.map(p => p.name),
    ['Klub Jones', 'Cade Klubnik', 'Jaylen Klubnik']);
  const coach = CfbSearch.matchPeople(peopleRows, 'lanning', 8).people[0];
  assert.equal(coach.kind, 'coach');
  assert.equal(coach.id, 4);
});

test('a two-word query matches a first and last name together', () => {
  assert.deepEqual(CfbSearch.matchPeople(peopleRows, 'cade klub', 8).people.map(p => p.name),
    ['Cade Klubnik']);
  assert.deepEqual(CfbSearch.matchPeople(peopleRows, 'jaylen klubnik', 8).people.map(p => p.name),
    ['Jaylen Klubnik']);
  // Words in the wrong order are not a match: that would make every pair of
  // common names match every player.
  assert.deepEqual(CfbSearch.matchPeople(peopleRows, 'klubnik cade', 8).people, []);
});

test('matching never begins mid-word, and punctuation in a name is folded', () => {
  assert.deepEqual(CfbSearch.matchPeople(peopleRows, 'lub', 8).people, []);
  assert.deepEqual(CfbSearch.matchPeople(peopleRows, 'arning', 8).people, []);
  assert.deepEqual(CfbSearch.matchPeople(peopleRows, 'naeem', 8).people.map(p => p.id), [5]);
  assert.deepEqual(CfbSearch.matchPeople(peopleRows, "na'eem", 8).people.map(p => p.id), [5]);
});

test('two letters is every roster in the country, so it is not a search', () => {
  assert.deepEqual(CfbSearch.matchPeople(peopleRows, 'kl', 8), { people: [], total: 0 });
  assert.deepEqual(CfbSearch.matchPeople(peopleRows, '', 8), { people: [], total: 0 });
  assert.equal(CfbSearch.matchPeople(peopleRows, 'klu', 8).total, 3);
});

test('the limit caps what is shown but not what is counted', () => {
  const r = CfbSearch.matchPeople(peopleRows, 'klu', 2);
  assert.equal(r.people.length, 2);
  assert.equal(r.total, 3);      // so the panel can say "+1 more"
});

test('a person with no team or position still matches, with nulls not blanks', () => {
  const r = CfbSearch.matchPeople(peopleRows, 'karas', 8);
  assert.deepEqual(r.people, [{ kind: 'player', id: 6, name: 'Aaron Karas', teamId: null, role: null }]);
});

test('an absent or empty shard is not an error', () => {
  for (const rows of [null, undefined, []]) {
    assert.deepEqual(CfbSearch.matchPeople(rows, 'klubnik', 8), { people: [], total: 0 });
  }
});
