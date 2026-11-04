// Two leagues in one Elocast. These tests drive the rules from
// docs/basketball-ui.md that a wrong implementation would break silently:
// a football deep link that stops working, a basketball slug written into
// football's favorites key, a league switch that lands on an empty panel, a
// game with no tip-off time sorted as though it tipped at midnight, and a
// zeroed clock on a sport that has no live feed.
// A tip-off time is rendered in the TIMEZONE the reader's browser is set to,
// which has nothing to do with their locale, so an assertion about one has to
// fix the other. Pinned here, before the first Date is formatted, or this
// file's expectations would change with the machine running it.
process.env.TZ = 'America/New_York';

const test = require('node:test');
const assert = require('node:assert/strict');
const app = require('../ui/ncaambb_app.js');

function storage() {
  const data = new Map();
  return {
    getItem: key => (data.has(key) ? data.get(key) : null),
    setItem: (key, value) => { data.set(key, String(value)); },
    removeItem: key => { data.delete(key); },
    dump: () => Object.fromEntries(data)
  };
}

function hostile() {
  // A private window, or a blocked cookie jar: every accessor throws.
  return {
    getItem() { throw new Error('denied'); },
    setItem() { throw new Error('denied'); }
  };
}

// --- the route rule football parity depends on -----------------------------

test('a hash with no league is football, which is what keeps every published link working', () => {
  const route = app.readRoute('#section=rankings&view=top25');
  assert.equal(route.league, 'cfb');
  assert.deepEqual(route.params, { section: 'rankings', view: 'top25' });
});

test('a football route round-trips without gaining a league parameter', () => {
  const hash = '#section=rankings&view=top25';
  assert.equal(app.writeRoute(app.readRoute(hash)), hash);
});

test('basketball states its league, first in the hash', () => {
  const hash = '#league=ncaambb&section=rankings&view=top25';
  const route = app.readRoute(hash);
  assert.equal(route.league, 'ncaambb');
  assert.equal(app.writeRoute(route), hash);
});

test('an unknown league opens football rather than a dead page', () => {
  assert.equal(app.readRoute('#league=nba&section=teams').league, 'cfb');
  assert.equal(app.writeRoute({ league: 'nba', params: { section: 'teams' } }),
    '#section=teams');
});

test('a player route with no section survives, because football has one', () => {
  // #player=<id> with no section is a real football route; a league-aware
  // reader must not require a section.
  const route = app.readRoute('#player=4433970');
  assert.deepEqual(route.params, { player: '4433970' });
  assert.equal(app.writeRoute(route), '#player=4433970');
});

test('a value with an & or = in it survives a round trip', () => {
  const hash = app.writeRoute({ league: 'ncaambb',
    params: { q: 'St. John&Co=1' } });
  assert.equal(app.readRoute(hash).params.q, 'St. John&Co=1');
});

// --- switching leagues -----------------------------------------------------

test('a league switch keeps a destination both leagues have', () => {
  const switched = app.switchLeague(
    app.readRoute('#section=standings'), 'ncaambb');
  assert.equal(switched.kept, true);
  assert.equal(switched.hash, '#league=ncaambb&section=standings');
  assert.equal(switched.reason, null);
});

test('a league switch to a section the league lacks lands on home and says why', () => {
  // Basketball has no Stats until Player Impact or Roster Scenarios is ready.
  const switched = app.switchLeague(
    app.readRoute('#section=stats&view=adjusted'), 'ncaambb');
  assert.equal(switched.kept, false);
  assert.equal(switched.hash, '#league=ncaambb');
  assert.match(switched.reason, /no stats for Men's Basketball yet/);
});

test('a view is not carried across a switch even when the section survives', () => {
  // A football view name is not a promise about a basketball subview.
  const switched = app.switchLeague(
    app.readRoute('#section=teams&view=stadiums'), 'ncaambb');
  assert.equal(switched.hash, '#league=ncaambb&section=teams');
});

test('switching back to football drops the league parameter entirely', () => {
  const switched = app.switchLeague(
    app.readRoute('#league=ncaambb&section=teams'), 'cfb');
  assert.equal(switched.hash, '#section=teams');
});

// --- which league opens ----------------------------------------------------

test('a URL carrying a league beats the remembered one', () => {
  const store = storage();
  app.rememberLeague(store, 'ncaambb');
  assert.equal(app.openingLeague('#league=cfb&section=teams', store), 'cfb');
});

test('with no league in the URL, the last league visited opens', () => {
  const store = storage();
  app.rememberLeague(store, 'ncaambb');
  assert.equal(app.openingLeague('#section=teams', store), 'ncaambb');
  assert.equal(app.openingLeague('', store), 'ncaambb');
});

test('a storage that throws means football, not a crash', () => {
  assert.equal(app.openingLeague('#section=teams', hostile()), 'cfb');
  assert.equal(app.rememberLeague(hostile(), 'ncaambb'), false);
});

test('a remembered league nobody recognises is ignored', () => {
  const store = storage();
  store.setItem(app.LEAGUE_KEY, 'nhl');
  assert.equal(app.rememberedLeague(store), 'cfb');
});

// --- favorites are per league and never reinterpreted ----------------------

test("football's favorites key is untouched and stays football's", () => {
  assert.equal(app.favoritesKey('cfb'), 'elocast.favorites.v1');
  assert.equal(app.favoritesKey('ncaambb'),
    'elocast.favorites.ncaambb.v1');
  assert.notEqual(app.favoritesKey('ncaambb'), app.FOOTBALL_FAVORITES_KEY);
});

test('a basketball team never lands in the football key', () => {
  // A basketball slug read as football would render a team that does not
  // exist.
  const store = storage();
  app.writeFavorites(store, 'cfb', ['byu']);
  app.toggleFavorite(store, 'ncaambb', 'ncaambb:cbbd:team:252');
  assert.deepEqual(app.readFavorites(store, 'cfb'), ['byu']);
  assert.deepEqual(app.readFavorites(store, 'ncaambb'),
    ['ncaambb:cbbd:team:252']);
  assert.deepEqual(Object.keys(store.dump()).sort(),
    ['elocast.favorites.ncaambb.v1', 'elocast.favorites.v1']);
});

test('following in one league only OFFERS the other, never acts', () => {
  const store = storage();
  const offer = app.crossLeagueOffer('cfb', 'byu', { name: 'BYU' });
  assert.equal(offer.league, 'ncaambb');
  assert.equal(offer.applied, false);
  assert.match(offer.label, /Follow BYU Men's Basketball too\?/);
  assert.deepEqual(app.readFavorites(store, 'ncaambb'), [],
    'the offer must not have subscribed anybody');
});

test('no counterpart team means no offer at all', () => {
  assert.equal(app.crossLeagueOffer('cfb', 'byu', null), null);
});

test('corrupt favorites read as none rather than throwing', () => {
  const store = storage();
  store.setItem('elocast.favorites.ncaambb.v1', '{not json');
  assert.deepEqual(app.readFavorites(store, 'ncaambb'), []);
  store.setItem('elocast.favorites.ncaambb.v1', '[1,null,"ok"]');
  assert.deepEqual(app.readFavorites(store, 'ncaambb'), ['ok']);
});

// --- the season label ------------------------------------------------------

test('2026-27 is ending year 2027, everywhere', () => {
  assert.equal(app.seasonLabel(2027), '2026-27');
  assert.equal(app.seasonLabel(2026), '2025-26');
  // The zero pad matters: 2000-01, not 2000-1.
  assert.equal(app.seasonLabel(2001), '2000-01');
  assert.equal(app.seasonLabel(2010), '2009-10');
});

test('a season label is never guessed from nothing', () => {
  assert.equal(app.seasonLabel(null), '');
  assert.equal(app.seasonLabel('soon'), '');
});

// --- a basketball day ------------------------------------------------------

const DAY = '2026-11-07';

// The shape the export actually writes: `at` is the calendar day the game is
// played on in the Eastern United States, and `tip` is the instant it starts,
// present only when the provider has decided one. These were one field until
// the export was measured against it -- `at` had always been a bare date, so
// every game read as having no time and the whole scoreboard said
// "Scheduled". tests/test_ncaambb_export.py pins the same two fields on the
// Python side.
function game(overrides) {
  return Object.assign({
    id: 'g', at: DAY, tip: DAY + 'T19:00:00Z', status: 'scheduled',
    home: 'H', away: 'A', home_points: null, away_points: null
  }, overrides || {});
}

test('the date strip centres the day it is given', () => {
  const strip = app.dayStrip(DAY, 2, DAY);
  assert.equal(strip.length, 5);
  assert.equal(strip[2].day, DAY);
  assert.equal(strip[2].today, true);
  assert.equal(strip[0].day, '2026-11-05');
  assert.equal(strip[4].day, '2026-11-09');
});

test('only the real today is marked today, wherever the strip is centred',
  () => {
    // The centre used to be marked today by its offset, so moving off today
    // -- or opening on any other day -- relabelled that day "Today".
    const strip = app.dayStrip(DAY, 2, '2026-11-09');
    assert.deepEqual(strip.filter(entry => entry.today).map(e => e.day),
      ['2026-11-09']);
    const away = app.dayStrip(DAY, 1, '2027-03-01');
    assert.deepEqual(away.filter(entry => entry.today), [],
      'a strip nowhere near today marks no day as today');
  });

test('the strip falls back to the real date when none is given', () => {
  const today = new Date().toISOString().slice(0, 10);
  const strip = app.dayStrip(today, 1);
  assert.deepEqual(strip.filter(entry => entry.today).map(e => e.day),
    [today]);
  assert.equal(app.todayKey(), today);
});

test('today is the day the game is played on, not the UTC date', () => {
  // The same measurement that moved the export's own `playing_date` to
  // Eastern. 7:30pm on the 14th in New York is already the 15th in UTC, so
  // for the whole of every evening the UTC date is tomorrow -- and the page
  // opened on tomorrow's schedule while those games were being played.
  const evening = new Date('2026-11-15T00:30:00Z');
  assert.equal(app.todayKey(evening, app.NCAAMBB_DAY_ZONE), '2026-11-14');
  assert.equal(app.todayKey(evening), '2026-11-15',
    'the UTC default is unchanged, because other leagues state UTC days');

  // Daylight saving, in both directions, and a moment where the two
  // calendars agree so the zone cannot be doing nothing.
  assert.equal(app.todayKey(new Date('2026-11-15T17:00:00Z'),
    app.NCAAMBB_DAY_ZONE), '2026-11-15');
  assert.equal(app.todayKey(new Date('2027-03-20T03:30:00Z'),
    app.NCAAMBB_DAY_ZONE), '2027-03-19', 'EDT is four hours, not five');
  assert.equal(app.todayKey(new Date('2026-11-15T04:30:00Z'),
    app.NCAAMBB_DAY_ZONE), '2026-11-14', 'EST is five');

  // A zone the browser cannot resolve falls back to the UTC date rather
  // than throwing on the page's first paint.
  assert.equal(app.todayKey(evening, 'Mars/Olympus'), '2026-11-15');
});

test('a scoreboard opens on today, or on the nearest end of the season',
  () => {
    // Not on the last date in the payload: a published schedule runs to
    // April, so that opened on the championship -- months away -- and the
    // strip called it Today.
    const days = ['2026-11-03', '2027-01-15', '2027-04-06'];
    assert.equal(app.openingDay(days, '2027-01-20'), '2027-01-20',
      'a day inside the season with no games is still today');
    assert.equal(app.openingDay(days, '2027-01-15'), '2027-01-15');
    assert.equal(app.openingDay(days, '2026-07-04'), '2026-11-03',
      'before the season: opening night, not the final');
    assert.equal(app.openingDay(days, '2027-09-01'), '2027-04-06');
    assert.equal(app.openingDay([], '2027-01-20'), null);
    assert.equal(app.openingDay(null, '2027-01-20'), null);
  });

test('the strip crosses a month boundary correctly', () => {
  const strip = app.dayStrip('2026-12-31', 1).map(entry => entry.day);
  assert.deepEqual(strip, ['2026-12-30', '2026-12-31', '2027-01-01']);
});

test('a game belongs to the day its date names', () => {
  const games = [game({ id: 'a' }), game({ id: 'b', at: '2026-11-08' })];
  assert.deepEqual(app.gamesOn(games, DAY).map(g => g.id), ['a']);
});

test('a game with no tip-off time sorts last on its day, not first', () => {
  // 2,669 of the 2026-27 season's 6,015 games have no time. Rendering an
  // unknown time as midnight would put them at the top and read as real.
  // The ids are deliberately in the OPPOSITE alphabetical order to the
  // times, because ordering by id is the fallback: with ids that agreed, a
  // sort that ignored the tip-off entirely would pass this test.
  const rows = app.sortGames([
    game({ id: 'a-tbd', tip: null, time_tbd: 1 }),
    game({ id: 'b-late', tip: DAY + 'T23:00:00Z' }),
    game({ id: 'c-early', tip: DAY + 'T17:00:00Z' })
  ], 'tipoff');
  assert.deepEqual(rows.map(row => row.id),
    ['c-early', 'b-late', 'a-tbd']);
});

test('two undecided games keep a stable order instead of an invented one', () => {
  const rows = app.sortGames([
    game({ id: 'z', tip: null, time_tbd: 1 }),
    game({ id: 'a', tip: null, time_tbd: 1 })
  ], 'tipoff');
  assert.deepEqual(rows.map(row => row.id), ['a', 'z']);
});

test('featured puts a followed team first, and still buries the undecided', () => {
  const rows = app.sortGames([
    game({ id: 'early', tip: DAY + 'T17:00:00Z' }),
    game({ id: 'mine', tip: DAY + 'T21:00:00Z', home: 'BYU' }),
    game({ id: 'tbd-mine', tip: null, time_tbd: 1, away: 'BYU' })
  ], 'featured', ['BYU']);
  assert.deepEqual(rows.map(row => row.id), ['mine', 'tbd-mine', 'early']);
});

test('the live filter returns nothing, because basketball has no live feed', () => {
  // The control matches football's, and answers honestly rather than
  // guessing from a clock that does not exist.
  const games = [game({ status: 'final' }), game({ id: 's' })];
  assert.deepEqual(app.filterGames(games, 'live'), []);
  assert.equal(app.filterGames(games, 'final').length, 1);
  assert.equal(app.filterGames(games, 'upcoming').length, 1);
  assert.equal(app.filterGames(games, 'all').length, 2);
});

// --- what a card may say ---------------------------------------------------

test('a scheduled game with no time says so, and never shows a clock', () => {
  assert.equal(app.statusLine(game({ time_tbd: 1, tip: null })), 'Time TBD');
  assert.equal(app.statusLine(game({ status: 'final' })), 'Final');
  assert.equal(app.statusLine(game({ status: 'postponed' })), 'Postponed');
  // Under way. There is no basketball clock to put next to it.
  assert.equal(app.statusLine(game({ status: 'in_progress' })),
    'In progress');
});

test('a scheduled game with a tip-off time says when, in one timezone', () => {
  // 19:00Z is 2pm in New York and 7pm in London. The card states the
  // reader's own evening, which is the only one they can act on.
  assert.equal(app.statusLine(game(), 'en-US'), '2:00 PM');
  assert.equal(app.tipText(game(), 'en-GB'), '14:00');
  // A timestamp no browser can read is absent, not "Invalid Date".
  // Long enough to get past the length check, so this reaches the guard that
  // turns an unreadable timestamp into an absence rather than "Invalid Date".
  assert.equal(app.tipText(game({ tip: 'whenever, honestly' })), null);
  // A time exists and cannot be rendered, which is not the same as no time
  // having been decided, so the card says neither a clock nor "Time TBD".
  assert.equal(app.statusLine(game({ tip: 'whenever, honestly' })),
    'Scheduled');
});

test('a basketball game whose day field is a bare date has no known time', () => {
  // The old bug, pinned. `at` is basketball's DAY, so a game with no `tip`
  // has no tip-off time however the day is written.
  assert.equal(app.timeKnown({ at: DAY, status: 'scheduled' }), false);
  assert.equal(app.statusLine({ at: DAY, status: 'scheduled' }), 'Time TBD');
});

test('an NFL-shaped game keeps its instant in `at`, and gets no clock', () => {
  // This module is shared by all three leagues: the NFL export states the
  // instant in `at` and no `tip` at all. Its games must still sort and
  // bucket by time -- and must NOT start showing a tip-off time on a page
  // this change was never about.
  const nfl = { at: DAY + 'T19:00:00Z', status: 'scheduled' };
  assert.equal(app.startInstant(nfl), DAY + 'T19:00:00Z');
  assert.equal(app.timeKnown(nfl), true);
  assert.equal(app.tipText(nfl), null);
  assert.equal(app.statusLine(nfl), 'Scheduled');
});

test('no status line anywhere is a zeroed clock', () => {
  // A 0:00 reads as the period expiring. There is no basketball clock yet.
  for (const status of ['scheduled', 'final', 'cancelled', 'postponed']) {
    assert.doesNotMatch(app.statusLine(game({ status })), /0:00/);
  }
});

test('a missing score is absent, not nil-nil', () => {
  assert.equal(app.scoreLine(game()), null);
  assert.equal(app.scoreLine(game({ home_points: 0, away_points: 0 })),
    '0 - 0', 'a real nil-nil is still a score');
  assert.equal(app.scoreLine(game({ home_points: 70, away_points: 61 })),
    '61 - 70');
});

test('a rating displays as a whole number and a missing one as an em dash', () => {
  assert.equal(app.ratingText(2273.7), '2274');
  assert.equal(app.ratingText(1500), '1500');
  assert.equal(app.ratingText(null), '—');
  assert.equal(app.ratingText(undefined), '—');
});

// --- the tabs each league may honestly show --------------------------------

test('basketball has no Stats tab, because an empty tab is worse than none', () => {
  assert.equal(app.hasSection('cfb', 'stats'), true);
  assert.equal(app.hasSection('ncaambb', 'stats'), false);
});

test('basketball has the tabs its data supports and no others', () => {
  assert.deepEqual(app.sections('ncaambb').slice().sort(),
    ['games', 'home', 'people', 'standings', 'teams']);
});

test('search names the league it will search', () => {
  assert.match(app.searchPlaceholder('ncaambb'), /college basketball/);
  assert.match(app.searchPlaceholder('cfb'), /college football/);
});

// --- football parity -------------------------------------------------------

test("football's own router is untouched by a league parameter", () => {
  // The published football links are read by ui/navigation.js, not by this
  // module. A league parameter it does not know must be ignored, not break
  // the route around it.
  const nav = require('../ui/navigation.js');
  const config = { yearsAll: [2025, 2026], playoffYears: [2026],
    gameSeasons: [2026] };
  const football = nav.readRoute('#section=rankings&view=top25', config);
  const withLeague = nav.readRoute(
    '#league=ncaambb&section=rankings&view=top25', config);
  assert.equal(football.section, 'rankings');
  assert.equal(football.subview, 'top25');
  assert.deepEqual(withLeague, football,
    'a parameter football does not know must change nothing about its route');
});

/* ---- finding a club by name ------------------------------------------ */

const CLUBS = {
  't1': { name: 'Miami (FL)', school: 'Miami', abbr: 'MIA' },
  't2': { name: 'North Carolina', school: 'North Carolina', abbr: 'UNC' },
  't3': { name: "St. John's", school: "St. John's", abbr: 'SJU' },
  't4': { name: 'Central Miami Tech', school: 'Central Miami Tech',
          abbr: 'CMT' },
  't5': { name: 'Hawaiʻi', school: 'Hawaiʻi', abbr: 'HAW' }
};

const BOARD = [
  { team: 't1', rating: 1800 }, { team: 't2', rating: 1900 },
  { team: 't3', rating: 1700 },
  // Rated ABOVE Miami on purpose: a word match must not beat a match on
  // the start of the name just because the club is better.
  { team: 't4', rating: 2100 }, { team: 't5', rating: 1600 }
];

test('a club is found by the start of its name before a word inside one',
  () => {
    assert.deepEqual(app.searchTeams(BOARD, CLUBS, 'mia').map(r => r.team),
      ['t1', 't4']);
  });

test('two clubs in the same tier come best-rated first', () => {
  const board = [{ team: 't1', rating: 1800 }, { team: 't4', rating: 2100 }];
  const clubs = { t1: { name: 'Miami' }, t4: { name: 'Miami Tech' } };
  assert.deepEqual(app.searchTeams(board, clubs, 'miami').map(r => r.team),
    ['t4', 't1']);
});

test('a club is found by its abbreviation as readily as by its name', () => {
  assert.deepEqual(app.searchTeams(BOARD, CLUBS, 'unc').map(r => r.team),
    ['t2']);
});

test('punctuation and accents are not something a reader has to type', () => {
  assert.deepEqual(app.searchTeams(BOARD, CLUBS, 'st johns').map(r => r.team),
    ['t3']);
  assert.deepEqual(app.searchTeams(BOARD, CLUBS, 'hawaii').map(r => r.team),
    ['t5']);
});

test('an empty query finds nothing, rather than everything', () => {
  // Every name contains the empty string, so without its own guard this
  // returns the whole board -- which the page would draw as "365 clubs
  // match" the moment a reader cleared the box.
  assert.deepEqual(app.searchTeams(BOARD, CLUBS, ''), []);
  assert.deepEqual(app.searchTeams(BOARD, CLUBS, '   '), []);
  assert.deepEqual(app.searchTeams(BOARD, CLUBS, '!!'), []);
});

test('a query matching nothing finds nothing', () => {
  assert.deepEqual(app.searchTeams(BOARD, CLUBS, 'notaschool'), []);
});

test('the limit is honoured', () => {
  const board = [];
  const clubs = {};
  for (let index = 0; index < 40; index += 1) {
    board.push({ team: 'k' + index, rating: 1500 + index });
    clubs['k' + index] = { name: 'Tech ' + index };
  }
  assert.equal(app.searchTeams(board, clubs, 'tech').length, 25);
  assert.equal(app.searchTeams(board, clubs, 'tech', 5).length, 5);
});

// --- the people layer ------------------------------------------------------
//
// src/ncaambb/export_people.py writes the files these read, and its own tests
// pin the Python side. They cannot pin the shard arithmetic: Python's `%` is
// floored already, so the floored form and the bare form are the same
// function there. On this side they are not, and a disagreement sends a
// player page to a file that does not exist. These are the assertions that
// can catch it.

test('a shard is a floored modulo on this side too', () => {
  assert.equal(app.peopleShard(0), 0);
  assert.equal(app.peopleShard(101), 5);
  assert.equal(app.peopleShard(16), 0);
  assert.equal(app.peopleShard(4321), 1);
  // The whole point. A bare `id % 16` gives -3 here and Python gives 13, so
  // every negative provider id -- which the football archive really carries,
  // and nothing promises basketball never will -- would 404.
  assert.equal(app.peopleShard(-3), 13);
  assert.equal(app.peopleShard(-16), 0);
  assert.equal(app.peopleShard(-17), 15);
});

test('a shard count comes from the export, not from this file', () => {
  // The manifest states `shards`. A page that ignored it and used its own
  // constant would keep working until the export was ever resplit.
  assert.equal(app.peopleShard(101, 8), 5);
  assert.equal(app.peopleShard(101, 32), 5);
  assert.equal(app.peopleShard(37, 8), 5);
  assert.equal(app.peopleShard(37, 32), 5);
  assert.equal(app.peopleShard(37), 5);
  // And a count that is ignored would be invisible without a pair that
  // disagrees: 21 is shard 5 of sixteen and shard 21 of thirty-two.
  assert.equal(app.peopleShard(21, 16), 5);
  assert.equal(app.peopleShard(21, 32), 21);
});

test('an id that is not a number is not shard zero', () => {
  // `parseInt('')` is NaN and `NaN % 16` is NaN, which indexes nothing; the
  // page has to be told so rather than fetching `people/2026/NaN.js`.
  assert.equal(app.peopleShard(''), null);
  assert.equal(app.peopleShard('nobody'), null);
  assert.equal(app.peopleShard(undefined), null);
});

test('a bare provider id is turned back into a full key, and back', () => {
  assert.equal(app.playerKey(101), 'ncaambb:cbbd:player:101');
  assert.equal(app.providerIdOf('ncaambb:cbbd:player:101'), '101');
  assert.equal(app.providerIdOf(app.playerKey(-3)), '-3');
});

const SHARD_2026 = {
  season: 2026,
  t: ['ncaambb:cbbd:team:1', 'ncaambb:cbbd:team:2'],
  m: { 101: [[0, '3', 'G', 74, 190]] },
  p: { 101: [[700, 0, 30, 22, 1], [701, 0, 28, 14, 0]],
       102: [[700, 1, null, 0, 0]] }
};
const SHARD_2025 = {
  season: 2025,
  t: ['ncaambb:cbbd:team:2'],
  m: {},
  // A line with no roster snapshot behind it: 29 Division I players in
  // 2025-26 are in exactly this position and their minutes are real.
  p: { 101: [[600, 0, 12, 4, 0]] }
};

test('a player’s seasons come back newest first', () => {
  const seasons = app.playerSeasons([SHARD_2025, SHARD_2026], 101);
  assert.deepEqual(seasons.map(entry => entry.season), [2026, 2025]);
  assert.equal(seasons[0].games.length, 2);
  assert.equal(seasons[0].clubs[0].team, 'ncaambb:cbbd:team:1');
  assert.equal(seasons[0].clubs[0].jersey, '3');
});

test('a season with a line and no roster row is still his season', () => {
  const seasons = app.playerSeasons([SHARD_2025], 101);
  assert.deepEqual(seasons.map(entry => entry.season), [2025]);
  assert.deepEqual(seasons[0].clubs, []);
  assert.equal(seasons[0].games[0].team, 'ncaambb:cbbd:team:2');
});

test('a player in no loaded shard has no seasons, and does not throw', () => {
  assert.deepEqual(app.playerSeasons([SHARD_2025, SHARD_2026], 999), []);
  assert.deepEqual(app.playerSeasons(undefined, 101), []);
  assert.deepEqual(app.playerSeasons([null, {}], 101), []);
});

test('a total adds up only what was recorded', () => {
  const totals = app.playerTotals(
    app.playerSeasons([SHARD_2026], 101)[0].games);
  assert.deepEqual(totals, { games: 2, minutes: 58, points: 36, starts: 1 });
});

test('nil is a measurement and nothing-recorded is not', () => {
  const totals = app.playerTotals(
    app.playerSeasons([SHARD_2026], 102)[0].games);
  // One game, nobody wrote down his minutes, and he scored nothing. A zero
  // minute total would claim a measurement that was never taken; a null
  // points total would hide one that was.
  assert.equal(totals.games, 1);
  assert.equal(totals.minutes, null);
  assert.equal(totals.points, 0);
});

test('a per-game average refuses to divide by no games', () => {
  assert.equal(app.perGame(36, 2), 18);
  assert.equal(app.perGame(0, 1), 0);
  assert.equal(app.perGame(null, 2), null);
  // The one that matters: 0 games would read as a player who played and
  // scored nothing, which is a different player.
  assert.equal(app.perGame(36, 0), null);
});

const PEOPLE = {
  101: { n: 'Hal Guard', h: 'Provo', s: 'UT' },
  205: { n: 'Otto Center' },
  102: { n: 'Ivan Bench', h: 'Gary', s: 'IN' },
  103: { n: "Sean O'Connell" }
};

test('a player search matches the way the club search does', () => {
  assert.deepEqual(app.searchPlayers(PEOPLE, 'center').map(row => row.name),
    ['Otto Center']);
  assert.deepEqual(app.searchPlayers(PEOPLE, 'oconnell').map(row => row.name),
    ["Sean O'Connell"]);
  // An earlier match first: "Guard" ranks behind a name that starts with it.
  assert.deepEqual(app.searchPlayers(PEOPLE, 'hal').map(row => row.id),
    ['101']);
});

test('an empty player query finds nobody rather than everybody', () => {
  assert.deepEqual(app.searchPlayers(PEOPLE, ''), []);
  assert.deepEqual(app.searchPlayers(PEOPLE, '  '), []);
  assert.deepEqual(app.searchPlayers(PEOPLE, 'nobodyatall'), []);
  assert.deepEqual(app.searchPlayers(undefined, 'hal'), []);
});

test('a player search is capped', () => {
  const many = {};
  for (let index = 0; index < 40; index += 1) {
    many[1000 + index] = { n: 'Guard ' + index };
  }
  assert.equal(app.searchPlayers(many, 'guard').length, 25);
  assert.equal(app.searchPlayers(many, 'guard', 5).length, 5);
});

test('a run of seasons is named, not listed with "and" six times', () => {
  // It read "2021-22 and 2022-23 and 2023-24 and 2024-25 and 2025-26 and
  // 2026-27" on the People tab, which is how it looked in the browser.
  assert.equal(app.seasonRange([2027]), '2026-27');
  assert.equal(app.seasonRange([2026, 2027]), '2025-26 and 2026-27');
  assert.equal(app.seasonRange([2025, 2026, 2027]), '2024-25 through 2026-27');
  assert.equal(app.seasonRange([2022, 2023, 2024, 2025, 2026, 2027]),
    '2021-22 through 2026-27');
  // Oldest first whatever order they arrive in, and nothing at all for none.
  assert.equal(app.seasonRange([2027, 2022, 2025]), '2021-22 through 2026-27');
  assert.equal(app.seasonRange([]), '');
  assert.equal(app.seasonRange(undefined), '');
});

// ---- which game a night's card opens with ---------------------------------
//
// A Division I evening holds three hundred games. Ordering them by tip-off
// alone puts two winless clubs at noon above a top-ten meeting at nine, and
// the first screen is the only screen most readers look at.

test('a game is worth about what its weaker side is worth', () => {
  const ratings = { A: 2000, B: 2000, C: 1200 };
  const even = app.gameInterest({ home: 'A', away: 'B' }, ratings);
  const mismatch = app.gameInterest({ home: 'A', away: 'C' }, ratings);
  assert.equal(even, 2000);
  // Not the mean (1600), which would rank a 2000-v-1200 blowout above an
  // even game between two 1700s.
  assert.equal(mismatch, 1400);
  assert.ok(app.gameInterest({ home: 'A', away: 'C' }, ratings)
            < app.gameInterest({ home: 'D', away: 'E' },
                               { D: 1700, E: 1700 }));
});

test('a stronger favourite still lifts a game over an evener, weaker one', () => {
  // #1 against #40 should beat #35 against #36; the credit for the gap is
  // what makes that true, so a rule of "the weaker side alone" is wrong.
  const top = app.gameInterest({ home: 'one', away: 'forty' },
                               { one: 2120, forty: 1800 });
  const middling = app.gameInterest({ home: 'a', away: 'b' },
                                    { a: 1810, b: 1805 });
  assert.ok(top > middling, `${top} should beat ${middling}`);
});

test('an unrated side leaves a game unscored rather than scoring it zero', () => {
  // Every non-Division I opponent is unrated. Zero would read as a measured
  // "worst game of the night" and sort below nothing.
  assert.equal(app.gameInterest({ home: 'A', away: 'nobody' }, { A: 2000 }),
               null);
  assert.equal(app.gameInterest({ home: 'A', away: 'B' },
                                { A: 2000, B: null }), null);
  assert.equal(app.gameInterest(null, { A: 1 }), null);
  assert.equal(app.gameInterest({ home: 'A', away: 'B' }, null), null);
});

test('the board states the ratings the ordering reads', () => {
  const ratings = app.ratingsOf([
    { team: 'A', rating: 2000 },
    { team: 'B', rating: null },
    { team: 'C' },
    null
  ]);
  assert.deepEqual(ratings, { A: 2000 });
  assert.deepEqual(app.ratingsOf(null), {});
});

test('best puts the better game first, whatever time it tips', () => {
  // The tip-off order is deliberately the OPPOSITE of the interest order,
  // so a sort that ignored the ratings would fail rather than pass by luck.
  const ratings = { top1: 2100, top2: 2080, low1: 1300, low2: 1290 };
  const rows = app.sortGames([
    game({ id: 'noon-mismatch', home: 'low1', away: 'low2',
           tip: DAY + 'T17:00:00Z' }),
    game({ id: 'night-classic', home: 'top1', away: 'top2',
           tip: DAY + 'T23:00:00Z' })
  ], 'best', null, ratings);
  assert.deepEqual(rows.map(row => row.id),
                   ['night-classic', 'noon-mismatch']);
});

test('best buries a game it cannot score under every game it can', () => {
  const ratings = { low1: 1300, low2: 1290 };
  const rows = app.sortGames([
    game({ id: 'unrated', home: 'low1', away: 'nobody',
           tip: DAY + 'T17:00:00Z' }),
    game({ id: 'rated', home: 'low1', away: 'low2',
           tip: DAY + 'T23:00:00Z' })
  ], 'best', null, ratings);
  assert.deepEqual(rows.map(row => row.id), ['rated', 'unrated']);
});

test('two equally good games keep the chronological order', () => {
  // Interest decides between games, not within a pair of them: a reader
  // reading down a column of equals wants the evening in order.
  const ratings = { a: 1800, b: 1800, c: 1800, d: 1800 };
  const rows = app.sortGames([
    game({ id: 'late', home: 'a', away: 'b', tip: DAY + 'T23:00:00Z' }),
    game({ id: 'early', home: 'c', away: 'd', tip: DAY + 'T17:00:00Z' })
  ], 'best', null, ratings);
  assert.deepEqual(rows.map(row => row.id), ['early', 'late']);
});

test('tip-off order ignores the ratings entirely', () => {
  const ratings = { top1: 2100, top2: 2080, low1: 1300, low2: 1290 };
  const rows = app.sortGames([
    game({ id: 'noon-mismatch', home: 'low1', away: 'low2',
           tip: DAY + 'T17:00:00Z' }),
    game({ id: 'night-classic', home: 'top1', away: 'top2',
           tip: DAY + 'T23:00:00Z' })
  ], 'tipoff', null, ratings);
  assert.deepEqual(rows.map(row => row.id),
                   ['noon-mismatch', 'night-classic']);
});
