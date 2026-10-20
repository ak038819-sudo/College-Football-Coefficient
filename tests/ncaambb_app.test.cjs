// Two leagues in one Elocast. These tests drive the rules from
// docs/basketball-ui.md that a wrong implementation would break silently:
// a football deep link that stops working, a basketball slug written into
// football's favorites key, a league switch that lands on an empty panel, a
// game with no tip-off time sorted as though it tipped at midnight, and a
// zeroed clock on a sport that has no live feed.
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

function game(overrides) {
  return Object.assign({
    id: 'g', at: DAY + 'T19:00:00Z', status: 'scheduled',
    home: 'H', away: 'A', home_points: null, away_points: null
  }, overrides || {});
}

test('the date strip centres today', () => {
  const strip = app.dayStrip(DAY, 2);
  assert.equal(strip.length, 5);
  assert.equal(strip[2].day, DAY);
  assert.equal(strip[2].today, true);
  assert.equal(strip[0].day, '2026-11-05');
  assert.equal(strip[4].day, '2026-11-09');
});

test('the strip crosses a month boundary correctly', () => {
  const strip = app.dayStrip('2026-12-31', 1).map(entry => entry.day);
  assert.deepEqual(strip, ['2026-12-30', '2026-12-31', '2027-01-01']);
});

test('a game belongs to the day its date names', () => {
  const games = [game({ id: 'a' }), game({ id: 'b', at: '2026-11-08T01:00:00Z' })];
  assert.deepEqual(app.gamesOn(games, DAY).map(g => g.id), ['a']);
});

test('a game with no tip-off time sorts last on its day, not first', () => {
  // 2,669 of the 2026-27 season's 6,015 games have no time. Rendering an
  // unknown time as midnight would put them at the top and read as real.
  const rows = app.sortGames([
    game({ id: 'tbd', at: DAY, time_tbd: 1 }),
    game({ id: 'late', at: DAY + 'T23:00:00Z' }),
    game({ id: 'early', at: DAY + 'T17:00:00Z' })
  ], 'tipoff');
  assert.deepEqual(rows.map(row => row.id), ['early', 'late', 'tbd']);
});

test('two undecided games keep a stable order instead of an invented one', () => {
  const rows = app.sortGames([
    game({ id: 'z', at: DAY, time_tbd: 1 }),
    game({ id: 'a', at: DAY, time_tbd: 1 })
  ], 'tipoff');
  assert.deepEqual(rows.map(row => row.id), ['a', 'z']);
});

test('featured puts a followed team first, and still buries the undecided', () => {
  const rows = app.sortGames([
    game({ id: 'early', at: DAY + 'T17:00:00Z' }),
    game({ id: 'mine', at: DAY + 'T21:00:00Z', home: 'BYU' }),
    game({ id: 'tbd-mine', at: DAY, time_tbd: 1, away: 'BYU' })
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
  assert.equal(app.statusLine(game({ time_tbd: 1, at: DAY })), 'Time TBD');
  assert.equal(app.statusLine(game()), 'Scheduled');
  assert.equal(app.statusLine(game({ status: 'final' })), 'Final');
  assert.equal(app.statusLine(game({ status: 'postponed' })), 'Postponed');
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
