// Tonight's scores, merged onto the archive's slate.
//
// The rules under test are the ones that make a live feed safe to draw at
// all: an unplayed game must not become a 0-0 draw, an in-progress score
// must disappear once the snapshot stops being refreshed, and the archive's
// own number must always win over a feed that is only display.
const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');

const app = require(path.join(__dirname, '..', 'ui', 'nba_app.js'));

const NYK = 'nba:espn:team:18';
const SAS = 'nba:espn:team:24';

function liveRow(options) {
  const o = options || {};
  return {
    id: '401909966',
    game_key: o.key || 'nba:espn:game:401909966',
    state: o.state || 'in',
    completed: o.completed === undefined ? false : o.completed,
    detail: o.detail === undefined ? '5:21 - 3rd' : o.detail,
    period: o.period === undefined ? 3 : o.period,
    clock: o.clock === undefined ? '5:21' : o.clock,
    home: { id: 18, key: NYK, abbrev: 'NY', points: o.home === undefined ? 58 : o.home },
    away: { id: 24, key: SAS, abbrev: 'SA', points: o.away === undefined ? 61 : o.away }
  };
}

function snapshot(rows, minutesAgo) {
  const at = new Date(Date.now() - (minutesAgo || 0) * 60000);
  return { source: 'espn-scoreboard', sport: 'nba',
           updated_at: at.toISOString().replace(/\.\d+Z$/, 'Z'),
           days: ['2026-11-13'], games: rows };
}

function unplayed() {
  return { id: 'nba:espn:game:401909966', date: '2026-11-13',
           at: '2026-11-14T00:30:00Z', type: 'regular', home: NYK, away: SAS,
           home_points: null, away_points: null, rateable: 0 };
}

function settled() {
  return Object.assign(unplayed(), { home_points: 112, away_points: 108,
                                     rateable: 1 });
}

test('a game being played shows the score it has reached', () => {
  const games = [unplayed()];
  assert.equal(app.attachLive(games, snapshot([liveRow()])), 1);
  assert.equal(app.isLive(games[0]), true);
  assert.equal(app.shownScoreText(games[0], 'home'), '58');
  assert.equal(app.shownScoreText(games[0], 'away'), '61');
  assert.equal(app.statusLine(games[0]), '5:21 - 3rd');
});

test('a game that has not tipped off shows no score at all', () => {
  // THE TRAP, one layer up from the fetcher: both sources write an unplayed
  // game 0-0, so a slate of nine evening games becomes nine scoreless ties.
  const games = [unplayed()];
  app.attachLive(games, snapshot([liveRow({ state: 'pre', home: null,
                                            away: null, clock: null,
                                            period: null, detail: '7:00 PM' })]));
  assert.equal(games[0].live, undefined);
  assert.equal(app.shownScoreText(games[0], 'home'), '');
  assert.equal(app.isLive(games[0]), false);
});

test('a final from last night shows while the archive still lacks it', () => {
  // The whole reason this layer exists: the Parquet source runs about a day
  // behind, so last night's finals are not in the export yet.
  const games = [unplayed()];
  app.attachLive(games, snapshot([liveRow({ state: 'post', completed: true,
                                            detail: 'Final', home: 112,
                                            away: 108 })]));
  assert.equal(app.statusLine(games[0]), 'Final');
  assert.equal(app.shownScoreText(games[0], 'home'), '112');
  assert.equal(app.isLive(games[0]), false);
});

test('the archive wins wherever it has settled the game', () => {
  // A live feed is display only. If the two ever disagree, the number that
  // a rating was replayed from is the one the page shows.
  const games = [settled()];
  app.attachLive(games, snapshot([liveRow({ state: 'post', completed: true,
                                            home: 999, away: 1 })]));
  assert.equal(app.shownScoreText(games[0], 'home'), '112');
  assert.equal(app.shownScoreText(games[0], 'away'), '108');
});

test('a stale snapshot drops a game still being played', () => {
  // If the loop stops at nine in the evening, the 58-61 it last saw is not
  // the score an hour later. A page showing it confidently is worse than a
  // page showing none.
  const games = [unplayed()];
  const old = app.LIVE_STALE_AFTER_MINUTES + 1;
  assert.equal(app.attachLive(games, snapshot([liveRow()], old)), 0);
  assert.equal(games[0].live, undefined);
  assert.equal(app.shownScoreText(games[0], 'home'), '');
});

test('a stale snapshot keeps a final, which does not change', () => {
  const games = [unplayed()];
  const old = app.LIVE_STALE_AFTER_MINUTES + 120;
  assert.equal(app.attachLive(games,
    snapshot([liveRow({ state: 'post', completed: true, home: 112,
                        away: 108 })], old)), 1);
  assert.equal(app.statusLine(games[0]), 'Final');
});

test('a snapshot with no timestamp is treated as stale', () => {
  const fresh = app.liveFreshness({ updated_at: null });
  assert.equal(fresh.known, false);
  assert.equal(fresh.stale, true);
  const games = [unplayed()];
  assert.equal(app.attachLive(games, { games: [liveRow()] }), 0);
});

test('a row for a game this season does not have is ignored', () => {
  const games = [unplayed()];
  assert.equal(app.attachLive(games,
    snapshot([liveRow({ key: 'nba:espn:game:999' })])), 0);
  assert.equal(games[0].live, undefined);
});

test('a second attach clears a row that is no longer live', () => {
  // The page re-fetches every minute. A game that has dropped out of the
  // snapshot must not keep drawing the last score it had.
  const games = [unplayed()];
  app.attachLive(games, snapshot([liveRow()]));
  assert.equal(app.isLive(games[0]), true);
  app.attachLive(games, snapshot([]));
  assert.equal(games[0].live, undefined);
  assert.equal(app.shownScoreText(games[0], 'home'), '');
});

test('a game in progress with no detail still says it is in progress', () => {
  const games = [unplayed()];
  app.attachLive(games, snapshot([liveRow({ detail: null })]));
  assert.equal(app.statusLine(games[0]), 'In progress');
});

test('an unplayed game with no live row keeps its tip-off time', () => {
  const games = [unplayed()];
  app.attachLive(games, snapshot([]));
  assert.equal(app.statusLine(games[0], () => '7:30 PM'), '7:30 PM');
});

test('nothing here moves the archive own points', () => {
  // The guarantee the whole design rests on: records, standings and ratings
  // read these fields, and a score that is still moving must never reach
  // them.
  const games = [unplayed()];
  app.attachLive(games, snapshot([liveRow()]));
  assert.equal(games[0].home_points, null);
  assert.equal(games[0].away_points, null);
  assert.equal(app.isFinal(games[0]), false);
});

test('a final the archive lacks still dims its loser', () => {
  const games = [unplayed()];
  app.attachLive(games, snapshot([liveRow({ state: 'post', completed: true,
                                            home: 118, away: 109 })]));
  assert.equal(app.winnerOf(games[0]), NYK);
});

test('a game at halftime has a leader, not a winner', () => {
  // Dimming a club at halftime says something the data does not.
  const games = [unplayed()];
  app.attachLive(games, snapshot([liveRow()]));
  assert.equal(app.winnerOf(games[0]), null);
});
