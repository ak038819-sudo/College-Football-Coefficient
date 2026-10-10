// Games against opponents this project holds no rating for (mostly FCS) are in
// the database but in none of the ratings. These tests drive the REAL schedule
// panel so the two promises the page makes about them cannot drift apart: the
// game is shown and counted in the record, and every rating column on its row
// stays empty.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const shell = fs.readFileSync(path.join(__dirname, '../ui/dashboard_shell.html'), 'utf8');
function sourceOf(name) {
  const match = shell.match(new RegExp(`function ${name}\\([^)]*\\) \\{[\\s\\S]*?\\n\\}`));
  assert.ok(match, `missing ${name}`);
  return match[0];
}

const TEAM = { id: 7, name: 'Alabama' };
const OPP = { id: 9, name: 'Auburn' };

// One rated win on Sep 7 and one rated loss on Nov 30, so an unrated game dated
// between them has somewhere to land and the interleaving is observable.
function ratedFixture() {
  const gamesById = new Map([
    [100, { id: 100, season: 2024, date: '2024-09-07', week: 2, home: TEAM.id, away: OPP.id,
            hs: 42, as: 10, neutral: false, phase: 0, ot: false, kickoff_utc: null }],
    [300, { id: 300, season: 2024, date: '2024-11-30', week: 14, home: OPP.id, away: TEAM.id,
            hs: 28, as: 21, neutral: false, phase: 0, ot: false, kickoff_utc: null }],
  ]);
  return {
    gamesById,
    eloByTeam: new Map([[TEAM.id, [
      { gid: 100, tid: TEAM.id, pre: 1700, oppPre: 1500, exp: 0.78, chg: 6.2, post: 1706.2, mov: 1 },
      { gid: 300, tid: TEAM.id, pre: 1706.2, oppPre: 1650, exp: 0.6, chg: -11.4, post: 1694.8, mov: 1 },
    ]]]),
    seasonsByTeam: new Map([[TEAM.id, new Map([[2024, {
      tid: TEAM.id, season: 2024, games: 2, w: 1, l: 1, t: 0, startElo: 1700, endElo: 1694.8,
      eloChange: -5.2, sos: 1575, sosRank: 20, expWins: 1.38, actWins: 1, aboveExp: -0.38,
      coe2: null, coe2Rank: null, conf: 'SEC',
    }]])]]),
    teamsPerSeason: new Map([[2024, 134]]),
    nonFbsByTeam: new Map(),
    nonFbsRecords: {},
  };
}

// An FCS home win on Sep 21, between the two rated games above.
function unratedGame(overrides = {}) {
  return Object.assign({
    id: 50, season: 2024, date: '2024-09-21', week: 4, tid: TEAM.id, opp: 'Mercer',
    mine: 52, theirs: 7, home: true, neutral: false, phase: 0, ot: false, cls: 'fcs',
  }, overrides);
}

function withUnrated(tp, games, record) {
  tp.nonFbsByTeam = new Map([[TEAM.id, games]]);
  tp.nonFbsRecords = record ? { [TEAM.id + '|2024']: record } : {};
  return tp;
}

function panelHtml(tp, { team = TEAM, season = 2024 } = {}) {
  const el = { innerHTML: '' };
  const ctx = {
    document: { getElementById: id => (id === 'tp-schedule' ? el : null) },
    UPCOMING_GAMES: [],
    TEAM_BY_ID: new Map([[String(TEAM.id), TEAM], [String(OPP.id), OPP]]),
    teamPageState: { schedSeason: season },
    esc: x => String(x),
    oppLink: name => `<a class="opp-link">${name}</a>`,
    panelHead: (title, control) => `<h2>${title}</h2>${control || ''}`,
    seasonSelect: () => '<select></select>',
    gameHref: (id, yr) => `#game=${id}&season=${yr}`,
    fmtKickoff: iso => `KICKOFF ${iso}`,
    probPct: p => ({ home: Math.round(p * 100), away: Math.round((1 - p) * 100) }),
    phaseLabel: () => '',
    teamSeasonsList: () => [2024],
    wrapWideTables: () => {},
    tp,
  };
  vm.createContext(ctx);
  // The record strings, the per-game point of view and the date format are all
  // the shipped implementations: a stub for any of them would let this file
  // pass while the page was wrong.
  vm.runInContext(
    'let liveResults = new Map();\nlet liveResultsSignature = "";\n' +
    'const MONTHS = ' + JSON.stringify(
      ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']) + ';\n' +
    sourceOf('fmtDate') + '\n' +
    sourceOf('fmtGameDate') + '\n' +
    sourceOf('signed') + '\n' +
    sourceOf('fmtWins') + '\n' +
    sourceOf('recStr') + '\n' +
    sourceOf('gameFor') + '\n' +
    sourceOf('eloShade') + '\n' +
    sourceOf('teamScheduleSeasons') + '\n' +
    sourceOf('nonFbsRecord') + '\n' +
    sourceOf('recWithUnrated') + '\n' +
    sourceOf('fillSchedulePanel') + '\n' +
    'const SHADE_FULL = 25, SHADE_MIN_PCT = 4, SHADE_MAX_PCT = 20;\n' +
    'this.fill = fillSchedulePanel;', ctx);
  ctx.fill(tp, team);
  return el.innerHTML;
}

// The order the rows are emitted in, as 'result score' per row, so a test can
// assert the interleaving without matching the whole table.
function resultOrder(html) {
  return Array.from(html.matchAll(/<td class="(?:win|loss)">([WLT] \d+–\d+)<\/td>/g)).map(m => m[1]);
}

test('an unrated game is on the schedule with its real score', () => {
  const html = panelHtml(withUnrated(ratedFixture(), [unratedGame()], [1, 0, 0]));
  assert.match(html, /<td class="win">W 52–7<\/td>/);
  assert.match(html, /Mercer/);
});

// The unrated game's id (50) sorts before both rated ids (100, 300) while its
// date falls between them, so a table ordered by anything but the date fails.
test('an unrated row lands between the rated games it was played between', () => {
  const html = panelHtml(withUnrated(ratedFixture(), [unratedGame()], [1, 0, 0]));
  assert.deepEqual(Array.from(resultOrder(html)), ['W 42–10', 'W 52–7', 'L 21–28']);
});

test('an unrated row leaves every rating column empty', () => {
  const html = panelHtml(withUnrated(ratedFixture(), [unratedGame()], [1, 0, 0]));
  const row = html.match(/<tr class="sched-unrated">[\s\S]*?<\/tr>/);
  assert.ok(row, 'no unrated row rendered');
  // Nine columns: date, opponent, round, result, then the five rating columns,
  // which must every one be a dash. Asserting on the tail rather than on the
  // whole row, because the date legitimately contains a four-digit year.
  const cells = row[0].match(/<td[^>]*>[\s\S]*?<\/td>/g);
  assert.equal(cells.length, 9);
  assert.deepEqual(Array.from(cells.slice(4)), new Array(5).fill('<td class="dim">\u2014</td>'));
});

test('an unrated row carries no Elo tint, because there was no Elo change', () => {
  const html = panelHtml(withUnrated(ratedFixture(), [unratedGame()], [1, 0, 0]));
  const row = html.match(/<tr class="sched-unrated">[\s\S]*?<\/tr>/)[0];
  assert.doesNotMatch(row, /elo-shade/);
  // The rated rows above and below still have theirs, so the tint is not simply off.
  assert.match(html, /<tr class="elo-shade"/);
});

test('an unrated opponent is plain text, with no team page and no game page', () => {
  const html = panelHtml(withUnrated(ratedFixture(), [unratedGame()], [1, 0, 0]));
  const row = html.match(/<tr class="sched-unrated">[\s\S]*?<\/tr>/)[0];
  assert.doesNotMatch(row, /<a/);
  assert.doesNotMatch(row, /#game=50/);
});

test('the tag names the division the source gave, or says unrated when it gave none', () => {
  const fcs = panelHtml(withUnrated(ratedFixture(), [unratedGame()], [1, 0, 0]));
  assert.match(fcs, /<span class="tag-unrated"[^>]*>FCS<\/span>/);
  const none = panelHtml(withUnrated(ratedFixture(), [unratedGame({ cls: null })], [1, 0, 0]));
  assert.match(none, /<span class="tag-unrated"[^>]*>unrated<\/span>/);
  // A 1980s opponent CFBD still calls FBS which this project does not carry is
  // not FCS, and must not be labelled as if it were.
  const fbs = panelHtml(withUnrated(ratedFixture(), [unratedGame({ cls: 'fbs' })], [1, 0, 0]));
  assert.match(fbs, /<span class="tag-unrated"[^>]*>unrated<\/span>/);
});

test('an unrated game says vs at home, at on the road, and marks a neutral site', () => {
  const home = panelHtml(withUnrated(ratedFixture(), [unratedGame()], [1, 0, 0]));
  assert.match(home, /<span class="swing-where">vs<\/span> Mercer/);
  const away = panelHtml(withUnrated(ratedFixture(), [unratedGame({ home: false })], [1, 0, 0]));
  assert.match(away, /<span class="swing-where">at<\/span> Mercer/);
  // A neutral-site game is nobody's home game, so it reads vs and is flagged.
  const neutral = panelHtml(withUnrated(
    ratedFixture(), [unratedGame({ home: false, neutral: true })], [1, 0, 0]));
  assert.match(neutral, /<span class="swing-where">vs<\/span> Mercer <span class="swing-where">\(N\)<\/span>/);
});

test('an unrated row keeps its week, its postseason label and its OT marker', () => {
  const reg = panelHtml(withUnrated(ratedFixture(), [unratedGame()], [1, 0, 0]));
  assert.match(reg, /<tr class="sched-unrated"><td>[^<]*<\/td><td class="opp">[\s\S]*?<\/td><td>Wk 4<\/td>/);
  // A postseason game is labelled by phase, not by week: CFBD numbers some
  // postseason games week 1, so a week there would read as September.
  const post = panelHtml(withUnrated(ratedFixture(), [unratedGame({ phase: 1, week: 1 })], [1, 0, 0]));
  assert.match(post, /<td>Post<\/td>/);
  assert.doesNotMatch(post, /<td>Wk 1<\/td>/);
  // No week at all is a dash rather than "Wk null".
  const noWeek = panelHtml(withUnrated(ratedFixture(), [unratedGame({ week: null })], [1, 0, 0]));
  assert.doesNotMatch(noWeek, /Wk null/);
  const ot = panelHtml(withUnrated(
    ratedFixture(), [unratedGame({ mine: 24, theirs: 21, ot: true })], [1, 0, 0]));
  assert.match(ot, /<td class="win">W 24\u201321 OT<\/td>/);
});

test('the footer record counts the unrated game and says which part is rated', () => {
  const html = panelHtml(withUnrated(ratedFixture(), [unratedGame()], [1, 0, 0]));
  assert.match(html, /2024 · 2–1 overall, 1–1 rated/);
});

test('every other footer number stays the rated set', () => {
  const html = panelHtml(withUnrated(ratedFixture(), [unratedGame()], [1, 0, 0]));
  const foot = html.match(/<tfoot>[\s\S]*?<\/tfoot>/)[0];
  assert.match(foot, /<td>1 W<\/td>/);          // actual wins: rated only
  assert.match(foot, /<td>1700<\/td>/);         // opening Elo
  assert.match(foot, /<td>1575<\/td>/);         // strength of schedule
  assert.match(foot, /<td>69%<\/td>/);          // 1.38 expected over 2 rated games
});

test('a team with no unrated games sees its record unchanged and no tag', () => {
  const html = panelHtml(ratedFixture());
  assert.match(html, /2024 · 1–1</);
  assert.doesNotMatch(html, /overall/);
  assert.doesNotMatch(html, /tag-unrated/);
  assert.doesNotMatch(html, /sched-unrated/);
});

test('losses and ties against unrated opponents count in their own columns', () => {
  const html = panelHtml(withUnrated(ratedFixture(), [
    unratedGame(),
    unratedGame({ id: 201, date: '2024-09-28', opp: 'Furman', mine: 14, theirs: 17 }),
    unratedGame({ id: 202, date: '2024-10-05', opp: 'Samford', mine: 21, theirs: 21 }),
  ], [1, 1, 1]));
  // Rated 1-1 plus unrated 1-1-1 is 2-2-1; the rated half is still 1-1.
  assert.match(html, /2024 · 2–2–1 overall, 1–1 rated/);
  assert.match(html, /<td class="loss">L 14–17<\/td>/);
  // A tie takes neither the win nor the loss class.
  assert.match(html, /<td class="">T 21–21<\/td>/);
});

test('only the selected season\u2019s unrated games are shown', () => {
  const html = panelHtml(withUnrated(ratedFixture(), [
    unratedGame(),
    unratedGame({ id: 203, season: 2023, date: '2023-09-02', opp: 'Chattanooga' }),
  ], [1, 0, 0]));
  assert.match(html, /Mercer/);
  assert.doesNotMatch(html, /Chattanooga/);
  assert.equal((html.match(/sched-unrated/g) || []).length, 1);
});

test('a season whose only games were unrated still renders a schedule', () => {
  const tp = ratedFixture();
  tp.eloByTeam = new Map();
  tp.seasonsByTeam = new Map();
  const html = panelHtml(withUnrated(tp, [unratedGame()], [1, 0, 0]));
  assert.match(html, /<td class="win">W 52–7<\/td>/);
  assert.match(html, /2024 · 1–0 overall/);
  assert.match(html, /No rated games this season/);
});

test('a season known only from an unrated game is offered in the picker', () => {
  const ctx = vm.createContext({ UPCOMING_GAMES: [], teamSeasonsList: () => [2024] });
  vm.runInContext(sourceOf('teamScheduleSeasons') +
    '\nthis.seasons = teamScheduleSeasons;', ctx);
  const tp = { nonFbsByTeam: new Map([[TEAM.id, [unratedGame({ season: 1987 })]]]) };
  assert.deepEqual(Array.from(ctx.seasons(tp, TEAM)), [2024, 1987]);
});
