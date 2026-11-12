// The Bowling Merits research column on a team's Schedule tab.
//
// Bowling Merits is a research model (analysis/bowling_merits/, awarded by
// src/build_bowling_merits.py). Three things about it are easy to get wrong and
// are each pinned here by driving the REAL schedule panel:
//
//   1. Every row has to grow with it. The column is public (Austin, 2026-11-12),
//      so the header, each played row, each unrated and scheduled row, the
//      group separators' colspans and the footer must all be eleven wide --
//      a row one cell short silently shifts every value left of it.
//   2. A regulation loss earns a NEGATIVE award, unlike Merits where it earns
//      0. So 0.000 is not an outcome, and a dash is not a zero.
//   3. The footer is Python's season sum, not one taken in the browser.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const shell = fs.readFileSync(path.join(__dirname, '../ui/dashboard_shell.html'), 'utf8');
const config = JSON.parse(fs.readFileSync(path.join(__dirname, '../config/model_config.json'), 'utf8'));

function sourceOf(name) {
  const match = shell.match(new RegExp(`function ${name}\\([^)]*\\) \\{[\\s\\S]*?\\n\\}`));
  assert.ok(match, `missing ${name}`);
  return match[0];
}

const TEAM = { id: 7, name: 'Alabama' };
const OPP = { id: 9, name: 'Auburn' };
const DASH = '<td class="dim">—</td>';

// Three rated games: a win, a regulation loss, and one the research model
// never scored (a tie, or a season it does not cover). The three Bowling
// Merits values are deliberately a positive, a negative and nothing.
function fixture({
  bowl = [3.344, -0.104, null],
  bowlingRollup = 18.224,
  upcoming = [],
  unrated = false,
} = {}) {
  const dates = ['2024-09-07', '2024-10-12', '2024-11-30'];
  const gamesById = new Map(dates.map((date, i) => [100 + i, {
    id: 100 + i, season: 2024, date, week: 2 + i, home: i === 1 ? OPP.id : TEAM.id,
    away: i === 1 ? TEAM.id : OPP.id, hs: i === 1 ? 28 : 42, as: i === 1 ? 21 : 10,
    neutral: false, phase: 0, ot: false, kickoff_utc: null,
  }]));
  return {
    gamesById,
    eloByTeam: new Map([[TEAM.id, bowl.map((b, i) => ({
      gid: 100 + i, tid: TEAM.id, pre: 1700, oppPre: 1500, exp: 0.78,
      chg: i === 1 ? -11.4 : 6.2, post: 1706.2, mov: 1, coe: i === 1 ? 0 : 2.5, bowl: b,
    }))]]),
    seasonsByTeam: new Map([[TEAM.id, new Map([[2024, {
      tid: TEAM.id, season: 2024, games: 3, w: 2, l: 1, t: 0, startElo: 1700, endElo: 1694.8,
      eloChange: -5.2, sos: 1575, sosRank: 20, expWins: 1.38, actWins: 2, aboveExp: 0.62,
      coe2: 3.418, coe2Rank: 12, conf: 'SEC',
    }]])]]),
    teamsPerSeason: new Map([[2024, 134]]),
    // A game against an opponent this project holds no rating for. It earns no
    // Bowling Merits for the same reason it earns no Elo, so its row has to
    // grow a dash rather than stay one cell short of the others.
    nonFbsByTeam: unrated ? new Map([[TEAM.id, [{
      id: 300, season: 2024, date: '2024-08-31', week: 1, team: TEAM.id,
      opp: 'Mercer', mine: 56, theirs: 7, home: true, neutral: false, phase: 0,
      ot: false, cls: 'fcs',
    }]]]) : new Map(),
    nonFbsRecords: {},
    rollups: { [TEAM.id + '|2024']: { games: 5.0, bonus: 0, total: 5.0, counted: 3, items: [] } },
    bowlingRollups: bowlingRollup == null ? {} : { [TEAM.id + '|2024']: bowlingRollup },
    _upcoming: upcoming,
  };
}

// Every formatter is the shipped one. A stub for bowlingMeritCell() or
// schedColumns() would let this file pass while the column was wrong.
function panelHtml(tp, { search = '' } = {}) {
  const el = { innerHTML: '' };
  const ctx = {
    document: { getElementById: id => (id === 'tp-schedule' ? el : null) },
    location: { search },
    URLSearchParams,
    UPCOMING_GAMES: tp._upcoming || [],
    TEAM_BY_ID: new Map([[String(TEAM.id), TEAM], [String(OPP.id), OPP]]),
    teamPageState: { schedSeason: 2024 },
    STATIC_MANIFEST: { model_params: { coe2: config.coe } },
    esc: x => String(x),
    oppLink: name => `<a class="opp-link">${name}</a>`,
    panelHead: (title, control) => `<h2>${title}</h2>${control || ''}`,
    seasonSelect: () => '<select></select>',
    gameHref: (id, yr) => `#game=${id}&season=${yr}`,
    fmtKickoff: iso => `KICKOFF ${iso}`,
    fmtGameDate: g => `DATE ${String(g.date || g.kickoff_utc).slice(0, 10)}`,
    probPct: p => ({ home: Math.round(p * 100), away: Math.round((1 - p) * 100) }),
    phaseLabel: () => '',
    teamSeasonsList: () => [2024],
    wrapWideTables: () => {},
    logoMarkup: () => '',
    DATED_LOGOS: {},
    tp,
  };
  vm.createContext(ctx);
  vm.runInContext(
    'let liveResults = new Map();\nlet liveResultsSignature = "";\n' +
    sourceOf('signed') + '\n' + sourceOf('fmtWins') + '\n' + sourceOf('recStr') + '\n' +
    sourceOf('gameFor') + '\n' + sourceOf('eloShade') + '\n' + sourceOf('meritCell') + '\n' +
    sourceOf('meritConst') + '\n' + sourceOf('seasonMerits') + '\n' +
    sourceOf('showBowlingMerits') + '\n' + sourceOf('schedColumns') + '\n' +
    sourceOf('bowlingMeritCell') + '\n' + sourceOf('bowlingMeritBlank') + '\n' +
    sourceOf('seasonBowlingMerits') + '\n' +
    sourceOf('teamScheduleSeasons') + '\n' + sourceOf('nonFbsRecord') + '\n' +
    sourceOf('recWithUnrated') + '\n' + sourceOf('fillSchedulePanel') + '\n' +
    'const SHADE_FULL = 25, SHADE_MIN_PCT = 4, SHADE_MAX_PCT = 20;\n' +
    'this.fill = fillSchedulePanel;', ctx);
  ctx.fill(tp, TEAM);
  return el.innerHTML;
}

function headers(html) {
  const head = html.slice(html.indexOf('<thead>'), html.indexOf('</thead>'));
  return Array.from(head.matchAll(/<th scope="col"[^>]*>([\s\S]*?)<\/th>/g)).map(m => m[1]);
}
function bodyRows(html) {
  const body = html.slice(html.indexOf('<tbody>'), html.indexOf('</tbody>'));
  return Array.from(body.matchAll(/<tr[^>]*>[\s\S]*?<\/tr>/g)).map(m => m[0]);
}
function cells(row) {
  return row.match(/<td[^>]*>[\s\S]*?<\/td>/g) || [];
}
function lastCells(html) {
  return bodyRows(html).map(r => cells(r)[cells(r).length - 1]);
}
function footCells(html) {
  const foot = html.slice(html.indexOf('<tfoot>'), html.indexOf('</tfoot>'));
  return foot.match(/<td[^>]*>[\s\S]*?<\/td>/g) || [];
}

test('every row in the table is eleven cells wide', () => {
  // The column is public, so nothing is behind a query string any more. What
  // has to hold is that every kind of row grew by exactly one cell: a played
  // row, a row against an unrated opponent, a scheduled row, the group
  // separators' colspan and the footer.
  const html = panelHtml(fixture({
    unrated: true,
    upcoming: [{ id: 500, season: 2024, home: TEAM.id, away: OPP.id, neutral: false,
      phase: 0, week: 15, kickoff: '2024-12-07T18:00:00Z', tbd: false,
      homeElo: 1700, awayElo: 1600, pHome: 0.7 }],
  }));
  assert.equal(headers(html).length, 11);
  const dataRows = bodyRows(html).filter(r => !r.includes('sched-group'));
  assert.ok(dataRows.length >= 5, 'fixture rendered nothing to measure');
  for (const row of dataRows) assert.equal(cells(row).length, 11, row);
  assert.ok(dataRows.some(r => r.includes('tag-unrated')), 'no unrated row rendered');
  assert.ok(dataRows.some(r => r.includes('KICKOFF')), 'no scheduled row rendered');
  assert.equal(footCells(html).length, 9); // three columns are one colspan=3 cell
  assert.deepEqual(
    Array.from(html.matchAll(/<td colspan="(\d+)" class="sched-group">/g)).map(m => Number(m[1])),
    [11]);
});

test('with the flag the column is last and headed Bowling Merits', () => {
  const html = panelHtml(fixture());
  const heads = headers(html);
  assert.equal(heads.length, 11);
  assert.equal(heads[heads.length - 1], 'Bowling Merits');
  assert.equal(heads[heads.length - 2], 'Merits', 'it must sit after Merits, not replace it');
});

test('a win prints plainly and a regulation loss prints a negative', () => {
  // The negative is the fact that separates this column from Merits: the same
  // game shows 0.000 under Merits and a minus here.
  const html = panelHtml(fixture({ bowl: [3.344, -0.104, 2.0] }));
  assert.deepEqual(lastCells(html),
    ['<td>3.344</td>', '<td class="neg">−0.104</td>', '<td>2.000</td>']);
  // No '+' on the win: the column is an award, not a change.
  assert.ok(!html.includes('>+3.344<'), 'a win must not be signed like an Elo delta');
  const merits = bodyRows(html).map(r => cells(r)[9]);
  assert.equal(merits[1], '<td>0.000</td>',
    'the Merits column must still award the loss zero, which is the contrast');
});

test('an unscored game is a dash, never a zero', () => {
  // 0.000 is not an award any Bowling Merits outcome gives, so a dash and a
  // zero cannot be allowed to render alike here either.
  assert.deepEqual(lastCells(panelHtml(fixture({ bowl: [null, null, 1.5] }))),
    [DASH, DASH, '<td>1.500</td>']);
});

test("the footer is Python's season sum, not a sum of the column", () => {
  // 18.224 is deliberately not 3.344 + (-0.104): if the footer were adding the
  // rows up it would print 3.240.
  const html = panelHtml(fixture({ bowl: [3.344, -0.104, null], bowlingRollup: 18.224 }));
  const foot = footCells(html);
  assert.equal(foot[foot.length - 1], '<td>18.224</td>');
  assert.equal(foot[foot.length - 2], '<td>5.000</td>', 'the Merits total must be untouched');
});

test('a season with no research total dashes its footer', () => {
  const html = panelHtml(fixture({ bowlingRollup: null }));
  const foot = footCells(html);
  assert.equal(foot[foot.length - 1], DASH);
});

test('a negative season total keeps its minus in the footer', () => {
  const html = panelHtml(fixture({ bowlingRollup: -0.853 }));
  const foot = footCells(html);
  assert.equal(foot[foot.length - 1], '<td class="neg">−0.853</td>');
});

test('a scheduled row has no award and the group row spans all eleven columns', () => {
  const html = panelHtml(fixture({
    upcoming: [{ id: 500, season: 2024, home: TEAM.id, away: OPP.id, neutral: false,
      phase: 0, week: 15, kickoff: '2024-12-07T18:00:00Z', tbd: false,
      homeElo: 1700, awayElo: 1600, pHome: 0.7 }],
  }));
  const scheduled = bodyRows(html).filter(r => r.includes('KICKOFF'));
  assert.equal(scheduled.length, 1);
  assert.equal(cells(scheduled[0]).length, 11);
  assert.equal(cells(scheduled[0])[10], DASH);
  assert.deepEqual(
    Array.from(html.matchAll(/<td colspan="(\d+)" class="sched-group">/g)).map(m => Number(m[1])),
    [11]);
});

test('the help text says plainly that it is a research model', () => {
  const help = panelHtml(fixture()).slice(panelHtml(fixture()).indexOf('<details class="help">'));
  assert.match(help, /<b>Bowling Merits<\/b> is a <b>research model<\/b>/);
  // The two facts a reader needs in order not to misread the column.
  assert.match(help, /a regulation loss a <i>negative<\/i> award/);
  assert.match(help, /A tie is not scored at all/);
  // And that nothing else on the site is built on it.
  assert.match(help, /nothing\s+else on the site reads it/);
});

test('the caption names the research total', () => {
  assert.match(panelHtml(fixture()),
    /the season’s Merits from games, the season’s Bowling Merits\./);
});

test('one predicate decides whether the column shows at all', () => {
  // Hiding it again has to stay a one-line change: if showBowlingMerits() goes
  // false, the table must fall back to exactly the ten columns it had, with no
  // stray empty cell and no colspan left at eleven.
  const ctx = { tp: null };
  vm.createContext(ctx);
  vm.runInContext(
    'function showBowlingMerits() { return false; }\n' +
    sourceOf('schedColumns') + '\n' + sourceOf('bowlingMeritCell') + '\n' +
    sourceOf('bowlingMeritBlank') + '\n' +
    'this.cols = schedColumns; this.cell = bowlingMeritCell; this.blank = bowlingMeritBlank;',
    ctx);
  assert.equal(ctx.cols(), 10);
  assert.equal(ctx.cell(3.344), '');
  assert.equal(ctx.cell(null), '');
  assert.equal(ctx.blank(), '');
});
