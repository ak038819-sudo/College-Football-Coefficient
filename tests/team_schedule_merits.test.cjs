// The Merits column on a team's schedule. Merits is the site's achievement
// measure, and the number on a row is the award build_hybrid_coefficients.py
// stored for that game -- not something recomputed in the browser. These tests
// drive the REAL schedule panel, because the two facts the column has to keep
// straight are both easy to lose: an absent award is not a zero, and the footer
// is Python's own season figure rather than a sum taken here.
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

// A season of three rated games: an upset win, a loss, and a game the hybrid
// layer never valued. The three awards are deliberately different kinds of
// value -- a number, a real zero, and nothing at all.
function fixture({ awards = [3.418, 0, null], rollup = { games: 3.418, bonus: 0, total: 3.418, counted: 3, items: [] } } = {}) {
  const dates = ['2024-09-07', '2024-10-12', '2024-11-30'];
  const gamesById = new Map(dates.map((date, i) => [100 + i, {
    id: 100 + i, season: 2024, date, week: 2 + i, home: i === 1 ? OPP.id : TEAM.id,
    away: i === 1 ? TEAM.id : OPP.id, hs: i === 1 ? 28 : 42, as: i === 1 ? 21 : 10,
    neutral: false, phase: 0, ot: false, kickoff_utc: null,
  }]));
  return {
    gamesById,
    eloByTeam: new Map([[TEAM.id, awards.map((coe, i) => ({
      gid: 100 + i, tid: TEAM.id, pre: 1700, oppPre: 1500, exp: 0.78,
      chg: i === 1 ? -11.4 : 6.2, post: 1706.2, mov: 1, coe,
    }))]]),
    seasonsByTeam: new Map([[TEAM.id, new Map([[2024, {
      tid: TEAM.id, season: 2024, games: 3, w: 2, l: 1, t: 0, startElo: 1700, endElo: 1694.8,
      eloChange: -5.2, sos: 1575, sosRank: 20, expWins: 1.38, actWins: 2, aboveExp: 0.62,
      coe2: 3.418, coe2Rank: 12, conf: 'SEC',
    }]])]]),
    teamsPerSeason: new Map([[2024, 134]]),
    nonFbsByTeam: new Map(),
    nonFbsRecords: {},
    rollups: rollup ? { [TEAM.id + '|2024']: rollup } : {},
  };
}

function panelHtml(tp, { team = TEAM, season = 2024 } = {}) {
  const el = { innerHTML: '' };
  const ctx = {
    document: { getElementById: id => (id === 'tp-schedule' ? el : null) },
    UPCOMING_GAMES: [],
    TEAM_BY_ID: new Map([[String(TEAM.id), TEAM], [String(OPP.id), OPP]]),
    teamPageState: { schedSeason: season },
    // The build copies config/model_config.json's coe2 section into the
    // manifest, so the help text is read from the real values, not from prose
    // typed into the page.
    STATIC_MANIFEST: { model_params: { coe2: config.coe } },
    esc: x => String(x),
    oppLink: name => `<a class="opp-link">${name}</a>`,
    panelHead: (title, control) => `<h2>${title}</h2>${control || ''}`,
    seasonSelect: () => '<select></select>',
    gameHref: (id, yr) => `#game=${id}&season=${yr}`,
    fmtKickoff: iso => `KICKOFF ${iso}`,
    fmtGameDate: g => `DATE ${String(g.date).slice(0, 10)}`,
    probPct: p => ({ home: Math.round(p * 100), away: Math.round((1 - p) * 100) }),
    phaseLabel: () => '',
    teamSeasonsList: () => [2024],
    wrapWideTables: () => {},
    DATED_LOGOS: {},
    tp,
  };
  vm.createContext(ctx);
  // Every formatter here is the shipped one: a stub for signed(), recStr() or
  // meritCell() would let this file pass while the column was wrong.
  vm.runInContext(
    'let liveResults = new Map();\nlet liveResultsSignature = "";\n' +
    sourceOf('signed') + '\n' +
    sourceOf('fmtWins') + '\n' +
    sourceOf('recStr') + '\n' +
    sourceOf('gameFor') + '\n' +
    sourceOf('eloShade') + '\n' +
    sourceOf('meritCell') + '\n' +
    sourceOf('meritConst') + '\n' +
    sourceOf('seasonMerits') + '\n' +
    sourceOf('teamScheduleSeasons') + '\n' +
    sourceOf('nonFbsRecord') + '\n' +
    sourceOf('recWithUnrated') + '\n' +
    sourceOf('fillSchedulePanel') + '\n' +
    'const SHADE_FULL = 25, SHADE_MIN_PCT = 4, SHADE_MAX_PCT = 20;\n' +
    'this.fill = fillSchedulePanel;', ctx);
  ctx.fill(tp, team);
  return el.innerHTML;
}

// The last cell of each body row, which is the Merits column.
function meritColumn(html) {
  const body = html.slice(html.indexOf('<tbody>'), html.indexOf('</tbody>'));
  return Array.from(body.matchAll(/<tr[^>]*>[\s\S]*?<\/tr>/g))
    .map(row => {
      const cells = row[0].match(/<td[^>]*>[\s\S]*?<\/td>/g) || [];
      return cells[cells.length - 1];
    });
}
const DASH = '<td class="dim">—</td>';

test('the schedule has a Merits column, last, with a header of its own', () => {
  const html = panelHtml(fixture());
  const head = html.slice(html.indexOf('<thead>'), html.indexOf('</thead>'));
  const heads = Array.from(head.matchAll(/<th scope="col">([\s\S]*?)<\/th>/g)).map(m => m[1]);
  assert.deepEqual(heads, ['Date', 'Opponent', 'Round', 'Result', 'Elo', 'Opp Elo',
    'Win prob', 'Elo &Delta;', 'Elo after', 'Merits']);
});

test("a game's row carries the award that game earned", () => {
  assert.deepEqual(meritColumn(panelHtml(fixture({ awards: [3.418, 0, 2.5] }))),
    ['<td>3.418</td>', '<td>0.000</td>', '<td>2.500</td>']);
});

// The distinction the column exists to keep: a regulation loss really does earn
// zero Merits, while a bootstrap season or a tie has no award at all. Rendering
// both as 0.000, or both as a dash, loses a fact either way.
test('an absent award is a dash and a zero award is a zero', () => {
  const col = meritColumn(panelHtml(fixture({ awards: [null, 0, null] })));
  assert.deepEqual(col, [DASH, '<td>0.000</td>', DASH]);
});

test("the footer shows Python's own season total from games, not a browser sum", () => {
  // 9.9 is not the sum of the rows: if the footer were adding the column up it
  // would print 3.418, and the point is that it prints the stored figure.
  const html = panelHtml(fixture({ awards: [3.418, 0, null],
    rollup: { games: 9.9, bonus: 0, total: 9.9, counted: 3, items: [] } }));
  const foot = html.slice(html.indexOf('<tfoot>'), html.indexOf('</tfoot>'));
  const cells = foot.match(/<td[^>]*>[\s\S]*?<\/td>/g);
  assert.equal(cells[cells.length - 1], '<td>9.900</td>');
});

// The rollups carry a zero-game placeholder row for every season the awards
// never covered. Reading its 0.0 as a measured zero had Ohio State's 1982
// footer claim 0.000 Merits under twelve dashed rows, which is the one reading
// this column must never produce.
test('a placeholder rollup row over zero games is no measurement', () => {
  const html = panelHtml(fixture({ awards: [null, null, null],
    rollup: { games: 0, bonus: 0, total: 0, counted: 0, items: [] } }));
  const foot = html.slice(html.indexOf('<tfoot>'), html.indexOf('</tfoot>'));
  const cells = foot.match(/<td[^>]*>[\s\S]*?<\/td>/g);
  assert.equal(cells[cells.length - 1], DASH);
  assert.deepEqual(meritColumn(html), [DASH, DASH, DASH]);
});

test('a real zero-game count does not suppress a season that was measured', () => {
  const html = panelHtml(fixture({ awards: [2.0, 0, 1.5],
    rollup: { games: 3.5, bonus: 0, total: 3.5, counted: 3, items: [] } }));
  const foot = html.slice(html.indexOf('<tfoot>'), html.indexOf('</tfoot>'));
  const cells = foot.match(/<td[^>]*>[\s\S]*?<\/td>/g);
  assert.equal(cells[cells.length - 1], '<td>3.500</td>');
});

test('a season with no rollup row dashes its footer rather than printing zero', () => {
  const html = panelHtml(fixture({ rollup: null }));
  const foot = html.slice(html.indexOf('<tfoot>'), html.indexOf('</tfoot>'));
  const cells = foot.match(/<td[^>]*>[\s\S]*?<\/td>/g);
  assert.equal(cells[cells.length - 1], DASH);
});

// The group rows that separate live and scheduled games span the whole table;
// one column short and they pull the table out of shape.
test('the grouping rows span every column', () => {
  const upcoming = [{ id: 500, season: 2024, home: TEAM.id, away: OPP.id, neutral: false,
    phase: 0, week: 15, kickoff: '2024-12-07T18:00:00Z', tbd: false, homeElo: 1700,
    awayElo: 1600, pHome: 0.7 }];
  const el = { innerHTML: '' };
  const tp = fixture();
  const ctx = {
    document: { getElementById: id => (id === 'tp-schedule' ? el : null) },
    UPCOMING_GAMES: upcoming,
    TEAM_BY_ID: new Map([[String(TEAM.id), TEAM], [String(OPP.id), OPP]]),
    teamPageState: { schedSeason: 2024 },
    STATIC_MANIFEST: { model_params: { coe2: config.coe } },
    esc: x => String(x), oppLink: n => `<a>${n}</a>`,
    panelHead: (t2, c) => `<h2>${t2}</h2>${c || ''}`, seasonSelect: () => '<select></select>',
    gameHref: (id, yr) => `#game=${id}&season=${yr}`, fmtKickoff: iso => `KICKOFF ${iso}`,
    fmtGameDate: g => `DATE ${String(g.date || g.kickoff_utc).slice(0, 10)}`,
    probPct: p => ({ home: Math.round(p * 100), away: Math.round((1 - p) * 100) }),
    phaseLabel: () => '', teamSeasonsList: () => [2024], wrapWideTables: () => {},
    DATED_LOGOS: {}, tp,
  };
  vm.createContext(ctx);
  vm.runInContext(
    'let liveResults = new Map();\nlet liveResultsSignature = "";\n' +
    sourceOf('signed') + '\n' + sourceOf('fmtWins') + '\n' + sourceOf('recStr') + '\n' +
    sourceOf('gameFor') + '\n' + sourceOf('eloShade') + '\n' + sourceOf('meritCell') + '\n' +
    sourceOf('meritConst') + '\n' + sourceOf('seasonMerits') + '\n' +
    sourceOf('teamScheduleSeasons') + '\n' + sourceOf('nonFbsRecord') + '\n' +
    sourceOf('recWithUnrated') + '\n' + sourceOf('fillSchedulePanel') + '\n' +
    'const SHADE_FULL = 25, SHADE_MIN_PCT = 4, SHADE_MAX_PCT = 20;\n' +
    'this.fill = fillSchedulePanel;', ctx);
  ctx.fill(tp, TEAM);
  const html = el.innerHTML;
  const columns = Array.from(html.matchAll(/<th scope="col">/g)).length;
  const spans = Array.from(html.matchAll(/class="sched-group"/g)).length;
  assert.ok(spans > 0, 'no grouping row rendered');
  assert.deepEqual(Array.from(html.matchAll(/<td colspan="(\d+)" class="sched-group">/g)).map(m => Number(m[1])),
    new Array(spans).fill(columns));
});

// A scheduled game has not been played, so it has earned nothing yet. That is
// an em-dash for the same reason its Elo change is one.
test('a scheduled row has no award yet', () => {
  const upcoming = [{ id: 500, season: 2024, home: TEAM.id, away: OPP.id, neutral: false,
    phase: 0, week: 15, kickoff: '2024-12-07T18:00:00Z', tbd: false, homeElo: 1700,
    awayElo: 1600, pHome: 0.7 }];
  const tp = fixture();
  const el = { innerHTML: '' };
  const ctx = {
    document: { getElementById: id => (id === 'tp-schedule' ? el : null) },
    UPCOMING_GAMES: upcoming,
    TEAM_BY_ID: new Map([[String(TEAM.id), TEAM], [String(OPP.id), OPP]]),
    teamPageState: { schedSeason: 2024 },
    STATIC_MANIFEST: { model_params: { coe2: config.coe } },
    esc: x => String(x), oppLink: n => `<a>${n}</a>`,
    panelHead: (t2, c) => `<h2>${t2}</h2>${c || ''}`, seasonSelect: () => '<select></select>',
    gameHref: (id, yr) => `#game=${id}&season=${yr}`, fmtKickoff: iso => `KICKOFF ${iso}`,
    fmtGameDate: g => `DATE ${String(g.date || g.kickoff_utc).slice(0, 10)}`,
    probPct: p => ({ home: Math.round(p * 100), away: Math.round((1 - p) * 100) }),
    phaseLabel: () => '', teamSeasonsList: () => [2024], wrapWideTables: () => {},
    DATED_LOGOS: {}, tp,
  };
  vm.createContext(ctx);
  vm.runInContext(
    'let liveResults = new Map();\nlet liveResultsSignature = "";\n' +
    sourceOf('signed') + '\n' + sourceOf('fmtWins') + '\n' + sourceOf('recStr') + '\n' +
    sourceOf('gameFor') + '\n' + sourceOf('eloShade') + '\n' + sourceOf('meritCell') + '\n' +
    sourceOf('meritConst') + '\n' + sourceOf('seasonMerits') + '\n' +
    sourceOf('teamScheduleSeasons') + '\n' + sourceOf('nonFbsRecord') + '\n' +
    sourceOf('recWithUnrated') + '\n' + sourceOf('fillSchedulePanel') + '\n' +
    'const SHADE_FULL = 25, SHADE_MIN_PCT = 4, SHADE_MAX_PCT = 20;\n' +
    'this.fill = fillSchedulePanel;', ctx);
  ctx.fill(tp, TEAM);
  const html = el.innerHTML;
  const columns = Array.from(html.matchAll(/<th scope="col">/g)).length;
  const body = html.slice(html.indexOf('<tbody>'), html.indexOf('</tbody>'));
  const scheduled = Array.from(body.matchAll(/<tr[^>]*>[\s\S]*?<\/tr>/g))
    .map(r => r[0]).filter(r => r.includes('KICKOFF'));
  assert.equal(scheduled.length, 1, 'no scheduled row rendered');
  // The cell count is what proves the column is there: the last cell of a
  // scheduled row is a dash whether or not the Merits cell was written.
  assert.equal((scheduled[0].match(/<td[^>]*>[\s\S]*?<\/td>/g) || []).length, columns);
  assert.equal(meritColumn(html).pop(), DASH, 'the scheduled row must not claim an award');
});

// The help text explains the award, and every number in it is the model's own.
// A value typed into the prose would go stale the moment the config changed.
test('the help text prints the configured award values, not typed ones', () => {
  const html = panelHtml(fixture());
  const help = html.slice(html.indexOf('<details class="help">'));
  const C = config.coe;
  // Pinned as a sentence, not as "the number appears somewhere": win_base and
  // difficulty_alpha are both 2.0 in the shipped config, so a line that printed
  // one in place of the other would satisfy any looser check.
  assert.match(help, new RegExp(
    '<b>Merits</b> is what the result earned: a win is worth at least ' + C.win_base +
    ', plus up to ' + C.difficulty_alpha + ' more the less likely the win was, and a loss earns ' +
    C.regulation_loss + ' \\(' + C.ot_loss + ' in overtime\\)'));
  // The award's difficulty is the hybrid expectation, not the Elo win
  // probability in the column beside it; saying so is the point of the line.
  assert.match(help, /not the <b>Win prob<\/b> beside it/);
});

// Analytics shows the same season decomposed, from the same rollup rows, so it
// has to make the same judgement: a placeholder row is not a season total of
// zero. Two tabs of one page contradicting each other is the bug here.
test('the Analytics season breakdown ignores a placeholder row too', () => {
  const ctx = { esc: x => String(x), BONUS_LABELS: {} };
  vm.createContext(ctx);
  vm.runInContext(sourceOf('coe2SeasonBreakdown') + '\nthis.fn = coe2SeasonBreakdown;', ctx);
  const team = { id: 7 };
  const placeholder = { rollups: { '7|1982': { games: 0, bonus: 0, total: 0, counted: 0, items: [] } } };
  const measured = { rollups: { '7|1985': { games: 22.715, bonus: 0, total: 22.715, counted: 12, items: [] } } };
  assert.equal(ctx.fn(placeholder, team, 1982), '', 'a zero-game season has nothing to decompose');
  assert.match(ctx.fn(measured, team, 1985), /22\.715/);
});
