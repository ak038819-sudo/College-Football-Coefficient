// A finished game reaches the live scoreboard within minutes and the model
// export only on its own rebuild schedule. These tests drive the real schedule
// panel so a regression that silently drops the live score (or stops calling
// the row "not yet rated") fails here rather than on the published site.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const shell = fs.readFileSync('ui/dashboard_shell.html', 'utf8');
function sourceOf(name) {
  const match = shell.match(new RegExp(`function ${name}\\([^)]*\\) \\{[\\s\\S]*?\\n\\}`));
  assert.ok(match, `missing ${name}`);
  return match[0];
}

// Pitt at Virginia Tech, 2026 week 5: the game from the report. The model
// export still lists it as upcoming; the scoreboard has it final 35-33 Pitt.
const PITT = { id: 88, name: 'Pittsburgh' };
const VT = { id: 92, name: 'Virginia Tech' };
const GAME_ID = 401858245;

function upcomingGame(overrides = {}) {
  return Object.assign({
    id: GAME_ID, season: 2026, week: 5, kickoff: '2026-10-02T23:00:00.000Z', tbd: false,
    home: VT.id, away: PITT.id, neutral: false, phase: 0,
    homeElo: 1534, awayElo: 1604, pHome: 0.47, homeProv: false, awayProv: false, venue: 'Lane Stadium',
  }, overrides);
}

function liveGame(overrides = {}) {
  return Object.assign({
    id: GAME_ID, start_date: '2026-10-02T23:00:00.000Z', status: 'completed',
    home: { name: 'Virginia Tech Hokies', points: 33 },
    away: { name: 'Pittsburgh Panthers', points: 35 },
  }, overrides);
}

// Build a context holding the real panel plus the smallest honest stand-ins for
// the page around it. Nothing here reimplements what the panel is being tested
// for: the rows, the grouping and the score all come from the shipped source.
function panelContext({ upcoming = [upcomingGame()], liveGames = [] } = {}) {
  const el = { innerHTML: '' };
  const ctx = {
    document: {
      getElementById: id => (id === 'tp-schedule' ? el : null),
    },
    UPCOMING_GAMES: upcoming,
    UPCOMING_BY_ID: new Map(upcoming.map(g => [g.id, g])),
    TEAM_BY_ID: new Map([[String(VT.id), VT], [String(PITT.id), PITT]]),
    teamPageState: { schedSeason: 2026 },
    esc: x => String(x),
    oppLink: name => `<a>${name}</a>`,
    panelHead: (title, control) => `<h2>${title}</h2>${control || ''}`,
    seasonSelect: () => '<select></select>',
    gameHref: (id, season) => `#game=${id}&year=${season}`,
    fmtKickoff: iso => `KICKOFF ${iso}`,
    fmtGameDate: g => `DATE ${String(g.kickoff_utc || g.date).slice(0, 10)}`,
    fmtDate: d => d,
    probPct: p => ({ home: Math.round(p * 100), away: Math.round((1 - p) * 100) }),
    signed: (n, d) => (n >= 0 ? '+' : '') + n.toFixed(d),
    recStr: (w, l) => `${w}-${l}`,
    fmtWins: n => String(n),
    gameFor: () => ({ where: 'vs', opp: VT, neutral: false, result: 'W', mine: 1, theirs: 0 }),
    phaseLabel: () => '',
    teamSeasonsList: () => [2026],
    // Pitt's three played games are not what these tests are about; an empty
    // history keeps the assertions on the live rows unambiguous.
    tp: { eloByTeam: new Map(), gamesById: new Map(), seasonsByTeam: new Map(), teamsPerSeason: new Map() },
  };
  vm.createContext(ctx);
  vm.runInContext(
    'let liveResults = new Map();\n' +
    sourceOf('setLiveResults') + '\n' +
    sourceOf('teamScheduleSeasons') + '\n' +
    sourceOf('fillSchedulePanel') + '\n' +
    'this.setLiveResults = setLiveResults; this.fill = fillSchedulePanel;', ctx);
  ctx.setLiveResults(liveGames);
  ctx.fill(ctx.tp, PITT);
  return el.innerHTML;
}

test('with no scoreboard the game still reads as an upcoming kickoff', () => {
  const html = panelContext();
  assert.match(html, /Scheduled · current ratings, not yet played/);
  assert.match(html, /KICKOFF 2026-10-02T23:00:00\.000Z/);
  assert.doesNotMatch(html, /sched-group">Live scoreboard/);
  assert.doesNotMatch(html, /35–33/);
});

test('a final on the scoreboard shows its score on the team schedule', () => {
  const html = panelContext({ liveGames: [liveGame()] });
  // Pitt won 35-33 on the road, so Pitt's own row reads W 35-33.
  assert.match(html, /<td class="win">W 35–33<\/td>/);
  assert.match(html, /sched-group">Live scoreboard · score is in, the ratings have not rebuilt yet/);
  assert.doesNotMatch(html, /Scheduled · current ratings/);
  // The date replaces the kickoff time once there is a result.
  assert.doesNotMatch(html, /KICKOFF/);
});

test('the losing side of the same final reads as a loss', () => {
  const html = panelContextFor(VT, [upcomingGame()], [liveGame()]);
  assert.match(html, /<td class="loss">L 33–35<\/td>/);
});

// The same harness, viewed from the other team's page.
function panelContextFor(team, upcoming, liveGames) {
  const el = { innerHTML: '' };
  const ctx = {
    document: { getElementById: id => (id === 'tp-schedule' ? el : null) },
    UPCOMING_GAMES: upcoming,
    UPCOMING_BY_ID: new Map(upcoming.map(g => [g.id, g])),
    TEAM_BY_ID: new Map([[String(VT.id), VT], [String(PITT.id), PITT]]),
    teamPageState: { schedSeason: 2026 },
    esc: x => String(x), oppLink: name => `<a>${name}</a>`,
    panelHead: (t, c) => `<h2>${t}</h2>${c || ''}`, seasonSelect: () => '',
    gameHref: id => `#game=${id}`, fmtKickoff: iso => `KICKOFF ${iso}`,
    fmtGameDate: g => `DATE ${String(g.kickoff_utc || g.date).slice(0, 10)}`,
    probPct: p => ({ home: Math.round(p * 100), away: Math.round((1 - p) * 100) }),
    signed: (n, d) => n.toFixed(d), recStr: (w, l) => `${w}-${l}`, fmtWins: String,
    gameFor: () => ({ where: 'vs', opp: PITT, neutral: false, result: 'W', mine: 1, theirs: 0 }),
    phaseLabel: () => '', teamSeasonsList: () => [2026],
  };
  const tp = { eloByTeam: new Map(), gamesById: new Map(), seasonsByTeam: new Map(), teamsPerSeason: new Map() };
  vm.createContext(ctx);
  vm.runInContext(
    'let liveResults = new Map();\n' + sourceOf('setLiveResults') + '\n' +
    sourceOf('teamScheduleSeasons') + '\n' + sourceOf('fillSchedulePanel') + '\n' +
    'this.setLiveResults = setLiveResults; this.fill = fillSchedulePanel;', ctx);
  ctx.setLiveResults(liveGames);
  ctx.fill(tp, team);
  return el.innerHTML;
}

test('a game in progress shows the running score, not a kickoff time', () => {
  const html = panelContext({
    liveGames: [liveGame({ status: 'in_progress', period: 3, home: { name: 'Virginia Tech Hokies', points: 14 }, away: { name: 'Pittsburgh Panthers', points: 21 } })],
  });
  assert.match(html, /<td class="live-now">Live 21–14 · Q3<\/td>/);
  assert.match(html, /sched-group">Live scoreboard/);
});

test('a scheduled game in the feed is left alone, zero-zero points included', () => {
  // CFBD carries a pregame row with 0-0 on the board. Reading status rather
  // than the presence of points keeps that from rendering as a 0-0 tie.
  for (const sides of [
    { home: { name: 'Virginia Tech Hokies' }, away: { name: 'Pittsburgh Panthers' } },
    { home: { name: 'Virginia Tech Hokies', points: 0 }, away: { name: 'Pittsburgh Panthers', points: 0 } },
  ]) {
    const html = panelContext({ liveGames: [liveGame(Object.assign({ status: 'scheduled' }, sides))] });
    assert.match(html, /Scheduled · current ratings, not yet played/);
    assert.doesNotMatch(html, /sched-group">Live scoreboard/);
    assert.doesNotMatch(html, /0–0/);
  }
});

test('a live row keeps the pregame rating columns blank of post-game values', () => {
  const html = panelContext({ liveGames: [liveGame()] });
  const row = html.slice(html.indexOf('<td class="win">'));
  // Elo, Opp Elo and win prob are the pregame numbers; Elo delta and Elo after
  // stay em-dashes, because the model has not rated this game.
  assert.match(row, /<td>1604<\/td><td>1534<\/td>/);
  assert.match(row, /<td>53%<\/td><td class="dim">—<\/td><td class="dim">—<\/td>/);
});

test('both live and still-scheduled games appear, each under its own heading', () => {
  const later = upcomingGame({ id: 999, week: 6, kickoff: '2026-10-10T16:00:00.000Z', home: PITT.id, away: 77 });
  const html = panelContext({ upcoming: [upcomingGame(), later], liveGames: [liveGame()] });
  assert.ok(html.indexOf('sched-group">Live scoreboard') < html.indexOf('Scheduled · current ratings'),
    'finished games belong above the ones still to come');
  assert.match(html, /W 35–33/);
  assert.match(html, /KICKOFF 2026-10-10T16:00:00\.000Z/);
});

test('a final the model already rated is not re-shown from the scoreboard', () => {
  // The rebuild removed the game from the upcoming export, so nothing in the
  // panel's scheduled section can duplicate the history row above it.
  const html = panelContext({ upcoming: [], liveGames: [liveGame()] });
  assert.doesNotMatch(html, /sched-group">Live scoreboard/);
  assert.doesNotMatch(html, /35–33/);
});

test('a newer final is detected by game id, not by comparing kickoff to a date', () => {
  const upcoming = [upcomingGame()];
  const ctx = {
    liveTeamId: t => (String(t.name).startsWith('Pitt') ? PITT : VT),
    UPCOMING_BY_ID: new Map(upcoming.map(g => [g.id, g])),
  };
  vm.createContext(ctx);
  vm.runInContext(sourceOf('pendingFinals') + '\n' + sourceOf('newerFinalTeams') +
    '\nthis.newer = newerFinalTeams;', ctx);

  // The regression this replaces: a Friday night game kicks off at 23:00 UTC on
  // the same date the model exported, and ends after midnight UTC. Date math
  // called it "not newer" and the page never warned that the result was missing.
  assert.deepEqual([...ctx.newer([liveGame()])].sort(), ['Pittsburgh', 'Virginia Tech']);
  // Still scheduled, or already absorbed into the model: neither is newer.
  assert.equal(ctx.newer([liveGame({ status: 'scheduled' })]).size, 0);
  assert.equal(ctx.newer([liveGame({ id: 123456 })]).size, 0);
});
