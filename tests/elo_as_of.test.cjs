// The Elo table rewound to a point in the season.
//
// Every column is recomputed in the browser from the per-game chain in
// team_pages.js, counting only the games played by the chosen stage. So the
// check that matters is the one the data can settle: run the recomputation at
// the LAST stage and it must reproduce, team by team, the season numbers the
// exporter itself wrote. If it does, the arithmetic is right and the only thing
// a mid-season stage changes is which games are counted.
//
// Helpers are lifted from the shell for real rather than stubbed.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const shell = fs.readFileSync('ui/dashboard_shell.html', 'utf8');
function sourceOf(name) {
  const match = shell.match(new RegExp(`function ${name}\\([^)]*\\) \\{[\\s\\S]*?\\n\\}`));
  assert.ok(match, `missing ${name}`);
  return match[0];
}
function constOf(name) {
  const match = shell.match(new RegExp(`const ${name} = \\[[\\s\\S]*?\\];`));
  assert.ok(match, `missing ${name}`);
  return match[0];
}

const ctx = {};
vm.runInNewContext(
  constOf('POSTSEASON_PHASES') + '\n' + sourceOf('gameStageKey') + '\n' +
  sourceOf('eloAsOfTeamGames') + '\n' + sourceOf('eloAsOfRecord') + '\n' +
  sourceOf('eloAsOfStrength') + '\n' + sourceOf('eloAsOfRows'), ctx);

// ---- the shipped payloads, loaded the way the browser loads them ------------

function loadGlobalScript(file, globalName) {
  const full = path.join(process.cwd(), file);
  if (!fs.existsSync(full)) return null;
  const sandbox = { window: {} };
  vm.runInNewContext(fs.readFileSync(full, 'utf8'), sandbox);
  return sandbox.window[globalName] ?? sandbox[globalName] ?? null;
}

const rawTeamPages = loadGlobalScript('ui/data/team_pages.js', '__TEAM_PAGES__');
const tp = rawTeamPages && (() => {
  // The shell's own indexer, so the shapes under test are the shapes the page
  // reads -- the phase code in particular, which is an integer here and a word
  // in the database.
  const holder = { raw: rawTeamPages, result: null };
  vm.runInNewContext(sourceOf('indexTeamPages') + '\nresult = indexTeamPages(raw);', holder);
  return holder.result;
})();

function timelineFor(season) {
  const file = `ui/data/elo_timeline/${season}.js`;
  const raw = loadGlobalScript(file, '__CFB_ELO_TIMELINE__');
  if (!raw || !raw[season]) return null;
  const payload = raw[season];
  const unpack = (fields, row) => Object.fromEntries(fields.map((f, i) => [f, row[i]]));
  return {
    season,
    teams: payload.teams.map(([id, name, conference]) => ({ id, name, conference: conference || null })),
    stages: payload.stages.map(r => unpack(payload.stage_fields, r)),
    rows: payload.rows.map(stage => stage.map(r => unpack(payload.row_fields, r))),
  };
}

// Finished seasons, so the last stage really is the whole season -- the current
// one is still being played and its "final" numbers are not final. 1990 is in
// the list on purpose: college football still had TIES, and a tie is the one
// result the sign of an Elo change cannot tell you (it is K x (0.5 - exp), so
// it takes whichever sign the expectation had).
const SEASONS = [1990, 2015, 2024];

function available(season) {
  return tp && timelineFor(season);
}

// ---- the stage rule agrees with the exporter's own -------------------------

test('a game lands in the stage the exporter counted it in', () => {
  let checked = 0;
  for (const season of SEASONS) {
    const timeline = available(season);
    if (!timeline) continue;
    // Count every game of the season by the browser's rule, then compare with
    // the per-stage game counts src/elo_timeline.py wrote into the payload.
    const counted = new Map();
    for (const game of tp.gamesById.values()) {
      if (game.season !== season || game.hs == null || game.as == null) continue;
      const key = ctx.gameStageKey(game);
      counted.set(key, (counted.get(key) || 0) + 1);
    }
    for (const stage of timeline.stages) {
      if (stage.key === 'pre') { assert.equal(stage.games, 0, season); continue; }
      assert.equal(counted.get(stage.key) || 0, stage.games,
        `${season} ${stage.key}: the two stage rules disagree`);
      checked++;
    }
  }
  if (!checked) {
    test.skip('no shipped timeline or team pages to check against');
    return;
  }
  assert.ok(checked > 10, 'too few stages checked to mean anything');
});

// ---- the recomputation reproduces the exporter's season numbers ------------

test('at the last stage every column equals the season the exporter exported', () => {
  let teamsChecked = 0;
  for (const season of SEASONS) {
    const timeline = available(season);
    if (!timeline) continue;
    const rows = ctx.eloAsOfRows(tp, timeline, timeline.stages.length - 1);
    assert.ok(rows.length, `${season} has no rows`);
    for (const row of rows) {
      const exported = (tp.seasonsByTeam.get(row.tid) || new Map()).get(season);
      if (!exported) continue;
      // The RATED part of the record: the exporter's season record counts only
      // the games that feed Elo, while the column shows those plus the games
      // against opponents this project holds no rating for.
      assert.equal(row.record.rated.w, exported.w, `${season} ${row.name} wins`);
      assert.equal(row.record.rated.l, exported.l, `${season} ${row.name} losses`);
      assert.equal(row.record.rated.t, exported.t, `${season} ${row.name} ties`);
      assert.ok(row.record.w >= row.record.rated.w, 'an unrated game can only add wins');
      // Not exact, and cannot be: the exporter averages the opponents' ratings
      // at full precision and rounds once, while the browser averages the
      // SHIPPED values, each already rounded to a tenth. Over a twelve-game
      // season that drifts by up to about a tenth (worst in 2015: Fresno State,
      // 1445.33 against a stored 1445.4). A tenth of an Elo point of schedule
      // strength is not a number anyone reads; a whole point would mean the
      // recomputation is counting the wrong games.
      assert.ok(Math.abs(row.sos - exported.sos) <= 0.15, `${season} ${row.name} SOS`);
      assert.equal(row.sosRank, exported.sosRank, `${season} ${row.name} SOS rank`);
      assert.ok(Math.abs(row.aboveExp - exported.aboveExp) < 0.05,
        `${season} ${row.name} wins above expectation`);
      teamsChecked++;
    }
  }
  if (!teamsChecked) {
    test.skip('no shipped timeline or team pages to check against');
    return;
  }
  assert.ok(teamsChecked > 100, `only ${teamsChecked} teams checked`);
});

// ---- what a mid-season stage must mean -------------------------------------

test('a mid-season stage counts fewer games than the season, for everyone', () => {
  const season = SEASONS.find(available);
  if (!season) { test.skip('no shipped payloads'); return; }
  const timeline = timelineFor(season);
  const middle = Math.floor(timeline.stages.length / 2);
  const early = ctx.eloAsOfRows(tp, timeline, middle);
  const final = ctx.eloAsOfRows(tp, timeline, timeline.stages.length - 1);
  const byId = new Map(final.map(r => [r.tid, r]));
  let fewer = 0;
  for (const row of early) {
    const end = byId.get(row.tid);
    const played = row.record.w + row.record.l + row.record.t;
    const ended = end.record.w + end.record.l + end.record.t;
    assert.ok(played <= ended, `${row.name} played more by week ${middle} than all season`);
    if (played < ended) fewer++;
  }
  assert.ok(fewer > early.length / 2, 'half the league should have games still to play mid-season');
});

test('preseason has no record, no schedule faced and no wins above expectation', () => {
  const season = SEASONS.find(available);
  if (!season) { test.skip('no shipped payloads'); return; }
  const rows = ctx.eloAsOfRows(tp, timelineFor(season), 0);
  assert.ok(rows.length);
  for (const row of rows) {
    assert.equal(row.record.text, '', `${row.name} has a record before a game was played`);
    assert.equal(row.sos, null, `${row.name} has faced a schedule before a game was played`);
    assert.equal(row.aboveExp, null);
    assert.equal(row.last5, null);
  }
});

test('the rank and the rating come from the timeline, never recomputed here', () => {
  const season = SEASONS.find(available);
  if (!season) { test.skip('no shipped payloads'); return; }
  const timeline = timelineFor(season);
  const index = Math.floor(timeline.stages.length / 2);
  const rows = ctx.eloAsOfRows(tp, timeline, index);
  const source = timeline.rows[index];
  assert.equal(rows.length, source.length);
  rows.forEach((row, i) => {
    assert.equal(row.rank, source[i].rank);
    assert.equal(row.elo, source[i].elo);
  });
});

test('schedule strength ranks 1 as the toughest and ties share a rank', () => {
  const season = SEASONS.find(available);
  if (!season) { test.skip('no shipped payloads'); return; }
  const timeline = timelineFor(season);
  const rows = ctx.eloAsOfRows(tp, timeline, timeline.stages.length - 1)
    .filter(r => r.sos != null).sort((a, b) => a.sosRank - b.sosRank);
  assert.equal(rows[0].sosRank, 1);
  for (let i = 1; i < rows.length; i++) {
    assert.ok(rows[i].sos <= rows[i - 1].sos, 'rank 1 must be the toughest schedule');
    if (rows[i].sos === rows[i - 1].sos) assert.equal(rows[i].sosRank, rows[i - 1].sosRank);
  }
});

// ---- which ballot answers "as of week N" -----------------------------------

const pollCtx = {};
vm.runInNewContext(sourceOf('pollReleaseIndexForStage') + '\n' + sourceOf('conferenceEloAsOf'), pollCtx);

// Releases numbered the way CFBD numbers them: the first is the preseason
// ballot, voted before a game, and the last is the postseason one, numbered
// week 1 again.
const TRACK = { releases: [
  { key: 'r1', season_type: 'regular', week: 1 },
  { key: 'r2', season_type: 'regular', week: 2 },
  { key: 'r3', season_type: 'regular', week: 3 },
  { key: 'p1', season_type: 'postseason', week: 1 },
] };

test('as of week N the ballot shown is the one released in week N+1', () => {
  // A poll released in week N is voted on the games through week N-1, so the
  // ballot that answers "after week 1" is the week-2 release. Measured on the
  // 2026 AP releases: movement agrees with the previous week's result 88% of
  // the time and with the same week's 52%.
  assert.equal(pollCtx.pollReleaseIndexForStage(TRACK, { key: 'w1', week: 1 }), 1);
  assert.equal(pollCtx.pollReleaseIndexForStage(TRACK, { key: 'w2', week: 2 }), 2);
});

test('the preseason stage shows the preseason ballot, not a later one', () => {
  assert.equal(pollCtx.pollReleaseIndexForStage(TRACK, { key: 'pre', week: null }), 0);
});

test('the postseason stage shows the final ballot whatever week it is numbered', () => {
  assert.equal(pollCtx.pollReleaseIndexForStage(TRACK, { key: 'post', week: null }), 3);
  // No postseason release: the last one stands rather than nothing at all.
  const regularOnly = { releases: TRACK.releases.slice(0, 3) };
  assert.equal(pollCtx.pollReleaseIndexForStage(regularOnly, { key: 'post', week: null }), 2);
});

test('a week whose ballot has not been released yet shows no board', () => {
  // Week 3 would want the week-4 release, which this season has not reached.
  assert.equal(pollCtx.pollReleaseIndexForStage(TRACK, { key: 'w3', week: 3 }), -1);
  assert.equal(pollCtx.pollReleaseIndexForStage(null, { key: 'w1', week: 1 }), -1);
  assert.equal(pollCtx.pollReleaseIndexForStage({ releases: [] }, { key: 'w1', week: 1 }), -1);
});

// ---- conference Elo as of a week -------------------------------------------

const CONF_TIMELINE = {
  season: 2026,
  teams: [
    { id: 1, name: 'A', conference: 'Big Ten' },
    { id: 2, name: 'B', conference: 'Big Ten' },
    { id: 3, name: 'C', conference: 'SEC' },
    { id: 4, name: 'D', conference: 'FBS Independents' },
    { id: 5, name: 'E', conference: null },
  ],
  stages: [{ key: 'pre' }, { key: 'w1' }],
  rows: [
    [0, 1, 2, 3, 4].map(i => ({ team: i, elo: 1500 + i * 10, rank: i + 1, played: 0 })),
    [{ team: 0, elo: 1600, rank: 1, played: 1 }, { team: 1, elo: 1400, rank: 4, played: 1 },
     { team: 2, elo: 1550, rank: 2, played: 1 }, { team: 3, elo: 1500, rank: 3, played: 1 },
     { team: 4, elo: 1450, rank: 5, played: 0 }],
  ],
};

test('conference Elo averages that season members, excluding independents', () => {
  const rows = pollCtx.conferenceEloAsOf(CONF_TIMELINE, 1);
  assert.deepEqual(Array.from(rows.map(r => r.conference)), ['SEC', 'Big Ten']);
  assert.equal(rows.find(r => r.conference === 'Big Ten').elo, 1500);   // (1600 + 1400) / 2
  // An independent is not a conference, and a team with no membership row for
  // the season cannot be put in one.
  assert.ok(!rows.some(r => r.conference === 'FBS Independents' || r.conference == null));
});

test('a team that has not played is left out of the average but still a member', () => {
  const timeline = JSON.parse(JSON.stringify(CONF_TIMELINE));
  timeline.teams[1].conference = 'SEC';
  timeline.rows[1][1].played = 0;                 // B has not played
  const sec = pollCtx.conferenceEloAsOf(timeline, 1).find(r => r.conference === 'SEC');
  assert.equal(sec.rated, 1);
  assert.equal(sec.members, 2);
  assert.equal(sec.elo, 1550, 'the unplayed team must not drag the average');
});

test('preseason averages the ratings carried in, rather than emptying the table', () => {
  // Nobody has played at the preseason stage, so the rule that excludes an
  // unplayed team would leave no conference with a number at all.
  const rows = pollCtx.conferenceEloAsOf(CONF_TIMELINE, 0);
  assert.ok(rows.length, 'the preseason table must not be empty');
  assert.equal(rows.find(r => r.conference === 'Big Ten').elo, 1505);   // (1500 + 1510) / 2
});


// ---- a tie, which is where the shortcuts break -----------------------------

test('a tie counts half a win, and half an expectation beaten', () => {
  // Built by hand because the shortcut this guards against -- reading the
  // result off the sign of the Elo change -- is right for every win and every
  // loss, and wrong only here.
  const tp = {
    gamesById: new Map([[1, { id: 1, season: 2030, week: 1, phase: 0, home: 7, away: 8, hs: 21, as: 21 }]]),
    eloByTeam: new Map([[7, [{ gid: 1, tid: 7, pre: 1500, oppPre: 1600, exp: 0.3, chg: 8, post: 1508 }]]]),
    nonFbsByTeam: new Map(),
  };
  const played = { rated: tp.eloByTeam.get(7), unrated: [] };
  assert.deepEqual(Array.from([ctx.eloAsOfRecord(tp, 7, played).text]), ['0\u20130\u20131']);
  const strength = ctx.eloAsOfStrength(tp, 7, played.rated);
  assert.equal(strength.sos, 1600);
  // 0.5 - 0.3, not 1 - 0.3 (the Elo change is positive, so a sign-reading
  // shortcut would score this a win) and not 0 - 0.3.
  assert.ok(Math.abs(strength.aboveExp - 0.2) < 1e-9, `got ${strength.aboveExp}`);
});

test('a loss where the team still gained rating is a loss', () => {
  // A heavy underdog can lose and gain Elo under a margin-aware update, which
  // is the other half of why the result comes from the score.
  const tp = {
    gamesById: new Map([[1, { id: 1, season: 2030, week: 1, phase: 0, home: 7, away: 8, hs: 10, as: 13 }]]),
    eloByTeam: new Map([[7, [{ gid: 1, tid: 7, pre: 1300, oppPre: 1800, exp: 0.05, chg: 3, post: 1303 }]]]),
    nonFbsByTeam: new Map(),
  };
  const played = { rated: tp.eloByTeam.get(7), unrated: [] };
  assert.equal(ctx.eloAsOfRecord(tp, 7, played).text, '0\u20131');
  assert.ok(Math.abs(ctx.eloAsOfStrength(tp, 7, played.rated).aboveExp + 0.05) < 1e-9);
});
