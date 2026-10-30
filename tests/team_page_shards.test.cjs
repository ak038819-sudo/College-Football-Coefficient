// What a team shard has to carry for the page's panels to find anything.
//
// The shards are indexed by the shell's own indexTeamPages, against the
// committed export rather than a fixture, because the bug this file exists
// for was invisible to a fixture: the splitter wrote a team's swings,
// history and advanced statistics into the shard but never said WHOSE shard
// it was, so the indexer re-keyed all three onto `undefined` and the History
// panel, both Swing panels and the advanced block drew empty with the data
// sitting in the file. Every number there is present; nothing is reachable.
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

function shard(file, globalName, key) {
  const sandbox = { window: {} };
  vm.runInNewContext(fs.readFileSync(file, 'utf8'), sandbox);
  const bucket = sandbox.window[globalName];
  assert.ok(bucket, `${file} did not define ${globalName}`);
  return key == null ? bucket : bucket[key];
}

function index(raw, core) {
  const holder = { raw, core, result: null };
  vm.runInNewContext(sourceOf('indexTeamPages') + '\nresult = indexTeamPages(raw, core);', holder);
  return holder.result;
}

const CORE = shard('ui/data/team_pages/core.js', '__TEAM_PAGES_CORE__');
// Ohio State: a team with a long history, swings in both directions and
// advanced statistics, so an empty map here means broken, not absent.
const TEAM_ID = 71;
const TEAM = shard(`ui/data/team_pages/team/${TEAM_ID}.js`, '__TEAM_PAGES_TEAM__', String(TEAM_ID));

test('a team shard names the team it holds', () => {
  assert.equal(TEAM.team_id, TEAM_ID,
    'without team_id the indexer has nothing to re-key the team-only data onto');
});

test('the team-only panels find their data after indexing', () => {
  const tp = index(TEAM, CORE);
  // Each of these is read by a panel as map[team.id]; the panel draws an
  // empty state when the lookup misses, which is what a missing key gives.
  assert.ok(tp.swings[String(TEAM_ID)], 'the Swing panels would show no wins and no losses');
  assert.ok(tp.swings[String(TEAM_ID)].gains.length, 'the swing lists are empty');
  assert.ok(tp.history[String(TEAM_ID)], 'the History panel would report no games');
  assert.ok(tp.history[String(TEAM_ID)].seasons > 0, 'the history counts no seasons');
  assert.ok(tp.advanced, 'the advanced block lost its league-wide metadata');
  assert.ok(tp.advanced.teams[String(TEAM_ID)], 'the advanced block would say unavailable');
  // These two are already keyed "<team>|<season>" in the export, so they are
  // checked for presence rather than re-keying.
  assert.ok(Object.keys(tp.rollups).length, 'the merits ledger is empty');
  assert.ok(Object.keys(tp.nonFbsRecords).length, 'the unrated-opponent records are empty');
  assert.ok(tp.eloByTeam.get(TEAM_ID).length, 'the Elo chain is empty');
  assert.ok(tp.seasonsByTeam.get(TEAM_ID).size, 'the team has no seasons');
});

test('a season shard carries none of the team-only data, and says so', () => {
  const year = Math.max(...Object.keys(CORE.teams_per_season).map(Number));
  const raw = shard(`ui/data/team_pages/season/${year}.js`, '__TEAM_PAGES_SEASON__', String(year));
  assert.equal(raw.team_id, undefined, 'a season shard is not one team’s');
  const tp = index(raw, CORE);
  // No season view reads them, and inventing a key here would be worse than
  // an empty map: it would attribute one team's history to the whole season.
  // Key counts, not deepEqual: these objects are built inside the vm's own
  // realm, which deepStrictEqual refuses to compare against a host literal.
  assert.equal(Object.keys(tp.swings).length, 0);
  assert.equal(Object.keys(tp.history).length, 0);
  assert.equal(Object.keys(tp.advanced.teams).length, 0);
  // What a season view does read is there for every team in the season.
  assert.ok(tp.seasonsByTeam.size > 100, `only ${tp.seasonsByTeam.size} teams in ${year}`);
  assert.equal(tp.teamsPerSeason.get(year), CORE.teams_per_season[String(year)]);
});
