// The page's half of the shard contract: which file it asks for, and that it
// asks for one when it knows the game. A game page used to download a whole
// season of box scores -- 1.6 MB gzipped for 2025 -- to show one game.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const shell = fs.readFileSync('ui/dashboard_shell.html', 'utf8');

// An encoded shard, written the way src/export_static_data.py writes one: a
// line is [nameIndex, stat, idIndex] against the shard's own string tables.
// Built here rather than stubbed, because the decoder is what is under test.
function encoded(season, gameId, team, lines) {
  const names = [...new Set(lines.map(l => l[0]))];
  const ids = [...new Set(lines.map(l => l[2]))];
  return {
    v: 2, s: season, n: names, i: ids, c: ['defensive'], t: ['TOT'],
    g: {[gameId]: [[team, 'away', [[0, 0, lines.map(
      l => [names.indexOf(l[0]), l[1], ids.indexOf(l[2])])]]]]},
  };
}

const LINES = [['Marlin Dean', '7', '5220652'], ['Brent Washington', '4', '5195994']];

function load(manifest) {
  const names = ['boxScoreShard', 'playerArchiveEntry', 'decodeBoxScores',
                 'drainBoxScoreQueue', 'loadPlayerArchive'];
  const source = names.map(name =>
    shell.match(new RegExp('function ' + name + '\\([^]*?\\n\\}'))[0]).join('\n');
  const asked = [];
  // The global starts ABSENT, because that is how a real page starts: a data
  // file creates it. A legacy file ASSIGNS it and a v2 shard PUSHES onto the
  // queue, and the loader has to survive both -- pre-seeding it here once hid
  // a bug where the legacy branch read a detached object the script had
  // already replaced.
  const context = {
    STATIC_MANIFEST: manifest,
    window: {},
    loadDataScript: src => {
      asked.push(src);
      return Promise.resolve().then(() => {
        if (src.startsWith('data/players/2025.js')) {
          context.window.__CFB_PLAYERS__ = {2025: {'401838053': ['a box score']}};
        } else if (/\/5\.js/.test(src)) {
          const queue = context.window.__CFB_BOX_QUEUE__ ||= [];
          queue.push(encoded(2025, '401838053', 'Georgia State', LINES));
        }
      });
    },
  };
  vm.runInNewContext(source + '\nthis.__fns = {' + names.join(',') + '};', context);
  return {fns: context.__fns, asked, context};
}

// What the two shard tests below expect to read back out of the loader.
const DECODED = [{
  name: 'Georgia State', home_away: 'away',
  categories: [{name: 'defensive', type: 'TOT', lines: [
    {name: 'Marlin Dean', stat: '7', id: '5220652'},
    {name: 'Brent Washington', stat: '4', id: '5195994'},
  ]}],
}];

const sharded = {players: {2025: {shards: 32, files: Object.fromEntries(
  Array.from({length: 32}, (_, n) => [String(n), 'data/players/2025/' + n + '.js?v=abc'])) }}};

test('a shard is chosen by a floored modulo, matching the exporter', () => {
  const {fns} = load(sharded);
  assert.equal(fns.boxScoreShard(401838053, 32), 401838053 % 32);
  assert.equal(fns.boxScoreShard(401838053, 32), 5);
  // Python's -7 % 32 is 25; JavaScript's bare % says -7, which is a file that
  // was never written.
  assert.equal(fns.boxScoreShard(-7, 32), 25);
  assert.equal(fns.boxScoreShard(-32, 32), 0);
});

test('a game page asks for exactly one shard', async () => {
  const {fns, asked} = load(sharded);
  const archive = await fns.loadPlayerArchive(2025, 401838053);
  assert.deepEqual(asked, ['data/players/2025/5.js?v=abc']);
  // Decoded on the way out, so every panel still reads the objects it always
  // did and nothing downstream knows the file was encoded.
  assert.deepEqual(JSON.parse(JSON.stringify(archive['401838053'])), DECODED);
});

test('the Stats view, which aggregates a season, asks for all of them', async () => {
  const {fns, asked} = load(sharded);
  await fns.loadPlayerArchive(2025);
  assert.equal(asked.length, 32);
});

test('a manifest from before the sharding still loads its one file', async () => {
  // A page is published with its manifest, but a cached page paired with a new
  // manifest -- or the reverse -- must not lose the box score entirely.
  const {fns, asked} = load({players: {2025: 'data/players/2025.js?v=old'}});
  const archive = await fns.loadPlayerArchive(2025, 401838053);
  assert.deepEqual(asked, ['data/players/2025.js?v=old']);
  assert.deepEqual(archive['401838053'], ['a box score']);
});

test('a season with no box scores asks for nothing and renders empty', async () => {
  const {fns, asked} = load({players: {}});
  assert.deepEqual(Object.keys(await fns.loadPlayerArchive(1996, 123)), []);
  assert.deepEqual(asked, []);
});

// The encoding. The published site is capped at 1 GB and these box scores were
// 245 MB of it, so a shard carries string tables and positional rows and the
// page puts the objects back. src/export_static_data.py holds its own encoder
// to the same round trip; this is the half that actually runs in a browser.
test('an encoded shard is decoded into the objects the panels render', () => {
  const {fns} = load(sharded);
  const games = fns.decodeBoxScores(encoded(2025, '401838053', 'Georgia State', LINES));
  assert.deepEqual(JSON.parse(JSON.stringify(games['401838053'])), DECODED);
});

test('two shards of one season both survive the drain', async () => {
  // The reason a shard queues itself instead of merging: an encoded payload
  // and a decoded season cannot share one object, so the merge happens here.
  const {fns, context} = load(sharded);
  context.window.__CFB_BOX_QUEUE__ = [
    encoded(2025, '401838053', 'Georgia State', LINES),
    encoded(2025, '401838054', 'Troy', LINES),
  ];
  fns.drainBoxScoreQueue();
  assert.deepEqual(Object.keys(context.window.__CFB_PLAYERS__[2025]).sort(),
                   ['401838053', '401838054']);
  assert.deepEqual(context.window.__CFB_BOX_QUEUE__, [], 'the queue is emptied');
});

test('a drain keeps what an earlier drain decoded', () => {
  const {fns, context} = load(sharded);
  context.window.__CFB_BOX_QUEUE__ = [encoded(2025, '401838053', 'Georgia State', LINES)];
  fns.drainBoxScoreQueue();
  context.window.__CFB_BOX_QUEUE__.push(encoded(2025, '401838054', 'Troy', LINES));
  fns.drainBoxScoreQueue();
  assert.deepEqual(Object.keys(context.window.__CFB_PLAYERS__[2025]).sort(),
                   ['401838053', '401838054']);
});

test('shards of different seasons land in their own seasons', () => {
  const {fns, context} = load(sharded);
  context.window.__CFB_BOX_QUEUE__ = [
    encoded(2025, '401838053', 'Georgia State', LINES),
    encoded(2024, '401520281', 'Troy', LINES),
  ];
  fns.drainBoxScoreQueue();
  assert.deepEqual(Object.keys(context.window.__CFB_PLAYERS__[2025]), ['401838053']);
  assert.deepEqual(Object.keys(context.window.__CFB_PLAYERS__[2024]), ['401520281']);
});

test('an index the tables do not reach costs that value, not the box score', () => {
  const {fns} = load(sharded);
  const payload = encoded(2025, '401838053', 'Georgia State', LINES);
  payload.n = [];
  const line = fns.decodeBoxScores(payload)['401838053'][0].categories[0].lines[0];
  assert.equal(line.name, '', 'an unreachable name reads empty');
  assert.equal(line.stat, '7', 'and the statistic beside it still renders');
});

test('a drain with nothing queued leaves the archive alone', () => {
  const {fns, context} = load(sharded);
  fns.drainBoxScoreQueue();
  assert.equal(context.window.__CFB_PLAYERS__, undefined,
               'an untouched page should not grow an empty archive');
});

// What the panel leaves out, and what it must not. CFBD publishes a row per
// rostered player per statistic, so 38.9% of every line in the archive reads
// zero. The filter lives HERE, at render, because the Stats hub reads the same
// shards and aggregatePlayers counts a player's games from the lines that
// exist -- stripping them in the exporter moved 312 of 2025's player-category
// game counts. These render the REAL panel rather than grepping the shell,
// because a test that reads source text passes while the code is broken.
function panel() {
  const consts = ['STAT_CATEGORY_ORDER', 'STAT_TYPE_ORDER'];
  const fns = ['esc', 'statGroupRank', 'orderStatGroups', 'zeroBoxScoreStat',
               'recordedBoxScoreTeams', 'boxScoreNameLookup', 'boxScorePlayerCell',
               'boxScoreLineCell', 'rosterIdSet', 'playerBoxscorePanel'];
  const source = consts.map(name => {
    const m = shell.match(new RegExp('^const ' + name + ' = [^]*?;$', 'm'));
    assert.ok(m, 'the shell must declare ' + name);
    return m[0];
  }).concat(fns.map(name => {
    const m = shell.match(new RegExp('^function ' + name + '\\([^]*?\\n\\}', 'm'));
    assert.ok(m, 'the shell must declare ' + name);
    return m[0];
  })).join('\n');
  const context = {TEAM_BY_NAME: new Map()};
  vm.runInNewContext(source + '\nthis.__x = {playerBoxscorePanel, zeroBoxScoreStat, recordedBoxScoreTeams};',
                     context);
  return context.__x;
}

function team(name, ...cats) {
  return {name, home_away: 'away', categories: cats.map(([cat, type, lines]) => ({
    name: cat, type, lines: lines.map(([who, stat]) => ({name: who, stat, id: '1'})),
  }))};
}

test('a line the player recorded nothing in is not drawn', () => {
  const {playerBoxscorePanel} = panel();
  const html = playerBoxscorePanel([team('Georgia State',
    ['defensive', 'TOT', [['Marlin Dean', '7'], ['Brent Washington', '0']]],
    ['defensive', 'TD', [['Marlin Dean', '0'], ['Brent Washington', '0']]])],
    null, null, null);
  assert.match(html, /Marlin Dean/);
  assert.doesNotMatch(html, /Brent Washington/, 'he recorded nothing at all');
  // The whole TD group went: every line in it was zero. An empty container
  // draws as a heading with nothing under it.
  assert.doesNotMatch(html, /TD/);
  assert.match(html, /TOT/);
});

test('what counts as zero is every digit being zero', () => {
  const {zeroBoxScoreStat} = panel();
  for (const zero of ['0', '0.0', '0/0', '0-0', '0:00', '00', '--', '-', '', '  ', null]) {
    assert.equal(zeroBoxScoreStat(zero), true, JSON.stringify(zero));
  }
  // The ones that are NOT zero matter more: a five-yard loss, a missed kick
  // that was a real attempt, and half a sack.
  for (const kept of ['-5', '0/1', '0-7', '10', '0.5', '1', 'DNP', '1/0']) {
    assert.equal(zeroBoxScoreStat(kept), false, JSON.stringify(kept));
  }
});

test('a team that recorded nothing is left out', () => {
  const {recordedBoxScoreTeams} = panel();
  const kept = recordedBoxScoreTeams([
    team('Georgia State', ['defensive', 'TD', [['A', '0']]]),
    team('Troy', ['passing', 'YDS', [['B', '23']]]),
  ]);
  assert.deepEqual(kept.map(t => t.name), ['Troy']);
});

test('the filter does not mutate the archive the Stats hub reads', () => {
  // loadPlayerArchive hands out the cached season object, so a filter that
  // edited it in place would strip the aggregator's input on the way past.
  const {recordedBoxScoreTeams} = panel();
  const source = [team('Georgia State', ['defensive', 'TD', [['A', '0'], ['B', '3']]])];
  recordedBoxScoreTeams(source);
  assert.equal(source[0].categories[0].lines.length, 2, 'both lines still there');
  assert.equal(source[0].categories[0].lines[0].stat, '0');
});

test('the box score states that it lists only what a player recorded', () => {
  const {playerBoxscorePanel} = panel();
  const html = playerBoxscorePanel(
    [team('Georgia State', ['defensive', 'TOT', [['Marlin Dean', '7']]])], null, null, null);
  assert.match(html, /Only statistics a player recorded are listed/);
  // Drawn whatever the links did: the note used to exist solely to explain the
  // linking rules, and with no line linked there was no note at all.
  assert.match(html, /class="section-note"/);
  assert.match(html, /Marlin Dean/, 'and the box score itself still renders');
});

test('a game CFBD has not published still says so', () => {
  // Keyed on the unfiltered input, so this stays a different statement from
  // "it published and every line was zero".
  const {playerBoxscorePanel} = panel();
  assert.match(playerBoxscorePanel([], null, null, null),
               /Player lines appear here when CFBD publishes/);
  assert.match(playerBoxscorePanel(
    [team('Georgia State', ['defensive', 'TD', [['A', '0']]])], null, null, null),
    /No player recorded a statistic in this game/);
});
