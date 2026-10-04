// The page's half of the shard contract: which file it asks for, and that it
// asks for one when it knows the game. A game page used to download a whole
// season of box scores -- 1.6 MB gzipped for 2025 -- to show one game.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const shell = fs.readFileSync('ui/dashboard_shell.html', 'utf8');

function load(manifest) {
  const names = ['boxScoreShard', 'playerArchiveEntry', 'loadPlayerArchive'];
  const source = names.map(name =>
    shell.match(new RegExp('function ' + name + '\\([^]*?\\n\\}'))[0]).join('\n');
  const asked = [];
  const context = {
    STATIC_MANIFEST: manifest,
    window: {__CFB_PLAYERS__: {2025: {'401838053': ['a box score']}}},
    loadDataScript: src => { asked.push(src); return Promise.resolve(); },
  };
  vm.runInNewContext(source + '\nthis.__fns = {' + names.join(',') + '};', context);
  return {fns: context.__fns, asked};
}

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
  assert.deepEqual(archive['401838053'], ['a box score']);
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
