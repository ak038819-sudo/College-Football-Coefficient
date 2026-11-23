/* The team-stats panel on a game page.
 *
 * The values arrive as the provider's own strings, so every rule about what
 * one means -- which are fractions, which are clocks, which belong where --
 * lives in two places at once: src/team_box_score.py for anything built on
 * the server, and these functions for the page. The last block here holds
 * the two spellings to each other, because a silent disagreement between
 * them is the defect neither suite would catch alone.
 *
 * The fetch and load side is tests/test_game_team_stats.py.
 */
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const { execFileSync } = require('node:child_process');

const shell = fs.readFileSync('ui/dashboard_shell.html', 'utf8');

for (const name of ['teamBoxEntry', 'loadTeamBox', 'teamStatLabel',
  'teamStatPercent', 'teamBoxRows', 'gameTeamStatsPanel']) {
  // Two declarations of one name silently replace each other here, and a test
  // that lifts BY NAME never notices which one it got.
  const found = shell.match(new RegExp('function ' + name + '\\(', 'g')) || [];
  assert.equal(found.length, 1, name + ' must be declared exactly once in the shell');
}

const lift = name => shell.match(new RegExp('function ' + name + '\\([^]*?\\n\\}'))[0];
const constant = name => {
  const found = shell.match(new RegExp('const ' + name + ' = \\[[^]*?\\];'));
  assert.ok(found, name + ' must be declared in the shell');
  return found[0];
};

const context = {
  esc: v => String(v).replaceAll('&', '&amp;').replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;').replaceAll('"', '&quot;')
};
vm.runInNewContext([
  constant('TEAM_STAT_ORDER'), constant('TEAM_STAT_SUPPRESSED'),
  constant('TEAM_STAT_NOT_A_FRACTION'),
  lift('teamStatLabel'), lift('teamStatPercent'), lift('teamBoxRows'),
  lift('gameTeamStatsPanel'),
  'this.label = teamStatLabel; this.pct = teamStatPercent;',
  'this.rows = teamBoxRows; this.panel = gameTeamStatsPanel;',
  'this.ORDER = TEAM_STAT_ORDER;'
].join('\n'), context);
const { label, pct, rows, panel, ORDER } = context;
const text = html => html.replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();

/* ---- reading one value ------------------------------------------------- */

test('a conversion out of no attempts has no percentage', () => {
  // 0 of 0 is not a 0% conversion rate. Nobody failed on fourth down if
  // nobody went for it.
  assert.equal(pct('fourthDownEff', '0-0'), null);
  assert.equal(pct('thirdDownEff', '6-14'), 42.9);
});

test('penalties are not read as made-of-attempted', () => {
  // "5-45" is five penalties for forty-five yards.
  assert.equal(pct('totalPenaltiesYards', '5-45'), null);
});

test('a clock is not mistaken for a fraction', () => {
  assert.equal(pct('possessionTime', '31:45'), null);
});

test('a missing value is not coerced into a number', () => {
  // Number(null) is 0, so an absent value checked the wrong way reads as a
  // measured zero. This repository has hit that three times.
  assert.equal(pct('thirdDownEff', null), null);
  assert.equal(pct('thirdDownEff', undefined), null);
  assert.equal(pct('thirdDownEff', ''), null);
});

/* ---- which rows, in which order ---------------------------------------- */

test('the rows come out in box-score order, not the provider’s', () => {
  const out = rows({ away: { totalYards: '417', firstDowns: '24' },
                     home: { totalYards: '388', firstDowns: '19' } });
  // Array.from: a vm-context array is not deepEqual to a host array.
  assert.deepEqual(Array.from(out.map(r => r.category)), ['firstDowns', 'totalYards']);
});

test('an unfamiliar category is shown, after the known ones', () => {
  // CFBD adds categories season by season. Hiding one is how a measurement
  // disappears without anybody deciding it should.
  const out = rows({ away: { zzNew: '1', totalYards: '417' }, home: {} });
  assert.deepEqual(Array.from(out.map(r => r.category)), ['totalYards', 'zzNew']);
});

test('a category only one side reported still appears', () => {
  const out = rows({ away: { sacks: '3' }, home: {} });
  assert.equal(out.length, 1);
  assert.equal(out[0].away, '3');
  assert.equal(out[0].home, '');
});

test('a category neither side reported is not a row of dashes', () => {
  assert.deepEqual(Array.from(rows({ away: { sacks: '' }, home: { sacks: '' } })), []);
});

test('what the scoreboard already says is not repeated here', () => {
  assert.deepEqual(Array.from(rows({ away: { passingTDs: '3' }, home: { rushingTDs: '2' } })), []);
});

test('no sides at all is an empty list, not a crash', () => {
  assert.deepEqual(Array.from(rows(null)), []);
  assert.deepEqual(Array.from(rows({})), []);
});

/* ---- the panel ---------------------------------------------------------- */

test('a game with nothing recorded draws no panel at all', () => {
  // An empty section would read as a game in which neither team gained a
  // yard, which is the shape of a real answer.
  assert.equal(panel(null, 'Auburn', 'Alabama'), '');
  assert.equal(panel({ away: {}, home: {} }, 'Auburn', 'Alabama'), '');
});

test('the away team is the first column, as on the scoreboard', () => {
  const html = panel({ away: { totalYards: '417' }, home: { totalYards: '388' } },
                     'Auburn', 'Alabama');
  assert.ok(html.indexOf('Auburn') < html.indexOf('Alabama'),
    'the columns must not be swapped relative to the hero');
  assert.match(text(html), /Total yards 417 388/);
});

test('a side with no value gets a dash, never a zero', () => {
  const html = panel({ away: { sacks: '3' }, home: {} }, 'Auburn', 'Alabama');
  assert.match(text(html), /Sacks 3 &mdash;/);
  assert.doesNotMatch(text(html), /Sacks 3 0/);
});

test('a conversion carries its rate beside the raw figure', () => {
  const html = panel({ away: { thirdDownEff: '6-14' }, home: { thirdDownEff: '0-0' } },
                     'Auburn', 'Alabama');
  assert.match(text(html), /Third down 6-14 42\.9% 0-0/);
  assert.doesNotMatch(text(html), /0-0 0\.0%/, 'no attempts, no rate');
});

test('a team name is escaped, not injected', () => {
  const html = panel({ away: { sacks: '3' }, home: {} }, '<script>x</script>', 'A');
  assert.doesNotMatch(html, /<script>/);
});

/* ---- the manifest gate --------------------------------------------------- */

test('a season with no team box score asks for no file', async () => {
  const ctx = { STATIC_MANIFEST: { team_box: {} },
                boxScoreShard: (id, n) => Number(id) % n,
                loadDataScript: () => { throw new Error('must not fetch'); },
                Promise, window: {} };
  vm.runInNewContext(lift('teamBoxEntry') + '\n' + lift('loadTeamBox') +
    '\nthis.entry = teamBoxEntry; this.load = loadTeamBox;', ctx);
  assert.equal(ctx.entry(2024), null);
  assert.equal(await ctx.load(2024, 500), null);
});

test('a season whose shard was never written asks for no file', async () => {
  // The exporter writes only the shards that have games, and the entry names
  // only the files that exist. Asking for a missing one is a 404 the reader
  // sees as a broken page.
  const ctx = { STATIC_MANIFEST: { team_box: { 2024: { shards: 32, files: { 4: 'a.js' } } } },
                boxScoreShard: (id, n) => Number(id) % n,
                loadDataScript: () => { throw new Error('must not fetch'); },
                Promise, window: {} };
  vm.runInNewContext(lift('teamBoxEntry') + '\n' + lift('loadTeamBox') +
    '\nthis.load = loadTeamBox;', ctx);
  assert.equal(await ctx.load(2024, 500), null);   // 500 % 32 === 20, not 4
});

/* ---- the page and the exporter must agree -------------------------------- */

test('the page and src/team_box_score.py label and rank identically', () => {
  // Two implementations of one rule drift, and the drift is invisible: the
  // page would show a category the export considers suppressed, or spell a
  // label one way on a game page and another in a summary.
  const cases = ['firstDowns', 'thirdDownEff', 'totalPenaltiesYards',
                 'qbHurriesAllowed', 'some_new_metric', 'ESPNRating', '', 'x',
                 // Pluralised abbreviations: both sides must keep the capitals.
                 'defensiveTDs', 'kickReturnTDs', 'TDs', 'totalFumbles'];
  const out = execFileSync('python3', ['-c', `
import json, sys
sys.path.insert(0, 'src')
import team_box_score as box
cases = json.loads(sys.argv[1])
print(json.dumps({
    "labels": [box.label_for(c) for c in cases],
    "order": [c for c, _ in box.DISPLAY_ORDER],
    "suppressed": sorted(box.SUPPRESSED),
    "pct": [box.percent("thirdDownEff", "6-14"),
            box.percent("fourthDownEff", "0-0"),
            box.percent("totalPenaltiesYards", "5-45")]}))
`, JSON.stringify(cases)], { encoding: 'utf8' });
  const python = JSON.parse(out);
  assert.deepEqual(cases.map(label), python.labels);
  assert.deepEqual(Array.from(ORDER).map(pair => pair[0]), python.order);
  assert.deepEqual(python.suppressed,
    ['kickingPoints', 'passingTDs', 'rushingTDs']);
  assert.deepEqual([pct('thirdDownEff', '6-14'), pct('fourthDownEff', '0-0'),
                    pct('totalPenaltiesYards', '5-45')], python.pct);
});
