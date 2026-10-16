// The game page's scoring summary and drive list.
//
// The one claim worth testing hard is who scored. CFBD says which team had the
// ball, not which team put points on the board, so a pick six or a safety is a
// drive where the DEFENCE scored -- and crediting the offence would name the
// wrong team in the most-read table on the page.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const shell = fs.readFileSync('ui/dashboard_shell.html', 'utf8');
function sourceOf(name) {
  const source = shell.match(new RegExp(`function ${name}\\([^)]*\\) \\{[\\s\\S]*?\\n\\}`))?.[0];
  assert.ok(source, `missing ${name}`);
  return source;
}

// The real field order from the exporter, not a hand-written copy: if the two
// drift apart the page reads the wrong column and these tests should say so.
const FIELDS = fs.readFileSync('src/export_static_data.py', 'utf8')
  .match(/DRIVE_FIELDS = \[([\s\S]*?)\]/)[1]
  .match(/"[a-z_]+"/g).map(s => s.replaceAll('"', ''));

const ctx = {esc: v => String(v).replaceAll('&', '&amp;').replaceAll('<', '&lt;'),
  STATIC_MANIFEST: {drive_fields: FIELDS}};
vm.runInNewContext([sourceOf('driveObjects'), sourceOf('driveClock'),
  sourceOf('scoringDrives'), sourceOf('gameDrivesPanel'),
  'this.objects = driveObjects; this.clock = driveClock;',
  'this.scoring = scoringDrives; this.panel = gameDrivesPanel;'].join('\n'), ctx);

function drive(values) {
  const row = FIELDS.map(f => values[f] === undefined ? null : values[f]);
  return row;
}

test('a drive row becomes an object by the exporter own field order', () => {
  const [d] = ctx.objects([drive({number: 1, home_offense: 1, result: 'TD', plays: 8,
    yards: 75, start_period: 1, start_clock: 900, end_period: 1, end_clock: 690,
    home_score: 7, away_score: 0})]);
  assert.equal(d.number, 1);
  assert.equal(d.result, 'TD');
  assert.equal(d.plays, 8);
  assert.equal(d.home_score, 7);
  assert.equal(d.away_score, 0);
});

test('without the exported field order nothing is guessed', () => {
  const bare = {STATIC_MANIFEST: {}};
  vm.runInNewContext(sourceOf('driveObjects') + '\nthis.objects = driveObjects;', bare);
  assert.deepEqual(Array.from(bare.objects([drive({number: 1})])), []);
});

test('the clock reads as a quarter, an overtime period, or just the period', () => {
  assert.equal(ctx.clock(1, 900), 'Q1 15:00');
  assert.equal(ctx.clock(4, 7), 'Q4 0:07');
  assert.equal(ctx.clock(5, 0), 'OT 0:00');
  assert.equal(ctx.clock(6, 120), 'OT2 2:00');
  // A drive CFBD timed but did not clock still says which quarter it was in.
  assert.equal(ctx.clock(2, null), 'Q2');
  assert.equal(ctx.clock(null, 300), '');
});

test('a scoring drive is one the SCOREBOARD moved across, whoever had the ball', () => {
  const rows = ctx.objects([
    drive({number: 1, home_offense: 1, result: 'TD', home_score: 7, away_score: 0}),
    drive({number: 2, home_offense: 0, result: 'PUNT', home_score: 7, away_score: 0}),
    // The away team had the ball and the HOME score moved: a pick six.
    drive({number: 3, home_offense: 0, result: 'INT TD', home_score: 14, away_score: 0}),
    drive({number: 4, home_offense: 0, result: 'FG', home_score: 14, away_score: 3}),
  ]);
  const scoring = ctx.scoring(rows);
  assert.deepEqual(Array.from(scoring).map(d => d.number), [1, 3, 4]);
  assert.deepEqual(Array.from(scoring).map(d => [d.home_points, d.away_points]),
    [[7, 0], [7, 0], [0, 3]]);
});

test('a drive with no recorded score is skipped rather than read as 0-0', () => {
  const rows = ctx.objects([
    drive({number: 1, home_offense: 1, result: 'TD', home_score: 7, away_score: 0}),
    drive({number: 2, home_offense: 0, result: 'PUNT'}),
    drive({number: 3, home_offense: 1, result: 'FG', home_score: 10, away_score: 0}),
  ]);
  // Without the guard the blank row reads as 0-0, which would invent a 7-point
  // swing back to level and then another one away from it.
  assert.deepEqual(Array.from(ctx.scoring(rows)).map(d => d.number), [1, 3]);
});

test('the panel names the scoring team, lists every drive, and is empty for none', () => {
  const rows = ctx.objects([
    drive({number: 1, home_offense: 1, result: 'TD', plays: 8, yards: 75,
      start_period: 1, start_clock: 900, end_period: 1, end_clock: 690,
      home_score: 7, away_score: 0}),
    drive({number: 2, home_offense: 0, result: 'INT TD', plays: 3, yards: 12,
      start_period: 1, start_clock: 690, end_period: 1, end_clock: 600,
      home_score: 14, away_score: 0}),
  ]);
  const html = ctx.panel(rows, 'Visitors', 'Hosts');
  assert.match(html, /id="gp-drives"/);
  // Both scoring drives are the home team's points, including the one the
  // visitors' offence was on the field for.
  assert.equal((html.match(/<td>Hosts<\/td>/g) || []).length, 3);
  assert.match(html, /All 2 drives/);
  assert.match(html, /Q1 11:30/);
  // The drive list names the team that HAD THE BALL, which on drive 2 is the
  // visitors even though the points went to the hosts.
  const list = html.split('<details')[1];
  assert.deepEqual(Array.from(list.matchAll(/<td>(Hosts|Visitors)<\/td>/g)).map(m => m[1]),
    ['Hosts', 'Visitors']);
  assert.equal(ctx.panel([], 'Visitors', 'Hosts'), '');
});

test('a game where nobody scored says so instead of showing an empty table', () => {
  const rows = ctx.objects([drive({number: 1, home_offense: 1, result: 'PUNT',
    start_period: 1, start_clock: 900, home_score: 0, away_score: 0})]);
  const html = ctx.panel(rows, 'Visitors', 'Hosts');
  assert.match(html, /No points were scored/);
  assert.match(html, /All 1 drives/);
});
