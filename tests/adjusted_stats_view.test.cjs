// The Opponent-adjusted Stats view.
//
// The fit itself is tested in tests/test_adjusted_stats.py. What can go wrong
// here is presentation, and this table has its own ways to mislead:
//
//   * a defense column read as "higher is better", which would invert the one
//     number readers most want to compare;
//   * the rank-movement triangle pointing the wrong way, which would say a
//     schedule flattered a team when it punished it;
//   * the two "flattered" lists holding the punished teams, which is the same
//     error one level up and the part of the page a reader trusts most;
//   * a cell failing to sort, which is PR #85's bug -- one unkeyed cell sends
//     the whole column to a text sort;
//   * the metric selector appearing on a season that has only one metric.
//
// Every helper is lifted from the shell for real rather than stubbed, because
// stubbing a helper is how #71 silently replaced #72's statCell().
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

// Three teams with the same raw scoring defense rank order as their adjusted
// one would NOT test anything, so these cross: Akron's raw defense is better
// than its adjusted one (it played bad offenses), Florida's is worse.
const SEASON = {
  season: 2025,
  metrics: [
    {key: 'points', label: 'Points per game', unit: 'points', digits: 1,
     mu: 26.168, home: 3.863, lambda: 2.0, rated: 136, observations: 1616},
    {key: 'ppa', label: 'EPA per play', unit: 'epa', digits: 3,
     mu: 0.10116, home: 0.05789, lambda: 200.0, rated: 136, observations: 1616},
  ],
  teams: [
    {id: 1, name: 'Indiana', conference: 'Big Ten', unrated: 1,
     points: {games: 12, off_raw: 39.5, off_adj: 42.7, def_raw: 12.5, def_adj: 11.4,
              off_raw_rank: 3, off_adj_rank: 1, def_raw_rank: 3, def_adj_rank: 2},
     ppa: {games: 12, off_raw: 0.41, off_adj: 0.44, def_raw: -0.12, def_adj: -0.15,
           off_raw_rank: 2, off_adj_rank: 1, def_raw_rank: 4, def_adj_rank: 2}},
    {id: 2, name: 'Akron', conference: 'Mid-American', unrated: 1,
     points: {games: 12, off_raw: 20.1, off_adj: 17.9, def_raw: 28.9, def_adj: 33.4,
              off_raw_rank: 110, off_adj_rank: 126, def_raw_rank: 89, def_adj_rank: 126},
     ppa: {games: 12, off_raw: -0.05, off_adj: -0.11, def_raw: 0.14, def_adj: 0.22,
           off_raw_rank: 115, off_adj_rank: 128, def_raw_rank: 90, def_adj_rank: 127}},
    // The raw table's best offense, which the adjustment demotes: it crosses
    // Indiana, so a table ordered by the raw rank renders in a different order
    // from one ordered by the adjusted rank.
    {id: 4, name: 'North Texas', conference: 'American Athletic', unrated: 1,
     points: {games: 12, off_raw: 44.6, off_adj: 41.9, def_raw: 28.5, def_adj: 29.9,
              off_raw_rank: 1, off_adj_rank: 2, def_raw_rank: 84, def_adj_rank: 97},
     ppa: {games: 12, off_raw: 0.46, off_adj: 0.42, def_raw: 0.13, def_adj: 0.16,
           off_raw_rank: 1, off_adj_rank: 2, def_raw_rank: 85, def_adj_rank: 98}},
    {id: 3, name: 'Florida', conference: 'SEC', unrated: 0,
     points: {games: 12, off_raw: 24.0, off_adj: 27.8, def_raw: 23.6, def_adj: 19.9,
              off_raw_rank: 86, off_adj_rank: 44, def_raw_rank: 70, def_adj_rank: 32},
     ppa: {games: 12, off_raw: 0.02, off_adj: 0.13, def_raw: 0.06, def_adj: -0.04,
           off_raw_rank: 90, off_adj_rank: 40, def_raw_rank: 72, def_adj_rank: 30}},
  ],
};

// Thirteen filler teams, so adjustedMoversHtml clears its ten-row floor. Their
// movement is zero, so they can never displace a real mover at either end.
function withFiller(data) {
  const filler = [];
  for (let i = 0; i < 13; i++) {
    filler.push({id: 100 + i, name: 'Filler ' + i, conference: 'Sun Belt', unrated: 0,
      points: {games: 12, off_raw: 26.2, off_adj: 26.2, def_raw: 26.2, def_adj: 26.2,
               off_raw_rank: 60 + i, off_adj_rank: 60 + i,
               def_raw_rank: 60 + i, def_adj_rank: 60 + i}});
  }
  return {...data, teams: [...data.teams, ...filler]};
}

function view(data, {manifest, metric} = {}) {
  const ctx = {
    STATIC_MANIFEST: {adjusted_stats: manifest === undefined
      ? {2025: 'data/adjusted_stats/2025.js?v=abc'} : manifest},
    state: {year: 2025, section: 'stats', subview: 'adjusted'},
    esc: s => String(s),
    teamLink: name => '<a class="t">' + name + '</a>',
    logoMarkup: () => '',
  };
  vm.runInNewContext(
    sourceOf('adjustedValue') + '\n' + sourceOf('adjustedMove') + '\n' +
    sourceOf('adjustedMoveNote') + '\n' + sourceOf('adjustedCell') + '\n' +
    sourceOf('adjustedMetricOf') + '\n' + sourceOf('adjustedMoversHtml') + '\n' +
    sourceOf('adjustedHelpHtml') + '\n' + sourceOf('adjustedStatsHtml') + '\n' +
    sourceOf('renderAdjustedStats') + '\n' +
    'var adjustedMetric = ' + JSON.stringify(metric || null) + ';' +
    'this.html = adjustedStatsHtml; this.render = renderAdjustedStats;' +
    'this.cell = adjustedCell; this.value = adjustedValue; this.move = adjustedMove;' +
    'this.movers = adjustedMoversHtml; this.metricOf = adjustedMetricOf;',
    ctx);
  return {html: data ? ctx.html(data) : null, ctx};
}

function rowOf(html, name) {
  // The movers lists name the same teams further down the page, so only the
  // table body is searched.
  const body = html.slice(html.indexOf('<tbody>'), html.indexOf('</tbody>'));
  const rows = body.split('<tr>').filter(r => r.includes('>' + name + '<'));
  assert.equal(rows.length, 1, 'expected one row for ' + name);
  return rows[0];
}
function cellsOf(row) {
  return [...row.matchAll(/<td class="num"[^>]*>([\s\S]*?)<\/td>/g)].map(m => m[1]);
}

test('the four value columns are offense raw, offense adjusted, defense raw, defense adjusted', () => {
  const {html} = view(withFiller(SEASON));
  const heads = [...html.matchAll(/<th class="num">([^<]*)</g)].map(m => m[1]);
  assert.deepEqual(heads, ['Offense', 'Offense adjusted', 'Defense', 'Defense adjusted']);
  const cells = cellsOf(rowOf(html, 'Indiana'));
  assert.equal(cells.length, 4);
  assert.match(cells[0], /^39\.5 /);
  assert.match(cells[1], /^42\.7 /);
  assert.match(cells[2], /^12\.5 /);
  assert.match(cells[3], /^11\.4 /);
});

test('each value carries its own rank, out of the metric\'s own team count', () => {
  const {html} = view(withFiller(SEASON));
  const cells = cellsOf(rowOf(html, 'Florida'));
  assert.match(cells[0], /#86/);
  assert.match(cells[1], /#44/);
  assert.match(cells[2], /#70/);
  assert.match(cells[3], /#32/);
  assert.match(html, /Ranks are out of 136 teams, 1 = best/);
});

test('the triangle points up when the adjustment improved the rank, down when it worsened it', () => {
  const {html} = view(withFiller(SEASON));
  // Florida's defense went from #70 to #32: the schedule had been hiding it.
  assert.match(cellsOf(rowOf(html, 'Florida'))[3], /#32 ▲ ?38|#32 ▲38/);
  // Akron's went from #89 to #126: the schedule had been flattering it.
  assert.match(cellsOf(rowOf(html, 'Akron'))[3], /#126 ▼ ?37|#126 ▼37/);
  // A rank that did not move carries no mark at all.
  const flat = cellsOf(rowOf(html, 'Filler 0'));
  assert.ok(!/[▲▼]/.test(flat.join(' ')), 'an unmoved rank should carry no triangle');
});

test('adjustedMove is raw minus adjusted, so a better adjusted rank is positive', () => {
  const {ctx} = view(null);
  assert.equal(ctx.move({def_raw_rank: 70, def_adj_rank: 32}, 'def'), 38);
  assert.equal(ctx.move({def_raw_rank: 89, def_adj_rank: 126}, 'def'), -37);
  assert.equal(ctx.move({off_raw_rank: 5, off_adj_rank: 5}, 'off'), 0);
  assert.equal(ctx.move({off_raw_rank: null, off_adj_rank: 5}, 'off'), null);
  assert.equal(ctx.move(null, 'off'), null);
});

test('the flattered lists hold the teams the adjustment marked DOWN', () => {
  const {html} = view(withFiller(SEASON));
  const sections = html.split('<h3 class="section-title">').slice(1);
  const titled = {};
  sections.forEach(s => { titled[s.slice(0, s.indexOf('<'))] = s; });
  // A flattered defense is one whose raw rank was BETTER than its adjusted one.
  assert.ok(titled['Defenses the schedule flattered'].includes('Akron'),
    'Akron (#89 raw, #126 adjusted) is the flattered defense');
  assert.ok(!titled['Defenses the schedule flattered'].includes('Florida'));
  assert.ok(titled['Defenses the schedule punished'].includes('Florida'),
    'Florida (#70 raw, #32 adjusted) is the punished defense');
  assert.ok(titled['Offenses the schedule flattered'].includes('Akron'));
  assert.ok(titled['Offenses the schedule punished'].includes('Florida'));
  // Each entry reads raw -> adjusted, in that order.
  assert.match(titled['Defenses the schedule flattered'], /Akron<\/a> <span[^>]*>#89 → #126/);
});

test('a list with too few teams to rank is left out rather than shown short', () => {
  const {ctx} = view(null);
  assert.equal(ctx.movers(SEASON, 'points'), '');
  assert.notEqual(ctx.movers(withFiller(SEASON), 'points'), '');
});

test('every value cell carries its own sort key, so one dash cannot break the column', () => {
  const {html} = view(withFiller(SEASON));
  const cells = html.match(/<td class="num" data-sort=|<td class="num">/g) || [];
  assert.equal(cells.filter(c => !c.includes('data-sort')).length, 0);
  // The key is the raw number, not the text with its rank and triangle beside it.
  assert.match(html, /data-sort="39\.5"/);
  assert.match(html, /data-sort="11\.4"/);
  assert.ok(!/data-sort="#/.test(html));
});

test('a team with no number on one side shows dashes there and sorts last', () => {
  // The per-game metrics are nullable, so a team can have a defense for a
  // metric and no offense. A null rank subtracts as a zero, which would put
  // such a team at the TOP of a table ordered by adjusted offense.
  const oneSided = {id: 5, name: 'AAA No Offense', conference: 'Sun Belt', unrated: 0,
    points: {games: 12, off_raw: null, off_adj: null, def_raw: 24.0, def_adj: 25.5,
             off_raw_rank: null, off_adj_rank: null, def_raw_rank: 50, def_adj_rank: 55}};
  const data = withFiller({...SEASON, teams: [...SEASON.teams, oneSided]});
  const {html} = view(data);
  const cells = cellsOf(rowOf(html, 'AAA No Offense'));
  assert.equal(cells[0], '\u2014');
  assert.equal(cells[1], '\u2014');
  assert.match(cells[3], /^25\.5 /);
  const names = [...html.matchAll(/<a class="t">([^<]*)<\/a>/g)].map(m => m[1]);
  // Last in the table despite a name that sorts first alphabetically.
  const body = html.slice(html.indexOf('<tbody>'), html.indexOf('</tbody>'));
  const inBody = [...body.matchAll(/<a class="t">([^<]*)<\/a>/g)].map(m => m[1]);
  assert.equal(inBody[inBody.length - 1], 'AAA No Offense');
  assert.ok(names.includes('Indiana'));
});

test('an absent value is a dash whose key sorts as absent, never as zero', () => {
  const {ctx} = view(null);
  assert.equal(ctx.cell(null, 4, 'points', 2), '<td class="num" data-sort="">—</td>');
  assert.ok(!ctx.cell(null, 4, 'points', 2).includes('data-sort="0"'));
});

test('each unit is written in its own notation', () => {
  const {ctx} = view(null);
  assert.equal(ctx.value(26.168, 'points'), '26.2');
  assert.equal(ctx.value(0.10116, 'epa'), '+0.101');
  assert.equal(ctx.value(-0.0412, 'epa'), '−0.041');
  assert.equal(ctx.value(0.41978, 'pct'), '42.0%');
  assert.equal(ctx.value(null, 'points'), '—');
  // Zero is a value, not an absence.
  assert.equal(ctx.value(0, 'points'), '0.0');
});

test('the note states the league average and the measured home-field effect', () => {
  const {html} = view(withFiller(SEASON));
  assert.match(html, /The average was 26\.2 and playing at home was worth 3\.9 of it/);
});

test('a season with one metric has no selector; a season with two does', () => {
  const one = view(withFiller({...SEASON, metrics: [SEASON.metrics[0]]})).html;
  assert.ok(!one.includes('id="adj-metric"'), 'one metric needs no selector');
  const two = view(withFiller(SEASON)).html;
  assert.match(two, /<select id="adj-metric">/);
  assert.match(two, /<option value="points" selected>Points per game/);
});

test('the chosen metric is the one rendered, and an unknown choice falls back', () => {
  const ppa = view(withFiller(SEASON), {metric: 'ppa'}).html;
  assert.match(ppa, /both in epa per play/);
  assert.match(ppa, /<option value="ppa" selected>/);
  assert.match(ppa, /data-sort="0\.44"/);
  const {ctx} = view(null, {metric: 'rushing-yards-nobody-exports'});
  assert.equal(ctx.metricOf(SEASON), 'points');
});

test('rows are ordered by adjusted offense, not by the raw number', () => {
  const {html} = view(withFiller(SEASON));
  const names = [...html.matchAll(/<a class="t">([^<]*)<\/a>/g)].map(m => m[1]);
  // Indiana (#1 adjusted) before North Texas (#2) before Florida (#44) before
  // Akron (#126) -- although by the RAW rank North Texas is first and Florida
  // is 86th, so this ordering can only come from the adjusted column.
  assert.ok(names.indexOf('Indiana') < names.indexOf('North Texas'),
    'Indiana is #1 adjusted but only #3 raw');
  assert.ok(names.indexOf('North Texas') < names.indexOf('Florida'));
  assert.ok(names.indexOf('Florida') < names.indexOf('Akron'));
});

test('a season the manifest does not carry says so instead of rendering empty', () => {
  const {ctx} = view(null, {manifest: {}});
  const html = ctx.render();
  assert.match(html, /too few rated games/);
  assert.ok(!html.includes('adj-body'), 'nothing to hydrate when there is no data');
  const present = view(null).ctx.render();
  assert.match(present, /id="adj-body"/);
});

test('a season whose metrics are all empty says so rather than drawing a table', () => {
  const {ctx} = view(null);
  const html = ctx.html({season: 2002, metrics: [], teams: []});
  assert.match(html, /No statistic has enough of 2002/);
  assert.ok(!html.includes('<table'));
});

test('the help text says what the numbers are not', () => {
  const {html} = view(withFiller(SEASON));
  assert.match(html, /Nothing here feeds Elo, Merits, the playoff model or any prediction/);
  assert.match(html, /Games against opponents this project does not rate are excluded/);
});

test('no cell renders as NaN or undefined', () => {
  const {html} = view(withFiller(SEASON));
  assert.ok(!/NaN|undefined/.test(html), 'found NaN or undefined in the rendered table');
});
