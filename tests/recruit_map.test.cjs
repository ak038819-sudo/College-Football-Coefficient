// The heat map on a coach's page, and the class picker beside it. Four things
// can go wrong invisibly: a signee the projection cannot draw being dropped
// without being counted, a hex cell holding a point whose centre is nearer a
// different cell, a career view that does not add up to its classes, and a
// "top signees" list that hands the top of a class to the players the feed
// never ranked. These lift the real functions out of the shell.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const shell = fs.readFileSync('ui/dashboard_shell.html', 'utf8');

// The constants are lifted rather than stubbed. The ramp's thresholds and the
// hex size are the thing under test as much as the code reading them: stubbing
// them would let the shell ship a ramp nothing here ever checked.
function liftConsts(names) {
  return names.map(name => {
    const m = shell.match(new RegExp('^const ' + name + ' = .*?;$', 'm'));
    assert.ok(m, 'the shell must declare ' + name);
    return m[0];
  }).join('\n');
}

function lift(names, extra = {}) {
  const src = names.map(name => {
    const m = shell.match(new RegExp('^function ' + name + '\\([^]*?\\n\\}', 'm'));
    assert.ok(m, 'the shell must declare ' + name);
    return m[0];
  }).join('\n');
  const context = Object.assign({}, extra);
  vm.runInNewContext(liftConsts(['RECRUIT_HEX_R', 'RECRUIT_HEAT_STEPS', 'RECRUIT_TOP_SHOWN']) +
    '\n' + src + '\nthis.__out = {' + names.join(',') + ', RECRUIT_HEAT_STEPS, RECRUIT_HEX_R};',
    context);
  return context.__out;
}

// The real projection, so a coordinate that lands off the frame fails here.
const geography = require('../ui/geography.js');

const esc = s => String(s).replace(/[&<>"]/g, c =>
  ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));

const { recruitMapPoints, recruitClasses, recruitClassPoints } =
  lift(['recruitMapPoints', 'recruitClasses', 'recruitClassPoints']);

const { recruitHexes, recruitHexPath, recruitHeatLevel, RECRUIT_HEAT_STEPS, RECRUIT_HEX_R } =
  lift(['recruitHexes', 'recruitHexPath', 'recruitHeatLevel']);

const ALL = ['recruitMap', 'coachRecruitPanel', 'recruitMapPoints', 'recruitClasses',
             'recruitClassPoints', 'recruitHexes', 'recruitHexPath', 'recruitHexTitle',
             'recruitHeatLevel', 'recruitHeatLegend', 'recruitSelector', 'recruitTopSignees',
             'recruitSigneeRows'];
const { recruitMap, coachRecruitPanel, recruitSelector, recruitTopSignees, recruitHexTitle } =
  lift(ALL, { CfbGeography: geography, esc, playerHref: id => '#player=' + id });

const at = (points, unlocated = 0) => ({ points, unlocated });
const cls = (year, points, extra = {}) =>
  Object.assign({ year, team: 1, points, unlocated: 0, top: [] }, extra);
const career = (...classes) => ({ classes });

const MOBILE = [30.69, -88.04, 12, 'Mobile, AL'];
const BIRMINGHAM = [33.52, -86.80, 3, 'Birmingham, AL'];
const SYDNEY = [-33.87, 151.21, 1, 'Sydney, NSW'];

// ---- what is on the map and what is only counted ---------------------------

test('a signee the projection cannot place is counted, not dropped', () => {
  const out = recruitMapPoints(at([MOBILE, SYDNEY]));
  assert.deepEqual(Array.from(out.inside), [MOBILE]);
  assert.equal(out.offmap, 1);
  assert.equal(out.signees, 13);      // every signee, drawn or not
});

test('a point with no usable coordinates is off the map', () => {
  const out = recruitMapPoints(at([[null, null, 2, 'Nowhere'], ['x', 'y', 1, 'Nowhere']]));
  assert.equal(out.inside.length, 0);
  assert.equal(out.offmap, 3);
});

test('no points at all is not an error', () => {
  const out = recruitMapPoints(null);
  assert.equal(out.inside.length, 0);
  assert.equal(out.outside.length, 0);
  assert.equal(out.signees, 0);
  assert.equal(out.offmap, 0);
});

// ---- the classes, and the career as their sum -------------------------------

test('a career view adds up to exactly the classes it is made of', () => {
  // The exporter keeps the classes apart and this merges them. If the merge
  // drifts, the panel publishes a career total that is not its own parts.
  const out = recruitClassPoints(career(
    cls(2019, [[30.69, -88.04, 2, 'Mobile, AL'], [33.52, -86.80, 1, 'Birmingham, AL']], {unlocated: 1}),
    cls(2020, [[30.69, -88.04, 4, 'Mobile, AL']], {unlocated: 2})));
  assert.deepEqual(Array.from(out.points).map(p => Array.from(p)),
                   [[30.69, -88.04, 6, 'Mobile, AL'], [33.52, -86.80, 1, 'Birmingham, AL']]);
  assert.equal(out.unlocated, 3);
  assert.equal(recruitMapPoints(out).signees, 10);
});

test('one class is only that class', () => {
  const out = recruitClassPoints(career(
    cls(2019, [[30.69, -88.04, 2, 'Mobile, AL']], {unlocated: 1}),
    cls(2020, [[33.52, -86.80, 4, 'Birmingham, AL']])), 2020);
  assert.deepEqual(Array.from(out.points).map(p => Array.from(p)), [[33.52, -86.80, 4, 'Birmingham, AL']]);
  assert.equal(out.unlocated, 0);
});

test('a year the coach has no class for is not silently the career', () => {
  const out = recruitClassPoints(career(cls(2019, [MOBILE])), 1999);
  assert.equal(out.points.length, 0);
  assert.equal(out.unlocated, 0);
});

test('classes are read oldest first however the export ordered them', () => {
  const years = recruitClasses(career(cls(2020, []), cls(2018, []), cls(2019, [])))
    .map(c => c.year);
  assert.deepEqual(Array.from(years), [2018, 2019, 2020]);
});

test('a class with no usable year is left out rather than drawn as NaN', () => {
  assert.equal(recruitClasses(career({year: null, points: []}, cls(2020, []))).length, 1);
  assert.equal(recruitClasses(null).length, 0);
});

// ---- the binning ------------------------------------------------------------

test('a point lands in the hex whose centre is nearest it', () => {
  // The cheap row-and-column rounding assigns a point to a RECTANGLE around
  // each centre, which disagrees with the hexagon actually drawn near its
  // corners. This walks a grid of pixels and checks every one.
  const bin = (x, y) => recruitHexes([[0, 0, 1, '']], () => [x, y])[0];
  for (let x = 40; x < 160; x += 7) for (let y = 40; y < 160; y += 5) {
    const got = bin(x, y);
    const d = (cx, cy) => Math.hypot(cx - x, cy - y);
    // Every neighbouring centre of the same lattice must be further away.
    for (let dq = -2; dq <= 2; dq++) for (let dr = -2; dr <= 2; dr++) {
      const q = got.q + dq, r = got.rr + dr;
      const cx = RECRUIT_HEX_R * Math.sqrt(3) * (q + r / 2), cy = RECRUIT_HEX_R * 1.5 * r;
      assert.ok(d(got.cx, got.cy) <= d(cx, cy) + 1e-9,
                'a point at ' + x + ',' + y + ' was binned away from its nearest centre');
    }
  }
});

test('two towns in one cell are one cell carrying both counts', () => {
  const project = (lon, lat) => [lat === 1 ? 100 : 103, 100];
  const bins = recruitHexes([[1, 0, 4, 'A'], [2, 0, 3, 'B']], project);
  assert.equal(bins.length, 1);
  assert.equal(bins[0].count, 7);
  assert.equal(bins[0].towns.length, 2);
});

test('a hexagon is drawn around its own centre, six sides and closed', () => {
  const d = recruitHexPath(100, 200, 10);
  const pts = [...d.matchAll(/(-?[\d.]+) (-?[\d.]+)/g)].map(m => [Number(m[1]), Number(m[2])]);
  assert.equal(pts.length, 6);
  assert.ok(d.endsWith('Z'));
  for (const [x, y] of pts) assert.ok(Math.abs(Math.hypot(x - 100, y - 200) - 10) < 0.01);
  // Pointy-top, matching the axial frame the binning uses: a vertex straight up.
  assert.ok(pts.some(([x, y]) => Math.abs(x - 100) < 0.01 && Math.abs(y - 190) < 0.01));
});

test('the cooler cells are drawn first so a hot one is never painted over', () => {
  const project = (lon, lat) => [100 + lat * 60, 100];
  const bins = recruitHexes([[1, 0, 2, 'cool'], [2, 0, 9, 'hot'], [3, 0, 5, 'warm']], project);
  assert.deepEqual(Array.from(bins.map(b => b.count)), [2, 5, 9]);
});

// ---- the ramp ---------------------------------------------------------------

test('the ramp is absolute, so two coaches shade the same count the same', () => {
  // A ramp relative to each coach's busiest cell would paint a cell that sent
  // two players exactly as hot as Mobile, and the pages would stop comparing.
  assert.equal(recruitHeatLevel(1), 0);
  assert.equal(recruitHeatLevel(RECRUIT_HEAT_STEPS[RECRUIT_HEAT_STEPS.length - 1]),
               RECRUIT_HEAT_STEPS.length - 1);
  assert.equal(recruitHeatLevel(999), RECRUIT_HEAT_STEPS.length - 1);
  // Monotone, with a step exactly at each threshold and nowhere else.
  let last = -1;
  for (let n = 1; n < 40; n++) {
    const level = recruitHeatLevel(n);
    assert.ok(level >= last);
    assert.equal(level > last, RECRUIT_HEAT_STEPS.includes(n), 'the step at ' + n);
    last = level;
  }
});

test('every shade in the ramp is both drawable and in the legend', () => {
  const { recruitHeatLegend } = lift(['recruitHeatLegend'], { esc });
  const legend = recruitHeatLegend();
  for (let i = 0; i < RECRUIT_HEAT_STEPS.length; i++) {
    assert.ok(legend.includes('class="heat-' + i + '"'), 'the legend must show heat-' + i);
    assert.ok(shell.includes('.heat-' + i + ' {'), 'the stylesheet must fill heat-' + i);
  }
  // The top band is open-ended and the ones below it name their range.
  assert.ok(legend.includes(RECRUIT_HEAT_STEPS[RECRUIT_HEAT_STEPS.length - 1] + '+'));
});

// ---- the drawing ------------------------------------------------------------

test('a coach with nothing to draw gets no map rather than an empty frame', () => {
  assert.equal(recruitMap(at([])), '');
  assert.equal(recruitMap(at([SYDNEY])), '');
  assert.equal(coachRecruitPanel(null), '');
});

test('every drawable town is in a cell and the undrawable ones are not', () => {
  const svg = recruitMap(at([MOBILE, BIRMINGHAM, SYDNEY]));
  assert.equal((svg.match(/<path class="heat-/g) || []).length, 2);
  assert.ok(svg.includes('Mobile, AL · 12'));
  assert.ok(svg.includes('Birmingham, AL · 3'));
  assert.ok(!svg.includes('Sydney'));
});

test('the busier cell is drawn hotter than the quieter one', () => {
  // The ramp above is only a function until the map actually asks it for each
  // cell's shade. Painting every cell heat-0 passed every other test here.
  const svg = recruitMap(at([[36, -120, 25, 'Busy'], [36, -100, 1, 'Quiet']]));
  const levels = [...svg.matchAll(/<path class="heat-(\d)"/g)].map(m => Number(m[1]));
  assert.equal(levels.length, 2);
  assert.equal(Math.min(...levels), recruitHeatLevel(1));
  assert.equal(Math.max(...levels), recruitHeatLevel(25));
  assert.ok(Math.max(...levels) > Math.min(...levels));
});

test('a cell says how many signees it holds and which towns they are', () => {
  const title = recruitHexTitle({ count: 15, towns: [BIRMINGHAM, MOBILE] });
  assert.equal(title, '15 signees — Mobile, AL · 12; Birmingham, AL · 3');
  assert.ok(recruitHexTitle({ count: 1, towns: [[1, 2, 1, 'Somewhere']] }).startsWith('1 signee —'));
});

test('a cell holding many towns names the busiest and counts the rest', () => {
  const towns = Array.from({ length: 7 }, (_, i) => [1, 2, 7 - i, 'Town ' + i]);
  const title = recruitHexTitle({ count: 28, towns });
  assert.ok(title.includes('Town 0 · 7'));
  assert.ok(title.includes('and 3 more'));
  assert.ok(!title.includes('Town 5'));
});

test('a cell lands where the projection puts it', () => {
  const { project } = geography.projection(false);
  const [bin] = recruitHexes([MOBILE], project);
  const [x, y] = project(-88.04, 30.69);
  assert.ok(Math.hypot(bin.cx - x, bin.cy - y) <= RECRUIT_HEX_R);
  assert.ok(recruitMap(at([MOBILE])).includes(recruitHexPath(bin.cx, bin.cy, RECRUIT_HEX_R)));
});

test('an Alaskan hometown widens the projection rather than falling off it', () => {
  const anchorage = [61.22, -149.9, 1, 'Anchorage, AK'];
  const inset = geography.projection(true);
  const [bin] = recruitHexes([anchorage], inset.project);
  const svg = recruitMap(at([anchorage]));
  assert.ok(svg.includes('viewBox="0 0 ' + inset.width + ' ' + inset.height + '"'));
  assert.ok(svg.includes(recruitHexPath(bin.cx, bin.cy, RECRUIT_HEX_R)));
});

// ---- the panel around it ----------------------------------------------------

const ONE = career(cls(2020, [MOBILE, BIRMINGHAM]));

test('the panel counts towns and signees, both of them', () => {
  assert.ok(coachRecruitPanel(ONE).includes('15 signees from 2 towns'));
});

test('an off-map signee is said to be off the map, not quietly missing', () => {
  const html = coachRecruitPanel(career(cls(2020, [MOBILE, SYDNEY])));
  assert.ok(html.includes('13 signees from 1 town'));
  assert.ok(html.includes('1 signee is counted here but not drawn, being from outside this map'));
});

test('the panel says nothing about undrawn signees when there are none', () => {
  assert.ok(!coachRecruitPanel(ONE).includes('not drawn'));
});

test('a signee with no hometown is in the total, not quietly missing', () => {
  // 440 of them across the archive, 11 on Lance Leipold's page alone. A total
  // that leaves them out is a wrong number, not a smaller one.
  const html = coachRecruitPanel(career(cls(2020, [MOBILE], {unlocated: 11})));
  assert.ok(html.includes('23 signees from 1 town'));
  assert.ok(html.includes('11 signees are counted here but not drawn, having no hometown in the feed'));
});

test('no hometown and off the map are told apart in the note', () => {
  const html = coachRecruitPanel(career(cls(2020, [MOBILE, SYDNEY], {unlocated: 2})));
  assert.ok(html.includes('3 signees are counted here but not drawn, 2 of them having no ' +
                          'hometown in the feed and the rest being outside this map'));
});

test('a coach whose every signee is unplaceable still gets a panel, with no map', () => {
  // He signed a class; the page says so and says it cannot draw it. Returning
  // nothing here used to hide the class as well as the map.
  const html = coachRecruitPanel(career(cls(2020, [], {unlocated: 4})));
  assert.ok(html.includes('No hometown on this map'));
  assert.ok(!html.includes('<svg'));
  assert.ok(html.includes('4 signees from 0 towns'));
});

test('the list beside the map stops at the eight busiest towns', () => {
  // The MAP still bins all twelve -- it is the list that is a top eight.
  // Four degrees of longitude apart, so each town is genuinely its own cell
  // and this measures the list's cut-off rather than the binning's.
  const many = Array.from({ length: 12 }, (_, i) => [36, -120 + 4 * i, 12 - i, 'Town ' + i]);
  const html = coachRecruitPanel(career(cls(2020, many)));
  const list = html.match(/<ol class="recruit-top">[^]*?<\/ol>/)[0];
  assert.equal((list.match(/<li>/g) || []).length, 8);
  assert.ok(list.includes('Town 0</span><b>12</b>'));
  assert.ok(!list.includes('Town 8'));
  assert.equal((html.match(/<path class="heat-/g) || []).length, 12);
});

test('the panel says the map is the classes he signed', () => {
  assert.ok(coachRecruitPanel(ONE).includes('inherited is left out'));
});

test('a hometown name from the feed is escaped, not injected', () => {
  const html = coachRecruitPanel(career(cls(2020, [[30.69, -88.04, 1, '<script>x</script>']])));
  assert.ok(!html.includes('<script>x'));
  assert.ok(html.includes('&lt;script&gt;'));
});

// ---- the class picker -------------------------------------------------------

const THREE = career(
  cls(2018, [MOBILE], {top: [['Old Guy', 'QB', 4, 0.95, 120, 'Mobile, AL', null]]}),
  cls(2019, [BIRMINGHAM], {top: [['Best Guy', 'RB', 5, 0.99, 2, 'Birmingham, AL', 4241457]]}),
  cls(2020, [MOBILE], {top: [['Mid Guy', 'WR', 4, 0.96, 60, 'Mobile, AL', null]]}));

test('a coach with one class gets no picker to pick with', () => {
  assert.equal(recruitSelector(ONE), '');
  assert.ok(!coachRecruitPanel(ONE).includes('coach-recruit-class'));
});

test('the picker offers every class, newest first, and the career', () => {
  const html = recruitSelector(THREE);
  assert.deepEqual([...html.matchAll(/value="(\w+)"/g)].map(m => m[1]),
                   ['all', '2020', '2019', '2018']);
  assert.ok(html.includes('All 3 classes'));
});

test('the picker shows which view is on screen', () => {
  assert.ok(recruitSelector(THREE, null).includes('value="all" selected'));
  assert.ok(recruitSelector(THREE, 2019).includes('value="2019" selected'));
  assert.ok(!recruitSelector(THREE, 2019).includes('value="all" selected'));
});

test('the panel names the period it is showing, career or one class', () => {
  assert.ok(coachRecruitPanel(THREE).includes('the 3 classes he signed, 2018–2020'));
  assert.ok(coachRecruitPanel(THREE, 2019).includes('the 2019 class'));
  assert.ok(coachRecruitPanel(ONE).includes('the 2020 class'));
});

test('a class the coach never signed falls back to the career, not to nothing', () => {
  const html = coachRecruitPanel(THREE, 1999);
  assert.ok(html.includes('the 3 classes he signed'));
  assert.ok(html.includes('value="all" selected'));
});

test('picking a class draws that class only', () => {
  // 2019 is Birmingham's three; the career is Mobile twice and Birmingham once.
  assert.ok(coachRecruitPanel(THREE, 2019).includes('3 signees from 1 town'));
  assert.ok(coachRecruitPanel(THREE).includes('27 signees from 2 towns'));
});

// ---- the top signees --------------------------------------------------------

test('an unranked signee sorts after every ranked one, not at the top', () => {
  // The feed leaves national_rank empty on most pre-2010 rows. Treating a
  // missing rank as rank zero would hand the top of a class to the players
  // nobody rated.
  const rows = recruitTopSignees(career(cls(2020, [], {top: [
    ['Unranked', 'OL', 3, 0.81, null, 'Dallas, TX', null],
    ['Ranked 300', 'DE', 4, 0.89, 300, 'Tampa, FL', null],
  ]})));
  assert.deepEqual(Array.from(rows.map(r => r.name)), ['Ranked 300', 'Unranked']);
});

test('two unranked signees fall back to rating and then to stars', () => {
  const rows = recruitTopSignees(career(cls(2020, [], {top: [
    ['Lower rating', 'OL', 4, 0.80, null, '', null],
    ['Higher rating', 'OL', 3, 0.88, null, '', null],
    ['No rating at all', 'OL', 5, null, null, '', null],
  ]})));
  assert.deepEqual(Array.from(rows.map(r => r.name)),
                   ['Higher rating', 'Lower rating', 'No rating at all']);
});

test('a career list is the best of every class, in one order', () => {
  const rows = recruitTopSignees(THREE);
  assert.deepEqual(Array.from(rows.map(r => r.name)), ['Best Guy', 'Mid Guy', 'Old Guy']);
  assert.deepEqual(Array.from(rows.map(r => r.year)), [2019, 2020, 2018]);
});

test('one class is only its own signees', () => {
  assert.deepEqual(Array.from(recruitTopSignees(THREE, 2018).map(r => r.name)), ['Old Guy']);
});

test('the table shows stars, rating, rank and hometown', () => {
  const html = coachRecruitPanel(THREE, 2019);
  const table = html.match(/<table class="recruit-signees">[^]*?<\/table>/)[0];
  assert.ok(table.includes('Best Guy'));
  assert.ok(table.includes('★★★★★'));
  assert.ok(table.includes('0.990'));
  assert.ok(table.includes('>2<'));
  assert.ok(table.includes('Birmingham, AL'));
});

test('a signee links to a career page only where this site carries him', () => {
  const html = coachRecruitPanel(THREE, 2019);
  assert.ok(html.includes('href="#player=4241457">Best Guy</a>'));
  assert.ok(!coachRecruitPanel(THREE, 2018).includes('href="#player='));
});

test('the career table says which class each signee came in, a class table does not', () => {
  assert.ok(coachRecruitPanel(THREE).includes('>2019</span>'));
  assert.ok(!coachRecruitPanel(THREE, 2019).includes('muted-inline'));
});

test('a stat the feed never gave is a dash, not a zero', () => {
  const html = coachRecruitPanel(career(cls(2020, [MOBILE], {top: [
    ['Nobody Rated', null, null, null, null, null, null]]})));
  const row = html.match(/<tbody>[^]*?<\/tbody>/)[0];
  assert.ok(row.includes('Nobody Rated'));
  assert.ok(!/>0<|0\.000/.test(row), 'an absent rating must not read as zero: ' + row);
  assert.equal((row.match(/—/g) || []).length, 5);
});

test('the table stops at ten and the shell says so in one place', () => {
  const many = Array.from({ length: 30 }, (_, i) =>
    cls(2000 + i, [MOBILE], {top: [['Guy ' + i, 'QB', 4, 0.9, i + 1, '', null]]}));
  const html = coachRecruitPanel(career(...many));
  const table = html.match(/<tbody>[^]*?<\/tbody>/)[0];
  assert.equal((table.match(/<tr>/g) || []).length, 10);
  assert.ok(table.includes('Guy 0'));
  assert.ok(!table.includes('Guy 10'));
});

test('a signee name from the feed is escaped, not injected', () => {
  const html = coachRecruitPanel(career(cls(2020, [MOBILE],
    {top: [['<img src=x>', '<b>', 5, 0.9, 1, '<i>', null]]})));
  assert.ok(!html.includes('<img src=x'));
  assert.ok(html.includes('&lt;img'));
});

test('a coach with no signees named still gets his map', () => {
  const html = coachRecruitPanel(ONE);
  assert.ok(html.includes('<svg'));
  assert.ok(!html.includes('recruit-signees'));
});
