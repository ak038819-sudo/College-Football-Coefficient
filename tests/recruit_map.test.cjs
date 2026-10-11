// The map on a coach's page. Two things can go wrong invisibly: a signee the
// projection cannot draw being dropped without being counted, and the count a
// dot carries being turned into ink by radius rather than by area -- a town
// that sent twelve players would then be drawn with 144 times the ink of a
// town that sent one. These lift the real functions out of the shell.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const shell = fs.readFileSync('ui/dashboard_shell.html', 'utf8');

function lift(names, extra = {}) {
  const src = names.map(name => {
    const m = shell.match(new RegExp('^function ' + name + '\\([^]*?\\n\\}', 'm'));
    assert.ok(m, 'the shell must declare ' + name);
    return m[0];
  }).join('\n');
  const context = Object.assign({}, extra);
  vm.runInNewContext(src + '\nthis.__out = {' + names.join(',') + '};', context);
  return context.__out;
}

// The real projection, so a coordinate that lands off the frame fails here.
const geography = require('../ui/geography.js');

const { recruitMapPoints } = lift(['recruitMapPoints']);
const { recruitMap, coachRecruitPanel } = lift(
  ['recruitMap', 'coachRecruitPanel', 'recruitMapPoints'],
  { CfbGeography: geography, esc: s => String(s).replace(/[&<>"]/g, c =>
      ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c])) });

const MOBILE = [30.69, -88.04, 12, 'Mobile, AL'];
const BIRMINGHAM = [33.52, -86.80, 3, 'Birmingham, AL'];
const SYDNEY = [-33.87, 151.21, 1, 'Sydney, NSW'];

// ---- what is on the map and what is only counted ---------------------------

test('a signee the projection cannot place is counted, not dropped', () => {
  const out = recruitMapPoints([MOBILE, SYDNEY]);
  assert.deepEqual(Array.from(out.inside), [MOBILE]);
  assert.equal(out.offmap, 1);
  assert.equal(out.signees, 13);      // every signee, drawn or not
});

test('a point with no usable coordinates is off the map', () => {
  const out = recruitMapPoints([[null, null, 2, 'Nowhere'], ['x', 'y', 1, 'Nowhere']]);
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

// ---- the drawing ------------------------------------------------------------

test('a coach with nothing to draw gets no map rather than an empty frame', () => {
  assert.equal(recruitMap([]), '');
  assert.equal(recruitMap([SYDNEY]), '');
  assert.equal(coachRecruitPanel([SYDNEY]), '');
  assert.equal(coachRecruitPanel(null), '');
});

test('every drawable town gets exactly one circle', () => {
  const svg = recruitMap([MOBILE, BIRMINGHAM, SYDNEY]);
  assert.equal((svg.match(/<circle /g) || []).length, 2);
  assert.ok(svg.includes('Mobile, AL · 12 signees'));
  assert.ok(svg.includes('Birmingham, AL · 3 signees'));
  assert.ok(!svg.includes('Sydney'));
});

test('one signee is a signee, not 1 signees', () => {
  assert.ok(recruitMap([[30.69, -88.04, 1, 'Mobile, AL']]).includes('· 1 signee<'));
});

test('a dot carries its count as area, not as radius', () => {
  // Twelve signees against three: four times the players, so four times the
  // ink. Radius-linear scaling would make it sixteen times the ink.
  const radii = [...recruitMap([MOBILE, BIRMINGHAM]).matchAll(/ r="([\d.]+)"/g)]
    .map(m => Number(m[1]));
  assert.equal(radii.length, 2);
  const area = r => Math.PI * (r - 3) * (r - 3);   // the 3px floor is the dot itself
  assert.ok(Math.abs(area(radii[0]) / area(radii[1]) - 4) < 0.01,
            'twelve signees must cover four times the area of three: ' + radii);
});

test('the busiest town is drawn first so a smaller one on top stays visible', () => {
  const svg = recruitMap([BIRMINGHAM, MOBILE]);
  assert.ok(svg.indexOf('Mobile') < svg.indexOf('Birmingham'));
});

test('a dot lands where the projection puts it', () => {
  const [x, y] = geography.projection(false).project(-88.04, 30.69);
  assert.ok(recruitMap([MOBILE]).includes('cx="' + x.toFixed(2) + '" cy="' + y.toFixed(2) + '"'));
});

test('an Alaskan hometown widens the projection rather than falling off it', () => {
  const anchorage = [61.22, -149.9, 1, 'Anchorage, AK'];
  const inset = geography.projection(true);
  const [x, y] = inset.project(-149.9, 61.22);
  const svg = recruitMap([anchorage]);
  assert.ok(svg.includes('viewBox="0 0 ' + inset.width + ' ' + inset.height + '"'));
  assert.ok(svg.includes('cx="' + x.toFixed(2) + '" cy="' + y.toFixed(2) + '"'));
});

// ---- the panel around it ----------------------------------------------------

test('the panel counts towns and signees, both of them', () => {
  const html = coachRecruitPanel([MOBILE, BIRMINGHAM]);
  assert.ok(html.includes('15 signees from 2 towns'));
});

test('an off-map signee is said to be off the map, not quietly missing', () => {
  const html = coachRecruitPanel([MOBILE, SYDNEY]);
  assert.ok(html.includes('13 signees from 1 town'));
  assert.ok(html.includes('1 signee came from outside this map'));
});

test('the panel says nothing about off-map signees when there are none', () => {
  assert.ok(!coachRecruitPanel([MOBILE]).includes('outside this map'));
});

test('the list beside the map stops at the eight busiest towns', () => {
  // The MAP still draws all twelve -- it is the list that is a top eight.
  const many = Array.from({ length: 12 }, (_, i) =>
    [30 + i / 10, -88, 12 - i, 'Town ' + i]);
  const html = coachRecruitPanel(many);
  const list = html.match(/<ol class="recruit-top">[^]*?<\/ol>/)[0];
  assert.equal((list.match(/<li>/g) || []).length, 8);
  assert.ok(list.includes('Town 0</span><b>12</b>'));
  assert.ok(list.includes('Town 7</span><b>5</b>'));
  assert.ok(!list.includes('Town 8'));
  assert.equal((html.match(/<circle /g) || []).length, 12);
});

test('the panel says the map is the classes he signed', () => {
  assert.ok(coachRecruitPanel([MOBILE]).includes('inherited is left out'));
});

test('a hometown name from the feed is escaped, not injected', () => {
  const html = coachRecruitPanel([[30.69, -88.04, 1, '<script>x</script>']]);
  assert.ok(!html.includes('<script>x'));
  assert.ok(html.includes('&lt;script&gt;'));
});
