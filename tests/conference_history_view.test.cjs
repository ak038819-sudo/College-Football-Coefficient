// Conference strength over time.
//
// Every number comes from DATA.conference_elo_by_year, which the exporter
// already built for the one-season Conference Elo view. So what can go wrong
// here is reading it: the rank, the gaps, the conferences that no longer
// exist, and the scale the grey context lines are drawn against.
//
// Helpers are lifted from the shell for real rather than stubbed. Stubbing the
// helper a view depends on is how #71 silently replaced #72's statCell() and
// put an em dash in every player stat on the live site.
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

// Four seasons, four conferences. The Old Eight stops after 1996 (so it has a
// "Now" of nothing); the Upstart skips 1995 entirely (so its line must break);
// and each season has its own order, so a rank read off the wrong thing shows.
const BY_YEAR = {
  1994: [
    {conference: 'SEC', elo: 1600, rated: 10, members: 12},
    {conference: 'Old Eight', elo: 1550, rated: 8, members: 8},
    {conference: 'Mid-American', elo: 1400, rated: 9, members: 10},
  ],
  1995: [
    {conference: 'Old Eight', elo: 1580, rated: 8, members: 8},
    {conference: 'SEC', elo: 1570, rated: 12, members: 12},
    {conference: 'Mid-American', elo: 1390, rated: 10, members: 10},
  ],
  1996: [
    {conference: 'SEC', elo: 1620, rated: 12, members: 12},
    {conference: 'Old Eight', elo: 1500, rated: 8, members: 8},
    {conference: 'Upstart', elo: 1450, rated: 6, members: 8},
    {conference: 'Mid-American', elo: 1380, rated: 10, members: 10},
  ],
  1997: [
    {conference: 'SEC', elo: 1650, rated: 12, members: 12},
    {conference: 'Upstart', elo: 1470, rated: 8, members: 8},
    {conference: 'Mid-American', elo: 1360, rated: 10, members: 10},
  ],
};

function panel(byYear) {
  const ctx = {
    DATA: {conference_elo_by_year: byYear === undefined ? BY_YEAR : byYear},
    esc: s => String(s),
    conferenceLink: name => '<a class="c">' + name + '</a>',
    logoMarkup: () => '',
  };
  vm.runInNewContext(
    sourceOf('eloValue') + '\n' + sourceOf('conferenceHistorySeries') + '\n' +
    sourceOf('conferenceHistorySummary') + '\n' + sourceOf('conferenceHistoryCell') + '\n' +
    sourceOf('conferenceHistoryPath') + '\n' + sourceOf('conferenceHistoryTip') + '\n' +
    sourceOf('niceStep') + '\n' + sourceOf('conferenceHistoryScale') + '\n' +
    sourceOf('renderConferenceHistory') + '\n' +
    'this.series = conferenceHistorySeries; this.summary = conferenceHistorySummary;' +
    'this.render = renderConferenceHistory; this.cell = conferenceHistoryCell;' +
    'this.path = conferenceHistoryPath; this.tip = conferenceHistoryTip;' +
    'this.scale = conferenceHistoryScale;',
    ctx);
  return ctx;
}

test('each conference becomes one series across the seasons it existed', () => {
  const {series} = panel().series();
  assert.deepEqual(Array.from(series.get('Old Eight')).map(p => p.season), [1994, 1995, 1996]);
  assert.deepEqual(Array.from(series.get('Upstart')).map(p => p.season), [1996, 1997]);
});

test('the rank is that season position, not a rank over the whole history', () => {
  // The SEC is second in 1995 and first in every other season here. A rank
  // computed once over the whole series would call it first in 1995 too.
  const {series} = panel().series();
  const sec = series.get('Old Eight');
  assert.deepEqual(Array.from(sec).map(p => p.rank), [2, 1, 2]);
  assert.deepEqual(Array.from(series.get('SEC')).map(p => p.rank), [1, 2, 1, 1]);
});

test('the rank is out of the conferences rated that season, not of all time', () => {
  const {series} = panel().series();
  // 1996 has four conferences; 1997 has three.
  assert.deepEqual(Array.from(series.get('Mid-American')).map(p => p.of), [3, 3, 4, 3]);
});

test('a conference that no longer exists has no current season', () => {
  const ctx = panel();
  const data = ctx.series();
  const rows = ctx.summary(data.series, data.seasons);
  const old = rows.find(r => r.conference === 'Old Eight');
  assert.equal(old.last, 1996);
  assert.equal(old.current, null);
  // And it still has its best season, which is the point of keeping the row.
  assert.equal(old.best.elo, 1580);
  assert.equal(old.best.season, 1995);
});

test('best and worst are that conference own extremes, with the season they fell in', () => {
  const ctx = panel();
  const data = ctx.series();
  const rows = ctx.summary(data.series, data.seasons);
  const mac = rows.find(r => r.conference === 'Mid-American');
  assert.deepEqual([mac.best.elo, mac.best.season], [1400, 1994]);
  assert.deepEqual([mac.worst.elo, mac.worst.season], [1360, 1997]);
});

test('the table is ordered by where a conference stands now, defunct ones last', () => {
  const ctx = panel();
  const data = ctx.series();
  const rows = Array.from(ctx.summary(data.series, data.seasons)).map(r => r.conference);
  assert.deepEqual(rows, ['SEC', 'Upstart', 'Mid-American', 'Old Eight']);
});

test('a conference with nothing now shows a dash that sorts as absent, not as zero', () => {
  // data-sort="0" would rank a defunct conference as the weakest in the sport
  // instead of as having no current rating at all.
  const absent = panel().cell(null);
  assert.ok(absent.includes('data-sort=""'), absent);
  assert.ok(!absent.includes('data-sort="0"'));
  assert.ok(absent.includes('—'));
});

test('every value cell carries its own sort key, so one dash cannot break the column', () => {
  // ui/stats.js reads data-sort per cell on the click; one unkeyed cell sends
  // the whole column to a text sort. This is PR #85's bug.
  const html = panel().render();
  const valueCells = html.match(/<td class="num" data-sort=|<td class="num">/g) || [];
  const unkeyed = valueCells.filter(c => !c.includes('data-sort'));
  // Seasons/From/To are counts and years: their text IS their value, so they
  // need no key. The three rating columns on all four rows do.
  assert.equal(unkeyed.length, 3 * 4);
  assert.equal(valueCells.length - unkeyed.length, 3 * 4);
  assert.match(html, /data-sort="1650"/);
});

test('a rating is rendered as a whole number with its season beside it', () => {
  const cell = panel().cell({elo: 1584.6, season: 2011});
  assert.match(cell, />1585 /);
  assert.match(cell, /2011/);
  // The decimal survives only in the sort key, where it belongs.
  assert.match(cell, /data-sort="1584\.6"/);
  assert.doesNotMatch(cell.replace(/data-sort="[^"]*"/, ''), /1584\.6/);
});

test('a line breaks where a conference missed a season', () => {
  // Drawn straight through, the Upstart would appear to have had a rating in
  // 1995, and a defunct conference would run flat to the present day.
  const ctx = panel();
  const {series} = ctx.series();
  const gapped = [{season: 1994, elo: 1500}, {season: 1996, elo: 1600}];
  const d = ctx.path(gapped, s => s, e => e);
  assert.equal(d.match(/M/g).length, 2, 'both points start their own segment: ' + d);
  // And a continuous run is one segment.
  const solid = ctx.path(Array.from(series.get('SEC')), s => s, e => e);
  assert.equal(solid.match(/M/g).length, 1, solid);
  assert.equal(solid.match(/L/g).length, 3);
});

test('the note says the span, how many conferences survive, and how many do not', () => {
  const html = panel().render();
  assert.match(html, /1994–1997/);
  assert.match(html, /3 conferences are in the newest season and 1 no longer exist/);
  assert.match(html, /never today’s/);
});

test('the chosen conference defaults to one that still exists', () => {
  // Opening the page on a conference that folded in 1996 would be a strange
  // first impression, and the select is ordered current-first for that reason.
  const html = panel().render();
  const selected = html.match(/<option value="([^"]*)" selected>/);
  assert.ok(selected, html.slice(0, 400));
  assert.equal(selected[1], 'SEC');
});

test('a defunct conference is labelled with its years in the picker', () => {
  const html = panel().render();
  assert.match(html, /Old Eight \(1994–1996\)/);
  // A current one is not: its years are in the table and would be noise here.
  assert.doesNotMatch(html, /SEC \(1994/);
});

test('the tooltip gives the season rank and the coverage behind the number', () => {
  // "Average member Elo" is only as good as how many members were rated, so
  // the count travels with the number rather than being left in a note.
  const tip = panel().tip('Upstart', {season: 1996, elo: 1450, rank: 3, of: 4,
    rated: 6, members: 8});
  assert.match(tip, /1996/);
  assert.match(tip, /Upstart 1450/);
  assert.match(tip, /#3 of 4 conferences/);
  assert.match(tip, /6 of 8 members rated/);
});

test('no membership data renders a reason rather than an empty chart', () => {
  const html = panel({}).render();
  assert.match(html, /no history/);
  assert.doesNotMatch(html, /<table/);
  assert.doesNotMatch(html, /<svg/);
});

test('the view is a registered rankings route, or the tab would fall back to Top 25', () => {
  // navigation.js validates the subview against its own list and silently
  // substitutes the first entry when it does not match. The Schedule Strength
  // view rendered perfectly for an hour while this was missing (PR #90).
  const nav = fs.readFileSync('ui/navigation.js', 'utf8');
  const rankings = nav.match(/rankings: \[([^\]]*)\]/);
  assert.ok(rankings, 'rankings route list missing');
  assert.match(rankings[1], /'conference-history'/);
  assert.match(shell, /\['conference-history', 'Conference History'\]/);
  assert.match(shell, /'conference-history': renderConferenceHistory/);
  assert.match(shell, /subview === 'conference-history'\) hydrateConferenceHistory/);
});

// The chart's one piece of arithmetic. It is a function of its own so this can
// be checked without a DOM: the failure it guards against is silent, because a
// line drawn outside the plot area is simply invisible.
const GEOM = {l: 46, r: 14, t: 12, b: 26};

test('the y-axis covers every conference, not only the chosen one', () => {
  const ctx = panel();
  const data = ctx.series();
  const scale = ctx.scale(data, GEOM, 600, 320);
  // The fixture spans 1360 (Mid-American 1997) to 1650 (SEC 1997). Scaling to
  // any one series would clip the others out of the plot entirely.
  assert.ok(scale.lo <= 1360, 'low end must reach the weakest conference: ' + scale.lo);
  assert.ok(scale.hi >= 1650, 'high end must reach the strongest: ' + scale.hi);
});

test('every point of every conference lands inside the plot area', () => {
  const ctx = panel();
  const data = ctx.series();
  const scale = ctx.scale(data, GEOM, 600, 320);
  const top = GEOM.t, bottom = 320 - GEOM.b;
  data.series.forEach(points => points.forEach(p => {
    const y = scale.Y(p.elo);
    assert.ok(y >= top - 0.01 && y <= bottom + 0.01,
      'Elo ' + p.elo + ' drew at y=' + y + ', outside ' + top + '..' + bottom);
  }));
});

test('the x-axis is the season, so a short-lived conference sits under its years', () => {
  const ctx = panel();
  const scale = ctx.scale(ctx.series(), GEOM, 600, 320);
  // 1994 at the left edge, 1997 at the right, and 1995 a third of the way in.
  assert.equal(scale.X(1994), GEOM.l);
  assert.equal(scale.X(1997), 600 - GEOM.r);
  const third = GEOM.l + (600 - GEOM.l - GEOM.r) / 3;
  assert.ok(Math.abs(scale.X(1995) - third) < 0.01, scale.X(1995) + ' vs ' + third);
});

test('one conference in one season draws a line rather than nothing', () => {
  // lo === hi makes Y() divide by zero, and an SVG path full of NaN renders
  // as empty space with no error anywhere. The guard gives it a flat line.
  const one = panel({1996: [{conference: 'SEC', elo: 1600, rated: 12, members: 12}]});
  const data = one.series();
  const scale = one.scale(data, GEOM, 600, 320);
  assert.equal(scale.X(1996), GEOM.l);
  const y = scale.Y(1600);
  assert.ok(Number.isFinite(y), 'y was ' + y);
  assert.ok(y > GEOM.t && y < 320 - GEOM.b, 'the line should sit inside the plot: ' + y);
  const d = one.path(Array.from(data.series.get('SEC')), scale.X, scale.Y);
  assert.doesNotMatch(d, /NaN/);
});
