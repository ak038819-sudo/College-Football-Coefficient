// The three marks a player page carries in place of a portrait: the jersey
// plate, the hometown pin and the career strip. Each has a way of being wrong
// that a green build would not show: a number written in white on a near-white
// team colour, a pin drawn with latitude and longitude the wrong way round, a
// strip drawn newest-first or in whichever category happened to sort first, and
// negative rushing yardage flattened to nothing.
//
// Everything is lifted out of the shell, constants included, and the geography
// is the real module: a projection stub would have passed the swapped
// coordinates these tests exist to catch.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const shell = fs.readFileSync('ui/dashboard_shell.html', 'utf8');
const geography = require('../ui/geography.js');

const esc = s => String(s).replace(/[&<>"]/g, c =>
  ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));

function liftConsts(names) {
  return names.map(name => {
    // The one-line form FIRST. A lazy multi-line pattern tried first matches a
    // scalar declaration and then keeps going to the next `};` anywhere in the
    // file, which lifts a few hundred lines of unrelated code with it.
    const m = shell.match(new RegExp('^const ' + name + ' = [^\\n]*;$', 'm'))
      || shell.match(new RegExp('^const ' + name + ' = \\{[^]*?\\n\\};$', 'm'))
      || shell.match(new RegExp('^const ' + name + ' = \\[[^]*?\\];$', 'm'));
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
  vm.runInNewContext(
    liftConsts(['STAT_CATEGORY_ORDER', 'STAT_CATEGORY_LABELS', 'STRIP_PRIMARY_STAT',
                'STRIP_STAT_LABELS', 'STRIP_POSITION_CATEGORY', 'STRIP_POSITION_MINIMUM',
                'STRIP_BAR_WIDTH', 'STRIP_BAR_GAP', 'STRIP_SEASON_GAP', 'STRIP_HEIGHT']) +
    '\n' + src + '\nthis.__out = {' + names.join(',') +
    ', STRIP_PRIMARY_STAT, STRIP_HEIGHT, STRIP_BAR_WIDTH, STRIP_BAR_GAP, STRIP_SEASON_GAP};',
    context);
  return context.__out;
}

// Two eras of one team's colour, so a page asking for the wrong season gets a
// visibly different answer. The real asset file has 499 of these ranges.
const DATED_LOGOS = {
  'Example State': [
    { start: 1980, end: 2000, bg: '#0F172A' },
    { start: 2000, end: null, bg: '#F8FAFC' }
  ],
  'Dark U': [{ start: 1980, end: null, bg: '#102A43' }]
};
const TEAM_NAMES = { 7: 'Example State', 9: 'Dark U', 11: 'No Logo Tech' };

const plateBits = lift(
  ['plateInk', 'playerPlateColor', 'playerPlate', 'pickDatedLogoEntry', 'hashHue', 'initials'],
  { esc, DATED_LOGOS, teamNameById: id => TEAM_NAMES[id] || null });
const { plateInk, playerPlateColor, playerPlate } = plateBits;

const pinBits = lift(['hometownPin'], { esc, CfbGeography: geography });
const { hometownPin } = pinBits;

const stripBits = lift(
  ['stripValue', 'stripCategory', 'stripGames', 'careerStripPanel'],
  { esc, CfbGeography: geography,
    teamNameById: id => TEAM_NAMES[id] || null,
    gameHref: (id, season) => '#game=' + id + '&season=' + season,
    fmtGameDate: g => String(g.date || '') });
const { stripValue, stripCategory, stripGames, careerStripPanel,
        STRIP_HEIGHT, STRIP_BAR_WIDTH, STRIP_BAR_GAP } = stripBits;

// ---------------- the plate ----------------

test('ink is chosen for the colour it is written on, not assumed', () => {
  // 335 of the 499 team chip colours are light. White numerals on Wyoming's
  // #F8FAFC are invisible, which no test asserting markup shape would catch.
  assert.equal(plateInk('#F8FAFC'), '#10161f');
  assert.equal(plateInk('#0F172A'), '#fff');
  // Mid-tones either side of the threshold.
  assert.equal(plateInk('#9E7C0C'), '#fff');
  assert.equal(plateInk('#E8C547'), '#10161f');
  // Anything that is not a six-digit hex is not a colour to reason about.
  assert.equal(plateInk(null), '#fff');
  assert.equal(plateInk('rebeccapurple'), '#fff');
});

test('the plate takes the colour the team wore that season', () => {
  assert.equal(playerPlateColor(7, 1995), '#0F172A');
  assert.equal(playerPlateColor(7, 2024), '#F8FAFC');
  assert.equal(playerPlateColor(11, 2024), null, 'a team with no logo asset has no colour');
  assert.equal(playerPlateColor(null, 2024), null);
});

test('the plate carries the number he wore, in readable ink', () => {
  const light = playerPlate({ display_name: 'Ada Back' }, { jersey: 7, team_id: 7, season_year: 2024 });
  assert.match(light, /class="jersey-plate"/);
  assert.match(light, />7</, 'the number itself must be on the plate');
  assert.match(light, /background:#F8FAFC/);
  assert.match(light, /color:#10161f/, 'a near-white plate needs dark numerals');
  assert.match(light, /aria-label="Jersey number 7"/);

  const dark = playerPlate({ display_name: 'Ada Back' }, { jersey: 42, team_id: 9, season_year: 2024 });
  assert.match(dark, /background:#102A43/);
  assert.match(dark, /color:#fff/);
  assert.match(dark, />42</);
});

test('a player with no number gets initials, never an empty plate', () => {
  const plate = playerPlate({ display_name: 'Ada Back' }, { team_id: 9, season_year: 2024 });
  assert.match(plate, /class="jersey-plate initials"/);
  assert.match(plate, />AB</);
  assert.doesNotMatch(plate, /plate-hash/, 'there is no number to put a # in front of');
  // Jersey 0 is a number a player really wears, and it is not "no number".
  assert.match(playerPlate({ display_name: 'Ada Back' }, { jersey: 0, team_id: 9, season_year: 2024 }),
    /class="jersey-plate"[^]*>0</);
});

test('a player whose team has no colour still gets a coloured plate', () => {
  const plate = playerPlate({ display_name: 'Ada Back' }, { jersey: 3, team_id: 11, season_year: 2024 });
  assert.match(plate, /background:hsl\(\d+,48%,38%\)/);
  assert.match(plate, /color:#fff/);
});

test('a licensed photo takes the slot from the plate', () => {
  // The schema's headshot columns are empty today by policy, not by accident.
  // The day a source clears, the plate has to step aside without another change.
  const plate = playerPlate(
    { display_name: 'Ada Back', headshot_url: 'https://example.test/a.png' },
    { jersey: 7, team_id: 9, season_year: 2024 });
  assert.match(plate, /<img class="logo-img" src="https:\/\/example.test\/a.png"/);
  assert.doesNotMatch(plate, /jersey-plate/);
});

// ---------------- the pin ----------------

test('the pin is drawn where the projection puts the town', () => {
  // Nashville. The projection takes longitude first; calling it the other way
  // round compiles, draws, and puts every player in the wrong hemisphere.
  const lat = 36.1627, lon = -86.7816;
  const [x, y] = geography.projection(false).project(lon, lat);
  const svg = hometownPin({ hometown: 'Nashville', home_state: 'TN', hometown_point: [lat, lon, 'own'] });
  assert.match(svg, new RegExp('cx="' + x.toFixed(1) + '" cy="' + y.toFixed(1) + '"'));
  assert.match(svg, /Nashville, TN/);
  const frame = geography.projection(false);
  assert.ok(x > 0 && x < frame.width && y > 0 && y < frame.height, 'and inside the frame');
});

test('an Alaskan hometown switches to the frame that can hold it', () => {
  const lat = 61.2181, lon = -149.9003;      // Anchorage
  const svg = hometownPin({ hometown: 'Anchorage', home_state: 'AK', hometown_point: [lat, lon, 'own'] });
  const wide = geography.projection(true);
  assert.match(svg, new RegExp('viewBox="0 0 ' + wide.width + ' ' + wide.height + '"'));
  const [x, y] = wide.project(lon, lat);
  assert.match(svg, new RegExp('cx="' + x.toFixed(1) + '" cy="' + y.toFixed(1) + '"'));
});

test('the pin says whether the coordinate is his or his town name\'s', () => {
  const own = hometownPin({ hometown: 'Nashville', home_state: 'TN', hometown_point: [36.16, -86.78, 'own'] });
  assert.match(own, /title="from his own recruiting record"/);
  const town = hometownPin({ hometown: 'Nashville', home_state: 'TN', hometown_point: [36.16, -86.78, 'town'] });
  assert.match(town, /title="located by matching the town name and state"/);
});

test('no coordinate means no pin, never a pin at nowhere', () => {
  assert.equal(hometownPin({ hometown: 'Lagos' }), '');
  assert.equal(hometownPin({}), '');
  assert.equal(hometownPin({ hometown_point: ['x', 'y', 'own'] }), '');
  assert.equal(hometownPin({ hometown_point: [36.16] }), '');
});

// ---------------- the strip ----------------

const game = (id, season, date, stats, opponent = 9) =>
  [id, date, opponent, 1, 24, 17, stats, date + 'T18:00:00.000Z', 0];

test('a paired box-score value is not a quantity to draw', () => {
  assert.equal(stripValue({ 'C/ATT': '18/29' }, 'C/ATT'), null);
  assert.equal(stripValue({ FG: '0/0' }, 'FG'), null);
  assert.equal(stripValue({ YDS: '' }, 'YDS'), null);
  assert.equal(stripValue({ YDS: null }, 'YDS'), null);
  assert.equal(stripValue({ YDS: '118' }, 'YDS'), 118);
  assert.equal(stripValue({ YDS: -8 }, 'YDS'), -8);
  assert.equal(stripValue({ YDS: 0 }, 'YDS'), 0);
});

test('a quarterback is drawn as a passer, not as the sacks he took', () => {
  // The real case this rule exists for: Max Johnson appears in the rushing box
  // score of 31 games and the passing one of 30, so counting games alone drew
  // his career as rushing yardage, eleven bars of it negative from sacks.
  const log = {
    2023: [game(3, 2023, '2023-09-16', { passing: { YDS: 240 }, rushing: { YDS: -7 } }),
           game(2, 2023, '2023-09-09', { passing: { YDS: 310 }, rushing: { YDS: -3 } }),
           game(1, 2023, '2023-09-02', { rushing: { YDS: 4 } })]
  };
  assert.deepEqual(Object.assign({}, stripCategory(log, 'QB')), { category: 'passing', byPosition: true });
  assert.deepEqual(Object.assign({}, stripCategory(log, undefined)),
    { category: 'rushing', byPosition: false }, 'and without a position, games still decide');
  assert.match(careerStripPanel(log, 'QB'), /Passing yards/);
});

test('a position has to earn its category', () => {
  // A quarterback who moved to receiver is the receiver he became. One passing
  // game is not a career to draw.
  const log = {
    2023: [game(3, 2023, '2023-09-16', { receiving: { YDS: 40 } }),
           game(2, 2023, '2023-09-09', { receiving: { YDS: 70 } }),
           game(1, 2023, '2023-09-02', { passing: { YDS: 12 } })]
  };
  assert.deepEqual(Object.assign({}, stripCategory(log, 'QB')), { category: 'receiving', byPosition: false });
  // A lineman's position maps to no box-score category at all.
  assert.deepEqual(Object.assign({}, stripCategory({ 2023: [
    game(2, 2023, '2023-09-09', { defensive: { TOT: 1 } }),
    game(1, 2023, '2023-09-02', { defensive: { TOT: 2 } })] }, 'OL')),
    { category: 'defensive', byPosition: false });
});

test('the category is the one he appears in most, not the first one listed', () => {
  // Passing sorts first in STAT_CATEGORY_ORDER; this player is a receiver who
  // threw one pass. Picking by order rather than by count would draw his
  // career as that single trick play.
  const log = {
    2023: [game(3, 2023, '2023-09-16', { receiving: { YDS: 40 } }),
           game(2, 2023, '2023-09-09', { receiving: { YDS: 70 }, passing: { YDS: 12 } }),
           game(1, 2023, '2023-09-02', { receiving: { YDS: 55 } })]
  };
  assert.equal(stripCategory(log).category, 'receiving');
  // A category with no readable number in it is not a category he appears in.
  assert.equal(stripCategory({ 2023: [game(1, 2023, '2023-09-02', { passing: { 'C/ATT': '1/2' } })] }), null);
  assert.equal(stripCategory({}), null);
  // Fumbles are never the category a career is drawn in.
  assert.equal(stripCategory({ 2023: [game(1, 2023, '2023-09-02', { fumbles: { FUM: 2 } })] }), null);
});

test('the strip reads oldest first, across seasons and within them', () => {
  const log = {
    2024: [game(22, 2024, '2024-09-14', { rushing: { YDS: 90 } }),
           game(21, 2024, '2024-09-07', { rushing: { YDS: 80 } })],
    2023: [game(12, 2023, '2023-09-09', { rushing: { YDS: 60 } }),
           game(11, 2023, '2023-09-02', { rushing: { YDS: 50 } })]
  };
  assert.deepEqual(Array.from(stripGames(log, 'rushing').map(g => g.gameId)), [11, 12, 21, 22]);
  assert.deepEqual(Array.from(stripGames(log, 'rushing').map(g => g.value)), [50, 60, 80, 90]);
});

test('every game with a number becomes a bar that links to its game', () => {
  const log = {
    2023: [game(2, 2023, '2023-09-09', { rushing: { YDS: 40 } }),
           game(1, 2023, '2023-09-02', { rushing: { YDS: 120 } })]
  };
  const html = careerStripPanel(log);
  assert.equal((html.match(/<rect /g) || []).length, 2);
  // esc() escapes the ampersand, so the markup carries the entity.
  assert.match(html, /href="#game=1&amp;season=2023"/);
  assert.match(html, /href="#game=2&amp;season=2023"/);
  assert.match(html, /Rushing yards/);
  assert.match(html, /high of 120/);
  // The tallest game fills the strip; the shorter one is proportional to it.
  const heights = Array.from(html.matchAll(/height="([\d.]+)"/g)).map(m => Number(m[1]));
  assert.equal(Math.max(...heights), STRIP_HEIGHT);
  assert.ok(Math.abs(Math.min(...heights) - STRIP_HEIGHT * (40 / 120)) < 0.2);
});

test('negative yardage hangs below a baseline instead of vanishing', () => {
  // A sacked quarterback rushes for -8. Clamping that to zero would draw the
  // same bar as a carry for no gain.
  const log = {
    2023: [game(2, 2023, '2023-09-09', { rushing: { YDS: -8 } }),
           game(1, 2023, '2023-09-02', { rushing: { YDS: 24 } })]
  };
  const html = careerStripPanel(log);
  assert.match(html, /class="strip-zero"/, 'a range crossing zero needs the line it crosses');
  const zero = Number(/class="strip-zero" x1="0" y1="([\d.]+)"/.exec(html)[1]);
  const rects = Array.from(html.matchAll(/y="([\d.]+)" width="\d+" height="([\d.]+)"/g))
    .map(m => ({ y: Number(m[1]), height: Number(m[2]) }));
  const positive = rects[0], negative = rects[1];
  assert.ok(Math.abs(positive.y + positive.height - zero) < 0.2, 'a gain rises to the baseline');
  assert.ok(Math.abs(negative.y - zero) < 0.2, 'a loss starts at the baseline');
  assert.ok(zero > 0 && zero < STRIP_HEIGHT, 'and the baseline is inside the strip');
  assert.match(html, /class="strip-bar negative"/);
  // A strip with nothing negative in it draws no baseline.
  assert.doesNotMatch(careerStripPanel({ 2023: [
    game(2, 2023, '2023-09-09', { rushing: { YDS: 8 } }),
    game(1, 2023, '2023-09-02', { rushing: { YDS: 24 } })] }), /strip-zero/);
});

test('a game he recorded nothing in is a tick, not a gap', () => {
  const log = {
    2023: [game(2, 2023, '2023-09-09', { rushing: { YDS: 0 } }),
           game(1, 2023, '2023-09-02', { rushing: { YDS: 60 } })]
  };
  const html = careerStripPanel(log);
  assert.equal((html.match(/<rect /g) || []).length, 2);
  const heights = Array.from(html.matchAll(/height="([\d.]+)"/g)).map(m => Number(m[1]));
  assert.equal(Math.min(...heights), 1);
});

test('seasons are separated and labelled where they start', () => {
  const log = {
    2024: [game(21, 2024, '2024-09-07', { rushing: { YDS: 80 } })],
    2023: [game(11, 2023, '2023-09-02', { rushing: { YDS: 50 } })]
  };
  const html = careerStripPanel(log);
  assert.match(html, /class="strip-season"[^>]*>2023</);
  assert.match(html, /class="strip-season"[^>]*>2024</);
  const xs = Array.from(html.matchAll(/<rect class="strip-bar" x="([\d.]+)"/g)).map(m => Number(m[1]));
  assert.ok(xs[1] - xs[0] > STRIP_BAR_WIDTH + STRIP_BAR_GAP,
    'a new season leaves a wider gap than the next game in the same one');
});

test('the note says which rule chose the category, not a rule it did not use', () => {
  // A note reading "the category he appears in most often" under a strip chosen
  // by position is a false explanation of what the reader is looking at.
  const qb = {
    2023: [game(3, 2023, '2023-09-16', { passing: { YDS: 240 }, rushing: { YDS: -7 } }),
           game(2, 2023, '2023-09-09', { passing: { YDS: 310 }, rushing: { YDS: -3 } }),
           game(1, 2023, '2023-09-02', { rushing: { YDS: 4 } })]
  };
  const byPosition = careerStripPanel(qb, 'QB');
  assert.match(byPosition, /Drawn in the category his listed position plays in/);
  assert.doesNotMatch(byPosition, /appears in most often/);
  const byGames = careerStripPanel(qb, 'OL');
  assert.match(byGames, /Drawn in the category he appears in most often/);
  assert.doesNotMatch(byGames, /listed position plays in/);
});

test('the category and the statistic are not named twice over', () => {
  // A defender before 2016 has no tackles in this archive -- the box scores do
  // not carry them that far back -- so his strip falls back to interceptions,
  // where naming the category and the statistic both reads "Interceptions
  // interceptions".
  const log = {
    2009: [game(2, 2009, '2009-09-12', { interceptions: { INT: 1 } }),
           game(1, 2009, '2009-09-05', { interceptions: { INT: 2 } })]
  };
  const html = careerStripPanel(log, 'LB');
  assert.match(html, /Interceptions in each of his 2 games/);
  assert.doesNotMatch(html, /Interceptions interceptions/i);
  // Where they differ, both are still named.
  assert.match(careerStripPanel({ 2023: [
    game(2, 2023, '2023-09-09', { defensive: { TOT: 4 } }),
    game(1, 2023, '2023-09-02', { defensive: { TOT: 7 } })] }, 'LB'), /Defense tackles/);
});

test('one game is not a shape, and no numbers are no strip', () => {
  assert.equal(careerStripPanel({ 2023: [game(1, 2023, '2023-09-02', { rushing: { YDS: 60 } })] }), '');
  assert.equal(careerStripPanel({ 2023: [game(1, 2023, '2023-09-02', { passing: { 'C/ATT': '1/2' } })] }), '');
  assert.equal(careerStripPanel({}), '');
  assert.equal(careerStripPanel(null), '');
});
