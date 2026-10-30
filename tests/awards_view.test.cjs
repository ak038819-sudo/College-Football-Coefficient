// The Awards view and the awards panel on a player page.
//
// The identity work behind these pages is tested in tests/test_player_awards.py.
// What can go wrong HERE is presentation, and a page about trophies has its own
// ways to mislead:
//
//   * a winner this project never identified rendered as a link, which would
//     send a reader to someone else's page under a Heisman winner's name;
//   * a school label that disagrees with the link beside it;
//   * an empty Awards panel on a player page, which on 99,563 of 99,813 pages
//     would be the loudest thing on the page;
//   * a shared trophy showing one of its winners, so a co-winner disappears;
//   * a season the archive does not hold rendering as a season with no awards
//     given, which is a different claim.
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

// 2024, trimmed. Travis Hunter's sweep is the real shape of the data -- one
// person holding six trophies in a season -- and the Lombardi is shared here so
// a co-winner has somewhere to be dropped from.
const DATA = {
  awards: [
    {key: 'heisman-memorial-trophy', name: 'Heisman Memorial Trophy',
     description: 'Outstanding Player', category: 'player', first: 2000, last: 2024},
    {key: 'fred-biletnikoff-award', name: 'Fred Biletnikoff Award',
     description: 'Outstanding Receiver', category: 'player', first: 2000, last: 2024},
    {key: 'lombardi-award', name: 'Lombardi Award',
     description: 'Lineman of the Year', category: 'player', first: 2000, last: 2024},
    {key: 'rimington-trophy', name: 'Rimington Trophy',
     description: 'Outstanding Center', category: 'player', first: 2000, last: 2024},
    {key: 'the-home-depot-award', name: 'The Home Depot Award',
     description: 'Coach of the Year', category: 'coach', first: 2000, last: 2024},
  ],
  winners: [
    {season: 2024, key: 'heisman-memorial-trophy', name: 'Travis Hunter',
     school: 'Colorado', position: 'WR', player: 4685415, team: 38},
    {season: 2024, key: 'fred-biletnikoff-award', name: 'Travis Hunter',
     school: 'Colorado', position: 'WR', player: 4685415, team: 38},
    // Shared, and the second name is one the project never identified.
    {season: 2024, key: 'lombardi-award', name: 'Kelvin Banks Jr.',
     school: 'Texas', position: 'OT', player: 4685001, team: 2633},
    {season: 2024, key: 'lombardi-award', name: 'A Second Winner',
     school: 'Texas', position: 'OT'},
    // No page, and no school this project carries either.
    {season: 2024, key: 'rimington-trophy', name: 'Seth McLaughlin',
     school: 'Ohio State', position: 'C'},
    {season: 2024, key: 'the-home-depot-award', name: 'Curt Cignetti',
     school: 'Indiana', team: 84},
    {season: 2023, key: 'heisman-memorial-trophy', name: 'Jayden Daniels',
     school: 'LSU', position: 'QB', player: 4426348, team: 99},
  ],
  by_player: {4685415: [0, 1], 4685001: [2], 4426348: [6]},
};

function view(season, {manifest, data} = {}) {
  const ctx = {
    STATIC_MANIFEST: {awards: manifest === undefined ? 'data/awards.js?v=abc' : manifest},
    state: {year: season, section: 'stats', subview: 'awards'},
    // The REAL esc, lifted from the shell rather than stubbed as identity.
    // It was `s => String(s)`, which quietly made every claim in this file
    // about escaping vacuous: a name carrying an ampersand or a quote would
    // have come through raw and no assertion here would have noticed.

    teamCellById: id => '<a class="t" href="#team=' + id + '">Team ' + id + '</a>',
    playerHref: id => '#player=' + id,
  };
  vm.runInNewContext(
    sourceOf('esc') + '\n' +
    sourceOf('awardsWinnerCell') + '\n' + sourceOf('awardsHtml') + '\n' +
    sourceOf('renderAwards') + '\n' + sourceOf('playerAwardsPanel') + '\n' +
    'this.html = awardsHtml; this.render = renderAwards;' +
    'this.cell = awardsWinnerCell; this.panel = playerAwardsPanel;',
    ctx);
  const payload = data === undefined ? DATA : data;
  return {html: payload ? ctx.html(payload, season) : null, ctx, payload};
}

function rowOf(html, award) {
  const body = html.slice(html.indexOf('<tbody>'), html.indexOf('</tbody>'));
  const rows = body.split('<tr>').filter(r => r.includes('>' + award + '<'));
  return rows;
}

test('the awards view', async t => {
  await t.test('every trophy awarded that season has a row', () => {
    const {html} = view(2024);
    const body = html.slice(html.indexOf('<tbody>'), html.indexOf('</tbody>'));
    assert.equal(body.split('<tr>').length - 1, 6, 'six winners in 2024');
    for (const name of ['Heisman Memorial Trophy', 'Fred Biletnikoff Award',
                        'Lombardi Award', 'Rimington Trophy', 'The Home Depot Award']) {
      assert.ok(html.includes(name), name + ' is on the page');
    }
  });

  await t.test('a shared trophy keeps both of its winners', () => {
    const {html} = view(2024);
    const rows = rowOf(html, 'Lombardi Award');
    assert.equal(rows.length, 2, 'a co-winner is a winner');
    assert.ok(rows.some(r => r.includes('Kelvin Banks Jr.')));
    assert.ok(rows.some(r => r.includes('A Second Winner')));
  });

  await t.test('a winner links to his page only where one was identified', () => {
    const {html} = view(2024);
    const [heisman] = rowOf(html, 'Heisman Memorial Trophy');
    assert.match(heisman, /href="#player=4685415">Travis Hunter<\/a>/,
      'an identified winner links to his page');
    const [rimington] = rowOf(html, 'Rimington Trophy');
    assert.ok(rimington.includes('Seth McLaughlin'), 'the name is still shown');
    assert.ok(!/href="#player=/.test(rimington),
      'a winner this project never identified must not link to anyone');
  });

  await t.test('a co-winner with no page does not borrow the other one’s link', () => {
    const {html} = view(2024);
    const second = rowOf(html, 'Lombardi Award').find(r => r.includes('A Second Winner'));
    assert.ok(!/href="#player=/.test(second));
  });

  await t.test('the school links where it is a team here and is plain text where it is not', () => {
    const {html} = view(2024);
    const [heisman] = rowOf(html, 'Heisman Memorial Trophy');
    assert.match(heisman, /href="#team=38"/, 'a resolved school links to its team page');
    const [rimington] = rowOf(html, 'Rimington Trophy');
    assert.ok(rimington.includes('Ohio State'), 'the school is still named');
    assert.ok(!/href="#team=/.test(rimington),
      'a school with no team id here cannot link to a team page');
  });

  await t.test('the coach awards are shown and say why they do not link', () => {
    const {html} = view(2024);
    const [coach] = rowOf(html, 'The Home Depot Award');
    assert.ok(coach.includes('Curt Cignetti'));
    assert.ok(!/href="#player=/.test(coach), 'a coach is not a player page');
    assert.match(html, /coach-of-the-year awards are included and carry no link/);
  });

  await t.test('the note counts the winners and how many of them have a page', () => {
    const {html} = view(2024);
    // Three of the six 2024 rows carry a player id.
    assert.match(html, /6 winners of 5 trophies in 2024; 3 of them are identified/);
  });

  await t.test('only that season is shown', () => {
    const {html} = view(2023);
    assert.ok(html.includes('Jayden Daniels'), '2023 shows its own winner');
    assert.ok(!html.includes('Travis Hunter'), '2024 must not leak into 2023');
    assert.match(html, /1 winners of 1 trophies in 2023/);
  });

  await t.test('a season the archive does not hold says so, and says what it holds', () => {
    const {html} = view(1995);
    assert.match(html, /No award winners are recorded for 1995/);
    // Not "no awards were given in 1995", which is a different claim.
    assert.match(html, /archive covers 2023–2024/);
    assert.ok(!html.includes('<table'), 'no empty table');
  });

  await t.test('an uncovered season offers the latest one that IS covered', () => {
    // The stats views share one season picker, so Awards opens on the current
    // season -- which the archive is always a year behind, because trophies
    // are handed out after a season ends. Without this the view is a dead end
    // from the end of one season until the next lot are published.
    const {html} = view(1995);
    assert.match(html, /href="#section=stats&amp;view=awards&amp;season=2024"/);
    assert.match(html, /Show 2024/);
  });

  await t.test('the door points at the LAST covered season, not the first', () => {
    // 2023 is also covered, and offering it would be a link to older data
    // than the archive has.
    const {html} = view(1995);
    assert.ok(!/Show 2023/.test(html), 'must not offer the earliest season');
    assert.ok(!/season=2023"/.test(html), 'must not link the earliest season');
  });

  await t.test('a covered season shows no door at all', () => {
    // The link belongs to the empty state. On a season with winners it would
    // be an invitation to leave the page the reader just asked for.
    const {html} = view(2023);
    assert.ok(!/Show 20\d\d/.test(html), 'no door on a season with winners');
  });

  await t.test('a build with no awards file never asks for one', () => {
    const {ctx} = view(2024, {manifest: null});
    const html = ctx.render();
    assert.match(html, /holds no award winners/);
    assert.ok(!html.includes('awards-body'),
      'no body to hydrate means hydrateAwards cannot fetch');
  });

  await t.test('a build that has one renders a body for the loader to fill', () => {
    const {ctx} = view(2024);
    assert.match(ctx.render(), /id="awards-body"/);
  });
});

test('the awards panel on a player page', async t => {
  await t.test('a player with no trophies gets no panel at all', () => {
    const {ctx} = view(2024);
    assert.equal(ctx.panel(DATA, 999999), '', 'no heading on a page with nothing under it');
    assert.equal(ctx.panel(null, 4685415), '', 'nor when the archive did not load');
    assert.equal(ctx.panel({awards: [], winners: [], by_player: {}}, 4685415), '');
  });

  await t.test('a player with trophies gets one row for each', () => {
    const {ctx} = view(2024);
    const html = ctx.panel(DATA, 4685415);
    const body = html.slice(html.indexOf('<tbody>'), html.indexOf('</tbody>'));
    assert.equal(body.split('<tr>').length - 1, 2);
    assert.ok(html.includes('Heisman Memorial Trophy'));
    assert.ok(html.includes('Fred Biletnikoff Award'));
    assert.ok(html.includes('Outstanding Receiver'), 'what the trophy is for');
  });

  await t.test('it shows only his own trophies', () => {
    const {ctx} = view(2024);
    const html = ctx.panel(DATA, 4685001);
    assert.ok(html.includes('Lombardi Award'));
    assert.ok(!html.includes('Heisman'), 'another player’s trophy is not his');
    assert.ok(!html.includes('A Second Winner'), 'nor is a co-winner’s name');
  });

  await t.test('a career reads newest first', () => {
    const {ctx} = view(2024);
    const data = {...DATA, by_player: {4685415: [0, 6]}};   // 2024 and a 2023 trophy
    const html = ctx.panel(data, 4685415);
    const body = html.slice(html.indexOf('<tbody>'), html.indexOf('</tbody>'));
    assert.ok(body.indexOf('2024') < body.indexOf('2023'),
      'the most recent season first, as a career is read');
  });

  await t.test('it points at the view that holds every winner', () => {
    const {ctx} = view(2024);
    assert.match(ctx.panel(DATA, 4685415), /href="#section=stats&view=awards"/);
  });
});
