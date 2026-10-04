// A person CFBD recorded under two athlete ids, with both records read by hand
// and judged one person. Neither id is rewritten, so the only thing that makes
// the career readable is each page naming the other. These tests drive the real
// functions out of the shell rather than asserting on its source text.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const shell = fs.readFileSync('ui/dashboard_shell.html', 'utf8');

function load(names, extra) {
  const source = names.map(name =>
    shell.match(new RegExp('function ' + name + '\\([^]*?\\n\\}'))[0]).join('\n');
  const context = Object.assign({
    esc: value => String(value).replaceAll('&', '&amp;').replaceAll('<', '&lt;'),
    playerHref: id => '#player=' + id,
    teamCellById: id => 'TEAM' + id,
    personStatsNote: () => '<p>NO STATS</p>',
    statCell: value => String(value),
    FRACTION_STATS: {PCT: true},
    STAT_CATEGORY_LABELS: {rushing: 'Rushing'},
    STAT_CATEGORY_ORDER: ['rushing'],
  }, extra);
  vm.runInNewContext(source + '\nthis.__fns = {' + names.join(',') + '};', context);
  return context.__fns;
}

test('a split career names the page that holds the rest of it', () => {
  const {samePersonNote} = load(['samePersonNote']);
  const html = samePersonNote({same_person: {id: 4052841, name: 'Delvon Randall', season: 2016}});
  assert.match(html, /href="#player=4052841"/);
  assert.match(html, /Delvon Randall/);
  assert.match(html, /2016/);
  // The reader is told the ids are not merged, so a short total is explained
  // rather than quietly standing as the whole career.
  assert.match(html, /not merged/);
});

test('a player with no counterpart gets no panel at all', () => {
  const {samePersonNote} = load(['samePersonNote']);
  for (const player of [{}, null, {same_person: null}, {same_person: {id: null}}]) {
    assert.equal(samePersonNote(player), '');
  }
});

test('the partial-season dagger points at the other id when there is one', () => {
  const {playerStatsPanel} = load(['playerStatsPanel']);
  const stats = {2022: {rushing: {team_id: 3, CAR: 17, YDS: 41}, _partial: 'two ids'}};

  const linked = playerStatsPanel(stats, {id: 4052841, name: 'Delvon Randall', season: 2016});
  assert.match(linked, /href="#player=4052841"/, 'the note must say where the rest is');

  // Unlinked -- the 9 hometown contradictions and the one pair who are two
  // people -- keeps the caveat and offers no link to follow.
  const alone = playerStatsPanel(stats, undefined);
  assert.match(alone, /part of/);
  assert.ok(!alone.includes('#player='), 'an unlinked caveat must not invent a counterpart');
});
