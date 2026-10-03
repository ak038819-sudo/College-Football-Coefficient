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
const ctx = {esc: value => String(value).replaceAll('&', '&amp;').replaceAll('<', '&lt;'),
  stadiumHref: id => `#section=teams&view=stadiums&stadium=${id}`};
vm.runInNewContext(sourceOf('gameStory') + '\n' + sourceOf('storyPlayerLeaders') +
  '\nthis.story = gameStory; this.leaders = storyPlayerLeaders;', ctx);

test('completed game story uses stored outcome, venue, SR+ and Elo change', () => {
  const g = {completed: true, home_score: 20, away_score: 27, p_home: 0.8,
    stadium_id: 42, stadium: 'The Ground', home_elo_change: -18.2};
  const html = ctx.story(g, {home_sr_plus: -0.08, away_sr_plus: 0.08}, 2025, 'Visitors', 'Hosts');
  assert.match(html, /Visitors beat Hosts, 27–20.*20% pregame win chance/);
  assert.match(html, /The Ground<\/a>/);
  assert.match(html, /Visitors \+8\.0 pp/);
  assert.match(html, /Visitors \+18\.2/);
  assert.match(html, /#gp-efficiency/);
});

test('missing stats stay absent, and upcoming game shows stored win chance', () => {
  const g = {completed: false, neutral: true, p_home: 0.65, stadium: 'Unverified Park', home_elo_change: null};
  const html = ctx.story(g, null, 2026, 'Visitors', 'Hosts');
  assert.match(html, /Hosts 65%/);
  assert.match(html, /Unverified Park \(venue unverified\)/);
  assert.doesNotMatch(html, /#gp-efficiency|#gp-players|Explore the stadium/);
  assert.match(html, /Efficiency edge<\/small><strong>Unavailable/);
  assert.match(ctx.story({...g, completed: true, home_score: 7, away_score: 7},
    {home_sr_plus: 0, away_sr_plus: 0}, 2026, 'Visitors', 'Hosts'),
    /Efficiency edge<\/small><strong>Even/);
});

test('player leaders use only numeric yardage from published categories', () => {
  const html = ctx.leaders([
    {name: 'Hosts', categories: [{name: 'Passing', type: 'YDS', lines: [
      {name: 'A', stat: '230'}, {name: 'B', stat: '180'}]},
    {name: 'Passing', type: 'TD', lines: [{name: 'C', stat: '4'}]}]},
    {name: 'Visitors', categories: [{name: 'Passing', type: 'YDS', lines: [{name: 'D', stat: '301'}]},
      {name: 'Rushing', type: 'YDS', lines: [{name: '<Runner>', stat: '115'}]}]},
  ]);
  assert.match(html, /D<\/strong><p>Visitors · 301 yards/);
  assert.match(html, /&lt;Runner>/);
  assert.doesNotMatch(html, /C<\/strong>|Receiving<\/small>/);
  assert.match(ctx.leaders([]), /not available/);
});

test('active game story waits for the official model update', () => {
  const live = {esc: ctx.esc, stadiumHref: ctx.stadiumHref, storyPlayerLeaders: ctx.leaders,
    liveSnapshot: {player_boxscores: {}}, liveSeasonData: {byId: new Map()},
    UPCOMING_BY_ID: new Map(), livePrediction: () => ({pHome: 0.7}),
    probPct: () => ({home: 70, away: 30}), liveTeamId: t => ({name: t.name}),
    liveSeason: () => 2026, liveLabel: () => 'Final', teamLink: name => name,
    liveEloPanel: () => 'Elo table', livePlayerPanel: () => 'Player box',
  };
  vm.runInNewContext(sourceOf('liveDetail') + '\nthis.detail = liveDetail;', live);
  const html = live.detail({id: 12, status: 'completed', venue: 'Sample Field',
    away: {name: 'Visitors', points: 27}, home: {name: 'Hosts', points: 20}});
  assert.match(html, /Game Story/);
  assert.match(html, /Visitors beat Hosts, 27–20/);
  assert.match(html, /Model update<\/small><strong>Pending/);
  assert.match(html, /Sample Field \(venue unverified\)/);
});

// ---- Linking a box-score name to a player page (v0.1.1) ---------------------
// The archive carries no player id, so a link rests on name + team + season.
// These cover the one rule that keeps that honest: exactly one candidate, or no
// link at all.

test('a box-score name links only when it matches exactly one player on that roster', () => {
  const source = ['rosterNameLookup', 'boxScoreNameLookup', 'boxScorePlayerCell']
    .map(name => {
      const fn = shell.match(new RegExp('function ' + name + '\\([^]*?\\n\\}'))?.[0];
      assert.ok(fn, name + ' exists');
      return fn;
    }).join('\n');
  const context = {
    esc: value => String(value).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('"', '&quot;'),
    CfbNavigation: { normalizeSearch: require('../ui/navigation.js').normalizeSearch },
    TEAM_BY_NAME: new Map([['TCU', { id: 38 }], ['North Carolina', { id: 153 }]]),
    playerHref: id => '#player=' + id
  };
  vm.runInNewContext(source + '\nthis.lookup = boxScoreNameLookup; this.cell = boxScorePlayerCell;', context);

  const roster = { fields: [], teams: new Map([[38, [
    { player_id: 1, name: 'Ed Small' },
    { player_id: 2, name: "Na'eem Offord" },
    { player_id: 3, name: 'Mike Williams' },
    { player_id: 4, name: 'Mike Williams' },     // two real people, one name
    // The source's own text is what gets displayed, so the LINKED branch has to
    // escape it too -- not only the unlinked fallback.
    { player_id: 5, name: 'Sam <b>Ash' }
  ]]]) };

  const tcu = context.lookup(roster, 'TCU');
  assert.equal(context.cell('Ed Small', tcu), '<a href="#player=1">Ed Small</a>');
  // Punctuation and case are folded on both sides, the way search folds them.
  assert.equal(context.cell('naeem offord', tcu), '<a href="#player=2">naeem offord</a>');
  // Two candidates is a collision, not a tie-break: no link, just the name.
  assert.equal(context.cell('Mike Williams', tcu), 'Mike Williams');
  // A name nobody on the roster has: the walk-on added after the snapshot.
  assert.equal(context.cell('Gil Jackson', tcu), 'Gil Jackson');
  // The team-total row CFBD puts in every category.
  assert.equal(context.cell(' Team', tcu), ' Team');
  // And a name is escaped whether or not it becomes a link.
  assert.equal(context.cell('<script>', tcu), '&lt;script>');
  assert.equal(context.cell('Sam <b>Ash', tcu), '<a href="#player=5">Sam &lt;b>Ash</a>');
});

test('no roster, an unknown school, or an empty roster means no link at all', () => {
  const source = ['rosterNameLookup', 'boxScoreNameLookup', 'boxScorePlayerCell']
    .map(name => shell.match(new RegExp('function ' + name + '\\([^]*?\\n\\}'))[0]).join('\n');
  const context = {
    esc: String, CfbNavigation: { normalizeSearch: require('../ui/navigation.js').normalizeSearch },
    TEAM_BY_NAME: new Map([['TCU', { id: 38 }]]), playerHref: id => '#player=' + id
  };
  vm.runInNewContext(source + '\nthis.lookup = boxScoreNameLookup; this.cell = boxScorePlayerCell;', context);
  const roster = { teams: new Map([[38, [{ player_id: 1, name: 'Ed Small' }]], [99, []]]) };

  // A season with no roster file: the whole feature is simply absent.
  assert.equal(context.lookup(null, 'TCU'), null);
  // A school outside this FBS-only dataset.
  assert.equal(context.lookup(roster, 'Alabama A&M'), null);
  // A school in the dataset with no roster row for that season.
  assert.equal(context.lookup({ teams: new Map([[38, []]]) }, 'TCU'), null);
  for (const lookup of [null, undefined]) {
    assert.equal(context.cell('Ed Small', lookup), 'Ed Small');
  }
});

// ---- Attribution, once the archive carries athlete ids ----------------------
// The re-fetch's whole point. An id is the SOURCE saying whose line this is; a
// name matching one roster row is this project's own inference. The page has to
// keep those apart, and prefer the first.

test('a numbered box-score line is attributed by its id, not matched by name', () => {
  const source = ['rosterNameLookup', 'rosterIdSet', 'boxScoreNameLookup',
                  'boxScorePlayerCell', 'boxScoreLineCell']
    .map(name => {
      const fn = shell.match(new RegExp('function ' + name + '\\([^]*?\\n\\}'))?.[0];
      assert.ok(fn, name + ' exists');
      return fn;
    }).join('\n');
  const context = {
    esc: value => String(value).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('"', '&quot;'),
    CfbNavigation: { normalizeSearch: require('../ui/navigation.js').normalizeSearch },
    TEAM_BY_NAME: new Map([['TCU', { id: 38 }]]),
    playerHref: id => '#player=' + id
  };
  vm.runInNewContext(source + '\nthis.cell = boxScoreLineCell; this.ids = rosterIdSet;' +
    '\nthis.names = boxScoreNameLookup;', context);

  const roster = { fields: [], teams: new Map([[38, [
    { player_id: 4361182, name: 'Numbered Passer' },
    { player_id: 3, name: 'Mike Williams' },
    { player_id: 4, name: 'Mike Williams' },        // two real people, one name
    { player_id: -1044305, name: 'Placeholder Id' },
    { player_id: 9, name: 'Sam <b>Ash' }
  ]]]) };
  const idSet = context.ids(roster, 38);
  const lookup = context.names(roster, 'TCU');

  // Numbered: the source's own attribution.
  const numbered = context.cell({ name: 'Numbered Passer', stat: '288', id: '4361182' },
                                lookup, idSet);
  assert.equal(numbered.attributed, true);
  assert.match(numbered.html, /#player=4361182/);

  // An id beats a name collision. Two Mike Williamses leave the name rule with
  // nothing, but CFBD numbering the line settles it.
  const settled = context.cell({ name: 'Mike Williams', stat: '7', id: '4' }, lookup, idSet);
  assert.equal(settled.attributed, true);
  assert.match(settled.html, /#player=4"/);

  // Unnumbered: the old contextual rule, and still only an inference.
  const unnumbered = context.cell({ name: 'Sam <b>Ash', stat: '1' }, lookup, idSet);
  assert.equal(unnumbered.attributed, false);
  assert.equal(unnumbered.matched, true);
  assert.match(unnumbered.html, /#player=9/);
  // The source's text is displayed either way, so the linked branch escapes it.
  assert.match(unnumbered.html, /Sam &lt;b/);
  assert.doesNotMatch(unnumbered.html, /<b>/);

  // An unnumbered shared name is still left alone rather than guessed at.
  const shared = context.cell({ name: 'Mike Williams', stat: '2' }, lookup, idSet);
  assert.equal(shared.attributed, false);
  assert.equal(shared.matched, false);
  assert.equal(shared.html, 'Mike Williams');

  // A negative id is a real CFBD athlete id and must link.
  const negative = context.cell({ name: 'Placeholder Id', stat: '3', id: '-1044305' },
                                lookup, idSet);
  assert.equal(negative.attributed, true);
  assert.match(negative.html, /#player=-1044305/);
});

test('an id for somebody this dataset has no page for is not linked', () => {
  // Rosters start in 2009, so a 2004 line can carry a perfectly good CFBD id
  // for a person with no page here. Linking it would send a reader to "player
  // not found", which is worse than plain text.
  const source = ['rosterNameLookup', 'rosterIdSet', 'boxScoreNameLookup',
                  'boxScorePlayerCell', 'boxScoreLineCell']
    .map(name => shell.match(new RegExp('function ' + name + '\\([^]*?\\n\\}'))[0]).join('\n');
  const context = {
    esc: String,
    CfbNavigation: { normalizeSearch: require('../ui/navigation.js').normalizeSearch },
    TEAM_BY_NAME: new Map([['TCU', { id: 38 }]]),
    playerHref: id => '#player=' + id
  };
  vm.runInNewContext(source + '\nthis.cell = boxScoreLineCell; this.ids = rosterIdSet;' +
    '\nthis.names = boxScoreNameLookup;', context);

  const roster = { fields: [], teams: new Map([[38, [{ player_id: 7, name: 'On The Roster' }]]]) };
  const idSet = context.ids(roster, 38);
  const lookup = context.names(roster, 'TCU');

  const stranger = context.cell({ name: 'Not In This Dataset', stat: '40', id: '999999' },
                                lookup, idSet);
  assert.equal(stranger.attributed, false);
  assert.equal(stranger.matched, false);
  assert.equal(stranger.html, 'Not In This Dataset');

  // With no roster for the season at all, nothing links and nothing throws.
  const noRoster = context.cell({ name: 'On The Roster', stat: '1', id: '7' }, null, null);
  assert.equal(noRoster.attributed, false);
  assert.equal(noRoster.html, 'On The Roster');
  assert.equal(context.ids(null, 38), null);
  assert.equal(context.ids(roster, 99), null);
});
