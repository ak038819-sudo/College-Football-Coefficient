const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const {createResolver} = require('../ui/live_teams.js');
const DATA = JSON.parse(fs.readFileSync('ui/dashboard_data.json', 'utf8'));
const teams = DATA.teams;
const resolve = createResolver(teams);
const shell = fs.readFileSync('ui/dashboard_shell.html', 'utf8');
const fn = name => shell.match(new RegExp('function ' + name + '\\([^)]*\\) \\{[\\s\\S]*?\\n\\}'))[0];
// The real artwork registry, not a stub: liveTeam reads it to decide whether a
// row gets a team panel instead of a logo watermark.
const teamPanels = shell.match(/^const TEAM_PANELS = Object\.freeze\(\{[\s\S]*?^\}\);/m);
if (!teamPanels) throw new Error('missing TEAM_PANELS in the shell');
const panelSource = teamPanels[0] + '\n' + fn('teamPanelStyle');

test('every supported provider identity and alias resolves; all other provider full names stay unmatched', () => {
  const registry = JSON.parse(fs.readFileSync('data/reference/team_identities.json', 'utf8')).teams;
  const directory = JSON.parse(fs.readFileSync('data/reference/espn_team_directory.json', 'utf8')).teams;
  const providerResolve = createResolver(teams, 'espn');
  const supported = new Map(registry.map(r => [String(r.provider_ids.espn), r]));
  for (const row of registry) {
    for (const name of [row.canonical_name, ...row.aliases]) {
      assert.equal(resolve({name}).name, row.canonical_name, name);
    }
    assert.equal(providerResolve({id: row.provider_ids.espn, name: row.espn_display_name}).name, row.canonical_name);
  }
  for (const row of directory) {
    const match = providerResolve({id: row.id, name: row.displayName});
    assert.equal(match?.name || null, supported.get(String(row.id))?.canonical_name || null, row.displayName);
  }
  assert.equal(providerResolve({id: 57, name: 'Texas Longhorns'}), null);
  assert.equal(providerResolve({id: 251, name: 'Florida Gators'}), null);
});

test('Texas Southern at Florida Atlantic never becomes Texas at Florida', () => {
  assert.equal(resolve({id: 2640, name: 'Texas Southern Tigers', classification: 'fcs'}), null);
  assert.equal(resolve({id: 2226, name: 'Florida Atlantic Owls'}).name, 'FAU');
  assert.equal(resolve({name: 'Texas Longhorns'}).name, 'Texas');
  assert.equal(resolve({name: 'Florida Gators'}).name, 'Florida');
});

test('unknown suffixes, shared prefixes, and provider ID collisions cannot borrow identities', () => {
  for (const name of ['Texas Southern', 'Alabama State Hornets', 'Alabama A&M Bulldogs',
    'North Carolina Central Eagles', 'South Carolina State Bulldogs', 'Howard Bison',
    'Texas Unknown Mascot', 'Florida Atlantic Unknown Mascot', '']) {
    assert.equal(resolve({id: 47, name}), null, name);
  }
  assert.equal(resolve(null), null);
  assert.equal(resolve({id: 47}), null);
  assert.equal(createResolver(teams.filter(t => t.name !== 'FAU'))({name: 'Florida Atlantic Owls'}), null);
});

test('known aliases and full mascot names preserve distinct school identities', () => {
  for (const [name, expected] of [
    ['Florida International Panthers', 'FIU'], ['Florida State Seminoles', 'Florida State'],
    ['Texas Tech Red Raiders', 'Texas Tech'], ['Texas A&M Aggies', 'Texas A&M'],
    ['Miami Hurricanes', 'Miami (FL)'], ['Miami (OH) RedHawks', 'Miami (OH)'],
    ['UMass Minutemen', 'Massachusetts'], ['UL Monroe Warhawks', 'ULM'],
    ['Hawaii Rainbow Warriors', "Hawai'i"], ['  RUTGERS   SCARLET KNIGHTS ', 'Rutgers'],
    ["Louisiana Ragin’ Cajuns", 'Louisiana']
  ]) assert.equal(resolve({name}).name, expected, name);
  for (const team of teams) assert.equal(resolve({name: team.name}), team);
});

test('the current FBS feed resolves exactly; FCS opponents remain unmatched', () => {
  const snapshot = JSON.parse(fs.readFileSync('ui/data/live_scores.json', 'utf8'));
  for (const game of snapshot.games) for (const side of [game.home, game.away]) {
    if (side.classification === 'fbs') assert.ok(resolve(side), side.name);
    else assert.equal(resolve(side), null, side.name);
  }
});

test('unknown teams retain their feed name and cannot receive borrowed Elo predictions', () => {
  const context = {
    liveTeamId: resolve, UPCOMING_BY_ID: new Map(), liveSeasonData: null,
    CURRENT_ELO: new Map(teams.map(t => [t.id, {elo: 1800, rank: 4}])),
    STATIC_MANIFEST: {model_params: {elo: {scale: 400, home_field: 55}}},
    pickDatedLogoEntry: () => null, DATED_LOGOS: {}, TEAM_BRAND_COLORS: {},
    // The feed names an FCS school ESPN's way, so its mark is filed under the
    // ESPN id rather than that name: Texas Southern's id, not 'Texas Southern
    // Tigers'. The real index, so a card that drew the wrong school's mark, or
    // none, cannot pass here.
    NON_FBS_BY_ESPN_ID: new Map([['2640', 'Texas Southern']]),
    liveSeason: () => 2026, logoMarkup: name => '[' + name + ']',
    teamAnchor: (_, text) => text, esc: value => value, liveChange: () => '',
    // The real record helper, not a stub: a card that showed the wrong team's
    // record, or none, would otherwise still pass here.
    DATA, TEAM_BY_NAME: new Map(teams.map(t => [t.name, t]))
  };
  vm.runInNewContext(panelSource + '\n' + fn('livePrediction') + '\n' + fn('seasonRecord') + '\n' +
    fn('liveLogoName') + '\n' + fn('liveTeam') +
    '\nthis.predict = livePrediction; this.renderTeam = liveTeam;' +
    '\nthis.logoName = liveLogoName;', context);
  const game = {id: 401862790, status: 'scheduled',
    away: {id: 2640, name: 'Texas Southern Tigers', classification: 'fcs'},
    home: {id: 2226, name: 'Florida Atlantic Owls'}};
  assert.equal(context.predict(game), null);
  const html = context.renderTeam(game.away, game, null);
  assert.match(html, /Texas Southern Tigers/);
  // The card keeps the feed's name, and carries the school's own mark, found by
  // the id the non-FBS set filed it under.
  assert.match(html, /\[Texas Southern\]/);
  assert.equal(context.logoName(game.away, null), 'Texas Southern');
  // A school with no mark at all falls back to the feed name, which finds
  // nothing and draws the monogram, as it did before the non-FBS set existed.
  assert.equal(context.logoName({id: 9999, name: 'Lambuth Eagles'}, null), 'Lambuth Eagles');
  // A team this project holds is unaffected: its own name wins over any id.
  assert.equal(context.logoName({id: 2640, name: 'whatever'}, {name: 'FAU'}), 'FAU');
  assert.match(html, /Elo unrated/);
  assert.match(html, /N\/A/);
  assert.doesNotMatch(html, /live-rank|1800|Longhorns/);
  // A team the feed does not resolve has no record to show, and must not borrow one.
  assert.doesNotMatch(html, /\d+\u2013\d+/);
  const fau = teams.find(t => t.name === 'FAU');
  const rated = (DATA.team_records_by_year['2026'] || {})[fau.id] || [0, 0, 0];
  const unrated = (DATA.non_fbs_records_by_year['2026'] || {})[fau.id] || [0, 0, 0];
  const expected = (rated[0] + unrated[0]) + '\u2013' + (rated[1] + unrated[1]);
  assert.match(context.renderTeam(game.home, game, null), new RegExp(expected));
});

test('a panel team carries its artwork instead of a logo watermark; every other team keeps the watermark', () => {
  const context = {
    liveTeamId: resolve, UPCOMING_BY_ID: new Map(), liveSeasonData: null,
    CURRENT_ELO: new Map(), STATIC_MANIFEST: {model_params: {elo: {scale: 400, home_field: 55}}},
    pickDatedLogoEntry: (_, name) => ({src: 'logos/' + name + '.png'}),
    DATED_LOGOS: {}, TEAM_BRAND_COLORS: {},
    liveSeason: () => 2026, logoMarkup: name => '[' + name + ']',
    teamAnchor: (_, text) => text, esc: value => value, liveChange: () => '',
    NON_FBS_BY_ESPN_ID: new Map(),
    DATA, TEAM_BY_NAME: new Map(teams.map(t => [t.name, t]))
  };
  vm.runInNewContext(panelSource + '\n' + fn('livePrediction') + '\n' + fn('seasonRecord') + '\n' +
    fn('liveLogoName') + '\n' + fn('liveTeam') +
    '\nthis.renderTeam = liveTeam; this.panels = TEAM_PANELS;', context);
  const panelTeam = Object.keys(context.panels)[0];
  assert.ok(teams.some(t => t.name === panelTeam), panelTeam + ' is not a team in the data');
  const plainTeam = teams.find(t => !context.panels[t.name]).name;
  const game = {id: 1, status: 'scheduled', home: {name: panelTeam}, away: {name: plainTeam}};

  const panelled = context.renderTeam(game.home, game, null);
  assert.match(panelled, /class="live-team team-panel/);
  assert.match(panelled, new RegExp('--panel-art:url\\(\'' + context.panels[panelTeam].image + '\'\\)'));
  assert.doesNotMatch(panelled, /live-watermark/);

  const plain = context.renderTeam(game.away, game, null);
  assert.doesNotMatch(plain, /team-panel|--panel-art/);
  assert.match(plain, /live-watermark/);
});

test('every registered panel names a real team and an asset that exists', () => {
  const context = {};
  vm.runInNewContext(panelSource + '\nthis.panels = TEAM_PANELS;', context);
  const names = new Set(teams.map(t => t.name));
  const entries = Object.entries(context.panels);
  assert.ok(entries.length > 0, 'the registry is empty');
  for (const [name, panel] of entries) {
    // A key that is not a team name is not an error anywhere: the row simply
    // never finds its artwork, which is only visible by looking at the page.
    assert.ok(names.has(name), name + ' is not a team in the data');
    assert.ok(fs.existsSync('ui/' + panel.image), panel.image + ' is not in the repository');
  }
});
