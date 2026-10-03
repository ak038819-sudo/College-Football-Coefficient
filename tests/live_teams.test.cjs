const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const {createResolver} = require('../ui/live_teams.js');
const teams = JSON.parse(fs.readFileSync('ui/dashboard_data.json', 'utf8')).teams;
const resolve = createResolver(teams);
const shell = fs.readFileSync('ui/dashboard_shell.html', 'utf8');
const fn = name => shell.match(new RegExp('function ' + name + '\\([^)]*\\) \\{[\\s\\S]*?\\n\\}'))[0];

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
    liveSeason: () => 2026, logoMarkup: name => '[' + name + ']',
    teamAnchor: (_, text) => text, esc: value => value, liveChange: () => ''
  };
  vm.runInNewContext(fn('livePrediction') + '\n' + fn('liveTeam') +
    '\nthis.predict = livePrediction; this.renderTeam = liveTeam;', context);
  const game = {id: 401862790, status: 'scheduled',
    away: {name: 'Texas Southern Tigers'}, home: {name: 'Florida Atlantic Owls'}};
  assert.equal(context.predict(game), null);
  const html = context.renderTeam(game.away, game, null);
  assert.match(html, /Texas Southern Tigers/);
  assert.match(html, /Elo unrated/);
  assert.match(html, /N\/A/);
  assert.doesNotMatch(html, /live-rank|1800|Longhorns/);
});
