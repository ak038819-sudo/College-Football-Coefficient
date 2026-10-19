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

test('the panel scrim never thins to where artwork can swallow the labels', () => {
  // Measured, not guessed: with the scrim's middle stop at 10% navy, the names
  // and ratings on Tennessee's and Kentucky's panels came out at 1.0:1 against
  // their own checkerboards -- the same luminance as the artwork behind them.
  // 50% is the floor that keeps every label on all eight panels above 4.5:1.
  const rule = shell.match(/\.live-team\.team-panel[\s\S]*?\n  \}/)?.[0];
  assert.ok(rule, 'missing the shared panel background rule');
  const stops = [...rule.matchAll(/#[0-9a-f]{6}([0-9a-f]{2})/g)].map(m => parseInt(m[1], 16) / 255);
  assert.ok(stops.length >= 3, 'expected the scrim to be a gradient of at least three stops');
  for (const alpha of stops) {
    assert.ok(alpha >= 0.5, 'a scrim stop at ' + Math.round(alpha * 100) + '% cannot cover bright artwork');
  }
});

test('the narrow-width scrim is darker, because a phone row crops the artwork differently', () => {
  // A 390px row shows a different part of the same picture and the label runs
  // further across the scrim, so the wide-screen middle stop is not enough:
  // measured in a browser at 390x844, Tennessee's and Kentucky's names came out
  // at 4.19:1 and their Elo ratings at 3.64:1 and 3.59:1 with the middle stop
  // at 55%. 65% is the lightest middle that holds all forty labels above
  // 4.5:1 there; 60% leaves the worst at 4.14:1.
  const styles = shell.slice(0, shell.indexOf('</style>'));
  // Balanced braces, not a lazy match: this stylesheet has one-line media
  // queries, and a regex that stops at the first closing brace on its own line
  // runs straight past them into unrelated rules.
  const queries = [];
  for (const m of styles.matchAll(/@media\s*\(max-width:\s*(\d+)px\)\s*\{/g)) {
    let depth = 1, i = m.index + m[0].length;
    for (; i < styles.length && depth; i++) {
      if (styles[i] === '{') depth++;
      else if (styles[i] === '}') depth--;
    }
    queries.push([null, m[1], styles.slice(m.index + m[0].length, i - 1)]);
  }
  const narrow = queries.find(m => /\.live-team\.team-panel[\s\S]*?background-image:[^;]*--panel-art/.test(m[2]));
  assert.ok(narrow, 'no narrow-width rule darkens the panel scrim, so a phone keeps the wide-screen one');
  assert.ok(Number(narrow[1]) >= 700, 'the narrow scrim starts at ' + narrow[1] + 'px, which leaves phone widths on the wide one');
  const stops = [...narrow[2].matchAll(/#[0-9a-f]{6}([0-9a-f]{2})/g)].map(m => parseInt(m[1], 16) / 255);
  assert.ok(stops.length >= 3, 'expected the narrow scrim to be a gradient of at least three stops');
  const middle = Math.min(...stops.slice(1, -1));
  assert.ok(middle >= 0.65, 'a narrow-width middle stop at ' + Math.round(middle * 100) +
    '% measured below 4.5:1 on the two checkerboard panels');
});

test('panel labels carry their own light ink, and the cascade lets them keep it', () => {
  const styles = shell.slice(0, shell.indexOf('</style>'));
  const rules = [...styles.matchAll(/([^{}]+)\{([^}]*)\}/g)].map(m => ({sel: m[1].trim(), body: m[2], at: m.index}));
  const panelRule = rules.find(x => x.sel.includes('.team-panel .team-link-name') && /(^|[;{\s])color:/.test(x.body));
  assert.ok(panelRule, 'no rule gives panel labels a colour of their own');

  // The gold link colour is mid-luminance, so it loses against the bright half
  // of a two-tone panel however dark the scrim is: Tennessee's name measured
  // 1.0:1 in gold and 7.8:1 in near-white over the same artwork.
  const hex = panelRule.body.match(/color:\s*#([0-9a-f]{6})/)?.[1];
  assert.ok(hex, 'the panel label colour is not a plain hex this test can weigh');
  const channel = v => { v /= 255; return v <= 0.03928 ? v / 12.92 : ((v + 0.055) / 1.055) ** 2.4; };
  const [r, g, b] = [0, 2, 4].map(i => parseInt(hex.slice(i, i + 2), 16));
  const luminance = 0.2126 * channel(r) + 0.7152 * channel(g) + 0.0722 * channel(b);
  assert.ok(luminance > 0.6, 'ink at luminance ' + luminance.toFixed(2) + ' is too dark to read over artwork');

  // And the half a source-text assertion cannot see: a colour declared here for
  // a label that a LATER rule also colours is thrown away by the cascade, which
  // reads as protection that is not there. Three such declarations were written
  // and silently discarded before a browser showed the computed colour had not
  // moved off the gold.
  const finals = panelRule.sel.split(',').map(s => s.trim().split(/\s+/).pop());
  for (const later of rules) {
    if (later.at <= panelRule.at || !/(^|[;{\s])color:/.test(later.body)) continue;
    for (const final of finals) {
      const beaten = later.sel.split(',').some(s => s.trim().split(/\s+/).pop() === final);
      assert.ok(!beaten, 'a later rule (' + later.sel.trim().slice(0, 60) + ') re-colours ' + final +
        ', so the panel ink for it never applies');
    }
  }
});
