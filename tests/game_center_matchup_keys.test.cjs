// Keys to the Matchup must only summarize the published rated scouting evidence.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const shell = fs.readFileSync('ui/dashboard_shell.html', 'utf8');
const source = shell.match(/function gameMatchupKeys\([^]*?\n\}/)?.[0];
assert.ok(source, 'Keys helper must exist in the production shell');
const context = {
  SCOUTING_TRAITS: [
    ['rush_success_rate','Rush success'], ['rush_ppa','Rush PPA'],
    ['rush_explosiveness','Rush explosiveness'], ['pass_success_rate','Pass success'],
    ['pass_ppa','Pass PPA'], ['pass_explosiveness','Pass explosiveness']
  ],
  MATCHUP_EDGE_CLEAR:12,
  esc: v => String(v).replaceAll('&','&amp;').replaceAll('<','&lt;')
    .replaceAll('>','&gt;').replaceAll('"','&quot;'),
  scoutingOrdinal: v => Math.round(v)+'th',
};
vm.runInNewContext(source+'\nthis.keys = gameMatchupKeys;',context);
const rated = (off,def) => ({
  status:'rated', offense_percentile:off, defense_percentile:def
});
test('top ranked contrast is a measured percentile gap, not a projected outcome',()=>{
 const away={rush_success_rate:rated(86,51),rush_ppa:rated(78,59),pass_ppa:rated(70,62)};
 const home={rush_success_rate:rated(44,42),rush_ppa:rated(51,58),pass_ppa:rated(58,90)};
 const html=context.keys('Away','Home',away,home);
 assert.match(html,/Away offense/);
 assert.match(html,/Rush success/);
 assert.match(html,/44-point national percentile difference/);
 assert.match(html,/Home defense/);
 assert.doesNotMatch(html,/will win|points scored|projected score/i);
});
test('two directions represented when both have clear rated contrasts',()=>{
 const away={rush_success_rate:rated(85,92)};
 const home={rush_success_rate:rated(50,49)};
 const html=context.keys('Away','Home',away,home);
 assert.match(html,/Away offense/);
 assert.match(html,/Away defense/);
 assert.equal((html.match(/class="gp-key"/g)||[]).length,2);
});
test('no fabricated key when sample ratings are missing, weak or nonfinite',()=>{
 const away={rush_ppa:rated(83,30),pass_ppa:rated(NaN,91),pass_success_rate:rated(50,50)};
 const home={rush_ppa:{status:'insufficient_sample',offense_percentile:10,defense_percentile:22},
   pass_ppa:rated(5,5),pass_success_rate:rated(51,51)};
 const html=context.keys('Away','Home',away,home);
 assert.match(html,/No clear, fully rated/);
 assert.doesNotMatch(html,/class="gp-key"/);
});
test('escapes team names and never uses a bare unrated percentile',()=>{
 const away={pass_success_rate:rated(81,42)};
 const home={pass_success_rate:rated(30,72)};
 const html=context.keys('<Away>','Home & Guests',away,home);
 assert.match(html,/&lt;Away&gt;/);
 assert.match(html,/Home &amp; Guests/);
 assert.doesNotMatch(html,/<Away>|NaN|undefined/);
});
test('summary remains inside same date-gated pregame scouting panel',()=>{
 const panel = shell.match(/function gameScoutingPanel\([^]*?\n\}/)?.[0];
 assert.ok(panel);
 assert.match(panel,/gameScoutingEligible\(g, season, bundle\)/);
 assert.match(panel,/gameMatchupKeys\(an, hn, away\.metrics, home\.metrics\)/);
 assert.match(shell,/if \(!g\.completed\) hydrateGameScouting/);
});
