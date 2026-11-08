// Regression checks for the analytics-first pregame presentation.
// Run: node --test tests/pregame_intelligence.test.cjs
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const shell = fs.readFileSync('ui/dashboard_shell.html', 'utf8');
const match = shell.match(/function gamePregameIntelligence\([^]*?\n\}/);
assert.ok(match, 'real Game Center helper exists');
const ctx = {
  esc: x => String(x).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;'),
  gameBarColor: () => '#304250',
  eloValue: v => Math.round(v).toString(),
  probPct: p => ({home: Math.round(p*100), away: 100-Math.round(p*100)})
};
vm.runInNewContext(match[0] + '\nthis.show = gamePregameIntelligence;',ctx);
test('uses stored model probabilities, venue explanation and pregame Elo',()=>{
 const html=ctx.show({p_home:.49,away_pre_elo:1887,home_pre_elo:1830},'Miami (FL)','Notre Dame');
 assert.match(html,/51%/);
 assert.match(html,/49%/);
 assert.match(html,/1887/);
 assert.match(html,/1830/);
 assert.match(html,/57 raw Elo points/);
 assert.match(html,/venue effects/);
 assert.match(html,/width:51%/);
 assert.doesNotMatch(html,/projected score of|fictional result/i);
});
test('missing prediction and pregame ratings cannot turn into invented zeros',()=>{
 const html=ctx.show({p_home:null,away_pre_elo:null,home_pre_elo:null},'Away','Home');
 assert.match(html,/win chance is unavailable/i);
 assert.match(html,/ratings are unavailable/i);
 assert.doesNotMatch(html,/width:0%|NaN|undefined/);
});
test('html-escapes untrusted display names, including accessible meter label',()=>{
 const html=ctx.show({p_home:.25,away_pre_elo:1500,home_pre_elo:1600},'<Away>', 'Home "X"');
 assert.doesNotMatch(html,/<Away>/);
 assert.match(html,/&lt;Away&gt;/);
 assert.match(html,/Home &quot;X&quot;/);
});
test('production pregame path is conditional and completed game flow unchanged',()=>{
 assert.match(shell,/g\.completed \? '' : gamePregameIntelligence\(g, an, hn\)/);
 assert.match(shell,/g\.completed \? '' : ' gp-pregame-hero'/);
 assert.match(shell,/gameStory\(g, d, season, an, hn\)/);
 assert.match(shell,/lineScoreTable\(d, g, an, hn\)/);
});
