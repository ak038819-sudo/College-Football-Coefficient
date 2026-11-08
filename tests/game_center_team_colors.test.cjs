// Game Center matchup bars must follow team identity without touching probabilities.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const shell = fs.readFileSync('ui/dashboard_shell.html','utf8');
// The game-page design moved out of the shell and into a stylesheet every
// league page links; the markup that sets these two custom properties stayed
// in the shell. Both halves still have to agree, which is what the rule
// below is checked for.
const style = fs.readFileSync('ui/game_page.css','utf8');
function fn(name) {
 const m = shell.match(new RegExp('function '+name+'\\([^]*?\\n\\}'));
 assert.ok(m,'missing helper: '+name);
 return m[0];
}
const palette = {LSU:'#381d53',Texas:'#53261d',Akron:'#1d2f53'};
const ctx = {
 teamProfileColor:name=>({Texas:'#BF5700'}[name]||palette[name]||'#304250'),
 gameBarColor:null,
 probPct:p=>({home:Math.round(p*100),away:100-Math.round(p*100)}),
 esc:s=>String(s).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;'),
 eloValue:v=>Math.round(v).toString()
};
vm.runInNewContext(fn('gameBarColor')+'\n'+fn('gamePregameIntelligence')+
  '\nthis.render=gamePregameIntelligence;this.bar=gameBarColor;',ctx);
test('LSU purple and Texas burnt orange are mapped to the appropriate teams',()=>{
 const html=ctx.render({p_home:.57,away_pre_elo:1750,home_pre_elo:1810},'LSU','Texas');
 assert.match(html,/--gp-away-color:#461D7C;--gp-home-color:#BF5700/);
 assert.match(html,/width:43%/);
 assert.match(html,/width:57%/);
 assert.match(html,/Elo rating analysis/);
 assert.doesNotMatch(html,/background:#461D7C/);
});
test('reversed fixtures swap colors without changing model probability',()=>{
 const html=ctx.render({p_home:.43,away_pre_elo:1810,home_pre_elo:1750},'Texas','LSU');
 assert.match(html,/--gp-away-color:#BF5700;--gp-home-color:#461D7C/);
 assert.match(html,/width:57%/);
 assert.match(html,/width:43%/);
});
test('all schools use existing team color registry, unknown teams safely fall back',()=>{
 assert.equal(ctx.bar('Akron'),'#1d2f53');
 assert.equal(ctx.bar('Unknown'),'#304250');
 assert.match(style,/\.gp-intel-meter span:first-child\{background:var\(--gp-away-color/);
 assert.match(style,/\.gp-intel-meter span:last-child\{background:var\(--gp-home-color/);
});
test('scouting meters flip team and opponent colors with matchup direction',()=>{
 const p=fn('gameScoutingPanel');
 assert.match(p,/--tm-team-color:' \+ matchupBrandColor\(an/);
 assert.match(p,/--tm-opp-color:' \+ matchupBrandColor\(hn/);
 assert.match(p,/--tm-team-color:' \+ matchupBrandColor\(hn/);
 assert.match(p,/--tm-opp-color:' \+ matchupBrandColor\(an/);
 assert.match(p,/gameScoutingEligible\(g, season, bundle\)/);
});
test('live-scoreboard scheduled-game route uses shared colored pregame panels',()=>{
 assert.match(shell,/gamePregameIntelligence\(archive, an, hn\)/);
 assert.match(shell,/g\.status === 'scheduled' && archive && !archive\.completed/);
});
