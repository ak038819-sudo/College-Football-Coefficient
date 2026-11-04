// Snapshot safety for pregame Game Center scouting, no browser dependencies.
const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const shell=fs.readFileSync('ui/dashboard_shell.html','utf8');
function src(name){
 const m=shell.match(new RegExp('function '+name+'\\([^]*?\\n\\}'));
 assert.ok(m,'missing '+name);
 return m[0];
}
const ctx={
 esc:v=>String(v).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;'),
 renderMatchupMeters:(a,b)=>'<div class="stub">'+String(a)+' / '+String(b)+'</div>'
};
vm.runInNewContext(src('gameScoutingEligible')+'\n'+src('gameScoutingPanel')+'\nthis.eligible=gameScoutingEligible;this.panel=gameScoutingPanel;',ctx);
const fixture={id:12,away_id:1,home_id:2,kickoff_utc:'2026-11-07T18:30:00Z'};
const team=id=>({team_id:id,metrics:{rush_success_rate:{status:'rated',sample_games:8,distinct_opponents:8,reliability:'moderate'}}});
const bundle={schema_version:'elocast-descriptive-v1',season:2026,as_of_exclusive:'2026-11-02',teams:{'1':team(1),'2':team(2)}};
test('only a same-season snapshot before kickoff can appear',()=>{
 assert.equal(ctx.eligible(fixture,2026,bundle),true);
 assert.equal(ctx.eligible({...fixture,kickoff_utc:'2026-11-01T20:00:00Z'},2026,bundle),false);
 assert.equal(ctx.eligible(fixture,2025,bundle),false);
 assert.equal(ctx.eligible(fixture,2026,{...bundle,as_of_exclusive:'2026-11-08'}),false);
 assert.equal(ctx.eligible(fixture,2026,{...bundle,as_of_exclusive:null}),false);
 assert.equal(ctx.eligible({...fixture,kickoff_utc:null,date:null},2026,bundle),false);
});
test('displays both opposing units with time and evidence provenance',()=>{
 const html=ctx.panel(fixture,2026,'Miami','Notre Dame',bundle);
 assert.match(html,/Miami offense vs\. Notre Dame defense/);
 assert.match(html,/Notre Dame offense vs\. Miami defense/);
 assert.match(html,/2026-11-02/);
 assert.match(html,/8 eligible games/);
 assert.match(html,/moderate reliability/);
 assert.match(html,/not game predictions/);
});
test('absent, wrong-schema or later snapshots never show a misleading panel',()=>{
 assert.equal(ctx.panel(fixture,2026,'A','B',null),'');
 assert.equal(ctx.panel(fixture,2026,'A','B',{...bundle,schema_version:'bad'}),'');
 assert.equal(ctx.panel(fixture,2026,'A','B',{...bundle,teams:{}}),'');
 assert.equal(ctx.panel({...fixture,date:'2026-10-15',kickoff_utc:null},2026,'A','B',bundle),'');
});
test('game route injects scouting only on archived upcoming games',()=>{
 assert.match(shell,/g\.completed \? '' : '<div id="gp-scouting-slot"/);
 assert.match(shell,/if \(!g\.completed\) hydrateGameScouting\(g, season, an, hn, version\)/);
 assert.match(shell,/version !== renderVersion \|\| state\.view !== 'game'/);
});
