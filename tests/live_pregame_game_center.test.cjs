// The Home scoreboard routes scheduled games through liveDetail(), not
// hydrateGamePage(). Verify that both entry points share the pregame design.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const shell = fs.readFileSync('ui/dashboard_shell.html', 'utf8');
const block = shell.match(/function liveDetail\(g\) \{[\s\S]*?\n\}(?=\nfunction hydrateLiveGamePage)/)?.[0];
assert.ok(block, 'live feed Game Center source found');
const archive = {game_id:999,completed:0,home_pre_elo:1700,away_pre_elo:1600,p_home:.64};
const g = status => ({
 id:999,status,neutral_site:false,venue:'Stadium',tv:'TV',
 home:{id:2,name:'Home',points:null},away:{id:1,name:'Away',points:null}
});
const stub = {
 liveSnapshot:{updated_at:null,player_boxscores:{}},
 liveSeasonData:{byId:new Map([[999,archive]])},
 UPCOMING_BY_ID:new Map(),
 livePrediction:()=>({pHome:.64,estimated:false}),
 probPct:()=>({home:64,away:36}),
 liveTeamId:t=>({name:t.name}),
 liveLabel:x=>x.status,
 liveSeason:()=>2026,
 esc:s=>String(s).replaceAll('&','&amp;').replaceAll('<','&lt;'),
 teamLink:n=>'<a>'+n+'</a>',
 logoMarkup:()=>'<div>logo</div>',
 liveLogoName:t=>t.name,
 storyPlayerLeaders:()=>'<p>players</p>',
 gamePregameIntelligence:()=>'<section class="gp-pregame-insights">New model panels</section>',
 liveEloPanel:()=>'<section id="old-live-elo">Old live Elo table</section>',
 livePlayerPanel:()=>'<section id="players">Players</section>',
 stadiumHref:()=> '#stadium=1',
 feedAgeNote:()=>'',
};
vm.runInNewContext(block+'\nthis.show=liveDetail;',stub);
test('scheduled scoreboard games display the new pregame model instead of old live Elo table',()=>{
 const html=stub.show(g('scheduled'));
 assert.match(html,/gp-pregame-hero/);
 assert.match(html,/gp-pregame-insights/);
 assert.match(html,/gp-scouting-slot/);
 assert.doesNotMatch(html,/old-live-elo/);
});
test('a live or completed scoreboard retains the existing score-focused presentation',()=>{
 for(const status of ['in_progress','completed']){
   const html=stub.show(g(status));
   assert.doesNotMatch(html,/gp-pregame-insights|gp-scouting-slot|gp-pregame-hero/);
   assert.match(html,/old-live-elo/);
 }
});
test('scheduled game without an archival fixture keeps working with its legacy fallback',()=>{
 stub.liveSeasonData={byId:new Map()};
 const html=stub.show(g('scheduled'));
 assert.doesNotMatch(html,/gp-pregame-insights|gp-scouting-slot/);
 assert.match(html,/old-live-elo/);
});
test('live feed hydration triggers guarded scouting only for scheduled archival fixtures',()=>{
 const hydration=shell.match(/function hydrateLiveGamePage\(version\) \{[\s\S]*?\n\}(?=\n\/\/ Recheck)/)?.[0];
 assert.ok(hydration);
 assert.match(hydration,/g\.status === 'scheduled' && archived && !archived\.completed/);
 assert.match(hydration,/hydrateGameScouting\(archived, liveSeason\(\), an, hn, version\)/);
 assert.match(hydration,/g\.status === 'completed' && archived\?\.completed/);
});
