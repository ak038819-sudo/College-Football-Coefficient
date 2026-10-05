const {test}=require('node:test');
const assert=require('node:assert/strict');
const vm=require('node:vm');
const fs=require('node:fs');
const source=fs.readFileSync('ui/dashboard_shell.html','utf8').match(/function fillTeamStatistics\(team\) \{[^]*?\n\}/)[0];
test('older season responses and failures cannot overwrite the latest team statistics',async()=>{
 for(const fail of [false,true]){
  const pending=new Map(),target={innerHTML:''},state={statSeason:2024};
  const ctx={document:{getElementById:()=>target},teamPageState:state,renderVersion:1,teamStatisticsSeq:0,
   activeTab:()=> 'stats',panelHead:(title,year)=>String(year),seasonSelect:(_,years,season)=>season,
   DATA:{years_all:[2024,2025]},loadStats:year=>new Promise((resolve,reject)=>pending.set(year,{resolve,reject})),
   TEAM_STAT_CATEGORIES:{overall:['games']},CfbStats:{labels:{games:'Games'}},esc:String,statValue:String,wrapWideTables:()=>{}};
  target.addEventListener=()=>{};
  vm.runInNewContext(source+';this.fill=fillTeamStatistics;',ctx);
  ctx.fill({id:1,name:'BYU'});state.statSeason=2025;ctx.fill({id:1,name:'BYU'});
  pending.get(2025).resolve({teams:[{teamId:1,team:'BYU',games:5}]});await new Promise(r=>setImmediate(r));
  const current=target.innerHTML;assert.ok(current.includes('2025'));
  if(fail)pending.get(2024).reject(new Error('old season failed'));
  else pending.get(2024).resolve({teams:[{teamId:1,team:'BYU',games:4}]});
  await new Promise(r=>setImmediate(r));assert.equal(target.innerHTML,current);
 }
});
