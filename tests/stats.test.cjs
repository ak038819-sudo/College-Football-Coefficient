const {test}=require('node:test');
const assert=require('node:assert/strict');
const stats=require('../ui/stats.js');
const geo=require('../ui/geography.js');
const nav=require('../ui/navigation.js');
const fs=require('node:fs');
const vm=require('node:vm');
const config={yearsAll:[2024,2026],playoffYears:[2024,2026],gameSeasons:[{season:2024,scheduled:0},{season:2026,scheduled:20}]};
const ids=rows=>rows.map(r=>r.name);
test('numeric, percentage and alphabetical sorts preserve nulls at the end in both directions',()=>{
 const rows=[{name:'Zed',yards:100,pct:'62%'},{name:'Amy',yards:20,pct:'9%'},{name:'Null',yards:null,pct:null},{name:'Missing'}];
 assert.deepEqual(ids(stats.sortRows(rows,'yards','asc')),['Amy','Zed','Null','Missing']);
 assert.deepEqual(ids(stats.sortRows(rows,'yards','desc')),['Zed','Amy','Null','Missing']);
 assert.deepEqual(ids(stats.sortRows(rows,'pct','desc')),['Zed','Amy','Null','Missing']);
 assert.deepEqual(ids(stats.sortRows(rows,'name','asc','string')),['Amy','Missing','Null','Zed']);
 assert.deepEqual(ids(stats.sortRows(rows,'name','desc','string')),['Zed','Null','Missing','Amy']);
 assert.equal(rows[0].name,'Zed','sort must not mutate its input');
});
test('filter and sort compose across conference, team, search and minimum',()=>{
 const rows=[{name:'Amy',team:'BYU',conference:'Big 12',yards:20,attempts:4},{name:'Anne',team:'BYU',conference:'Big 12',yards:110,attempts:10},{name:'Adam',team:'Utah',conference:'Big 12',yards:120,attempts:12}];
 const filtered=stats.filterRows(rows,{team:'BYU',conference:'Big 12',query:'an',minimum:5,minimumKey:'attempts'});
 assert.deepEqual(ids(stats.sortRows(filtered,'yards')),['Anne']);
});
const teams=new Map([['1',{name:'BYU'}],['2',{name:'Utah'}]]);
const games=[{game_id:1,completed:true,home_id:1,away_id:2,home_score:30,away_score:20},{game_id:2,completed:true,home_id:2,away_id:1,home_score:24,away_score:10},{game_id:3,completed:false,home_id:1,away_id:2}];
const cats=(cmp,yds)=>[{name:'passing',type:'C/ATT',lines:[{name:'QB',stat:cmp}]},{name:'passing',type:'YDS',lines:[{name:'QB',stat:yds}]},{name:'passing',type:'AVG',lines:[{name:'QB',stat:'999'}]}];
const archive={1:[{name:'BYU',home_away:'home',categories:cats('10/20','100')}],2:[{name:'BYU',home_away:'away',categories:cats('20/25','300')}],3:[{name:'BYU',home_away:'home',categories:cats('100/100','10000')}],999:[{name:'BYU',home_away:'home',categories:cats('100/100','10000')}]};
test('season aggregation sums counting fields and recomputes rates from denominators',()=>{
 const p=stats.aggregatePlayers(archive,new Map(games.map(g=>[g.game_id,g])),{'1':'Big 12'},teams);
 assert.equal(p.rows.length,1);assert.equal(p.rows[0].yards,400);assert.equal(p.rows[0].attempts,45);assert.equal(p.rows[0].games,2);
 assert.equal(p.rows[0].completionPct,30/45*100);assert.equal(p.rows[0].ypg,200);
 assert.equal(p.rows[0].td,undefined,'missing fields are not invented');
 const t=stats.aggregateTeams(games,p,{'1':'Big 12'},teams,{'BYU':{elo:1600}}).find(r=>r.team==='BYU');
 assert.equal(t.games,2);assert.equal(t.ppg,20);assert.equal(t.oppPpg,22);assert.equal(t.passYards,400);assert.equal(t.boxGames,2);assert.equal(t.elo,1600);
});
test('legacy tools and game bookmarks canonicalize into the four destinations',()=>{
 for(const [old,expected] of [['#section=games&season=2024&status=completed','#section=home&view=games'],['#section=playoff&view=bracket','#section=stats&view=playoff&tool=bracket'],['#section=matchups&home=1&away=2','#section=stats&view=matchup'],['#section=stadiums&stadium=42','#section=teams&view=stadiums']]){
 const r=nav.readRoute(old,config),canonical=nav.hashFor(r);assert.ok(canonical.startsWith(expected));assert.deepEqual(nav.readRoute(canonical,config),r);
 }
 for(const section of ['home','stats','rankings','teams']) assert.equal(nav.readRoute('#section='+section,config).section,section);
});
test('statistical and combined Home discovery state round trip in the URL',()=>{
 for(const hash of ['#section=stats&view=players&season=2024&category=passing&statConf=Big+12&statTeam=BYU&minimum=30&sort=completionPct&dir=asc&page=2','#section=home&findseason=2024&school=byu&status=completed&conf=Big+12&week=5&slateweek=4']){
 const r=nav.readRoute(hash,config);assert.deepEqual(nav.readRoute(nav.hashFor(r),config),r);
 }
});
test('one geographic projection includes Hawaii, state outlines, and real stadium coordinates',()=>{
 const projection=geo.projection(),hi=projection.project(-157.8,21.3),ut=projection.project(-111.65,40.25);
 assert.ok(hi[0]<ut[0]&&hi[1]>ut[1]);
 for(const point of [hi,ut])assert.ok(point[0]>=0&&point[0]<=projection.width&&point[1]>=0&&point[1]<=projection.height);
 assert.equal(geo.states.length,51);assert.ok(geo.statePaths().find(s=>s.name==='Hawaii'));assert.ok(geo.statePaths().find(s=>s.name==='Utah'));
 assert.ok(geo.statePaths(true).find(s=>s.name==='Alaska'));
 const shell=fs.readFileSync('ui/dashboard_shell.html','utf8');
 const src=shell.match(/function stadiumMap\([^]*?\n\}/)[0];
 const context={CfbGeography:geo,esc:String,stadiumHref:id=>'#section=teams&view=stadiums&stadium='+id};
 vm.runInNewContext(src+';this.map=stadiumMap;',context);
 const result=context.map([{id:7,name:'Hawaii stadium',lat:21.3,lon:-157.8,teams:[{name:'Hawaii'}],capacity:15000},{id:8,name:'Utah stadium',lat:40.25,lon:-111.65}],7);
 assert.equal((result.match(/<svg/g)||[]).length,1);assert.match(result,/state-boundary/);assert.match(result,/data-stadium-marker="7"/);assert.match(result,/Capacity: 15,000/);assert.match(result,/selected-stadium/);
 assert.match(result,new RegExp('cx="'+hi[0].toFixed(2)+'" cy="'+hi[1].toFixed(2)+'"'));
});
test('Home combined discovery filters retain finals and use feed status for live games',()=>{
 const shell=fs.readFileSync('ui/dashboard_shell.html','utf8');
 const src=['archiveStatus','discoveryMatch'].map(n=>shell.match(new RegExp('function '+n+'\\([^]*?\\n\\}'))[0]).join('\n');
 const context={currentStatusGames:[{id:2,status:'in_progress'}]};
 vm.runInNewContext(src+';this.matches=discoveryMatch;',context);
 const g={game_id:1,home_id:1,away_id:2,week:5,phase:0,completed:true};
 assert.equal(context.matches(g,{week:null,teamId:1,conf:'Big 12',status:'completed'},{'1':'Big 12'}),true);
 assert.equal(context.matches(g,{week:4,teamId:1,status:'completed'},{}),false);
 assert.equal(context.matches({...g,game_id:2,completed:false},{week:5,teamId:1,status:'live'},{}),true);
});

test('source athlete IDs keep namesakes separate and reject a mislabeled team side',()=>{
 const rows=[{id:'10',name:'Same Name',stat:'100'},{id:'20',name:'Same Name',stat:'200'}];
 const box={1:[{name:'BYU',home_away:'home',categories:[{name:'passing',type:'YDS',lines:rows}]}]};
 const result=stats.aggregatePlayers(box,new Map(games.map(g=>[g.game_id,g])),{},teams);
 assert.equal(result.rows.length,2);assert.deepEqual(result.rows.map(r=>r.id).sort(),['10','20']);
 box[1][0].name='Texas Southern';
 assert.equal(stats.aggregatePlayers(box,new Map(games.map(g=>[g.game_id,g])),{},teams).rows.length,0);
});

// ---- derived rates added so the stats tables carry their own context ----
//
// Each of these is computed from sums the aggregation already had. The tests
// pin the DENOMINATOR, because the bug these invite is dividing by the wrong
// one (per recorded game vs per season game, carries vs completions).

const defLines=(tot,solo,sacks)=>[
  {name:'defensive',type:'TOT',lines:[{name:'LB',stat:String(tot)}]},
  {name:'defensive',type:'SOLO',lines:[{name:'LB',stat:String(solo)}]},
  {name:'defensive',type:'SACKS',lines:[{name:'LB',stat:String(sacks)}]}];

function aggregate(archiveRows){
  const g=[{game_id:1,completed:true,home_id:1,away_id:2,home_score:30,away_score:20},
           {game_id:2,completed:true,home_id:2,away_id:1,home_score:24,away_score:10}];
  return stats.aggregatePlayers(archiveRows,new Map(g.map(x=>[x.game_id,x])),{'1':'Big 12'},teams).rows;
}

test('assisted tackles are total minus solo, and absent when either side is',()=>{
 const [row]=aggregate({1:[{name:'BYU',home_away:'home',categories:defLines(9,6,1)}],
                        2:[{name:'BYU',home_away:'away',categories:defLines(5,3,0)}]});
 assert.equal(row.tackles,14);assert.equal(row.solo,9);assert.equal(row.assisted,5);
 // A feed that reported totals and no solo column must not read as "no assists".
 const [onlyTotal]=aggregate({1:[{name:'BYU',home_away:'home',
   categories:[{name:'defensive',type:'TOT',lines:[{name:'LB',stat:'9'}]}]}]});
 assert.equal(onlyTotal.tackles,9);assert.equal(onlyTotal.assisted,null);
});

test('tackles per game divides by games the player appeared in, not games played',()=>{
 // Two appearances, fourteen tackles.
 const [row]=aggregate({1:[{name:'BYU',home_away:'home',categories:defLines(9,6,1)}],
                        2:[{name:'BYU',home_away:'away',categories:defLines(5,3,0)}]});
 assert.equal(row.games,2);assert.equal(row.tacklesPg,7);
 // One appearance only: the rate is over one game, not over the season.
 const [once]=aggregate({1:[{name:'BYU',home_away:'home',categories:defLines(9,6,1)}]});
 assert.equal(once.games,1);assert.equal(once.tacklesPg,9);
});

test('extra-point percentage is made over attempted, and null on no attempts',()=>{
 const kick=(fg,xp)=>[{name:'kicking',type:'FG',lines:[{name:'K',stat:fg}]},
                      {name:'kicking',type:'XP',lines:[{name:'K',stat:xp}]}];
 const [row]=aggregate({1:[{name:'BYU',home_away:'home',categories:kick('2/3','4/5')}],
                        2:[{name:'BYU',home_away:'away',categories:kick('1/1','1/3')}]});
 assert.equal(row.xpMade,5);assert.equal(row.xpAttempts,8);assert.equal(row.xpPct,62.5);
 const [none]=aggregate({1:[{name:'BYU',home_away:'home',categories:kick('2/3','0/0')}]});
 assert.equal(none.xpPct,null);
});

test('yards per completion divides by completions, not attempts',()=>{
 const [row]=aggregate({1:[{name:'BYU',home_away:'home',categories:cats('10/20','300')}]});
 assert.equal(row.completions,10);assert.equal(row.attempts,20);
 assert.equal(row.ypc,30);     // 300 / 10 completions
 assert.equal(row.ypa,15);     // 300 / 20 attempts
});

test('carries per touchdown divides by touchdowns, and is null with none',()=>{
 const rush=(car,td)=>[{name:'rushing',type:'CAR',lines:[{name:'RB',stat:car}]},
                       {name:'rushing',type:'TD',lines:[{name:'RB',stat:td}]}];
 const [row]=aggregate({1:[{name:'BYU',home_away:'home',categories:rush('20','2')}],
                        2:[{name:'BYU',home_away:'away',categories:rush('10','1')}]});
 assert.equal(row.attempts,30);assert.equal(row.td,3);assert.equal(row.tdRate,10);
 const [noTd]=aggregate({1:[{name:'BYU',home_away:'home',categories:rush('20','0')}]});
 assert.equal(noTd.tdRate,null);
});

test('receptions per game divides by appearances',()=>{
 const rec=n=>[{name:'receiving',type:'REC',lines:[{name:'WR',stat:n}]}];
 const [row]=aggregate({1:[{name:'BYU',home_away:'home',categories:rec('7')}],
                        2:[{name:'BYU',home_away:'away',categories:rec('3')}]});
 assert.equal(row.receptions,10);assert.equal(row.recPg,5);
});

test('every declared category field has a label and is produced by the aggregation',()=>{
 // A field in a category's list with no label renders its raw key as a header;
 // one the aggregation never computes renders a column of dashes. Both have
 // shipped before, so both are pinned here.
 const produced=new Set(['yards','td','completions','attempts','completionPct','interceptions',
   'ypa','ypc','ypg','tdRate','receptions','ypr','recPg','tackles','solo','assisted','tacklesPg',
   'sacks','tfl','passesDefended','fgMade','fgAttempts','fgPct','xpMade','xpAttempts','xpPct','points']);
 for(const [name,schema] of Object.entries(stats.categories)){
  for(const field of schema.fields){
   assert.ok(stats.labels[field],`${name}.${field} has no label`);
   assert.ok(produced.has(field),`${name}.${field} is not produced by aggregatePlayers`);
  }
 }
});
