// Full static dashboard interaction tests without an external server or network.
const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {JSDOM,ResourceLoader,VirtualConsole}=require('jsdom');
const {execFileSync}=require('node:child_process');
const root=path.resolve(__dirname,'..');
// Test the current shell, not the last dashboard committed by a deployment.
// Reuse authoritative exports; this only renders HTML and never rebuilds ratings.
execFileSync('python3',['build_dashboard.py','--reuse-exports'],{cwd:root});
class LocalAssets extends ResourceLoader {
  fetch(url){const filename=path.resolve(root,'.'+new URL(url).pathname);if(!filename.startsWith(root+path.sep)||!filename.endsWith('.js'))return null;return Promise.resolve(fs.readFileSync(filename));}
}
async function until(w,predicate){for(let i=0;i<250;i++){if(predicate())return;await new Promise(r=>setTimeout(r,10));}throw new Error('Dashboard did not reach expected state: '+w.document.querySelector('#content')?.textContent.slice(0,250));}
test('dashboard navigation, discovery, leaders, tools and stadium interactions',async t=>{
 const errors=[],vc=new VirtualConsole();vc.on('jsdomError',e=>errors.push(e.message));
 let liveOverride=null;
 const dom=new JSDOM(fs.readFileSync(path.join(root,'ui/dashboard.html'),'utf8'),{url:'http://dashboard.test/ui/dashboard.html',runScripts:'dangerously',resources:new LocalAssets(),pretendToBeVisual:true,virtualConsole:vc,beforeParse(w){
   w.matchMedia=()=>({matches:false,addListener(){},removeListener(){}});w.scrollTo=()=>{};w.HTMLElement.prototype.scrollIntoView=()=>{};
   w.fetch=async url=>({ok:true,json:async()=>String(url).split('?')[0]==='data/live_scores.json'&&liveOverride
     ? JSON.parse(JSON.stringify(liveOverride))
     : JSON.parse(fs.readFileSync(path.join(root,'ui',String(url).split('?')[0]),'utf8'))});
 }});
 const w=dom.window,d=w.document;
 const route=async(hash,predicate)=>{const old=d.querySelector('#content').firstElementChild;w.location.hash=hash;await until(w,()=>d.querySelector('#content').firstElementChild!==old&&predicate());};
 const change=(id,value)=>{d.getElementById(id).value=String(value);d.getElementById(id).dispatchEvent(new w.Event('change',{bubbles:true}));};
 try {
  await t.test('Merits defaults read 2.0 totals and keep legacy comparison explicit', async()=>{
   await until(w,()=>d.querySelector('#home-team'));
   const data=JSON.parse(fs.readFileSync(path.join(root,'ui/dashboard_data.json'),'utf8'));
   const year=Math.max(...Object.keys(data.team_merits_by_year).map(Number));
   await route('#section=rankings&view=team-coe&season='+year,()=>d.querySelector('.rank-table'));
   const row=d.querySelector('.rank-table tbody tr');
   assert.ok(row.textContent.includes(data.team_merits_by_year[year][0].team));
   assert.ok(row.textContent.includes(data.team_merits_by_year[year][0].merits.toFixed(3)));
   assert.equal(d.querySelector('details.panel').open,false);
   await route('#section=rankings&view=conference-coe&season='+year,()=>d.querySelector('.rank-table'));
   assert.match(d.querySelector('#content').textContent,/Conference Merits entering/);
   assert.ok(d.querySelector('.rank-table tbody tr').textContent.includes(data.conference_coe2_5yr_by_year[year][0].conference));
   await route('#section=rankings&view=conference-coe-v1&season='+year,()=>d.querySelector('.rank-table'));
   assert.match(d.querySelector('#content').textContent,/Legacy Season Merits/);
   await route('#section=home',()=>d.querySelector('#home-team'));
  });
  await t.test('Home has current games, previous weeks and combined historical filters',async()=>{
   await until(w,()=>d.querySelector('#home-team'));
   assert.deepEqual([...d.querySelectorAll('#tabs a')].map(a=>a.textContent),['Home','Stats','Standings','Teams','People']);
   assert.ok(d.querySelector('#global-search'));assert.ok(d.querySelector('#live-games').textContent.trim());
   assert.ok(d.querySelector('#home-weeks').textContent.includes('Previous Week'));
   // One toolbar: a week select between the two steps, Today beside it, and
   // ordering in a dropdown rather than its own row of buttons.
   assert.ok(d.querySelector('#home-weeks .week-pick select[data-route-select]'), 'the week bar picks weeks from a select');
   assert.ok(d.querySelector('#home-weeks .week-today'), 'the week bar carries Today');
   assert.deepEqual([...d.querySelectorAll('[data-live-order-select] option')].map(o=>o.value),['hype','time']);
   assert.equal(d.querySelectorAll('[data-live-order]').length,0,'ordering moved out of the filter row');
   d.querySelector('#home-weeks a').click();await until(w,()=>w.location.hash.includes('slateweek=')&&d.querySelector('#home-weeks select')?.value===w.location.hash&&d.querySelector('#live-games .live-card')&&d.querySelector('#live-status')?.textContent==='');
   const selectedWeek=w.location.hash;
   const cards=()=>[...d.querySelectorAll('#live-games .live-card')];
   assert.ok(cards().length>0, 'week browsing keeps full scoreboard cards');
   assert.ok(d.querySelector('#live-games .live-watermark'), 'team artwork survives week browsing');
   assert.ok(d.querySelector('#live-games .live-rating'), 'Elo labels survive week browsing');
   const ids=cards().map(c=>c.dataset.liveId).sort();
   const featured=cards().map(c=>c.dataset.liveId);
   assert.ok(featured.length>2, 'the week needs enough games for an order to be visible');
   const order=d.querySelector('[data-live-order-select]');order.value='time';order.dispatchEvent(new w.Event('change',{bubbles:true}));
   assert.notDeepEqual(cards().map(c=>c.dataset.liveId),featured, 'the sort dropdown reorders the board');
   assert.deepEqual(cards().map(c=>c.dataset.liveId).sort(),ids, 'sorting stays on the selected week');
   d.querySelector('[data-live-filter="completed"]').click();
   assert.ok(cards().every(c=>ids.includes(c.dataset.liveId)), 'filtering stays on selected week');
   d.querySelector('[data-live-filter="all"]').click();
   await route('#section=home',()=>d.querySelector('#live-games .live-card'));
   await route(selectedWeek,()=>d.querySelector('#home-weeks select')?.value===selectedWeek&&d.querySelector('#live-games .live-card')&&d.querySelector('#live-status')?.textContent==='');
   assert.deepEqual(cards().map(c=>c.dataset.liveId).sort(),ids, 'returning to the week keeps cards and game identities');
   await route('#section=home&findseason=2024&school=byu&status=completed',()=>d.querySelector('#home-season')?.value==='2024');
   assert.ok(d.querySelectorAll('#home-results .listing-game').length>0);
   assert.ok([...d.querySelectorAll('#home-results .listing-game')].every(row=>row.textContent.includes('BYU')&&row.textContent.includes('Final')));
   const before=w.location.hash; change('home-conf','Big 12'); change('home-status','completed'); assert.equal(w.location.hash,before,'filters wait until Apply'); d.querySelector('#home-filter-form').dispatchEvent(new w.Event('submit',{bubbles:true,cancelable:true}));await until(w,()=>w.location.hash.includes('conf=Big')&&d.querySelector('#home-conf')?.value==='Big 12');
   assert.equal(d.querySelector('#home-team').value,'byu');assert.equal(d.querySelector('#home-status').value,'completed');
  });
  await t.test('Saturday final retained in the live feed shows archived efficiency',async()=>{
   // Keep this archived final in the mocked feed even after the live slate rolls.
   liveOverride={source:'test',updated_at:'2026-10-04T12:00:00Z',games:[{
     id:401858476,start_date:'2026-10-03T16:00:00Z',status:'completed',neutral_site:false,
     home:{id:77,name:'Northwestern Wildcats',points:34,classification:'fbs'},
     away:{id:213,name:'Penn State Nittany Lions',points:13,classification:'fbs'}
   }]};
   try {
   await route('#game=401858476&season=2026',()=>d.querySelector('#gp-efficiency'));
   assert.match(d.querySelector('#gp-efficiency').textContent,/Success Rate.*SRDiff.*xSRDiff.*SR\+/s);
   assert.match(d.querySelector('.story-panel').textContent,/Efficiency edge/);
   assert.doesNotMatch(d.querySelector('#gp-efficiency').textContent,/N\/A/);

   // Elo, Game CoE and Efficiency are ONE table, a row per team, not three
   // transposed ones. The shape is the point: a reader compares across, and
   // the table is wide enough to fill the panel it sits in.
   const matchup=d.querySelector('table.gp-matchup');
   assert.ok(matchup,'the matchup table is missing');
   assert.equal(d.querySelectorAll('table.gp-matchup').length,1,'the three panels must not come back');
   const groups=[...matchup.querySelectorAll('thead tr:first-child th.gp-group')].map(h=>h.textContent.trim());
   assert.deepEqual(groups,['Elo','Game Merits','Efficiency']);
   const metrics=[...matchup.querySelectorAll('thead tr:last-child th')].map(h=>h.textContent.trim());
   assert.deepEqual(metrics,['Team','Before','Win chance','After','\u0394 Elo','Result','Hybrid','P',
     'Game Merits','Success Rate','SRDiff','xSRDiff','SR+']);
   // Each group header spans exactly the metric columns beneath it, or the
   // body cells land under the wrong headings.
   const spans=[...matchup.querySelectorAll('thead tr:first-child th.gp-group')].map(h=>Number(h.getAttribute('colspan')));
   assert.deepEqual(spans,[4,4,4]);
   assert.equal(spans.reduce((a,b)=>a+b,0)+1,metrics.length);
   const bodyRows=[...matchup.querySelectorAll('tbody tr')];
   assert.equal(bodyRows.length,2,'one row per team');
   // Away first, matching the away-then-home column order the old tables used.
   // Thirteen columns is enough content to earn the panel's full width.
   assert.ok(matchup.classList.contains('is-wide'),'a full matchup table should fill the panel');
   assert.equal(bodyRows[0].querySelector('th').textContent.trim(),'Penn State');
   assert.equal(bodyRows[1].querySelector('th').textContent.trim(),'Northwestern');
   for(const row of bodyRows) assert.equal(row.querySelectorAll('td').length,metrics.length-1);
   // The numbers are still the stored ones: Northwestern won, so its Elo moved up.
   const nw=[...bodyRows[1].querySelectorAll('td')].map(c=>c.textContent.trim());
   assert.match(nw[3],/^\+/, 'the winner\u2019s Elo change should be positive');
   assert.equal(nw[4],'Win');
   } finally { liveOverride=null; }
  });
  await t.test('a game with only Elo shows only Elo, and does not stretch to fill',async()=>{
   // 1984: CoE 2.0 begins in 1985 and Success Rate in 2001, so this game has
   // one group. The table must say so by being short, not by carrying two
   // groups of dashes, and must not stretch five columns across the panel.
   await route('#game=52663&season=1984',()=>d.querySelector('table.gp-matchup'));
   const t2=d.querySelector('table.gp-matchup');
   assert.deepEqual([...t2.querySelectorAll('thead tr:first-child th.gp-group')].map(h=>h.textContent.trim()),['Elo']);
   assert.deepEqual([...t2.querySelectorAll('thead tr:last-child th')].map(h=>h.textContent.trim()),
     ['Team','Before','Win chance','After','\u0394 Elo']);
   assert.ok(!t2.classList.contains('is-wide'),'five columns must not be stretched across the panel');
   assert.match(d.querySelector('#gp-efficiency').textContent,/Merits begins in 1985/);
   assert.doesNotMatch(d.querySelector('#gp-efficiency').textContent,/SRDiff/);
  });
  await t.test('player category, conference, minimum and numeric sort work together',async()=>{
   await route('#section=stats&view=players&season=2026&category=rushing',()=>d.querySelector('.stats-table')&&d.querySelector('#tabs a[aria-current="page"]')?.textContent==='Stats');
   assert.ok(d.querySelectorAll('.stats-table tbody tr').length<=50);
   d.getElementById('stats-conf').value='Big 12';d.getElementById('stats-minimum').value='10';
   const oldForm=d.getElementById('stats-filters');
   d.getElementById('stats-filters').dispatchEvent(new w.Event('submit',{bubbles:true,cancelable:true}));
   await until(w,()=>d.getElementById('stats-filters')!==oldForm&&w.location.hash.includes('statConf=Big')&&d.querySelector('#stats-conf')?.value==='Big 12');
   const yds=d.querySelector('[data-stat-sort="yards"]');yds.click();
   await until(w,()=>w.location.hash.includes('sort=yards')&&w.location.hash.includes('dir=asc')&&d.querySelector('th[aria-sort="ascending"]'));
   assert.equal(d.getElementById('stats-conf').value,'Big 12');assert.equal(d.getElementById('stats-minimum').value,'10');
   const heads=[...d.querySelectorAll('.stats-table th')],index=heads.findIndex(h=>h.textContent.startsWith('YDS '));
   const values=[...d.querySelectorAll('.stats-table tbody tr')].map(row=>Number(row.cells[index].textContent.replaceAll(',','')));
   assert.deepEqual(values,[...values].sort((a,b)=>a-b));
   d.querySelector('[data-stat-sort="yards"]').click();await until(w,()=>w.location.hash.includes('dir=desc')&&d.querySelector('th[aria-sort="descending"]'));
   const oldTable=d.querySelector('.stats-table');change('season-select',2024);await until(w,()=>w.location.hash.includes('season=2024')&&d.querySelector('.stats-table')&&d.querySelector('.stats-table')!==oldTable);
   assert.equal(d.getElementById('stats-conf').value,'Big 12');
  });
  await t.test('team stats, linked leader cards, and restored detail return addresses',async()=>{
   await route('#section=stats&view=teams&season=2026&category=ratings&statConf=Big+12&sort=elo&dir=desc',()=>d.querySelector('.stats-table th:nth-child(2)')?.textContent.startsWith('Team'));
   assert.ok(d.querySelectorAll('.stats-table tbody tr').length>0);
   const teamLink=d.querySelector('.stats-table tbody a');const href=teamLink.getAttribute('href');assert.ok(href.includes('from='));
   teamLink.click();await until(w,()=>w.location.hash.startsWith('#team='));
   const back=d.querySelector('.back-link');assert.ok(back.getAttribute('href').includes('statConf=Big'));back.click();
   await until(w,()=>d.querySelector('#stats-conf')?.value==='Big 12');
   await route('#section=stats&view=overview&season=2026',()=>d.querySelector('.stats-leaders'));
   assert.ok(d.querySelectorAll('.stats-leaders article').length>5);
   d.querySelector('.stats-leaders article a').click();await until(w,()=>d.querySelector('.stats-table'));
   assert.ok(w.location.hash.includes('sort=yards'));
  });
  await t.test('migrated matchup calculates and playoff simulator still runs',async()=>{
   await route('#section=matchups&home=1&away=2',()=>d.querySelector('.matchup-board'));
   assert.ok(w.location.hash.startsWith('#section=stats&view=matchup'));assert.ok(d.querySelector('.matchup-board').textContent.includes('%'));
   const before=d.querySelector('#matchup-a').value;d.querySelector('#matchup-swap').click();await until(w,()=>d.querySelector('#matchup-b')?.value===before);
   await route('#section=playoff&view=bracket&season=2026',()=>d.querySelector('#simulate-btn'));
   assert.ok(w.location.hash.startsWith('#section=stats&view=playoff'));d.querySelector('#simulate-btn').click();
   await until(w,()=>d.querySelector('.bracket-tree')||d.querySelector('.bracket-round-col'));
  });
  await t.test('Standings remains accessible and legacy statistical tables sort',async()=>{
   await route('#section=rankings&view=elo&season=2026',()=>d.querySelector('.rank-table thead .sort-button'));
   const button=[...d.querySelectorAll('.rank-table th button')].find(b=>b.textContent==='Elo');assert.ok(button);button.click();
   assert.equal(button.parentElement.getAttribute('aria-sort'),'descending');
  });
  await t.test('an unreleased poll gives its width back instead of holding an empty column',async()=>{
   await route('#section=rankings&view=top25&season=2026',()=>d.querySelector('.rank-boards'));
   const boards=()=>[...d.querySelectorAll('.rank-boards .board-col')];
   const now=boards().length;
   assert.equal(d.querySelector('.rank-boards').dataset.boards,String(now),
     'the row declares how many boards it holds');
   assert.ok(boards().every(b=>b.querySelector('.mini-board li')),'no board is drawn empty');
   // Polls are exported for the current season only, so a past season has none.
   await route('#section=rankings&view=top25&season=1998',()=>d.querySelector('.rank-boards'));
   assert.equal(boards().length,1,'a season with no poll shows the Elo board alone');
   assert.equal(d.querySelector('.rank-boards').dataset.boards,'1');
   const note=d.querySelector('#content .section-note')?.textContent||'';
   assert.match(note,/AP Top 25 and CFP Rankings/,'the note names the polls that are missing');
   assert.match(note,/1998/);
  });
  await t.test('the Elo page explains each rating, and the detail joins the right team-season',async()=>{
   // The previous subtest may already be on this view, and route() waits for the
   // rendered node to change, so step away first.
   await route('#section=rankings&view=top25&season=2026',()=>d.querySelector('.rank-boards'));
   await route('#section=rankings&view=elo&season=2026',()=>d.querySelector('.elo-table tbody tr'));
   await until(w,()=>[...d.querySelectorAll('.elo-table .sos')].every(c=>c.textContent.trim()!=='\u2026'));
   const rows=[...d.querySelectorAll('.elo-table tbody tr')];
   assert.ok(rows.length>100,'the Elo page lists every rated team');
   assert.deepEqual([...d.querySelectorAll('.elo-table thead th')].map(th=>th.textContent.trim()),
     ['#','Team','Conf','Record','Elo','Last 5','SOS','vs Exp']);
   for (const cls of ['.last5','.sos','.vsexp'])
     assert.ok(rows.every(r=>r.querySelector(cls).textContent.trim()!=='\u2026'),cls+' fills in');
   assert.ok(rows.every(r=>{const text=r.querySelector('.conf-cell').textContent.trim();
     return text && text!=='\u2026';}),'every team shows a conference or an explicit dash');
   // Each team-season carries its own schedule-strength rank, so the column must
   // be a permutation: a join that borrowed another team's or another season's
   // row would repeat one and drop another.
   const ranks=rows.map(r=>Number(r.querySelector('.sos .fpv').textContent.replace('#','')));
   assert.deepEqual([...ranks].sort((a,b)=>a-b),ranks.map((_,i)=>i+1),'SOS ranks are 1..N exactly once');
   // And the number itself must be the one the team page shows for that team.
   const target=rows[3];
   const name=target.querySelector('.team-link-name').textContent.trim();
   const sos=target.querySelector('.sos').textContent.replace(/\s+/g,' ').trim();
   await route('#team='+encodeURIComponent(name)+'&season=2026&tab=analytics',
     ()=>/Strength of schedule/i.test(d.querySelector('#content').textContent));
   const stat=[...d.querySelectorAll('.team-stat')].find(el=>/Strength of schedule/i.test(el.querySelector('.lbl')?.textContent||''));
   assert.ok(stat,'the team page shows a strength of schedule for '+name);
   const expected=stat.querySelector('.val').textContent.trim()+' #'+
     stat.querySelector('.sub').textContent.trim().match(/#(\d+)/)[1];
   assert.equal(sos,expected,'the Elo page and the team page disagree on '+name+'\u2019s schedule');
  });
  await t.test('a ranking board carries each team\'s record, and it agrees with the team page',async()=>{
   await route('#section=rankings&view=top25&season=2026',()=>d.querySelector('.rank-boards .mini-board'));
   const rows=[...d.querySelectorAll('.rank-boards .board-col:first-child .mini-board li')];
   assert.ok(rows.length>=25,'the Elo board lists a full top 25');
   assert.ok(rows.every(li=>/^\d+\u2013\d+(\u2013\d+)?$/.test(li.querySelector('.rec')?.textContent||'')),
     'every ranked team shows a win-loss record');
   // The team page is the authority on a record; the board must not disagree.
   const check=async(li)=>{
    const name=li.querySelector('.team-link-name, a')?.textContent.trim();
    const board=li.querySelector('.rec').textContent;
    await route('#team='+encodeURIComponent(name)+'&season=2026',()=>d.querySelector('.team-stats'));
    const stat=[...d.querySelectorAll('.team-stats .team-stat')]
      .find(el=>el.querySelector('.lbl')?.textContent.trim()==='Record');
    assert.ok(stat,'the team page shows a record for '+name);
    // Compare the headline value only: the sub-line names the rated part, so a
    // board that quietly dropped the FCS games would match it by substring.
    assert.equal(board,stat.querySelector('.val').textContent.trim(),
      'the board and the team page disagree on '+name+'\u2019s record');
    return {name,droppable:/against rated opponents/.test(stat.querySelector('.sub').textContent)};
   };
   // The check only has teeth against a team with a non-FBS game to drop, and
   // whether the leader has one is this week's data, not a property of the
   // code: walk the board for the highest-ranked team that does. Asserting it
   // of rows[0] alone made a scheduled refresh a publish blocker.
   const leader=await check(rows[0]);
   let droppable=leader.droppable?leader:null;
   for (const li of rows.slice(1)) {
    if (droppable) break;
    const seen=await check(li);
    if (seen.droppable) droppable=seen;
   }
   assert.ok(droppable,'no ranked team has a non-FBS game, so the board could '+
     'drop every FCS result and still match the team pages');
  });
  await t.test('the page chrome names each control once and keeps them on one row',async()=>{
   // The view selector and the season selector are one control group: if they
   // stop sharing a parent they stack, which on a phone pushed the first row of
   // content below the fold. Headings that merely repeat the selected view or
   // the active tab are the other half of that cost.
   for (const hash of ['#section=rankings&view=top25&season=2026','#section=rankings&view=elo&season=2026','#section=playoff&view=bracket&season=2026']) {
    await route(hash,()=>d.querySelector('#content').firstElementChild);
    const row=d.querySelector('.control-row');
    assert.ok(row,'the controls share a row on '+hash);
    assert.equal(d.querySelector('#subnav').parentElement,row);
    assert.equal(d.querySelector('#year-row').parentElement,row);
    // Every view selector, not only the one #subnav happens to hold: a
    // selector left inside #content sits on a row of its own below this one.
    const selectors=[...d.querySelectorAll('[data-route-select]')];
    assert.ok(selectors.length,'the view is chosen by a selector on '+hash);
    for (const select of selectors)
      assert.ok(row.contains(select),'a view selector is outside the control row on '+hash);
    const spoken=new Set([...d.querySelectorAll('#tabs a.active, #subnav a[aria-current="page"]')].map(a=>a.textContent.trim().toLowerCase()));
    for (const select of d.querySelectorAll('[data-route-select]')) spoken.add(select.selectedOptions[0].textContent.trim().toLowerCase());
    const repeats=[...d.querySelectorAll('#content h1, #content h2')]
      .map(h=>h.textContent.trim().toLowerCase()).filter(text=>spoken.has(text));
    assert.deepEqual(repeats,[],'no heading repeats a control already on screen at '+hash);
   }
   // A route with no controls at all must not keep the row's spacing: the
   // wrapper is always in the document, so it cannot rely on :empty.
   for (const hash of ['#section=home','#section=methodology']) {
    await route(hash,()=>d.querySelector('#content').firstElementChild);
    const row=d.querySelector('.control-row');
    assert.ok([...row.children].every(child=>!child.textContent.trim()),hash+' has no controls');
    // The spacing belongs to the controls, which hide when empty, not to the
    // wrapper, which is always in the document. (Whether the children hide is
    // a layout question jsdom does not answer; this pins where the margin is.)
    assert.equal(w.getComputedStyle(row).marginBottom,'0px',
      'the empty control row adds no chrome at '+hash);
   }
  });
  await t.test('one geographic stadium map supports marker and team navigation',async()=>{
   await route('#section=teams&view=stadiums',()=>d.querySelector('.stadium-map'));
   assert.equal(d.querySelectorAll('.stadium-map').length,1);assert.equal(d.querySelectorAll('.state-boundary').length,50);
   const markers=[...d.querySelectorAll('[data-stadium-marker]')];assert.ok(markers.length>100);
   assert.ok(markers.some(m=>m.getAttribute('aria-label').includes('Hawai')));
   const marker=markers.find(m=>m.getAttribute('aria-label').includes('LaVell'));assert.ok(marker);marker.dispatchEvent(new w.MouseEvent('click',{bubbles:true,cancelable:true}));
   await until(w,()=>d.querySelector('.selected-stadium'));
   assert.equal(d.querySelectorAll('.stadium-map').length,1);assert.ok(d.querySelector('.stadium-facts').textContent.includes('Capacity'));
   const team=d.querySelector('.stadium-facts a');assert.ok(team.getAttribute('href').startsWith('#team='));team.click();
   await until(w,()=>d.querySelector('#team-stadiums a'));
   d.querySelector('#team-stadiums a').click();await until(w,()=>d.querySelector('.selected-stadium'));
  });
  await t.test('People directory filters roles and links to profiles',async()=>{
   await route('#section=people&q=Klubnik',()=>d.querySelector('#people-results a'));
   assert.ok(d.querySelector('#people-results a').getAttribute('href').startsWith('#player='));
   d.querySelector('#people-query').value='Saban';d.querySelector('#people-kind').value='coach';
   d.querySelector('#people-form').dispatchEvent(new w.Event('submit',{bubbles:true,cancelable:true}));
   await until(w,()=>d.querySelector('#people-results a')?.getAttribute('href').startsWith('#coach='));
   assert.ok(w.location.hash.includes('q=Saban')&&w.location.hash.includes('kind=coach'));
   const profile=d.querySelector('#people-results a');assert.ok(decodeURIComponent(profile.href).includes('q=Saban'));
   assert.equal(d.querySelector('#people-kind').value,'coach');
  });
  await t.test('Modern team scouting uses the verified season asset and compares opposing units',async()=>{
   await route('#team=byu&tab=analytics',()=>d.querySelector('#tp-scouting'));
   await until(w,()=>d.querySelector('#tm-opponent') || /not available for this season/i.test(d.querySelector('#tp-scouting')?.textContent||''));
   const panel=d.querySelector('#tp-scouting');
   const select=d.querySelector('#tm-opponent');
   assert.ok(select,'2026 authenticated scouting asset must be available in the PR test build');
   assert.equal(d.querySelectorAll('.tm-scout-card').length,2);
   assert.match(panel.textContent,/Rush success/);
   assert.match(panel.textContent,/Observed/);
   assert.match(panel.textContent,/denominators remain unavailable/);
   assert.match(panel.textContent,/FBS games completed before/);
   assert.match(panel.textContent,/verified snapshot|synced source/i);
   const option=[...select.options].find(x=>x.value);
   assert.ok(option);
   change('tm-opponent',option.value);
   assert.equal(d.querySelectorAll('#tm-opponent-result .tm-match-row').length,6);
   assert.match(d.querySelector('#tm-opponent-result').textContent,/defense/);
   // The verdict is real text, not a colour, and the meter is decoration.
   assert.match(d.querySelector('.tm-match-verdict').textContent,/edge|Even matchup|Not rated/);
   assert.equal(d.querySelector('.tm-duel-meter').getAttribute('aria-hidden'),'true');
   // Percentiles spell their own ordinal: 81st, never 81th.
   assert.doesNotMatch(d.querySelector('#tm-opponent-result').textContent,/(?<![1])[123]th\b/);
   // The toggle turns the panel around to the other half of the matchup.
   const first=[...d.querySelectorAll('.tm-match-side-label')].map(x=>x.textContent);
   assert.ok(first.every(x=>/ offense$/.test(x)||/ defense$/.test(x)));
   assert.equal(d.querySelector('.tm-unit-button[data-unit="offense"]').getAttribute('aria-pressed'),'true');
   d.querySelector('.tm-unit-button[data-unit="defense"]').dispatchEvent(new w.Event('click',{bubbles:true}));
   assert.equal(d.querySelector('.tm-unit-button[data-unit="defense"]').getAttribute('aria-pressed'),'true');
   const second=[...d.querySelectorAll('.tm-match-side-label')].map(x=>x.textContent);
   assert.equal(second.length,first.length);
   assert.notDeepEqual(second,first);
   assert.equal(d.querySelectorAll('#tm-opponent-result .tm-match-row').length,6);
   // Each row reads as a sentence, and it turns around with the toggle.
   const notes=[...d.querySelectorAll('.tm-match-note')].map(x=>x.textContent);
   assert.equal(notes.length,6);
   assert.ok(notes.every(x=>/\.$/.test(x.trim())),'each note is a sentence: '+notes[0]);
   assert.ok(notes.some(x=>/stronger than what|has defended|closely matched/.test(x)));
   // Both schools' colours reach the rows, and neither is left unset.
   const style=d.querySelector('#tm-opponent-result').style;
   assert.match(style.getPropertyValue('--tm-team-color'),/^#[0-9a-f]{6}$/);
   assert.match(style.getPropertyValue('--tm-opp-color'),/^#[0-9a-f]{6}$/);
   assert.notEqual(style.getPropertyValue('--tm-team-color'),style.getPropertyValue('--tm-opp-color'));
   assert.doesNotMatch(panel.textContent,/\bwin probability\s*:\s*\d/i);
   await route('#team=byu&tab=overview',()=>d.querySelector('#tp-scouting'));
   await until(w,()=>d.querySelector('#tm-opponent'));
   assert.ok(d.querySelector('.team-header .team-stats'),'original team header remains');
  });
  await t.test('Team statistics, leaders, roster sorting and chart ranges work',async()=>{
   await route('#team=byu&tab=stats',()=>d.querySelector('#tp-statistics tbody tr'));
   assert.ok(d.querySelector('#tp-statistics').textContent.includes('Games'));
   await route('#team=byu&tab=leaders',()=>d.querySelector('#tp-statistics a[href^="#player="]'));
   await route('#team=byu&tab=roster',()=>d.querySelector('[data-roster-column="1"]'));
   const button=d.querySelector('[data-roster-column="1"]');button.click();
   const names=[...d.querySelectorAll('.roster-table tbody tr')].map(r=>r.cells[1].textContent);
   assert.deepEqual(names,[...names].sort((a,b)=>a.localeCompare(b,undefined,{numeric:true,sensitivity:'base'})));
   await route('#team=byu',()=>d.querySelector('#tp-chart-from'));
   change('tp-chart-from',2020);change('tp-chart-to',2023);
   assert.equal(d.querySelector('#tp-chart-from').value,'2020');assert.equal(d.querySelector('#tp-chart-to').value,'2023');
   assert.ok(d.querySelector('#tp-elo-chart svg'));
  });
  await t.test('Team tables let readers choose columns',async()=>{
   await route('#section=stats&view=teams&season=2026',()=>d.querySelector('[data-team-field]'));
   const checkbox=d.querySelector('[data-team-field][value="ppg"]');assert.ok(checkbox.checked);
   checkbox.checked=false;checkbox.dispatchEvent(new w.Event('change',{bubbles:true}));
   assert.equal(d.querySelector('[data-stat-sort="ppg"]'),null);
  });
  assert.deepEqual(errors,[],'dashboard script execution should not throw');
 }finally{w.close();}
});
