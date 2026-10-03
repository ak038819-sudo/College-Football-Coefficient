// Full static dashboard interaction tests without an external server or network.
const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {JSDOM,ResourceLoader,VirtualConsole}=require('jsdom');
const root=path.resolve(__dirname,'..');
class LocalAssets extends ResourceLoader {
  fetch(url){const filename=path.resolve(root,'.'+new URL(url).pathname);if(!filename.startsWith(root+path.sep)||!filename.endsWith('.js'))return null;return Promise.resolve(fs.readFileSync(filename));}
}
async function until(w,predicate){for(let i=0;i<250;i++){if(predicate())return;await new Promise(r=>setTimeout(r,10));}throw new Error('Dashboard did not reach expected state: '+w.document.querySelector('#content')?.textContent.slice(0,250));}
test('dashboard navigation, discovery, leaders, tools and stadium interactions',async t=>{
 const errors=[],vc=new VirtualConsole();vc.on('jsdomError',e=>errors.push(e.message));
 const dom=new JSDOM(fs.readFileSync(path.join(root,'ui/dashboard.html'),'utf8'),{url:'http://dashboard.test/ui/dashboard.html',runScripts:'dangerously',resources:new LocalAssets(),pretendToBeVisual:true,virtualConsole:vc,beforeParse(w){
   w.matchMedia=()=>({matches:false,addListener(){},removeListener(){}});w.scrollTo=()=>{};w.HTMLElement.prototype.scrollIntoView=()=>{};
   w.fetch=async url=>({ok:true,json:async()=>JSON.parse(fs.readFileSync(path.join(root,'ui',String(url).split('?')[0]),'utf8'))});
 }});
 const w=dom.window,d=w.document;
 const route=async(hash,predicate)=>{const old=d.querySelector('#content').firstElementChild;w.location.hash=hash;await until(w,()=>d.querySelector('#content').firstElementChild!==old&&predicate());};
 const change=(id,value)=>{d.getElementById(id).value=String(value);d.getElementById(id).dispatchEvent(new w.Event('change',{bubbles:true}));};
 try {
  await t.test('Home has current games, previous weeks and combined historical filters',async()=>{
   await until(w,()=>d.querySelector('#home-team'));
   assert.deepEqual([...d.querySelectorAll('#tabs a')].map(a=>a.textContent),['Home','Stats','Standings','Teams']);
   assert.ok(d.querySelector('#global-search'));assert.ok(d.querySelector('#live-games').textContent.trim());
   assert.ok(d.querySelector('#home-weeks').textContent.includes('Previous Week'));
   d.querySelector('#home-weeks a').click();await until(w,()=>w.location.hash.includes('slateweek=')&&d.querySelector('#home-weeks strong')&&d.querySelector('#live-games .live-card')&&d.querySelector('#live-status')?.textContent==='');
   const selectedWeek=w.location.hash;
   const cards=()=>[...d.querySelectorAll('#live-games .live-card')];
   assert.ok(cards().length>0, 'week browsing keeps full scoreboard cards');
   assert.ok(d.querySelector('#live-games .live-watermark'), 'team artwork survives week browsing');
   assert.ok(d.querySelector('#live-games .live-rating'), 'Elo labels survive week browsing');
   const ids=cards().map(c=>c.dataset.liveId).sort();
   d.querySelector('[data-live-order="time"]').click();
   assert.deepEqual(cards().map(c=>c.dataset.liveId).sort(),ids, 'sorting stays on the selected week');
   d.querySelector('[data-live-filter="completed"]').click();
   assert.ok(cards().every(c=>ids.includes(c.dataset.liveId)), 'filtering stays on selected week');
   d.querySelector('[data-live-filter="all"]').click();
   await route('#section=home',()=>d.querySelector('#live-games .live-card'));
   await route(selectedWeek,()=>d.querySelector('#home-weeks strong')&&d.querySelector('#live-games .live-card')&&d.querySelector('#live-status')?.textContent==='');
   assert.deepEqual(cards().map(c=>c.dataset.liveId).sort(),ids, 'returning to the week keeps cards and game identities');
   await route('#section=home&findseason=2024&school=byu&status=completed',()=>d.querySelector('#home-season')?.value==='2024');
   assert.ok(d.querySelectorAll('#home-results .listing-game').length>0);
   assert.ok([...d.querySelectorAll('#home-results .listing-game')].every(row=>row.textContent.includes('BYU')&&row.textContent.includes('Final')));
   change('home-conf','Big 12');await until(w,()=>w.location.hash.includes('conf=Big')&&d.querySelector('#home-conf')?.value==='Big 12');
   assert.equal(d.querySelector('#home-team').value,'byu');assert.equal(d.querySelector('#home-status').value,'completed');
  });
  await t.test('player category, conference, minimum and numeric sort work together',async()=>{
   await route('#section=stats&view=players&season=2026&category=rushing',()=>d.querySelector('.stats-table')&&d.querySelector('[aria-current="page"]')?.textContent==='Stats');
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
   await route('#section=stats&view=teams&season=2026&category=ratings&statConf=Big+12&sort=elo&dir=desc',()=>d.querySelector('.stats-table th')?.textContent.startsWith('Team'));
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
  assert.deepEqual(errors,[],'dashboard script execution should not throw');
 }finally{w.close();}
});
