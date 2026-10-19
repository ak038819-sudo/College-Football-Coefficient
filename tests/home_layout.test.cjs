// Home as the app opens it.
//
// The shape of this page is the feature: on a phone the followed team used to
// take 278px of an 844px screen and the controls another 170, so the first
// game sat below the fold. What is checked here is the order of the page, the
// strip that replaced the nested card, and the freshness notice that now sits
// beside the heading instead of carrying a sentence of its own.
const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {JSDOM,ResourceLoader,VirtualConsole}=require('jsdom');
const {execFileSync}=require('node:child_process');
const root=path.resolve(__dirname,'..');
execFileSync('python3',['build_dashboard.py','--reuse-exports'],{cwd:root});

class LocalAssets extends ResourceLoader {
  fetch(url){
    const filename=path.resolve(root,'.'+new URL(url).pathname);
    if(!filename.startsWith(root+path.sep)||!filename.endsWith('.js')) return null;
    return Promise.resolve(fs.readFileSync(filename));
  }
}
async function until(w,predicate){
  for(let i=0;i<300;i++){if(predicate()) return;await new Promise(r=>setTimeout(r,10));}
  throw new Error('never reached: '+w.document.querySelector('#content')?.textContent.slice(0,200));
}
const live=()=>JSON.parse(fs.readFileSync(path.join(root,'ui/data/live_scores.json'),'utf8'));

/** @param follow team slugs to put in the device's favorites before boot.
    @param updated what the live feed should claim as its timestamp. */
function open({follow=[],updated=null}={}){
  const vc=new VirtualConsole();vc.on('jsdomError',()=>{});
  const dom=new JSDOM(fs.readFileSync(path.join(root,'ui/dashboard.html'),'utf8'),{
    url:'http://dashboard.test/ui/dashboard.html',runScripts:'dangerously',
    resources:new LocalAssets(),pretendToBeVisual:true,virtualConsole:vc,
    beforeParse(w){
      w.matchMedia=query=>({matches:false,media:query,addEventListener(){},
        removeEventListener(){},addListener(){},removeListener(){}});
      w.scrollTo=()=>{};w.HTMLElement.prototype.scrollIntoView=()=>{};
      if(follow.length) w.localStorage.setItem('elocast.favorites.v1',JSON.stringify(follow));
      w.fetch=async url=>{
        const file=String(url).split('?')[0];
        if(file==='data/live_scores.json'&&updated) return {ok:true,json:async()=>({...live(),updated_at:updated})};
        return {ok:true,json:async()=>JSON.parse(fs.readFileSync(path.join(root,'ui',file),'utf8'))};
      };
    }});
  return {dom,w:dom.window,d:dom.window.document};
}
const booted=(w,d)=>until(w,()=>w.CfbNavigation&&d.querySelector('#live-games')?.querySelector('.live-card'));

test('a followed team is one wide strip, not a card in a box',async()=>{
  const {dom,d}=open({follow:['byu']});
  try {
    await booted(dom.window,d);
    const cards=d.querySelectorAll('#fav-teams .fav-card');
    assert.equal(cards.length,1,'one followed team, one strip');
    const card=cards[0];
    assert.equal(card.querySelectorAll('.panel,.card').length,0,'the strip holds no nested box');
    // Logo, record, rank, rating and next opponent: everything Austin asked the
    // strip to carry, each from the part of it that is meant to carry it.
    assert.ok(card.querySelector('.fav-card-head img,.fav-card-head svg'),'no logo');
    assert.equal(card.querySelector('.fav-card-name').textContent,'BYU');
    assert.match(card.querySelector('.fav-card-sub').textContent,/^\d+–\d+/,'no record');
    const elo=card.querySelector('.fav-card-elo');
    assert.match(elo.textContent,/^\d{3,4}/,'no rating');
    assert.match(elo.querySelector('small').textContent,/^Elo · #\d+ of \d+$/,'no rank beside the rating');
    assert.match(card.querySelector('.fav-card-next').textContent,/^(vs|at) .+|^No game scheduled/,'no next opponent');
  } finally { dom.window.close(); }
});

test('Home reaches the games: followed team, one toolbar, then the board',async()=>{
  const {dom,d}=open({follow:['byu']});
  try {
    await booted(dom.window,d);
    const order=['#fav-teams','.live-head','#home-weeks','.live-controls','#live-games']
      .map(sel=>{const el=d.querySelector(sel);assert.ok(el,sel+' is missing from Home');return el;});
    for(let i=1;i<order.length;i++){
      assert.ok(order[i-1].compareDocumentPosition(order[i])&dom.window.Node.DOCUMENT_POSITION_FOLLOWING,
        order[i].id||order[i].className);
    }
    // The heading, the freshness notice and Refresh share one row, so the
    // notice cannot grow into a sentence of its own above the board.
    const head=d.querySelector('.live-head');
    assert.ok(head.querySelector('.live-head-title'));
    assert.ok(head.querySelector('#live-status'));
    assert.ok(head.querySelector('#live-refresh'));
    // Three filter buttons wide at most for the status row, and the ordering
    // beside them as a select rather than a second row of buttons.
    const controls=d.querySelector('.live-controls');
    assert.deepEqual(Array.from(controls.querySelectorAll('[data-live-filter]'),b=>b.textContent),
      ['All','Live','Upcoming','Final']);
    assert.ok(controls.querySelector('.order-pick select[data-live-order-select]'));
  } finally { dom.window.close(); }
});

test('a stale feed says so in a few words, a fresh one says nothing',async()=>{
  const stale=open({updated:new Date(Date.now()-33*3600*1000).toISOString()});
  try {
    await booted(stale.dom.window,stale.d);
    await until(stale.dom.window,()=>stale.d.querySelector('#live-status').textContent!=='Loading…');
    assert.equal(stale.d.querySelector('#live-status').textContent,'Updated 33h ago');
  } finally { stale.dom.window.close(); }
  const fresh=open({updated:new Date().toISOString()});
  try {
    await booted(fresh.dom.window,fresh.d);
    await until(fresh.dom.window,()=>fresh.d.querySelector('#live-status').textContent!=='Loading…');
    assert.equal(fresh.d.querySelector('#live-status').textContent,'');
  } finally { fresh.dom.window.close(); }
});

test('Refresh asks the feed again',async()=>{
  const {dom,d,w}=open({updated:new Date(Date.now()-33*3600*1000).toISOString()});
  try {
    await booted(w,d);
    await until(w,()=>d.querySelector('#live-status').textContent==='Updated 33h ago');
    let asked=0;
    const real=w.fetch;
    w.fetch=async url=>{if(String(url).split('?')[0]==='data/live_scores.json') asked++;return real(url);};
    d.querySelector('#live-refresh').click();
    assert.equal(d.querySelector('#live-status').textContent,'Checking…');
    await until(w,()=>asked>0);
    await until(w,()=>d.querySelector('#live-status').textContent==='Updated 33h ago');
  } finally { dom.window.close(); }
});
