// The chrome that only exists in an installed window.
//
// Driven through the real dashboard in jsdom, because what breaks here is the
// wiring: a bottom bar that never lights up the section you are on, a bar whose
// five destinations drift from the masthead's five, an installed window that
// still shows the browser's own navigation. The CSS that hides each one is
// checked in a real browser instead -- jsdom resolves a stylesheet far enough
// to be misleading and not far enough to be evidence.
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
  throw new Error('never reached');
}
/** The bar is static markup, so it is in the DOM before a single script has
    run. Every assertion here is about what the scripts do to it, and closing
    the window with its own requests still open throws inside jsdom. */
const booted=(w,d)=>until(w,()=>w.CfbNavigation&&d.querySelector('#content').firstElementChild);

/** @param display what matchMedia should report for the display-mode query. */
function open({standalone=false,display=false}={}){
  const vc=new VirtualConsole();const errors=[];vc.on('jsdomError',e=>errors.push(e.message));
  const dom=new JSDOM(fs.readFileSync(path.join(root,'ui/dashboard.html'),'utf8'),{
    url:'http://dashboard.test/ui/dashboard.html',runScripts:'dangerously',
    resources:new LocalAssets(),pretendToBeVisual:true,virtualConsole:vc,
    beforeParse(w){
      // The real signal, not a stand-in for it: the page asks matchMedia for
      // the display-mode query and nothing else decides this.
      w.matchMedia=query=>({matches:/display-mode/.test(query)?display:false,
        media:query,addEventListener(){},removeEventListener(){},
        addListener(){},removeListener(){}});
      if(standalone) Object.defineProperty(w.navigator,'standalone',{value:true,configurable:true});
      w.scrollTo=()=>{};w.HTMLElement.prototype.scrollIntoView=()=>{};
      w.fetch=async url=>({ok:true,json:async()=>JSON.parse(
        fs.readFileSync(path.join(root,'ui',String(url).split('?')[0]),'utf8'))});
    }});
  return {dom,w:dom.window,d:dom.window.document,errors};
}

test('the app bar and the masthead offer the same sections',async()=>{
  const {dom,d}=open();
  try {
    await booted(dom.window,d);
    const sections=nav=>Array.from(d.querySelectorAll(nav+' a'),a=>a.dataset.section);
    // Not a count and not a hardcoded list: the two navigations have to agree,
    // in order, or the app offers somewhere the site does not.
    assert.deepEqual(sections('#app-tabs'),sections('#tabs'));
    assert.ok(sections('#tabs').length>=5,'the masthead lost its tabs');
    for(const a of d.querySelectorAll('#app-tabs a')){
      // An empty <svg> satisfies a search for an icon and draws nothing, so
      // the shapes inside it are what is actually checked.
      const icon=a.querySelector('svg[viewBox]');
      assert.ok(icon,a.dataset.section+' has no icon');
      assert.ok(icon.querySelector('path,circle,rect,polyline,polygon'),
        a.dataset.section+' has an empty icon');
      assert.ok(a.querySelector('span').textContent.trim(),a.dataset.section+' has no label');
      assert.equal(a.getAttribute('href'),'#section='+a.dataset.section);
    }
  } finally { dom.window.close(); }
});

test('a browser tab keeps the browser chrome',async()=>{
  const {dom,d,errors}=open();
  try {
    await booted(dom.window,d);
    assert.equal(d.body.classList.contains('is-app'),false);
    // The three About links stay in the masthead, where the site has them.
    assert.equal(d.querySelectorAll('.header-links a').length,3);
    assert.deepEqual(Array.from(errors),[]);
  } finally { dom.window.close(); }
});

test('display-mode standalone is what turns the app chrome on',async()=>{
  const {dom,d}=open({display:true});
  try {
    await booted(dom.window,d);
    assert.ok(d.body.classList.contains('is-app'),'display-mode standalone did not register');
  } finally { dom.window.close(); }
});

test('an older iPhone is recognised by navigator.standalone alone',async()=>{
  // iOS only learned the display-mode query in 16.4; before that this flag was
  // the only way to tell an installed window from a Safari tab.
  const {dom,d}=open({standalone:true,display:false});
  try {
    await booted(dom.window,d);
    assert.ok(d.body.classList.contains('is-app'),'navigator.standalone was ignored');
  } finally { dom.window.close(); }
});

test('the app bar marks the section you are on, everywhere the masthead does',async()=>{
  const {dom,w,d}=open({display:true});
  const current=nav=>{
    const a=d.querySelector(nav+' a[aria-current="page"]');
    return a&&a.dataset.section;
  };
  try {
    await booted(w,d);
    for(const [hash,expected] of [['#section=home','home'],['#section=people','people'],
                                  ['#section=rankings','rankings'],
                                  // A section the bar has no entry of its own
                                  // for still lights the entry it lives under.
                                  ['#section=games','home'],['#section=playoff','stats']]){
      w.location.hash=hash;
      await until(w,()=>current('#app-tabs')===expected);
      assert.equal(current('#app-tabs'),expected,hash);
      assert.equal(current('#tabs'),expected,hash+' in the masthead');
      assert.ok(d.querySelector('#app-tabs a[aria-current="page"]').classList.contains('active'));
    }
  } finally { dom.window.close(); }
});
