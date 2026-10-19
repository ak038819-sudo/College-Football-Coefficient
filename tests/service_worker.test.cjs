// The service worker's caching rules, driven as real code.
//
// A service worker is the one part of this site that can keep serving a
// week-old page after a deploy, and nothing else in the suite runs it: there
// is no browser here and jsdom implements no ServiceWorkerGlobalScope. So this
// file builds one -- a Cache that is really a Map, a fetch that is really a
// counter -- loads service-worker.js into it, and dispatches synthetic events
// at the handlers it registered. Every assertion below is about what the
// worker ANSWERED WITH, never about what its source says.
//
// The rule that matters most: cache-first, the only strategy that can serve
// something stale forever, must apply to versioned URLs and to nothing else.
const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');

const ROOT=path.resolve(__dirname,'..');
const ORIGIN='https://example.test/College-Football-Coefficient';

// A Response stand-in. The real one is not available to a worker-less Node
// context in a way that survives cloning, and the worker only ever reads
// .ok/.status/.type and calls .clone().
function makeResponse(body,{status=200,type='basic'}={}){
  const response={body,status,type,ok:status>=200&&status<300};
  response.clone=()=>makeResponse(body,{status,type});
  return response;
}

class FakeCache {
  constructor(){this.entries=new Map();}
  async put(request,response){
    const key=typeof request==='string'?request:request.url;
    this.entries.delete(key);                 // re-putting moves it to the back
    this.entries.set(key,response);
  }
  async match(request){
    const key=typeof request==='string'?request:request.url;
    return this.entries.get(key)||undefined;
  }
  async keys(){return [...this.entries.keys()].map(url=>({url}));}
  async delete(request){
    return this.entries.delete(typeof request==='string'?request:request.url);
  }
  async add(request){
    const url=typeof request==='string'?request:request.url;
    const response=await this.scope.fetch({url,method:'GET',headers:new Map()});
    if(!response.ok) throw new Error('precache '+url+' failed');
    await this.put(url,response);
  }
}

function makeRequest(url,{mode='no-cors',destination='',method='GET',range=false}={}){
  return {url,mode,destination,method,
    headers:{get:name=>name.toLowerCase()==='range'&&range?'bytes=0-1':null}};
}

/** A loaded worker, plus the handles a test needs to poke at it. */
function loadWorker({network}={}){
  const caches={
    store:new Map(),
    async open(name){
      if(!this.store.has(name)){const c=new FakeCache();c.scope=scope;this.store.set(name,c);}
      return this.store.get(name);
    },
    async keys(){return [...this.store.keys()];},
    async delete(name){return this.store.delete(name);},
    async match(request){
      const key=typeof request==='string'?new URL(key0(request),ORIGIN+'/service-worker.js').href:request.url;
      for(const cache of this.store.values()){const hit=await cache.match({url:key});if(hit) return hit;}
      return undefined;
    },
  };
  const key0=r=>r;
  const fetched=[];
  const listeners=new Map();
  const scope={
    location:new URL(ORIGIN+'/service-worker.js'),
    registration:{},
    clients:{claim:async()=>{}},
    skipWaiting:async()=>{scope.skipped=true;},
    skipped:false,
    addEventListener:(type,handler)=>{listeners.set(type,handler);},
    async fetch(request){
      const url=typeof request==='string'?request:request.url;
      fetched.push(url);
      const made=network?network(url):makeResponse('network:'+url);
      if(made instanceof Error) throw made;
      return made;
    },
  };
  scope.self=scope;
  const sandbox={self:scope,caches,fetch:(...a)=>scope.fetch(...a),Response:function(body,init){
    const status=(init&&init.status)||200;
    return makeResponse(body,{status});
  },URL,Promise,console};
  sandbox.globalThis=sandbox;
  vm.createContext(sandbox);
  vm.runInContext(fs.readFileSync(path.join(ROOT,'service-worker.js'),'utf8'),sandbox,
    {filename:'service-worker.js'});

  /** Dispatch a fetch event and return what the worker answered, or undefined
      when it declined to handle the request at all. */
  async function respond(request){
    let answer;
    listeners.get('fetch')({request,respondWith(p){answer=p;}});
    return answer===undefined?undefined:await answer;
  }
  async function life(type){
    let done;
    listeners.get(type)({waitUntil(p){done=p;}});
    await done;
  }
  return {scope,caches,fetched,respond,life,listeners};
}

const u=p=>ORIGIN+p;

test('cache-first is reserved for URLs whose bytes cannot change',async t=>{
  await t.test('a versioned asset is fetched once and then served from cache',async()=>{
    const w=loadWorker();
    const request=makeRequest(u('/ui/navigation.js?v=abc1234567'));
    const first=await w.respond(request);
    assert.equal(first.body,'network:'+request.url);
    const second=await w.respond(makeRequest(request.url));
    assert.equal(second.body,'network:'+request.url);
    assert.deepEqual(w.fetched,[request.url],'a second request must not reach the network');
  });

  await t.test('the same file without a version always reaches the network',async()=>{
    // This is the guard. If isVersioned() ever loosened to "any .js", a deploy
    // would be invisible to every browser that had already cached one.
    const w=loadWorker();
    const url=u('/ui/navigation.js');
    await w.respond(makeRequest(url));
    await w.respond(makeRequest(url));
    assert.deepEqual(w.fetched,[url,url],'an unversioned asset must be revalidated');
  });

  await t.test('an empty version is not a version',async()=>{
    const w=loadWorker();
    const url=u('/ui/stats.js?v=');
    await w.respond(makeRequest(url));
    await w.respond(makeRequest(url));
    assert.equal(w.fetched.length,2);
  });
});

test('a document is never served from cache while there is a network',async()=>{
  const w=loadWorker();
  const request=makeRequest(u('/ui/dashboard.html'),{mode:'navigate',destination:'document'});
  assert.equal((await w.respond(request)).body,'network:'+request.url);
  assert.equal((await w.respond(makeRequest(request.url,{mode:'navigate'}))).body,'network:'+request.url);
  assert.equal(w.fetched.length,2,'every navigation asks the network first');
});

test('offline, the dashboard falls back to the last copy this device saw',async()=>{
  const online=url=>makeResponse('dashboard for '+url);
  const w=loadWorker({network:online});
  const url=u('/ui/dashboard.html');
  await w.respond(makeRequest(url,{mode:'navigate',destination:'document'}));
  // The network dies; the same navigation must still answer with the page.
  w.scope.fetch=async()=>{throw new Error('offline');};
  const offline=await w.respond(makeRequest(url,{mode:'navigate',destination:'document'}));
  assert.equal(offline.body,'dashboard for '+url);
  assert.equal(offline.status,200);
});

test('offline with nothing cached answers with a page, not a browser error',async()=>{
  const w=loadWorker({network:()=>{throw new Error('offline');}});
  const answer=await w.respond(makeRequest(u('/ui/dashboard.html'),{mode:'navigate',destination:'document'}));
  assert.equal(answer.status,503);
  assert.match(String(answer.body),/offline/i);
});

test('the live scoreboard prefers the network and keeps the last good copy',async()=>{
  let body='fresh';
  const w=loadWorker({network:()=>makeResponse(body)});
  const url=u('/ui/data/live_scores.json');
  assert.equal((await w.respond(makeRequest(url))).body,'fresh');
  body='fresher';
  assert.equal((await w.respond(makeRequest(url))).body,'fresher','a cached scoreboard must not win');
  w.scope.fetch=async()=>{throw new Error('offline');};
  assert.equal((await w.respond(makeRequest(url))).body,'fresher','the last scoreboard survives offline');
});

test('requests a cache cannot answer honestly are passed straight through',async()=>{
  const w=loadWorker();
  assert.equal(await w.respond(makeRequest(u('/ui/x'),{method:'POST'})),undefined);
  assert.equal(await w.respond(makeRequest(u('/ui/video.mp4'),{range:true})),undefined);
  assert.equal(await w.respond(makeRequest('https://cdn.example.com/thing.js')),undefined,
    'an unrelated cross-origin host is none of the worker\'s business');
  assert.deepEqual(w.fetched,[]);
});

test('Google Fonts are cached so an installed app does not launch unstyled',async()=>{
  const w=loadWorker({network:()=>makeResponse('font',{type:'opaque',status:0})});
  const url='https://fonts.gstatic.com/s/graduate/v1.woff2';
  assert.equal((await w.respond(makeRequest(url))).body,'font');
  w.scope.fetch=async()=>{throw new Error('offline');};
  assert.equal((await w.respond(makeRequest(url))).body,'font','an opaque font response is kept');
});

test('activate deletes this project\'s older caches and nothing else',async()=>{
  const w=loadWorker();
  await w.caches.open('elocast-assets-v0');
  await w.caches.open('some-other-app');
  await w.life('activate');
  const names=await w.caches.keys();
  assert.ok(!names.includes('elocast-assets-v0'),'a superseded cache is removed');
  assert.ok(names.includes('some-other-app'),'another origin\'s cache is left alone');
});

test('install precaches the shell and survives a file that is not there',async()=>{
  const w=loadWorker({network:url=>url.endsWith('manifest.webmanifest')
    ? makeResponse('missing',{status:404}) : makeResponse('ok')});
  await w.life('install');
  assert.ok(w.scope.skipped,'a new worker takes over rather than waiting for every tab');
  const cached=[...w.caches.store.values()].flatMap(c=>[...c.entries.keys()]);
  assert.ok(cached.some(url=>url.endsWith('ui/dashboard.html')),'the dashboard is precached');
  assert.ok(!cached.some(url=>url.endsWith('manifest.webmanifest')),'a 404 is not cached');
});

test('the asset cache is bounded, oldest out first',async()=>{
  const w=loadWorker();
  const limit=Number(/MAX_ASSET_ENTRIES\s*=\s*(\d+)/.exec(
    fs.readFileSync(path.join(ROOT,'service-worker.js'),'utf8'))[1]);
  for(let i=0;i<limit+5;i++) await w.respond(makeRequest(u('/ui/data/g'+i+'.js?v=aaaaaaaaaa')));
  const cache=w.caches.store.get([...w.caches.store.keys()].find(k=>k.includes('assets')));
  assert.equal(cache.entries.size,limit);
  assert.ok(!(await cache.match({url:u('/ui/data/g0.js?v=aaaaaaaaaa')})),'the oldest entry was evicted');
  assert.ok(await cache.match({url:u('/ui/data/g'+(limit+4)+'.js?v=aaaaaaaaaa')}),'the newest is kept');
});
