// Favorite teams: what survives a round trip through a device's storage, and
// what a page gets when that storage is missing, full or full of nonsense.
//
// The real ui/favorites.js is loaded; the storage is a stand-in, because the
// failure modes worth testing here are the ones a real localStorage will not
// reproduce on demand (private browsing, a quota error, a hand-edited value).
const {test}=require('node:test');
const assert=require('node:assert/strict');
const favorites=require('../ui/favorites.js');

function memoryStorage(initial){
  const map=new Map(initial?Object.entries(initial):[]);
  return {
    map,
    getItem:key=>map.has(key)?map.get(key):null,
    setItem:(key,value)=>{map.set(key,String(value));},
    removeItem:key=>{map.delete(key);},
  };
}

test('a followed team survives a round trip',()=>{
  const storage=memoryStorage();
  assert.deepEqual(favorites.read(storage),[]);
  assert.deepEqual(favorites.add(storage,'ohio-state'),['ohio-state']);
  assert.ok(favorites.has(storage,'ohio-state'));
  // A second reader of the same device sees it: the list is in storage, not in
  // a variable this module happens to be holding.
  assert.deepEqual(favorites.read(memoryStorage(Object.fromEntries(storage.map))),['ohio-state']);
});

test('the order teams were followed in is the order they come back',()=>{
  const storage=memoryStorage();
  ['byu','ohio-state','miami-fl'].forEach(slug=>favorites.add(storage,slug));
  assert.deepEqual(favorites.read(storage),['byu','ohio-state','miami-fl']);
  favorites.remove(storage,'ohio-state');
  favorites.add(storage,'ohio-state');
  assert.deepEqual(favorites.read(storage),['byu','miami-fl','ohio-state']);
});

test('following twice is following once',()=>{
  const storage=memoryStorage();
  favorites.add(storage,'byu');
  favorites.add(storage,'byu');
  assert.deepEqual(favorites.read(storage),['byu']);
});

test('toggle adds then removes',()=>{
  const storage=memoryStorage();
  assert.deepEqual(favorites.toggle(storage,'byu'),['byu']);
  assert.deepEqual(favorites.toggle(storage,'byu'),[]);
  assert.equal(favorites.has(storage,'byu'),false);
});

test('only team slugs are kept',()=>{
  const storage=memoryStorage();
  // Everything here could reach storage: a stale format, another tab's bug, or
  // someone editing the value by hand in a developer console.
  for(const bad of ['Ohio State','../../etc/passwd','<script>',42,null,'',{slug:'byu'},'-leading']){
    favorites.add(storage,bad);
  }
  assert.deepEqual(favorites.read(storage),[]);
  favorites.add(storage,'ohio-state');
  assert.deepEqual(favorites.read(storage),['ohio-state']);
});

test('a corrupted stored value reads as no favorites, never as a throw',()=>{
  for(const raw of ['not json','{"a":1}','null','[1,2,3]','"byu"']){
    const storage=memoryStorage({[favorites.KEY]:raw});
    assert.deepEqual(favorites.read(storage),[],raw);
  }
  // And a duplicate written by an older version is deduped on the way out.
  const dupes=memoryStorage({[favorites.KEY]:'["byu","byu","ohio-state"]'});
  assert.deepEqual(favorites.read(dupes),['byu','ohio-state']);
});

test('storage that is absent or refuses to answer reads as no favorites',()=>{
  const throwing={getItem(){throw new Error('SecurityError');},setItem(){throw new Error('SecurityError');}};
  assert.deepEqual(favorites.read(throwing),[]);
  assert.equal(favorites.has(throwing,'byu'),false);
  // The caller still gets the list it just asked for, so the page it draws
  // matches what the user did -- it simply will not be there next visit.
  assert.deepEqual(favorites.add(throwing,'byu'),['byu']);
});

test('the list is capped, and the cap is applied on the way in and out',()=>{
  const storage=memoryStorage();
  const many=Array.from({length:favorites.MAX+10},(_,i)=>'team-'+i);
  many.forEach(slug=>favorites.add(storage,slug));
  assert.equal(favorites.read(storage).length,favorites.MAX);
  const overfull=memoryStorage({[favorites.KEY]:JSON.stringify(many)});
  assert.equal(favorites.read(overfull).length,favorites.MAX);
});

test('removing a team that was never followed changes nothing',()=>{
  const storage=memoryStorage();
  favorites.add(storage,'byu');
  assert.deepEqual(favorites.remove(storage,'ohio-state'),['byu']);
});
