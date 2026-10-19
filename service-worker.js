/* Elocast's service worker: the offline shell, and the reason the site can be
   installed on a phone home screen.

   It lives at the repository root rather than beside the page it serves,
   because a service worker's scope can never be wider than its own directory.
   From here it covers index.html and the whole of ui/; from ui/ it could not
   cover the root redirect that people actually paste.

   ## The caching rules, and why each is what it is

   A stale service worker is the classic way to serve a week-old site forever,
   so the one dangerous strategy -- cache-first, which never asks the network
   again -- is allowed for exactly one kind of URL:

     versioned assets (?v=<hash>)   cache-first
         Every generated asset this site loads carries ?v=sha256(file)[:10],
         stamped in by build_dashboard.py and src/export_static_data.py. The
         URL therefore CHANGES whenever the bytes change, so a cached copy can
         never be stale: a new deploy asks for a URL this cache has never seen.
         isVersioned() is the guard, and tests/service_worker.test.cjs pins it.

     documents (navigations and .html)   network-first
         dashboard.html has no hash of its own -- it is the thing the hashes
         are stamped into -- so it is fetched fresh whenever there is a
         network, and only falls back to the cached copy when there is not.
         That fallback is the offline screen: the dashboard carries its
         ratings payload inline, so an offline launch shows the last ratings
         this browser saw rather than a browser error.

     ui/data/live_scores.json   network-first
         A scoreboard is worth nothing when it is stale, but a last-known
         scoreboard beats an empty one, so the last good copy is kept.

     everything else same-origin   stale-while-revalidate
         The brand PNGs, the manifest, the state outlines: served from cache
         for speed, refetched in the background so the next load is current.

     Google Fonts   stale-while-revalidate
         The only cross-origin hosts this page loads. Cached so an installed
         app does not launch unstyled on a plane. Opaque responses are cached
         as they come; nothing else cross-origin is touched.

   Anything that is not a GET, and any request carrying a Range header, is
   passed straight through: a cache cannot answer either one honestly.

   ## Versioning

   CACHE_VERSION names this file's own rules, not the site's data. Bump it
   when a rule above changes; activate then deletes every cache that does not
   belong to the current version, and the new worker takes over at once
   (skipWaiting + claim) rather than waiting for every tab to close. */

const CACHE_VERSION = 'v1';
const DOCUMENT_CACHE = `elocast-documents-${CACHE_VERSION}`;
const ASSET_CACHE = `elocast-assets-${CACHE_VERSION}`;
const CURRENT_CACHES = [DOCUMENT_CACHE, ASSET_CACHE];

// A season of browsing can touch thousands of per-game and per-player files.
// The asset cache is trimmed to the most recently added of them, oldest out
// first, so an installed app cannot grow without limit on someone's phone.
const MAX_ASSET_ENTRIES = 600;

const FONT_HOSTS = ['fonts.googleapis.com', 'fonts.gstatic.com'];

// Precached on install, so a first offline launch has a page and an icon
// without having had to visit one first. Paths are relative to this file,
// which is also the scope, so they work under a project path or a bare domain.
//
// Documents and assets go to their own caches rather than all landing in one.
// caches.match() searches every cache and returns the FIRST hit, in creation
// order, so an asset precached into the document cache would keep winning over
// the fresh copy stale-while-revalidate writes to the asset cache, and the
// deploy that changed it would never reach anybody.
const PRECACHE_DOCUMENTS = ['index.html', 'ui/dashboard.html'];
const PRECACHE_ASSETS = ['manifest.webmanifest', 'ui/brand/mark.svg',
                         'ui/brand/icon-192.png', 'ui/brand/icon-512.png'];

// The dashboard's own <script src="...?v=hash"> files, which it cannot render
// without: with none of them the page reaches an undefined CfbNavigation and
// draws nothing. They are not in the list above because their URLs change on
// every deploy, and they are not picked up by the normal runtime rules either
// -- the worker registers on `load`, by which time this visit has already
// fetched them uncontrolled. So install reads the page it just cached and
// takes the script URLs from it. Nothing here is hardcoded, so the list cannot
// drift from what the page actually loads.
const SCRIPT_SRC = /<script[^>]+src=["']([^"']+)["']/gi;

async function precacheRuntimeScripts(documents) {
  const page = await documents.match('ui/dashboard.html');
  if (!page) return;
  const html = await page.text();
  const assets = await caches.open(ASSET_CACHE);
  const urls = new Set();
  let match;
  while ((match = SCRIPT_SRC.exec(html)) !== null) {
    const src = match[1];
    // Same-origin only: a cross-origin script would need CORS to be cached,
    // and the page has none of those.
    const url = new URL(src, new URL('ui/dashboard.html', self.location.href));
    if (url.origin === self.location.origin) urls.add(url.href);
  }
  await Promise.all([...urls].map(url => assets.add(url).catch(() => null)));
}

const OFFLINE_HTML = `<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Elocast is offline</title>
<style>html{background:#0e1512;color:#eef1ec;font-family:system-ui,sans-serif}
body{margin:0;min-height:100vh;display:flex;align-items:center;justify-content:center;padding:24px}
div{max-width:30rem;text-align:center}h1{font-size:1.4rem;margin:0 0 .6rem}
p{color:#a8b3ac;line-height:1.5;margin:0}</style></head>
<body><div><h1>Elocast is offline</h1>
<p>This device has no connection, and no copy of the ratings has been saved yet.
Open Elocast once while online and it will work offline after that.</p></div></body></html>`;

function offlineResponse() {
  return new Response(OFFLINE_HTML, {
    status: 503,
    headers: {'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store'},
  });
}

/** Cache-first is safe for exactly these: the URL changes when the bytes do. */
function isVersioned(url) {
  return url.searchParams.has('v') && url.searchParams.get('v') !== '';
}

function isDocument(request, url) {
  return request.mode === 'navigate' ||
    (request.destination === 'document') ||
    url.pathname.endsWith('.html');
}

function isLiveFeed(url) {
  return url.pathname.endsWith('/data/live_scores.json');
}

function isFont(url) {
  return FONT_HOSTS.includes(url.hostname);
}

/** A response worth keeping: a real 200, or an opaque cross-origin one. */
function cacheable(response) {
  if (!response) return false;
  if (response.type === 'opaque') return true;
  return response.ok && response.status === 200;
}

async function putAndTrim(cacheName, request, response) {
  const cache = await caches.open(cacheName);
  await cache.put(request, response);
  if (cacheName !== ASSET_CACHE) return;
  const keys = await cache.keys();
  // cache.keys() is in insertion order, so the front of the list is the least
  // recently ADDED -- which is the only ordering a Cache can offer.
  for (let i = 0; i < keys.length - MAX_ASSET_ENTRIES; i++) await cache.delete(keys[i]);
}

async function cacheFirst(request) {
  const hit = await caches.match(request);
  if (hit) return hit;
  const response = await fetch(request);
  if (cacheable(response)) await putAndTrim(ASSET_CACHE, request, response.clone());
  return response;
}

async function networkFirst(request, cacheName) {
  try {
    const response = await fetch(request);
    if (cacheable(response)) await putAndTrim(cacheName, request, response.clone());
    return response;
  } catch (err) {
    const hit = await caches.match(request);
    if (hit) return hit;
    throw err;
  }
}

async function documentRequest(request) {
  try {
    return await networkFirst(request, DOCUMENT_CACHE);
  } catch (err) {
    // Nothing cached for this exact URL. An installed app launched offline on
    // a deep link lands here, and the dashboard it already has is a better
    // answer than a browser error page.
    const dashboard = await caches.match('ui/dashboard.html');
    return dashboard || offlineResponse();
  }
}

async function staleWhileRevalidate(request, event) {
  const hit = await caches.match(request);
  const network = fetch(request)
    .then(response => cacheable(response)
      ? putAndTrim(ASSET_CACHE, request, response.clone()).then(() => response)
      : response)
    .catch(() => null);
  // The whole point of this strategy is the refetch, and respondWith() settling
  // lets the browser kill the worker -- so the event has to stay open until the
  // new copy is actually written, or the "revalidate" half never happens.
  if (event) event.waitUntil(network);
  if (hit) return hit;
  const response = await network;
  return response || offlineResponse();
}

self.addEventListener('install', event => {
  event.waitUntil((async () => {
    const [documents, assets] = await Promise.all([caches.open(DOCUMENT_CACHE),
                                                   caches.open(ASSET_CACHE)]);
    // Never fatal: a precache entry that 404s on a branch where it has not
    // been generated yet must not stop the worker installing.
    await Promise.all([
      ...PRECACHE_DOCUMENTS.map(path => documents.add(path).catch(() => null)),
      ...PRECACHE_ASSETS.map(path => assets.add(path).catch(() => null)),
    ]);
    await precacheRuntimeScripts(documents).catch(() => null);
    await self.skipWaiting();
  })());
});

self.addEventListener('activate', event => {
  event.waitUntil((async () => {
    const names = await caches.keys();
    await Promise.all(names.filter(name => name.startsWith('elocast-') && !CURRENT_CACHES.includes(name))
      .map(name => caches.delete(name)));
    await self.clients.claim();
  })());
});

self.addEventListener('fetch', event => {
  const request = event.request;
  if (request.method !== 'GET' || request.headers.get('range')) return;
  let url;
  try {
    url = new URL(request.url);
  } catch (err) {
    return;
  }
  if (url.protocol !== 'http:' && url.protocol !== 'https:') return;
  const sameOrigin = url.origin === self.location.origin;
  if (!sameOrigin && !isFont(url)) return;

  if (sameOrigin && isDocument(request, url)) event.respondWith(documentRequest(request));
  else if (sameOrigin && isLiveFeed(url)) event.respondWith(networkFirst(request, ASSET_CACHE));
  else if (sameOrigin && isVersioned(url)) event.respondWith(cacheFirst(request));
  else event.respondWith(staleWhileRevalidate(request, event));
});

// The page asks for this when the user pulls the scoreboard, so a new deploy
// is picked up without waiting for every tab to be closed.
self.addEventListener('message', event => {
  if (event.data === 'skip-waiting') self.skipWaiting();
});
