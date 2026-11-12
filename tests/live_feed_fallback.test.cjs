// The live scoreboard's source is a RUNTIME decision, and the page falls back
// to the committed snapshot by itself.
//
// This is the rollback plan for the live backend, so these tests drive the real
// functions out of ui/dashboard_shell.html against a fake fetch rather than
// asserting on source text. The property they exist to protect is narrow and
// worth stating: pointing this page at a new live endpoint can never show a
// reader worse data than leaving it alone, and a backup snapshot is never
// presented as the live feed.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const shell = fs.readFileSync(path.join(__dirname, '../ui/dashboard_shell.html'), 'utf8');

function sourceOf(name) {
  const match = shell.match(new RegExp(`function ${name}\\([^)]*\\) \\{[\\s\\S]*?\\n\\}`));
  assert.ok(match, `missing function ${name}`);
  return match[0];
}
function declarationOf(name) {
  const match = shell.match(new RegExp(`^(?:const|let) ${name} = [^\\n]*;$`, 'm'));
  assert.ok(match, `missing declaration ${name}`);
  return match[0];
}

const STATIC = 'data/live_scores.json';

// A vm-context object is not reference-equal to an identical host object, so
// deepStrictEqual rejects it across the realm boundary. Compare the fields.
function assertSource(actual, expected, message) {
  assert.equal(actual.source, expected.source, message);
  assert.equal(actual.fellBack, expected.fellBack, message);
  assert.equal(actual.reason, expected.reason, message);
}

/**
 * Build a context holding the real implementations.
 *
 * Every function the chain calls is lifted, not stubbed. A stub here would make
 * the suite pass while the shipped chain was broken -- which is exactly how a
 * previous version of these tests missed a helper that had started calling a
 * new one.
 */
function context({baked = '', search = '', stored = null, responses = {}, now = 1000000} = {}) {
  const requested = [];
  const ctx = {
    console,
    URLSearchParams,
    Number,
    Date: class extends Date {
      constructor(...args) { super(...(args.length ? args : [now])); }
      static now() { return now; }
    },
    location: {search},
    localStorage: {
      getItem(key) {
        if (stored === 'throws') throw new Error('site data blocked');
        return key === 'elocast.liveFeed' ? stored : null;
      },
    },
    fetch(url) {
      const base = String(url).split('?')[0];
      requested.push(base);
      const answer = responses[base];
      if (answer === undefined) return Promise.reject(new Error('no route for ' + base));
      if (answer instanceof Error) return Promise.reject(answer);
      if (answer === 'not-ok') {
        // It answers json() too. A 503 page that happens to parse is exactly
        // the case the r.ok check exists for, and a fake with no json() would
        // pass whether that check was there or not.
        return Promise.resolve({ok: false, status: 503, json: () => Promise.resolve(snapshot(0))});
      }
      if (answer === 'bad-json') {
        return Promise.resolve({ok: true, json: () => Promise.reject(new SyntaxError('not JSON'))});
      }
      return Promise.resolve({ok: true, json: () => Promise.resolve(answer)});
    },
  };
  vm.createContext(ctx);
  vm.runInContext([
    declarationOf('LIVE_FEED_URL').replace('__LIVE_FEED_URL__', baked),
    declarationOf('LIVE_FEED_STATIC'),
    declarationOf('LIVE_FEED_STALE_MS'),
    declarationOf('liveFeedSource'),
    sourceOf('resolveLiveFeed'),
    sourceOf('liveFeedUrl'),
    sourceOf('liveSnapshotAge'),
    sourceOf('fetchLiveJson'),
    sourceOf('fetchLiveSnapshot'),
    'this.resolveLiveFeed = resolveLiveFeed;',
    'this.liveFeedUrl = liveFeedUrl;',
    'this.fetchLiveSnapshot = fetchLiveSnapshot;',
    'this.feedSource = () => liveFeedSource;',
    'this.staleMs = LIVE_FEED_STALE_MS;',
  ].join('\n'), ctx);
  ctx.requested = requested;
  return ctx;
}

/** A snapshot stamped `minutes` before the context's frozen clock. */
function snapshot(minutes, extra = {}) {
  return Object.assign({
    source: 'CollegeFootballData.com',
    updated_at: new Date(1000000 - minutes * 60000).toISOString(),
    games: [],
  }, extra);
}

// --- every live read goes through one place --------------------------------

test('every live fetch goes through fetchLiveSnapshot, and none hardcodes the path', () => {
  // Three call sites: the rating-freshness hydrate, the live board, and the
  // open game page. If one of them went back to its own fetch, it would keep
  // reading the static file after the others moved, with no fallback and no
  // source reporting.
  assert.equal((shell.match(/fetchLiveSnapshot\(\)/g) || []).length, 4,
    'expected the definition plus exactly three call sites');
  assert.equal((shell.match(/fetch\(liveFeedUrl\(\)/g) || []).length, 0,
    'a live fetch still bypasses fetchLiveSnapshot');
  assert.equal(shell.includes("data/live_scores.json?t="), false,
    'a live fetch still hardcodes the snapshot path');
});

// --- runtime resolution -----------------------------------------------------

test('with nothing configured the page reads the committed snapshot, as it always did', () => {
  const ctx = context();
  assert.equal(ctx.resolveLiveFeed(), STATIC);
  assert.match(ctx.liveFeedUrl(), /^data\/live_scores\.json\?t=\d+$/);
});

test('the baked value is the default when there is one', () => {
  const ctx = context({baked: 'https://scores.example.com/live'});
  assert.equal(ctx.resolveLiveFeed(), 'https://scores.example.com/live');
  // Still cache-busted, and an endpoint that already carries a query keeps it:
  // appending a second "?" would make the whole thing unparseable.
  assert.match(ctx.liveFeedUrl(), /^https:\/\/scores\.example\.com\/live\?t=\d+$/);
  const q = context({baked: 'https://scores.example.com/live?season=2026'});
  assert.match(q.liveFeedUrl(), /^https:\/\/scores\.example\.com\/live\?season=2026&t=\d+$/);
});

test('?liveFeed=static reads the committed snapshot even when an endpoint is baked in', () => {
  // This is the rollback lever, and it needs no deploy.
  const ctx = context({baked: 'https://scores.example.com/live', search: '?liveFeed=static'});
  assert.equal(ctx.resolveLiveFeed(), STATIC);
});

test('?liveFeed=live reads the endpoint, which is the canary with no deploy', () => {
  const ctx = context({baked: 'https://scores.example.com/live', search: '?section=live&liveFeed=live'});
  assert.equal(ctx.resolveLiveFeed(), 'https://scores.example.com/live');
  // Case and surrounding whitespace do not matter; a shared link gets mangled.
  assert.equal(context({baked: 'https://x.test/l', search: '?liveFeed=LIVE'}).resolveLiveFeed(), 'https://x.test/l');
});

test('?liveFeed=live with nothing baked in falls through to the snapshot, not to nothing', () => {
  assert.equal(context({search: '?liveFeed=live'}).resolveLiveFeed(), STATIC);
});

test('the query parameter NEVER takes a URL, only the two words', () => {
  // A URL in a query string is attacker-supplied. Anyone could share a link
  // that made this page draw scores from their server and look like ours, so
  // an arbitrary endpoint requires localStorage and therefore a console.
  for (const hostile of [
    '?liveFeed=https://evil.example.com/scores.json',
    '?liveFeed=//evil.example.com/scores.json',
    '?liveFeed=http://evil.example.com/',
    '?liveFeed=data:application/json,[]',
    '?liveFeed=../../etc/passwd',
    '?liveFeed=javascript:alert(1)',
  ]) {
    assert.equal(context({search: hostile}).resolveLiveFeed(), STATIC, hostile);
    assert.equal(context({baked: 'https://ours.test/l', search: hostile}).resolveLiveFeed(),
      'https://ours.test/l', hostile + ' with a baked endpoint');
  }
});

test('a localStorage override is honoured, and only for an absolute https URL', () => {
  assert.equal(context({stored: 'https://dev.example.com/live'}).resolveLiveFeed(),
    'https://dev.example.com/live');
  // It beats the baked default, which is the point of a development override.
  assert.equal(context({baked: 'https://prod.test/l', stored: 'https://dev.test/l'}).resolveLiveFeed(),
    'https://dev.test/l');
  for (const bad of ['http://dev.test/l', '//dev.test/l', 'data/other.json', '', '   ', 'javascript:1',
    'HTTPS://dev.test/l']) {
    assert.equal(context({stored: bad}).resolveLiveFeed(), STATIC, JSON.stringify(bad));
  }
});

test('the query words beat a localStorage override, which is what makes them levers', () => {
  // Without this, `?liveFeed=live` would be indistinguishable from no
  // parameter at all whenever nothing else is set -- and the rollback lever
  // would silently lose to whatever a console had left behind.
  const stat = context({baked: 'https://p.test/l', stored: 'https://d.test/l', search: '?liveFeed=static'});
  assert.equal(stat.resolveLiveFeed(), STATIC);
  const live = context({baked: 'https://p.test/l', stored: 'https://d.test/l', search: '?liveFeed=live'});
  assert.equal(live.resolveLiveFeed(), 'https://p.test/l');
  // And case still does not matter, which only shows here: anywhere else both
  // spellings fall through to the same answer.
  for (const spelling of ['LIVE', 'Live', ' live ']) {
    assert.equal(context({baked: 'https://p.test/l', stored: 'https://d.test/l',
      search: '?liveFeed=' + spelling}).resolveLiveFeed(), 'https://p.test/l', spelling);
  }
  for (const spelling of ['STATIC', 'Static', ' static ']) {
    assert.equal(context({baked: 'https://p.test/l', stored: 'https://d.test/l',
      search: '?liveFeed=' + spelling}).resolveLiveFeed(), STATIC, spelling);
  }
});

test('an unreadable query string does not break the page', () => {
  // `location` is absent in a worker and the getter can throw in an embedded
  // context. Resolution has to survive it rather than taking the scoreboard
  // down, and the stored override must still be reached afterwards.
  const noLocation = context({baked: 'https://p.test/l'});
  delete noLocation.location;
  assert.equal(noLocation.resolveLiveFeed(), 'https://p.test/l');

  const throwing = context({baked: 'https://p.test/l', stored: 'https://d.test/l'});
  Object.defineProperty(throwing, 'location', {get() { throw new Error('blocked'); }});
  assert.equal(throwing.resolveLiveFeed(), 'https://d.test/l');
});

test('blocked site data does not break the page', () => {
  // localStorage throws in a private window and wherever site data is blocked.
  // Resolution has to survive it rather than taking the scoreboard down.
  assert.equal(context({stored: 'throws'}).resolveLiveFeed(), STATIC);
  assert.equal(context({baked: 'https://p.test/l', stored: 'throws'}).resolveLiveFeed(), 'https://p.test/l');
});

// --- the fallback -----------------------------------------------------------

test('a fresh live feed is used, and the static file is not even requested', async () => {
  const ctx = context({
    baked: 'https://live.test/feed',
    responses: {'https://live.test/feed': snapshot(1)},
  });
  const data = await ctx.fetchLiveSnapshot();
  assert.equal(data.updated_at, snapshot(1).updated_at);
  assert.deepEqual(ctx.requested, ['https://live.test/feed']);
  assertSource(ctx.feedSource(), {source: 'live', fellBack: false, reason: null});
});

test('a live feed that fails falls back to the committed snapshot and says so', async () => {
  for (const failure of [new Error('network down'), 'not-ok', 'bad-json']) {
    const ctx = context({
      baked: 'https://live.test/feed',
      responses: {'https://live.test/feed': failure, [STATIC]: snapshot(30)},
    });
    const data = await ctx.fetchLiveSnapshot();
    assert.equal(data.updated_at, snapshot(30).updated_at, String(failure));
    assert.equal(ctx.feedSource().source, 'static');
    assert.equal(ctx.feedSource().fellBack, true);
    assert.match(ctx.feedSource().reason, /could not be read/);
  }
});

test('a stale live feed loses to a newer snapshot', async () => {
  const ctx = context({
    baked: 'https://live.test/feed',
    responses: {'https://live.test/feed': snapshot(60), [STATIC]: snapshot(5)},
  });
  const data = await ctx.fetchLiveSnapshot();
  assert.equal(data.updated_at, snapshot(5).updated_at);
  assert.equal(ctx.feedSource().fellBack, true);
  assert.match(ctx.feedSource().reason, /behind/);
});

test('a stale live feed still WINS over an even older snapshot', async () => {
  // The property that makes the canary safe: pointing the page at the live
  // endpoint cannot show a reader worse data than leaving it alone. A fixed
  // threshold alone would have handed this reader the older file.
  const ctx = context({
    baked: 'https://live.test/feed',
    responses: {'https://live.test/feed': snapshot(60), [STATIC]: snapshot(600)},
  });
  const data = await ctx.fetchLiveSnapshot();
  assert.equal(data.updated_at, snapshot(60).updated_at);
  assert.equal(ctx.feedSource().source, 'live');
  assert.equal(ctx.feedSource().fellBack, false);
  assert.match(ctx.feedSource().reason, /still newer/);
});

test('equal timestamps keep the live feed rather than churning to the backup', async () => {
  const ctx = context({
    baked: 'https://live.test/feed',
    responses: {'https://live.test/feed': snapshot(60), [STATIC]: snapshot(60)},
  });
  await ctx.fetchLiveSnapshot();
  assert.equal(ctx.feedSource().source, 'live');
});

test('the staleness trigger is the page\'s own ten minutes, either side of it', async () => {
  const ctx = context({baked: 'https://live.test/feed'});
  assert.equal(ctx.staleMs, 10 * 60 * 1000);

  const justFresh = context({
    baked: 'https://live.test/feed',
    responses: {'https://live.test/feed': snapshot(9.9), [STATIC]: snapshot(0)},
  });
  await justFresh.fetchLiveSnapshot();
  assert.deepEqual(justFresh.requested, ['https://live.test/feed'], 'a fresh feed triggered a backup fetch');

  const justStale = context({
    baked: 'https://live.test/feed',
    responses: {'https://live.test/feed': snapshot(10.1), [STATIC]: snapshot(0)},
  });
  await justStale.fetchLiveSnapshot();
  assert.deepEqual(justStale.requested, ['https://live.test/feed', STATIC],
    'a stale feed was not checked against the backup');
});

test('a live feed with no usable timestamp is checked against the backup', async () => {
  // A snapshot that cannot say when it was taken is not a fresh snapshot.
  for (const stamp of [undefined, null, '', 'recently', 0]) {
    const ctx = context({
      baked: 'https://live.test/feed',
      responses: {
        'https://live.test/feed': Object.assign(snapshot(1), {updated_at: stamp}),
        [STATIC]: snapshot(20),
      },
    });
    const data = await ctx.fetchLiveSnapshot();
    assert.equal(data.updated_at, snapshot(20).updated_at, JSON.stringify(stamp));
    assert.equal(ctx.feedSource().fellBack, true);
    assert.match(ctx.feedSource().reason, /no usable timestamp/);
  }
});

test('when neither snapshot has a usable timestamp the live answer is kept', async () => {
  // There is no evidence the backup is any better, and swapping sources on no
  // evidence would just make the reported source untrue.
  const ctx = context({
    baked: 'https://live.test/feed',
    responses: {
      'https://live.test/feed': Object.assign(snapshot(1), {updated_at: null}),
      [STATIC]: Object.assign(snapshot(1), {updated_at: 'nonsense'}),
    },
  });
  await ctx.fetchLiveSnapshot();
  assert.equal(ctx.feedSource().source, 'live');
  assert.equal(ctx.feedSource().fellBack, false);
});

test('a stale live feed is still served when the backup cannot be read either', async () => {
  const ctx = context({
    baked: 'https://live.test/feed',
    responses: {'https://live.test/feed': snapshot(90), [STATIC]: new Error('404')},
  });
  const data = await ctx.fetchLiveSnapshot();
  assert.equal(data.updated_at, snapshot(90).updated_at);
  assert.equal(ctx.feedSource().source, 'live');
  assert.match(ctx.feedSource().reason, /backup could not be read/);
});

test('both sources failing rejects, so each call site runs its own error path', async () => {
  const ctx = context({
    baked: 'https://live.test/feed',
    responses: {'https://live.test/feed': new Error('down'), [STATIC]: new Error('down')},
  });
  await assert.rejects(() => ctx.fetchLiveSnapshot());
});

test('reading the static file directly never reports a fallback', async () => {
  // Nothing to fall back TO, and claiming a fallback would put "backup feed" on
  // a page that is reading its normal source.
  const ctx = context({responses: {[STATIC]: snapshot(400)}});
  const data = await ctx.fetchLiveSnapshot();
  assert.equal(data.updated_at, snapshot(400).updated_at);
  assert.deepEqual(ctx.requested, [STATIC]);
  assertSource(ctx.feedSource(), {source: 'static', fellBack: false, reason: null});
});

test('?liveFeed=static reads the snapshot once, with no second request', async () => {
  const ctx = context({
    baked: 'https://live.test/feed',
    search: '?liveFeed=static',
    responses: {[STATIC]: snapshot(2), 'https://live.test/feed': snapshot(0)},
  });
  await ctx.fetchLiveSnapshot();
  assert.deepEqual(ctx.requested, [STATIC]);
  assert.equal(ctx.feedSource().fellBack, false);
});

// --- the page says it -------------------------------------------------------

test('the live status line reports a fallback, and does not invent one otherwise', () => {
  // Driving the real status assignment needs the whole hydrate; this holds the
  // two things that would make the page lie: the word appearing when nothing
  // fell back, and it never appearing at all.
  const assignment = shell.match(/const backup = liveFeedSource[^\n]*\n[^\n]*status\.textContent = [\s\S]*?\.filter\(Boolean\)\.join\([^\n]*\);/);
  assert.ok(assignment, 'the live status line no longer reports the feed source');
  const line = assignment[0];
  assert.match(line, /liveFeedSource && liveFeedSource\.fellBack/,
    'the status line reports a source other than an actual fallback');
  assert.match(line, /'backup feed'/);

  const run = (fellBack, stale) => {
    const ctx = {liveFeedSource: fellBack === null ? null : {fellBack}, status: {}, Boolean};
    vm.createContext(ctx);
    vm.runInContext(`const validStamp = ${stale !== null}; const stale = ${JSON.stringify(stale)};\n` + line, ctx);
    return ctx.status.textContent;
  };
  assert.equal(run(true, '45m ago'), 'Updated 45m ago · backup feed');
  assert.equal(run(true, ''), 'backup feed');
  assert.equal(run(false, '45m ago'), 'Updated 45m ago');
  assert.equal(run(false, ''), '');
  assert.equal(run(null, ''), '');
  assert.equal(run(true, null), 'Not active yet · backup feed');
});
