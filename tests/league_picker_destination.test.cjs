/* Where a league tab actually takes a reader.
 *
 * Austin, 2026-11-05: "if I'm on the NCAAM page and try to go to the NFL
 * page, it kicks me to the CFB page." He was right, and the published build
 * had been doing it since the basketball page shipped: every JavaScript
 * picker carried its own map of league to page, and basketball.html's read
 *
 *     window.location.href = 'dashboard.html';
 *
 * for every league but its own. The registry now owns the map (`page` on each
 * entry, read through `pageFor`), but a registry nobody consults is no fix,
 * and the existing picker tests ask only WHICH leagues are offered -- never
 * where pressing one goes. That is the hole this file closes.
 *
 * It drives each page for real and asks the handler what it computed, because
 * jsdom cannot navigate and its "not implemented" error does not name a URL.
 * The spy wraps `pageFor` rather than replacing it, so the page's own answer
 * is what gets asserted, against page names written out as literals here.
 */

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { JSDOM, VirtualConsole } = require('jsdom');

const UI = path.join(__dirname, '..', 'ui');

// Every league, and the page it lives on, spelled out. Pinned to literals on
// purpose: comparing the picker against the registry's own `page` field would
// pass however wrong both are.
const PAGES = {
  cfb: 'dashboard.html',
  ncaambb: 'basketball.html',
  nfl: 'nfl.html',
  nba: 'nba.html'
};

// The pages that BUILD a picker in JavaScript, with the scripts each needs.
// The football header is static markup and is checked separately, below.
const PICKERS = {
  'basketball.html': ['ncaambb_loader.js', 'ncaambb_app.js'],
  'nfl.html': ['nfl_loader.js', 'ncaambb_app.js', 'nfl_app.js'],
  'nba.html': ['nba_loader.js', 'ncaambb_app.js', 'nba_app.js']
};

function settle(dom, done, tries) {
  const limit = tries === undefined ? 200 : tries;
  return new Promise((resolve, reject) => {
    let left = limit;
    const tick = () => {
      if (done()) return resolve();
      if (left <= 0) return reject(new Error('page never settled'));
      left -= 1;
      setTimeout(tick, 10);
    };
    tick();
  });
}

/* The page, loaded with its real scripts and no data behind it: the picker is
 * drawn before any manifest arrives, so a page with nothing staged still has
 * to answer this question. `navigations` collects jsdom's refusals to
 * navigate, which is how we know the handler assigned a location at all
 * rather than only computing one.
 */
async function load(page) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'league-dest-'));
  for (const name of [page].concat(PICKERS[page])) {
    fs.copyFileSync(path.join(UI, name), path.join(root, name));
  }
  const file = path.join(root, page);
  const navigations = [];
  const virtualConsole = new VirtualConsole();
  virtualConsole.on('jsdomError', error => {
    if (/navigation/.test(error.message)) navigations.push(error.message);
  });
  const dom = new JSDOM(fs.readFileSync(file, 'utf8'), {
    url: 'file://' + file,
    runScripts: 'dangerously',
    resources: 'usable',
    pretendToBeVisual: true,
    virtualConsole,
    beforeParse(window) {
      window.fetch = () => Promise.resolve({
        ok: false, json: () => Promise.reject(new Error('no manifest'))
      });
    }
  });
  await settle(dom, () => dom.window.document
    .querySelectorAll('#leagues button').length > 0);
  return { dom, navigations };
}

/* Wrap the registry's `pageFor` so a click reports the page it resolved.
 * Both pickers reach it as a property of the shared registry object at call
 * time (`app.pageFor(...)` / `leagues.pageFor(...)`), so replacing the
 * property is enough and the real function still does the work.
 */
function spy(window) {
  const registry = window.ncaambbApp;
  const original = registry.pageFor;
  const asked = [];
  registry.pageFor = function (slug) {
    const resolved = original.call(this, slug);
    asked.push([slug, resolved]);
    return resolved;
  };
  return asked;
}

function tabs(document) {
  return [...document.querySelectorAll('#leagues button')];
}

for (const page of Object.keys(PICKERS)) {
  const own = Object.keys(PAGES).find(slug => PAGES[slug] === page);

  test(`${page}'s league tabs open the league they name`, async () => {
    for (const tab of ['cfb', 'ncaambb', 'nfl', 'nba']) {
      if (tab === own) continue;
      const { dom, navigations } = await load(page);
      const button = tabs(dom.window.document)
        .find(node => node.dataset.league === tab);
      assert.ok(button, `${page} offers no ${tab} tab`);
      const asked = spy(dom.window);
      button.click();
      assert.deepEqual(asked, [[tab, PAGES[tab]]],
        `${page}'s ${tab} tab must resolve ${PAGES[tab]} through the registry`);
      // And it must actually go there. Without this, deleting the assignment
      // and keeping the lookup would still pass.
      assert.equal(navigations.length, 1,
        `${page}'s ${tab} tab computed a page but never navigated`);
      dom.window.close();
    }
  });

  test(`${page}'s own league tab navigates nowhere`, async () => {
    const { dom, navigations } = await load(page);
    const button = tabs(dom.window.document)
      .find(node => node.dataset.league === own);
    assert.ok(button, `${page} does not press its own league`);
    assert.equal(button.getAttribute('aria-pressed'), 'true');
    const asked = spy(dom.window);
    button.click();
    assert.deepEqual(asked, [], 'the page you are on resolves no page');
    assert.equal(navigations.length, 0, 'and reloads nothing');
    dom.window.close();
  });
}

test('the football header links each league to its own page', () => {
  // Static markup, so this is the same question asked of the DOM the shell
  // ships. ui/dashboard.html is generated from it and is checked too: the
  // header a reader meets is the built one.
  for (const name of ['dashboard_shell.html', 'dashboard.html']) {
    const file = path.join(UI, name);
    if (!fs.existsSync(file)) continue;  // dashboard.html is a build product
    const document = new JSDOM(fs.readFileSync(file, 'utf8')).window.document;
    const links = [...document.querySelectorAll('#league-selector a')];
    assert.ok(links.length > 1, `${name} draws no league selector`);
    for (const link of links) {
      const slug = link.dataset.league;
      const href = link.getAttribute('href');
      assert.ok(PAGES[slug], `${name} offers unknown league ${slug}`);
      if (slug === 'cfb') {
        // Football IS this page, so its tab is a route, not a page.
        assert.ok(href.startsWith('#'),
          `${name} sends football off the page it is already on`);
      } else {
        assert.equal(href, PAGES[slug],
          `${name} sends ${slug} to ${href}`);
      }
    }
  }
});
