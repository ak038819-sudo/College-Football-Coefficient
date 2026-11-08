/* The files a hand-written page needs, read from the page itself.

   Ten test files each kept their own hard-coded list of the scripts to copy
   into a temporary site -- `['nfl.html', 'nfl_loader.js', 'nfl_app.js',
   'ncaambb_app.js']` and nine near-copies of it. Adding one module to a page
   broke all ten at once, each with "the page never rendered", because a list
   nobody updates is a list that is wrong.

   The page already states what it needs. This reads that, so a module added
   to a page is there for every test of it with no list to remember. */
const fs = require('node:fs');
const path = require('node:path');

const SCRIPT = /<script src="([A-Za-z0-9_.\-]+\.js)(?:\?v=[0-9a-f]+)?"/g;
const STYLE = /<link rel="stylesheet" href="([A-Za-z0-9_.\-]+\.css)(?:\?v=[0-9a-f]+)?"/g;

/* Every local file `page` loads, the page itself first. */
function assetsOf(uiDir, page) {
  const html = fs.readFileSync(path.join(uiDir, page), 'utf8');
  const names = [page];
  for (const pattern of [SCRIPT, STYLE]) {
    pattern.lastIndex = 0;
    let match;
    while ((match = pattern.exec(html)) !== null) {
      // A page may name a file this tree does not have; that is the page's
      // bug to report, not a reason to throw here.
      if (fs.existsSync(path.join(uiDir, match[1]))) names.push(match[1]);
    }
  }
  return names;
}

/* Copy them into `root`. Returns what was copied, so a caller can assert. */
function copyAssets(uiDir, page, root) {
  const names = assetsOf(uiDir, page);
  for (const name of names) {
    fs.copyFileSync(path.join(uiDir, name), path.join(root, name));
  }
  return names;
}

module.exports = { assetsOf, copyAssets };
