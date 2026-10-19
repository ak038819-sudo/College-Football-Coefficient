// How a logo is drawn on the page. The coloured disc behind every mark is gone:
// a mark now stands on the page background, with ESPN's light-ink artwork on the
// dark theme where the set carries it, and a quiet plate only behind a mark
// measured as dim against the page it lands on (src/export_logo_files.py's
// is_dim_on, exported as entry.dim).
//
// These drive the REAL logoMarkup out of the shell -- stubbing it is what let a
// chipped mark and a bare one pass the same assertions for a day.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const shell = fs.readFileSync(path.join(__dirname, '../ui/dashboard_shell.html'), 'utf8');
function sourceOf(name) {
  const match = shell.match(new RegExp(`function ${name}\\([^)]*\\) \\{[\\s\\S]*?\\n\\}`));
  assert.ok(match, `missing ${name}`);
  return match[0];
}

// Entries in the shape src/export_logo_files.py writes them: a colour mark, the
// light-ink one beside it where ESPN publishes one, and dim naming the themes the
// colour/dark pair would disappear on.
const PLAIN = { start: 0, end: null, src: 'assets/logos/teams/a/0-now.png?v=1111111111', bg: '#102030' };
const PAIRED = { ...PLAIN, darkSrc: 'assets/logos/teams/a/0-now-dark.png?v=2222222222' };

function markup(entry, size) {
  const ctx = { state: { year: 2026 }, DATED_LOGOS: entry ? { Alabama: [entry] } : {},
                DATED_CONFERENCE_LOGOS: {} };
  vm.createContext(ctx);
  vm.runInContext(sourceOf('hashHue') + '\n' + sourceOf('initials') + '\n'
    + sourceOf('pickDatedLogoEntry') + '\n' + sourceOf('logoMarkup') + '\n'
    + `logoMarkup('Alabama', 'team', ${size === undefined ? 'undefined' : `'${size}'`})`, ctx);
  return vm.runInContext(
    `logoMarkup('Alabama', 'team', ${size === undefined ? 'undefined' : `'${size}'`})`, ctx);
}

test('a mark carries no coloured disc behind it', () => {
  const html = markup(PLAIN, 'sm');
  assert.doesNotMatch(html, /style=/, `the mark still has an inline style: ${html}`);
  assert.doesNotMatch(html, /logo-chip/, 'the chip class is back');
  assert.match(html, /^<span class="logo-mark sm">/);
  assert.match(html, /<img class="logo-img" src="assets\/logos\/teams\/a\/0-now\.png\?v=1111111111"/);
});

test('the dark theme gets the light-ink artwork and the light theme the colour mark', () => {
  const html = markup(PAIRED, 'sm');
  assert.match(html, /^<picture class="logo-mark sm">/,
    `a paired entry has to swap by media query: ${html}`);
  assert.match(html,
    /<source srcset="assets\/logos\/teams\/a\/0-now\.png\?v=1111111111" media="\(prefers-color-scheme: light\)">/,
    'the colour mark is not offered to the light theme');
  assert.match(html, /<img class="logo-img" src="assets\/logos\/teams\/a\/0-now-dark\.png\?v=2222222222"/,
    'the default image is not the light-ink artwork');
});

test('only a mark measured dim takes a plate, and only on the theme it is dim on', () => {
  assert.doesNotMatch(markup(PLAIN, 'sm'), /dim-/, 'an undimmed mark was plated');
  assert.match(markup({ ...PAIRED, dim: ['dark'] }, 'sm'), /class="logo-mark sm dim-dark"/);
  assert.match(markup({ ...PLAIN, dim: ['light'] }, 'sm'), /class="logo-mark sm dim-light"/);
  const both = markup({ ...PLAIN, dim: ['dark', 'light'] }, 'sm');
  assert.match(both, /class="logo-mark sm dim-dark dim-light"/);
});

test('a school with no mark still falls back to its monogram', () => {
  const html = markup(null, 'sm');
  assert.match(html, /^<span class="monogram sm" style="background:hsl\(/);
  assert.match(html, />AL</, `the monogram lost its initials: ${html}`);
});

test('the stylesheet plates a dim mark with the page ink and nothing else', () => {
  // The plate is a variable on the mark, so a size class cannot carry a colour
  // and the base rule cannot paint every mark again.
  const base = shell.match(/\n  \.logo-mark \{([^}]*)\}/);
  assert.ok(base, 'no .logo-mark rule in the shell');
  assert.match(base[1], /--plate:\s*transparent/,
    `a mark is plated by default: ${base[1]}`);
  assert.match(base[1], /background:\s*var\(--plate\)/);
  for (const size of ['lg', 'sm', 'xs']) {
    // .xs shares its rule with the monogram, so match the whole selector list.
    const rule = shell.match(new RegExp(`\\n  [^{}\\n]*\\.logo-mark\\.${size}[^{}\\n]*\\{([^}]*)\\}`));
    assert.ok(rule, `no .logo-mark.${size} rule`);
    assert.doesNotMatch(rule[1], /background|--plate/,
      `.logo-mark.${size} paints the mark: ${rule[1]}`);
  }
  assert.match(shell, /\.logo-mark\.dim-dark \{[^}]*--plate:\s*rgba\(238, 241, 236/,
    'a mark dim on the dark theme gets no plate of the page ink');
  assert.match(shell, /\.logo-mark\.dim-light \{[^}]*--plate:\s*rgba\(26, 31, 27/,
    'a mark dim on the light theme gets no plate of the page ink');
});
