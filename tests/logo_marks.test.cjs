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

function markup(entry, size, onDark) {
  const ctx = { state: { year: 2026 }, DATED_LOGOS: entry ? { Alabama: [entry] } : {},
                DATED_CONFERENCE_LOGOS: {} };
  vm.createContext(ctx);
  vm.runInContext(sourceOf('hashHue') + '\n' + sourceOf('initials') + '\n'
    + sourceOf('pickDatedLogoEntry') + '\n' + sourceOf('logoMarkup') + '\n'
    + 'void 0', ctx);
  return vm.runInContext(
    `logoMarkup('Alabama', 'team', ${size === undefined ? 'undefined' : `'${size}'`}, undefined, ${!!onDark})`,
    ctx);
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

test('a mark on an always-dark row takes the light-ink artwork in either theme', () => {
  // A live scoreboard row and a board row with team artwork are dark whatever
  // the reader's theme is, so the media query would answer the wrong question
  // there: in light mode it hands over the colour mark, on a dark row.
  const html = markup({ ...PAIRED, dim: ['dark'] }, 'xs', true);
  assert.doesNotMatch(html, /<picture|<source/, `a dark row must not swap by theme: ${html}`);
  assert.match(html, /^<span class="logo-mark xs plate-on-dark">/);
  assert.match(html, /src="assets\/logos\/teams\/a\/0-now-dark\.png\?v=2222222222"/);
  // Not dim on the dark page: no plate, and the light measurement is irrelevant here.
  assert.match(markup({ ...PAIRED, dim: ['light'] }, 'xs', true), /^<span class="logo-mark xs">/);
  // No light-ink artwork: the colour mark, still with no theme swap.
  assert.match(markup(PLAIN, 'xs', true),
    /^<span class="logo-mark xs"><img class="logo-img" src="assets\/logos\/teams\/a\/0-now\.png/);
  assert.match(shell, /\.logo-mark\.plate-on-dark \{[^}]*--plate:\s*rgba\(238, 241, 236/,
    'the always-dark plate is not the dark page ink');
});

test('every board row with team artwork asks for the dark-page mark', () => {
  // The bug this guards is a board that calls teamLink directly and quietly
  // drops the flag, so the real boardTeamLink and teamLink are lifted whole.
  const ctx = { state: { year: 2026 }, DATED_LOGOS: { Alabama: [PAIRED], Auburn: [PAIRED] },
                DATED_CONFERENCE_LOGOS: {}, TEAM_PANELS: { Alabama: { image: 'a.png' } },
                teamAnchor: (name, inner) => inner, esc: s => s };
  vm.createContext(ctx);
  vm.runInContext(sourceOf('hashHue') + '\n' + sourceOf('initials') + '\n'
    + sourceOf('pickDatedLogoEntry') + '\n' + sourceOf('logoMarkup') + '\n'
    + sourceOf('teamLink') + '\n' + sourceOf('boardTeamLink'), ctx);
  assert.match(vm.runInContext("boardTeamLink('Alabama', 'xs')", ctx), /^<span class="logo-mark xs">/,
    'a row with artwork still swaps by theme');
  assert.match(vm.runInContext("boardTeamLink('Auburn', 'xs')", ctx), /^<picture class="logo-mark xs">/,
    'a row without artwork lost its theme swap');
  // Every mini-board goes through it. A row is scoped by its own teamPanelRow
  // call -- the same condition that makes the row dark -- so a new board that
  // reaches for teamLink or logoMarkup instead fails here.
  const rows = shell.split('teamPanelRow(').slice(1)
    .filter(part => !part.startsWith('name) {'));  // the definition itself
  assert.ok(rows.length >= 5, `only ${rows.length} board rows found; did they move?`);
  for (const row of rows) {
    const markup = row.slice(0, row.indexOf('</li>'));
    assert.doesNotMatch(markup, /(?<!board)teamLink\(|logoMarkup\(/,
      `a board row draws its mark without boardTeamLink: ${markup.slice(0, 160)}`);
  }
});

test('a mark is padded in px, never as a share of whatever contains it', () => {
  // A percentage resolves against the CONTAINING BLOCK's width, so the same
  // mark would be padded differently in every table, card and header -- and a
  // wide enough cell eats the whole fixed-size box.
  for (const rule of shell.match(/[^{}\n]*\.logo-mark[^{}\n]*\{[^}]*\}/g) || []) {
    assert.doesNotMatch(rule, /padding[^;]*%/, `percentage padding on a mark: ${rule}`);
  }
  const base = shell.match(/\n  \.logo-mark \{([^}]*)\}/)[1];
  assert.match(base, /padding:\s*\d+px/, `the mark has no px padding: ${base}`);
  // Each size is a different box, so each sets its own padding: the base 6px
  // is far too much on a 30px mark and too little on a 140px one.
  for (const size of ['lg', 'sm', 'xs']) {
    // The mark's OWN rule, not a page's override of it: `.gp-side
    // .logo-mark.lg` repads the hero on a phone and must not stand in for it.
    const rules = [...shell.matchAll(new RegExp(`([^{}\\n]*)\\.logo-mark\\.${size}([^{}\\n]*)\\{([^}]*)\\}`, 'g'))]
      .filter(m => !m[1].trim() && !m[2].trim())
      .map(m => m[3]);
    assert.ok(rules.length, `no rule sizes .logo-mark.${size}`);
    assert.ok(rules.some(r => /padding:\s*\d+px/.test(r)),
      `.logo-mark.${size} has no px padding of its own: ${rules.join(' | ')}`);
  }
});
