// Text has to be readable in BOTH themes, and the light one is where it slips.
//
// Light mode was shipped with `--amber: #b5741a`, which measures 3.36:1 on the
// page background where WCAG AA wants 4.5 for normal text -- and amber is the
// colour of every link, the current tab and a winning score, in 76 places
// across the three pages. Dark mode measured 8.58 the whole time, which is
// exactly why nobody noticed: the theme the author reads is the theme that
// gets checked.
//
// This drives the real stylesheets. A colour is read out of the page's own
// `:root[data-theme="light"]` block, not from a copy kept here, so a palette
// edit that drops below the line fails this test rather than shipping.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const UI = path.join(__dirname, '..', 'ui');
const PAGES = ['dashboard_shell.html', 'nfl.html', 'basketball.html'];

// WCAG 2.1 contrast, verbatim: sRGB -> linear -> relative luminance.
function channel(value) {
  const c = value / 255;
  return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
}

function luminance(hex) {
  const h = hex.replace('#', '');
  const [r, g, b] = [0, 2, 4].map(i => parseInt(h.slice(i, i + 2), 16));
  return 0.2126 * channel(r) + 0.7152 * channel(g) + 0.0722 * channel(b);
}

function contrast(a, b) {
  const [hi, lo] = [luminance(a), luminance(b)].sort((x, y) => y - x);
  return (hi + 0.05) / (lo + 0.05);
}

// The palette a page declares for one theme. The light values are read from
// the explicit `:root[data-theme="light"]` block rather than the media query,
// because the two are written to agree and a test that reads one proves the
// other only while they do -- which is itself worth asserting, below.
function palette(page, selector) {
  const text = fs.readFileSync(path.join(UI, page), 'utf8');
  const at = text.indexOf(selector);
  assert.ok(at >= 0, `${page} has no ${selector} block`);
  const open = text.indexOf('{', at);
  const close = text.indexOf('}', open);
  const body = text.slice(open + 1, close);
  const out = {};
  for (const [, name, value] of body.matchAll(/--([a-z0-9-]+):\s*(#[0-9a-f]{6})/gi)) {
    out[name] = value.toLowerCase();
  }
  return out;
}

const AA_NORMAL = 4.5;

// Every variable the stylesheets set as a `color:`, against every variable
// they set as a `background:` behind text. Both lists were read off the
// stylesheet, not guessed: `--amber-dim` is NOT here because it only ever
// appears as a border-color or a fill, and `--line` only as a border.
// `--red` is NOT here: it is a fill, and the ink beside it is `--red-ink`.
// They were one token until this change, which is the trap -- lightening red
// enough to read as text on the dark page drops the white-on-badge pairing
// from 4.93 to 3.42, so a single token cannot satisfy both and a test that
// only looked at text would have waved that through.
const INKS = ['ink', 'ink-dim', 'amber', 'red-ink', 'green-hl'];
const SURFACES = ['bg', 'panel', 'panel-2'];

// Every rule that puts text ON a palette fill, read out of the stylesheet
// rather than listed by hand.
//
// A hand-written list is why this file missed a regression it was written to
// prevent: darkening `--amber` for readable links also darkened the fill
// behind the selected league button, whose `color: #15110a` was chosen
// against the OLD amber and fell to 3.32:1. The list only named `--red`, so
// nothing failed. Reading the rules means a new amber-filled control is
// covered the day it is written.
function fillRules(page) {
  const text = fs.readFileSync(path.join(UI, page), 'utf8');
  const style = text.slice(text.indexOf('<style>'), text.indexOf('</style>'));
  const out = [];
  // Declaration blocks, crudely but adequately: everything between braces.
  for (const [, body] of style.matchAll(/\{([^{}]*)\}/g)) {
    const fill = body.match(/background:\s*var\(--([a-z0-9-]+)\)/i);
    const ink = body.match(/(?:^|[;\s])color:\s*(var\(--[a-z0-9-]+\)|#[0-9a-f]{3,6})/i);
    if (fill && ink) out.push({ fill: fill[1], ink: ink[1] });
  }
  return out;
}

// '#abc' -> '#aabbcc'; 'var(--x)' -> the palette's value; else null.
function resolve(value, colours) {
  const named = value.match(/^var\(--([a-z0-9-]+)\)$/i);
  if (named) return colours[named[1]] || null;
  const hex = value.toLowerCase();
  if (/^#[0-9a-f]{6}$/.test(hex)) return hex;
  if (/^#[0-9a-f]{3}$/.test(hex)) {
    return '#' + [1, 2, 3].map(i => hex[i] + hex[i]).join('');
  }
  return null;
}

for (const page of PAGES) {
  test(`${page} light-mode text clears WCAG AA on every surface`, () => {
    const light = palette(page, ':root[data-theme="light"]');
    for (const ink of INKS) {
      if (!light[ink]) continue;          // a page need not define every one
      for (const surface of SURFACES) {
        if (!light[surface]) continue;
        const ratio = contrast(light[ink], light[surface]);
        assert.ok(ratio >= AA_NORMAL,
          `--${ink} (${light[ink]}) on --${surface} (${light[surface]}) is ` +
          `${ratio.toFixed(2)}:1, and AA wants ${AA_NORMAL}`);
      }
    }
  });

  test(`${page} dark-mode text clears WCAG AA on every surface`, () => {
    const dark = palette(page, ':root {');
    for (const ink of INKS) {
      if (!dark[ink]) continue;
      for (const surface of SURFACES) {
        if (!dark[surface]) continue;
        const ratio = contrast(dark[ink], dark[surface]);
        assert.ok(ratio >= AA_NORMAL,
          `--${ink} (${dark[ink]}) on --${surface} (${dark[surface]}) is ` +
          `${ratio.toFixed(2)}:1, and AA wants ${AA_NORMAL}`);
      }
    }
  });

  for (const theme of [[':root[data-theme="light"]', 'light'],
                       [':root {', 'dark']]) {
    test(`${page} ${theme[1]}-mode text ON a palette fill clears AA`, () => {
      const colours = palette(page, theme[0]);
      const rules = fillRules(page);
      assert.ok(rules.length, `${page} has no text-on-fill rules to check`);
      for (const rule of rules) {
        const back = colours[rule.fill];
        const front = resolve(rule.ink, colours);
        // A fill or ink this theme does not define, or a keyword like
        // `inherit`, is not a measurable pair rather than a passing one.
        if (!back || !front) continue;
        const ratio = contrast(front, back);
        assert.ok(ratio >= AA_NORMAL,
          `${rule.ink} on --${rule.fill} (${back}) is ${ratio.toFixed(2)}:1, ` +
          `and AA wants ${AA_NORMAL}`);
      }
    });
  }

  test(`${page} states the same light palette twice over`, () => {
    // The media query and the explicit attribute selector are two copies of
    // one palette: a reader on a light system and a reader who pressed the
    // toggle must see the same page. Editing one and not the other is the
    // obvious way to half-fix a colour, and it would leave the test above
    // passing on the half nobody looks at.
    const viaQuery = palette(page, ':root:not([data-theme="dark"])');
    const viaAttribute = palette(page, ':root[data-theme="light"]');
    assert.deepEqual(viaAttribute, viaQuery);
  });
}

test('the measurement itself is right', () => {
  // Black on white is 21:1 and anything on itself is 1:1. Without these the
  // assertions above could pass on a formula that returns 99 for everything.
  assert.equal(contrast('#000000', '#ffffff').toFixed(2), '21.00');
  assert.equal(contrast('#ffffff', '#ffffff').toFixed(2), '1.00');
  // The value this change replaced, so the number in the comments above is
  // checked rather than remembered.
  assert.equal(contrast('#b5741a', '#f2f0e8').toFixed(2), '3.36');
  // And the pairing that forced --red to split in two.
  assert.equal(contrast('#ffffff', '#c4453c').toFixed(2), '4.93');
  assert.equal(contrast('#ffffff', '#d16e67').toFixed(2), '3.42');
});
