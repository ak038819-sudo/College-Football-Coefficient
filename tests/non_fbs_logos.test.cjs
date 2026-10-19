// The one lookup every page's logos come out of. Two sections feed it: the
// dated eras of the teams this project rates, and one undated mark each for the
// FCS and lower schools it does not. A school can be in both -- one of ours met
// in a season it was not FBS -- and the dated set has to win there, or a 1985
// page would show the logo the school uses now.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const shell = fs.readFileSync(path.join(__dirname, '../ui/dashboard_shell.html'), 'utf8');

// The real line out of the real page, not a copy of it here.
function lookup(manifest) {
  const source = shell.match(/^const DATED_LOGOS = .*$/m);
  assert.ok(source, 'DATED_LOGOS is no longer one statement in the shell');
  const ctx = { LOGO_MANIFEST: manifest };
  vm.createContext(ctx);
  vm.runInContext(source[0] + '\nthis.logos = DATED_LOGOS;', ctx);
  return ctx.logos;
}

const DATED = [{ start: 1980, end: 2005, src: 'teams/idaho/1980-2005.png', bg: '#111111' },
               { start: 2005, end: null, src: 'teams/idaho/2005-now.png', bg: '#222222' }];
const UNDATED = [{ start: 0, end: null, src: 'non_fbs/idaho/0-now.png', bg: '#333333' }];

test('a school in both sections keeps its dated eras', () => {
  const logos = lookup({ teams: { Idaho: DATED }, non_fbs: { Idaho: UNDATED } });
  assert.deepEqual(Array.from(logos.Idaho), DATED);
});

test('a non-FBS school is in the lookup, and a team is too', () => {
  const logos = lookup({ teams: { Idaho: DATED }, non_fbs: { Mercer: UNDATED } });
  // Array.from, because an array built inside the vm context is not
  // deepStrictEqual to an identical one out here.
  assert.deepEqual(Array.from(Object.keys(logos)).sort(), ['Idaho', 'Mercer']);
});

// The page shipped for a year with no non_fbs section at all, and the fetch
// workflow can be re-run at any time, so each section has to be optional.
test('either section may be missing', () => {
  assert.deepEqual(Array.from(Object.keys(lookup({ teams: { Idaho: DATED } }))), ['Idaho']);
  assert.deepEqual(Array.from(Object.keys(lookup({ non_fbs: { Mercer: UNDATED } }))), ['Mercer']);
  assert.deepEqual(Array.from(Object.keys(lookup({}))), []);
});

// An undated mark is stored as starting at year 0 so that every season finds
// it. pickDatedLogoEntry is what has to agree with that, and it is lifted here
// from the page rather than restated.
test('an undated mark is found for any season', () => {
  const ctx = {};
  const source = shell.match(/function pickDatedLogoEntry\([^)]*\) \{[\s\S]*?\n\}/);
  assert.ok(source, 'missing pickDatedLogoEntry');
  vm.runInNewContext(source[0] + '\nthis.pick = pickDatedLogoEntry;', ctx);
  const marks = { Mercer: UNDATED };
  for (const season of [1980, 2004, 2026]) {
    assert.equal(ctx.pick(marks, 'Mercer', season).src, 'non_fbs/idaho/0-now.png');
  }
  // A school with no mark at all is a miss, which is what draws the monogram.
  assert.equal(ctx.pick(marks, 'Lambuth', 2026), null);
});
