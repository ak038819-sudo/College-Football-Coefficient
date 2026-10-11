// A coach's recruiting summary. The whole subtlety is WHOSE class a class is:
// CFBD attributes one to whoever held the job that February, so a coach's
// first season at a school carries his predecessor's recruiting. Saban's
// first LSU class ranked 21st and his next four ranked 2nd, 21st, 3rd and
// 4th. These lift the real function out of the shell rather than restating
// the rule next to it.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const shell = fs.readFileSync('ui/dashboard_shell.html', 'utf8');

function load() {
  const m = shell.match(/^function coachRecruiting\([^]*?\n\}/m);
  assert.ok(m, 'the shell must declare coachRecruiting');
  const context = {};
  vm.runInNewContext(m[0] + '\nthis.__x = coachRecruiting;', context);
  return context.__x;
}

const coachRecruiting = load();
const season = (season_year, class_rank, class_inherited) =>
  class_inherited ? { season_year, class_rank, class_inherited } : { season_year, class_rank };

test('a coach with no ranked class gets no summary at all', () => {
  assert.equal(coachRecruiting([]), null);
  assert.equal(coachRecruiting([{ season_year: 1975 }]), null);
  assert.equal(coachRecruiting(null), null);
});

test('an inherited class is left out of the average', () => {
  // Saban at LSU: 21st inherited, then 2nd, 21st, 3rd, 4th.
  const out = coachRecruiting([
    season(2000, 21, true), season(2001, 2), season(2002, 21),
    season(2003, 3), season(2004, 4),
  ]);
  assert.equal(out.classes, 4);
  assert.equal(out.inherited, 1);
  assert.equal(out.average, 7.5);
});

test('a coach whose only class was inherited has nothing to average', () => {
  // One season at a school, fired after it: nothing here is his.
  assert.equal(coachRecruiting([season(2015, 30, true)]), null);
});

test('the best class is the lowest rank, with the season it came in', () => {
  const out = coachRecruiting([season(2011, 1), season(2012, 5), season(2013, 3)]);
  assert.equal(out.best, 1);
  assert.equal(out.bestSeason, 2011);
});

test('the best class is never an inherited one', () => {
  const out = coachRecruiting([season(2007, 1, true), season(2008, 9)]);
  assert.equal(out.best, 9);
  assert.equal(out.bestSeason, 2008);
});

test('a season with no class is skipped rather than counted as rank zero', () => {
  // Classes start in 2000 and most of this archive predates them. Counting a
  // missing class as 0 would make every old coach the best recruiter alive.
  const out = coachRecruiting([
    { season_year: 1995 }, { season_year: 1996, class_rank: null }, season(2001, 10),
  ]);
  assert.equal(out.classes, 1);
  assert.equal(out.average, 10);
});

test('inherited seasons are counted even when nothing else is', () => {
  const out = coachRecruiting([season(2007, 12, true), season(2008, 3, true), season(2009, 5)]);
  assert.equal(out.inherited, 2);
  assert.equal(out.classes, 1);
});
