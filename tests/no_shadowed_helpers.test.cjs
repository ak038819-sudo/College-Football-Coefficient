// No two top-level functions in the shell may share a name.
//
// JavaScript does not complain about this: the later declaration wins, for
// every call site of both, silently. It has now happened twice in this file.
// `signed()` was added for the coach page over the one the team pages already
// used, and `statCell()` was added for the team statistics tables over the one
// the player pages used -- which turned every number on every player's stat
// table into an em dash, on main, until a review caught it. Neither broke a
// test, because each test lifted the function it meant out of the file by
// name and got the one it wanted.
//
// So the guard is on the file, not on either name.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');

const SHELL = 'ui/dashboard_shell.html';

function topLevelFunctionNames(source) {
  // A top-level declaration starts in column one; anything indented is inside
  // something and has its own scope.
  return (source.match(/^function\s+([A-Za-z_$][\w$]*)\s*\(/gm) || [])
    .map(line => line.replace(/^function\s+/, '').replace(/\s*\($/, ''));
}

test('every top-level function in the shell has its own name', () => {
  const names = topLevelFunctionNames(fs.readFileSync(SHELL, 'utf8'));
  assert.ok(names.length > 100, 'the pattern must actually be finding functions: ' + names.length);
  const seen = new Map();
  const duplicated = [];
  for (const name of names) {
    seen.set(name, (seen.get(name) || 0) + 1);
    if (seen.get(name) === 2) duplicated.push(name);
  }
  assert.deepEqual(duplicated, [],
    'these names are declared more than once at the top level of ' + SHELL +
    ', so the last declaration has silently replaced the others everywhere: ' +
    duplicated.join(', '));
});

test('the check would catch a duplicate', () => {
  // Without this, a pattern that matched nothing would pass the test above
  // for the wrong reason.
  const names = topLevelFunctionNames(
    'function a(x) {\n}\nfunction b(y) {\n}\nfunction a(z) {\n}\n');
  assert.deepEqual(names, ['a', 'b', 'a']);
});
