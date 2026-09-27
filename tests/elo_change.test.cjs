const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const shell = fs.readFileSync('ui/dashboard_shell.html', 'utf8');
function sourceOf(name) {
  const match = shell.match(new RegExp(`function ${name}\\([^)]*\\) \\{[\\s\\S]*?\\n\\}`));
  assert.ok(match, `missing ${name}`);
  return match[0];
}

test('published game Elo movement is green/up or red/down; missing and zero stay blank', () => {
  const ctx = {};
  vm.runInNewContext(sourceOf('eloChangeMarkup') + '\nthis.delta = eloChangeMarkup;', ctx);
  assert.match(ctx.delta(14.5), /class="elo-change up"[^>]*>\(▲14\.5\)/);
  assert.match(ctx.delta(-8.2), /class="elo-change down"[^>]*>\(▼8\.2\)/);
  assert.equal(ctx.delta(null), '');
  assert.equal(ctx.delta(0), '');
});

test('Elo rankings pass each audited change to the table', () => {
  const ctx = {
    DATA: {team_elo_by_year: {2026: [{team: 'Florida', elo: 1551.4, change: -8.2}]}},
    state: {year: 2026}, rankTable: rows => JSON.stringify(rows),
  };
  vm.runInNewContext(sourceOf('renderElo') + '\nthis.render = renderElo;', ctx);
  assert.match(ctx.render(), /"change":-8\.2/);
});
