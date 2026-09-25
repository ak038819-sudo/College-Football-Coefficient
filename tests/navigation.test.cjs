// Dependency-free route regressions: node --test tests/navigation.test.cjs
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { readRoute, hashFor, normalizeSearch } = require('../ui/navigation.js');
const config = {
  yearsAll: [1980, 2024, 2025, 2026], playoffYears: [2024, 2025, 2026],
  gameSeasons: [{ season: 1980, scheduled: 0 }, { season: 2025, scheduled: 0 }, { season: 2026, scheduled: 604 }]
};
const read = hash => readRoute(hash, config);

test('all eight legacy tabs retain their original destination', () => {
  for (const [tab, section, view] of [
    ['home', 'home', ''], ['teams', 'rankings', 'team-coe'], ['elo', 'rankings', 'elo'],
    ['conferences', 'rankings', 'conference-coe'], ['field', 'playoff', 'field'],
    ['bracket', 'playoff', 'bracket'], ['odds', 'playoff', 'odds'], ['history', 'playoff', 'history']
  ]) {
    const route = read('#tab=' + tab);
    assert.equal(route.section, section);
    assert.equal(route.subview, view);
  }
  assert.equal(read('#section=teams').section, 'teams');
  assert.equal(read('#section=teams&tab=elo').section, 'teams');
});

test('season, subview, status and query survive URL round trips', () => {
  for (const hash of ['#section=rankings&view=conference-coe&season=1980',
    '#section=playoff&view=bracket&season=2025', '#section=games&season=1980&status=completed',
    '#section=teams&q=Texas%20A%26M']) {
    assert.deepEqual(read(hashFor(read(hash))), read(hash));
  }
});

test('missing and invalid URLs have deterministic defaults', () => {
  assert.equal(read('').section, 'home');
  assert.equal(read('#section=nope').section, 'home');
  assert.equal(read('#section=__proto__').section, 'home');
  assert.equal(read('#tab=constructor').section, 'home');
  assert.equal(read('#section=rankings&view=odds').subview, 'elo');
  assert.equal(read('#section=playoff&season=1980').year, 2026);
  assert.equal(read('#section=games&season=NaN').year, 2026);
  assert.equal(read('#section=games&season=2025').status, 'completed');
  assert.equal(read('#section=games').status, 'upcoming');
  assert.equal(read('#section=games&season=2025&status=upcoming').status, 'upcoming');
});

test('team links carry a safe return destination including the selected season', () => {
  const from = '#section=rankings&view=team-coe&season=2025';
  const team = read('#team=byu&from=' + encodeURIComponent(from));
  assert.equal(team.view, 'team');
  assert.equal(team.section, 'teams');
  assert.equal(team.returnTo, from);
  assert.deepEqual(read(hashFor(team)), team);
  for (const from of ['https://example.com', 'javascript:alert(1)', '#team=byu', '#section=teams&team=byu']) {
    assert.equal(read('#team=byu&from=' + encodeURIComponent(from)).returnTo, '#section=teams');
  }
});

test('normalization handles punctuation, accents, and common name variants', () => {
  assert.equal(normalizeSearch('Texas A&M'), 'texasam');
  assert.equal(normalizeSearch("Hawaiʻi"), 'hawaii');
  assert.equal(normalizeSearch('San José State'), 'sanjosestate');
  assert.equal(normalizeSearch('Miami (FL)'), normalizeSearch('miami-fl'));
});

test('no-season exports stay usable without inventing a year', () => {
  const empty = { yearsAll: [], playoffYears: [], gameSeasons: [] };
  const route = readRoute('#section=games', empty);
  assert.equal(route.year, null);
  assert.equal(hashFor(route), '#section=games&status=completed');
});

test('a games URL can point at one game; anything but digits is dropped', () => {
  const r = read('#section=games&season=2019&status=completed&game=401112233');
  assert.equal(r.game, 401112233);
  assert.deepEqual(read(hashFor(r)), r);
  for (const bad of ['abc', '12e4', '-5', '1234567890123', '<script>', '']) {
    assert.equal(read('#section=games&season=2019&game=' + encodeURIComponent(bad)).game, null);
  }
  assert.equal(read('#section=rankings&game=401112233').game, null);     // only Games keeps it
  assert.equal(read('#team=byu&game=401112233').game, null);
  assert.equal(hashFor(read('#section=games&season=2019&game=5')).includes('game=5'), true);
});
