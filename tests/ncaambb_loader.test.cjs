// The reader's half of the basketball export contract. The build gate never
// runs on the reader's machine, so these tests drive the refusal that does:
// an unsupported schema, a football manifest, a sample passed off as national
// coverage, a model with no held-out score, and the staleness that must be
// SAID rather than treated as a reason to show nothing.
const test = require('node:test');
const assert = require('node:assert/strict');
const loader = require('../ui/ncaambb_loader.js');

const NOW = Date.parse('2026-10-20T12:00:00Z');

function manifest(overrides) {
  const base = {
    schema_version: 1,
    sport: 'ncaambb',
    generated_at: '2026-10-20T11:30:00Z',
    latest_season: 2026,
    model: {
      name: 'ncaambb_elo_v1',
      fitted_on: [2015, 2016],
      held_out: [2026],
      holdout_brier: 0.18656,
      holdout_games: 5752
    },
    coverage: [
      { season: 2026, division_one_teams: 365, rated_teams: 364, games: 6318 }
    ],
    seasons: { 2026: 'seasons/2026.js?v=abc1234567' },
    files: { teams: 'teams.js?v=def7654321' }
  };
  return Object.assign(base, overrides || {});
}

test('a national export of a validated model is accepted', () => {
  const verdict = loader.accept(manifest(), NOW);
  assert.equal(verdict.ok, true);
  assert.deepEqual(verdict.reasons, []);
  assert.equal(verdict.latestSeason, 2026);
  assert.equal(verdict.stale, false);
});

test('a schema this build does not know is refused, and only that is said', () => {
  // Reading on would mean reading fields whose meaning is not agreed.
  const verdict = loader.accept(manifest({ schema_version: 2, coverage: [] }),
    NOW);
  assert.equal(verdict.ok, false);
  assert.equal(verdict.reasons.length, 1);
  assert.match(verdict.reasons[0], /does not understand export schema 2/);
});

test('a football manifest is refused outright', () => {
  const verdict = loader.accept(manifest({ sport: 'cfb' }), NOW);
  assert.equal(verdict.ok, false);
  assert.match(verdict.reasons[0], /not college basketball/);
});

test('a one-team sample is refused rather than drawn as rankings', () => {
  const verdict = loader.accept(manifest({
    coverage: [{ season: 2026, division_one_teams: 1, rated_teams: 1,
      games: 31 }]
  }), NOW);
  assert.equal(verdict.ok, false);
  assert.match(verdict.reasons[0], /sample rather than the country/);
});

test('a season whose teams are mostly unrated is refused', () => {
  const verdict = loader.accept(manifest({
    coverage: [{ season: 2026, division_one_teams: 365, rated_teams: 200,
      games: 6318 }]
  }), NOW);
  assert.equal(verdict.ok, false);
  assert.match(verdict.reasons[0], /rates only 200 of 365/);
});

test('a model with no held-out score is refused', () => {
  const verdict = loader.accept(manifest({
    model: { name: 'x', fitted_on: [2025], held_out: [], holdout_brier: null }
  }), NOW);
  assert.equal(verdict.ok, false);
  assert.equal(verdict.reasons.length, 2);
});

test('a holdout no better than a coin flip is refused', () => {
  const verdict = loader.accept(manifest({
    model: { name: 'x', fitted_on: [2025], held_out: [2026],
      holdout_brier: 0.25 }
  }), NOW);
  assert.equal(verdict.ok, false);
  assert.match(verdict.reasons[0], /coin flip/);
});

test('no manifest at all is refused without throwing', () => {
  assert.equal(loader.accept(null, NOW).ok, false);
  assert.equal(loader.accept(undefined, NOW).reasons.length, 1);
});

// --- freshness is said, not enforced ---------------------------------------

test('an old export is still shown, and marked stale with its age', () => {
  const verdict = loader.accept(
    manifest({ generated_at: '2026-10-18T12:00:00Z' }), NOW);
  assert.equal(verdict.ok, true, 'the last good snapshot stays publishable');
  assert.equal(verdict.stale, true);
  assert.equal(verdict.age.minutes, 2880);
  assert.equal(verdict.age.label, 'updated 2d ago');
});

test('the stale line is drawn at three hours, not at any read', () => {
  const fresh = loader.accept(
    manifest({ generated_at: '2026-10-20T09:30:00Z' }), NOW);
  const stale = loader.accept(
    manifest({ generated_at: '2026-10-20T08:30:00Z' }), NOW);
  assert.equal(fresh.stale, false, '150 minutes is not stale');
  assert.equal(stale.stale, true, '210 minutes is');
});

test('an unreadable as-of time is stale and says so, not zero minutes ago', () => {
  // The alternative is a page that claims to be current because a date
  // failed to parse.
  const verdict = loader.freshness(manifest({ generated_at: 'soon' }), NOW);
  assert.equal(verdict.known, false);
  assert.equal(verdict.stale, true);
  assert.equal(verdict.minutes, null);
});

test('ages read the way a reader expects at each scale', () => {
  assert.equal(loader.describeAge(0), 'updated just now');
  assert.equal(loader.describeAge(7), 'updated 7m ago');
  assert.equal(loader.describeAge(59), 'updated 59m ago');
  assert.equal(loader.describeAge(60), 'updated 1h ago');
  assert.equal(loader.describeAge(47 * 60), 'updated 47h ago');
  assert.equal(loader.describeAge(48 * 60), 'updated 2d ago');
});

// --- the files a page may fetch --------------------------------------------

test('a season URL comes from the manifest, version stamp included', () => {
  assert.equal(loader.seasonSource(manifest(), 2026),
    'data/ncaambb/seasons/2026.js?v=abc1234567');
  assert.equal(loader.fileSource(manifest(), 'teams'),
    'data/ncaambb/teams.js?v=def7654321');
});

test('a season the manifest does not name has no URL to guess at', () => {
  // Building the path from the season number would fetch a cached file under
  // a newer manifest, which is how last week's ratings get served as this
  // week's.
  assert.equal(loader.seasonSource(manifest(), 2019), null);
  assert.equal(loader.fileSource(manifest(), 'players'), null);
  assert.equal(loader.seasonSource(null, 2026), null);
});

test('nothing here reads or writes a football global', () => {
  const source = require('node:fs')
    .readFileSync(require.resolve('../ui/ncaambb_loader.js'), 'utf8');
  for (const banned of ['CFB_', 'DASHBOARD_DATA', 'window.STATIC_MANIFEST']) {
    assert.ok(!source.includes(banned), `${banned} must not appear`);
  }
});
