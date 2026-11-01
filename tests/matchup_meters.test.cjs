// The head-to-head meters under Matchup Intelligence on a team page. The panel
// replaced a percentile table, so the numbers themselves are not new -- what is
// new is the reading of them: an edge is one team's offensive percentile minus
// the other's defensive percentile, its size decides the verdict wording, and a
// trait neither team has rated has no edge at all. That last case is the one
// worth guarding, because a missing percentile that coerces to 0 reads as a
// major edge for whoever is on the other side.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const shell = fs.readFileSync('ui/dashboard_shell.html', 'utf8');

function lift(names) {
  const traits = shell.match(/^const SCOUTING_TRAITS = \[[^]*?^\];/m);
  assert.ok(traits, 'the shell must declare SCOUTING_TRAITS');
  const edges = shell.match(/^const MATCHUP_EDGE_SPAN[^\n]*\n^const MATCHUP_EDGE_EVEN[^\n]*/m);
  assert.ok(edges, 'the shell must declare the edge scale');
  const source = [traits[0], edges[0]].concat(names.map(name => {
    const m = shell.match(new RegExp('^function ' + name + '\\([^]*?\\n\\}', 'm'));
    assert.ok(m, 'the shell must declare ' + name);
    return m[0];
  })).join('\n');
  const context = {
    esc: v => String(v).replace(/[&<>"]/g, c =>
      ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c])),
  };
  vm.runInNewContext(source + '\nthis.__out = {' + names.join(',') + '};', context);
  return context.__out;
}

const { scoutingOrdinal, matchupEdgeLabel, matchupEdgeClass,
        matchupMeterPosition, matchupMeterRow, renderMatchupMeters } =
  lift(['scoutingOrdinal', 'matchupEdgeLabel', 'matchupEdgeClass',
        'matchupMeterPosition', 'matchupMeterRow', 'renderMatchupMeters']);

// One asset's worth of metrics: the same offensive and defensive percentile on
// every trait, so a whole panel can be read in one assertion.
const rated = (offense, defense) => Object.fromEntries(
  ['rush_success_rate', 'rush_ppa', 'rush_explosiveness',
   'pass_success_rate', 'pass_ppa', 'pass_explosiveness'].map(key =>
    [key, { status: 'rated', offense_percentile: offense, defense_percentile: defense }]));

test('a percentile reads as an ordinal, and an absent one as nothing', () => {
  assert.equal(scoutingOrdinal(81), '81st');
  assert.equal(scoutingOrdinal(2), '2nd');
  assert.equal(scoutingOrdinal(3), '3rd');
  assert.equal(scoutingOrdinal(76), '76th');
  // The teens take 'th' even though their last digit does not.
  assert.equal(scoutingOrdinal(11), '11th');
  assert.equal(scoutingOrdinal(12), '12th');
  assert.equal(scoutingOrdinal(13), '13th');
  assert.equal(scoutingOrdinal(111), '111th');
  assert.equal(scoutingOrdinal(21), '21st');
  assert.equal(scoutingOrdinal(50.6), '51st');
  assert.equal(scoutingOrdinal(NaN), '—');
  assert.equal(scoutingOrdinal(null), '—');
  assert.equal(scoutingOrdinal(undefined), '—');
});

test('the verdict names the side with the edge and how big it is', () => {
  assert.equal(matchupEdgeLabel(0, 'BYU', 'Utah'), 'Even matchup');
  assert.equal(matchupEdgeLabel(4, 'BYU', 'Utah'), 'Even matchup');
  assert.equal(matchupEdgeLabel(-4, 'BYU', 'Utah'), 'Even matchup');
  assert.equal(matchupEdgeLabel(5, 'BYU', 'Utah'), 'Slight BYU edge');
  assert.equal(matchupEdgeLabel(11, 'BYU', 'Utah'), 'Slight BYU edge');
  assert.equal(matchupEdgeLabel(12, 'BYU', 'Utah'), 'Clear BYU edge');
  assert.equal(matchupEdgeLabel(19, 'BYU', 'Utah'), 'Clear BYU edge');
  assert.equal(matchupEdgeLabel(20, 'BYU', 'Utah'), 'Major BYU edge');
  assert.equal(matchupEdgeLabel(-20, 'BYU', 'Utah'), 'Major Utah edge');
  assert.equal(matchupEdgeLabel(-12, 'BYU', 'Utah'), 'Clear Utah edge');
  assert.equal(matchupEdgeLabel(-5, 'BYU', 'Utah'), 'Slight Utah edge');
  assert.equal(matchupEdgeLabel(null, 'BYU', 'Utah'), 'Not rated');
});

test('the verdict colour follows the same three cases as its wording', () => {
  assert.equal(matchupEdgeClass(9), 'tm-edge-team');
  assert.equal(matchupEdgeClass(-9), 'tm-edge-opp');
  assert.equal(matchupEdgeClass(3), 'tm-edge-even');
  assert.equal(matchupEdgeClass(-3), 'tm-edge-even');
  assert.equal(matchupEdgeClass(null), 'tm-edge-even');
});

test('the marker leaves the centre with the edge and stops short of the ends', () => {
  assert.equal(matchupMeterPosition(0), 50);
  assert.equal(matchupMeterPosition(null), 50);
  assert.equal(matchupMeterPosition(40), 85);
  assert.equal(matchupMeterPosition(-40), 15);
  // Clamped: a 99-point gap is no further out than a 40-point one.
  assert.equal(matchupMeterPosition(99), 85);
  assert.equal(matchupMeterPosition(-99), 15);
  // And it is monotone in between, on both sides of the centre.
  assert.ok(matchupMeterPosition(5) > 50 && matchupMeterPosition(5) < matchupMeterPosition(20));
  assert.ok(matchupMeterPosition(-5) < 50 && matchupMeterPosition(-5) > matchupMeterPosition(-20));
});

test('a row shows both percentiles, the verdict, and a fill on the leading side', () => {
  const ours = matchupMeterRow('Rush success',
    { teamName: 'BYU', oppName: 'Utah', teamPct: 81, oppPct: 76 });
  assert.match(ours, /BYU offense/);
  assert.match(ours, /Utah defense/);
  assert.match(ours, /<strong>81st<\/strong>/);
  assert.match(ours, /<strong>76th<\/strong>/);
  assert.match(ours, /Slight BYU edge/);
  assert.match(ours, /class="tm-match-verdict tm-edge-team"/);
  assert.match(ours, /tm-team-fill/);
  assert.doesNotMatch(ours, /tm-opp-fill/);
  // The fill runs from the centre out to the marker, so its width is the gap.
  const marker = Number(ours.match(/tm-duel-marker" style="left:([\d.]+)%/)[1]);
  const width = Number(ours.match(/tm-team-fill" style="left:50%;width:([\d.]+)%/)[1]);
  assert.ok(Math.abs(width - (marker - 50)) < 0.01, width + ' vs ' + (marker - 50));

  const theirs = matchupMeterRow('Pass success',
    { teamName: 'BYU', oppName: 'Utah', teamPct: 79, oppPct: 92 });
  assert.match(theirs, /Clear Utah edge/);
  assert.match(theirs, /tm-opp-fill/);
  assert.doesNotMatch(theirs, /tm-team-fill/);
  // The opponent's fill starts at the marker and ends at the centre.
  const oppMarker = Number(theirs.match(/tm-duel-marker" style="left:([\d.]+)%/)[1]);
  const oppFill = theirs.match(/tm-opp-fill" style="left:([\d.]+)%;width:([\d.]+)%/);
  assert.equal(Number(oppFill[1]), oppMarker);
  assert.ok(Math.abs(Number(oppFill[2]) - (50 - oppMarker)) < 0.02);
});

test('an unrated trait shows no number, no edge and no fill', () => {
  const missing = matchupMeterRow('Pass PPA',
    { teamName: 'BYU', oppName: 'Utah', teamPct: 88, oppPct: null });
  // The absent percentile must not read as a zero -- which would be a major edge.
  assert.doesNotMatch(missing, /Slight|Clear|Major|Even matchup/);
  assert.match(missing, /Not rated/);
  assert.doesNotMatch(missing, /tm-duel-fill/);
  assert.match(missing, /<strong>88th<\/strong>/);
  assert.match(missing, /<strong>—<\/strong>/);
  assert.match(missing, /tm-duel-marker" style="left:50.00%/);
});

test('team and opponent names are escaped into the row', () => {
  const row = matchupMeterRow('Rush PPA',
    { teamName: 'Texas A&M', oppName: '<script>', teamPct: 60, oppPct: 40 });
  assert.match(row, /Texas A&amp;M offense/);
  assert.match(row, /&lt;script&gt; defense/);
  assert.doesNotMatch(row, /<script>/);
});

test('the panel renders one card per scouting trait, in the panel order', () => {
  const BYU = rated(81, 70), UTAH = rated(65, 76);
  const html = renderMatchupMeters('BYU', 'Utah', BYU, UTAH);
  assert.equal((html.match(/class="tm-match-row"/g) || []).length, 6);
  const traits = [...html.matchAll(/class="tm-match-trait">([^<]+)</g)].map(m => m[1]);
  assert.deepEqual(traits, ['Rush success', 'Rush PPA', 'Rush explosiveness',
                            'Pass success', 'Pass PPA', 'Pass explosiveness']);
  // Our offence against their defence, never our own defence.
  assert.equal((html.match(/BYU offense/g) || []).length, 6);
  assert.equal((html.match(/Utah defense/g) || []).length, 6);
  assert.doesNotMatch(html, /BYU defense|Utah offense/);
  // The reading is directional: the same pair of teams swapped flips every verdict.
  assert.equal((html.match(/Slight BYU edge/g) || []).length, 6);
  // Utah's offence (65th) against BYU's defence (70th) is the same five points
  // the other way, so BYU keeps the edge when the page is read from Utah's side.
  const back = renderMatchupMeters('Utah', 'BYU', UTAH, BYU);
  assert.equal((back.match(/Slight BYU edge/g) || []).length, 6);
  assert.doesNotMatch(back, /Utah edge/);
});

test('a trait the asset has not rated is skipped even when it carries numbers', () => {
  const metrics = rated(81, 70);
  metrics.pass_ppa = { status: 'unrated', offense_percentile: 99, defense_percentile: 1 };
  const html = renderMatchupMeters('BYU', 'Utah', metrics, rated(65, 76));
  assert.equal((html.match(/class="tm-match-row"/g) || []).length, 6);
  assert.equal((html.match(/Not rated/g) || []).length, 1);
  assert.doesNotMatch(html, /Major BYU edge/);
  // A trait missing from the opponent's asset entirely is the same case.
  const thin = renderMatchupMeters('BYU', 'Utah', rated(81, 70), {});
  assert.equal((thin.match(/Not rated/g) || []).length, 6);
});
