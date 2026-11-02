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
  const card = shell.match(/^const MATCHUP_CARD_BG[^\n]*/m);
  assert.ok(card, 'the shell must declare the card background it measures against');
  const units = shell.match(/^const MATCHUP_UNITS = \{[^\n]*\n^function matchupUnitSides[^\n]*/m);
  assert.ok(units, 'the shell must declare the unit pairs');
  const source = [traits[0], edges[0], card[0], units[0]].concat(names.map(name => {
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

const { scoutingOrdinal, matchupEdgeLabel, matchupEdgeClass, matchupMeterPosition,
        matchupInterpretation, matchupMeterRow, renderMatchupMeters, matchupUnitToggle,
        matchupLighten, matchupReadableColor, matchupRelativeLuminance } =
  lift(['scoutingOrdinal', 'matchupEdgeLabel', 'matchupEdgeClass', 'matchupMeterPosition',
        'matchupInterpretation', 'matchupMeterRow', 'renderMatchupMeters', 'matchupUnitToggle',
        'matchupLighten', 'matchupReadableColor', 'matchupRelativeLuminance']);

// WCAG 2.1 contrast, the same formula the stylesheet test uses.
function contrast(a, b) {
  const [hi, lo] = [matchupRelativeLuminance(a), matchupRelativeLuminance(b)]
    .sort((x, y) => y - x);
  return (hi + 0.05) / (lo + 0.05);
}

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

test('the marker travels toward the side holding the edge, and stops short of the ends', () => {
  assert.equal(matchupMeterPosition(0), 50);
  assert.equal(matchupMeterPosition(null), 50);
  // Our team's percentile is printed on the LEFT, so our edge moves left.
  assert.equal(matchupMeterPosition(40), 15);
  assert.equal(matchupMeterPosition(-40), 85);
  // Clamped: a 99-point gap is no further out than a 40-point one.
  assert.equal(matchupMeterPosition(99), 15);
  assert.equal(matchupMeterPosition(-99), 85);
  // And it is monotone in between, on both sides of the centre.
  assert.ok(matchupMeterPosition(5) < 50 && matchupMeterPosition(5) > matchupMeterPosition(20));
  assert.ok(matchupMeterPosition(-5) > 50 && matchupMeterPosition(-5) < matchupMeterPosition(-20));
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
  // The fill runs from the marker back to the centre, on our side of the track.
  const marker = Number(ours.match(/tm-duel-marker" style="left:([\d.]+)%/)[1]);
  assert.ok(marker < 50, 'our edge belongs on our own side: ' + marker);
  const fill = ours.match(/tm-team-fill" style="left:([\d.]+)%;width:([\d.]+)%/);
  assert.equal(Number(fill[1]), marker);
  assert.ok(Math.abs(Number(fill[2]) - (50 - marker)) < 0.02);

  const theirs = matchupMeterRow('Pass success',
    { teamName: 'BYU', oppName: 'Utah', teamPct: 79, oppPct: 92 });
  assert.match(theirs, /Clear Utah edge/);
  assert.match(theirs, /tm-opp-fill/);
  assert.doesNotMatch(theirs, /tm-team-fill/);
  // Theirs starts at the centre and runs out to the marker, on their side.
  const oppMarker = Number(theirs.match(/tm-duel-marker" style="left:([\d.]+)%/)[1]);
  assert.ok(oppMarker > 50, 'their edge belongs on their own side: ' + oppMarker);
  const oppFill = theirs.match(/tm-opp-fill" style="left:50%;width:([\d.]+)%/);
  assert.ok(Math.abs(Number(oppFill[1]) - (oppMarker - 50)) < 0.02);
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

test('the toggle turns the panel around: our defence against their offence', () => {
  // Rated offence and defence on both sides, so the two halves cannot be
  // confused with each other: BYU is the better offence, Utah the better
  // defence, and the second half of the matchup belongs to Utah.
  const BYU = rated(81, 40), UTAH = rated(55, 76);
  const first = renderMatchupMeters('BYU', 'Utah', BYU, UTAH, 'offense');
  assert.equal((first.match(/BYU offense/g) || []).length, 6);
  assert.equal((first.match(/Utah defense/g) || []).length, 6);
  assert.equal((first.match(/<strong>81st<\/strong>/g) || []).length, 6);
  assert.equal((first.match(/Slight BYU edge/g) || []).length, 6);

  const second = renderMatchupMeters('BYU', 'Utah', BYU, UTAH, 'defense');
  assert.equal((second.match(/BYU defense/g) || []).length, 6);
  assert.equal((second.match(/Utah offense/g) || []).length, 6);
  // Our defensive percentile and their offensive one, not the other pair.
  assert.equal((second.match(/<strong>40th<\/strong>/g) || []).length, 6);
  assert.equal((second.match(/<strong>55th<\/strong>/g) || []).length, 6);
  assert.doesNotMatch(second, /81st|76th/);
  assert.equal((second.match(/Clear Utah edge/g) || []).length, 6);
  // An unknown or absent unit is the offensive half, which is what the panel opens on.
  assert.equal(renderMatchupMeters('BYU', 'Utah', BYU, UTAH, 'nonsense'), first);
  assert.equal(renderMatchupMeters('BYU', 'Utah', BYU, UTAH), first);
});

test('the toggle says which half is showing, in text as well as in colour', () => {
  const html = matchupUnitToggle('BYU', 'Utah', 'offense');
  assert.match(html, /data-unit="offense" aria-pressed="true"/);
  assert.match(html, /data-unit="defense" aria-pressed="false"/);
  assert.match(html, /BYU offense vs Utah defense/);
  assert.match(html, /BYU defense vs Utah offense/);
  const flipped = matchupUnitToggle('BYU', 'Utah', 'defense');
  assert.match(flipped, /data-unit="offense" aria-pressed="false"/);
  assert.match(flipped, /data-unit="defense" aria-pressed="true"/);
});

test('a school colour is lifted until it can be read on the card', () => {
  // Navy on navy is the case that matters: BYU's #0057B8 measures 2.3:1 on the
  // card and has to come up, Wyoming's near-white is already clear of it.
  assert.ok(contrast('#0057B8', '#142d4c') < 4.5);
  for (const brand of ['#0057B8', '#CC0000', '#000000', '#001E62', '#F8FAFC', '#BF5700']) {
    const lifted = matchupReadableColor(brand, '#7fe3bc');
    assert.match(lifted, /^#[0-9a-f]{6}$/);
    assert.ok(contrast(lifted, '#142d4c') >= 4.5,
      brand + ' lifted to ' + lifted + ' is still ' + contrast(lifted, '#142d4c').toFixed(2) + ':1');
  }
  // Lifting keeps the hue: BYU's navy comes up blue, not grey.
  const navy = matchupReadableColor('#002E5D', '#7fe3bc');
  const [nr, ng, nb] = [1, 3, 5].map(i => parseInt(navy.slice(i, i + 2), 16));
  assert.ok(nb > nr + 40, 'a navy must stay blue once lifted: ' + navy);
  const crimson = matchupReadableColor('#9E1B32', '#7fe3bc');
  const [cr, cg, cb] = [1, 3, 5].map(i => parseInt(crimson.slice(i, i + 2), 16));
  assert.ok(cr > cb + 40 && cr > cg + 40, 'a crimson must stay red once lifted: ' + crimson);
  // A grey brand has no hue to keep, and stays grey.
  const grey = matchupLighten([60, 60, 60], 0.7);
  assert.match(grey, /^#b3b3b3$/);

  // A colour that already reads is left exactly as it is.
  assert.equal(matchupReadableColor('#F8FAFC', '#7fe3bc'), '#f8fafc');
  // And anything that is not a colour falls back rather than being invented.
  assert.equal(matchupReadableColor(null, '#7fe3bc'), '#7fe3bc');
  assert.equal(matchupReadableColor('rebeccapurple', '#7fe3bc'), '#7fe3bc');
  assert.equal(matchupReadableColor('#abc', '#7fe3bc'), '#7fe3bc');
});

test('the sentence under a row names who produced and who allowed', () => {
  const offensive = { teamName: 'BYU', oppName: 'Utah', teamUnit: 'offense', oppUnit: 'defense' };
  // Our offence leads: we produced it, they allowed it.
  assert.equal(matchupInterpretation('Rush success', 11, offensive),
    "BYU's rush success has been stronger than what Utah usually allows.");
  // Their defence leads: they defended it, we produced it.
  assert.equal(matchupInterpretation('Rush success', -11, offensive),
    'Utah has defended rush success better than BYU usually produces it.');
  assert.equal(matchupInterpretation('Pass PPA', 3, offensive),
    'BYU and Utah are closely matched in pass PPA.');
  assert.equal(matchupInterpretation('Pass PPA', -4, offensive),
    'BYU and Utah are closely matched in pass PPA.');
  // An acronym stays an acronym inside the sentence.
  assert.match(matchupInterpretation('Rush PPA', 11, offensive), /rush PPA has been stronger/);
  // A trait neither side has rated says nothing at all.
  assert.equal(matchupInterpretation('Pass PPA', null, offensive), '');
});

test('the sentence turns around with the toggle, not just the labels', () => {
  // In the defensive half OUR defence faces THEIR offence, so a positive edge
  // -- still ours -- means we defended it, not that we produced it.
  const defensive = { teamName: 'BYU', oppName: 'Utah', teamUnit: 'defense', oppUnit: 'offense' };
  assert.equal(matchupInterpretation('Rush success', 11, defensive),
    'BYU has defended rush success better than Utah usually produces it.');
  assert.equal(matchupInterpretation('Rush success', -11, defensive),
    "Utah's rush success has been stronger than what BYU usually allows.");
  // The even wording is symmetric, so it reads the same in either half.
  assert.equal(matchupInterpretation('Rush PPA', 0, defensive),
    'BYU and Utah are closely matched in rush PPA.');
  // Nobody is ever said to have produced their own defence, in either half.
  for (const sides of [defensive, { teamName: 'BYU', oppName: 'Utah', teamUnit: 'offense', oppUnit: 'defense' }]) {
    for (const edge of [-25, -11, -5, 5, 11, 25]) {
      const line = matchupInterpretation('Rush success', edge, sides);
      assert.match(line, /^(BYU|Utah)('s| has)/);
      assert.ok(!/defended .* better than .* usually allows/.test(line), line);
    }
  }
});

test('every rendered row carries its sentence, escaped', () => {
  const html = renderMatchupMeters('Texas A&M', 'Utah', rated(81, 70), rated(65, 76), 'offense');
  assert.equal((html.match(/class="tm-match-note"/g) || []).length, 6);
  // The ampersand is escaped; the apostrophe is text, and needs no escaping.
  assert.match(html, /Texas A&amp;M's rush success has been stronger/);
  assert.doesNotMatch(html, /Texas A&M/);
  // An unrated trait has a card but no sentence.
  const thin = renderMatchupMeters('BYU', 'Utah', rated(81, 70), {}, 'offense');
  assert.equal((thin.match(/class="tm-match-row"/g) || []).length, 6);
  assert.equal((thin.match(/class="tm-match-note"/g) || []).length, 0);
});
