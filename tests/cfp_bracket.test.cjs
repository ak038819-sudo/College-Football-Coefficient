// The page runs its own copy of the bracket simulation, for the Simulate
// button, and the published Title Odds come from the Python one. Two
// implementations of the same bracket is exactly the shape that drifts, and the
// earlier guard against it was a set of assertions on the shell's source text
// -- which passes while the code is broken, as this project has been bitten by
// before. So these drive the real functions out of the shipped page instead.
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

// Arrays the page builds inside the sandbox belong to its realm, so
// deepStrictEqual rejects them against an identical host array. Copying first
// compares the contents, which is what these tests are about.
const plain = a => Array.from(a);

const FIRST_ROUND_PAIRS = [[5, 12], [6, 11], [7, 10], [8, 9]];
const QUARTERFINALS = [[1, [8, 9]], [4, [5, 12]], [2, [7, 10]], [3, [6, 11]]];
const seedName = s => 'Seed' + String(s).padStart(2, '0');

// A payload shaped exactly as src/export_dashboard_data.py writes one, with CoE
// chosen per seed so a structural property can be made decisive.
function payload(coeBySeed, { temperature = 4.5, homeField = 1.25 } = {}) {
  const field = [];
  for (let seed = 1; seed <= 12; seed++) {
    field.push({ seed, team: seedName(seed), conference: 'Conf' + seed,
                 bid_type: 'at_large', champion_of: null, team_coe_5yr: coeBySeed(seed) });
  }
  const bySeed = {};
  field.forEach(q => { bySeed[q.seed] = q; });
  return {
    format: { field_size: 12, auto_bids: 5, bye_seeds: 4, seeding: 'straight' },
    field,
    byes: field.filter(q => q.seed <= 4).map(q => ({ seed: q.seed, team: q.team })),
    first_round: FIRST_ROUND_PAIRS.map(([hi, lo]) => ({
      home_seed: hi, away_seed: lo, home: bySeed[hi].team, away: bySeed[lo].team,
      home_conf: bySeed[hi].conference, away_conf: bySeed[lo].conference,
      home_coe: bySeed[hi].team_coe_5yr, away_coe: bySeed[lo].team_coe_5yr,
    })),
    quarterfinals: QUARTERFINALS.map(([bye_seed, pair]) => ({ bye_seed, pair })),
    sim_meta: { n_sims: 1, temperature, home_field: homeField },
  };
}

// The real page functions, with Math.random seeded so a run is reproducible.
function simulator(seed = 1) {
  let state = seed;
  const ctx = {
    Math: Object.assign(Object.create(Math), {
      random: () => { state = (state * 1103515245 + 12345) % 2147483648; return state / 2147483648; },
    }),
  };
  vm.createContext(ctx);
  vm.runInContext(
    [sourceOf('winProbability'), sourceOf('simulateGame'),
     sourceOf('runBracketSimulation'), sourceOf('bracketSlots')].join('\n') +
    '\nthis.run = runBracketSimulation; this.slots = bracketSlots;', ctx);
  return ctx;
}

test('the page simulates the bracket the export describes', () => {
  const pf = payload(seed => 20 - seed);
  const sim = simulator().run(pf);
  assert.equal(sim.firstRound.length, 4);
  assert.equal(sim.qfField.length, 4);
  assert.equal(sim.sfField.length, 2);
  assert.ok(sim.champion);
  // Every first-round winner is one of the two teams that played.
  sim.firstRound.forEach((winner, i) => {
    const g = pf.first_round[i];
    assert.ok(winner === g.home || winner === g.away, winner);
  });
  // Every quarterfinal is its bye seed against the right first-round winner.
  sim.qfField.forEach((winner, i) => {
    const q = pf.quarterfinals[i];
    const feeder = sim.firstRound[pf.first_round.findIndex(
      g => g.home_seed === q.pair[0] && g.away_seed === q.pair[1])];
    assert.ok(winner === seedName(q.bye_seed) || winner === feeder,
      `quarterfinal ${i} produced ${winner}, which played in neither slot`);
  });
  assert.ok(plain(sim.sfField).every(t => plain(sim.qfField).includes(t)));
  assert.ok(plain(sim.sfField).includes(sim.champion));
});

test('the top two seeds can only meet in the final', () => {
  // Unbeatable top two: if the page paired the halves wrong they would meet a
  // round early and only one could reach the final.
  const pf = payload(seed => (seed <= 2 ? 1000 : 1));
  for (let s = 1; s <= 20; s++) {
    const sim = simulator(s).run(pf);
    assert.deepEqual(plain(sim.sfField).sort(), ['Seed01', 'Seed02'],
      'run ' + s + ' had a final of ' + sim.sfField + ' -- the bracket halves are wrong');
  }
});

test('the page applies the host edge in the first round and nowhere else', () => {
  // Twelve equal teams and a decisive venue edge: the first round is settled
  // entirely by who hosts, and every later round has to be a coin flip.
  const pf = payload(() => 5, { homeField: 500 });
  let hostWins = 0, byeAdvances = 0;
  const runs = 200;
  for (let s = 1; s <= runs; s++) {
    const sim = simulator(s).run(pf);
    pf.first_round.forEach((g, i) => { if (sim.firstRound[i] === g.home) hostWins++; });
    pf.quarterfinals.forEach((q, i) => {
      if (sim.qfField[i] === seedName(q.bye_seed)) byeAdvances++;
    });
  }
  assert.equal(hostWins, runs * 4,
    'the host did not win every first-round game, so the page is not passing the venue edge');
  const share = byeAdvances / (runs * 4);
  assert.ok(share > 0.4 && share < 0.6,
    'bye seeds took ' + Math.round(share * 100) + '% of quarterfinals between equal teams; ' +
    'a neutral site means about half, so the venue edge is leaking past the first round');
});

test('a payload with no temperature does not quietly get a made-up one', () => {
  // The button once ran its own hardcoded 6.0 and so disagreed with the Title
  // Odds table beside it. A `|| 6.0` fallback would make that silent again:
  // the page would publish plausible-looking odds from a number nobody fitted.
  // With nothing shipped, every game degenerates instead of looking fine --
  // the away side wins all four first-round games, which no real temperature
  // produces when the host is this much stronger.
  // Gaps wide enough that any real temperature makes the host a certainty.
  // Only the temperature is withheld: a missing host edge poisons the same
  // arithmetic, so leaving both out would not say which one was substituted.
  const pf = payload(seed => 1000 / seed);
  const sim = simulator(3).run(Object.assign({}, pf, { sim_meta: { home_field: 0 } }));
  assert.deepEqual(plain(sim.firstRound), ['Seed12', 'Seed11', 'Seed10', 'Seed09'],
    'a missing temperature produced a plausible bracket, which means the page ' +
    'substituted a constant of its own instead of using the shipped value');
});

test('a payload with no host edge does not quietly get a made-up one', () => {
  // The same again for the venue term, which was measured at 1.25 CoE points.
  // A fallback would have the page claim a host advantage the export never
  // shipped -- and the neutral-site case, 0, is the one that must survive
  // being passed explicitly.
  const pf = payload(seed => 1000 / seed);
  const sim = simulator(3).run(Object.assign({}, pf, { sim_meta: { temperature: 4.5 } }));
  assert.deepEqual(plain(sim.firstRound), ['Seed12', 'Seed11', 'Seed10', 'Seed09'],
    'a missing host edge produced a plausible bracket, so the page substituted ' +
    'a constant of its own');
  // And an explicit zero is honoured rather than treated as absent.
  const neutral = simulator(3).run(Object.assign({}, pf,
    { sim_meta: { temperature: 0.001, home_field: 0 } }));
  assert.deepEqual(plain(neutral.firstRound), ['Seed05', 'Seed06', 'Seed07', 'Seed08']);
});

test('the page reads the temperature and host edge from the payload', () => {
  // A payload with no sim_meta must not silently fall back to the old
  // hand-picked 6.0 or to a hardcoded 1.25: it must produce NaN-free nonsense
  // loudly rather than publishing numbers from a constant nobody fitted.
  const pf = payload(seed => 20 - seed);
  const low = simulator(5).run(Object.assign({}, pf,
    { sim_meta: { temperature: 0.001, home_field: 0 } }));
  // At a temperature near zero the stronger team always wins, so the bracket is
  // the seeds. That can only be true if the page actually read the payload.
  assert.deepEqual(plain(low.firstRound), ['Seed05', 'Seed06', 'Seed07', 'Seed08']);
  assert.equal(low.champion, 'Seed01');
});

test('the bracket graphic and the simulation agree on every slot', () => {
  const pf = payload(seed => 20 - seed);
  const ctx = simulator();
  const slots = ctx.slots(pf);
  assert.equal(slots.length, 8, 'the bracket draws eight quarterfinal slots');
  // Alternating bye seed and the game that feeds it, in quarterfinal order.
  slots.forEach((slot, i) => {
    const q = pf.quarterfinals[Math.floor(i / 2)];
    if (i % 2 === 0) {
      assert.ok(slot.isTeam && slot.seed === q.bye_seed, 'slot ' + i);
      assert.equal(slot.team, seedName(q.bye_seed));
    } else {
      assert.equal(slot.isTeam, false);
      assert.equal(slot.game.home_seed, q.pair[0]);
      assert.equal(slot.game.away_seed, q.pair[1]);
      // The index the graphic fills from must be the index the simulation
      // returns, or the picture shows one team and the maths used another.
      assert.equal(slot.index, pf.first_round.indexOf(slot.game));
    }
  });
  assert.equal(new Set(slots.filter(s => s.isTeam).map(s => s.seed)).size, 4);
});
