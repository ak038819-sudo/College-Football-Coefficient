#!/usr/bin/env python3
"""
Monte Carlo bracket simulation: since nobody's built a way to know who
actually WINS a game (that's a genuinely different problem from
selection/seeding), this converts the CoE rating gap between two teams
into a win probability and simulates the full 24-team bracket thousands
of times, tallying how often each team reaches each round and wins the
whole thing.

Win probability model (standard logistic, same family as Elo):
    P(A beats B) = 1 / (1 + exp(-(coe_A - coe_B) / TEMPERATURE))

TEMPERATURE controls how much a given CoE gap matters. Default of 6.0
was picked to produce plausible-looking upset odds (a 5-point favorite
wins about 70% of the time, a 15-point favorite about 92%) -- this is
a genuine modeling choice, not derived from the rules, and is exposed
as a CLI flag so it can be tuned.

No home-field boost is added on top of this -- home field was already
decided by CoE (the higher-CoE team hosts), so adding a separate boost
would double-count that advantage.

Bracket structure: Round of 24 (8 games, already drawn) feeds into a
16-seed Round of 16 using the same seeding as the bracket graphic (byes
ranked 1-8 by 5yr CoE, R24 games as placeholder seeds 9-16, paired
1v16, 2v15, ... 8v9). From there it's a standard single-elimination
bracket: adjacent R16 winners meet in the Quarterfinals, and so on
through the Final.

Usage:
    python src/coefficients/simulate_bracket.py --year 2025 --draw-seed 1 --sims 10000
"""
from __future__ import annotations

import argparse
import math
import random
import sqlite3
from collections import defaultdict

from select_playoff_field_v2 import (
    YEAR1_BIDS, YEAR2_BIDS, load_conference_coe_rank, load_team_coe_5yr,
    select_qualifiers, assign_pots, assign_homefield,
    get_independent_teams_with_coe, apply_independent_threshold,
)
from draw_playoff_bracket_v2 import backtrack_pairings, choose_home_away, build_conf_map

DEFAULT_TEMPERATURE = 6.0


def win_probability(coe_a: float, coe_b: float, temperature: float) -> float:
    """P(team A beats team B), given their CoE values."""
    gap = coe_a - coe_b
    return 1.0 / (1.0 + math.exp(-gap / temperature))


def simulate_game(team_a: str, team_b: str, team_coe: dict, temperature: float, rng: random.Random) -> str:
    p_a = win_probability(team_coe.get(team_a, 0.0), team_coe.get(team_b, 0.0), temperature)
    return team_a if rng.random() < p_a else team_b


def build_field(db_path: str, year: int) -> tuple[list, list, dict, dict]:
    """Returns (byes, round_of_24_pairs, team_coe, conf_of)."""
    bid_table = YEAR1_BIDS if year == 2014 else YEAR2_BIDS
    conn = sqlite3.connect(db_path)
    conf_ranked = load_conference_coe_rank(year)
    team_coe = load_team_coe_5yr(year)
    qualifiers = select_qualifiers(conn, year, conf_ranked, bid_table)
    qualifiers = assign_pots(qualifiers)
    assign_homefield(qualifiers, team_coe)
    independents = get_independent_teams_with_coe(conn, year, team_coe)
    qualifiers, _ = apply_independent_threshold(qualifiers, independents)
    conn.close()

    byes = [q["team_name"] for q in qualifiers if q["pot"] == "bye"]
    pot1 = [q["team_name"] for q in qualifiers if q["pot"] == 1]
    pot2 = [q["team_name"] for q in qualifiers if q["pot"] == 2]
    conf_of = build_conf_map(qualifiers)
    return byes, pot1, pot2, team_coe, conf_of


def draw_round_of_24(byes: list, pot1: list, pot2: list, conf_of: dict, draw_seed: int) -> list:
    rng = random.Random(draw_seed)
    p1, p2 = pot1[:], pot2[:]
    rng.shuffle(p1)
    rng.shuffle(p2)
    pairs = backtrack_pairings(p1, p2, conf_of, p2[:])
    if pairs is None:
        raise SystemExit("No valid Round of 24 pairing for this draw-seed.")
    return pairs


def build_r16_seeds(byes: list, r24_pairs: list, team_coe: dict) -> list:
    """Seeds 1-8: byes by CoE desc. Seeds 9-16: R24 game slots, in order."""
    byes_sorted = sorted(byes, key=lambda t: -team_coe.get(t, 0.0))
    seeds = [{"seed": i + 1, "game_idx": None} for i in range(8)]
    for i, s in enumerate(seeds):
        s["fixed_team"] = byes_sorted[i]
    for i, pair in enumerate(r24_pairs):
        seeds.append({"seed": 9 + i, "fixed_team": None, "game_idx": i, "pair": pair})
    return seeds


def r16_bracket_order(seeds: list) -> list:
    """Standard seeding pairing: 1v16, 2v15, ... 8v9, in bracket order."""
    by_seed = {s["seed"]: s for s in seeds}
    order = []
    for i in range(1, 9):
        order.append(by_seed[i])
        order.append(by_seed[17 - i])
    return order


def simulate_one_bracket(byes, r24_pairs, r16_seed_order, team_coe, temperature, rng) -> dict:
    """
    Runs one full simulated bracket. Returns {team: furthest_round_reached}
    where furthest_round is one of: 'r24_participant', 'r16', 'qf', 'sf', 'final', 'champion'.
    Bye teams always start at 'r16' (guaranteed, not simulated).
    """
    result = {}

    # Round of 24
    r16_field = []  # list of team names, in the same bracket-order positions as r16_seed_order
    for slot in r16_seed_order:
        if slot["fixed_team"] is not None:
            team = slot["fixed_team"]
            result[team] = "r16"  # byes guaranteed here, may advance further below
            r16_field.append(team)
        else:
            a, b = slot["pair"]
            for t in (a, b):
                result.setdefault(t, "r24_participant")
            winner = simulate_game(a, b, team_coe, temperature, rng)
            result[winner] = "r16"
            r16_field.append(winner)

    # Round of 16 -> Quarterfinals
    qf_field = []
    for i in range(0, 16, 2):
        a, b = r16_field[i], r16_field[i + 1]
        winner = simulate_game(a, b, team_coe, temperature, rng)
        result[winner] = "qf"
        qf_field.append(winner)

    # Quarterfinals -> Semifinals
    sf_field = []
    for i in range(0, 8, 2):
        a, b = qf_field[i], qf_field[i + 1]
        winner = simulate_game(a, b, team_coe, temperature, rng)
        result[winner] = "sf"
        sf_field.append(winner)

    # Semifinals -> Final
    final_field = []
    for i in range(0, 4, 2):
        a, b = sf_field[i], sf_field[i + 1]
        winner = simulate_game(a, b, team_coe, temperature, rng)
        result[winner] = "final"
        final_field.append(winner)

    # Final -> Champion
    champion = simulate_game(final_field[0], final_field[1], team_coe, temperature, rng)
    result[champion] = "champion"

    return result


ROUND_ORDER = ["r24_participant", "r16", "qf", "sf", "final", "champion"]
ROUND_RANK = {r: i for i, r in enumerate(ROUND_ORDER)}


def run_simulation(db_path: str, year: int, draw_seed: int, n_sims: int, temperature: float, sim_seed: int = 0):
    byes, pot1, pot2, team_coe, conf_of = build_field(db_path, year)
    r24_pairs = draw_round_of_24(byes, pot1, pot2, conf_of, draw_seed)
    seeds = build_r16_seeds(byes, r24_pairs, team_coe)
    r16_seed_order = r16_bracket_order(seeds)

    rng = random.Random(sim_seed)
    counts = defaultdict(lambda: defaultdict(int))  # team -> round -> count reaching AT LEAST that round

    for _ in range(n_sims):
        result = simulate_one_bracket(byes, r24_pairs, r16_seed_order, team_coe, temperature, rng)
        for team, furthest in result.items():
            reached_rank = ROUND_RANK[furthest]
            for r in ROUND_ORDER:
                if ROUND_RANK[r] <= reached_rank:
                    counts[team][r] += 1

    return counts, n_sims, conf_of, team_coe


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--draw-seed", type=int, default=1)
    p.add_argument("--sims", type=int, default=10000)
    p.add_argument("--temperature", type=float, default=DEFAULT_TEMPERATURE)
    p.add_argument("--sim-seed", type=int, default=0)
    args = p.parse_args()

    counts, n_sims, conf_of, team_coe = run_simulation(
        args.db, args.year, args.draw_seed, args.sims, args.temperature, args.sim_seed
    )

    print(f"=== {args.year} Monte Carlo bracket simulation ({n_sims:,} runs, temperature={args.temperature}) ===\n")
    print(f"{'Team':<20} {'Conf':<18} {'CoE':>7} {'R16':>7} {'QF':>7} {'SF':>7} {'Final':>7} {'Champ':>7}")
    rows = sorted(counts.items(), key=lambda kv: -kv[1]["champion"])
    for team, c in rows:
        pct = lambda r: f"{100*c[r]/n_sims:.1f}%"
        print(
            f"{team:<20} {conf_of.get(team,''):<18} {team_coe.get(team,0.0):>7.3f} "
            f"{pct('r16'):>7} {pct('qf'):>7} {pct('sf'):>7} {pct('final'):>7} {pct('champion'):>7}"
        )


if __name__ == "__main__":
    main()
