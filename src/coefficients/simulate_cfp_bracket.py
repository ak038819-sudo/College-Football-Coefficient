#!/usr/bin/env python3
"""Monte Carlo simulation of the real 12-team College Football Playoff bracket.

Replaces the 24-team simulation as the live model (owner's decision,
2026-10-10). simulate_bracket.py is still here and still runs the 24-team
bracket; this module borrows its win-probability model unchanged, because
turning a CoE gap into a win chance has nothing to do with how many teams
are in the field:

    P(A beats B) = 1 / (1 + exp(-(coe_A - coe_B + home_edge) / TEMPERATURE))

TEMPERATURE (about 4.5) and the home-field edge (1.25 CoE points) are the
fitted values in config/model_config.json, measured in
src/calibrate_sim_temperature.py against every game since 1980 between two
playoff-calibre teams. Neither was refitted for this format: both describe
how a CoE gap and a home ground behave in a football game, not how a
bracket is shaped.

THE ODDS ARE NOW A PROPERTY OF THE BRACKET, NOT AN AVERAGE OVER DRAWS

This is the one real difference from the 24-team simulation, and it is an
improvement. That bracket paired a random Pot 1 team against a random Pot 2
team, so a team's title chance depended on a draw that had not happened:
in 2026, holding the field and the model fixed and changing only the draw,
Notre Dame's odds ran from 12.0% to 27.7%. The published figure had to
average over draws to mean anything, and still described no bracket anyone
could look at.

A 12-team bracket is fully determined by the seeds. Seed 5 plays seed 12,
and the winner plays seed 4, in every run. So these odds are the odds of
the bracket on the page, and the only randomness left is the games.

HOME FIELD APPLIES IN THE FIRST ROUND ONLY

The higher seed hosts the first round; the quarterfinals, semifinals and
final are at neutral sites, so they are simulated with no venue term at
all. Seeds 1-4 never play a hosted game -- their reward is not playing one.

Usage:
    python src/coefficients/simulate_cfp_bracket.py --year 2026 --sims 10000
"""
from __future__ import annotations

import argparse
import random
import sqlite3
from collections import defaultdict

from select_cfp_field import (
    FIRST_ROUND_PAIRS, QUARTERFINALS, byes, first_round_games,
    load_team_coe_5yr, select_field,
)
from simulate_bracket import DEFAULT_HOME_FIELD, DEFAULT_TEMPERATURE, simulate_game

# Reaching order, weakest first. A team's furthest round implies every
# earlier one, which is what makes the cumulative counting below correct.
ROUND_ORDER = ["first_round", "qf", "sf", "final", "champion"]
ROUND_RANK = {r: i for i, r in enumerate(ROUND_ORDER)}


def simulate_one_bracket(field: list[dict], team_coe: dict, temperature: float,
                         rng: random.Random, home_field: float = 0.0) -> dict:
    """One full bracket. Returns {team: furthest round reached}.

    Seeds 1-4 start at 'qf' because they are there without playing; seeds
    5-12 start at 'first_round' and reach 'qf' only by winning.
    """
    by_seed = {q["seed"]: q["team"] for q in field}
    result = {}

    # First round, at the higher seed's home.
    winner_of: dict[tuple[int, int], str] = {}
    for high, low in FIRST_ROUND_PAIRS:
        host, visitor = by_seed[high], by_seed[low]
        result[host] = result[visitor] = "first_round"
        won = simulate_game(host, visitor, team_coe, temperature, rng, home_field, host)
        winner_of[(high, low)] = won

    # Quarterfinals onward are neutral-site.
    qf_winners = []
    for bye_seed, pair in QUARTERFINALS:
        bye_team = by_seed[bye_seed]
        result[bye_team] = "qf"
        opponent = winner_of[pair]
        result[opponent] = "qf"
        qf_winners.append(simulate_game(bye_team, opponent, team_coe, temperature, rng))
    for team in qf_winners:
        result[team] = "sf"

    finalists = [
        simulate_game(qf_winners[0], qf_winners[1], team_coe, temperature, rng),
        simulate_game(qf_winners[2], qf_winners[3], team_coe, temperature, rng),
    ]
    for team in finalists:
        result[team] = "final"

    result[simulate_game(finalists[0], finalists[1], team_coe, temperature, rng)] = "champion"
    return result


def run_simulation(db_path: str, year: int, n_sims: int, temperature: float,
                   sim_seed: int = 0, home_field: float = 0.0):
    """Title odds over `n_sims` runs of the one bracket the seeds determine.

    Returns (counts, n_sims, field, team_coe), where counts[team][round] is how
    often that team reached AT LEAST that round.
    """
    conn = sqlite3.connect(db_path)
    team_coe = load_team_coe_5yr(year)
    field = select_field(conn, year, team_coe)
    conn.close()

    rng = random.Random(sim_seed)
    counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for _ in range(n_sims):
        for team, furthest in simulate_one_bracket(
                field, team_coe, temperature, rng, home_field).items():
            for r in ROUND_ORDER:
                if ROUND_RANK[r] <= ROUND_RANK[furthest]:
                    counts[team][r] += 1
    return counts, n_sims, field, team_coe


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--sims", type=int, default=10000)
    p.add_argument("--temperature", type=float, default=DEFAULT_TEMPERATURE)
    p.add_argument("--home-field", type=float, default=DEFAULT_HOME_FIELD,
                   help="the host's edge in CoE points, applied in the first round only "
                        "(0 turns it off)")
    p.add_argument("--sim-seed", type=int, default=0)
    args = p.parse_args()

    counts, n_sims, field, team_coe = run_simulation(
        args.db, args.year, args.sims, args.temperature, args.sim_seed, args.home_field)
    seed_of = {q["team"]: q["seed"] for q in field}
    conf_of = {q["team"]: q["conference"] for q in field}

    print(f"=== {args.year} College Football Playoff simulation ({n_sims:,} runs, "
          f"temperature={args.temperature}, home field {args.home_field} CoE in the "
          f"first round) ===\n")
    print(f"{'Sd':>3} {'Team':<22} {'Conf':<18} {'CoE':>7} {'QF':>7} {'SF':>7} "
          f"{'Final':>7} {'Champ':>7}")
    for team, c in sorted(counts.items(), key=lambda kv: -kv[1]["champion"]):
        pct = lambda r: f"{100 * c[r] / n_sims:.1f}%"
        print(f"{seed_of[team]:>3} {team:<22} {conf_of.get(team, ''):<18} "
              f"{team_coe.get(team, 0.0):>7.3f} {pct('qf'):>7} {pct('sf'):>7} "
              f"{pct('final'):>7} {pct('champion'):>7}")


if __name__ == "__main__":
    main()
