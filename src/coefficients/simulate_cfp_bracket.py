#!/usr/bin/env python3
"""Monte Carlo simulation of the real 12-team College Football Playoff bracket.

Replaces the 24-team simulation as the live model (owner's decision,
2026-10-10). simulate_bracket.py is still here and still runs the 24-team
bracket on the CoE gap at a fitted temperature.

THE GAME MODEL IS ELO'S OWN

Every game here is decided by the rating engine's pregame win probability,
computed by the same two functions build_elo.py uses on a real game:

    P(home) = 1 / (1 + 10 ** ((eff_away - eff_home) / scale))

with the host's rating raised by elo.home_field and both ratings left alone
at a neutral site. No temperature and no separate venue term: this
probability is already calibrated, it is the number on every game card, and
it is what the Track Record page scores at 72.5% over 30,408 games.

This followed the committee stand-in (owner, 2026-10-12). Seeding by Elo
and simulating on the CoE gap would have put two metrics on one page
disagreeing about who is stronger -- a 1-seed can be an underdog in its own
bracket when the thing that seeded it is not the thing that plays it. The
CoE temperature of 4.5 and its 1.25-point home edge are still fitted values
and still describe a CoE gap correctly; they simply are not what ranks this
field any more.

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
final are at neutral sites, so they are simulated as neutral games, which
in Elo's terms means neither side gets elo.home_field. Seeds 1-4 never play
a hosted game -- their reward is not playing one.

Usage:
    python src/coefficients/simulate_cfp_bracket.py --year 2026 --sims 10000
"""
from __future__ import annotations

import argparse
import random
import sqlite3
from collections import defaultdict

from predict_upcoming import elo_config, game_expectation
from select_cfp_field import (
    FIRST_ROUND_PAIRS, QUARTERFINALS, RANKING_LABEL, byes, first_round_games,
    load_team_elo, select_field,
)

# Reaching order, weakest first. A team's furthest round implies every
# earlier one, which is what makes the cumulative counting below correct.
ROUND_ORDER = ["first_round", "qf", "sf", "final", "champion"]
ROUND_RANK = {r: i for i, r in enumerate(ROUND_ORDER)}


def simulate_game(home: str, away: str, ratings: dict, cfg: dict, rng: random.Random,
                  neutral: bool) -> str:
    """One game, decided by Elo's own pregame probability.

    `home` hosts unless `neutral`, in which case the two names are only an
    order and carry no advantage. A team with no rating is played at the
    engine's own prior rather than at zero, which would be a 100% loss: a
    field is built from ranked teams, so this is a guard, not a path.
    """
    r_home = ratings.get(home, cfg["initial_rating"])
    r_away = ratings.get(away, cfg["initial_rating"])
    return home if rng.random() < game_expectation(r_home, r_away, neutral, cfg) else away


def simulate_one_bracket(field: list[dict], ratings: dict, cfg: dict,
                         rng: random.Random) -> dict:
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
        winner_of[(high, low)] = simulate_game(host, visitor, ratings, cfg, rng, neutral=False)

    # Quarterfinals onward are neutral-site.
    qf_winners = []
    for bye_seed, pair in QUARTERFINALS:
        bye_team = by_seed[bye_seed]
        result[bye_team] = "qf"
        opponent = winner_of[pair]
        result[opponent] = "qf"
        qf_winners.append(
            simulate_game(bye_team, opponent, ratings, cfg, rng, neutral=True))
    for team in qf_winners:
        result[team] = "sf"

    finalists = [
        simulate_game(qf_winners[0], qf_winners[1], ratings, cfg, rng, neutral=True),
        simulate_game(qf_winners[2], qf_winners[3], ratings, cfg, rng, neutral=True),
    ]
    for team in finalists:
        result[team] = "final"

    result[simulate_game(finalists[0], finalists[1], ratings, cfg, rng,
                         neutral=True)] = "champion"
    return result


def run_simulation(db_path: str, year: int, n_sims: int, sim_seed: int = 0,
                   cfg: dict | None = None):
    """Title odds over `n_sims` runs of the one bracket the seeds determine.

    Returns (counts, n_sims, field, ratings), where counts[team][round] is how
    often that team reached AT LEAST that round. One ranking does both jobs:
    it seeds the field and it plays the games.
    """
    cfg = cfg or elo_config()
    conn = sqlite3.connect(db_path)
    ratings = load_team_elo(conn, year)
    field = select_field(conn, year, ratings)
    conn.close()

    rng = random.Random(sim_seed)
    counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for _ in range(n_sims):
        for team, furthest in simulate_one_bracket(field, ratings, cfg, rng).items():
            for r in ROUND_ORDER:
                if ROUND_RANK[r] <= ROUND_RANK[furthest]:
                    counts[team][r] += 1
    return counts, n_sims, field, ratings


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--sims", type=int, default=10000)
    p.add_argument("--sim-seed", type=int, default=0)
    args = p.parse_args()

    cfg = elo_config()
    counts, n_sims, field, ratings = run_simulation(args.db, args.year, args.sims,
                                                   args.sim_seed, cfg)
    seed_of = {q["team"]: q["seed"] for q in field}
    conf_of = {q["team"]: q["conference"] for q in field}

    print(f"=== {args.year} College Football Playoff simulation ({n_sims:,} runs, "
          f"{RANKING_LABEL} win probability, host +{cfg['home_field']} in the "
          f"first round) ===\n")
    print(f"{'Sd':>3} {'Team':<22} {'Conf':<18} {RANKING_LABEL:>7} {'QF':>7} {'SF':>7} "
          f"{'Final':>7} {'Champ':>7}")
    for team, c in sorted(counts.items(), key=lambda kv: -kv[1]["champion"]):
        pct = lambda r: f"{100 * c[r] / n_sims:.1f}%"
        print(f"{seed_of[team]:>3} {team:<22} {conf_of.get(team, ''):<18} "
              f"{ratings.get(team, 0.0):>7.0f} {pct('qf'):>7} {pct('sf'):>7} "
              f"{pct('final'):>7} {pct('champion'):>7}")


if __name__ == "__main__":
    main()
