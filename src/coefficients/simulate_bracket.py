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

TEMPERATURE controls how much a given CoE gap matters. It used to be 6.0,
a value picked to produce plausible-looking upset odds and never checked
against a result. src/calibrate_sim_temperature.py has now fitted it
against the record -- the CoE gap entering each season against the actual
winner, on the games where both teams were playoff calibre -- and the
answer is about 4.5. At 6.0 the simulator was systematically
underconfident, pulling every matchup toward a coin flip: its calibration
error on those games was 0.078, against 0.020 at 4.5.

The value now lives in config/model_config.json under "simulation", with
the fit written up beside it, and is still exposed as a CLI flag. The
Brier curve is flat between roughly 4.25 and 5.0, so it is "about 4.5"
rather than a precise constant.

Home field IS applied, in the Round of 24 only, at the 1.25 CoE points the
same fit measured. It was previously measured and left inert on the
argument that CoE already decided who hosts, so applying it would
double-count. That argument was wrong: CoE decides WHICH team hosts, but it
carries no information about the advantage OF hosting, which is a venue
effect measured on real games. Leaving it out understated the host's chance
in all eight Round-of-24 games. The Round of 16 onward is played at neutral
sites -- nothing past the Round of 24 assigns a host -- so the boost stops
there, and every later round stays exactly symmetric.

Bracket structure: Round of 24 (8 games) feeds into a 16-seed Round of 16
using the same seeding as the bracket graphic (byes ranked 1-8 by 5yr CoE,
R24 games as placeholder seeds 9-16, paired 1v16, 2v15, ... 8v9). From
there it's a standard single-elimination bracket: adjacent R16 winners meet
in the Quarterfinals, and so on through the Final.

The draw is resampled for every run, so the odds average over the draw as
well as over the games. This used to be a single draw shared by all 10,000
runs, which made the published number a sample rather than the quantity: in
2026, with the field and the model held fixed and only the draw changed,
Notre Dame's title odds run from 12.0% to 27.7%. Averaged over draws the
same figure reproduces within about two points across independent runs.
Pass --fixed-draw for the other question -- the odds given a bracket that
has already been drawn, which is what the bracket page displays.

Usage:
    python src/coefficients/simulate_bracket.py --year 2025 --draw-seed 1 --sims 10000
"""
from __future__ import annotations

import argparse
import json
import math
import random
import sqlite3
from collections import defaultdict
from pathlib import Path

from select_playoff_field_v2 import (
    YEAR1_BIDS, YEAR2_BIDS, load_conference_coe_rank, load_team_coe_5yr,
    select_qualifiers, assign_pots, assign_homefield,
    get_independent_teams_with_coe, apply_independent_threshold,
)
from draw_playoff_bracket_v2 import backtrack_pairings, choose_home_away, build_conf_map

CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "model_config.json"


def _configured_temperature() -> float:
    """
    The fitted temperature from config/model_config.json.

    Read once at import, with the historical 6.0 as the fallback, so that a
    config trimmed down or missing in a bare checkout still runs rather than
    failing at the top of the module.
    """
    try:
        with open(CONFIG_PATH) as f:
            return float(json.load(f)["simulation"]["temperature"])
    except (OSError, KeyError, ValueError, TypeError):
        return 6.0


def _configured_home_field() -> float:
    """
    The host's edge in CoE points, from the same config section.

    Falls back to 0.0 rather than to the measured 1.25: if the config cannot be
    read, the safe default is the neutral-site model that makes no claim about
    venue, not a number this function only guessed at.
    """
    try:
        with open(CONFIG_PATH) as f:
            return float(json.load(f)["simulation"]["home_field"])
    except (OSError, KeyError, ValueError, TypeError):
        return 0.0


DEFAULT_TEMPERATURE = _configured_temperature()
DEFAULT_HOME_FIELD = _configured_home_field()


def win_probability(coe_a: float, coe_b: float, temperature: float,
                    home_field: float = 0.0) -> float:
    """
    P(team A beats team B), given their CoE values.

    `home_field` is team A's venue edge in CoE points: positive when A hosts,
    negative when B hosts, zero at a neutral site. It defaults to zero so that
    the neutral-site case is the one you get by saying nothing.
    """
    gap = coe_a - coe_b + home_field
    return 1.0 / (1.0 + math.exp(-gap / temperature))


def simulate_game(team_a: str, team_b: str, team_coe: dict, temperature: float,
                  rng: random.Random, home_field: float = 0.0, host: str | None = None) -> str:
    """
    One game. `host` names whichever of the two teams is at home, or None for a
    neutral site; `home_field` is the edge in CoE points. A host that is neither
    team is a caller bug, so it raises rather than silently playing it neutral.
    """
    edge = 0.0
    if host is not None:
        if host == team_a:
            edge = home_field
        elif host == team_b:
            edge = -home_field
        else:
            raise ValueError(f"host {host!r} is not playing in {team_a!r} vs {team_b!r}")
    p_a = win_probability(team_coe.get(team_a, 0.0), team_coe.get(team_b, 0.0), temperature, edge)
    return team_a if rng.random() < p_a else team_b


def build_field(db_path: str, year: int) -> tuple[list, list, dict, dict]:
    """Returns (byes, round_of_24_pairs, team_coe, conf_of)."""
    bid_table = YEAR1_BIDS if year == 2014 else YEAR2_BIDS
    conn = sqlite3.connect(db_path)
    conf_ranked = load_conference_coe_rank(conn, year)
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


# How many shuffles to try before calling a field unpairable. A single shuffle
# can fail the same-conference constraint by luck, which matters now that a draw
# happens inside the simulation loop: one unlucky shuffle must not end a run.
# A field that genuinely cannot be paired fails every attempt, so the error is
# still reached -- it just takes evidence rather than one try.
DRAW_ATTEMPTS = 50


def draw_with(byes: list, pot1: list, pot2: list, conf_of: dict, rng: random.Random) -> list:
    """
    One Round of 24 draw, from the caller's own random source.

    Split out from draw_round_of_24 so the simulation can draw a FRESH bracket
    per run off its own rng, rather than every run sharing one bracket.
    """
    for _ in range(DRAW_ATTEMPTS):
        p1, p2 = pot1[:], pot2[:]
        rng.shuffle(p1)
        rng.shuffle(p2)
        pairs = backtrack_pairings(p1, p2, conf_of, p2[:])
        if pairs is not None:
            return pairs
    raise SystemExit(f"No valid Round of 24 pairing after {DRAW_ATTEMPTS} attempts.")


def draw_round_of_24(byes: list, pot1: list, pot2: list, conf_of: dict, draw_seed: int) -> list:
    """The one reproducible draw a given seed names -- what the bracket page shows."""
    return draw_with(byes, pot1, pot2, conf_of, random.Random(draw_seed))


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


def simulate_one_bracket(byes, r24_pairs, r16_seed_order, team_coe, temperature, rng,
                        home_field: float = 0.0) -> dict:
    """
    Runs one full simulated bracket. Returns {team: furthest_round_reached}
    where furthest_round is one of: 'r24_participant', 'r16', 'qf', 'sf', 'final', 'champion'.
    Bye teams always start at 'r16' (guaranteed, not simulated).

    `home_field` applies to the Round of 24 alone, where the higher-CoE team of
    each pair hosts. Later rounds are at neutral sites, so they are simulated
    with no venue term at all.
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
            # The one hosted round. choose_home_away is the same function the
            # bracket graphic uses to print "away @ home", so the simulation and
            # the displayed bracket cannot disagree about who is at home.
            host, _ = choose_home_away(a, b, team_coe)
            winner = simulate_game(a, b, team_coe, temperature, rng, home_field, host)
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


def run_simulation(db_path: str, year: int, draw_seed: int, n_sims: int, temperature: float,
                   sim_seed: int = 0, fixed_draw: bool = False, home_field: float = 0.0):
    """
    Title odds over `n_sims` brackets.

    By default the Round of 24 is REDRAWN for every run, so the odds average
    over the draw as well as over the games. That is what a title chance means
    before the draw happens, and the draw matters enormously: in 2026, holding
    the field and the model fixed and changing only which draw is used, Notre
    Dame's title odds run from 12.0% to 27.7%. Reporting one draw's number as
    "Title Odds" was reporting a sample as if it were the quantity.

    `fixed_draw` keeps the old behaviour -- every run on the single bracket
    `draw_seed` names -- which is the right question for odds conditioned on a
    bracket already drawn, as the bracket page shows one.
    """
    byes, pot1, pot2, team_coe, conf_of = build_field(db_path, year)

    rng = random.Random(sim_seed)
    # The draw rng is separate from the game rng, so that changing the number of
    # simulations does not reshuffle which brackets get drawn, and a fixed-draw
    # run and a redrawn run at the same sim_seed play the same games.
    draw_rng = random.Random((sim_seed, draw_seed).__hash__())

    fixed = None
    if fixed_draw:
        pairs = draw_round_of_24(byes, pot1, pot2, conf_of, draw_seed)
        fixed = (pairs, r16_bracket_order(build_r16_seeds(byes, pairs, team_coe)))

    counts = defaultdict(lambda: defaultdict(int))  # team -> round -> count reaching AT LEAST that round

    for _ in range(n_sims):
        if fixed is not None:
            r24_pairs, r16_seed_order = fixed
        else:
            r24_pairs = draw_with(byes, pot1, pot2, conf_of, draw_rng)
            r16_seed_order = r16_bracket_order(build_r16_seeds(byes, r24_pairs, team_coe))
        result = simulate_one_bracket(byes, r24_pairs, r16_seed_order, team_coe, temperature, rng,
                                      home_field)
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
    p.add_argument("--home-field", type=float, default=DEFAULT_HOME_FIELD,
                   help="the host's edge in CoE points, applied in the Round of 24 only "
                        "(0 turns it off)")
    p.add_argument("--sim-seed", type=int, default=0)
    p.add_argument("--fixed-draw", action="store_true",
                   help="run every simulation on the single bracket --draw-seed names, "
                        "instead of redrawing the Round of 24 each run")
    args = p.parse_args()

    counts, n_sims, conf_of, team_coe = run_simulation(
        args.db, args.year, args.draw_seed, args.sims, args.temperature, args.sim_seed,
        fixed_draw=args.fixed_draw, home_field=args.home_field,
    )

    drawing = (f"one fixed draw, seed {args.draw_seed}" if args.fixed_draw
               else "redrawn each run")
    print(f"=== {args.year} Monte Carlo bracket simulation ({n_sims:,} runs, "
          f"temperature={args.temperature}, home field {args.home_field} CoE in the "
          f"Round of 24, {drawing}) ===\n")
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
