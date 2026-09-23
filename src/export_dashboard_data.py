#!/usr/bin/env python3
"""
Exports everything the dashboard needs into one JSON file:
  - team ratings by season (all years with data)
  - team 5yr rolling CoE (years with a computed window)
  - conference ratings by season (years with membership data, detected dynamically)
  - conference 5yr rolling CoE
  - playoff field + Round-of-24 bracket draw for every season that has
    both membership data AND enough of it to actually build a 24-team
    field (a year can have membership but still fail, e.g. too few
    teams overall -- see the try/except around build_playoff_data)

Usage:
    python src/export_dashboard_data.py --draw-seed 1 --out ui/dashboard_data.json
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "coefficients"))
from select_playoff_field_v2 import (  # noqa: E402
    YEAR1_BIDS, YEAR2_BIDS, load_conference_coe_rank, load_team_coe_5yr,
    select_qualifiers, assign_pots, assign_homefield,
    get_independent_teams_with_coe, apply_independent_threshold,
)
from draw_playoff_bracket_v2 import (  # noqa: E402
    backtrack_pairings, choose_home_away, build_conf_map,
)
from simulate_bracket import run_simulation, DEFAULT_TEMPERATURE  # noqa: E402
import random

DATA_DIR = Path("data/processed")


def load_elo_by_season(db_path: str) -> dict:
    """
    Each team's END-OF-SEASON Elo rating: their postgame_elo from their
    LAST game (by date, game_id as tiebreak) within that season_year.
    elo_game_history is a per-GAME audit trail (build_elo.py, CoE 2.0
    Phase 2) with no season_year column of its own, so this joins
    through games for that and picks the season's final row per team
    via a window function. Elo itself is a single continuously-carried
    rating (unlike CoE's discrete per-season sums), so "end of season"
    is the natural snapshot for a season-by-season display -- there's
    no equivalent of CoE's 5yr rolling window for Elo, it's already a
    single running number.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT season_year, team_name, postgame_elo FROM (
            SELECT g.season_year, t.team_name, e.postgame_elo,
                   ROW_NUMBER() OVER (
                       PARTITION BY g.season_year, e.team_id
                       ORDER BY g.game_date DESC, e.game_id DESC
                   ) AS rn
            FROM elo_game_history e
            JOIN games g ON g.game_id = e.game_id
            JOIN teams t ON t.team_id = e.team_id
        )
        WHERE rn = 1
        """
    ).fetchall()
    conn.close()

    by_year = defaultdict(list)
    for r in rows:
        by_year[r["season_year"]].append({"team": r["team_name"], "elo": round(r["postgame_elo"], 1)})
    for year in by_year:
        by_year[year].sort(key=lambda x: -x["elo"])
    return dict(by_year)


def years_with_membership_data(db_path: str) -> list[int]:
    """
    Which years actually have conference-membership data in the DB right
    now -- NOT a static range. This matters because the current season's
    membership can only come from a LIVE CFBD API fetch (a manual step
    with your own API key), which isn't captured in any committed file.
    A fresh --force rebuild (e.g. in CI, with no API key) will have games
    for the current year but zero membership rows for it, exactly like
    2010-2013 already correctly have no membership at all. Treating this
    dynamically means a year silently degrades to "ratings only, no
    playoff field" instead of crashing the whole export.
    """
    conn = sqlite3.connect(db_path)
    years = sorted(
        int(r[0])
        for r in conn.execute(
            "SELECT DISTINCT season_year FROM team_membership_by_season WHERE season_year >= 2014"
        )
    )
    conn.close()
    return years


def load_csv_by_year(filename: str, year_field: str) -> dict:
    out = defaultdict(list)
    with open(DATA_DIR / filename, newline="") as f:
        for r in csv.DictReader(f):
            out[int(r[year_field])].append(r)
    return out


def build_playoff_data(db_path: str, year: int, draw_seed: int, sims: int, temperature: float) -> dict:
    bid_table = YEAR1_BIDS if year == 2014 else YEAR2_BIDS
    conn = sqlite3.connect(db_path)
    conf_ranked = load_conference_coe_rank(conn, year)
    team_coe = load_team_coe_5yr(year)
    qualifiers = select_qualifiers(conn, year, conf_ranked, bid_table)
    qualifiers = assign_pots(qualifiers)
    assign_homefield(qualifiers, team_coe)
    independents = get_independent_teams_with_coe(conn, year, team_coe)
    qualifiers, replacements = apply_independent_threshold(qualifiers, independents)
    conn.close()

    byes = [q["team_name"] for q in qualifiers if q["pot"] == "bye"]
    pot1 = [q["team_name"] for q in qualifiers if q["pot"] == 1]
    pot2 = [q["team_name"] for q in qualifiers if q["pot"] == 2]

    conf_of = build_conf_map(qualifiers)
    rng = random.Random(draw_seed)
    pot1_drawn, pot2_drawn = pot1[:], pot2[:]
    rng.shuffle(pot1_drawn)
    rng.shuffle(pot2_drawn)
    pairs = backtrack_pairings(pot1_drawn, pot2_drawn, conf_of, pot2_drawn[:]) or []

    games = []
    for a, b in pairs:
        home, away = choose_home_away(a, b, team_coe)
        games.append({
            "home": home, "away": away,
            "home_conf": conf_of[home], "away_conf": conf_of[away],
            "home_coe": round(team_coe.get(home, 0.0), 3),
            "away_coe": round(team_coe.get(away, 0.0), 3),
        })

    counts, n_sims, sim_conf_of, sim_team_coe = run_simulation(db_path, year, draw_seed, sims, temperature)
    simulation = [
        {
            "team": team,
            "conference": sim_conf_of.get(team, ""),
            "coe": round(sim_team_coe.get(team, 0.0), 3),
            "r16_pct": round(100 * c["r16"] / n_sims, 1),
            "qf_pct": round(100 * c["qf"] / n_sims, 1),
            "sf_pct": round(100 * c["sf"] / n_sims, 1),
            "final_pct": round(100 * c["final"] / n_sims, 1),
            "champion_pct": round(100 * c["champion"] / n_sims, 1),
        }
        for team, c in counts.items()
    ]
    simulation.sort(key=lambda r: -r["champion_pct"])

    def sort_key(q):
        if q["conf_coe_rank"] is None:
            return (999, -q["team_coe_5yr"])
        return (q["conf_coe_rank"], q["conf_standing_rank"])

    return {
        "conference_ranking": [
            {"conference": c, "coeff_5yr": round(v, 3)} for c, v in conf_ranked
        ],
        "independents": [{"team": n, "coe": round(c, 3)} for n, c in independents],
        "independent_replacements": replacements,
        "simulation": simulation,
        "sim_meta": {"n_sims": n_sims, "temperature": temperature},
        "qualifiers": [
            {
                "team": q["team_name"], "conference": q["conference"],
                "conf_coe_rank": q["conf_coe_rank"], "conf_standing_rank": q["conf_standing_rank"],
                "bid_type": q["bid_type"], "pot": str(q["pot"]),
                "team_coe_5yr": round(q["team_coe_5yr"], 3),
            }
            for q in sorted(qualifiers, key=sort_key)
        ],
        "byes": sorted(byes, key=lambda t: -team_coe.get(t, 0.0)),
        "round_of_24": games,
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--draw-seed", type=int, default=1)
    p.add_argument("--sims", type=int, default=10000)
    p.add_argument("--temperature", type=float, default=DEFAULT_TEMPERATURE)
    p.add_argument("--out", default="ui/dashboard_data.json")
    args = p.parse_args()

    team_ratings = load_csv_by_year("team_ratings_by_season.csv", "season_year")
    team_rolling = load_csv_by_year("team_coeff_5yr.csv", "end_year")
    conf_ratings = load_csv_by_year("conference_ratings_by_season.csv", "season_year")
    conf_rolling = load_csv_by_year("conference_coeff_5yr.csv", "end_year")
    team_elo = load_elo_by_season(args.db)

    membership_years = years_with_membership_data(args.db)

    def top(rows, key, n=None):
        rows_sorted = sorted(rows, key=lambda r: -float(r[key]))
        return rows_sorted if n is None else rows_sorted[:n]

    out = {
        "years_all": sorted(team_ratings.keys()),
        "years_playoff": membership_years,
        "team_ratings_by_year": {
            str(y): [{"team": r["team_name"], "rating": round(float(r["rating"]), 3)} for r in top(rows, "rating")]
            for y, rows in team_ratings.items()
        },
        "team_rolling_by_year": {
            str(y): [{"team": r["team_name"], "coeff_5yr": round(float(r["coeff_5yr"]), 3)} for r in top(rows, "coeff_5yr")]
            for y, rows in team_rolling.items()
        },
        "team_elo_by_year": {
            str(y): rows for y, rows in team_elo.items()
        },
        "conference_ratings_by_year": {
            str(y): sorted(
                [{"conference": r["conference_name"], "rating": round(float(r["rating"]), 3)} for r in rows],
                key=lambda x: -x["rating"],
            )
            for y, rows in conf_ratings.items()
        },
        "conference_rolling_by_year": {
            str(y): sorted(
                [{"conference": r["conference_name"], "coeff_5yr": round(float(r["coeff_5yr"]), 3)} for r in rows],
                key=lambda x: -x["coeff_5yr"],
            )
            for y, rows in conf_rolling.items()
        },
        "playoff_by_year": {},
    }

    usable_years = []
    for year in membership_years:
        print(f"Building playoff data for {year}...")
        try:
            out["playoff_by_year"][str(year)] = build_playoff_data(
                args.db, year, args.draw_seed, args.sims, args.temperature
            )
            usable_years.append(year)
        except Exception as e:
            print(f"  SKIPPED {year}: {e!r} (membership present but field couldn't be built -- "
                  f"likely incomplete data for this year)")

    out["years_playoff"] = usable_years

    # Aggregate playoff appearances (and bye counts) across every simulated season
    appearances: dict[str, dict] = {}
    for year in usable_years:
        pf = out["playoff_by_year"][str(year)]
        for q in pf["qualifiers"]:
            rec = appearances.setdefault(q["team"], {"team": q["team"], "appearances": 0, "byes": 0, "years": []})
            rec["appearances"] += 1
            rec["years"].append(year)
            if q["pot"] == "bye":
                rec["byes"] += 1

    out["playoff_appearances"] = sorted(
        appearances.values(), key=lambda r: (-r["appearances"], -r["byes"], r["team"])
    )

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=None, separators=(",", ":")))
    print(f"\nWrote {out_path} ({out_path.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
