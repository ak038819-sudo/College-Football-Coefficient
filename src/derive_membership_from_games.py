#!/usr/bin/env python3
"""
Derives team conference membership per season directly from already-
fetched game data, instead of needing a separate external membership
source. CFBD's /games endpoint carries home_conference/away_conference
on every game (confirmed populated with zero gaps on real 1980 and
1990 test fetches) -- this was previously blocked ("no source") for
years before 2014, when we only had an explicit external membership
feed. Extending fetch_cfbd_games.py to also capture these two fields
(a separate, already-applied change) is what made this derivation
possible.

For each team in a season, looks at every game they played (as either
home or away) and takes the MODE (most common) conference value seen
across those games as their conference for that season -- handles the
occasional single misclassified/inconsistent game without needing it
to be unanimous. A team whose games are evenly split between two
conferences (a genuine mid-season realignment, or a data quality
issue) is flagged as ambiguous rather than silently picking one.

is_fbs is set to 1 for every team that appears at all in a season's
games CSV -- fetch_cfbd_games.py's is_fbs_game() filter already only
keeps FBS-vs-FBS games, so any team surviving that filter for a given
game is confirmed FBS for that season.

Writes output in the EXACT format src/load_membership_snapshot.py
already expects (team_name,conference_real,is_fbs) -- so the existing,
unchanged loader can be reused directly, no new loading logic needed.

Usage:
    python src/derive_membership_from_games.py --years 1980 2013
    (or a single year: --years 1990 1990)
"""
from __future__ import annotations

import argparse
import csv
from collections import Counter, defaultdict
from pathlib import Path

RAW_DIR = Path("data/raw")


def derive_for_year(year: int) -> tuple[dict, list]:
    games_path = RAW_DIR / f"games_{year}.csv"
    if not games_path.exists():
        return {}, []

    conf_votes = defaultdict(Counter)  # team_name -> Counter({conference: count})
    with games_path.open(newline="") as f:
        for row in csv.DictReader(f):
            home, away = row["home_team"], row["away_team"]
            home_conf, away_conf = row.get("home_conference", ""), row.get("away_conference", "")
            if home and home_conf:
                conf_votes[home][home_conf] += 1
            if away and away_conf:
                conf_votes[away][away_conf] += 1

    result = {}
    ambiguous = []
    for team, counter in conf_votes.items():
        ranked = counter.most_common()
        top_conf, top_count = ranked[0]
        if len(ranked) > 1 and ranked[1][1] == top_count:
            ambiguous.append((team, ranked))
        result[team] = top_conf

    return result, ambiguous


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--years", type=int, nargs=2, metavar=("START", "END"), required=True)
    p.add_argument("--out-dir", default="data/raw")
    args = p.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    for year in range(args.years[0], args.years[1] + 1):
        result, ambiguous = derive_for_year(year)
        if not result:
            print(f"{year}: no games_{year}.csv found, skipped")
            continue

        out_path = out_dir / f"membership_{year}.csv"
        with out_path.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["team_name", "conference_real", "is_fbs"])
            for team in sorted(result):
                w.writerow([team, result[team], 1])

        print(f"{year}: wrote {len(result)} teams to {out_path}"
              + (f"  ({len(ambiguous)} AMBIGUOUS)" if ambiguous else ""))
        for team, ranked in ambiguous:
            print(f"    AMBIGUOUS: {team} -- {ranked}")


if __name__ == "__main__":
    main()
