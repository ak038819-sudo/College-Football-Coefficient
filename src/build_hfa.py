#!/usr/bin/env python3
"""
Builds team-specific home-field advantage tables (see src/hfa.py for the model).
ANALYSIS ONLY -- production Elo (build_elo.py) is untouched and keeps its flat
config "elo.home_field".

Outputs (database + CSV mirrors in data/processed/):
  team_hfa_by_season  one row per team per season, "entering season": uses only
                      games dated before that season's first game -- the same
                      frozen, point-in-time convention as the frozen 5yr CoE.
  team_hfa_current    the latest estimate for teams active in the latest season,
                      as of the day after the last completed game (not the wall
                      clock, so identical inputs always give identical output).

Requires elo_game_history (run src/build_elo.py first).

Usage:
    python src/build_hfa.py                          # build both tables
    python src/build_hfa.py --diagnose BYU           # per-game audit for one team (aliases accepted)
    python src/build_hfa.py --diagnose BYU --as-of 2015-08-29
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from hfa import (calculate_team_hfa, convert_hfa_to_elo_points, calculate_hfa_prior,  # noqa: E402
                 calculate_raw_hfa, generate_hfa_diagnostics, load_home_games, weighted_sums)
from load_games import resolve_team_name, team_id as canonical_team_id  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
SCHEMA = REPO / "sql" / "hfa_tables.sql"
CONFIG = REPO / "config" / "model_config.json"

COLUMNS = ["raw_hfa", "adjusted_hfa", "prior_hfa", "fbs_baseline", "fcs_prior_used", "effective_n", "lambda",
           "games_used", "weighted_actual_wins", "weighted_expected_wins", "oldest_game_used", "newest_game_used",
           "elo_hfa_points", "elo_points_at_bound", "raw_elo_hfa_points", "raw_points_at_bound"]


def load_config(path: Path = CONFIG) -> dict:
    cfg = json.loads(path.read_text(encoding="utf-8"))
    return {"elo": cfg["elo"], "hfa": cfg["hfa"]}


def season_starts(conn: sqlite3.Connection) -> dict:
    return {s: dt.date.fromisoformat(str(d)[:10]) for s, d in conn.execute(
        "SELECT season_year, MIN(game_date) FROM games WHERE home_score IS NOT NULL GROUP BY season_year")}


def active_teams(conn: sqlite3.Connection, season: int) -> list:
    rows = conn.execute("""SELECT home_team_id FROM games WHERE season_year = ? AND home_score IS NOT NULL
                           UNION SELECT away_team_id FROM games WHERE season_year = ? AND home_score IS NOT NULL""",
                        (season, season)).fetchall()
    ids = {r[0] for r in rows}
    if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='scheduled_games'").fetchone():
        ids |= {r[0] for r in conn.execute("""SELECT home_team_id FROM scheduled_games WHERE season_year = ?
                                              UNION SELECT away_team_id FROM scheduled_games WHERE season_year = ?""",
                                           (season, season))}
    return sorted(ids)


def build(conn: sqlite3.Connection, cfg: dict) -> tuple[list, list]:
    """Returns (by_season_rows, current_rows) as dicts. Pure with respect to the database contents."""
    hcfg, scale = cfg["hfa"], cfg["elo"]["scale"]
    games, _skipped = load_home_games(conn, scale)
    by_team = defaultdict(list)
    for g in games:
        by_team[g.team_id].append(g)

    def estimates_for(season, teams, as_of):
        nat = weighted_sums(games, as_of, hcfg["half_life_years"])
        if calculate_raw_hfa(nat) is None:
            return []                                    # e.g. the first season: nothing before it
        prior = calculate_hfa_prior(calculate_raw_hfa(nat), None, hcfg["fcs_prior_weight"])
        nat_points, _ = convert_hfa_to_elo_points(games, as_of, hcfg["half_life_years"], prior * nat.sum_wp, scale,
                                                  tuple(hcfg["point_bounds"]))
        out = []
        for tid in teams:
            est = calculate_team_hfa(by_team.get(tid, []), games, as_of, hcfg, scale, national_points=nat_points)
            out.append({"team_id": tid, "season_year": season, **est})
        return out

    starts = season_starts(conn)
    by_season = []
    for season in sorted(starts):
        by_season += estimates_for(season, active_teams(conn, season), starts[season])

    last_game = conn.execute("SELECT MAX(game_date) FROM games WHERE home_score IS NOT NULL").fetchone()[0]
    latest = max(starts)
    as_of_now = dt.date.fromisoformat(str(last_game)[:10]) + dt.timedelta(days=1)
    current = estimates_for(latest, active_teams(conn, latest), as_of_now)
    current.sort(key=lambda r: (-r["adjusted_hfa"], r["team_id"]))
    return by_season, current


def write(conn: sqlite3.Connection, cfg: dict, by_season: list, current: list, out_dir: Path) -> None:
    conn.executescript(SCHEMA.read_text(encoding="utf-8"))
    params = (cfg["hfa"]["half_life_years"], cfg["hfa"]["shrinkage_k"], cfg["hfa"]["effective_n_method"])
    conn.execute("DELETE FROM team_hfa_by_season")
    conn.execute("DELETE FROM team_hfa_current")
    for table, rows in (("team_hfa_by_season", by_season), ("team_hfa_current", current)):
        cols = ["team_id", "season_year", "as_of"] + COLUMNS
        conn.executemany(
            f"INSERT INTO {table} ({', '.join(cols)}, half_life_years, shrinkage_k, effective_n_method) "
            f"VALUES ({', '.join('?' for _ in cols)}, ?, ?, ?)",
            [[r[c] for c in cols] + list(params) for r in rows])
    conn.commit()
    names = dict(conn.execute("SELECT team_id, team_name FROM teams"))
    out_dir.mkdir(parents=True, exist_ok=True)
    for fname, rows in (("team_hfa_by_season.csv", by_season), ("team_hfa_current.csv", current)):
        with (out_dir / fname).open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["team", "season_year", "as_of"] + COLUMNS)
            for r in rows:
                w.writerow([names.get(r["team_id"], r["team_id"]), r["season_year"], r["as_of"]] +
                           [round(r[c], 6) if isinstance(r[c], float) else r[c] for c in COLUMNS])


def resolve_team(conn: sqlite3.Connection, name: str) -> int:
    """Canonical team_id for a name or alias, via the same lookup load_games.py uses."""
    cur = conn.cursor()
    return canonical_team_id(cur, resolve_team_name(cur, name))


def diagnose(conn: sqlite3.Connection, cfg: dict, team: str, as_of: dt.date | None) -> None:
    tid = resolve_team(conn, team)
    hcfg, scale = cfg["hfa"], cfg["elo"]["scale"]
    games, skipped = load_home_games(conn, scale)
    if as_of is None:
        last = conn.execute("SELECT MAX(game_date) FROM games WHERE home_score IS NOT NULL").fetchone()[0]
        as_of = dt.date.fromisoformat(str(last)[:10]) + dt.timedelta(days=1)
    names = dict(conn.execute("SELECT team_id, team_name FROM teams"))
    team_games = [g for g in games if g.team_id == tid]
    rows = generate_hfa_diagnostics(team_games, as_of, hcfg["half_life_years"], names)
    est = calculate_team_hfa(team_games, games, as_of, hcfg, scale)
    path = REPO / "data" / "processed" / f"hfa_diagnostics_{names[tid].lower().replace(' ', '_').replace('(', '').replace(')', '')}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        if rows:
            w = csv.DictWriter(f, fieldnames=list(rows[0]))
            w.writeheader()
            w.writerows(rows)
    print(f"{names[tid]} as of {as_of} (L={hcfg['half_life_years']}, K={hcfg['shrinkage_k']}): "
          f"{len(rows)} qualifying home games -> {path}")
    for r in rows[-5:]:
        print("  ", r)
    for k in ("raw_hfa", "adjusted_hfa", "prior_hfa", "fbs_baseline", "effective_n", "lambda",
              "weighted_actual_wins", "weighted_expected_wins", "elo_hfa_points", "raw_elo_hfa_points"):
        v = est[k]
        print(f"  {k:24s} {v:.4f}" if isinstance(v, float) else f"  {k:24s} {v}")
    print(f"  excluded from ALL estimates: {skipped}")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default=str(REPO / "db" / "league.db"))
    p.add_argument("--diagnose", metavar="TEAM")
    p.add_argument("--as-of", type=dt.date.fromisoformat)
    args = p.parse_args()
    cfg = load_config()
    conn = sqlite3.connect(args.db)
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE name='elo_game_history'").fetchone():
        raise SystemExit("elo_game_history is missing: run src/build_elo.py first.")
    if args.diagnose:
        diagnose(conn, cfg, args.diagnose, args.as_of)
        return
    by_season, current = build(conn, cfg)
    write(conn, cfg, by_season, current, REPO / "data" / "processed")
    print(f"team_hfa_by_season: {len(by_season)} rows; team_hfa_current: {len(current)} teams "
          f"(L={cfg['hfa']['half_life_years']}, K={cfg['hfa']['shrinkage_k']}, "
          f"N_eff={cfg['hfa']['effective_n_method']})")
    conn.close()


if __name__ == "__main__":
    main()
