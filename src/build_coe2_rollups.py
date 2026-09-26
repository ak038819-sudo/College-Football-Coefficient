#!/usr/bin/env python3
"""
CoE 2.0 season rollups and the conference five-season aggregate (ENG-13, ENG-14).

Game CoE 2.0 already values each game (build_hybrid_coefficients.py). What was
missing is the level above it: a season total that can be decomposed, explicit
postseason bonuses that are visible rather than buried, and a conference
five-season value that bid allocation could consume.

    C_{i,Y} = sum(Game CoE 2.0 awards in Y) + B_{i,Y}

B is the sum of explicitly recorded bonuses, each one a row in
team_coe2_bonuses naming its category, how many times it applied and why. The
guide's requirement is that bonus provenance is visible and that a bonus can
never hide inside opponent strength -- so bonuses are computed HERE, after the
game awards, from real results only, and never re-enter the hybrid rating that
valued those games.

BONUS VALUES DEFAULT TO ZERO. The categories and the machinery are implemented
and tested; the point values live in config/model_config.json's "coe2_bonuses"
section and ship at 0.0, which makes a season total exactly the sum of its game
awards and keeps the current model reproducible. Choosing magnitudes is a
modelling decision to be made with the validation harness, not something this
script should invent -- the same stance taken for the xSRDiff beta.

Anti-circularity. The conference five-season value covers seasons Y-5..Y-1 only
-- the value ENTERING Y, frozen, mirroring team_coe_5yr_by_season. CoE v1's
conference_coefficient_rolling_5yr deliberately includes the current season
because it feeds current-season bid allocation; the two live in separate tables
with different names so they cannot be confused.

Usage:
    python src/build_coe2_rollups.py [--db db/league.db]
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent / "coefficients"))
from build_conference_coe2 import is_external_game  # noqa: E402
from export_team_pages import CFP_FIRST_SEASON, load_national_champions  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
SCHEMA = REPO / "sql" / "coe2_rollup_tables.sql"
TEAM_OUT = REPO / "data" / "processed" / "team_coe2_by_season.csv"
CONF_OUT = REPO / "data" / "processed" / "conference_coe2_5yr.csv"
DECAY_BASE = 0.92          # matches CoE v1's WITHIN_WINDOW_DECAY_BASE, intentionally
ROLLING_YEARS = 5

# Every bonus category, with the question it answers. Values come from config;
# these are the rules, not the magnitudes.
BONUS_CATEGORIES = [
    ("bowl_appearance", "Played in a bowl game"),
    ("bowl_win", "Won a bowl game"),
    ("cfp_appearance", "Played in a College Football Playoff game"),
    ("cfp_win", "Won a College Football Playoff game"),
    # NOT the conference championship game: the derived standings order the
     # playoff model uses (regular-season conference record with its tiebreak
     # heuristic), which only covers the seasons that model needs. Georgia went
     # 8-0 in SEC play in 2021 and leads these standings; Alabama won the title
     # game. Naming it for what it measures keeps the two apart.
    ("conference_standings_first", "Finished first in the derived conference standings"),
    ("national_title", "Awarded a national championship"),
]


def load_bonus_config(cfg: dict) -> dict:
    """
    Validate config/model_config.json's "coe2_bonuses" section.

    An unknown category is an error rather than a silent no-op: a typo that
    quietly awarded nothing would be indistinguishable from a deliberate zero.
    """
    section = dict(cfg or {})
    version = section.pop("version", "coe2_bonus_v1")
    section.pop("_comment", None)
    known = {k for k, _ in BONUS_CATEGORIES}
    unknown = sorted(set(section) - known)
    if unknown:
        raise ValueError(f"coe2_bonuses has unknown categories {unknown}; known: {sorted(known)}")
    values = {k: float(section.get(k, 0.0)) for k in known}
    return {"version": version, "values": values}


def game_awards(conn: sqlite3.Connection) -> tuple[dict, dict]:
    """{(team_id, season): summed Game CoE 2.0}, {(team_id, season): games counted}."""
    total: dict = defaultdict(float)
    count: dict = defaultdict(int)
    for team_id, season, coe in conn.execute(
            """SELECT h.team_id, g.season_year, h.game_coe
               FROM hybrid_game_ratings h JOIN games g ON g.game_id = h.game_id
               WHERE h.game_coe IS NOT NULL"""):
        total[(team_id, season)] += coe
        count[(team_id, season)] += 1
    return dict(total), dict(count)


def postseason_counts(conn: sqlite3.Connection) -> dict:
    """
    {(team_id, season): {category: count}} for the results-derived bonuses.

    Pre-2014 games with a "cfp" phase are BCS-era title bowls, counted as bowls,
    exactly as the team page and the conference export already count them -- the
    College Football Playoff began with the 2014 season.
    """
    out: dict = defaultdict(lambda: defaultdict(int))
    for team_id, season, phase, won in conn.execute(
            """SELECT e.team_id, g.season_year, g.game_phase,
                      CASE WHEN (e.team_id = g.home_team_id AND g.home_score > g.away_score)
                             OR (e.team_id = g.away_team_id AND g.away_score > g.home_score)
                           THEN 1 ELSE 0 END
               FROM elo_game_history e JOIN games g ON g.game_id = e.game_id
               WHERE g.home_score IS NOT NULL AND g.away_score IS NOT NULL
                 AND g.game_phase IN ('bowl', 'cfp')"""):
        is_cfp = phase == "cfp" and season >= CFP_FIRST_SEASON
        out[(team_id, season)]["cfp_appearance" if is_cfp else "bowl_appearance"] += 1
        if won:
            out[(team_id, season)]["cfp_win" if is_cfp else "bowl_win"] += 1
    return {k: dict(v) for k, v in out.items()}


def title_counts(conn: sqlite3.Connection, champions: list[dict] | None) -> dict:
    """
    {(team_id, season): {category: count}} for conference and national titles.

    Conference titles come from the derived standings, which only cover the
    seasons the playoff model needs; a season without standings data simply has
    no conference-title bonus rather than a guessed one. National titles come
    from the hand-maintained reference list and are never inferred from results.
    """
    out: dict = defaultdict(lambda: defaultdict(int))
    if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                    "AND name='conference_standings_by_year'").fetchone():
        for season, team_id in conn.execute(
                "SELECT season_year, team_id FROM conference_standings_by_year WHERE conf_rank = 1"):
            out[(team_id, season)]["conference_standings_first"] += 1
    # One title per season, however many polls awarded it. The reference file
    # carries a row per SYSTEM, so a unanimous champion has an AP row and a
    # Coaches row for a single championship -- counting rows would pay it twice.
    titled = {(c["team_id"], c["season"]) for c in (champions or []) if c["status"] == "awarded"}
    for team_id, season in titled:
        out[(team_id, season)]["national_title"] += 1
    return {k: dict(v) for k, v in out.items()}


def build_team_rollups(conn: sqlite3.Connection, bonus_cfg: dict,
                       champions: list[dict] | None = None) -> tuple[list, list]:
    """
    (season rows, bonus rows). Season rows always satisfy
    season_coe2 == game_coe_total + bonus_total, which is the invariant the
    whole table exists to make checkable.
    """
    totals, counts = game_awards(conn)
    earned: dict = defaultdict(dict)
    for key, cats in postseason_counts(conn).items():
        earned[key].update(cats)
    for key, cats in title_counts(conn, champions).items():
        earned[key].update(cats)

    version, values = bonus_cfg["version"], bonus_cfg["values"]
    labels = dict(BONUS_CATEGORIES)
    season_rows, bonus_rows = [], []
    # A team-season with bonuses but no game awards still gets a row: its total is
    # then just the bonus, which is a truthful statement, not a missing season.
    for key in sorted(set(totals) | set(earned)):
        team_id, season = key
        game_total = totals.get(key, 0.0)
        bonus_total = 0.0
        for category, n in sorted(earned.get(key, {}).items()):
            points = values[category] * n
            bonus_total += points
            bonus_rows.append((team_id, season, category, n, round(points, 6),
                               labels[category] + (f" x{n}" if n > 1 else ""), version))
        season_rows.append((team_id, season, round(game_total, 6), round(bonus_total, 6),
                            round(game_total + bonus_total, 6), counts.get(key, 0), version))
    return season_rows, bonus_rows


def conference_season_coe2(conn: sqlite3.Connection) -> dict:
    """
    {(conference, season): (coe2, external_games)} recomputed from game awards
    using the canonical external-game set, so the five-season window is built
    from contributions that can be traced back to individual games rather than
    from a pre-aggregated number.
    """
    membership = {(tid, season): conf for tid, season, conf in conn.execute(
        "SELECT team_id, season_year, conference_real FROM team_membership_by_season")}
    coe: dict = defaultdict(float)
    games: dict = defaultdict(int)
    for team_id, coe_val, season, phase, home, away in conn.execute(
            """SELECT h.team_id, h.game_coe, g.season_year, g.game_phase, g.home_team_id, g.away_team_id
               FROM hybrid_game_ratings h JOIN games g ON g.game_id = h.game_id
               WHERE h.game_coe IS NOT NULL"""):
        own = membership.get((team_id, season))
        if own is None:
            continue                       # no conference that season: nothing to credit
        opp = membership.get((away if team_id == home else home, season))
        if is_external_game(own, opp, phase):
            coe[(own, season)] += coe_val
            games[(own, season)] += 1
    return {k: (v, games[k]) for k, v in coe.items()}


def build_conference_5yr(per_season: dict, version: str,
                         decay_base: float = DECAY_BASE, rolling_years: int = ROLLING_YEARS) -> list:
    """
    The value ENTERING each season: seasons Y-5..Y-1 only, decay-weighted with the
    freshest prior season heaviest and anchored at Y-1 -- the same shape
    build_hybrid_coefficients.compute_frozen_5yr_coe uses for teams, so the two
    frozen windows cannot drift apart.

    A conference with no contribution in the whole window gets no row rather than
    a zero, so "never existed then" stays distinguishable from "existed and earned
    nothing".
    """
    seasons = sorted({s for _, s in per_season})
    if not seasons:
        return []
    rows = []
    for y in range(min(seasons) + 1, max(seasons) + 2):
        window = [wy for wy in range(y - rolling_years, y) if wy in seasons]
        if not window:
            continue
        anchor = y - 1
        confs = {c for (c, s) in per_season if s in window}
        for conf in sorted(confs):
            total, ext, counted = 0.0, 0, 0
            for wy in window:
                hit = per_season.get((conf, wy))
                if not hit:
                    continue
                total += hit[0] * (decay_base ** (anchor - wy))
                ext += hit[1]
                counted += 1
            if counted:
                rows.append((y, conf, round(total, 6), window[0], anchor, counted, ext, version))
    return rows


def conference_coe2_rank(conn: sqlite3.Connection, season_year: int,
                         version: str | None = None) -> list[tuple[str, float]]:
    """
    THE canonical CoE 2.0 conference ordering: [(conference, coe2_5yr), ...]
    strongest first.

    Deliberately the same shape and the same exclusions as CoE v1's
    select_playoff_field_v2.load_conference_coe_rank -- 'FBS Independents' is not
    a league and is not bid-eligible, and a conference with no actual members in
    season_year cannot hold a rank in it however strong its trailing window looks.
    Bid allocation can therefore consume either model through one contract
    without knowing which it got. Nothing here changes which one it currently
    uses: the live playoff model still runs on v1.
    """
    real = {r[0] for r in conn.execute(
        "SELECT DISTINCT conference FROM conference_standings_by_year WHERE season_year = ?", (season_year,))}
    sql = ("SELECT conference, coe2_5yr FROM conference_coe2_5yr_by_season WHERE season_year = ?"
           + (" AND formula_version = ?" if version else ""))
    params = (season_year, version) if version else (season_year,)
    rows = [(c, v) for c, v in conn.execute(sql, params)
            if c != "FBS Independents" and (not real or c in real)]
    return sorted(rows, key=lambda r: (-r[1], r[0]))


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default=str(REPO / "db" / "league.db"))
    p.add_argument("--config", default=str(REPO / "config" / "model_config.json"))
    args = p.parse_args()

    cfg = json.loads(Path(args.config).read_text(encoding="utf-8"))
    bonus_cfg = load_bonus_config(cfg.get("coe2_bonuses"))

    conn = sqlite3.connect(args.db)
    conn.executescript(SCHEMA.read_text(encoding="utf-8"))
    for table in ("team_coe2_by_season", "team_coe2_bonuses", "conference_coe2_5yr_by_season"):
        conn.execute(f"DELETE FROM {table} WHERE formula_version = ?", (bonus_cfg["version"],))

    champions = load_national_champions(conn)
    season_rows, bonus_rows = build_team_rollups(conn, bonus_cfg, champions)
    conn.executemany("INSERT INTO team_coe2_by_season (team_id, season_year, game_coe_total, bonus_total,"
                     " season_coe2, games_counted, formula_version) VALUES (?,?,?,?,?,?,?)", season_rows)
    conn.executemany("INSERT INTO team_coe2_bonuses (team_id, season_year, category, count, points, detail,"
                     " formula_version) VALUES (?,?,?,?,?,?,?)", bonus_rows)

    per_season = conference_season_coe2(conn)
    conf_rows = build_conference_5yr(per_season, bonus_cfg["version"])
    conn.executemany("INSERT INTO conference_coe2_5yr_by_season (season_year, conference, coe2_5yr,"
                     " window_start_year, window_end_year, seasons_counted, external_games, formula_version)"
                     " VALUES (?,?,?,?,?,?,?,?)", conf_rows)
    conn.commit()

    TEAM_OUT.parent.mkdir(parents=True, exist_ok=True)
    with TEAM_OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["team_id", "season_year", "game_coe_total", "bonus_total", "season_coe2",
                    "games_counted", "formula_version"])
        w.writerows(sorted(season_rows, key=lambda r: (r[1], -r[4])))
    with CONF_OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["season_year", "conference", "coe2_5yr", "window_start_year", "window_end_year",
                    "seasons_counted", "external_games", "formula_version"])
        w.writerows(sorted(conf_rows, key=lambda r: (r[0], -r[2])))

    awarded = sum(r[4] for r in bonus_rows)
    print(f"Team season rollups: {len(season_rows):,} team-seasons ({TEAM_OUT})")
    print(f"Bonuses: {len(bonus_rows):,} rows across {len({r[2] for r in bonus_rows})} categories, "
          f"{awarded:,.3f} points awarded")
    if awarded == 0:
        print("  (every coe2_bonuses value is 0.0, so a season total is exactly its game awards --"
              " set them in config/model_config.json once the magnitudes are chosen)")
    print(f"Conference five-season (entering) values: {len(conf_rows):,} rows ({CONF_OUT})")
    latest = max((r[0] for r in conf_rows), default=None)
    if latest:
        ranked = conference_coe2_rank(conn, latest, bonus_cfg["version"])
        print(f"\nCoE 2.0 conference order entering {latest} (bid-allocation input shape; "
              f"the live model still uses v1):")
        for i, (conf, val) in enumerate(ranked[:10], start=1):
            print(f"  {i:>2}. {conf:<20} {val:.3f}")
    conn.close()


if __name__ == "__main__":
    main()
