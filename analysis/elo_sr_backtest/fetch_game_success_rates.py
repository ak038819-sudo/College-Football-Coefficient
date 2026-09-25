#!/usr/bin/env python3
"""
EXPERIMENT (Success Rate vs MOV Elo backtest) -- step 1: per-game Success Rate.

Downloads CFBD's /stats/game/advanced (one row per team per game) for every
season in a range, caches a COMPACT copy locally (only the fields the backtest
uses), then builds:

  game_success_rates.csv  one row per FBS-vs-FBS game in db/league.db, with both
                          teams' offensive Success Rate and play counts, for two
                          variants: garbage time excluded ("nogt", primary --
                          matches the team pages) and all plays ("all").
  cfbd_sr_coverage.csv    per season: eligible games, games with SR for BOTH
                          teams, missing, coverage %; plus the earliest seasons
                          with any / >=90% / >=95% / >=99% coverage.

Matching uses CFBD's game id, which is the same id as games.game_id, so no
name matching is needed to find a game; team names go through the project's
alias table only to decide which row is the home team.

Re-running reuses the cache: no API calls for seasons already downloaded
(use --refresh to force). Production code and data are not touched.

Usage:
    python analysis/elo_sr_backtest/fetch_game_success_rates.py              # 1998..current season
    python analysis/elo_sr_backtest/fetch_game_success_rates.py 2001 2026
"""
from __future__ import annotations

import csv
import os
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
sys.path.insert(0, str(REPO / "src"))
from current_season import season_for  # noqa: E402
from load_games import resolve_team_name, team_id as canonical_team_id  # noqa: E402

BASE = "https://api.collegefootballdata.com"
CACHE = HERE / "cache"
VARIANTS = {"nogt": "true", "all": "false"}          # excludeGarbageTime
CACHE_FIELDS = ["game_id", "season", "season_type", "week", "team", "opponent",
                "off_plays", "off_success_rate", "off_ppa"]
FIRST_PROBE = 1998


def cache_path(year: int, season_type: str, variant: str) -> Path:
    return CACHE / f"game_adv_{year}_{season_type}_{variant}.csv"


def fetch_season(year: int, season_type: str, variant: str, headers: dict) -> list[dict]:
    import requests   # only needed when actually downloading
    r = requests.get(f"{BASE}/stats/game/advanced",
                     params={"year": year, "seasonType": season_type, "excludeGarbageTime": VARIANTS[variant]},
                     headers=headers, timeout=120)
    r.raise_for_status()
    rows = []
    for t in r.json() or []:
        off = t.get("offense") or {}
        rows.append({"game_id": t.get("gameId") or t.get("game_id"), "season": t.get("season", year),
                     "season_type": t.get("seasonType") or season_type, "week": t.get("week"),
                     "team": t.get("team"), "opponent": t.get("opponent"),
                     "off_plays": off.get("plays"), "off_success_rate": off.get("successRate"),
                     "off_ppa": off.get("ppa")})
    return rows


def cached_rows(year, season_type, variant, headers, refresh) -> list[dict]:
    path = cache_path(year, season_type, variant)
    if path.exists() and not refresh:
        with path.open(newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    rows = fetch_season(year, season_type, variant, headers)
    CACHE.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CACHE_FIELDS)
        w.writeheader()
        w.writerows([{k: ("" if r[k] is None else r[k]) for k in CACHE_FIELDS} for r in rows])
    return [{k: ("" if r[k] is None else str(r[k])) for k in CACHE_FIELDS} for r in rows]


def _num(v):
    return None if v is None or str(v).strip() == "" else float(v)


def detect_scale(values: list[float]) -> float:
    """1.0 if Success Rate arrives as a decimal (0.62), 100.0 if as a percentage (62)."""
    vals = [v for v in values if v is not None]
    return 100.0 if vals and max(vals) > 1.0 else 1.0


def build(conn: sqlite3.Connection, per_variant: dict, years: range) -> tuple[list[dict], list[dict], dict]:
    cur = conn.cursor()
    games = conn.execute("""
        SELECT g.game_id, g.season_year, g.week, g.game_date, th.team_name, ta.team_name, g.home_team_id,
               g.away_team_id, g.neutral_site, g.home_score, g.away_score, g.game_phase
        FROM games g JOIN teams th ON th.team_id = g.home_team_id JOIN teams ta ON ta.team_id = g.away_team_id
        WHERE g.home_score IS NOT NULL AND g.season_year BETWEEN ? AND ?
        ORDER BY g.game_date, g.game_id""", (years.start, years.stop - 1)).fetchall()
    scales = {v: detect_scale([_num(r["off_success_rate"]) for r in rows]) for v, rows in per_variant.items()}

    resolved: dict = {}
    def tid(name):
        if name not in resolved:
            try:
                resolved[name] = canonical_team_id(cur, resolve_team_name(cur, name))
            except ValueError:
                resolved[name] = None
        return resolved[name]

    index = {v: defaultdict(dict) for v in per_variant}
    for v, rows in per_variant.items():
        for r in rows:
            if r["game_id"] and tid(r["team"]) is not None:
                index[v][int(float(r["game_id"]))][tid(r["team"])] = r

    out, cov = [], defaultdict(lambda: defaultdict(int))
    for gid, season, week, date, hname, aname, hid, aid, neutral, hs, as_, phase in games:
        row = {"game_id": gid, "season": season, "week": week, "date": date, "home_team": hname, "away_team": aname,
               "neutral_site": neutral, "game_phase": phase, "home_score": hs, "away_score": as_}
        cov[season]["eligible"] += 1
        for v in per_variant:
            h, a = index[v].get(gid, {}).get(hid), index[v].get(gid, {}).get(aid)
            hsr = _num(h["off_success_rate"]) / scales[v] if h and _num(h["off_success_rate"]) is not None else None
            asr = _num(a["off_success_rate"]) / scales[v] if a and _num(a["off_success_rate"]) is not None else None
            row[f"home_success_rate_{v}"] = hsr
            row[f"away_success_rate_{v}"] = asr
            row[f"home_plays_{v}"] = _num(h["off_plays"]) if h else None
            row[f"away_plays_{v}"] = _num(a["off_plays"]) if a else None
            both = hsr is not None and asr is not None
            row[f"home_sr_diff_{v}"] = hsr - asr if both else None
            row[f"away_sr_diff_{v}"] = asr - hsr if both else None
            cov[season][f"any_{v}"] += int(hsr is not None or asr is not None)
            cov[season][f"both_{v}"] += int(both)
        out.append(row)

    coverage = []
    for season in sorted(cov):
        c = cov[season]
        rec = {"season": season, "eligible_fbs_games": c["eligible"]}
        for v in per_variant:
            rec[f"both_sr_{v}"] = c[f"both_{v}"]
            rec[f"missing_{v}"] = c["eligible"] - c[f"both_{v}"]
            rec[f"coverage_pct_{v}"] = round(100.0 * c[f"both_{v}"] / c["eligible"], 2) if c["eligible"] else 0.0
        coverage.append(rec)
    return out, coverage, scales


def thresholds(coverage: list[dict], variant: str) -> dict:
    """Earliest season from which coverage STAYS at/above each level through the latest season."""
    res = {}
    seasons = [c["season"] for c in coverage]
    any_seasons = [c["season"] for c in coverage if c[f"both_sr_{variant}"] > 0]
    res["earliest_any"] = min(any_seasons) if any_seasons else None
    for level in (90, 95, 99):
        start = None
        for c in reversed(coverage):
            if c[f"coverage_pct_{variant}"] >= level:
                start = c["season"]
            else:
                break
        res[f"earliest_sustained_{level}"] = start
    return res


def main(argv: list[str]) -> int:
    years = [int(a) for a in argv if a.isdigit()]
    start = years[0] if years else FIRST_PROBE
    end = years[1] if len(years) > 1 else season_for(__import__("datetime").date.today())
    refresh = "--refresh" in argv
    api_key = os.getenv("CFBD_API_KEY") or os.getenv("COLLEGEFOOTBALLDATA_API_KEY")
    need_api = refresh or any(not cache_path(y, st, v).exists()
                              for y in range(start, end + 1) for st in ("regular", "postseason") for v in VARIANTS)
    if need_api and not api_key:
        print("ERROR: set CFBD_API_KEY (some seasons aren't cached yet).")
        return 2
    headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}

    per_variant = {v: [] for v in VARIANTS}
    for year in range(start, end + 1):
        for season_type in ("regular", "postseason"):
            for v in VARIANTS:
                was_cached = cache_path(year, season_type, v).exists() and not refresh
                rows = cached_rows(year, season_type, v, headers, refresh)
                per_variant[v].extend(rows)
                print(f"{year} {season_type:10s} {v:4s}: {len(rows):5d} team-game rows{' (cached)' if was_cached else ''}")

    conn = sqlite3.connect(str(REPO / "db" / "league.db"))
    games, coverage, scales = build(conn, per_variant, range(start, end + 1))
    conn.close()

    for name, rows in (("game_success_rates.csv", games), ("cfbd_sr_coverage.csv", coverage)):
        with (HERE / name).open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0]))
            w.writeheader()
            w.writerows(rows)

    print(f"\nSuccess Rate units: " + ", ".join(f"{v}: {'percent (divided by 100)' if s == 100 else 'decimal'}" for v, s in scales.items()))
    for v in VARIANTS:
        print(f"coverage thresholds ({v}): {thresholds(coverage, v)}")
    check = next((g for g in games if g["home_team"] == "Colorado State" and g["away_team"] == "BYU" and g["season"] == 2026), None)
    if check and check["away_sr_diff_nogt"] is not None:
        d = check["away_sr_diff_nogt"]
        print(f"sanity: BYU SR diff at Colorado State 2026 = {d:+.3f} (a decimal like 0.17, never 17)")
        if abs(d) > 1:
            print("ERROR: Success Rate difference outside [-1, 1]; units are wrong.")
            return 1
    print(f"\nWrote {HERE / 'game_success_rates.csv'} and {HERE / 'cfbd_sr_coverage.csv'}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
