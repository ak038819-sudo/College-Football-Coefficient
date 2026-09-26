#!/usr/bin/env python3
"""
Team-page data export (Milestone 2): writes ui/data/team_pages.js, loaded by
the dashboard only when a team page is first opened.

Why a .js file rather than .json: the dashboard is often opened straight
from disk (file://), where browsers block fetch() of local JSON. A classic
<script src> still works from disk AND from GitHub Pages, so the data is
wrapped as `window.__TEAM_PAGES__ = {...};`.

Every number the team page shows is computed HERE, deterministically, from
the authoritative tables -- the browser only filters/sorts for display and
never recalculates Elo:
  - elo_game_history  (per team per game: pregame, opponent pregame,
                       pregame win expectation, change, postgame)
  - games             (completed games only: both scores present)
  - hybrid_game_ratings (Game CoE 2.0, where available)

Shared, normalized layout (no game duplicated per team):
  games:        [[game_id, season, date, week, home_id, away_id, home_score,
                  away_score, neutral, phase(0=regular,1=bowl,2=cfp), went_ot], ...]
  elo:          [[game_id, team_id, pregame, opp_pregame, expectation,
                  change, postgame], ...]   grouped by team, chronological
  team_seasons: [[team_id, season, games, w, l, t, start_elo, end_elo,
                  elo_change, sos, sos_rank, expected_wins, actual_wins,
                  wins_above_expected, coe2_season, coe2_rank,
                  conference], ...]
  swings:       {team_id: {"gains": [game_id...], "losses": [game_id...]}}
  history:      {team_id: {...REAL postseason history only...}}

Model (simulated) playoff history is deliberately NOT in this file; it
already lives in dashboard_data.json and is displayed under a separate,
labeled heading so the two can never be mixed.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from pathlib import Path
import sqlite3

sys.path.insert(0, str(Path(__file__).resolve().parent))
from load_games import resolve_team_name, team_id as canonical_team_id  # noqa: E402

CHAMPIONS_PATH = Path(__file__).resolve().parent.parent / "data" / "reference" / "national_champions.csv"
SYSTEMS = {"cfp": "championship_game", "bcs": "championship_game", "ap": "final_poll", "coaches": "final_poll"}

PHASE_CODE = {"regular": 0, "bowl": 1, "cfp": 2}
CFP_FIRST_SEASON = 2014   # 4-team CFP began with the 2014 season
SWING_COUNT = 10


def load_national_champions(conn: sqlite3.Connection, path: Path = CHAMPIONS_PATH) -> list[dict]:
    """
    Reads the hand-maintained data/reference/national_champions.csv (Milestone 6)
    and validates it, failing loudly rather than silently dropping a bad row:
    team must resolve through the canonical/alias lookup, system and title_type
    must agree, CFP rows only from 2014, BCS rows only 1998-2013.
    Titles are NEVER inferred from game results -- see data/reference/README.md.
    """
    cur = conn.cursor()
    out = []
    with open(path, newline="", encoding="utf-8") as f:
        for n, r in enumerate(csv.DictReader(f), start=2):
            where = f"{path.name} line {n}"
            season, system = int(r["season_year"]), r["system"].strip()
            if SYSTEMS.get(system) != r["title_type"].strip():
                raise ValueError(f"{where}: system {system!r} / title_type {r['title_type']!r} mismatch")
            if r["status"].strip() not in ("awarded", "vacated"):
                raise ValueError(f"{where}: status must be awarded or vacated")
            if system == "cfp" and season < CFP_FIRST_SEASON or system == "bcs" and not 1998 <= season <= 2013:
                raise ValueError(f"{where}: {system} title outside its era ({season})")
            try:
                tid = canonical_team_id(cur, resolve_team_name(cur, r["team_name"]))
            except ValueError as e:
                raise ValueError(f"{where}: {e}") from None
            out.append({"season": season, "team_id": tid, "system": system,
                        "status": r["status"].strip(), "notes": r["notes"].strip()})
    return out


def title_history(champions: list[dict]) -> dict:
    """
    {team_id: {"titles": [[season, system, status, shared, notes], ...] newest first,
               "title_count": distinct seasons with an AWARDED row}}.
    A season is "shared" when more than one team holds an awarded title in it.
    """
    holders: dict = defaultdict(set)
    for c in champions:
        if c["status"] == "awarded":
            holders[c["season"]].add(c["team_id"])
    by_team: dict = defaultdict(lambda: {"titles": [], "title_count": 0})
    order = {"cfp": 0, "bcs": 1, "ap": 2, "coaches": 3}
    for c in sorted(champions, key=lambda c: (-c["season"], order[c["system"]])):
        by_team[c["team_id"]]["titles"].append(
            [c["season"], c["system"], c["status"], len(holders[c["season"]]) > 1, c["notes"]])
    for tid, rec in by_team.items():
        rec["title_count"] = len({t[0] for t in rec["titles"] if t[2] == "awarded"})
    return dict(by_team)


# Advanced stats shown on team pages (Milestone 7): (column, side, label, higher_is_better, format).
# Display only -- nothing here feeds a rating engine.
ADVANCED_DISPLAY = [
    ("off_ppa", "offense", "EPA / play", True, "epa"),
    ("off_success_rate", "offense", "Success rate", True, "pct"),
    ("off_explosiveness", "offense", "Explosiveness", True, "dec2"),
    ("off_pts_per_opp", "offense", "Points / scoring opp.", True, "dec2"),
    ("off_line_yards", "offense", "Line yards / rush", True, "dec2"),
    ("def_ppa", "defense", "EPA / play allowed", False, "epa"),
    ("def_success_rate", "defense", "Success rate allowed", False, "pct"),
    ("def_explosiveness", "defense", "Explosiveness allowed", False, "dec2"),
    ("def_pts_per_opp", "defense", "Points / opp. allowed", False, "dec2"),
    ("def_havoc", "defense", "Havoc rate", True, "pct"),
]


def build_advanced(conn: sqlite3.Connection) -> dict | None:
    """
    {"metrics": [...], "first_season", "teams": {team_id: {season: [[value, rank, n], ...]}}}
    Ranks are competition ranks among teams with a REPORTED value that season,
    pointed the right way per metric (lower EPA allowed = #1; higher havoc = #1).
    A NULL value stays null with no rank -- the page shows N/A, never 0.
    """
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='team_season_advanced'").fetchone():
        return None
    cols = [m[0] for m in ADVANCED_DISPLAY]
    rows = conn.execute(f"SELECT team_id, season_year, {', '.join(cols)} FROM team_season_advanced").fetchall()
    if not rows:
        return None
    ranks: dict = {}
    for i, (_, _, _, higher, _) in enumerate(ADVANCED_DISPLAY):
        for season in {r[1] for r in rows}:
            vals = {r[0]: r[2 + i] for r in rows if r[1] == season and r[2 + i] is not None}
            ordered = rank_desc(vals if higher else {t: -v for t, v in vals.items()})
            for t, rk in ordered.items():
                ranks[(i, t, season)] = (rk, len(vals))
    teams: dict = defaultdict(dict)
    for r in rows:
        tid, season = r[0], r[1]
        cells = []
        for i in range(len(ADVANCED_DISPLAY)):
            v = r[2 + i]
            rk, n = ranks.get((i, tid, season), (None, None))
            cells.append([None if v is None else round(v, 4), rk, n])
        teams[str(tid)][str(season)] = cells
    first = conn.execute("SELECT MIN(season_year) FROM team_season_advanced").fetchone()[0]
    return {"metrics": [list(m[1:]) for m in ADVANCED_DISPLAY], "first_season": min(first, 2001),
            "teams": dict(teams)}


def load_memberships(conn: sqlite3.Connection) -> dict:
    """
    {(team_id, season): conference} -- the conference a team actually belonged
    to in that season (Milestone E), so the team page's season-by-season history
    names the right league for old seasons instead of today's.

    Empty when the table isn't there (synthetic Elo-only fixtures); every
    team-season in the real database has one, which
    tests/test_team_pages.py::test_every_team_season_names_its_conference pins.
    """
    if not conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='team_membership_by_season'").fetchone():
        return {}
    return {(tid, season): conf for tid, season, conf in conn.execute(
        "SELECT team_id, season_year, conference_real FROM team_membership_by_season "
        "WHERE conference_real IS NOT NULL")}


def load_completed_games(conn: sqlite3.Connection) -> list[tuple]:
    # Same ordering build_elo.py uses, so the chart can never disagree with the engine.
    return conn.execute(
        """
        SELECT game_id, season_year, game_date, week, home_team_id, away_team_id,
               home_score, away_score, neutral_site, game_phase, went_ot
        FROM games
        WHERE home_score IS NOT NULL AND away_score IS NOT NULL
        ORDER BY season_year, game_date, game_id
        """
    ).fetchall()


def load_elo_rows(conn: sqlite3.Connection) -> list[tuple]:
    return conn.execute(
        """
        SELECT e.game_id, e.team_id, e.pregame_elo, e.opponent_pregame_elo,
               e.elo_expectation, e.elo_change, e.postgame_elo,
               g.season_year, g.game_date, e.mov_multiplier
        FROM elo_game_history e
        JOIN games g ON g.game_id = e.game_id
        WHERE g.home_score IS NOT NULL AND g.away_score IS NOT NULL
        ORDER BY e.team_id, g.season_year, g.game_date, e.game_id
        """
    ).fetchall()


def load_coe2_rollups(conn: sqlite3.Connection) -> dict:
    """
    {(team_id, season): {"games": x, "bonus": y, "total": z, "items": [[category, count, points], ...]}}
    from the ENG-13 rollup tables, so the ledger can show C = sum(game awards) + B
    with each bonus named. Empty when the rollups have not been built.
    """
    need = {"team_coe2_by_season", "team_coe2_bonuses"}
    have = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name IN "
        "('team_coe2_by_season', 'team_coe2_bonuses')")}
    if need - have:
        return {}
    out = {}
    for tid, season, games, bonus, total in conn.execute(
            "SELECT team_id, season_year, game_coe_total, bonus_total, season_coe2 FROM team_coe2_by_season"):
        out[(tid, season)] = {"games": round(games, 3), "bonus": round(bonus, 3),
                              "total": round(total, 3), "items": []}
    for tid, season, cat, n, pts in conn.execute(
            "SELECT team_id, season_year, category, count, points FROM team_coe2_bonuses ORDER BY category"):
        if (tid, season) in out:
            out[(tid, season)]["items"].append([cat, n, round(pts, 3)])
    return out


def load_game_coe2(conn: sqlite3.Connection) -> dict:
    """{(team_id, season): summed Game CoE 2.0}; absent where hybrid data doesn't exist."""
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='hybrid_game_ratings'"
    ).fetchone()
    if not exists:
        return {}
    out: dict = defaultdict(float)
    for team_id, season, coe in conn.execute(
        """
        SELECT h.team_id, g.season_year, h.game_coe
        FROM hybrid_game_ratings h JOIN games g ON g.game_id = h.game_id
        WHERE h.game_coe IS NOT NULL
        """
    ):
        out[(team_id, season)] += coe
    return dict(out)


def team_result(game_row: tuple, team_id: int) -> str:
    _, _, _, _, home, away, hs, as_, *_ = game_row
    mine, theirs = (hs, as_) if team_id == home else (as_, hs)
    return "W" if mine > theirs else "L" if mine < theirs else "T"


def rank_desc(values: dict) -> dict:
    """Competition ranking (1,2,2,4) of {key: value}, highest first."""
    ordered = sorted(values.items(), key=lambda kv: -kv[1])
    ranks, prev, prev_rank = {}, None, 0
    for i, (k, v) in enumerate(ordered, start=1):
        rank = prev_rank if v == prev else i
        ranks[k] = rank
        prev, prev_rank = v, rank
    return ranks


def build_team_pages(conn: sqlite3.Connection, champions: list[dict] | None = None) -> dict:
    games = load_completed_games(conn)
    game_by_id = {g[0]: g for g in games}
    elo_rows = load_elo_rows(conn)
    coe2 = load_game_coe2(conn)
    membership = load_memberships(conn)

    # ---- per team-season analytics ----
    acc: dict = {}
    by_team: dict = defaultdict(list)
    for gid, tid, pre, opp_pre, exp, chg, post, season, _date, _mov in elo_rows:
        if gid not in game_by_id:
            continue
        by_team[tid].append((gid, pre, opp_pre, exp, chg, post, season))
        a = acc.setdefault((tid, season), {"n": 0, "w": 0, "l": 0, "t": 0, "start": pre, "end": post,
                                           "opp_sum": 0.0, "exp_sum": 0.0})
        res = team_result(game_by_id[gid], tid)
        a["n"] += 1
        a["w" if res == "W" else "l" if res == "L" else "t"] += 1
        a["end"] = post            # rows are chronological, so the last one wins
        a["opp_sum"] += opp_pre    # opponent PREGAME Elo
        a["exp_sum"] += exp        # PREGAME win expectation (never current Elo)

    sos = {k: a["opp_sum"] / a["n"] for k, a in acc.items()}
    sos_rank, coe2_rank = {}, {}
    for season in {s for (_, s) in acc}:
        sos_rank.update({(t, season): r for t, r in
                         rank_desc({t: v for (t, s), v in sos.items() if s == season}).items()})
        coe2_rank.update({(t, season): r for t, r in
                          rank_desc({t: v for (t, s), v in coe2.items() if s == season}).items()})

    team_seasons = []
    for (tid, season), a in sorted(acc.items()):
        actual = a["w"] + 0.5 * a["t"]    # ties count half, matching Elo's S = 0.5
        team_seasons.append([
            tid, season, a["n"], a["w"], a["l"], a["t"],
            round(a["start"], 1), round(a["end"], 1), round(a["end"] - a["start"], 1),
            round(sos[(tid, season)], 1), sos_rank[(tid, season)],
            round(a["exp_sum"], 2), actual, round(actual - a["exp_sum"], 2),
            round(coe2[(tid, season)], 3) if (tid, season) in coe2 else None,
            coe2_rank.get((tid, season)),
            # conference appended LAST (position 16) so existing positions never move.
            membership.get((tid, season)),
        ])

    # ---- biggest single-game swings (wins only / losses only; ties excluded) ----
    swings = {}
    for tid, rows in by_team.items():
        wins = [r for r in rows if team_result(game_by_id[r[0]], tid) == "W" and r[4] > 0]
        losses = [r for r in rows if team_result(game_by_id[r[0]], tid) == "L" and r[4] < 0]
        wins.sort(key=lambda r: (-r[4], r[0]))
        losses.sort(key=lambda r: (r[4], r[0]))
        swings[str(tid)] = {"gains": [r[0] for r in wins[:SWING_COUNT]],
                            "losses": [r[0] for r in losses[:SWING_COUNT]]}

    # ---- REAL postseason history (dataset years, FBS-vs-FBS games) ----
    titles = title_history(champions or [])
    history = {}
    for tid in by_team:
        rec = [0, 0, 0]
        bowl = [0, 0, 0]          # appearances, wins, losses (ties rare; counted in appearances)
        cfp = {"seasons": [], "w": 0, "l": 0}
        seasons = sorted({s for (t, s) in acc if t == tid})
        for gid, *_ in by_team[tid]:
            g = game_by_id[gid]
            res = team_result(g, tid)
            rec["WLT".index(res)] += 1
            phase, season = g[9], g[1]
            if phase == "cfp" and season >= CFP_FIRST_SEASON:
                if season not in cfp["seasons"]:
                    cfp["seasons"].append(season)
                if res == "W":
                    cfp["w"] += 1
                elif res == "L":
                    cfp["l"] += 1
            elif phase in ("bowl", "cfp"):
                # Pre-2014 "cfp"-phase games are BCS-era title games: bowls, not CFP.
                bowl[0] += 1
                if res == "W":
                    bowl[1] += 1
                elif res == "L":
                    bowl[2] += 1
        history[str(tid)] = {
            "first_season": seasons[0], "last_season": seasons[-1], "seasons": len(seasons),
            "record": rec, "bowl": bowl,
            "cfp_appearances": len(cfp["seasons"]), "cfp_seasons": cfp["seasons"],
            "cfp_record": [cfp["w"], cfp["l"]],
            # Milestone 6: from the hand-maintained reference file, never from results.
            "titles": titles.get(tid, {}).get("titles", []),
            "title_count": titles.get(tid, {}).get("title_count", 0),
        }

    from export_static_data import kickoff_map
    kickoffs = kickoff_map(conn)
    return {
        "cfp_first_season": CFP_FIRST_SEASON,
        # ENG-13: the season total decomposed into game awards plus named bonuses.
        "coe2_rollups": {f"{t}|{s}": v for (t, s), v in load_coe2_rollups(conn).items()},
        "titles_since": min((c["season"] for c in champions), default=None) if champions else None,
        "advanced": build_advanced(conn),
        # kickoff_utc appended LAST (position 11) so existing positions never move; null for
        # date-only seasons or when kickoff times haven't been fetched (see export_static_data.kickoff_map).
        "games": [[g[0], g[1], g[2], g[3], g[4], g[5], g[6], g[7], g[8],
                   PHASE_CODE.get(g[9], 0), g[10], kickoffs.get(g[0], (None, None))[0]] for g in games],
        # mov_multiplier appended LAST (position 7) for the Elo ledger; existing positions never move.
        # Expectation kept to 4 decimals so a ledger row's K x (S - E) x M reproduces the stored change.
        "elo": [[r[0], r[1], round(r[2], 1), round(r[3], 1), round(r[4], 4), round(r[5], 2), round(r[6], 1),
                 None if r[9] is None else round(r[9], 4)] for r in elo_rows if r[0] in game_by_id],
        "team_seasons": team_seasons,
        "swings": swings,
        "history": history,
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--out", default="ui/data/team_pages.js")
    p.add_argument("--champions", default=str(CHAMPIONS_PATH))
    args = p.parse_args()

    conn = sqlite3.connect(args.db)
    data = build_team_pages(conn, load_national_champions(conn, Path(args.champions)))
    conn.close()

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("window.__TEAM_PAGES__=" + json.dumps(data, separators=(",", ":")) + ";\n", encoding="utf-8")
    print(f"Wrote {out} ({out.stat().st_size:,} bytes): {len(data['games'])} games, "
          f"{len(data['elo'])} Elo rows, {len(data['team_seasons'])} team-seasons")


if __name__ == "__main__":
    main()
