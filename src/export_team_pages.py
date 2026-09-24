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
                  wins_above_expected, coe2_season, coe2_rank], ...]
  swings:       {team_id: {"gains": [game_id...], "losses": [game_id...]}}
  history:      {team_id: {...REAL postseason history only...}}

Model (simulated) playoff history is deliberately NOT in this file; it
already lives in dashboard_data.json and is displayed under a separate,
labeled heading so the two can never be mixed.
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
import sqlite3

PHASE_CODE = {"regular": 0, "bowl": 1, "cfp": 2}
CFP_FIRST_SEASON = 2014   # 4-team CFP began with the 2014 season
SWING_COUNT = 10


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
               g.season_year, g.game_date
        FROM elo_game_history e
        JOIN games g ON g.game_id = e.game_id
        WHERE g.home_score IS NOT NULL AND g.away_score IS NOT NULL
        ORDER BY e.team_id, g.season_year, g.game_date, e.game_id
        """
    ).fetchall()


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


def build_team_pages(conn: sqlite3.Connection) -> dict:
    games = load_completed_games(conn)
    game_by_id = {g[0]: g for g in games}
    elo_rows = load_elo_rows(conn)
    coe2 = load_game_coe2(conn)

    # ---- per team-season analytics ----
    acc: dict = {}
    by_team: dict = defaultdict(list)
    for gid, tid, pre, opp_pre, exp, chg, post, season, _date in elo_rows:
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
        }

    return {
        "cfp_first_season": CFP_FIRST_SEASON,
        "games": [[g[0], g[1], g[2], g[3], g[4], g[5], g[6], g[7], g[8],
                   PHASE_CODE.get(g[9], 0), g[10]] for g in games],
        "elo": [[r[0], r[1], round(r[2], 1), round(r[3], 1), round(r[4], 3), round(r[5], 1), round(r[6], 1)]
                for r in elo_rows if r[0] in game_by_id],
        "team_seasons": team_seasons,
        "swings": swings,
        "history": history,
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--out", default="ui/data/team_pages.js")
    args = p.parse_args()

    conn = sqlite3.connect(args.db)
    data = build_team_pages(conn)
    conn.close()

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("window.__TEAM_PAGES__=" + json.dumps(data, separators=(",", ":")) + ";\n", encoding="utf-8")
    print(f"Wrote {out} ({out.stat().st_size:,} bytes): {len(data['games'])} games, "
          f"{len(data['elo'])} Elo rows, {len(data['team_seasons'])} team-seasons")


if __name__ == "__main__":
    main()
