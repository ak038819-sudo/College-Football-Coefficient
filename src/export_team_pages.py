#!/usr/bin/env python3
"""
Team-page data export (Milestone 2): writes ui/data/team_pages/, one shard
per team and one per season, loaded a shard at a time as a reader opens a
team page or an Elo board.

Why .js files rather than .json: the dashboard is often opened straight
from disk (file://), where browsers block fetch() of local JSON. A classic
<script src> still works from disk AND from GitHub Pages, so each shard is
wrapped as an assignment onto `window`.

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
  non_fbs:      [[game_id, season, date, week, team_id, opponent_name,
                  team_score, opponent_score, is_home, neutral, phase, went_ot,
                  opponent_classification], ...]   NOT rated; see load_non_fbs_games
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
import hashlib
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


def load_non_fbs_games(conn: sqlite3.Connection) -> list[tuple]:
    """
    Completed games against an opponent this project does not hold a rating for.

    These live in their own table precisely so no rating can read them (see
    sql/non_fbs_game_tables.sql), and they stay out of `games`, `elo` and every
    team-season number in this file. They are exported only so a schedule can
    show the games that were actually played and a record can count them.

    Mostly FCS opponents, but not only: a handful of 1980s rows are programs
    CFBD still labels `fbs` which this project does not carry (Cal State
    Fullerton, Wichita State). "Unrated opponent" is therefore the honest
    description, and the source's own classification string rides along so the
    page can say which it was rather than guess.
    """
    have = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='non_fbs_games'"
    ).fetchone()
    if not have:
        return []
    return conn.execute(
        """
        SELECT game_id, season_year, game_date, week, team_id, opponent_name,
               team_score, opponent_score, is_home, neutral_site, game_phase,
               went_ot, opponent_classification
        FROM non_fbs_games
        ORDER BY team_id, season_year, game_date, game_id
        """
    ).fetchall()


def load_elo_rows(conn: sqlite3.Connection) -> list[tuple]:
    # game_coe is the Merits that one game awarded this team, straight from
    # build_hybrid_coefficients.py. LEFT JOINed because a rated game can have no
    # award: a bootstrap season -- one the archive holds fewer than five seasons
    # before -- carries no hybrid row at all, and a tie is stored as NULL. Those must reach the page as absent, not as zero,
    # which is a real award a regulation loss earns.
    hybrid = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='hybrid_game_ratings'"
    ).fetchone()
    award = ("h.game_coe" if hybrid else "NULL")
    join = ("LEFT JOIN hybrid_game_ratings h ON h.game_id = e.game_id AND h.team_id = e.team_id"
            if hybrid else "")
    return conn.execute(
        f"""
        SELECT e.game_id, e.team_id, e.pregame_elo, e.opponent_pregame_elo,
               e.elo_expectation, e.elo_change, e.postgame_elo,
               g.season_year, g.game_date, e.mov_multiplier, {award}
        FROM elo_game_history e
        JOIN games g ON g.game_id = e.game_id
        {join}
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
    # games_counted comes along because the rollups carry a zero-game placeholder
    # row for every pre-coverage season, and 0.0 there is not a measured zero --
    # the same exclusion src/export_merits.py makes for the Merits boards.
    for tid, season, games, bonus, total, counted in conn.execute(
            "SELECT team_id, season_year, game_coe_total, bonus_total, season_coe2, games_counted "
            "FROM team_coe2_by_season"):
        out[(tid, season)] = {"games": round(games, 3), "bonus": round(bonus, 3),
                              "total": round(total, 3), "counted": counted, "items": []}
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
    for gid, tid, pre, opp_pre, exp, chg, post, season, _date, _mov, _coe in elo_rows:
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

    # ---- games against unrated opponents (shown, never rated) ----
    non_fbs = load_non_fbs_games(conn)
    non_fbs_rec: dict = defaultdict(lambda: [0, 0, 0])
    for _gid, season, _date, _wk, tid, _opp, mine, theirs, *_rest in non_fbs:
        non_fbs_rec[(tid, season)]["WLT".index(
            "W" if mine > theirs else "L" if mine < theirs else "T")] += 1

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
        # mov_multiplier (position 7) and then the game's Merits award (position 8)
        # are appended LAST; existing positions never move.
        # Expectation kept to 4 decimals so a ledger row's K x (S - E) x M reproduces the stored change.
        "elo": [[r[0], r[1], round(r[2], 1), round(r[3], 1), round(r[4], 4), round(r[5], 2), round(r[6], 1),
                 None if r[9] is None else round(r[9], 4),
                 None if r[10] is None else round(r[10], 3)] for r in elo_rows if r[0] in game_by_id],
        # Games against opponents with no rating here. Deliberately a SEPARATE
        # array from "games": nothing downstream that computes a rating reads
        # this key, which is the same separation the table itself makes.
        "non_fbs": [[r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9],
                     PHASE_CODE.get(r[10], 0), r[11], r[12] or None] for r in non_fbs],
        # W-L-T of those games only, so the page can add them to a rated record
        # without re-deriving a record in the browser.
        "non_fbs_records": {f"{t}|{s_}": v for (t, s_), v in sorted(non_fbs_rec.items())},
        "team_seasons": team_seasons,
        "swings": swings,
        "history": history,
    }


# ---------------------------------------------------------------------------
# Sharding. build_team_pages() above returns ONE object holding every team and
# every season, which is what ui/data/team_pages.js used to be: 7.3 MB downloaded to
# draw one team's page, of which about 70 KB was that team's.
#
# The file is split along BOTH of the ways the site reads it, because the two
# are at right angles:
#
#   a team page   wants one team across all 47 seasons
#   the Elo board, the Top 25 and the "as of week N" views each want one
#                 season across all 139 teams, because they recompute Last 5,
#                 strength of schedule and the record as they stood then
#
# Sharding one way alone turns the other into 139 (or 47) downloads, so the
# Elo and game rows are written TWICE, once under each index. That reverses
# this module's original "no game duplicated per team" rule on purpose: it
# costs a few megabytes on a disk already holding half a gigabyte, and it is
# the difference between a reader fetching 7.3 MB and fetching 70 KB.
#
# `core` holds what neither index can answer alone -- including the two
# per-season totals the browser used to derive by counting every team-season
# row it had. A shard does not have them all, so counting there would quietly
# produce a smaller denominator and a wrong rank.


def _blank_scope() -> dict:
    return {"games": [], "elo": [], "team_seasons": [], "non_fbs": []}


def split_team_pages(data: dict) -> dict:
    """One whole-league payload in, {core, teams, seasons} out.

    Pure: it reads `data` and builds new lists, so the builder keeps its
    single authoritative pass over the database and this only re-files what
    that pass produced.
    """
    teams: dict[int, dict] = {}
    seasons: dict[int, dict] = {}

    def team(tid):
        return teams.setdefault(int(tid), _blank_scope())

    def season(year):
        return seasons.setdefault(int(year), _blank_scope())

    # A game belongs to BOTH clubs playing it and to its season.
    for game in data["games"]:
        season(game[1])["games"].append(game)
        team(game[4])["games"].append(game)
        if game[5] != game[4]:
            team(game[5])["games"].append(game)

    # An Elo row is one team's view of one game, so it has a single home in
    # the team index; its season comes from the game it names.
    season_of_game = {g[0]: g[1] for g in data["games"]}
    for row in data["elo"]:
        team(row[1])["elo"].append(row)
        year = season_of_game.get(row[0])
        if year is not None:
            season(year)["elo"].append(row)

    for row in data["team_seasons"]:
        team(row[0])["team_seasons"].append(row)
        season(row[1])["team_seasons"].append(row)

    # The season views count these toward a record, so both indexes carry them.
    for row in data.get("non_fbs", []):
        team(row[4])["non_fbs"].append(row)
        season(row[1])["non_fbs"].append(row)

    # Team-keyed extras that only a team page reads.
    for key, bucket in (("swings", "swings"), ("history", "history")):
        for tid, value in (data.get(key) or {}).items():
            team(tid)[bucket] = value
    advanced = data.get("advanced") or {}
    for tid, value in (advanced.get("teams") or {}).items():
        team(tid)["advanced"] = value
    for composite, value in (data.get("coe2_rollups") or {}).items():
        tid = composite.split("|", 1)[0]
        team(tid).setdefault("coe2_rollups", {})[composite] = value
    for composite, value in (data.get("non_fbs_records") or {}).items():
        tid = composite.split("|", 1)[0]
        team(tid).setdefault("non_fbs_records", {})[composite] = value

    # Counted over the WHOLE league, never inside a shard. The browser used to
    # derive these while indexing; a shard holds one season or one team, so the
    # same loop there would divide by the wrong number.
    teams_per_season: dict[int, int] = {}
    coe2_per_season: dict[int, int] = {}
    for row in data["team_seasons"]:
        year = int(row[1])
        teams_per_season[year] = teams_per_season.get(year, 0) + 1
        if row[14] is not None:
            coe2_per_season[year] = coe2_per_season.get(year, 0) + 1

    core = {
        "cfp_first_season": data.get("cfp_first_season"),
        "titles_since": data.get("titles_since"),
        "teams_per_season": teams_per_season,
        "coe2_per_season": coe2_per_season,
        # The metric list and its first season describe the Analytics tab for
        # every club; only the per-team numbers move into the shards.
        "advanced": ({k: v for k, v in advanced.items() if k != "teams"}
                     if advanced else None),
    }
    return {"core": core, "teams": teams, "seasons": seasons}


def write_team_pages(data: dict, out_dir: Path) -> dict:
    """Write core.js plus one file per team and per season; return the manifest."""
    split = split_team_pages(data)
    out_dir.mkdir(parents=True, exist_ok=True)

    def write(path: Path, variable: str, key, payload) -> str:
        path.parent.mkdir(parents=True, exist_ok=True)
        body = json.dumps(payload, separators=(",", ":"))
        if key is None:
            text = f"window.{variable}={body};\n"
        else:
            text = (f"(window.{variable}=window.{variable}||{{}})"
                    f"[{json.dumps(str(key))}]={body};\n")
        path.write_text(text, encoding="utf-8")
        return hashlib.sha256(path.read_bytes()).hexdigest()[:10]

    manifest = {"teams": {}, "seasons": {}}
    version = write(out_dir / "core.js", "__TEAM_PAGES_CORE__", None, split["core"])
    manifest["core"] = f"data/team_pages/core.js?v={version}"
    for tid, payload in sorted(split["teams"].items()):
        version = write(out_dir / "team" / f"{tid}.js", "__TEAM_PAGES_TEAM__", tid, payload)
        manifest["teams"][str(tid)] = f"data/team_pages/team/{tid}.js?v={version}"
    for year, payload in sorted(split["seasons"].items()):
        version = write(out_dir / "season" / f"{year}.js", "__TEAM_PAGES_SEASON__", year, payload)
        manifest["seasons"][str(year)] = f"data/team_pages/season/{year}.js?v={version}"
    return manifest


# Reading the shards back. The season index alone carries every game, every
# Elo row and every team-season the league has, each exactly once, so a reader
# that wants the whole league -- the identity audit and the conference-flow
# audit both do -- rebuilds it from the season files and never needs the
# per-team index, which holds the same rows a second time.


def _assignment_body(text: str, bracketed: bool) -> dict:
    """The JSON out of `window.X=<json>;` or `(window.X=...)["k"]=<json>;`."""
    body = text.split("]=", 1)[1] if bracketed else text.split("=", 1)[1]
    return json.loads(body.rstrip().rstrip(";"))


def read_sharded_team_pages(shard_dir: Path) -> dict:
    """Reassemble the league-wide payload from <shard_dir>/season/*.js."""
    out = {"games": [], "elo": [], "team_seasons": [], "non_fbs": []}
    for path in sorted((shard_dir / "season").glob("*.js")):
        payload = _assignment_body(path.read_text(encoding="utf-8"), bracketed=True)
        for key, rows in out.items():
            rows.extend(payload.get(key, []))
    out.update(_assignment_body((shard_dir / "core.js").read_text(encoding="utf-8"),
                                bracketed=False))
    return out


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--out", default="ui/data/team_pages")
    p.add_argument("--champions", default=str(CHAMPIONS_PATH))
    args = p.parse_args()

    conn = sqlite3.connect(args.db)
    data = build_team_pages(conn, load_national_champions(conn, Path(args.champions)))
    conn.close()

    manifest = write_team_pages(data, Path(args.out))
    print(f"Wrote {args.out}: {len(manifest['teams'])} team shards, "
          f"{len(manifest['seasons'])} season shards, "
          f"{len(data['games'])} games, {len(data['elo'])} Elo rows, "
          f"{len(data['team_seasons'])} team-seasons, "
          f"{len(data['non_fbs'])} games against unrated opponents")


if __name__ == "__main__":
    main()
