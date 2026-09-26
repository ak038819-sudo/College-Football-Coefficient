#!/usr/bin/env python3
"""
Point-in-time Elo standings and week-to-week movement (P1-05, P1-06).

The site could show a season's FINAL Elo and a team's own rating history, but
not "where did the table stand in week 8?" or "who moved, and how far?". This
module answers both from the engine's own audit trail. Nothing is recalculated:
every rating here is a `postgame_elo` that `build_elo.py` wrote, or a
`pregame_elo` for the preseason snapshot.

## Stages

A season is cut into stages, oldest first:

    pre       Preseason -- each team's rating ENTERING the season, which is the
              pregame_elo of its first game. Not 1500 for most teams: Elo carries
              over with the configured offseason retention, so this snapshot is
              the carry-in, and week 1's movement is measured against it.
    w<N>      one stage per regular-season week the season actually has
    post      every bowl and CFP game, as one stage

Postseason is one stage on purpose. `games.week` restarts at 1 for the bowl and
cfp phases, and the two phases INTERLEAVE in wall-clock time -- in 2024 bowls run
2024-12-15 to 2025-01-04 while the CFP runs 2024-12-21 to 2025-01-21 -- so
"after the bowls" is not a moment in time at all. One postseason stage is the
honest cut, and its snapshot is the season's final table.

## What a snapshot means

The rating shown for a team at stage k is the postgame Elo of the LAST game that
team played among stages 1..k, taken straight from `elo_game_history`. Because
each of those numbers was computed by walking every game in strict chronological
order, a snapshot is a real historical state, not a re-simulation of a subset.

One wrinkle worth knowing rather than hiding: stage membership follows the week
and phase labels, not the calendar, and in a few seasons a late regular-season
week is played after the first bowls (1998: week 15 on 12-05, bowls from 12-04).
So "as of week 15" can include a game played a day after a bowl game that lands
in the postseason stage. The alternative -- cutting by date and calling the
result "week 15" -- would be less honest, not more.

## Movement

`d_elo` and `d_rank` compare a team against its own previous stage. They are
computed from the ROUNDED ratings the site displays, so a column of movements
always reconciles with the ratings beside it rather than being off by a tenth.
Both are None when there is no previous stage, or when the team has no rating in
it -- never 0, which would claim the team stood still.

`d_rank` is reported the way a reader sees movement: positive means the team
climbed (rank 12 -> rank 8 is +4), even though the rank number went down.

`stage_games` is how many games the team played IN that stage, which is what
separates a real 0.0 from a bye week. A team on a bye keeps its rating, so its
movement is genuinely 0.0 -- but a reader should be told it did not play rather
than left to infer that a game came out exactly even.

Ranks are shared on a tie (two teams at the same rating are both 4th, and the
next team is 6th), the same convention build_coe2_rollups.conference_coe2_rank
uses.
"""
from __future__ import annotations

import sqlite3
from collections import defaultdict

# Positional rows with the field list shipped alongside, appended-last on change,
# matching every other payload the site loads (see src/export_static_data.py).
STAGE_FIELDS = ["key", "label", "phase", "week", "first_date", "last_date", "games"]
ROW_FIELDS = ["team", "elo", "rank", "d_elo", "d_rank", "played", "stage_games"]

PRESEASON_KEY = "pre"
POSTSEASON_KEY = "post"
POSTSEASON_PHASES = ("bowl", "cfp")


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return bool(conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone())


def stage_key(phase: str, week) -> str:
    """Which stage a game belongs to. The only place that mapping is defined."""
    return POSTSEASON_KEY if phase in POSTSEASON_PHASES else f"w{int(week)}"


def stage_label(key: str) -> str:
    if key == PRESEASON_KEY:
        return "Preseason"
    if key == POSTSEASON_KEY:
        return "Postseason"
    return f"Week {key[1:]}"


def ranks_for(values: list[float]) -> list[int]:
    """
    Competition ranks for values already sorted strongest first: ties share the
    better rank and the next distinct value skips the ranks they used up.
    """
    out: list[int] = []
    for i, value in enumerate(values):
        out.append(out[-1] if i and value == values[i - 1] else i + 1)
    return out


def _fetch_games(conn: sqlite3.Connection, season: int | None) -> list[sqlite3.Row]:
    """
    Completed games with their Elo rows, in the engine's own order: the same
    (season, game_date, game_id) ordering build_elo.py walks, so "the last game
    so far" means the same thing here as it does there.
    """
    where = "g.home_score IS NOT NULL AND g.away_score IS NOT NULL"
    params: tuple = ()
    if season is not None:
        where += " AND g.season_year = ?"
        params = (season,)
    # Its own cursor with its own row_factory: callers pass connections configured
    # either way (export_static_data.py does not set one), and this module reads
    # its rows by name.
    cur = conn.cursor()
    cur.row_factory = sqlite3.Row
    return cur.execute(
        f"""
        SELECT g.season_year, g.game_id, g.game_date, g.week, g.game_phase,
               e.team_id, e.pregame_elo, e.postgame_elo
        FROM games g
        JOIN elo_game_history e ON e.game_id = g.game_id
        WHERE {where}
        ORDER BY g.season_year, g.game_date, g.game_id
        """,
        params,
    ).fetchall()


def build_timeline(conn: sqlite3.Connection, season: int | None = None) -> dict:
    """
    {season: payload}. A season with no Elo rows is absent rather than present
    and empty, so a caller can tell "not built yet" from "built and empty".
    """
    if not (_table_exists(conn, "elo_game_history") and _table_exists(conn, "games")):
        return {}

    names = {tid: name for tid, name in conn.execute("SELECT team_id, team_name FROM teams")}

    by_season: dict[int, list[sqlite3.Row]] = defaultdict(list)
    for row in _fetch_games(conn, season):
        by_season[row["season_year"]].append(row)

    return {year: _season_payload(year, rows, names) for year, rows in sorted(by_season.items())}


def _season_payload(season: int, rows: list[sqlite3.Row], names: dict) -> dict:
    # Stage order: the regular-season weeks the season actually has, by week
    # number, then the postseason if it has one. Never a fixed 1..15 list -- a
    # season that played 14 weeks should not show an empty 15th.
    weeks = sorted({int(r["week"]) for r in rows if r["game_phase"] not in POSTSEASON_PHASES})
    keys = [PRESEASON_KEY] + [f"w{w}" for w in weeks]
    if any(r["game_phase"] in POSTSEASON_PHASES for r in rows):
        keys.append(POSTSEASON_KEY)

    entering: dict[int, float] = {}     # team -> rating carried into the season
    per_stage: dict[str, list[sqlite3.Row]] = defaultdict(list)
    meta: dict[str, dict] = {}
    for row in rows:                                  # already in chronological order
        team = row["team_id"]
        if team not in entering:
            entering[team] = row["pregame_elo"]
        key = stage_key(row["game_phase"], row["week"])
        per_stage[key].append(row)
        info = meta.setdefault(key, {"first": row["game_date"], "last": row["game_date"],
                                     "games": set(), "phase": row["game_phase"],
                                     "week": None if key == POSTSEASON_KEY else int(row["week"])})
        info["last"] = max(info["last"], row["game_date"])
        info["first"] = min(info["first"], row["game_date"])
        info["games"].add(row["game_id"])

    teams = sorted(entering, key=lambda tid: names.get(tid, str(tid)))
    index_of = {tid: i for i, tid in enumerate(teams)}

    current = dict(entering)
    played: dict[int, int] = defaultdict(int)
    in_stage: dict[int, int] = defaultdict(int)
    previous: dict[int, tuple[float, int]] = {}        # team -> (rating, rank) last stage
    stages, snapshots = [], []

    for key in keys:
        in_stage.clear()
        if key != PRESEASON_KEY:
            for row in per_stage[key]:                # chronological within the stage
                current[row["team_id"]] = row["postgame_elo"]
                played[row["team_id"]] += 1
                in_stage[row["team_id"]] += 1

        # Rounded first, then everything else derives from what the site shows.
        ordered = sorted(((round(current[tid], 1), tid) for tid in current),
                         key=lambda pair: (-pair[0], names.get(pair[1], "")))
        rank_of = ranks_for([elo for elo, _ in ordered])

        rows_out, now = [], {}
        for (elo, tid), rank in zip(ordered, rank_of):
            before = previous.get(tid)
            rows_out.append([index_of[tid], elo, rank,
                             None if before is None else round(elo - before[0], 1),
                             None if before is None else before[1] - rank,
                             played[tid], in_stage[tid]])
            now[tid] = (elo, rank)
        previous = now

        info = meta.get(key, {})
        stages.append([key, stage_label(key),
                       "preseason" if key == PRESEASON_KEY
                       else "postseason" if key == POSTSEASON_KEY else info.get("phase", ""),
                       info.get("week"),
                       info.get("first"), info.get("last"),
                       len(info.get("games", ()))])
        snapshots.append(rows_out)

    return {
        "season": season,
        "teams": [[tid, names.get(tid, str(tid))] for tid in teams],
        "stage_fields": STAGE_FIELDS,
        "stages": stages,
        "row_fields": ROW_FIELDS,
        "rows": snapshots,
    }
