#!/usr/bin/env python3
"""Browsable boards for the People section: career leaders, and coaches.

WHY THIS EXISTS. `#section=people` rendered a search box and nothing else:
with no query it said "enter a name to search". Behind it sat 99,813 players,
827 coaches, 1,112,976 season statistics and 5,287 measured coach tenures, and
a reader who did not already know a name could reach none of it.

PRECOMPUTED, BECAUSE A MILLION ROWS CANNOT BE SUMMED IN A BROWSER. The
per-season "National leaders" view sums box scores client-side, which is why
it starts in 2005 and only covers box-score categories. These boards are built
here, once, from the season-statistics archive.

ONLY COUNTING STATS ARE SUMMED. Yards, touchdowns, carries, receptions,
tackles, sacks all add up over a career. YPA, PCT, YPC, YPR, AVG and LONG do
not -- a career "completion percentage" built by adding up eighteen season
percentages is not a number, and LONG is a maximum, not a total. Those are
left out rather than published wrong; see COUNTING.

AND THESE ARE NOT ALL-TIME BOARDS. The archive begins in 2009, and defensive
statistics only in 2016, so "career" here means "since the archive starts".
Each board carries its own first season so the page can say which, rather than
implying this site knows what happened in 1975.

Usage:
    python src/export_people_boards.py --db db/league.db
"""
from __future__ import annotations

import argparse
import sqlite3
from collections import defaultdict

# The boards, in the order the page offers them. Each is (category, stat_type,
# label). A stat not listed here is deliberately absent: see the module
# docstring on why a rate or a maximum must not be summed.
COUNTING = [
    ("passing", "YDS", "Passing yards"),
    ("passing", "TD", "Passing touchdowns"),
    ("passing", "COMPLETIONS", "Completions"),
    ("rushing", "YDS", "Rushing yards"),
    ("rushing", "TD", "Rushing touchdowns"),
    ("rushing", "CAR", "Carries"),
    ("receiving", "YDS", "Receiving yards"),
    ("receiving", "TD", "Receiving touchdowns"),
    ("receiving", "REC", "Receptions"),
    ("defensive", "TOT", "Tackles"),
    ("defensive", "SACKS", "Sacks"),
    ("defensive", "TFL", "Tackles for loss"),
    ("defensive", "PD", "Passes defended"),
    ("interceptions", "INT", "Interceptions"),
    ("kicking", "FGM", "Field goals made"),
    ("kicking", "PTS", "Kicking points"),
]

# A rate or a maximum, named so the refusal is a decision on the record rather
# than an oversight. Nothing here is ever summed.
NOT_SUMMABLE = {"YPA", "PCT", "YPC", "YPR", "AVG", "LONG"}

BOARD_SIZE = 100


def _table(conn: sqlite3.Connection, name: str) -> bool:
    return bool(conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                             (name,)).fetchone())


def build_career_leaders(conn: sqlite3.Connection, limit: int = BOARD_SIZE) -> dict:
    """{board_key: {label, first_season, leaders: [...]}}.

    A board's `first_season` is the first season the archive has that stat in,
    measured rather than assumed: defensive statistics start seven years after
    passing ones, and a page that said "career" for both would be wrong about
    one of them.
    """
    if not (_table(conn, "player_season_stats") and _table(conn, "players")):
        return {}
    names = {int(pid): name for pid, name in
             conn.execute("SELECT player_id, display_name FROM players")}
    boards = {}
    for category, stat_type, label in COUNTING:
        assert stat_type not in NOT_SUMMABLE, f"{label} is a rate or a maximum"
        totals: dict[int, list] = defaultdict(lambda: [0.0, 0, None])
        first = None
        for pid, season, value in conn.execute(
                "SELECT player_id, season_year, stat FROM player_season_stats "
                "WHERE category = ? AND stat_type = ? AND stat IS NOT NULL",
                (category, stat_type)):
            row = totals[int(pid)]
            row[0] += float(value)
            row[1] += 1
            row[2] = season if row[2] is None else min(row[2], season)
            first = season if first is None else min(first, season)
        if not totals:
            continue
        ranked = sorted(((pid, v) for pid, v in totals.items() if pid in names),
                        key=lambda kv: (-kv[1][0], names[kv[0]]))[:limit]
        boards[f"{category}_{stat_type}"] = {
            "label": label, "category": category, "first_season": first,
            # value rounds to one place: sacks and TFL are halves in this feed,
            # and an integer cast would quietly lose every half.
            "leaders": [[pid, names[pid], round(total, 1), seasons, debut]
                        for pid, (total, seasons, debut) in ranked]}
    return boards


def build_coach_board(conn: sqlite3.Connection, limit: int = BOARD_SIZE) -> dict:
    """Career wins above the Elo expectation, per coach.

    A season shared with another head coach is NOT measured -- the feed never
    says which games were whose -- so it is counted and reported beside the
    total rather than silently folded in. A coach whose every season is shared
    has no total at all and is left off, not shown as a zero.
    """
    if not (_table(conn, "coach_season_metrics") and _table(conn, "coaches")):
        return {}
    names = {int(cid): name for cid, name in
             conn.execute("SELECT coach_id, display_name FROM coaches")}
    agg: dict[int, dict] = defaultdict(
        lambda: {"wae": 0.0, "measured": 0, "unmeasured": 0, "games": 0,
                 "wins": 0, "first": None, "last": None})
    for cid, season, attributed, games, wins, wae in conn.execute(
            "SELECT coach_id, season_year, attributed, games, wins, wins_above_expected "
            "FROM coach_season_metrics"):
        a = agg[int(cid)]
        a["first"] = season if a["first"] is None else min(a["first"], season)
        a["last"] = season if a["last"] is None else max(a["last"], season)
        if attributed and wae is not None:
            a["wae"] += float(wae)
            a["measured"] += 1
            a["games"] += int(games or 0)
            a["wins"] += int(wins or 0)
        else:
            a["unmeasured"] += 1
    rows = [(cid, a) for cid, a in agg.items() if a["measured"] and cid in names]
    rows.sort(key=lambda kv: (-kv[1]["wae"], names[kv[0]]))
    return {"fields": ["coach_id", "name", "wins_above_expected", "seasons_measured",
                       "seasons_unmeasured", "games", "wins", "first_season", "last_season"],
            "coaches": [[cid, names[cid], round(a["wae"], 2), a["measured"],
                         a["unmeasured"], a["games"], a["wins"], a["first"], a["last"]]
                        for cid, a in rows[:limit]],
            # The whole denominator, so the page can say what it is a top-100 OF.
            "measured_coaches": len(rows),
            "unmeasured_seasons": sum(a["unmeasured"] for _, a in agg.items())}


def build(conn: sqlite3.Connection) -> dict:
    leaders, coaches = build_career_leaders(conn), build_coach_board(conn)
    if not leaders and not coaches:
        return {}
    return {"leaders": leaders, "coaches": coaches, "board_size": BOARD_SIZE}


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="db/league.db")
    args = parser.parse_args(argv)
    conn = sqlite3.connect(args.db)
    try:
        out = build(conn)
    finally:
        conn.close()
    print(f"people boards: {len(out.get('leaders', {}))} career boards, "
          f"{len(out.get('coaches', {}).get('coaches', []))} coaches of "
          f"{out.get('coaches', {}).get('measured_coaches', 0)} measured")


if __name__ == "__main__":
    main()
