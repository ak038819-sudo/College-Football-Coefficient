#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
from typing import Dict, List, Tuple


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def get_team_strength_5yr(conn: sqlite3.Connection, year: int, team_id: int, formula_version: str) -> Tuple[float, float, int]:
    """
    Returns (total_points_5yr, ppg_5yr, games_counted_5yr). If missing, returns zeros.
    """
    row = conn.execute(
        """
        SELECT total_points_5yr, points_per_game_5yr, games_counted_5yr
        FROM team_coefficient_rolling_5yr
        WHERE season_year=? AND team_id=? AND formula_version=?
        """,
        (year, team_id, formula_version),
    ).fetchone()

    if not row:
        return (0.0, 0.0, 0)

    return (float(row["total_points_5yr"]), float(row["points_per_game_5yr"]), int(row["games_counted_5yr"]))


def choose_host(
    a: Dict,
    b: Dict,
) -> Dict:
    """
    Host is decided by team 5-year CoE:
      1) higher total_points_5yr
      2) higher points_per_game_5yr
      3) stable fallback: lower slot hosts
    """
    if a["tp5"] != b["tp5"]:
        return a if a["tp5"] > b["tp5"] else b
    if a["ppg5"] != b["ppg5"]:
        return a if a["ppg5"] > b["ppg5"] else b
    return a if a["slot"] < b["slot"] else b


def load_bracket_slots(
    conn: sqlite3.Connection,
    year: int,
    formula_version: str,
    ruleset: str,
) -> List[Dict]:
    rows = conn.execute(
        """
        SELECT
          b.slot,
          b.pot,
          b.team_id,
          t.team_name,
          p.conference,
          p.conf_coe_rank,
          p.conf_rank,
          p.bid_type,
          b.draw_seed
        FROM playoff_bracket_year1 b
        JOIN teams t ON t.team_id=b.team_id
        JOIN playoff_pots_by_year p
          ON p.season_year=b.season_year AND p.team_id=b.team_id
         AND p.formula_version=b.formula_version AND p.ruleset=b.ruleset
        WHERE b.season_year=? AND b.formula_version=? AND b.ruleset=?
        ORDER BY b.slot
        """,
        (year, formula_version, ruleset),
    ).fetchall()

    out: List[Dict] = []
    for r in rows:
        out.append(
            {
                "slot": int(r["slot"]),
                "pot": int(r["pot"]),
                "team_id": int(r["team_id"]),
                "team_name": r["team_name"],
                "conference": r["conference"],
                "conf_coe_rank": int(r["conf_coe_rank"]),
                "conf_rank": int(r["conf_rank"]),
                "bid_type": r["bid_type"],
                "draw_seed": int(r["draw_seed"]),
            }
        )
    return out


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--formula-version", default="v0")
    p.add_argument("--ruleset", default="year1")
    args = p.parse_args()

    conn = connect(args.db)
    try:
        slots = load_bracket_slots(conn, args.year, args.formula_version, args.ruleset)
        if len(slots) != 24:
            raise SystemExit(f"Expected 24 bracket slots, found {len(slots)}. Did draw_playoff_year1.py run?")

        draw_seed = slots[0]["draw_seed"] if slots else None

        # enrich with rolling team strength for host decisions
        for s in slots:
            tp5, ppg5, g5 = get_team_strength_5yr(conn, args.year, s["team_id"], args.formula_version)
            s["tp5"] = tp5
            s["ppg5"] = ppg5
            s["g5"] = g5

        byes = [s for s in slots if s["slot"] <= 8]
        playin = [s for s in slots if s["slot"] >= 9]

        print(f"\nYear 1 Bracket — {args.year} (formula={args.formula_version}, ruleset={args.ruleset}, draw_seed={draw_seed})\n")

        print("BYES (Round of 16):")
        for s in byes:
            print(
                f"  Slot {s['slot']:>2}: {s['team_name']:<18} "
                f"({s['conference']}, conf_finish={s['conf_rank']}, conf_coe_rank={s['conf_coe_rank']})"
            )

        # First round matchups by slot pairing: 9v24, 10v23, ..., 16v17
        pairs = [(9, 24), (10, 23), (11, 22), (12, 21), (13, 20), (14, 19), (15, 18), (16, 17)]
        slot_map = {s["slot"]: s for s in slots}

        print("\nFIRST ROUND (Play-in):")
        for a_slot, b_slot in pairs:
            a = slot_map[a_slot]
            b = slot_map[b_slot]
            host = choose_host(a, b)
            away = b if host is a else a

            print(
                f"  ({a_slot:>2} vs {b_slot:>2}) "
                f"{host['team_name']} hosts {away['team_name']} "
                f"[host CoE5={host['tp5']:.1f} ppg={host['ppg5']:.3f}]"
            )

        print("\nROUND OF 16 (Structure only):")
        # Standard bracket mapping: slot 1 plays winner of (16 vs 17), etc.
        r16_map = [
            (1, (16, 17)),
            (2, (15, 18)),
            (3, (14, 19)),
            (4, (13, 20)),
            (5, (12, 21)),
            (6, (11, 22)),
            (7, (10, 23)),
            (8, (9, 24)),
        ]
        for bye_slot, (x, y) in r16_map:
            bye_team = slot_map[bye_slot]["team_name"]
            print(f"  Slot {bye_slot:>2} {bye_team:<18} vs Winner({x} vs {y})")

    finally:
        conn.close()


if __name__ == "__main__":
    main()