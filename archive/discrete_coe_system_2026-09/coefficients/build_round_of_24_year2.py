#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
from typing import Dict, List, Tuple, Optional


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def strength_key(conn: sqlite3.Connection, year: int, team_id: int, formula_version: str) -> Tuple[float, float, str]:
    """
    Higher is better: (total_points_5yr, points_per_game_5yr).
    Deterministic fallback if rolling rows missing.
    """
    row = conn.execute(
        """
        SELECT r.total_points_5yr, r.points_per_game_5yr, t.team_name
        FROM teams t
        LEFT JOIN team_coefficient_rolling_5yr r
          ON r.team_id = t.team_id
         AND r.season_year = ?
         AND r.formula_version = ?
        WHERE t.team_id = ?
        """,
        (year, formula_version, team_id),
    ).fetchone()

    name = row["team_name"] if row and row["team_name"] else f"team_id={team_id}"
    tp5 = float(row["total_points_5yr"]) if row and row["total_points_5yr"] is not None else 0.0
    ppg5 = float(row["points_per_game_5yr"]) if row and row["points_per_game_5yr"] is not None else 0.0
    return (tp5, ppg5, name)


def choose_home_away(conn: sqlite3.Connection, year: int, a: int, b: int, formula_version: str) -> Tuple[int, int]:
    """
    Home team determined by CoE regardless of draw placement.
    Ties break by team_name ASC (deterministic).
    """
    ka = strength_key(conn, year, a, formula_version)
    kb = strength_key(conn, year, b, formula_version)

    if ka[0] != kb[0]:
        return (a, b) if ka[0] > kb[0] else (b, a)
    if ka[1] != kb[1]:
        return (a, b) if ka[1] > kb[1] else (b, a)
    return (a, b) if ka[2] < kb[2] else (b, a)


def load_team_conferences(
    conn: sqlite3.Connection, year: int, formula_version: str, ruleset: str
) -> Dict[int, str]:
    rows = conn.execute(
        """
        SELECT team_id, conference
        FROM playoff_field_by_year
        WHERE season_year=? AND formula_version=? AND ruleset=?
        """,
        (year, formula_version, ruleset),
    ).fetchall()

    if len(rows) != 24:
        raise SystemExit(f"Expected 24 rows in playoff_field_by_year, got {len(rows)}. Run draw_playoff_year_2.py first.")
    return {int(r["team_id"]): str(r["conference"]) for r in rows}


def backtrack_pairings(
    pot1: List[int],
    pot2: List[int],
    conf_of: Dict[int, str],
    preferred: List[int],
) -> Optional[List[Tuple[int, int]]]:
    """
    Find a full matching between pot1 and pot2 with constraint:
      conf(pot1[i]) != conf(pot2[j])
    Deterministic: tries options in a stable order (preferred first, then remaining pot2 order).
    Returns list of (p1_team, p2_team) in pot1 order, or None if impossible.
    """
    used = set()
    result: List[Tuple[int, int]] = []

    # Precompute candidate lists in deterministic order
    candidates: List[List[int]] = []
    for i, a in enumerate(pot1):
        pref_b = preferred[i]
        opts = []
        if conf_of[a] != conf_of[pref_b]:
            opts.append(pref_b)
        for b in pot2:
            if b == pref_b:
                continue
            if conf_of[a] != conf_of[b]:
                opts.append(b)
        candidates.append(opts)

    def dfs(i: int) -> bool:
        if i == len(pot1):
            return True
        a = pot1[i]
        for b in candidates[i]:
            if b in used:
                continue
            used.add(b)
            result.append((a, b))
            if dfs(i + 1):
                return True
            result.pop()
            used.remove(b)
        return False

    ok = dfs(0)
    return result if ok else None


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--formula-version", default="v0")
    p.add_argument("--ruleset", default="year2")
    args = p.parse_args()

    conn = connect(args.db)
    try:
        # Load draw slots
        rows = conn.execute(
            """
            SELECT slot, team_id, pot
            FROM playoff_bracket_by_year
            WHERE season_year=? AND formula_version=? AND ruleset=?
            ORDER BY slot ASC
            """,
            (args.year, args.formula_version, args.ruleset),
        ).fetchall()

        if len(rows) != 24:
            raise SystemExit(f"Expected 24 bracket slots, got {len(rows)}. Run draw_playoff_year_2.py first.")

        slot_to_team = {int(r["slot"]): int(r["team_id"]) for r in rows}
        slot_to_pot = {int(r["slot"]): int(r["pot"]) for r in rows}

        pot1_slots = list(range(9, 17))
        pot2_slots = list(range(17, 25))

        for s in pot1_slots:
            if slot_to_pot.get(s) != 1:
                raise SystemExit(f"Slot {s} expected pot=1 but found pot={slot_to_pot.get(s)}")
        for s in pot2_slots:
            if slot_to_pot.get(s) != 2:
                raise SystemExit(f"Slot {s} expected pot=2 but found pot={slot_to_pot.get(s)}")

        pot1 = [slot_to_team[s] for s in pot1_slots]
        pot2 = [slot_to_team[s] for s in pot2_slots]

        conf_of = load_team_conferences(conn, args.year, args.formula_version, args.ruleset)

        # Preferred pairings preserve ceremony intent: 9v17, 10v18, ...
        preferred = pot2[:]  # aligned by index with pot1

        pairs = backtrack_pairings(pot1, pot2, conf_of, preferred)
        if not pairs:
            # This is very rare; means the field composition makes it impossible.
            # If you ever hit it, you can relax the rule or allow one intra-conf pairing.
            raise SystemExit("No valid Round of 24 pairing exists without same-conference matchups.")

        # Clear prior R24 games for deterministic reruns
        conn.execute(
            """
            DELETE FROM playoff_games_by_year
            WHERE season_year=? AND round='R24' AND formula_version=? AND ruleset=?
            """,
            (args.year, args.formula_version, args.ruleset),
        )

        # Persist games; homefield by CoE
        for i, (a_team, b_team) in enumerate(pairs, start=1):
            home, away = choose_home_away(conn, args.year, a_team, b_team, args.formula_version)

            # For debugging: store original slots/pots for each team in this matchup
            a_slot = pot1_slots[pot1.index(a_team)]
            b_slot = pot2_slots[pot2.index(b_team)]

            # If home is b_team (pot2), swap debug slot fields accordingly
            if home == a_team:
                home_slot, away_slot = a_slot, b_slot
                home_pot, away_pot = 1, 2
            else:
                home_slot, away_slot = b_slot, a_slot
                home_pot, away_pot = 2, 1

            conn.execute(
                """
                INSERT INTO playoff_games_by_year
                  (season_year, round, game_no,
                   home_team_id, away_team_id,
                   home_slot, away_slot,
                   home_pot, away_pot,
                   home_is_host_by,
                   formula_version, ruleset)
                VALUES (?, 'R24', ?, ?, ?, ?, ?, ?, ?, 'COE', ?, ?)
                """,
                (
                    args.year, i,
                    home, away,
                    home_slot, away_slot,
                    home_pot, away_pot,
                    args.formula_version, args.ruleset
                ),
            )

        conn.commit()
        print(f"Built Round of 24 games for {args.year}: {len(pairs)} games (no same-conference matchups; homefield by COE).")

    finally:
        conn.close()


if __name__ == "__main__":
    main()