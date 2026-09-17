#!/usr/bin/env python3
from __future__ import annotations

import argparse
import random
import sqlite3
from typing import Dict, List, Tuple, Optional


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def get_conference_coe_ranks(conn: sqlite3.Connection, year: int, formula_version: str) -> Dict[str, int]:
    """
    Conference rank 1..N by rolling 5yr total points (ties broken deterministically).
    """
    rows = conn.execute(
        """
        SELECT conference, total_points_5yr, points_per_game_5yr
        FROM conference_coefficient_rolling_5yr
        WHERE season_year=? AND formula_version=?
        ORDER BY total_points_5yr DESC, points_per_game_5yr DESC, conference ASC
        """,
        (year, formula_version),
    ).fetchall()
    return {r["conference"]: i for i, r in enumerate(rows, start=1)}


def load_qualifiers(conn: sqlite3.Connection, year: int, formula_version: str, ruleset: str) -> List[sqlite3.Row]:
    return conn.execute(
        """
        SELECT conference, team_id, bid_type, conf_rank
        FROM playoff_qualifiers_by_year
        WHERE season_year=? AND formula_version=? AND ruleset=?
        """,
        (year, formula_version, ruleset),
    ).fetchall()


def get_conf_rank(conn: sqlite3.Connection, year: int, conference: str, team_id: int) -> Optional[int]:
    row = conn.execute(
        """
        SELECT conf_rank
        FROM conference_standings_by_year
        WHERE season_year=? AND conference=? AND team_id=?
        """,
        (year, conference, team_id),
    ).fetchone()
    return int(row["conf_rank"]) if row else None


def team_strength_key(conn: sqlite3.Connection, year: int, team_id: int, formula_version: str) -> Tuple[float, float]:
    """
    Returns (total_points_5yr, points_per_game_5yr). Missing => (0,0).
    """
    row = conn.execute(
        """
        SELECT total_points_5yr, points_per_game_5yr
        FROM team_coefficient_rolling_5yr
        WHERE season_year=? AND team_id=? AND formula_version=?
        """,
        (year, team_id, formula_version),
    ).fetchone()
    if not row:
        return (0.0, 0.0)
    return (float(row["total_points_5yr"]), float(row["points_per_game_5yr"]))


def get_team_name(conn: sqlite3.Connection, team_id: int) -> str:
    r = conn.execute("SELECT team_name FROM teams WHERE team_id=?", (team_id,)).fetchone()
    return r["team_name"] if r else f"team_id={team_id}"


def resolve_team_id_by_name(conn: sqlite3.Connection, team_name: str) -> Optional[int]:
    r = conn.execute("SELECT team_id FROM teams WHERE team_name=?", (team_name,)).fetchone()
    return int(r["team_id"]) if r else None


def get_pot_meta(
    conn: sqlite3.Connection,
    year: int,
    team_id: int,
    formula_version: str,
    ruleset: str,
) -> Tuple[int, int, str]:
    """
    Returns (conf_coe_rank, conf_rank, conference) from playoff_pots_by_year.
    """
    meta = conn.execute(
        """
        SELECT conference, conf_coe_rank, conf_rank
        FROM playoff_pots_by_year
        WHERE season_year=? AND team_id=? AND formula_version=? AND ruleset=?
        """,
        (year, team_id, formula_version, ruleset),
    ).fetchone()
    if not meta:
        return (999, 999, "UNKNOWN")
    return (int(meta["conf_coe_rank"]), int(meta["conf_rank"]), str(meta["conference"]))


def assign_pot_year2(conf_coe_rank: int, conf_finish: int) -> int:
    """
    Year 2+ rules (per your spec):

    Conf ranks 1-2:
      1st,2nd => BYE
      3rd => pot 1
      4th => pot 2

    Conf ranks 3-4:
      champ(1st) => BYE
      2nd,3rd => pot 1
      4th => pot 2

    Conf rank 5:
      champ => BYE
      2nd => pot 1
      3rd => pot 2

    Conf rank 6:
      champ => BYE
      (others => pot 2)

    Conf ranks 7-10:
      champ => pot 2 (special NIT exception handled separately)
    """
    r = conf_coe_rank
    f = conf_finish

    if r in (1, 2):
        if f in (1, 2):
            return 0
        if f == 3:
            return 1
        if f == 4:
            return 2
        return 2

    if r in (3, 4):
        if f == 1:
            return 0
        if f in (2, 3):
            return 1
        if f == 4:
            return 2
        return 2

    if r == 5:
        if f == 1:
            return 0
        if f == 2:
            return 1
        if f == 3:
            return 2
        return 2

    if r == 6:
        if f == 1:
            return 0
        return 2

    if 7 <= r <= 10:
        if f == 1:
            return 2
        return 2

    return 2


def rebalance_pots_to_8_8(
    conn: sqlite3.Connection,
    year: int,
    formula_version: str,
    ruleset: str,
    pot1: List[int],
    pot2: List[int],
) -> None:
    """
    Enforce pot1=8 and pot2=8 by:
      - promoting the STRONGEST team from pot2 -> pot1 (if pot1 short)
      - demoting the WEAKEST team from pot1 -> pot2 (if pot1 long)

    Strength criteria (transitional fairness):
      1) team total_points_5yr
      2) team points_per_game_5yr
      3) better conf_coe_rank (lower is better)
      4) better conf_rank (lower is better)
      5) stable: team_name
    """
    target = 8

    while len(pot1) < target and len(pot2) > target:
        candidates: List[Tuple[float, float, int, int, str, int]] = []
        for tid in pot2:
            tp5, ppg5 = team_strength_key(conn, year, tid, formula_version)
            conf_coe_rank, conf_rank, _conf = get_pot_meta(conn, year, tid, formula_version, ruleset)
            name = get_team_name(conn, tid)
            candidates.append((tp5, ppg5, -conf_coe_rank, -conf_rank, name, tid))

        candidates.sort(reverse=True)
        promote_tid = candidates[0][-1]

        pot2.remove(promote_tid)
        pot1.append(promote_tid)

        conn.execute(
            """
            UPDATE playoff_pots_by_year
            SET pot=1
            WHERE season_year=? AND team_id=? AND formula_version=? AND ruleset=?
            """,
            (year, promote_tid, formula_version, ruleset),
        )

    while len(pot1) > target and len(pot2) < target:
        candidates2: List[Tuple[float, float, int, int, str, int]] = []
        for tid in pot1:
            tp5, ppg5 = team_strength_key(conn, year, tid, formula_version)
            conf_coe_rank, conf_rank, _conf = get_pot_meta(conn, year, tid, formula_version, ruleset)
            name = get_team_name(conn, tid)
            candidates2.append((tp5, ppg5, conf_coe_rank, conf_rank, name, tid))

        candidates2.sort()
        demote_tid = candidates2[0][-1]

        pot1.remove(demote_tid)
        pot2.append(demote_tid)

        conn.execute(
            """
            UPDATE playoff_pots_by_year
            SET pot=2
            WHERE season_year=? AND team_id=? AND formula_version=? AND ruleset=?
            """,
            (year, demote_tid, formula_version, ruleset),
        )


def apply_nit_policy_overrides(
    conn: sqlite3.Connection,
    year: int,
    formula_version: str,
    ruleset: str,
    conf_ranks: Dict[str, int],
    nit_conference: Optional[str],
    nit_team_id: Optional[int],
    pot1: List[int],
    pot2: List[int],
) -> None:
    """
    Implements your Year2 NIT policy (as a hook/stub):
      - NIT team goes Pot 1 if its conference is ranked 1-6, else Pot 2.
      - If NIT conference is ranked 7-10, then conference #7 champion goes Pot 1 (instead of Pot 2).
        (We enforce this via move; rebalance later enforces 8/8.)
    """
    if not nit_conference and not nit_team_id:
        return

    # Derive nit_conference if only team provided.
    if nit_team_id and not nit_conference:
        row = conn.execute(
            """
            SELECT conference
            FROM playoff_pots_by_year
            WHERE season_year=? AND team_id=? AND formula_version=? AND ruleset=?
            """,
            (year, nit_team_id, formula_version, ruleset),
        ).fetchone()
        nit_conference = row["conference"] if row else None

    if not nit_conference:
        return

    nit_rank = conf_ranks.get(nit_conference, 999)

    # 1) Place NIT team into pot1 or pot2 by conference rank.
    if nit_team_id:
        desired_pot = 1 if nit_rank <= 6 else 2
        row = conn.execute(
            """
            SELECT pot
            FROM playoff_pots_by_year
            WHERE season_year=? AND team_id=? AND formula_version=? AND ruleset=?
            """,
            (year, nit_team_id, formula_version, ruleset),
        ).fetchone()
        if row:
            current_pot = int(row["pot"])
            if current_pot != desired_pot:
                if current_pot == 1 and nit_team_id in pot1:
                    pot1.remove(nit_team_id)
                if current_pot == 2 and nit_team_id in pot2:
                    pot2.remove(nit_team_id)

                if desired_pot == 1:
                    pot1.append(nit_team_id)
                else:
                    pot2.append(nit_team_id)

                conn.execute(
                    """
                    UPDATE playoff_pots_by_year
                    SET pot=?
                    WHERE season_year=? AND team_id=? AND formula_version=? AND ruleset=?
                    """,
                    (desired_pot, year, nit_team_id, formula_version, ruleset),
                )

    # 2) If NIT is in conf ranks 7-10, promote conf #7 champion to pot1.
    if 7 <= nit_rank <= 10:
        conf7 = None
        for c, r in conf_ranks.items():
            if r == 7:
                conf7 = c
                break

        if conf7:
            row = conn.execute(
                """
                SELECT team_id, pot
                FROM playoff_pots_by_year
                WHERE season_year=? AND conference=? AND conf_rank=1
                  AND formula_version=? AND ruleset=?
                """,
                (year, conf7, formula_version, ruleset),
            ).fetchone()

            if row:
                tid = int(row["team_id"])
                current_pot = int(row["pot"])
                if current_pot != 1:
                    if tid in pot2:
                        pot2.remove(tid)
                    if tid not in pot1:
                        pot1.append(tid)

                    conn.execute(
                        """
                        UPDATE playoff_pots_by_year
                        SET pot=1
                        WHERE season_year=? AND team_id=? AND formula_version=? AND ruleset=?
                        """,
                        (year, tid, formula_version, ruleset),
                    )


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--formula-version", default="v0")
    p.add_argument("--ruleset", default="year2")
    p.add_argument("--seed", type=int, default=20250101, help="RNG seed for reproducible draw")

    # NIT hook (optional): provide either conference name or team name (or both)
    p.add_argument("--nit-conference", default=None, help="NIT winner conference name (optional hook)")
    p.add_argument("--nit-team", default=None, help="NIT winner team_name from teams table (optional hook)")

    args = p.parse_args()
    rng = random.Random(args.seed)

    conn = connect(args.db)
    try:
        if args.ruleset != "year2":
            raise SystemExit("draw_playoff_year_2.py expects --ruleset year2")

        qualifiers = load_qualifiers(conn, args.year, args.formula_version, args.ruleset)
        if not qualifiers:
            raise SystemExit(
                f"No qualifiers found for year={args.year}, ruleset={args.ruleset}. "
                f"Run select_playoff_qualifiers_year_2.py first."
            )

        conf_ranks = get_conference_coe_ranks(conn, args.year, args.formula_version)

        # Clear prior outputs for deterministic reruns
        conn.execute(
            "DELETE FROM playoff_pots_by_year WHERE season_year=? AND formula_version=? AND ruleset=?",
            (args.year, args.formula_version, args.ruleset),
        )
        conn.execute(
            "DELETE FROM playoff_field_by_year WHERE season_year=? AND formula_version=? AND ruleset=?",
            (args.year, args.formula_version, args.ruleset),
        )
        conn.execute(
            "DELETE FROM playoff_bracket_by_year WHERE season_year=? AND formula_version=? AND ruleset=?",
            (args.year, args.formula_version, args.ruleset),
        )
        conn.execute(
            "DELETE FROM playoff_draws_by_year WHERE season_year=? AND formula_version=? AND ruleset=?",
            (args.year, args.formula_version, args.ruleset),
        )

        pot0: List[int] = []
        pot1: List[int] = []
        pot2: List[int] = []

        # Assign pots using Year2 rules and write staging pot rows
        for q in qualifiers:
            conf = str(q["conference"])
            team_id = int(q["team_id"])
            bid_type = str(q["bid_type"])

            conf_coe_rank = conf_ranks.get(conf, 999)

            conf_finish = get_conf_rank(conn, args.year, conf, team_id)
            if conf_finish is None:
                conf_finish = int(q["conf_rank"])

            pot = assign_pot_year2(conf_coe_rank, conf_finish)

            conn.execute(
                """
                INSERT OR REPLACE INTO playoff_pots_by_year
                  (season_year, team_id, conference, conf_rank, conf_coe_rank, pot, bid_type, formula_version, ruleset)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (args.year, team_id, conf, conf_finish, conf_coe_rank, pot, bid_type, args.formula_version, args.ruleset),
            )

            if pot == 0:
                pot0.append(team_id)
            elif pot == 1:
                pot1.append(team_id)
            else:
                pot2.append(team_id)

        if len(pot0) != 8:
            raise SystemExit(f"Year2 expects exactly 8 BYEs (pot0), got {len(pot0)}. Check qualifiers/rules.")

        # NIT hook application (optional)
        nit_team_id = resolve_team_id_by_name(conn, args.nit_team) if args.nit_team else None
        apply_nit_policy_overrides(
            conn,
            args.year,
            args.formula_version,
            args.ruleset,
            conf_ranks,
            args.nit_conference,
            nit_team_id,
            pot1,
            pot2,
        )

        # Balance pots to 8/8
        rebalance_pots_to_8_8(conn, args.year, args.formula_version, args.ruleset, pot1, pot2)
        if not (len(pot1) == 8 and len(pot2) == 8):
            raise SystemExit(
                f"Expected pot1=8 and pot2=8 after rebalance, got pot1={len(pot1)}, pot2={len(pot2)}."
            )

        # --- CANONICAL FIELD WRITE ---
        # Use the staging pot table as the authoritative source for conference/conf ranks/bid_type.
        conn.execute(
            """
            INSERT INTO playoff_field_by_year
              (season_year, team_id, conference, conf_rank, conf_coe_rank, bid_type, pot, formula_version, ruleset)
            SELECT
              season_year, team_id, conference, conf_rank, conf_coe_rank, bid_type, pot, formula_version, ruleset
            FROM playoff_pots_by_year
            WHERE season_year=? AND formula_version=? AND ruleset=?
            """,
            (args.year, args.formula_version, args.ruleset),
        )

        # --- DRAW METADATA WRITE ---
        conn.execute(
            """
            INSERT INTO playoff_draws_by_year
              (season_year, formula_version, ruleset, draw_seed)
            VALUES (?, ?, ?, ?)
            """,
            (args.year, args.formula_version, args.ruleset, args.seed),
        )

        # --- WORLD CUP CEREMONY DRAW (shuffle within each pot) ---
        rng.shuffle(pot0)
        rng.shuffle(pot1)
        rng.shuffle(pot2)

        # Slots: 1..8 byes, 9..16 pot1, 17..24 pot2
        bracket: List[Tuple[int, int, int]] = []
        slot = 1
        for tid in pot0:
            bracket.append((slot, tid, 0))
            slot += 1
        for tid in pot1:
            bracket.append((slot, tid, 1))
            slot += 1
        for tid in pot2:
            bracket.append((slot, tid, 2))
            slot += 1

        for slot, team_id, pot in bracket:
            conn.execute(
                """
                INSERT OR REPLACE INTO playoff_bracket_by_year
                  (season_year, slot, team_id, pot, formula_version, ruleset, draw_seed)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (args.year, slot, team_id, pot, args.formula_version, args.ruleset, args.seed),
            )

        conn.commit()

        print(f"Year 2 draw complete for {args.year} (seed={args.seed}).")
        print(f"Pot sizes: bye={len(pot0)}, pot1={len(pot1)}, pot2={len(pot2)}")
        if args.nit_conference or args.nit_team:
            print(f"NIT hook: nit_conference={args.nit_conference!r}, nit_team={args.nit_team!r}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()