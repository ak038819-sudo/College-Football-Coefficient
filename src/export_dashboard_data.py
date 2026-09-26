#!/usr/bin/env python3
"""
Exports everything the dashboard needs into one JSON file:
  - team ratings by season (all years with data)
  - team 5yr rolling CoE (years with a computed window)
  - conference ratings by season (years with membership data, detected dynamically)
  - conference 5yr rolling CoE
  - CoE 2.0 conference five-season (entering) values, in the canonical
    bid-allocation order, plus the bonus magnitudes they were built with
  - playoff field + Round-of-24 bracket draw for every season that has
    both membership data AND enough of it to actually build a 24-team
    field (a year can have membership but still fail, e.g. too few
    teams overall -- see the try/except around build_playoff_data)

Usage:
    python src/export_dashboard_data.py --draw-seed 1 --out ui/dashboard_data.json
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sqlite3
import unicodedata
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "coefficients"))
from select_playoff_field_v2 import (  # noqa: E402
    YEAR1_BIDS, YEAR2_BIDS, load_conference_coe_rank, load_team_coe_5yr,
    select_qualifiers, assign_pots, assign_homefield,
    get_independent_teams_with_coe, apply_independent_threshold,
)
from draw_playoff_bracket_v2 import (  # noqa: E402
    backtrack_pairings, choose_home_away, build_conf_map,
)
from simulate_bracket import run_simulation, DEFAULT_TEMPERATURE, DEFAULT_HOME_FIELD  # noqa: E402
sys.path.insert(0, str(Path(__file__).parent))
from build_coe2_rollups import conference_coe2_rank, load_bonus_config  # noqa: E402
import random

DATA_DIR = Path("data/processed")


def load_elo_by_season(db_path: str) -> dict:
    """
    Each team's END-OF-SEASON Elo rating: their postgame_elo from their
    LAST game (by date, game_id as tiebreak) within that season_year.
    elo_game_history is a per-GAME audit trail (build_elo.py, CoE 2.0
    Phase 2) with no season_year column of its own, so this joins
    through games for that and picks the season's final row per team
    via a window function. Elo itself is a single continuously-carried
    rating (unlike CoE's discrete per-season sums), so "end of season"
    is the natural snapshot for a season-by-season display -- there's
    no equivalent of CoE's 5yr rolling window for Elo, it's already a
    single running number.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT season_year, team_name, postgame_elo FROM (
            SELECT g.season_year, t.team_name, e.postgame_elo,
                   ROW_NUMBER() OVER (
                       PARTITION BY g.season_year, e.team_id
                       ORDER BY g.game_date DESC, e.game_id DESC
                   ) AS rn
            FROM elo_game_history e
            JOIN games g ON g.game_id = e.game_id
            JOIN teams t ON t.team_id = e.team_id
        )
        WHERE rn = 1
        """
    ).fetchall()
    conn.close()

    by_year = defaultdict(list)
    for r in rows:
        by_year[r["season_year"]].append({"team": r["team_name"], "elo": round(r["postgame_elo"], 1)})
    for year in by_year:
        by_year[year].sort(key=lambda x: -x["elo"])
    return dict(by_year)


def slugify(name: str) -> str:
    """
    URL slug for a team page (#team=<slug>). The dashboard's JS has a
    mirror of this function (slugify() in dashboard_shell.html) used only
    to resolve hand-typed URLs like #team=Indiana; tests/test_team_index.py
    and the Node check keep the two in agreement.
      "Miami (FL)" -> "miami-fl", "Texas A&M" -> "texas-am", "Hawai'i" -> "hawaii"
    """
    s = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    s = s.lower().replace("'", "").replace("&", "")
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-")


def build_team_index(db_path: str) -> list[dict]:
    """
    One small row per team for the team-page router: stable team_id (the
    internal key everywhere new), canonical team_name, URL slug, and the
    team's most recent conference in team_membership_by_season. Raises if
    two teams would share a slug, since that would make a URL ambiguous.
    """
    conn = sqlite3.connect(db_path)
    teams = conn.execute("SELECT team_id, team_name FROM teams ORDER BY team_name").fetchall()
    latest_conf = {
        tid: (yr, conf)
        for tid, yr, conf in conn.execute(
            """
            SELECT m.team_id, m.season_year, m.conference_real
            FROM team_membership_by_season m
            JOIN (
                SELECT team_id, MAX(season_year) AS y
                FROM team_membership_by_season
                WHERE conference_real IS NOT NULL
                GROUP BY team_id
            ) last ON last.team_id = m.team_id AND last.y = m.season_year
            """
        )
    }
    conn.close()

    index, seen = [], {}
    for tid, name in teams:
        slug = slugify(name)
        if slug in seen:
            raise ValueError(f"Slug collision: {name!r} and {seen[slug]!r} both -> {slug!r}")
        seen[slug] = name
        yr_conf = latest_conf.get(tid)
        index.append({
            "id": tid,
            "name": name,
            "slug": slug,
            "conference": yr_conf[1] if yr_conf else None,
            "conference_year": yr_conf[0] if yr_conf else None,
        })
    return index


def load_team_records_by_year(db_path: str) -> dict:
    """
    {season: {team_id: [wins, losses, ties]}} from COMPLETED games only
    (both scores present). The games table only holds FBS-vs-FBS games
    (load_games.py skips non-FBS opponents), so these are records vs FBS
    opponents and the dashboard labels them that way.
    """
    conn = sqlite3.connect(db_path)
    rec: dict = defaultdict(lambda: defaultdict(lambda: [0, 0, 0]))
    for y, h, a, hs, as_ in conn.execute(
        """
        SELECT season_year, home_team_id, away_team_id, home_score, away_score
        FROM games
        WHERE home_score IS NOT NULL AND away_score IS NOT NULL
        """
    ):
        if hs > as_:
            rec[y][h][0] += 1
            rec[y][a][1] += 1
        elif hs < as_:
            rec[y][a][0] += 1
            rec[y][h][1] += 1
        else:
            rec[y][h][2] += 1
            rec[y][a][2] += 1
    conn.close()
    return {str(y): {str(t): v for t, v in d.items()} for y, d in rec.items()}


def build_conference_board(db_path: str, membership_years: list[int]) -> dict:
    """
    {"season": S, "rows": [[conference, coeff_5yr], ...]} strongest first, for the
    latest season whose bid ranking can actually be computed (a season with no
    standings yet, e.g. before any games, falls back one year).
    """
    conn = sqlite3.connect(db_path)
    try:
        for season in sorted(membership_years, reverse=True)[:2]:
            ranked = load_conference_coe_rank(conn, season)
            if ranked:
                return {"season": season, "rows": [[c, round(v, 3)] for c, v in ranked]}
    finally:
        conn.close()
    return {"season": None, "rows": []}


def build_conference_coe2_5yr(db_path: str, version: str | None = None) -> dict:
    """
    {"<season>": [{conference, coe2_5yr, window, seasons_counted, external_games, rank}, ...]}
    strongest first: Conference CoE 2.0 over the seasons BEFORE that season -- the
    value ENTERING it, frozen (src/build_coe2_rollups.py, ENG-14). Early seasons
    carry a shorter window than five, which is why seasons_counted travels with
    every row.

    The ORDER and the exclusions are not decided here. They come from
    conference_coe2_rank(), which is the canonical bid-allocation contract
    ('FBS Independents' is not a league; a conference with no members that season
    cannot hold a rank in it). Everything else on the row is copied from
    conference_coe2_5yr_by_season so the window a number covers travels with it.

    ONE formula_version only -- the configured one. build_coe2_rollups.py keeps
    older versions' rows on purpose (that is what makes a v2 comparable against
    the v1 it supersedes), so an unfiltered read would list a conference once per
    version and pair its value with another version's window. A database whose
    rollups predate the configured version therefore exports nothing rather than
    a mixture: an absence the page reports honestly, where a mixture would be a
    wrong number nobody could spot.

    Empty until src/build_coe2_rollups.py has been run, which is why every caller
    must tolerate a missing season rather than render an absence as a zero.
    """
    conn = sqlite3.connect(db_path)
    try:
        if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND "
                            "name='conference_coe2_5yr_by_season'").fetchone():
            return {}
        rows = {(season, conf): rest for season, conf, *rest in conn.execute(
            """SELECT season_year, conference, window_start_year, window_end_year,
                      seasons_counted, external_games FROM conference_coe2_5yr_by_season
               WHERE formula_version = ?""", (version,))}
        out = {}
        for season in sorted({s for s, _ in rows}):
            ranked = conference_coe2_rank(conn, season, version)
            if not ranked:
                continue
            # Competition ranking (1, 2, 2, 4): two conferences on the same value
            # share a rank rather than being separated by float ordering.
            prev_value, prev_rank = None, 0
            season_rows = []
            for i, (conf, value) in enumerate(ranked, start=1):
                rank = prev_rank if value == prev_value else i
                prev_value, prev_rank = value, rank
                start, end, counted, external = rows[(season, conf)]
                season_rows.append({"conference": conf, "coe2_5yr": round(value, 3),
                                    "window": [start, end], "seasons_counted": counted,
                                    "external_games": external, "rank": rank})
            out[str(season)] = season_rows
        return out
    finally:
        conn.close()


def build_coe2_bonuses(config_path: Path = Path("config/model_config.json")) -> dict:
    """
    {"version", "values", "all_zero"} -- the bonus magnitudes a CoE 2.0 season
    total was built with.

    They all ship at 0.0 on purpose (see build_coe2_rollups.py), which means a
    season total is exactly the sum of its game awards. That is a fact a reader
    needs in order to read the number correctly, so it is exported rather than
    left as something the page has to assume.
    """
    if not config_path.exists():
        return {"version": None, "values": {}, "all_zero": True}
    cfg = json.loads(config_path.read_text(encoding="utf-8"))
    bonus = load_bonus_config(cfg.get("coe2_bonuses"))
    # Sorted: load_bonus_config builds its dict from a SET of category names, so
    # the insertion order changes between processes. Every other export in this
    # repo reproduces byte for byte on a rebuild, and the deploy job commits
    # "anything that actually changed" -- an unordered dict would hand it a
    # meaningless diff on every scheduled run.
    return {"version": bonus["version"], "values": dict(sorted(bonus["values"].items())),
            "all_zero": not any(bonus["values"].values())}


def build_polls(db_path: str, season) -> dict:
    """
    {"season": S, "ap": release|None, "cfp": release|None}, where release =
    {"season_type", "week", "rows": [[rank, team_id|None, school, first_place_votes|None, points|None]]}
    -- the latest release of each poll in `season` (postseason finals count as
    later than any regular-season week). A poll with no release this season is
    None, never an empty list, so the page can say "not released yet" honestly.
    """
    out = {"season": season, "ap": None, "cfp": None}
    if season is None:
        return out
    conn = sqlite3.connect(db_path)
    try:
        if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='poll_rankings'").fetchone():
            return out
        for poll in ("ap", "cfp"):
            latest = conn.execute(
                """SELECT season_type, week FROM poll_rankings WHERE season_year = ? AND poll = ?
                   ORDER BY (season_type = 'postseason') DESC, week DESC LIMIT 1""", (season, poll)).fetchone()
            if not latest:
                continue
            rows = conn.execute(
                """SELECT rank, team_id, school, first_place_votes, points FROM poll_rankings
                   WHERE season_year = ? AND poll = ? AND season_type = ? AND week = ?
                   ORDER BY rank, school""", (season, poll, latest[0], latest[1])).fetchall()
            out[poll] = {"season_type": latest[0], "week": latest[1], "rows": [list(r) for r in rows]}
    finally:
        conn.close()
    return out


def _build_upcoming(db_path: str) -> dict:
    from predict_upcoming import build_upcoming, elo_config
    conn = sqlite3.connect(db_path)
    try:
        return build_upcoming(conn, elo_config())
    finally:
        conn.close()


def years_with_membership_data(db_path: str) -> list[int]:
    """
    Which years actually have conference-membership data in the DB right
    now -- NOT a static range. This matters because the current season's
    membership can only come from a LIVE CFBD API fetch (a manual step
    with your own API key), which isn't captured in any committed file.
    A fresh --force rebuild (e.g. in CI, with no API key) will have games
    for the current year but zero membership rows for it, exactly like
    2010-2013 already correctly have no membership at all. Treating this
    dynamically means a year silently degrades to "ratings only, no
    playoff field" instead of crashing the whole export.
    """
    conn = sqlite3.connect(db_path)
    years = sorted(
        int(r[0])
        for r in conn.execute(
            "SELECT DISTINCT season_year FROM team_membership_by_season WHERE season_year >= 2014"
        )
    )
    conn.close()
    return years


def load_csv_by_year(filename: str, year_field: str) -> dict:
    out = defaultdict(list)
    with open(DATA_DIR / filename, newline="") as f:
        for r in csv.DictReader(f):
            out[int(r[year_field])].append(r)
    return out


def build_playoff_data(db_path: str, year: int, draw_seed: int, sims: int, temperature: float,
                       home_field: float = 0.0) -> dict:
    bid_table = YEAR1_BIDS if year == 2014 else YEAR2_BIDS
    conn = sqlite3.connect(db_path)
    conf_ranked = load_conference_coe_rank(conn, year)
    team_coe = load_team_coe_5yr(year)
    qualifiers = select_qualifiers(conn, year, conf_ranked, bid_table)
    qualifiers = assign_pots(qualifiers)
    assign_homefield(qualifiers, team_coe)
    independents = get_independent_teams_with_coe(conn, year, team_coe)
    qualifiers, replacements = apply_independent_threshold(qualifiers, independents)
    conn.close()

    byes = [q["team_name"] for q in qualifiers if q["pot"] == "bye"]
    pot1 = [q["team_name"] for q in qualifiers if q["pot"] == 1]
    pot2 = [q["team_name"] for q in qualifiers if q["pot"] == 2]

    conf_of = build_conf_map(qualifiers)
    rng = random.Random(draw_seed)
    pot1_drawn, pot2_drawn = pot1[:], pot2[:]
    rng.shuffle(pot1_drawn)
    rng.shuffle(pot2_drawn)
    pairs = backtrack_pairings(pot1_drawn, pot2_drawn, conf_of, pot2_drawn[:]) or []

    games = []
    for a, b in pairs:
        home, away = choose_home_away(a, b, team_coe)
        games.append({
            "home": home, "away": away,
            "home_conf": conf_of[home], "away_conf": conf_of[away],
            "home_coe": round(team_coe.get(home, 0.0), 3),
            "away_coe": round(team_coe.get(away, 0.0), 3),
        })

    # The odds redraw the Round of 24 every run, so they average over the draw
    # rather than describing the one bracket above. The bracket shown on this
    # page is `draw_seed`'s draw; a team's chances are not a property of it.
    counts, n_sims, sim_conf_of, sim_team_coe = run_simulation(
        db_path, year, draw_seed, sims, temperature, home_field=home_field)
    simulation = [
        {
            "team": team,
            "conference": sim_conf_of.get(team, ""),
            "coe": round(sim_team_coe.get(team, 0.0), 3),
            "r16_pct": round(100 * c["r16"] / n_sims, 1),
            "qf_pct": round(100 * c["qf"] / n_sims, 1),
            "sf_pct": round(100 * c["sf"] / n_sims, 1),
            "final_pct": round(100 * c["final"] / n_sims, 1),
            "champion_pct": round(100 * c["champion"] / n_sims, 1),
        }
        for team, c in counts.items()
    ]
    simulation.sort(key=lambda r: -r["champion_pct"])

    def sort_key(q):
        if q["conf_coe_rank"] is None:
            return (999, -q["team_coe_5yr"])
        return (q["conf_coe_rank"], q["conf_standing_rank"])

    return {
        "conference_ranking": [
            {"conference": c, "coeff_5yr": round(v, 3)} for c, v in conf_ranked
        ],
        "independents": [{"team": n, "coe": round(c, 3)} for n, c in independents],
        "independent_replacements": replacements,
        "simulation": simulation,
        # home_field ships so the page's own Simulate button uses the same venue
        # term as these odds, rather than keeping a second copy of it.
        "sim_meta": {"n_sims": n_sims, "temperature": temperature,
                     "home_field": home_field, "redraws": True},
        "qualifiers": [
            {
                "team": q["team_name"], "conference": q["conference"],
                "conf_coe_rank": q["conf_coe_rank"], "conf_standing_rank": q["conf_standing_rank"],
                "bid_type": q["bid_type"], "pot": str(q["pot"]),
                "team_coe_5yr": round(q["team_coe_5yr"], 3),
            }
            for q in sorted(qualifiers, key=sort_key)
        ],
        "byes": sorted(byes, key=lambda t: -team_coe.get(t, 0.0)),
        "round_of_24": games,
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--draw-seed", type=int, default=1)
    p.add_argument("--sims", type=int, default=10000)
    p.add_argument("--temperature", type=float, default=DEFAULT_TEMPERATURE)
    p.add_argument("--home-field", type=float, default=DEFAULT_HOME_FIELD)
    p.add_argument("--out", default="ui/dashboard_data.json")
    args = p.parse_args()

    team_ratings = load_csv_by_year("team_ratings_by_season.csv", "season_year")
    team_rolling = load_csv_by_year("team_coeff_5yr.csv", "end_year")
    conf_ratings = load_csv_by_year("conference_ratings_by_season.csv", "season_year")
    conf_rolling = load_csv_by_year("conference_coeff_5yr.csv", "end_year")
    team_elo = load_elo_by_season(args.db)

    membership_years = years_with_membership_data(args.db)
    # Read once: the same version labels the exported bonus magnitudes and selects
    # the rollup rows, so the two can never describe different formulas.
    coe2_bonuses = build_coe2_bonuses()

    def top(rows, key, n=None):
        rows_sorted = sorted(rows, key=lambda r: -float(r[key]))
        return rows_sorted if n is None else rows_sorted[:n]

    out = {
        "years_all": sorted(team_ratings.keys()),
        "years_playoff": membership_years,
        "team_ratings_by_year": {
            str(y): [{"team": r["team_name"], "rating": round(float(r["rating"]), 3)} for r in top(rows, "rating")]
            for y, rows in team_ratings.items()
        },
        "team_rolling_by_year": {
            str(y): [{"team": r["team_name"], "coeff_5yr": round(float(r["coeff_5yr"]), 3)} for r in top(rows, "coeff_5yr")]
            for y, rows in team_rolling.items()
        },
        "team_elo_by_year": {
            str(y): rows for y, rows in team_elo.items()
        },
        "conference_ratings_by_year": {
            str(y): sorted(
                [{"conference": r["conference_name"], "rating": round(float(r["rating"]), 3)} for r in rows],
                key=lambda x: -x["rating"],
            )
            for y, rows in conf_ratings.items()
        },
        "conference_rolling_by_year": {
            str(y): sorted(
                [{"conference": r["conference_name"], "coeff_5yr": round(float(r["coeff_5yr"]), 3)} for r in rows],
                key=lambda x: -x["coeff_5yr"],
            )
            for y, rows in conf_rolling.items()
        },
        "playoff_by_year": {},
        # Team-page router data (Milestone 1). Keyed by stable team_id; the
        # existing name-keyed exports above are left as-is.
        "teams": build_team_index(args.db),
        "team_records_by_year": load_team_records_by_year(args.db),
        # Milestone 3: not-yet-final games with Elo predictions from the engine's
        # own functions, plus the current Elo board they're based on.
        "upcoming": _build_upcoming(args.db),
        # Milestone 4 (homepage): conference leaderboard in the exact order that
        # sets playoff bids -- the same load_conference_coe_rank() the playoff
        # field uses, so defunct conferences can never appear.
        "conference_board": build_conference_board(args.db, membership_years),
        # CoE 2.0 conference five-season (entering) values + the bonus magnitudes
        # those totals were built with (ENG-14). Embedded rather than lazy-loaded
        # because both the rankings tab and the conference page need it, and it is
        # a few hundred rows. The live playoff model still runs on CoE v1; these
        # two must stay visibly separate, which is why they have separate keys.
        "conference_coe2_5yr_by_year": build_conference_coe2_5yr(args.db, coe2_bonuses["version"]),
        "coe2_bonuses": coe2_bonuses,
    }

    usable_years = []
    for year in membership_years:
        print(f"Building playoff data for {year}...")
        try:
            out["playoff_by_year"][str(year)] = build_playoff_data(
                args.db, year, args.draw_seed, args.sims, args.temperature, args.home_field
            )
            usable_years.append(year)
        except Exception as e:
            print(f"  SKIPPED {year}: {e!r} (membership present but field couldn't be built -- "
                  f"likely incomplete data for this year)")

    out["years_playoff"] = usable_years

    # Aggregate playoff appearances (and bye counts) across every simulated season
    appearances: dict[str, dict] = {}
    for year in usable_years:
        pf = out["playoff_by_year"][str(year)]
        for q in pf["qualifiers"]:
            rec = appearances.setdefault(q["team"], {"team": q["team"], "appearances": 0, "byes": 0, "years": []})
            rec["appearances"] += 1
            rec["years"].append(year)
            if q["pot"] == "bye":
                rec["byes"] += 1

    out["playoff_appearances"] = sorted(
        appearances.values(), key=lambda r: (-r["appearances"], -r["byes"], r["team"])
    )

    # Milestone 5: latest AP / CFP releases for the current season (display only).
    out["polls"] = build_polls(args.db, out["upcoming"]["season"])

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=None, separators=(",", ":")))
    print(f"\nWrote {out_path} ({out_path.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
