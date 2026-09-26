#!/usr/bin/env python3
"""
Conference-page data export (Milestone E): writes ui/data/conference_pages.js,
loaded by the dashboard only when a conference page is first opened.

Why a .js file rather than .json: the same reason as team_pages.js -- browsers
block fetch() of local JSON when dashboard.html is opened straight from disk,
but a classic <script src> works from disk AND from GitHub Pages. The payload
is wrapped as `window.__CONFERENCE_PAGES__ = {...};`.

Every number a conference page shows is computed HERE, deterministically, from
the authoritative sources; the browser only selects a season and draws:
  team_membership_by_season          who was in the conference that season
  games                              results (completed games only)
  elo_game_history                   end-of-season Elo per member
  data/processed/team_ratings_by_season.csv     CoE v1 season rating per team
  data/processed/conference_ratings_by_season.csv / conference_coeff_5yr.csv
                                     CoE v1 conference season + 5-yr rolling
  conference_coe2_by_season          experimental Conference CoE 2.0
  conference_standings_by_year       derived standings rank (2014+ only)
  data/reference/national_champions.csv        real titles, never inferred

Record definitions (ONE rule, stated on the page):
  conference game  both teams in the SAME conference that season, any phase.
                   This matches conference_team_records_by_year's definition,
                   so 2014+ rows agree exactly (pinned by a test).
  external game    the opponent was in a DIFFERENT conference that season.
                   "How has this conference performed against the outside
                   world?" -- the question conference CoE exists to answer.
Conference CoE 2.0's own game set is deliberately NOT the same (see
build_conference_coe2.is_external_game: it treats EVERY postseason game as
external, including the rare same-conference bowl, which would give a
conference both a win and a loss from one game). So its rating and game count
are copied from conference_coe2_by_season rather than recounted here, and the
page states the difference instead of hiding it.

Nothing here feeds a rating engine: this is a display export, downstream of
every model, and it never recomputes a rating.

Shape (field lists travel with the data, as in the other exports):
  conferences   [{slug, name, first_season, last_season, seasons, active}]
  seasons       {slug: [season_row, ...]}  newest LAST (chronological)
  members       {slug: {season: [member_row, ...]}}  strongest CoE first
  vs            {slug: {season: {opponent_slug: [w, l, t]}}}
  vs_all        {slug: {opponent_slug: [w, l, t]}}
  totals        {slug: {...all-time rollups...}}

Usage:
    python src/export_conference_pages.py --db db/league.db
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent / "coefficients"))
from export_dashboard_data import slugify  # noqa: E402
from export_team_pages import CFP_FIRST_SEASON, load_national_champions, rank_desc  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
PROCESSED = REPO / "data" / "processed"
OUT_PATH = REPO / "ui" / "data" / "conference_pages.js"

# "FBS Independents" is a bucket of unaffiliated programs, not a league. It has no
# champion, no standings and no internal schedule, so the page labels it as such.
INDEPENDENTS = "FBS Independents"

SEASON_FIELDS = [
    "season", "members", "coe", "coe_rank", "coe_of",           # CoE v1, season
    "coe5", "coe5_rank", "coe5_of",                             # CoE v1, 5-yr rolling
    "coe2", "coe2_games",                                       # experimental CoE 2.0
    "ext_w", "ext_l", "ext_t",                                  # vs other conferences
    "conf_games",                                               # intra-conference games played
    "bowl_teams", "bowl_w", "bowl_l", "bowl_t",                 # postseason, real results
    "cfp_teams", "cfp_w", "cfp_l",
    "champion_id",                                              # derived standings #1, 2014+ only
    "title_ids",                                                # national titles won by members
]
MEMBER_FIELDS = [
    "team_id", "coe", "share", "conf_rank",
    "w", "l", "t", "conf_w", "conf_l", "conf_t", "ext_w", "ext_l", "ext_t",
    "end_elo", "games",
]


def load_memberships(conn: sqlite3.Connection) -> dict:
    """{(team_id, season): conference}. The only membership truth in the project."""
    return {(tid, season): conf for tid, season, conf in conn.execute(
        "SELECT team_id, season_year, conference_real FROM team_membership_by_season "
        "WHERE conference_real IS NOT NULL")}


def load_completed_games(conn: sqlite3.Connection) -> list[tuple]:
    return conn.execute(
        """
        SELECT game_id, season_year, home_team_id, away_team_id, home_score, away_score, game_phase
        FROM games
        WHERE home_score IS NOT NULL AND away_score IS NOT NULL
        ORDER BY season_year, game_date, game_id
        """
    ).fetchall()


def load_end_of_season_elo(conn: sqlite3.Connection) -> dict:
    """
    {(team_id, season): postgame Elo of that team's LAST game of the season}.
    The same snapshot export_dashboard_data.load_elo_by_season takes, so a
    conference page and the Elo rankings can never disagree.
    """
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='elo_game_history'").fetchone():
        return {}
    return {(tid, season): elo for tid, season, elo in conn.execute(
        """
        SELECT team_id, season_year, postgame_elo FROM (
            SELECT e.team_id, g.season_year, e.postgame_elo,
                   ROW_NUMBER() OVER (PARTITION BY g.season_year, e.team_id
                                      ORDER BY g.game_date DESC, e.game_id DESC) AS rn
            FROM elo_game_history e JOIN games g ON g.game_id = e.game_id
            WHERE g.home_score IS NOT NULL AND g.away_score IS NOT NULL
        ) WHERE rn = 1
        """)}


def load_team_ratings(conn: sqlite3.Connection, path: Path) -> dict:
    """
    {(team_id, season): CoE v1 season rating}, from build_coefficients.py's own
    output. Keyed by team_id here so a conference page never matches on a name.
    """
    if not path.exists():
        return {}
    ids = {name: tid for tid, name in conn.execute("SELECT team_id, team_name FROM teams")}
    out = {}
    with path.open(newline="", encoding="utf-8") as f:
        for n, r in enumerate(csv.DictReader(f), start=2):
            tid = ids.get(r["team_name"])
            if tid is None:
                raise ValueError(f"{path.name} line {n}: unknown team {r['team_name']!r}")
            out[(tid, int(r["season_year"]))] = float(r["rating"])
    return out


def load_conference_csv(path: Path, year_field: str, value_field: str) -> dict:
    """{(conference, season): value} from one of build_coefficients.py's CSVs."""
    if not path.exists():
        return {}
    with path.open(newline="", encoding="utf-8") as f:
        return {(r["conference_name"], int(r[year_field])): float(r[value_field]) for r in csv.DictReader(f)}


def load_conference_coe2(conn: sqlite3.Connection) -> dict:
    """{(conference, season): (conference_coe2, external_games_counted)}; empty before it's built."""
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='conference_coe2_by_season'").fetchone():
        return {}
    return {(conf, season): (coe, games) for conf, season, coe, games in conn.execute(
        "SELECT conference, season_year, conference_coe2, external_games_counted FROM conference_coe2_by_season")}


def load_champions_by_conference(conn: sqlite3.Connection) -> dict:
    """
    {(conference, season): [team_id, ...]} of derived standings winners (conf_rank 1).
    conference_standings_by_year only covers the seasons the playoff model needs
    (2014+), so earlier seasons simply have no champion -- never a guessed one.
    """
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='conference_standings_by_year'").fetchone():
        return {}
    out = defaultdict(list)
    for season, conf, tid in conn.execute(
            "SELECT season_year, conference, team_id FROM conference_standings_by_year WHERE conf_rank = 1"):
        out[(conf, season)].append(tid)
    return dict(out)


def load_standings_ranks(conn: sqlite3.Connection) -> dict:
    """{(team_id, season): conf_rank} from the derived standings (2014+ only)."""
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='conference_standings_by_year'").fetchone():
        return {}
    return {(tid, season): rank for season, tid, rank in conn.execute(
        "SELECT season_year, team_id, conf_rank FROM conference_standings_by_year")}


def _blank() -> dict:
    return {"w": 0, "l": 0, "t": 0}


def _add(rec: dict, result: str) -> None:
    rec["w" if result == "W" else "l" if result == "L" else "t"] += 1


def _result(mine: int, theirs: int) -> str:
    return "W" if mine > theirs else "L" if mine < theirs else "T"


def build_conference_pages(conn: sqlite3.Connection, champions: list[dict] | None = None,
                           processed: Path | None = None) -> dict:
    """`processed` is build_coefficients.py's output directory (data/processed);
    tests point it at a fixture directory so a synthetic league stays synthetic."""
    processed = processed or PROCESSED
    membership = load_memberships(conn)
    games = load_completed_games(conn)
    team_ratings = load_team_ratings(conn, processed / "team_ratings_by_season.csv")
    elo = load_end_of_season_elo(conn)
    conf_season_coe = load_conference_csv(processed / "conference_ratings_by_season.csv", "season_year", "rating")
    conf_rolling_coe = load_conference_csv(processed / "conference_coeff_5yr.csv", "end_year", "coeff_5yr")
    coe2 = load_conference_coe2(conn)
    champion_of = load_champions_by_conference(conn)
    standings_rank = load_standings_ranks(conn)

    # ---- who was where, per season ----
    members_of: dict = defaultdict(set)          # (conf, season) -> {team_id}
    for (tid, season), conf in membership.items():
        members_of[(conf, season)].add(tid)

    # ---- walk every completed game once, from both teams' points of view ----
    team_rec: dict = defaultdict(_blank)         # (tid, season) -> overall vs FBS
    team_conf_rec: dict = defaultdict(_blank)    # ... intra-conference
    team_ext_rec: dict = defaultdict(_blank)     # ... vs other conferences
    conf_ext_rec: dict = defaultdict(_blank)     # (conf, season)
    conf_games: dict = defaultdict(int)          # (conf, season) -> intra-conference games played
    vs_rec: dict = defaultdict(lambda: defaultdict(_blank))   # (conf, season) -> opp_conf -> record
    bowl: dict = defaultdict(_blank)             # (conf, season)
    bowl_teams: dict = defaultdict(set)
    cfp: dict = defaultdict(_blank)
    cfp_teams: dict = defaultdict(set)

    for gid, season, home, away, hs, as_, phase in games:
        for tid, opp, mine, theirs in ((home, away, hs, as_), (away, home, as_, hs)):
            conf = membership.get((tid, season))
            if conf is None:
                continue                          # no membership row: nothing to credit
            opp_conf = membership.get((opp, season))
            res = _result(mine, theirs)
            _add(team_rec[(tid, season)], res)
            if opp_conf is not None and opp_conf == conf:
                _add(team_conf_rec[(tid, season)], res)
                conf_games[(conf, season)] += 1   # counted once per member, i.e. twice per game
            else:
                _add(team_ext_rec[(tid, season)], res)
                _add(conf_ext_rec[(conf, season)], res)
                _add(vs_rec[(conf, season)][opp_conf], res)
            # Real postseason history. Pre-2014 "cfp"-phase games are BCS-era title
            # bowls, exactly as the team page counts them.
            if phase == "cfp" and season >= CFP_FIRST_SEASON:
                _add(cfp[(conf, season)], res)
                cfp_teams[(conf, season)].add(tid)
            elif phase in ("bowl", "cfp"):
                _add(bowl[(conf, season)], res)
                bowl_teams[(conf, season)].add(tid)

    # ---- national titles won by a team while it was a member ----
    # A SET, not a list: load_national_champions returns one row per SYSTEM, so a
    # unanimous champion (AP and Coaches agreeing) has two awarded rows for one
    # title. Counting distinct (season, team) keeps a conference's tally on the
    # same footing as export_team_pages.title_history's distinct-season count,
    # while still crediting two different members in a genuinely split season.
    titles_of: dict = defaultdict(set)
    for c in champions or []:
        if c["status"] != "awarded":
            continue
        conf = membership.get((c["team_id"], c["season"]))
        if conf is not None:
            titles_of[(conf, c["season"])].add(c["team_id"])

    # ---- ranks: among the conferences that actually HAVE members that season ----
    season_rank, season_of_n, rolling_rank, rolling_of_n = {}, {}, {}, {}
    for season in {s for (_, s) in members_of}:
        live = {c for (c, s) in members_of if s == season}
        vals = {c: conf_season_coe[(c, season)] for c in live if (c, season) in conf_season_coe}
        season_of_n[season] = len(vals)
        season_rank.update({(c, season): r for c, r in rank_desc(vals).items()})
        roll = {c: conf_rolling_coe[(c, season)] for c in live if (c, season) in conf_rolling_coe}
        rolling_of_n[season] = len(roll)
        rolling_rank.update({(c, season): r for c, r in rank_desc(roll).items()})

    # ---- assemble, one conference at a time ----
    names = sorted({c for (c, _) in members_of})
    slugs = {}
    for name in names:
        slug = slugify(name)
        if slug in slugs:
            raise ValueError(f"conference slug collision: {name!r} and {slugs[slug]!r} both slugify to {slug!r}")
        slugs[slug] = name

    conferences, seasons_out, members_out, vs_out, vs_all_out, totals_out = [], {}, {}, {}, {}, {}
    latest_season = max((s for (_, s) in members_of), default=None)
    for name in names:
        slug = slugify(name)
        yrs = sorted(s for (c, s) in members_of if c == name)
        rows, member_rows, vs_by_season = [], {}, {}
        all_ext, all_bowl, all_cfp = _blank(), _blank(), _blank()
        all_vs: dict = defaultdict(_blank)
        title_total, bowl_team_total, cfp_team_total = 0, 0, 0
        for season in yrs:
            ids = members_of[(name, season)]
            shares = {tid: team_ratings.get((tid, season)) for tid in ids}
            total = sum(v for v in shares.values() if v is not None)
            mrows = []
            for tid in ids:
                r = team_rec[(tid, season)]
                cr = team_conf_rec[(tid, season)]
                er = team_ext_rec[(tid, season)]
                rating = shares[tid]
                mrows.append([
                    tid,
                    None if rating is None else round(rating, 3),
                    None if rating is None or total <= 0 else round(100 * rating / total, 2),
                    standings_rank.get((tid, season)),
                    r["w"], r["l"], r["t"], cr["w"], cr["l"], cr["t"], er["w"], er["l"], er["t"],
                    None if (tid, season) not in elo else round(elo[(tid, season)], 1),
                    r["w"] + r["l"] + r["t"],
                ])
            # Strongest CoE first; teams with no rating (no completed games) last, by id.
            mrows.sort(key=lambda m: (m[1] is None, -(m[1] or 0), m[0]))
            member_rows[str(season)] = mrows

            ext, bw, cf = conf_ext_rec[(name, season)], bowl[(name, season)], cfp[(name, season)]
            c2 = coe2.get((name, season))
            rows.append([
                season, len(ids),
                round(conf_season_coe[(name, season)], 3) if (name, season) in conf_season_coe else None,
                season_rank.get((name, season)), season_of_n.get(season),
                round(conf_rolling_coe[(name, season)], 3) if (name, season) in conf_rolling_coe else None,
                rolling_rank.get((name, season)), rolling_of_n.get(season),
                None if c2 is None else round(c2[0], 3), None if c2 is None else c2[1],
                ext["w"], ext["l"], ext["t"],
                conf_games[(name, season)] // 2,          # de-duplicate: both members counted it
                len(bowl_teams[(name, season)]), bw["w"], bw["l"], bw["t"],
                len(cfp_teams[(name, season)]), cf["w"], cf["l"],
                (champion_of.get((name, season)) or [None])[0] if name != INDEPENDENTS else None,
                sorted(titles_of.get((name, season), ())),
            ])
            season_vs = {}
            for opp, rec in vs_rec.get((name, season), {}).items():
                # "" = an opponent with no membership row that season, so no conference to name.
                key = slugify(opp) if opp is not None else ""
                season_vs[key] = [rec["w"], rec["l"], rec["t"]]
                for k in ("w", "l", "t"):
                    all_vs[key][k] += rec[k]
            vs_by_season[str(season)] = dict(sorted(season_vs.items()))
            for src, dst in ((ext, all_ext), (bw, all_bowl), (cf, all_cfp)):
                for k in ("w", "l", "t"):
                    dst[k] += src[k]
            title_total += len(titles_of.get((name, season), ()))
            bowl_team_total += len(bowl_teams[(name, season)])
            cfp_team_total += len(cfp_teams[(name, season)])

        best = min((r for r in rows if r[3] is not None), key=lambda r: (r[3], -r[0]), default=None)
        peak = max((r for r in rows if r[2] is not None), key=lambda r: (r[2], r[0]), default=None)
        conferences.append({"slug": slug, "name": name, "first_season": yrs[0], "last_season": yrs[-1],
                            "seasons": len(yrs), "active": yrs[-1] == latest_season,
                            "is_independents": name == INDEPENDENTS})
        seasons_out[slug] = rows
        members_out[slug] = member_rows
        vs_out[slug] = vs_by_season
        vs_all_out[slug] = dict(sorted({k: [v["w"], v["l"], v["t"]] for k, v in all_vs.items()}.items()))
        totals_out[slug] = {
            "ext": [all_ext["w"], all_ext["l"], all_ext["t"]],
            "bowl": [bowl_team_total, all_bowl["w"], all_bowl["l"], all_bowl["t"]],
            "cfp": [cfp_team_total, all_cfp["w"], all_cfp["l"]],
            "titles": title_total,
            "members_ever": len({tid for (tid, _), c in membership.items() if c == name}),
            "best_rank": None if best is None else [best[0], best[3], best[4]],
            "peak_coe": None if peak is None else [peak[0], peak[2]],
        }

    return {
        "season_fields": SEASON_FIELDS,
        "member_fields": MEMBER_FIELDS,
        "independents": INDEPENDENTS,
        "cfp_first_season": CFP_FIRST_SEASON,
        "standings_since": min((s for (_, s) in standings_rank), default=None),
        "coe2_since": min((s for (_, s) in coe2), default=None),
        "conferences": conferences,
        "seasons": seasons_out,
        "members": members_out,
        "vs": vs_out,
        "vs_all": vs_all_out,
        "totals": totals_out,
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default=str(REPO / "db" / "league.db"))
    p.add_argument("--out", default=str(OUT_PATH))
    args = p.parse_args()

    conn = sqlite3.connect(args.db)
    data = build_conference_pages(conn, load_national_champions(conn))
    conn.close()

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("window.__CONFERENCE_PAGES__=" + json.dumps(data, separators=(",", ":")) + ";\n",
                   encoding="utf-8")
    seasons = sum(len(v) for v in data["seasons"].values())
    print(f"Wrote {out} ({out.stat().st_size:,} bytes): {len(data['conferences'])} conferences, "
          f"{seasons} conference-seasons")


if __name__ == "__main__":
    main()
