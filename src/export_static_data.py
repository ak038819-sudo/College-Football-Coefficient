#!/usr/bin/env python3
"""
Per-season game files + search index (data foundation step).

The foundation the Games section, game pages and global search build on:

  ui/data/games/<season>.js   every game of one season, completed AND scheduled,
                              oldest first, loaded only when that season is needed
  ui/data/search_index.js     teams (+ aliases), seasons and every game, compactly,
                              loaded on the first search
  ui/data/static_manifest.json  seasons, counts and content versions; embedded in
                              dashboard.html so the page knows what exists without
                              downloading it

Files are `window.__CFB__...=` scripts rather than bare JSON for the same reason as
team_pages.js: browsers block fetch() of local JSON when dashboard.html is opened
from disk, but a <script src> still works there and on GitHub Pages.

Every value is copied from an authoritative table; nothing is recomputed:
  completed games  pregame Elo, pregame expectation (home field included, exactly as
                   the Elo engine predicted), postgame Elo and change: elo_game_history.
                   Game CoE: hybrid_game_ratings (CoE 2.0; null before it exists).
  scheduled games  current ratings and prediction from predict_upcoming.build_upcoming
                   (the engine's own functions). No score, no postgame Elo, no Game CoE
                   -- a scheduled game can't have them.
`date` is the database's game_date (the UTC date of CFBD's start time, which also
orders games for Elo and is never changed here). `kickoff_utc` is CFBD's full start
time -- for scheduled games always, for completed games once fetch_kickoffs.py has
been run -- and `time_tbd` marks a placeholder clock time. Pages show the date as
US Eastern from kickoff_utc when it exists (so a late Saturday game reads Saturday),
otherwise `date`.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
import shutil
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from coverage import build_coverage  # noqa: E402
from predict_upcoming import build_upcoming, elo_config  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
OUT_DIR = REPO / "ui" / "data"
PHASE_CODE = {"regular": 0, "bowl": 1, "cfp": 2}
FIELDS = ["game_id", "week", "date", "kickoff_utc", "completed", "phase", "neutral", "ot",
          "home_id", "away_id", "home_score", "away_score",
          "home_pre_elo", "away_pre_elo", "p_home", "home_post_elo", "away_post_elo", "home_elo_change",
          "home_game_coe", "away_game_coe", "home_provisional", "away_provisional",
          "time_tbd"]      # appended last so existing positions never move
SEARCH_GAME_FIELDS = ["game_id", "season", "home_id", "away_id"]

# Game pages (Milestone C). Loaded only when a game page opens.
DETAIL_FIELDS = ["game_id",
                 "home_elo_z", "home_coe_z", "home_hybrid_rating", "home_hybrid_p", "home_result",
                 "away_elo_z", "away_coe_z", "away_hybrid_rating", "away_hybrid_p", "away_result",
                 # xSRDiff performance layer (EXP-03), appended LAST so existing
                 # positions never move. Null where the game has no per-game
                 # Success Rate -- never 0, which would be a real claim.
                 "home_success_rate", "home_sr_diff", "home_xsr_diff", "home_sr_plus",
                 "away_success_rate", "away_sr_diff", "away_xsr_diff", "away_sr_plus",
                 "performance_model"]
SERIES_FIELDS = ["game_id", "season", "date", "kickoff_utc", "home_id", "away_id", "home_score", "away_score",
                 "p_home", "home_elo_change", "home_pre_elo", "away_pre_elo", "phase", "neutral", "ot"]
SERIES_SHARDS = 32


def series_shard(a: int, b: int) -> int:
    """Which series file holds a team pair. ui/dashboard_shell.html computes the same thing."""
    lo, hi = sorted((int(a), int(b)))
    return (lo * 131 + hi) % SERIES_SHARDS


def kickoff_map(conn: sqlite3.Connection) -> dict:
    """{game_id: (kickoff_utc or None, time_tbd)}. Date-only seasons yield (None, 1): their
    midnight-UTC stamp is not a time (see sql/kickoff_tables.sql). Shared by every export."""
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE name='game_kickoffs'").fetchone():
        return {}
    cols = {r[1] for r in conn.execute("PRAGMA table_info(game_kickoffs)")}
    return {g: ((None, 1) if d else (k, tbd)) for g, k, tbd, d in conn.execute(
        "SELECT game_id, kickoff_utc, time_tbd, " + ("date_only" if "date_only" in cols else "0") + " FROM game_kickoffs")}


def _r(v, nd):
    return None if v is None else round(v, nd)


def build_season_payloads(conn: sqlite3.Connection, upcoming: dict) -> dict:
    """{season: payload}. Pure with respect to the database contents + the upcoming export."""
    elo = {(g, t): (pre, exp, chg, post) for g, t, pre, exp, chg, post in conn.execute(
        "SELECT game_id, team_id, pregame_elo, elo_expectation, elo_change, postgame_elo FROM elo_game_history")}
    has_hybrid = conn.execute("SELECT 1 FROM sqlite_master WHERE name='hybrid_game_ratings'").fetchone()
    coe = {(g, t): c for g, t, c in conn.execute("SELECT game_id, team_id, game_coe FROM hybrid_game_ratings")} \
        if has_hybrid else {}
    kickoff = kickoff_map(conn)
    rows = defaultdict(list)

    for (gid, season, week, date, home, away, hs, as_, neutral, ot, phase) in conn.execute(
            """SELECT game_id, season_year, week, game_date, home_team_id, away_team_id, home_score, away_score,
                      neutral_site, went_ot, game_phase FROM games WHERE home_score IS NOT NULL"""):
        he, ae = elo.get((gid, home)), elo.get((gid, away))
        ko, ko_tbd = kickoff.get(gid, (None, None))                 # display only; null when not fetched
        rows[season].append([
            gid, week, str(date)[:10], ko, 1, PHASE_CODE.get(phase, 0), int(bool(neutral)), int(bool(ot)),
            home, away, hs, as_,
            _r(he[0], 1) if he else None, _r(ae[0], 1) if ae else None, _r(he[1], 4) if he else None,
            _r(he[3], 1) if he else None, _r(ae[3], 1) if ae else None, _r(he[2], 2) if he else None,
            _r(coe.get((gid, home)), 3), _r(coe.get((gid, away)), 3), 0, 0, ko_tbd])

    for g in upcoming.get("games", []):
        gid, season, week, kickoff, _tbd, home, away, neutral, phase, h_elo, a_elo, p, h_prov, a_prov = g
        rows[season].append([
            gid, week, (kickoff or "1900-01-01")[:10], kickoff, 0, phase, neutral, 0, home, away, None, None,
            h_elo, a_elo, p, None, None, None, None, None, h_prov, a_prov, int(bool(_tbd))])

    conf = defaultdict(dict)
    for tid, season, c in conn.execute("SELECT team_id, season_year, conference_real FROM team_membership_by_season"):
        conf[season][str(tid)] = c

    payloads = {}
    for season, rs in rows.items():
        rs.sort(key=lambda r: (r[2], r[3] or "", r[0]))            # date, kickoff, game_id
        teams = {str(t) for r in rs for t in (r[8], r[9])}
        payloads[season] = {"season": season, "fields": FIELDS,
                            "phase_codes": {v: k for k, v in PHASE_CODE.items()},
                            "game_coe_model": "CoE 2.0 (hybrid)",
                            "conferences": {t: conf[season].get(t) for t in sorted(teams, key=int)},
                            "games": rs}
    return payloads


def build_game_details(conn: sqlite3.Connection) -> dict:
    """
    {season: payload} of each completed game's stored CoE 2.0 inputs, per side: the Elo and
    frozen 5-yr CoE z-scores, the hybrid rating, the hybrid expectation P (home field
    included, exactly as the model used it) and the result type. Copied from
    hybrid_game_ratings; the page shows the formula with these numbers, it never recomputes.
    """
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE name='hybrid_game_ratings'").fetchone():
        return {}
    side = {(g, t): (ez, cz, hr, p, rt) for g, t, ez, cz, hr, p, rt in conn.execute(
        "SELECT game_id, team_id, elo_z, coe_z, hybrid_rating, hybrid_expectation, result_type FROM hybrid_game_ratings")}
    # The Elo engine's own stored performance-layer values (src/srdiff.py). Copied,
    # never recomputed here -- and never recomputed in the browser either.
    elo_cols = {r[1] for r in conn.execute("PRAGMA table_info(elo_game_history)")}
    sr_side = {}
    if {"sr_diff", "xsr_diff", "sr_plus", "performance_model"} <= elo_cols:
        sr_side = {(g, t): (sr, d, x, plus, model) for g, t, sr, d, x, plus, model in conn.execute(
            "SELECT game_id, team_id, success_rate_team, sr_diff, xsr_diff, sr_plus, performance_model "
            "FROM elo_game_history")}
    cfg = json.loads((REPO / "config" / "model_config.json").read_text(encoding="utf-8"))
    params = {k: v for sec in ("coe", "hybrid") for k, v in cfg.get(sec, {}).items() if not k.startswith("_")}
    out = defaultdict(list)
    for gid, season, home, away in conn.execute(
            "SELECT game_id, season_year, home_team_id, away_team_id FROM games WHERE home_score IS NOT NULL"):
        h, a = side.get((gid, home)), side.get((gid, away))
        if not h or not a:
            continue                                     # e.g. the 1980-84 bootstrap: no CoE 2.0 yet
        r = lambda v: None if v is None else round(v, 4)
        hs_sr = sr_side.get((gid, home), (None, None, None, None, None))
        as_sr = sr_side.get((gid, away), (None, None, None, None, None))
        out[season].append([gid, r(h[0]), r(h[1]), r(h[2]), r(h[3]), h[4], r(a[0]), r(a[1]), r(a[2]), r(a[3]), a[4],
                            r(hs_sr[0]), r(hs_sr[1]), r(hs_sr[2]), r(hs_sr[3]),
                            r(as_sr[0]), r(as_sr[1]), r(as_sr[2]), r(as_sr[3]),
                            hs_sr[4] or as_sr[4]])
    return {s: {"season": s, "fields": DETAIL_FIELDS, "params": params, "games": rows} for s, rows in out.items()}


def build_series(payloads: dict) -> dict:
    """{shard: {"lo-hi": [meeting rows, oldest first]}} over every completed game in the season files."""
    i = {f: n for n, f in enumerate(FIELDS)}
    shards = defaultdict(lambda: defaultdict(list))
    for season in sorted(payloads):
        for r in payloads[season]["games"]:
            if not r[i["completed"]]:
                continue
            h, a = r[i["home_id"]], r[i["away_id"]]
            lo, hi = sorted((h, a))
            shards[series_shard(h, a)][f"{lo}-{hi}"].append(
                [r[i["game_id"]], season] + [r[i[f]] for f in SERIES_FIELDS[2:]])
    return {k: {"shard": k, "fields": SERIES_FIELDS, "pairs": dict(v)} for k, v in shards.items()}


def _code_constants(path: Path, names: list) -> dict:
    """Values of simple NAME = <literal> assignments anywhere in a module, read from its syntax
    tree (never imported, never hand-copied) -- so the methodology page shows what the code uses."""
    out = {}
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            name = node.targets[0].id
            if name in names and name not in out:
                try:
                    out[name] = ast.literal_eval(node.value)
                except ValueError:
                    pass
    return out


def _argparse_default(path: Path, flag: str):
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Call) and node.args and isinstance(node.args[0], ast.Constant) and node.args[0].value == flag:
            for kw in node.keywords:
                if kw.arg == "default":
                    return ast.literal_eval(kw.value)
    return None


def model_params() -> dict:
    """The live parameters behind every number on the site, for the methodology page."""
    cfg = json.loads((REPO / "config" / "model_config.json").read_text(encoding="utf-8"))
    clean = lambda d: {k: v for k, v in (d or {}).items() if not k.startswith("_")}
    v1_path = REPO / "src" / "build_coefficients.py"
    v1 = _code_constants(v1_path, ["ITERATIONS", "PHASE_WEIGHTS", "ROLLING_YEARS", "USE_WITHIN_WINDOW_DECAY",
                                   "WITHIN_WINDOW_DECAY_BASE", "CONFIDENCE_GAMES", "LOSS_PENALTY"])
    v1["PRIOR_REGRESSION"] = _argparse_default(v1_path, "--prior-regression")
    bids = _code_constants(REPO / "src" / "coefficients" / "select_playoff_field_v2.py", ["YEAR2_BIDS"]).get("YEAR2_BIDS", {})
    return {"elo": clean(cfg.get("elo")), "hybrid": clean(cfg.get("hybrid")), "coe2": clean(cfg.get("coe")),
            "coe_v1": v1, "playoff_bids": [[lo, hi, n] for (lo, hi), n in sorted(bids.items())],
            # EXP-03: which performance multiplier the ratings on this page were built with.
            "performance": clean(cfg.get("performance"))}


# How people actually type a conference. Canonical name -> the other names that should
# find it; the conference itself is always matched by its own name and slug, so these are
# only the shorthands that are not substrings of it. Every key is checked against the
# database by tests/test_static_assets.py, so a renamed conference cannot leave a dead alias.
CONFERENCE_ALIASES = {
    "American Athletic": ["AAC", "The American", "American"],
    "Big Ten": ["B1G", "Big 10"],
    "Conference USA": ["C-USA", "CUSA"],
    "FBS Independents": ["Independents", "Independent"],
    "Mid-American": ["MAC"],
    "Mountain West": ["MWC", "Mountain West Conference"],
    "Pac-10": ["Pacific-10", "PAC 10"],
    "Pac-12": ["Pacific-12", "PAC 12"],
    "Southern": ["SoCon"],
    "Southwest": ["SWC", "Southwest Conference"],
    "Western Athletic": ["WAC"],
}


def build_conference_search_rows(conn: sqlite3.Connection) -> list:
    """
    [[slug, name, [aliases]], ...] for every conference that ever had members, so the
    header search can reach a conference page the same way it reaches a team page.
    Season coverage is not repeated here: the dashboard already knows it from the
    embedded conference ratings.
    """
    from export_dashboard_data import slugify
    names = sorted(r[0] for r in conn.execute(
        "SELECT DISTINCT conference_real FROM team_membership_by_season WHERE conference_real IS NOT NULL"))
    return [[slugify(n), n, CONFERENCE_ALIASES.get(n, [])] for n in names]


def build_search_index(conn: sqlite3.Connection, payloads: dict) -> dict:
    names = dict(conn.execute("SELECT team_id, team_name FROM teams"))
    aliases = defaultdict(list)
    for alias, canonical in conn.execute("SELECT alias, team_name FROM team_aliases ORDER BY alias"):
        tid = next((t for t, n in names.items() if n == canonical), None)
        if tid is not None:
            aliases[tid].append(alias)
    from export_dashboard_data import slugify
    teams = [[tid, name, slugify(name), aliases.get(tid, [])] for tid, name in sorted(names.items(), key=lambda x: x[1])]
    games = [[r[0], season, r[8], r[9]] for season in sorted(payloads) for r in payloads[season]["games"]]
    return {"teams": teams, "seasons": sorted(payloads), "game_fields": SEARCH_GAME_FIELDS, "games": games,
            "conferences": build_conference_search_rows(conn)}


def _write_js(path: Path, global_expr: str, payload) -> str:
    body = json.dumps(payload, separators=(",", ":"))
    path.write_text(f"{global_expr}={body};\n", encoding="utf-8")
    return hashlib.sha256(path.read_bytes()).hexdigest()[:10]


def export(conn: sqlite3.Connection, out_dir: Path = OUT_DIR) -> dict:
    payloads = build_season_payloads(conn, build_upcoming(conn, elo_config()))
    games_dir = out_dir / "games"
    if games_dir.exists():
        shutil.rmtree(games_dir)                                     # no orphan seasons
    games_dir.mkdir(parents=True)
    seasons = []
    for season in sorted(payloads):
        p = payloads[season]
        v = _write_js(games_dir / f"{season}.js", f"(window.__CFB_GAMES__=window.__CFB_GAMES__||{{}})[{season}]", p)
        seasons.append({"season": season, "completed": sum(r[4] for r in p["games"]),
                        "scheduled": sum(1 - r[4] for r in p["games"]), "src": f"data/games/{season}.js?v={v}"})
    sv = _write_js(out_dir / "search_index.js", "window.__CFB_SEARCH__", build_search_index(conn, payloads))
    details, series = {}, []
    for sub in ("details", "series"):
        if (out_dir / sub).exists():
            shutil.rmtree(out_dir / sub)
        (out_dir / sub).mkdir(parents=True)
    for season, p in sorted(build_game_details(conn).items()):
        v = _write_js(out_dir / "details" / f"{season}.js", f"(window.__CFB_DETAILS__=window.__CFB_DETAILS__||{{}})[{season}]", p)
        details[str(season)] = f"data/details/{season}.js?v={v}"
    shard_payloads = build_series(payloads)
    for k in range(SERIES_SHARDS):
        p = shard_payloads.get(k, {"shard": k, "fields": SERIES_FIELDS, "pairs": {}})
        v = _write_js(out_dir / "series" / f"{k}.js", f"(window.__CFB_SERIES__=window.__CFB_SERIES__||{{}})[{k}]", p)
        series.append(f"data/series/{k}.js?v={v}")
    manifest = {"seasons": seasons, "search_index": f"data/search_index.js?v={sv}", "game_fields": FIELDS,
                "details": details, "series": series, "series_shards": SERIES_SHARDS,
                "model_params": model_params(),
                # P1-08: what each season actually has, so an absent metric can say
                # why it is absent instead of rendering as a zero.
                "coverage": build_coverage(conn)}
    (out_dir / "static_manifest.json").write_text(json.dumps(manifest, separators=(",", ":")), encoding="utf-8")
    return manifest


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default=str(REPO / "db" / "league.db"))
    args = p.parse_args()
    conn = sqlite3.connect(args.db)
    m = export(conn)
    conn.close()
    games_bytes = sum((OUT_DIR / "games" / f"{s['season']}.js").stat().st_size for s in m["seasons"])
    print(f"Season game files: {len(m['seasons'])} seasons, {sum(s['completed'] for s in m['seasons'])} completed + "
          f"{sum(s['scheduled'] for s in m['seasons'])} scheduled games ({games_bytes:,} bytes total); "
          f"search index {(OUT_DIR / 'search_index.js').stat().st_size:,} bytes; "
          f"game details {len(m['details'])} seasons; series {m['series_shards']} files")


if __name__ == "__main__":
    main()
