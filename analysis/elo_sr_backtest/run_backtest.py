#!/usr/bin/env python3
"""
EXPERIMENT: does Success Rate Differential (SRDiff) beat Margin of Victory (MOV)
as the Elo performance multiplier? Production Elo is imported, never modified.

Reproduces every output in this folder (deterministic; ~1 minute):
  parameter_results.csv, walk_forward_results.csv, season_model_comparison.csv,
  disagreement_games.csv, sr_distribution.csv, play_count_reliability.csv,
  robustness.csv, byu_csu_case_study.csv, marshall_missouri_state_case_study.csv,
  results_summary.json   (BACKTEST_REPORT.md interprets these)

Inputs: db/league.db (games + elo_game_history), game_success_rates.csv
(from fetch_game_success_rates.py).

Design (see BACKTEST_REPORT.md for the reasoning):
  * Every model replays 1980-2007 identically with production MOV, so all enter
    ERA_START (2008, first season of >=95% sustained SR coverage) with the SAME
    ratings. Models differ only in the multiplier M from 2008 on.
  * Update: dR = K (S - E) M with M >= 0, so the winner always gains.
  * SRDiff is from the WINNER's side: SR_winner - SR_loser (a winner who lost
    the SR battle has SRDiff < 0 and gets a smaller update, never a reversal).
  * K: production's M averages ~2.4 while 1 + b*SRDiff averages far less, so at
    a fixed K an SR model would take smaller steps for reasons unrelated to the
    information in SR. Each SR config therefore gets K = 35 * mean(M_MOV) /
    mean(SRM), both means measured on 2008-2010 only (before every test season,
    so no leakage). Fixed K = 35 results are reported too.
  * Games in the SR era without SR for both teams fall back to production MOV,
    rescaled to production's update size.
  * Walk-forward: for each test season T (2011..last), choose a config using
    seasons 2008..T-1 ONLY (lowest log loss), score it on T.
"""
from __future__ import annotations

import csv
import json
import math
import sqlite3
import statistics as st
import sys
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from elo_variants import (REPO, elo_config, game_scores, load_games, pure_multiplier, production_multiplier,  # noqa: E402
                          replay, season_sums, summarize, verify_model_a, walk_forward)
from build_elo import mov_multiplier  # noqa: E402

ERA_START, FIRST_TEST = 2008, 2011
KNORM_SEASONS = set(range(2008, 2011))
VARIANT = "nogt"                     # primary: garbage time excluded (matches the team pages)
BETAS = list(range(1, 11))
DEADZONES = [0.0, 0.01, 0.02, 0.03, 0.04, 0.05]
FLOORS = [0.50, 0.60, 0.70, 0.75]
CEILINGS = [1.50, 1.75, 2.00, 2.25, 2.50]
SAT_A = [0.5, 1.0, 1.5]


# ---------------------------------------------------------------- data
def _f(v):
    return None if v in ("", None) else float(v)


def load_sr(variant: str = VARIANT) -> dict:
    out = {}
    with (HERE / "game_success_rates.csv").open(newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            h, a = _f(r[f"home_success_rate_{variant}"]), _f(r[f"away_success_rate_{variant}"])
            if h is not None and a is not None:
                out[int(r["game_id"])] = {"h": h, "a": a, "hp": _f(r[f"home_plays_{variant}"]),
                                          "ap": _f(r[f"away_plays_{variant}"])}
    return out


def winner_srdiff(g: dict, sr: dict):
    s = sr.get(g["game_id"])
    if s is None or g["home_score"] == g["away_score"]:
        return None
    return s["h"] - s["a"] if g["home_score"] > g["away_score"] else s["a"] - s["h"]


def deadzone(x: float, d: float) -> float:
    if abs(x) <= d:
        return 0.0
    return x - d if x > 0 else x + d


def srm(conf: dict, x: float) -> float:
    """Success Rate multiplier for a winner-side SRDiff x (never negative)."""
    a = deadzone(x, conf["dz"])
    b = conf["beta"]
    if conf["shape"] == "linear":
        raw = 1.0 + b * a
    elif conf["shape"] == "exp":
        raw = math.exp(b * a)
    else:                                   # saturating: slope b at 0, levels off at 1 +/- A
        raw = 1.0 + conf["A"] * math.tanh(b * a / conf["A"])
    if conf.get("floor") is not None:
        raw = max(conf["floor"], min(conf["ceil"], raw))
    return max(0.0, raw)


def configs() -> list:
    out = [{"family": "Linear SRDiff", "shape": "linear", "beta": b, "dz": 0.0} for b in [0] + BETAS]
    out += [{"family": "Dead-zone SRDiff", "shape": "linear", "beta": b, "dz": d} for b in BETAS for d in DEADZONES[1:]]
    out += [{"family": "Bounded SRDiff", "shape": "linear", "beta": b, "dz": d, "floor": fl, "ceil": ce}
            for b in BETAS for d in DEADZONES for fl in FLOORS for ce in CEILINGS]
    out += [{"family": "Nonlinear SRDiff", "shape": "exp", "beta": b, "dz": d} for b in BETAS for d in DEADZONES]
    out += [{"family": "Nonlinear SRDiff", "shape": "sat", "beta": b, "dz": d, "A": A}
            for b in BETAS for d in DEADZONES for A in SAT_A]
    return out


def conf_id(c: dict) -> str:
    parts = [c["shape"], f"b{c['beta']}", f"dz{c['dz']:.2f}"]
    if c.get("A") is not None:
        parts.append(f"A{c['A']}")
    if c.get("floor") is not None:
        parts.append(f"f{c['floor']:.2f}-c{c['ceil']:.2f}")
    return "_".join(parts)


# ---------------------------------------------------------------- run
def main() -> None:
    cfg = elo_config()
    K0 = cfg["k"]
    conn = sqlite3.connect(str(REPO / "db" / "league.db"))
    verify_model_a(conn, cfg)                                   # harness == production, or stop here
    games = load_games(conn)
    phase = dict(conn.execute("SELECT game_id, game_phase FROM games"))   # not in the engine's query
    for g in games:
        g["game_phase"] = phase[g["game_id"]]
    pre = [g for g in games if g["season_year"] < ERA_START]
    post = [g for g in games if g["season_year"] >= ERA_START]
    last_season = max(g["season_year"] for g in post)
    prod = production_multiplier(cfg)
    _, _, state = replay(pre, cfg, prod, return_state=True)
    sr = load_sr()
    sr_all = load_sr("all")

    # production M on the K-normalisation seasons
    _, prod_rows = replay(post, cfg, prod, state=state, keep_rows=True)
    norm_games = [g for g in post if g["season_year"] in KNORM_SEASONS and g["home_score"] != g["away_score"]]
    mov_mean = st.mean(r[5] for r in prod_rows[::2] if r[0] in {g["game_id"] for g in norm_games})

    def k_for(conf):
        vals = [srm(conf, winner_srdiff(g, sr)) for g in norm_games if winner_srdiff(g, sr) is not None]
        m = st.mean(vals) if vals else 0.0
        return K0 * mov_mean / m if m > 0 else K0

    def sr_multiplier(conf, k_model, srdata=sr):
        def m(g, pd, wa):
            x = winner_srdiff(g, srdata)
            if x is None:
                return mov_multiplier(pd, wa, cfg["mov_c"], cfg["mov_d"]) * K0 / k_model
            return srm(conf, x)
        return m

    def run(mult, k):
        preds, _ = replay(post, dict(cfg, k=k), mult, state=state)
        return preds

    results = {}          # key -> {"family", "k_mode", "K", "params", "sums"}

    def record(key, family, k_mode, k, params, preds):
        results[key] = {"family": family, "k_mode": k_mode, "K": k, "params": params, "sums": season_sums(preds)}

    record("MOV_production", "Current MOV Elo", "production", K0, {}, run(prod, K0))
    for k in (25, 30, 35, 40, 45, 50):
        record(f"MOV_K{k}", "MOV, K walk-forward", "tuned", k, {"K": k}, run(prod, k))
    for k in (35, 50, 60, 70, 80, 90, 100, 120):
        record(f"PURE_K{k}", "Pure W/L Elo", "tuned", k, {"K": k}, run(pure_multiplier, k))
    for conf in configs():
        cid = conf_id(conf)
        kn = k_for(conf)
        record(f"{cid}|norm", conf["family"], "normalized", kn, conf, run(sr_multiplier(conf, kn), kn))
        record(f"{cid}|fixed", conf["family"], "fixed", K0, conf, run(sr_multiplier(conf, K0), K0))

    test = set(range(FIRST_TEST, last_season + 1))
    # ---- parameter_results.csv (in-sample over all test seasons, for description only)
    with (HERE / "parameter_results.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(["config", "family", "k_mode", "K", "shape", "beta", "dead_zone", "floor", "ceiling", "sat_A",
                    "games", "brier", "log_loss", "accuracy"])
        for key, r in results.items():
            s = summarize(r["sums"], test)
            p = r["params"]
            w.writerow([key, r["family"], r["k_mode"], round(r["K"], 3), p.get("shape", ""), p.get("beta", ""),
                        p.get("dz", ""), p.get("floor", ""), p.get("ceil", ""), p.get("A", ""), s["n"],
                        round(s["brier"], 6), round(s["log_loss"], 6), round(s["accuracy"], 5)])

    # ---- walk-forward per family (and "any SR")
    def family_keys(family, k_mode):
        return {k: r["sums"] for k, r in results.items()
                if (r["family"] == family or (family == "Any SR model" and "SRDiff" in r["family"]))
                and (k_mode is None or r["k_mode"] == k_mode)}

    wf = {}
    rows_wf = []
    fams = [("Current MOV Elo", None), ("MOV, K walk-forward", None), ("Pure W/L Elo", None)]
    for fam in ("Linear SRDiff", "Dead-zone SRDiff", "Bounded SRDiff", "Nonlinear SRDiff", "Any SR model"):
        fams += [(fam, "normalized"), (fam, "fixed")]
    for fam, mode in fams:
        res = walk_forward(family_keys(fam, mode), FIRST_TEST, last_season, ERA_START)
        label = fam if mode is None else f"{fam} [{mode} K]"
        wf[label] = res
        for r in res["per_season"]:
            rows_wf.append([label, r["season"], r["chosen"], r["n"], round(r["brier"], 6), round(r["log_loss"], 6),
                            round(r["accuracy"], 5)])
    with (HERE / "walk_forward_results.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(["model", "test_season", "config_chosen_on_prior_seasons", "games", "brier", "log_loss", "accuracy"])
        w.writerows(rows_wf)

    # ---- per-game composite predictions for the leading SR model and MOV
    lead_label = "Any SR model [normalized K]"
    chosen = {r["season"]: r["chosen"] for r in wf[lead_label]["per_season"]}
    conf_by_key = {k: r["params"] for k, r in results.items()}
    cache = {}

    def preds_for(key):
        if key not in cache:
            r = results[key]
            cache[key] = {p["game_id"]: p for p in run(sr_multiplier(conf_by_key[key], r["K"]), r["K"])}
        return cache[key]

    mov_preds = {p["game_id"]: p for p in run(prod, K0)}
    lead = {}
    for g in post:
        s = g["season_year"]
        if s in chosen:
            lead[g["game_id"]] = preds_for(chosen[s])[g["game_id"]]

    diffs = []
    for gid, p in lead.items():
        m = mov_preds[gid]
        diffs.append(game_scores(p["p_home"], p["s_home"])[1] - game_scores(m["p_home"], m["s_home"])[1])
    mean_d = st.mean(diffs)
    se = st.stdev(diffs) / math.sqrt(len(diffs))

    # ---- season_model_comparison.csv
    seasons_rows = []
    for t in sorted(test):
        row = {"season": t}
        for label in ("Current MOV Elo", "MOV, K walk-forward", "Pure W/L Elo", "Linear SRDiff [normalized K]",
                      "Dead-zone SRDiff [normalized K]", "Bounded SRDiff [normalized K]",
                      "Nonlinear SRDiff [normalized K]", lead_label):
            ps = next((x for x in wf[label]["per_season"] if x["season"] == t), None)
            row[f"{label} log_loss"] = round(ps["log_loss"], 6) if ps else None
            row[f"{label} brier"] = round(ps["brier"], 6) if ps else None
        row["games"] = next(x["n"] for x in wf["Current MOV Elo"]["per_season"] if x["season"] == t)
        row["lead_SR_config"] = chosen.get(t)
        seasons_rows.append(row)
    with (HERE / "season_model_comparison.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(seasons_rows[0]), lineterminator="\n")
        w.writeheader()
        w.writerows(seasons_rows)
    lead_beats_mov = sum(1 for r in seasons_rows if r[f"{lead_label} log_loss"] < r["Current MOV Elo log_loss"])

    # ---- SR distribution (2008+, garbage time excluded)
    game_by_id = {g["game_id"]: g for g in post}
    team_rows, winner_rows = [], []
    for gid, s in sr.items():
        g = game_by_id.get(gid)
        if g is None:
            continue
        margin = g["home_score"] - g["away_score"]
        team_rows.append((s["h"] - s["a"], margin))
        team_rows.append((s["a"] - s["h"], -margin))
        if margin != 0:
            winner_rows.append(s["h"] - s["a"] if margin > 0 else s["a"] - s["h"])

    def pct(vals, q):
        v = sorted(vals)
        i = (len(v) - 1) * q
        lo, hi = math.floor(i), math.ceil(i)
        return v[lo] + (v[hi] - v[lo]) * (i - lo)

    dist_rows = []
    for label, vals in (("team perspective (every game counted from both sides)", [x for x, _ in team_rows]),
                        ("winner perspective (SR_winner - SR_loser)", winner_rows),
                        ("absolute |SRDiff| per game", [abs(x) for x in winner_rows])):
        dist_rows.append({"section": "summary", "series": label, "n": len(vals), "mean": round(st.mean(vals), 4),
                          "median": round(st.median(vals), 4), "std": round(st.pstdev(vals), 4),
                          **{f"p{int(q*100):02d}": round(pct(vals, q), 4) for q in (0.05, 0.10, 0.25, 0.75, 0.90, 0.95)},
                          "min": round(min(vals), 4), "max": round(max(vals), 4)})
    edges = [-1, -0.20, -0.15, -0.10, -0.05, 0.0, 0.05, 0.10, 0.15, 0.20, 1.01]
    labels = ["<= -20pp", "-20 to -15", "-15 to -10", "-10 to -5", "-5 to 0", "0 to +5", "+5 to +10",
              "+10 to +15", "+15 to +20", ">= +20pp"]
    for i, lab in enumerate(labels):
        lo, hi = edges[i], edges[i + 1]
        grp = [(x, m) for x, m in team_rows if (lo < x <= hi if i < 5 else lo <= x < hi)]
        if grp:
            dist_rows.append({"section": "bucket (team perspective)", "series": lab, "n": len(grp),
                              "win_pct": round(sum(1 for _, m in grp if m > 0) / len(grp), 4),
                              "avg_margin": round(st.mean(m for _, m in grp), 2),
                              "median_margin": st.median(m for _, m in grp)})
    abs_vals = sorted(abs(x) for x in winner_rows)
    rank = lambda v: sum(1 for x in abs_vals if x < v) / len(abs_vals)
    keys = sorted({k for r in dist_rows for k in r}, key=lambda k: ["section", "series", "n", "mean", "median", "std",
                  "p05", "p10", "p25", "p75", "p90", "p95", "min", "max", "win_pct", "avg_margin",
                  "median_margin"].index(k))
    with (HERE / "sr_distribution.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=keys, lineterminator="\n")
        w.writeheader()
        w.writerows(dist_rows)

    # ---- play-count reliability
    pc_rows = []
    pc_bins = [(0, 50, "<50"), (50, 60, "50-59"), (60, 70, "60-69"), (70, 80, "70-79"), (80, 999, "80+")]
    for lo, hi, lab in pc_bins:
        team_sr = []
        agree, n, xs, ms = 0, 0, [], []
        for gid, s in sr.items():
            g = game_by_id.get(gid)
            if g is None or s["hp"] is None or s["ap"] is None:
                continue
            for srv, pl in ((s["h"], s["hp"]), (s["a"], s["ap"])):
                if lo <= pl < hi:
                    team_sr.append(srv)
            if lo <= min(s["hp"], s["ap"]) < hi and g["home_score"] != g["away_score"]:
                d = s["h"] - s["a"]
                m = g["home_score"] - g["away_score"]
                n += 1
                agree += int((d > 0) == (m > 0))
                xs.append(d)
                ms.append(m)
        corr = None
        if len(xs) > 2:
            mx, mm = st.mean(xs), st.mean(ms)
            corr = sum((a - mx) * (b - mm) for a, b in zip(xs, ms)) / math.sqrt(
                sum((a - mx) ** 2 for a in xs) * sum((b - mm) ** 2 for b in ms))
        pc_rows.append({"offensive_plays": lab, "team_games": len(team_sr),
                        "team_sr_std": round(st.pstdev(team_sr), 4) if team_sr else None,
                        "games_by_fewer_plays_side": n,
                        "sr_winner_also_won_pct": round(agree / n, 4) if n else None,
                        "corr_srdiff_margin": round(corr, 4) if corr is not None else None})
    with (HERE / "play_count_reliability.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(pc_rows[0]), lineterminator="\n")
        w.writeheader()
        w.writerows(pc_rows)

    # ---- disagreement analysis: whose rating update better predicts the teams' NEXT games?
    team_games = defaultdict(list)
    for g in post:
        team_games[g["home_team_id"]].append(g)
        team_games[g["away_team_id"]].append(g)
    next_game = {}
    for tid, gl in team_games.items():
        for a, b in zip(gl, gl[1:]):
            if a["season_year"] == b["season_year"]:
                next_game[(a["game_id"], tid)] = b["game_id"]
    cats = {
        "winner lost Success Rate (SRDiff_winner < 0)": lambda x, m: x < 0,
        "close score (<=7) + dominant SR (|SRDiff| >= 10pp)": lambda x, m: abs(m) <= 7 and abs(x) >= 0.10,
        "blowout (>=17) + close SR (|SRDiff| <= 3pp)": lambda x, m: abs(m) >= 17 and abs(x) <= 0.03,
        "extreme SR dominance (|SRDiff| >= 15pp)": lambda x, m: abs(x) >= 0.15,
    }
    dis_rows, cat_stats = [], {}
    for name, test_fn in cats.items():
        flagged, subsequent = 0, set()
        for g in post:
            if g["season_year"] < FIRST_TEST or g["game_id"] not in lead:
                continue
            x = winner_srdiff(g, sr)
            if x is None:
                continue
            margin = abs(g["home_score"] - g["away_score"])
            if not test_fn(x, margin):
                continue
            flagged += 1
            for tid in (g["home_team_id"], g["away_team_id"]):
                nxt = next_game.get((g["game_id"], tid))
                if nxt is not None and nxt in lead:
                    subsequent.add(nxt)
            if len(dis_rows) < 100000:
                m_mov = mov_multiplier(margin, 0.0, cfg["mov_c"], cfg["mov_d"])
                dis_rows.append({"category": name, "game_id": g["game_id"], "season": g["season_year"],
                                 "date": g["game_date"], "home": g["home_name"], "away": g["away_name"],
                                 "home_score": g["home_score"], "away_score": g["away_score"],
                                 "winner_srdiff": round(x, 4), "margin": margin,
                                 "mov_multiplier_neutral": round(m_mov, 3),
                                 "sr_multiplier_lead": round(srm(conf_by_key[chosen[g["season_year"]]], x), 3)})
        sm = [game_scores(mov_preds[i]["p_home"], mov_preds[i]["s_home"]) for i in subsequent]
        ss = [game_scores(lead[i]["p_home"], lead[i]["s_home"]) for i in subsequent]
        cat_stats[name] = {"flagged_games": flagged, "subsequent_games": len(subsequent),
                           "mov_brier": st.mean(s[0] for s in sm) if sm else None,
                           "sr_brier": st.mean(s[0] for s in ss) if ss else None,
                           "mov_log_loss": st.mean(s[1] for s in sm) if sm else None,
                           "sr_log_loss": st.mean(s[1] for s in ss) if ss else None}
    with (HERE / "disagreement_games.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(dis_rows[0]), lineterminator="\n")
        w.writeheader()
        w.writerows(dis_rows)

    # ---- robustness of the leading SR model vs MOV (test seasons)
    membership = {(t, s): c for t, s, c in conn.execute(
        "SELECT team_id, season_year, conference_real FROM team_membership_by_season")}
    splits = defaultdict(lambda: {"mov": [], "sr": [], "mov_fav": [], "sr_fav": []})
    for g in post:
        gid = g["game_id"]
        if gid not in lead:
            continue
        pm, pl = mov_preds[gid], lead[gid]
        fav_home = pm["p_home"] >= 0.5
        venue = "neutral site" if g["neutral_site"] else ("favourite at home" if fav_home else "favourite on the road")
        ch = membership.get((g["home_team_id"], g["season_year"]))
        ca = membership.get((g["away_team_id"], g["season_year"]))
        conf_game = ch is not None and ch == ca and ch != "FBS Independents"
        p = pm["p_home"]
        band = "<20%" if p < 0.2 else "20-40%" if p < 0.4 else "40-60%" if p < 0.6 else "60-80%" if p < 0.8 else ">80%"
        for key in ("all games", venue, "conference" if conf_game else "nonconference",
                    "postseason" if g["game_phase"] != "regular" else "regular season", f"home win prob {band}"):
            splits[key]["mov"].append(game_scores(pm["p_home"], pm["s_home"]))
            splits[key]["sr"].append(game_scores(pl["p_home"], pl["s_home"]))
            fav_p_m = pm["p_home"] if fav_home else 1 - pm["p_home"]
            fav_p_s = pl["p_home"] if fav_home else 1 - pl["p_home"]
            fav_won = pm["s_home"] if fav_home else 1 - pm["s_home"]
            splits[key]["mov_fav"].append((fav_p_m, fav_won))
            splits[key]["sr_fav"].append((fav_p_s, fav_won))
    rob_rows = []
    for key, d in splits.items():
        rob_rows.append({"split": key, "games": len(d["mov"]),
                         "mov_log_loss": round(st.mean(x[1] for x in d["mov"]), 5),
                         "sr_log_loss": round(st.mean(x[1] for x in d["sr"]), 5),
                         "mov_brier": round(st.mean(x[0] for x in d["mov"]), 5),
                         "sr_brier": round(st.mean(x[0] for x in d["sr"]), 5),
                         "favourite_actual_win_pct": round(st.mean(x[1] for x in d["mov_fav"]), 4),
                         "mov_favourite_predicted": round(st.mean(x[0] for x in d["mov_fav"]), 4),
                         "sr_favourite_predicted": round(st.mean(x[0] for x in d["sr_fav"]), 4)})
    order = ["all games", "favourite at home", "favourite on the road", "neutral site", "conference", "nonconference",
             "regular season", "postseason", "home win prob <20%", "home win prob 20-40%", "home win prob 40-60%",
             "home win prob 60-80%", "home win prob >80%"]
    rob_rows.sort(key=lambda r: order.index(r["split"]) if r["split"] in order else 99)
    with (HERE / "robustness.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rob_rows[0]), lineterminator="\n")
        w.writeheader()
        w.writerows(rob_rows)

    # ---- case studies (production pregame state; SR under the 2026-chosen lead config)
    lead_2026 = chosen.get(last_season) or chosen[max(chosen)]
    lead_conf, lead_k = conf_by_key[lead_2026], results[lead_2026]["K"]
    case_files = {"BYU": "byu_csu_case_study.csv", "Marshall": "marshall_missouri_state_case_study.csv"}
    supplied = {"BYU": (0.62, 0.45), "Marshall": (0.55, 0.58)}
    stored = {(r[0], r[1]): r for r in conn.execute(
        "SELECT game_id, team_id, pregame_elo, opponent_pregame_elo, elo_expectation, mov_multiplier, elo_change "
        "FROM elo_game_history")}
    case_out = {}
    for team, fname in case_files.items():
        g = next(x for x in post if x["season_year"] == 2026 and team in (x["home_name"], x["away_name"])
                 and x["home_name"] in ("Colorado State", "Missouri State"))
        tid = g["home_team_id"] if g["home_name"] == team else g["away_team_id"]
        pre_elo, opp_elo, e, m_mov, d_mov = stored[(g["game_id"], tid)][2:]
        won = (g["home_score"] > g["away_score"]) == (tid == g["home_team_id"])
        s_val = 1.0 if won else 0.0
        rows = []
        for label, (a, b) in (("supplied", supplied[team]),
                              ("CFBD garbage time excluded", (sr[g["game_id"]]["a"], sr[g["game_id"]]["h"])),
                              ("CFBD all plays", (sr_all[g["game_id"]]["a"], sr_all[g["game_id"]]["h"]))):
            x = a - b if won else b - a                         # winner-side SRDiff
            mult = srm(lead_conf, x)
            rows.append({"source": label, "team": team, "team_sr": round(a, 4), "opponent_sr": round(b, 4),
                         "team_srdiff": round(a - b, 4), "pregame_elo": round(pre_elo, 1),
                         "opponent_pregame_elo": round(opp_elo, 1), "expected_win_prob_with_hfa": round(e, 4),
                         "actual": s_val, "mov_multiplier": round(m_mov, 4), "mov_elo_change": round(d_mov, 2),
                         "lead_sr_config": lead_2026, "sr_K": round(lead_k, 2), "sr_multiplier": round(mult, 4),
                         "sr_elo_change": round(lead_k * (s_val - e) * mult, 2)})
        with (HERE / fname).open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=list(rows[0]), lineterminator="\n")
            w.writeheader()
            w.writerows(rows)
        case_out[team] = rows

    summary = {
        "era_start": ERA_START, "first_test": FIRST_TEST, "last_test": last_season, "variant": VARIANT,
        "mov_mean_multiplier_2008_2010": mov_mean,
        "walk_forward": {k: v["pooled"] for k, v in wf.items()},
        "walk_forward_choices": {k: [r["chosen"] for r in v["per_season"]] for k, v in wf.items()},
        "lead_vs_mov_per_game_logloss_diff": {"mean": mean_d, "se": se, "z": mean_d / se if se else None,
                                              "games": len(diffs)},
        "lead_beats_mov_seasons": [lead_beats_mov, len(seasons_rows)],
        "srdiff_rank_of": {"+0.17": rank(0.17), "+0.237": rank(0.237), "+0.301": rank(0.301),
                           "-0.03": rank(0.03), "-0.056": rank(0.056)},
        "disagreement": cat_stats, "case_studies": case_out, "lead_2026_config": lead_2026,
    }
    (HERE / "results_summary.json").write_text(json.dumps(summary, indent=1, default=str), encoding="utf-8")
    print(json.dumps({k: summary[k] for k in ("walk_forward", "lead_vs_mov_per_game_logloss_diff",
                                               "lead_beats_mov_seasons")}, indent=1))


if __name__ == "__main__":
    main()
