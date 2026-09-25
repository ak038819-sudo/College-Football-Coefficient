#!/usr/bin/env python3
"""
EXPERIMENT (follow-up to BACKTEST_REPORT.md): does Success Rate add information
ON TOP OF margin of victory? Production Elo is imported, never modified.

    M = M_MOV * A(SRDiff_winner [, margin])

M_MOV is production's multiplier, unchanged. A is a Success Rate adjustment:
  exp     A = exp(g * dz(x, d))
  linear  A = max(0, 1 + g * dz(x, d))
  close   A = exp(g * x * exp(-margin / tau))    SR counts more the closer the score
Every form contains g = 0, which is EXACTLY production MOV, so walk-forward can
fall back to production whenever SR adds nothing.

Same protocol as run_backtest.py: identical ratings entering 2008 (1980-2007
replayed once with production MOV), K normalized to production's average
update size on 2008-2010 only (g = 0 gives exactly K = 35), fixed-K results
too, walk-forward selection on 2008..T-1, scored on T = 2011..last. Games
without SR use production MOV unchanged.

DECOMPOSITION (is the gain Success Rate, or just a better margin curve?):
  reshaped MOV   M = ln(margin+1)^p * c / (c + 0.001 * winner_edge)   -- NO Success Rate
  reshaped hyb   the same times exp(g * SRDiff_winner), g = 0 allowed
p, c and g are chosen walk-forward like everything else. Comparing reshaped
hybrid with reshaped MOV isolates what Success Rate adds on top of an
already-improved margin curve.

Outputs: hybrid_parameter_results.csv, hybrid_walk_forward_results.csv,
hybrid_season_comparison.csv, hybrid_robustness.csv, hybrid_case_studies.csv,
hybrid_results_summary.json. Interpretation: HYBRID_REPORT.md.
"""
from __future__ import annotations

import csv
import json
import math
import sqlite3
import statistics as st
import sys
from collections import Counter, defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import run_backtest as rb  # noqa: E402
from elo_variants import (elo_config, game_scores, load_games, production_multiplier, replay, season_sums,  # noqa: E402
                          summarize, verify_model_a, walk_forward)
from build_elo import mov_multiplier  # noqa: E402

GAMMAS_EXP = [round(0.25 * i, 2) for i in range(0, 17)]          # 0 .. 4
GAMMAS_LIN = [round(0.5 * i, 1) for i in range(0, 13)]           # 0 .. 6
GAMMAS_CLOSE = [round(0.5 * i, 1) for i in range(0, 13)]
DZ = [0.0, 0.02, 0.04]
TAUS = [3.0, 7.0, 14.0]


def adjustment(conf: dict, x: float, margin: int) -> float:
    g = conf["gamma"]
    if conf["form"] == "exp":
        return math.exp(g * rb.deadzone(x, conf["dz"]))
    if conf["form"] == "linear":
        return max(0.0, 1.0 + g * rb.deadzone(x, conf["dz"]))
    return math.exp(g * x * math.exp(-margin / conf["tau"]))


def configs() -> list:
    out = [{"form": "exp", "gamma": g, "dz": d} for g in GAMMAS_EXP for d in DZ]
    out += [{"form": "linear", "gamma": g, "dz": d} for g in GAMMAS_LIN for d in DZ]
    out += [{"form": "close", "gamma": g, "tau": t} for g in GAMMAS_CLOSE for t in TAUS]
    return out


def cid(c: dict) -> str:
    extra = f"dz{c['dz']:.2f}" if "dz" in c else f"tau{c['tau']:.0f}"
    return f"hyb_{c['form']}_g{c['gamma']}_{extra}"


def write_csv(name: str, rows: list) -> None:
    with (HERE / name).open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]), lineterminator="\n")
        w.writeheader()
        w.writerows(rows)


def main() -> None:
    cfg = elo_config()
    K0, mc, md = cfg["k"], cfg["mov_c"], cfg["mov_d"]
    conn = sqlite3.connect(str(rb.REPO / "db" / "league.db"))
    verify_model_a(conn, cfg)
    games = load_games(conn)
    phase = dict(conn.execute("SELECT game_id, game_phase FROM games"))
    for g in games:
        g["game_phase"] = phase[g["game_id"]]
    pre = [g for g in games if g["season_year"] < rb.ERA_START]
    post = [g for g in games if g["season_year"] >= rb.ERA_START]
    last = max(g["season_year"] for g in post)
    prod = production_multiplier(cfg)
    _, _, state = replay(pre, cfg, prod, return_state=True)
    sr = rb.load_sr()

    # production multipliers on the K-normalisation window, from the production replay
    _, prod_rows = replay(post, cfg, prod, state=state, keep_rows=True)
    m_by_game = {r[0]: r[5] for r in prod_rows[::2]}
    norm = [g for g in post if g["season_year"] in rb.KNORM_SEASONS and g["home_score"] != g["away_score"]]
    mov_mean = st.mean(m_by_game[g["game_id"]] for g in norm)

    def hybrid_mult(conf):
        def m(g, pd, wa):
            base = mov_multiplier(pd, wa, mc, md)
            x = rb.winner_srdiff(g, sr)
            return base if x is None else base * adjustment(conf, x, pd)
        return m

    def k_for(conf):
        # mean hybrid multiplier on 2008-10 using production's pregame state for M_MOV
        vals = []
        for g in norm:
            x = rb.winner_srdiff(g, sr)
            base = m_by_game[g["game_id"]]
            vals.append(base if x is None else base * adjustment(conf, x, abs(g["home_score"] - g["away_score"])))
        return K0 * mov_mean / st.mean(vals)

    def run(mult, k):
        return replay(post, dict(cfg, k=k), mult, state=state)[0]

    results = {"MOV_production": {"family": "Current MOV Elo", "mode": "production", "K": K0, "conf": None,
                                  "sums": season_sums(run(prod, K0))}}
    for conf in configs():
        kn = k_for(conf)
        for mode, k in (("normalized", kn), ("fixed", K0)):
            results[f"{cid(conf)}|{mode}"] = {"family": f"Hybrid {conf['form']}", "mode": mode, "K": k, "conf": conf,
                                             "sums": season_sums(run(hybrid_mult(conf), k))}

    test = set(range(rb.FIRST_TEST, last + 1))
    param_rows = []
    for key, r in results.items():
        s = summarize(r["sums"], test)
        c = r["conf"] or {}
        param_rows.append({"config": key, "family": r["family"], "k_mode": r["mode"], "K": round(r["K"], 3),
                           "gamma": c.get("gamma", ""), "dead_zone": c.get("dz", ""), "tau": c.get("tau", ""),
                           "games": s["n"], "brier": round(s["brier"], 6), "log_loss": round(s["log_loss"], 6),
                           "accuracy": round(s["accuracy"], 5)})
    write_csv("hybrid_parameter_results.csv", param_rows)

    def keys(family=None, mode=None):
        return {k: r["sums"] for k, r in results.items()
                if (family is None or r["family"] == family) and (mode is None or r["mode"] == mode)
                and r["family"] != "Current MOV Elo"}

    wf, wf_rows, wf_rows_extra = {}, [], []
    groups = [("Current MOV Elo", {"MOV_production": results["MOV_production"]["sums"]})]
    for fam in ("Hybrid exp", "Hybrid linear", "Hybrid close"):
        groups += [(f"{fam} [normalized K]", keys(fam, "normalized")), (f"{fam} [fixed K]", keys(fam, "fixed"))]
    groups += [("Any hybrid [normalized K]", keys(mode="normalized")), ("Any hybrid [fixed K]", keys(mode="fixed"))]
    for label, ks in groups:
        res = walk_forward(ks, rb.FIRST_TEST, last, rb.ERA_START)
        wf[label] = res
        for r in res["per_season"]:
            wf_rows.append({"model": label, "test_season": r["season"], "config_chosen_on_prior_seasons": r["chosen"],
                            "games": r["n"], "brier": round(r["brier"], 6), "log_loss": round(r["log_loss"], 6),
                            "accuracy": round(r["accuracy"], 5)})
    write_csv("hybrid_walk_forward_results.csv", wf_rows)

    # composite per-game predictions of the leading hybrid, vs production MOV
    lead_label = "Any hybrid [normalized K]"
    chosen = {r["season"]: r["chosen"] for r in wf[lead_label]["per_season"]}
    cache = {}

    def preds_for(key):
        if key not in cache:
            r = results[key]
            cache[key] = {p["game_id"]: p for p in run(hybrid_mult(r["conf"]), r["K"])}
        return cache[key]

    mov_p = {p["game_id"]: p for p in run(prod, K0)}
    lead = {g["game_id"]: preds_for(chosen[g["season_year"]])[g["game_id"]] for g in post if g["season_year"] in chosen}
    diffs = [game_scores(lead[i]["p_home"], lead[i]["s_home"])[1] - game_scores(mov_p[i]["p_home"], mov_p[i]["s_home"])[1]
             for i in lead]
    mean_d, se = st.mean(diffs), st.stdev(diffs) / math.sqrt(len(diffs))

    season_rows = []
    for t in sorted(test):
        m = next(x for x in wf["Current MOV Elo"]["per_season"] if x["season"] == t)
        h = next(x for x in wf[lead_label]["per_season"] if x["season"] == t)
        season_rows.append({"season": t, "games": m["n"], "mov_log_loss": round(m["log_loss"], 6),
                            "hybrid_log_loss": round(h["log_loss"], 6), "mov_brier": round(m["brier"], 6),
                            "hybrid_brier": round(h["brier"], 6), "hybrid_config": h["chosen"],
                            "hybrid_better": h["log_loss"] < m["log_loss"]})
    write_csv("hybrid_season_comparison.csv", season_rows)

    # robustness splits, same definitions as run_backtest.py
    membership = {(t, s): c for t, s, c in conn.execute(
        "SELECT team_id, season_year, conference_real FROM team_membership_by_season")}
    splits = defaultdict(lambda: {"m": [], "h": []})
    for g in post:
        i = g["game_id"]
        if i not in lead:
            continue
        pm = mov_p[i]
        fav_home = pm["p_home"] >= 0.5
        venue = "neutral site" if g["neutral_site"] else ("favourite at home" if fav_home else "favourite on the road")
        ch, ca = membership.get((g["home_team_id"], g["season_year"])), membership.get((g["away_team_id"], g["season_year"]))
        conf_game = ch is not None and ch == ca and ch != "FBS Independents"
        p = pm["p_home"]
        band = "<20%" if p < 0.2 else "20-40%" if p < 0.4 else "40-60%" if p < 0.6 else "60-80%" if p < 0.8 else ">80%"
        for key in ("all games", venue, "conference" if conf_game else "nonconference",
                    "postseason" if g["game_phase"] != "regular" else "regular season", f"home win prob {band}"):
            splits[key]["m"].append(game_scores(pm["p_home"], pm["s_home"])[1])
            splits[key]["h"].append(game_scores(lead[i]["p_home"], lead[i]["s_home"])[1])
    order = ["all games", "favourite at home", "favourite on the road", "neutral site", "conference", "nonconference",
             "regular season", "postseason", "home win prob <20%", "home win prob 20-40%", "home win prob 40-60%",
             "home win prob 60-80%", "home win prob >80%"]
    rob = [{"split": k, "games": len(v["m"]), "mov_log_loss": round(st.mean(v["m"]), 5),
            "hybrid_log_loss": round(st.mean(v["h"]), 5), "hybrid_minus_mov": round(st.mean(v["h"]) - st.mean(v["m"]), 5)}
           for k, v in splits.items()]
    rob.sort(key=lambda r: order.index(r["split"]))
    write_csv("hybrid_robustness.csv", rob)

    # case studies with the hybrid chosen for the latest season
    lead_key = chosen[last]
    lc, lk = results[lead_key]["conf"], results[lead_key]["K"]
    sr_all = rb.load_sr("all")
    stored = {(r[0], r[1]): r for r in conn.execute(
        "SELECT game_id, team_id, pregame_elo, opponent_pregame_elo, elo_expectation, mov_multiplier, elo_change "
        "FROM elo_game_history")}
    case_rows = []
    for team, supplied in (("BYU", (0.62, 0.45)), ("Marshall", (0.55, 0.58))):
        g = next(x for x in post if x["season_year"] == 2026 and team in (x["home_name"], x["away_name"])
                 and x["home_name"] in ("Colorado State", "Missouri State"))
        tid = g["home_team_id"] if g["home_name"] == team else g["away_team_id"]
        pre_e, opp_e, e, m_mov, d_mov = stored[(g["game_id"], tid)][2:]
        margin = abs(g["home_score"] - g["away_score"])
        for label, (a, b) in (("supplied", supplied), ("CFBD garbage time excluded", (sr[g["game_id"]]["a"], sr[g["game_id"]]["h"])),
                              ("CFBD all plays", (sr_all[g["game_id"]]["a"], sr_all[g["game_id"]]["h"]))):
            adj = adjustment(lc, a - b, margin)                 # both are road winners: SRDiff_winner = a - b
            case_rows.append({"game": f"{team} at {g['home_name']} 2026", "source": label, "srdiff": round(a - b, 4),
                              "expected_win_prob": round(e, 4), "mov_multiplier": round(m_mov, 4),
                              "production_elo_change": round(d_mov, 2), "hybrid_config": lead_key,
                              "hybrid_K": round(lk, 2), "sr_adjustment": round(adj, 4),
                              "hybrid_multiplier": round(m_mov * adj, 4),
                              "hybrid_elo_change": round(lk * (1.0 - e) * m_mov * adj, 2)})
    write_csv("hybrid_case_studies.csv", case_rows)

    # ---- decomposition: reshaped margin curve with and without Success Rate
    def reshaped(p, c, gamma):
        def m(g, pd, wa):
            base = (math.log(pd + 1) ** p) * (c / (c + 0.001 * wa))
            x = rb.winner_srdiff(g, sr)
            return base if (x is None or gamma == 0) else base * math.exp(gamma * x)
        vals = []
        for g in norm:
            r = rows_by_game[g["game_id"]]
            hfa = 0 if g["neutral_site"] else cfg["home_field"]
            wa = (r[2] + hfa - r[3]) if g["home_score"] > g["away_score"] else (r[3] - r[2] - hfa)
            vals.append(m(g, abs(g["home_score"] - g["away_score"]), wa))
        return m, K0 * mov_mean / st.mean(vals)

    rows_by_game = {r[0]: r for r in prod_rows[::2]}
    shape_models = {"reshaped MOV": {}, "reshaped hybrid": {}}
    # p = 1.0 with c = 2.2 and g = 0 is exactly production, so both families can fall back to it
    for p in (1.0, 1.25, 1.5, 2.0, 2.5, 3.0):
        for c in (1.1, 2.2, 4.4):
            shape_models["reshaped MOV"][f"shape_p{p}_c{c}"] = reshaped(p, c, 0.0)
            for gamma in (0.0, 1.0, 1.5, 2.0, 2.5, 3.0):
                shape_models["reshaped hybrid"][f"shape_p{p}_c{c}_g{gamma}"] = reshaped(p, c, gamma)
    shape_wf, shape_preds = {}, {}
    for name, models in shape_models.items():
        sums = {k: season_sums(run(m, kk)) for k, (m, kk) in models.items()}
        shape_wf[name] = walk_forward(sums, rb.FIRST_TEST, last, rb.ERA_START)
        ch = {r["season"]: r["chosen"] for r in shape_wf[name]["per_season"]}
        per = {k: {pp["game_id"]: pp for pp in run(*models[k])} for k in set(ch.values())}
        shape_preds[name] = {g["game_id"]: per[ch[g["season_year"]]][g["game_id"]] for g in post if g["season_year"] in ch}
        for r in shape_wf[name]["per_season"]:
            wf_rows_extra.append({"model": name, "test_season": r["season"], "config_chosen_on_prior_seasons": r["chosen"],
                                  "games": r["n"], "brier": round(r["brier"], 6), "log_loss": round(r["log_loss"], 6),
                                  "accuracy": round(r["accuracy"], 5)})

    def paired(a, b):
        # compare on the test-season games both models scored (walk-forward models start in FIRST_TEST)
        d = [game_scores(b[i]["p_home"], b[i]["s_home"])[1] - game_scores(a[i]["p_home"], a[i]["s_home"])[1]
             for i in b if i in a]
        e = st.stdev(d) / math.sqrt(len(d))
        return {"mean": st.mean(d), "se": e, "z": st.mean(d) / e, "games": len(d)}

    def seasons_better(a_label, b_label):
        return sum(1 for x, y in zip(shape_wf[a_label]["per_season"], shape_wf[b_label]["per_season"])
                   if y["log_loss"] < x["log_loss"])

    decomposition = {
        "reshaped_mov_vs_production": paired(mov_p, shape_preds["reshaped MOV"]),
        "sr_on_top_of_reshaped_mov": paired(shape_preds["reshaped MOV"], shape_preds["reshaped hybrid"]),
        "reshaped_hybrid_vs_production": paired(mov_p, shape_preds["reshaped hybrid"]),
        "sr_on_top_seasons_better": [seasons_better("reshaped MOV", "reshaped hybrid"), len(season_rows)],
        "walk_forward": {k: v["pooled"] for k, v in shape_wf.items()},
        "choices": {k: Counter(r["chosen"] for r in v["per_season"]).most_common() for k, v in shape_wf.items()},
    }
    with (HERE / "hybrid_walk_forward_results.csv").open("a", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(wf_rows_extra[0]), lineterminator="\n")
        w.writerows(wf_rows_extra)

    summary = {"decomposition": decomposition,
               "walk_forward": {k: v["pooled"] for k, v in wf.items()},
               "choices": {k: Counter(r["chosen"] for r in v["per_season"]).most_common() for k, v in wf.items()},
               "lead_minus_mov_logloss_per_game": {"mean": mean_d, "se": se, "z": mean_d / se, "games": len(diffs)},
               "lead_better_seasons": [sum(r["hybrid_better"] for r in season_rows), len(season_rows)],
               "lead_latest_config": lead_key}
    (HERE / "hybrid_results_summary.json").write_text(json.dumps(summary, indent=1), encoding="utf-8")
    print(json.dumps({k: summary[k] for k in ("walk_forward", "lead_minus_mov_logloss_per_game",
                                               "lead_better_seasons", "lead_latest_config", "decomposition")}, indent=1))


if __name__ == "__main__":
    main()
