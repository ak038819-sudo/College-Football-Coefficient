"""
Milestone D: the Rating Ledger and methodology page make exact claims -- "this change is
K x (S - E) x M", "this opening rating is the offseason regression of last season's end",
"this Game CoE is base + difficulty bonus". These tests pin those claims against the whole
database, and pin that the methodology page reads the live parameters, not copies.
"""
import json
import sqlite3
from collections import defaultdict
from pathlib import Path

import build_coefficients
import select_playoff_field_v2
from export_static_data import model_params
from export_team_pages import build_team_pages

REPO = Path(__file__).resolve().parent.parent
CFG = json.loads((REPO / "config" / "model_config.json").read_text(encoding="utf-8"))


def test_every_elo_change_is_k_times_s_minus_e_times_m(db_path):
    K = CFG["elo"]["k"]
    conn = sqlite3.connect(str(db_path))
    worst = 0.0
    for exp, m, chg, margin in conn.execute(
            """SELECT e.elo_expectation, e.mov_multiplier, e.elo_change,
                      CASE WHEN g.home_team_id = e.team_id THEN g.home_score - g.away_score ELSE g.away_score - g.home_score END
               FROM elo_game_history e JOIN games g USING(game_id)"""):
        s = 1.0 if margin > 0 else 0.0 if margin < 0 else 0.5
        worst = max(worst, abs(K * (s - exp) * m - chg))
    assert worst < 1e-9


def test_every_season_opening_is_the_offseason_regression(db_path):
    init, keep = CFG["elo"]["initial_rating"], CFG["elo"]["offseason_retention"]
    conn = sqlite3.connect(str(db_path))
    last = {}
    for tid, season, pre, post in conn.execute(
            """SELECT e.team_id, g.season_year, e.pregame_elo, e.postgame_elo FROM elo_game_history e JOIN games g USING(game_id)
               ORDER BY e.team_id, g.season_year, g.game_date, e.game_id"""):
        if tid not in last:
            assert abs(pre - init) < 1e-9                                   # first appearance
        elif last[tid][0] != season:
            n = season - last[tid][0]                                       # >1 when seasons were skipped
            assert abs(init + keep ** n * (last[tid][1] - init) - pre) < 1e-9, (tid, season)
        last[tid] = (season, post)


def test_every_game_coe_is_base_plus_difficulty_bonus(db_path):
    c = CFG["coe"]
    conn = sqlite3.connect(str(db_path))
    for p, rt, g in conn.execute("SELECT hybrid_expectation, result_type, game_coe FROM hybrid_game_ratings"):
        expected = {"WIN": c["win_base"] + c["difficulty_alpha"] * (1 - p), "OT_WIN": c["win_base"] + c["difficulty_alpha"] * (1 - p),
                    "OT_LOSS": c["ot_loss"], "LOSS": c["regulation_loss"], "TIE": 1 + c["tie_delta"] * (0.5 - p)}[rt]
        assert abs(expected - g) < 1e-9, (rt, p, g)


def test_team_pages_carry_what_the_ledger_needs(db_path):
    conn = sqlite3.connect(str(db_path))
    data = build_team_pages(conn)
    row = data["elo"][0]
    assert len(row) == 8                                                     # 7 original positions + mov_multiplier
    stored = dict(conn.execute("SELECT game_id || '-' || team_id, mov_multiplier FROM elo_game_history"))
    assert row[7] == round(stored[f"{row[0]}-{row[1]}"], 4)
    per_season = defaultdict(float)
    for g, t, coe, season in conn.execute("SELECT h.game_id, h.team_id, h.game_coe, g.season_year FROM hybrid_game_ratings h JOIN games g USING(game_id)"):
        per_season[(t, season)] += coe
    for s in data["team_seasons"]:
        if s[14] is not None:                                                # coe2_season: the ledger's reconciliation target
            assert abs(s[14] - per_season[(s[0], s[1])]) < 0.001


def test_methodology_reads_the_live_parameters_not_copies():
    m = model_params()
    assert m["elo"]["k"] == CFG["elo"]["k"] and m["coe2"]["win_base"] == CFG["coe"]["win_base"]
    v1 = m["coe_v1"]
    assert v1["ITERATIONS"] == build_coefficients.ITERATIONS
    assert v1["PHASE_WEIGHTS"] == build_coefficients.PHASE_WEIGHTS
    assert v1["WITHIN_WINDOW_DECAY_BASE"] == build_coefficients.WITHIN_WINDOW_DECAY_BASE
    assert v1["LOSS_PENALTY"] == 0.15 and v1["PRIOR_REGRESSION"] is not None   # read from inside a function / argparse
    assert m["playoff_bids"] == [[lo, hi, n] for (lo, hi), n in sorted(select_playoff_field_v2.YEAR2_BIDS.items())]
