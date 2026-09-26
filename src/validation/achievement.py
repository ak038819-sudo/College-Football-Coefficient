"""
CoE achievement diagnostics (MODEL-07).

CoE is not a forecast, so scoring it with Brier would be a category error: a
rating optimised to predict results is just Elo. CoE is supposed to answer "what
did this team earn against the schedule it actually played?", and these
diagnostics interrogate exactly that, each one a question with a defensible
expected direction rather than a single number to maximise.

  schedule_strength   Does a harder schedule earn more, holding wins roughly
                      equal? If two 9-3 teams score the same whether they played
                      the toughest or the softest schedule in the country, the
                      system is not measuring difficulty at all.
  elite_win_reward    Is beating a strong team worth more than beating a weak
                      one? The direct statement of what CoE claims to do.
  cupcake_resistance  Can a team match an elite win by stacking easy ones? Some
                      substitution is inevitable in any additive system; the
                      question is how many soft wins it takes, and whether that
                      number is defensible.
  conference_behavior Do conference ratings separate, and do internal games stay
                      out of conference strength? An intra-league game cannot
                      make a conference stronger relative to the outside world.
  historical_overlap  Do the model's top teams resemble the ones the sport
                      actually selected? Agreement is not the goal -- the whole
                      project exists because polls are opaque -- but a system
                      that never agrees is describing a different sport.
  edge_cases          The pathological shapes: undefeated against nobody, a
                      losing record against everybody, a one-game season, a
                      season of ties. Each should produce a defensible number
                      rather than a crash or a silent zero.

Every diagnostic returns its own numbers plus a `reading` string stating what
the value means, so a report is interpretable without this docstring.
"""
from __future__ import annotations

import sqlite3
import statistics
from collections import defaultdict

ELITE_QUANTILE = 0.20      # top/bottom fifth of opponents by pregame Elo


def _corr(xs, ys) -> float | None:
    """Pearson correlation, or None when either side has no spread."""
    if len(xs) < 3:
        return None
    mx, my = statistics.fmean(xs), statistics.fmean(ys)
    sxx = sum((x - mx) ** 2 for x in xs)
    syy = sum((y - my) ** 2 for y in ys)
    if sxx <= 0 or syy <= 0:
        return None
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    return sxy / (sxx ** 0.5 * syy ** 0.5)


def _game_rows(conn: sqlite3.Connection) -> list[tuple]:
    """(team_id, season, opponent pregame Elo, game_coe, won) for valued games."""
    return conn.execute(
        """SELECT h.team_id, g.season_year, e.opponent_pregame_elo, h.game_coe,
                  CASE WHEN (h.team_id = g.home_team_id AND g.home_score > g.away_score)
                         OR (h.team_id = g.away_team_id AND g.away_score > g.home_score)
                       THEN 1 ELSE 0 END
           FROM hybrid_game_ratings h
           JOIN games g ON g.game_id = h.game_id
           JOIN elo_game_history e ON e.game_id = h.game_id AND e.team_id = h.team_id
           WHERE h.game_coe IS NOT NULL
             AND g.home_score IS NOT NULL AND g.away_score IS NOT NULL"""
    ).fetchall()


def _team_seasons(conn: sqlite3.Connection) -> list[dict]:
    """Per team-season: wins, games, mean opponent pregame Elo (SoS), season CoE 2.0."""
    agg: dict = defaultdict(lambda: {"wins": 0.0, "games": 0, "opp": 0.0, "coe": 0.0})
    for team_id, season, opp_elo, coe, won in _game_rows(conn):
        a = agg[(team_id, season)]
        a["wins"] += won
        a["games"] += 1
        a["opp"] += opp_elo
        a["coe"] += coe
    return [{"team_id": t, "season": s, "wins": a["wins"], "games": a["games"],
             "sos": a["opp"] / a["games"], "coe": a["coe"]}
            for (t, s), a in agg.items() if a["games"]]


def schedule_strength_sensitivity(conn: sqlite3.Connection) -> dict:
    """
    Correlation between strength of schedule and season CoE among team-seasons
    with the SAME win total, then averaged over win totals. Comparing within a
    win bucket is the point: without it the correlation mostly measures that good
    teams win more.
    """
    rows = _team_seasons(conn)
    by_wins: dict = defaultdict(list)
    for r in rows:
        by_wins[round(r["wins"])].append(r)
    per_bucket = []
    for wins, group in sorted(by_wins.items()):
        if len(group) < 20:
            continue
        c = _corr([g["sos"] for g in group], [g["coe"] for g in group])
        if c is not None:
            per_bucket.append({"wins": wins, "n": len(group), "correlation": round(c, 4)})
    mean_c = statistics.fmean([b["correlation"] for b in per_bucket]) if per_bucket else None
    return {"mean_correlation": None if mean_c is None else round(mean_c, 4),
            "per_win_total": per_bucket,
            "reading": "Positive means a harder schedule earns more at the same win total, "
                       "which is what CoE claims to do. Near zero would mean difficulty is "
                       "not being measured; negative would mean it is being punished."}


def elite_win_reward(conn: sqlite3.Connection, quantile: float = ELITE_QUANTILE) -> dict:
    """Mean Game CoE for a win over a top-quantile opponent vs. a bottom-quantile one."""
    rows = [r for r in _game_rows(conn) if r[4] == 1]
    if len(rows) < 50:
        return {"reading": "not enough valued wins to judge", "n": len(rows)}
    elos = sorted(r[2] for r in rows)
    lo_cut = elos[int(len(elos) * quantile)]
    hi_cut = elos[int(len(elos) * (1 - quantile))]
    strong = [r[3] for r in rows if r[2] >= hi_cut]
    weak = [r[3] for r in rows if r[2] <= lo_cut]
    if not strong or not weak:
        return {"reading": "opponent strength has no spread", "n": len(rows)}
    s_mean, w_mean = statistics.fmean(strong), statistics.fmean(weak)
    return {"elite_win_mean": round(s_mean, 4), "elite_n": len(strong),
            "weak_win_mean": round(w_mean, 4), "weak_n": len(weak),
            "ratio": round(s_mean / w_mean, 4) if w_mean else None,
            "opponent_elo_cutoffs": [round(lo_cut, 1), round(hi_cut, 1)],
            "reading": "Ratio above 1 means beating a strong team is worth more than beating a "
                       "weak one. A ratio near 1 would mean opponent quality barely matters."}


def cupcake_resistance(conn: sqlite3.Connection, quantile: float = ELITE_QUANTILE) -> dict:
    """
    How many bottom-quantile wins it takes to equal one top-quantile win. The
    number that matters for "can a team schedule its way to a high rating?".
    """
    e = elite_win_reward(conn, quantile)
    if "ratio" not in e or not e["ratio"]:
        return {"reading": e.get("reading", "unavailable")}
    return {"weak_wins_per_elite_win": round(e["ratio"], 3),
            "reading": f"It takes about {e['ratio']:.2f} wins over bottom-{int(quantile * 100)}% "
                       "opponents to match one win over a top-"
                       f"{int(quantile * 100)}% opponent. Higher is more resistant to schedule "
                       "padding; a value near 1 would mean a soft schedule is as good as a hard one."}


def conference_behavior(conn: sqlite3.Connection) -> dict:
    """
    Whether conference CoE 2.0 separates conferences, and -- the invariant that
    actually matters -- whether any intra-conference game leaked into it.
    """
    # The internal-game count is answerable from games and membership alone, so it
    # is computed even when the conference rollup has not been built -- a partially
    # built database should still report what it can rather than nothing.
    has = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                       "AND name='conference_coe2_by_season'").fetchone()
    latest, vals, counted = None, [], None
    if has:
        latest = conn.execute("SELECT MAX(season_year) FROM conference_coe2_by_season").fetchone()[0]
        vals = [v for (v,) in conn.execute(
            "SELECT conference_coe2 FROM conference_coe2_by_season WHERE season_year = ?", (latest,))]
        counted = conn.execute("SELECT COALESCE(SUM(external_games_counted), 0) "
                               "FROM conference_coe2_by_season").fetchone()[0]

    # The invariant: recount external games independently and compare.
    membership = {(t, s): c for t, s, c in conn.execute(
        "SELECT team_id, season_year, conference_real FROM team_membership_by_season")}
    internal = 0
    for team_id, season, phase, home, away in conn.execute(
            """SELECT h.team_id, g.season_year, g.game_phase, g.home_team_id, g.away_team_id
               FROM hybrid_game_ratings h JOIN games g ON g.game_id = h.game_id
               WHERE h.game_coe IS NOT NULL AND g.game_phase = 'regular'"""):
        own = membership.get((team_id, season))
        opp = membership.get((away if team_id == home else home, season))
        if own is not None and own == opp:
            internal += 1
    return {"season": latest, "conferences": len(vals),
            "spread": round(max(vals) - min(vals), 3) if vals else None,
            "stdev": round(statistics.pstdev(vals), 3) if len(vals) > 1 else None,
            "internal_regular_season_games": internal,
            "external_games_counted": counted,
            "reading": "Conference strength counts only games against the outside world, so the "
                       f"{internal:,} intra-conference regular-season team-games must not appear in "
                       "the counted total. Spread shows whether the measure separates conferences "
                       "at all." + ("" if has else " Conference CoE 2.0 has not been built, so only "
                                                   "the internal-game count is available.")}


def historical_overlap(conn: sqlite3.Connection, top_n: int = 4) -> dict:
    """
    Overlap between the model's top teams by season CoE 2.0 and the teams the
    College Football Playoff actually selected. Agreement is context, not a
    target: this project exists because the selection process is opaque.
    """
    seasons = [s for (s,) in conn.execute(
        """SELECT DISTINCT season_year FROM games
           WHERE game_phase = 'cfp' AND season_year >= 2014 AND home_score IS NOT NULL
           ORDER BY season_year""")]
    per_season, hits, total = [], 0, 0
    for season in seasons:
        selected = {t for (t,) in conn.execute(
            """SELECT home_team_id FROM games WHERE game_phase='cfp' AND season_year=?
               UNION SELECT away_team_id FROM games WHERE game_phase='cfp' AND season_year=?""",
            (season, season))}
        ranked = [t for (t,) in conn.execute(
            """SELECT h.team_id FROM hybrid_game_ratings h JOIN games g ON g.game_id = h.game_id
               WHERE g.season_year = ? AND h.game_coe IS NOT NULL
               GROUP BY h.team_id ORDER BY SUM(h.game_coe) DESC LIMIT ?""", (season, top_n))]
        if not ranked or not selected:
            continue
        overlap = len(set(ranked) & selected)
        per_season.append({"season": season, "model_top": len(ranked),
                           "selected": len(selected), "overlap": overlap})
        hits += overlap
        total += len(ranked)
    return {"seasons": len(per_season), "per_season": per_season,
            "overlap_rate": round(hits / total, 4) if total else None,
            "reading": f"Share of the model's top {top_n} by season CoE 2.0 that the real playoff "
                       "also took. Informative context, not a target -- the model includes the "
                       "postseason games themselves, so some agreement is circular."}


def edge_cases(conn: sqlite3.Connection) -> dict:
    """
    The pathological shapes, named with real examples so a regression is visible
    rather than abstract. Each must produce a defensible number, not a crash and
    not a silent zero.
    """
    rows = _team_seasons(conn)
    if not rows:
        return {"reading": "no valued team-seasons"}
    by_games = defaultdict(list)
    for r in rows:
        by_games[r["games"]].append(r)

    def pick(pred, key, reverse=False):
        hits = [r for r in rows if pred(r)]
        return max(hits, key=key) if reverse else (min(hits, key=key) if hits else None)

    undefeated = [r for r in rows if r["games"] >= 8 and r["wins"] == r["games"]]
    winless = [r for r in rows if r["games"] >= 8 and r["wins"] == 0]
    soft_undefeated = min(undefeated, key=lambda r: r["sos"]) if undefeated else None
    hard_winless = max(winless, key=lambda r: r["sos"]) if winless else None
    single = sorted(by_games.get(1, []), key=lambda r: -r["coe"])[:1]
    return {
        "undefeated_vs_weakest_schedule": None if not soft_undefeated else {
            "team_id": soft_undefeated["team_id"], "season": soft_undefeated["season"],
            "games": soft_undefeated["games"], "sos": round(soft_undefeated["sos"], 1),
            "coe": round(soft_undefeated["coe"], 3)},
        "winless_vs_hardest_schedule": None if not hard_winless else {
            "team_id": hard_winless["team_id"], "season": hard_winless["season"],
            "games": hard_winless["games"], "sos": round(hard_winless["sos"], 1),
            "coe": round(hard_winless["coe"], 3)},
        "single_game_season": None if not single else {
            "team_id": single[0]["team_id"], "season": single[0]["season"],
            "coe": round(single[0]["coe"], 3)},
        "negative_season_totals": sum(1 for r in rows if r["coe"] < 0),
        "zero_season_totals": sum(1 for r in rows if r["coe"] == 0),
        "reading": "An undefeated team against nobody should still score below a strong team "
                   "against everybody; a winless season should not go negative under a model whose "
                   "losses are worth zero; a one-game season should be small, not extrapolated.",
    }


DIAGNOSTICS = {
    "schedule_strength": schedule_strength_sensitivity,
    "elite_win_reward": elite_win_reward,
    "cupcake_resistance": cupcake_resistance,
    "conference_behavior": conference_behavior,
    "historical_overlap": historical_overlap,
    "edge_cases": edge_cases,
}


def run_all(conn: sqlite3.Connection) -> dict:
    """Every achievement diagnostic. A diagnostic that cannot run says so rather
    than failing the whole report -- a partially built database should still
    produce whatever is answerable."""
    out = {}
    for name, fn in DIAGNOSTICS.items():
        try:
            out[name] = fn(conn)
        except sqlite3.Error as e:
            out[name] = {"reading": f"unavailable: {e}"}
    return out
