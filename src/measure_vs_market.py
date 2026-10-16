#!/usr/bin/env python3
"""
The ratings against the closing line: the only benchmark that really tests one.

Beating "always pick the home team" is table stakes, and the Track Record page
already says the ratings do. The market is the real question. A closing spread
is a point estimate of the margin produced by people with money at stake, and
it is the hardest consensus forecast in sport to beat.

This module measures the comparison and writes it; nothing here changes a
rating, and `game_lines` is never a rating input precisely so that this
measurement means something.

------------------------------------------------------------------------
Four comparisons, because "better" means different things
------------------------------------------------------------------------
1. MARGIN ERROR. Both sides state an expected margin, so both can be scored
   against what happened: mean absolute error and root mean square error over
   the games the market priced. This is the cleanest comparison on the page and
   the one the ratings are least likely to win.

2. AGAINST THE SPREAD. The model's disagreement with the line IS a pick: when
   the model expects the home team to beat the number, back the home team. A
   push is excluded, not counted as a half-win. Break-even here is 50% on
   these figures because no vigorish is modelled -- at the usual -110 a bettor
   needs 52.38%, and the page says so rather than letting 51% read as a profit.

3. STRAIGHT UP, WHERE THEY DISAGREE. Who called the winner more often over the
   games where the model's favourite and the market's favourite differ. Over
   every game the two agree far too often for the overall numbers to separate
   them, so agreement is excluded: this is the subset where a disagreement cost
   somebody something.

4. WIN PROBABILITY. The spread turned into a probability through the same
   normal the live win-probability model uses, scored against the rating
   engine's own pregame expectation with Brier and log loss. Two probabilities,
   one scale, no arbitrage of definitions.

------------------------------------------------------------------------
What this measurement cannot claim
------------------------------------------------------------------------
* A CLOSING line is not available when a bet is placed. Every number here is
  measured against the sharpest price of the week, which is the right test of a
  forecast and the wrong test of a betting strategy.
* Only priced games count, which skews modern and skews towards games people
  cared about. Coverage is stated beside every figure.
* No vigorish, no bet sizing, no line shopping. This page says whether the
  ratings carry information the market missed, not whether anyone could have
  made money.
"""
from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from win_probability import (  # noqa: E402
    REGULATION_SECONDS,
    WinProbabilityParams,
    normal_cdf,
)

REPO = Path(__file__).resolve().parents[1]
DEFAULT_DB = REPO / "db" / "league.db"
DEFAULT_OUT = REPO / "data" / "processed" / "market_comparison.json"

# Which provider's number to use, best first. "consensus" is CFBD's own
# average across books and is the closest thing to "the line" for a game; the
# named books are fallbacks for games it did not cover. One provider per game,
# chosen by this order, so a game with five books does not count five times.
PROVIDER_PREFERENCE = ("consensus", "Bovada", "DraftKings", "ESPN Bet", "teamrankings",
                       "numberfire", "William Hill (New Jersey)")

# What a bettor really needs at the standard price of -110, as a reminder that
# 50% is the break-even of this measurement and not of a wager.
VIG_BREAK_EVEN = 110 / 210

GAMES_SQL = """
    SELECT g.game_id,
           g.season_year,
           g.home_score - g.away_score AS final_margin,
           e.elo_diff_adjusted,
           e.elo_expectation
      FROM games g
      JOIN elo_game_history e
        ON e.game_id = g.game_id AND e.team_id = g.home_team_id
     WHERE g.home_score IS NOT NULL AND g.away_score IS NOT NULL
       AND e.elo_diff_adjusted IS NOT NULL
     ORDER BY g.season_year, g.game_id
"""

LINES_SQL = """
    SELECT game_id, provider, market_home_margin
      FROM game_lines
     WHERE market_home_margin IS NOT NULL
"""


def best_line(rows: list[tuple[str, float]]) -> tuple[str, float] | None:
    """One game's lines -> the one provider to use, by PROVIDER_PREFERENCE.

    A provider this does not know about is still usable -- it sorts after the
    named ones, in name order, so the choice is deterministic rather than
    whatever the database happened to return first. Determinism matters: a
    measurement that silently changed which book it read would move its own
    numbers between runs.
    """
    if not rows:
        return None
    ranked = sorted(rows, key=lambda row: (
        PROVIDER_PREFERENCE.index(row[0]) if row[0] in PROVIDER_PREFERENCE
        else len(PROVIDER_PREFERENCE), row[0]))
    return ranked[0]


def load_market_games(conn: sqlite3.Connection, params: WinProbabilityParams) -> list[dict]:
    """Every completed, rated, priced game, with both forecasts on it.

    The model's expected margin is `points_per_elo * elo_diff_adjusted`: the
    same conversion the live win-probability model uses, fitted on regulation
    margins across every season. It is the rating engine's own view of the game
    expressed in points, which is what the market states, so the two are
    comparable without any further choice.
    """
    if not conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='game_lines'").fetchone():
        return []
    lines: dict[int, list[tuple[str, float]]] = {}
    for game_id, provider, margin in conn.execute(LINES_SQL):
        lines.setdefault(int(game_id), []).append((provider, float(margin)))

    rows = []
    for game_id, season, final_margin, elo_diff, elo_expectation in conn.execute(GAMES_SQL):
        chosen = best_line(lines.get(int(game_id), []))
        if chosen is None:
            continue
        provider, market_margin = chosen
        rows.append({
            "game_id": int(game_id),
            "season": int(season),
            "actual_margin": int(final_margin),
            "provider": provider,
            "market_margin": market_margin,
            "model_margin": params.points_per_elo * float(elo_diff),
            "model_p_home": float(elo_expectation),
            # Filled in by `attach_market_probabilities` once the market's own
            # error spread has been measured on these same games.
            "market_p_home": None,
            "home_won": 1.0 if final_margin > 0 else (0.0 if final_margin < 0 else 0.5),
        })
    return rows


def attach_market_probabilities(rows: list[dict], params: WinProbabilityParams) -> float:
    """Turn every market margin into a probability, and return the sigma used.

    Measured on the rows it is applied to: the market's root mean square margin
    error IS the width of the normal its own numbers should be read through.
    """
    if not rows:
        return params.sigma
    errors = [row["actual_margin"] - row["market_margin"] for row in rows]
    sigma = math.sqrt(sum(e * e for e in errors) / len(errors))
    if not sigma > 0:
        sigma = params.sigma
    for row in rows:
        row["market_p_home"] = spread_to_probability(row["market_margin"], params, sigma)
    return sigma


def spread_to_probability(market_margin: float, params: WinProbabilityParams,
                          sigma: float | None = None) -> float:
    """A spread -> P(home wins), through a normal of the given width.

    The market states a margin and the rating engine states a probability, so
    comparing them needs one scale, and converting the spread is the honest
    direction: the rating engine's probability is what the site publishes and
    is left exactly as it is.

    `sigma` is how far games land from the number being converted, and it has
    to be the MARKET's own spread of errors rather than the model's. The live
    win-probability model's 16.65 is the spread around the RATINGS' expected
    margin; the market forecasts margins better than that, so reading its
    spreads through the wider curve would hand it probabilities that are too
    timid and then score it on them. `build_report` measures it from the same
    games (the root mean square error of the market's own margin) and passes it
    in. Without one, the model's sigma is used and the comparison is only as
    good as that assumption.
    """
    spread = params.sigma if sigma is None else sigma
    if not spread > 0:
        raise ValueError(f"sigma must be positive, got {spread}")
    upper = normal_cdf((params.tie_half_width - market_margin) / spread)
    lower = normal_cdf((-params.tie_half_width - market_margin) / spread)
    return (1.0 - upper) + (upper - lower) * params.overtime_home_win


def margin_errors(rows: list[dict]) -> dict:
    """Mean absolute and root mean square error of each expected margin."""
    if not rows:
        return {}
    out = {}
    for who, key in (("model", "model_margin"), ("market", "market_margin")):
        errors = [row["actual_margin"] - row[key] for row in rows]
        out[f"{who}_mae"] = sum(abs(e) for e in errors) / len(errors)
        out[f"{who}_rmse"] = math.sqrt(sum(e * e for e in errors) / len(errors))
        out[f"{who}_bias"] = sum(errors) / len(errors)
    return out


def against_the_spread(rows: list[dict]) -> dict:
    """The model's disagreement with the line, scored as a pick.

    The model takes the home team when it expects a bigger home margin than the
    line does, and the away team otherwise. A game that lands exactly on the
    number is a push: excluded, because counting it either way would be
    inventing a result the bettor never got.
    """
    wins = losses = pushes = 0
    for row in rows:
        margin, line = row["actual_margin"], row["market_margin"]
        if margin == line:
            pushes += 1
            continue
        model_takes_home = row["model_margin"] > line
        home_covered = margin > line
        if model_takes_home == home_covered:
            wins += 1
        else:
            losses += 1
    decided = wins + losses
    return {
        "wins": wins, "losses": losses, "pushes": pushes, "decided": decided,
        "rate": (wins / decided) if decided else None,
        # Stated so nobody reads 51% as a profit.
        "break_even_with_vig": VIG_BREAK_EVEN,
    }


def straight_up_disagreements(rows: list[dict]) -> dict:
    """Who called the winner, over the games where the two favourites differ.

    A game the two agree on tells you nothing about which is better, and over
    every game they agree so often that the difference drowns. A pick'em (line
    exactly 0) is not a market favourite at all, so it is excluded too -- as is
    a game the model itself calls exactly even.
    """
    model_right = market_right = 0
    for row in rows:
        model_home = row["model_margin"] > 0
        market_home = row["market_margin"] > 0
        if row["market_margin"] == 0 or row["model_margin"] == 0:
            continue
        if model_home == market_home:
            continue
        if row["home_won"] == 0.5:
            continue            # a tie makes neither of them right
        home_won = row["home_won"] == 1.0
        if model_home == home_won:
            model_right += 1
        else:
            market_right += 1
    games = model_right + market_right
    return {
        "games": games, "model_right": model_right, "market_right": market_right,
        "model_rate": (model_right / games) if games else None,
    }


def probability_scores(rows: list[dict]) -> dict:
    """Brier and log loss for both probabilities, on the same games.

    A tie is scored as a half-win for both, which is what the rating engine's
    own validation does, so the two pages cannot disagree about what a draw is.

    Rows whose market probability has not been attached yet are skipped rather
    than guessed at, so a summary built before `attach_market_probabilities`
    reports no probability scores instead of inventing some.
    """
    rows = [row for row in rows
            if row.get("model_p_home") is not None and row.get("market_p_home") is not None]
    if not rows:
        return {}
    out = {"scored": len(rows)}
    for who, key in (("model", "model_p_home"), ("market", "market_p_home")):
        brier = sum((row[key] - row["home_won"]) ** 2 for row in rows) / len(rows)
        clipped = [(min(max(row[key], 1e-12), 1 - 1e-12), row["home_won"]) for row in rows]
        log_loss = -sum(y * math.log(p) + (1 - y) * math.log(1 - p)
                        for p, y in clipped) / len(clipped)
        out[f"{who}_brier"] = brier
        out[f"{who}_log_loss"] = log_loss
    return out


def summarise(rows: list[dict]) -> dict:
    return {
        "games": len(rows),
        "first_season": min((r["season"] for r in rows), default=None),
        "last_season": max((r["season"] for r in rows), default=None),
        **margin_errors(rows),
        **probability_scores(rows),
        "ats": against_the_spread(rows),
        "disagreements": straight_up_disagreements(rows),
    }


def by_season(rows: list[dict]) -> list[dict]:
    seasons = sorted({row["season"] for row in rows})
    out = []
    for season in seasons:
        season_rows = [row for row in rows if row["season"] == season]
        summary = summarise(season_rows)
        summary["season"] = season
        out.append(summary)
    return out


def coverage(conn: sqlite3.Connection, rows: list[dict]) -> list[dict]:
    """How much of each season the market priced, so no figure stands alone.

    A season with 40 priced games out of 700 is not evidence about that season,
    and a reader cannot tell that from a hit rate.
    """
    totals = dict(conn.execute(
        "SELECT season_year, COUNT(*) FROM games "
        " WHERE home_score IS NOT NULL AND away_score IS NOT NULL "
        " GROUP BY season_year"))
    counts: dict[int, int] = {}
    for row in rows:
        counts[row["season"]] = counts.get(row["season"], 0) + 1
    return [{"season": season, "priced": counts.get(season, 0),
             "games": int(totals.get(season, 0)),
             "share": (counts.get(season, 0) / totals[season]) if totals.get(season) else None}
            for season in sorted(totals)]


def build_report(conn: sqlite3.Connection,
                 params: WinProbabilityParams | None = None) -> dict | None:
    """The whole comparison, or None when there are no priced games.

    None rather than an empty shell, so the page can be absent instead of
    present and blank -- the rule src/accuracy.py and the rest-of-season
    projection both follow.
    """
    params = params or WinProbabilityParams.from_config()
    rows = load_market_games(conn, params)
    if not rows:
        return None
    market_sigma = attach_market_probabilities(rows, params)
    providers: dict[str, int] = {}
    for row in rows:
        providers[row["provider"]] = providers.get(row["provider"], 0) + 1
    return {
        "overall": summarise(rows),
        "by_season": by_season(rows),
        "coverage": coverage(conn, rows),
        "providers": [{"provider": name, "games": count}
                      for name, count in sorted(providers.items(), key=lambda kv: -kv[1])],
        "points_per_elo": params.points_per_elo,
        "sigma": params.sigma,
        # The width the market's own spreads were read through, measured on
        # these games. Published because every market probability depends on it.
        "market_sigma": market_sigma,
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=str(DEFAULT_DB))
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    args = ap.parse_args(argv)

    conn = sqlite3.connect(args.db)
    try:
        report = build_report(conn)
    finally:
        conn.close()
    if report is None:
        print("No priced games found; run the betting-line backfill and the pipeline first.",
              file=sys.stderr)
        return 1

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")

    overall = report["overall"]
    ats, dis = overall["ats"], overall["disagreements"]
    print(f"{overall['games']} priced games, {overall['first_season']}-{overall['last_season']}")
    print(f"  margin error   model {overall['model_mae']:.3f} MAE / {overall['model_rmse']:.3f} RMSE")
    print(f"                 market {overall['market_mae']:.3f} MAE / {overall['market_rmse']:.3f} RMSE")
    print(f"  win prob       model Brier {overall['model_brier']:.5f}, "
          f"market Brier {overall['market_brier']:.5f}")
    if ats["rate"] is not None:
        print(f"  against the spread  {ats['wins']}-{ats['losses']}-{ats['pushes']} "
              f"({ats['rate']:.4f}; {VIG_BREAK_EVEN:.4f} needed at -110)")
    if dis["model_rate"] is not None:
        print(f"  where they disagree  model {dis['model_right']}-{dis['market_right']} "
              f"({dis['model_rate']:.4f}) over {dis['games']} games")
    print(f"Wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
