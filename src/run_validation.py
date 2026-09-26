#!/usr/bin/env python3
"""
The validation harness (MODEL-07): one command that scores every model this
repository predicts with, and writes the result somewhere a later run can be
compared against.

    python src/run_validation.py --db db/league.db
    python src/run_validation.py --from-season 2001

What it produces:

    data/processed/validation_report.json   the full record -- every metric,
                                            per season, plus the calibration
                                            table and the rank diagnostics
    data/processed/validation_by_season.csv the per-model, per-season rows,
                                            for a spreadsheet or a chart

Both are rewritten in full on every run, and the JSON records the database and
config it read, the window it scored and a content hash of the predictions, so
two reports can be compared without guessing whether they describe the same
data. It never writes to the database and never modifies config.

## What it scores, and why each is in the list

    elo                  elo_game_history.elo_expectation -- the live rating
                         engine's own pregame number
    hybrid               hybrid_game_ratings.hybrid_expectation -- Elo blended
                         with frozen prior-season CoE
    elo_on_hybrid_games  elo again, restricted to the games hybrid predicts.
                         The hybrid layer covers fewer games (it needs a frozen
                         prior-season CoE, which the earliest seasons have no
                         history for), so comparing its Brier against elo's
                         full-history Brier compares two different game sets.
                         This row is the like-for-like one.
    baseline_coin_flip   0.5 for every game. The floor; a model that does not
                         clear this is not a model.
    baseline_home_rate   the observed home-win rate, constant. Harder than it
                         sounds: home-field advantage alone is real signal, and
                         it is handed a fact measured on the very games it is
                         scored against, so anything that beats it has beaten
                         a baseline with hindsight.

Predictions are read out of the database rather than recomputed, so nothing
here can accidentally score a rating formed after the game it is predicting.
See src/validation.py's docstring on what makes these walk-forward.

## The rank diagnostics

CoE measures achievement, so it cannot be judged by prediction metrics at all
-- the design doc is explicit that optimising CoE for prediction accuracy is a
category error. The diagnostics section instead reports whether it behaves like
a coherent measure: v1-vs-v2 rank agreement per season, year-over-year
stability of each, and the association between a season's achievement and the
schedule strength a team actually faced. A measure that rewarded farming weak
opponents would show up there as a negative association.

Team-seasons with fewer than --confidence-games games are excluded from the
rank diagnostics, for the reason compare_models.py documents at length: a team
with one game played produces a spectacular, entirely spurious disagreement.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from validation import (  # noqa: E402
    DEFAULT_BINS,
    association,
    rank_agreement,
    rank_stability,
    walk_forward_report,
)

REPO = Path(__file__).resolve().parent.parent
DEFAULT_JSON = REPO / "data" / "processed" / "validation_report.json"
DEFAULT_CSV = REPO / "data" / "processed" / "validation_by_season.csv"

# Matches compare_models.py's own threshold, which in turn matches
# build_coefficients.py's early-season confidence blend. Not re-derived here.
CONFIDENCE_GAMES = 8

CSV_FIELDS = ["model", "season_year", "n", "brier", "log_loss", "accuracy",
              "base_rate", "brier_skill", "calibration_error"]


def _outcome(home_score: int, away_score: int) -> float:
    """From the home team's side: 1 win, 0 loss, 0.5 tie."""
    if home_score > away_score:
        return 1.0
    if away_score > home_score:
        return 0.0
    return 0.5


def elo_rows(conn: sqlite3.Connection) -> list[tuple[int, int, float, float]]:
    """
    (game_id, season, predicted, observed) for every completed game, from the
    home team's row only -- the away row is the same information mirrored.
    """
    rows = conn.execute(
        """
        SELECT g.game_id, g.season_year, e.elo_expectation, g.home_score, g.away_score
        FROM elo_game_history e
        JOIN games g ON g.game_id = e.game_id
        WHERE e.team_id = g.home_team_id
          AND g.home_score IS NOT NULL AND g.away_score IS NOT NULL
        ORDER BY g.season_year, g.game_date, g.game_id
        """
    ).fetchall()
    return [(r["game_id"], r["season_year"], r["elo_expectation"],
             _outcome(r["home_score"], r["away_score"])) for r in rows]


def hybrid_rows(conn: sqlite3.Connection) -> list[tuple[int, int, float, float]]:
    """
    Same shape for the hybrid expectation. Rows whose expectation is NULL are
    skipped, not treated as 0.5: the hybrid layer records an absent prediction
    that way, and scoring it as a coin flip would credit the model for a guess
    it never made.
    """
    rows = conn.execute(
        """
        SELECT g.game_id, g.season_year, h.hybrid_expectation, g.home_score, g.away_score
        FROM hybrid_game_ratings h
        JOIN games g ON g.game_id = h.game_id
        WHERE h.team_id = g.home_team_id
          AND h.hybrid_expectation IS NOT NULL
          AND g.home_score IS NOT NULL AND g.away_score IS NOT NULL
        ORDER BY g.season_year, g.game_date, g.game_id
        """
    ).fetchall()
    return [(r["game_id"], r["season_year"], r["hybrid_expectation"],
             _outcome(r["home_score"], r["away_score"])) for r in rows]


def entries(rows: list[tuple[int, int, float, float]]) -> list[tuple[int, float, float]]:
    """Drop the game_id, leaving what validation.py scores."""
    return [(season, p, s) for _, season, p, s in rows]


def restricted_to(rows: list[tuple[int, int, float, float]],
                  game_ids: set) -> list[tuple[int, int, float, float]]:
    """The same rows, keeping only the games another model also predicted."""
    return [row for row in rows if row[0] in game_ids]


def constant_entries(scored: list[tuple[int, float, float]],
                     probability: float | None = None) -> list[tuple[int, float, float]]:
    """
    A baseline over the same games: one fixed probability for all of them. With
    no probability given it uses the observed base rate of these very games,
    which is the hindsight-assisted version described in the module docstring.
    """
    if probability is None:
        observed = [s for _, _, s in scored]
        probability = (sum(observed) / len(observed)) if observed else 0.5
    return [(season, probability, s) for season, _, s in scored]


def _games_played(conn: sqlite3.Connection) -> dict:
    rows = conn.execute(
        """
        SELECT g.season_year, e.team_id, COUNT(*) AS n
        FROM elo_game_history e
        JOIN games g ON g.game_id = e.game_id
        WHERE g.home_score IS NOT NULL AND g.away_score IS NOT NULL
        GROUP BY g.season_year, e.team_id
        """
    ).fetchall()
    return {(r["season_year"], r["team_id"]): r["n"] for r in rows}


def _season_coe2(conn: sqlite3.Connection) -> dict:
    """{(season, team_id): season CoE 2.0}, latest formula_version per row."""
    rows = conn.execute(
        """
        SELECT season_year, team_id, season_coe2
        FROM team_coe2_by_season
        WHERE formula_version = (SELECT MAX(formula_version) FROM team_coe2_by_season)
        """
    ).fetchall()
    return {(r["season_year"], r["team_id"]): r["season_coe2"] for r in rows}


def _v1_ratings(conn: sqlite3.Connection, csv_path: Path) -> dict:
    """CoE v1's team-season ratings, keyed by team_id so both models share a key."""
    if not csv_path.exists():
        return {}
    name_to_id = {r["team_name"]: r["team_id"]
                  for r in conn.execute("SELECT team_id, team_name FROM teams")}
    out = {}
    with csv_path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            team_id = name_to_id.get(row["team_name"])
            if team_id is not None:
                out[(int(row["season_year"]), team_id)] = float(row["rating"])
    return out


def _mean_opponent_elo(conn: sqlite3.Connection) -> dict:
    """
    {(season, team_id): mean opponent PREGAME Elo} -- the strength of schedule a
    team actually faced, measured from what was known before each game rather
    than from end-of-season ratings.
    """
    rows = conn.execute(
        """
        SELECT g.season_year, e.team_id, AVG(e.opponent_pregame_elo) AS strength
        FROM elo_game_history e
        JOIN games g ON g.game_id = e.game_id
        WHERE g.home_score IS NOT NULL AND g.away_score IS NOT NULL
        GROUP BY g.season_year, e.team_id
        """
    ).fetchall()
    return {(r["season_year"], r["team_id"]): r["strength"] for r in rows}


def _by_season(flat: dict, eligible: set) -> dict:
    """Regroup {(season, team): value} into {season: {team: value}}, eligible only."""
    out: dict[int, dict] = defaultdict(dict)
    for (season, team), value in flat.items():
        if (season, team) in eligible:
            out[season][team] = value
    return dict(out)


def diagnostics(conn: sqlite3.Connection, v1_csv: Path, confidence_games: int) -> dict:
    """
    The achievement-side report: agreement, stability, and the associations that
    would expose a measure rewarding a weak schedule. Every section says how
    many team-seasons it covers, so a thin one cannot be read as a strong
    result.
    """
    played = _games_played(conn)
    eligible = {key for key, n in played.items() if n >= confidence_games}

    coe2 = _by_season(_season_coe2(conn), eligible)
    v1 = _by_season(_v1_ratings(conn, v1_csv), eligible)
    strength = _by_season(_mean_opponent_elo(conn), eligible)
    wins = _by_season({key: n for key, n in played.items()}, eligible)

    shared_seasons = sorted(set(coe2) & set(v1))
    return {
        "confidence_games": confidence_games,
        "eligible_team_seasons": len(eligible),
        "v1_v2_agreement_by_season": [
            {"season_year": season, **rank_agreement(v1[season], coe2[season])}
            for season in shared_seasons
        ],
        "stability": {
            "coe2": rank_stability(coe2),
            "v1": rank_stability(v1),
        },
        "associations": {
            "coe2": [association(coe2[season], strength.get(season, {}), "mean_opponent_pregame_elo")
                     | {"season_year": season} for season in sorted(coe2)],
        },
        "association_notes": {
            "mean_opponent_pregame_elo": "Negative values would mean the measure rewards a weaker "
                                         "schedule. Read the sign before the size.",
            "games_counted": f"{len(wins)} seasons carry eligible team-seasons.",
        },
    }


def _describe_path(path: str) -> str:
    """
    Repo-relative where possible, absolute otherwise. The report is committed by
    the scheduled refresh, so an absolute path would record whichever machine
    wrote it and churn the file on every environment change.
    """
    resolved = Path(path).resolve()
    try:
        return str(resolved.relative_to(REPO))
    except ValueError:
        return str(resolved)


def _fingerprint(entries_by_model: dict) -> str:
    """
    A content hash of every prediction scored, so two reports can be told apart
    when the code is identical but the data underneath moved.
    """
    digest = hashlib.sha256()
    for model in sorted(entries_by_model):
        digest.update(model.encode())
        for season, p, s in entries_by_model[model]:
            digest.update(f"{season}:{p!r}:{s!r}".encode())
    return digest.hexdigest()[:16]


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default=str(REPO / "db" / "league.db"))
    p.add_argument("--v1-ratings-csv", default=str(REPO / "data" / "processed" / "team_ratings_by_season.csv"))
    p.add_argument("--json-out", default=str(DEFAULT_JSON))
    p.add_argument("--csv-out", default=str(DEFAULT_CSV))
    p.add_argument("--from-season", type=int, default=None,
                   help="score only games from this season on (e.g. past the flat-1500 Elo burn-in)")
    p.add_argument("--bins", type=int, default=DEFAULT_BINS,
                   help="calibration bins (default 10, i.e. deciles)")
    p.add_argument("--confidence-games", type=int, default=CONFIDENCE_GAMES,
                   help="minimum games played for a team-season to enter the rank diagnostics")
    p.add_argument("--no-diagnostics", action="store_true",
                   help="prediction metrics only; skip the achievement-side rank report")
    args = p.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    elo = elo_rows(conn)
    if not elo:
        raise SystemExit(f"{args.db} has no scored games in elo_game_history -- "
                         "build the ratings first (python src/build_elo.py).")
    hybrid = hybrid_rows(conn)
    hybrid_games = {row[0] for row in hybrid}

    entries_by_model = {
        "elo": entries(elo),
        "hybrid": entries(hybrid),
        "baseline_coin_flip": constant_entries(entries(elo), 0.5),
        "baseline_home_rate": constant_entries(entries(elo)),
    }
    # Only worth a row when the two really do cover different games; otherwise
    # it would be an exact duplicate of `elo` pretending to be a second result.
    if hybrid_games and hybrid_games != {row[0] for row in elo}:
        entries_by_model["elo_on_hybrid_games"] = entries(restricted_to(elo, hybrid_games))

    reports = {name: walk_forward_report(name, entries, bins=args.bins,
                                         from_season=args.from_season)
               for name, entries in entries_by_model.items()}

    report = {
        "generated_for": {
            "db": _describe_path(args.db),
            "v1_ratings_csv": _describe_path(args.v1_ratings_csv),
            "from_season": args.from_season,
            "bins": args.bins,
            "prediction_fingerprint": _fingerprint(entries_by_model),
        },
        "models": reports,
        "diagnostics": None if args.no_diagnostics
        else diagnostics(conn, Path(args.v1_ratings_csv), args.confidence_games),
    }
    conn.close()

    json_out = Path(args.json_out)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    # sort_keys and a trailing newline so a rerun on unchanged data produces a
    # byte-identical file and shows up as no diff at all.
    json_out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    csv_out = Path(args.csv_out)
    with csv_out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for name in sorted(reports):
            for row in reports[name]["by_season"]:
                writer.writerow({"model": name, **{k: row.get(k) for k in CSV_FIELDS[1:]}})

    window = f" from {args.from_season}" if args.from_season else ""
    print(f"Scored {len(elo):,} games{window}. Wrote {json_out} and {csv_out}.\n")
    print(f"{'model':<20}{'n':>9}{'Brier':>10}{'log loss':>11}{'acc':>8}{'skill':>9}{'calib':>9}")
    for name in sorted(reports, key=lambda k: (reports[k]["overall"]["brier"] is None,
                                               reports[k]["overall"]["brier"])):
        o = reports[name]["overall"]
        if o["n"] == 0:
            print(f"{name:<20}{0:>9}   (nothing to score)")
            continue
        print(f"{name:<20}{o['n']:>9,}{o['brier']:>10.5f}{o['log_loss']:>11.5f}"
              f"{o['accuracy']:>8.3f}{o['brier_skill']:>9.3f}{o['calibration_error']:>9.5f}")
    print("\nLower Brier, log loss and calibration error are better; higher accuracy and "
          "skill are better.\nSkill is measured against baseline_home_rate's own reference, "
          "so a positive value beats\na constant prediction of the observed home-win rate.")
    if "elo_on_hybrid_games" in reports:
        covered = reports["hybrid"]["overall"]["n"]
        full = reports["elo"]["overall"]["n"]
        print(f"\nhybrid predicts {covered:,} of {full:,} games, so compare it against "
              "elo_on_hybrid_games,\nnot against elo -- the full-history row includes seasons "
              "hybrid has no frozen CoE for.")

    if report["diagnostics"]:
        d = report["diagnostics"]
        agreements = [r for r in d["v1_v2_agreement_by_season"] if r["spearman"] is not None]
        if agreements:
            mean_rho = sum(r["spearman"] for r in agreements) / len(agreements)
            print(f"\nCoE v1 vs 2.0: mean rank correlation {mean_rho:+.3f} across "
                  f"{len(agreements)} seasons ({d['eligible_team_seasons']:,} eligible team-seasons).")
        assoc = [r for r in d["associations"]["coe2"] if r["spearman"] is not None]
        if assoc:
            mean_assoc = sum(r["spearman"] for r in assoc) / len(assoc)
            print(f"CoE 2.0 vs schedule strength faced: mean {mean_assoc:+.3f} "
                  "(negative would mean a weak schedule pays).")


if __name__ == "__main__":
    main()
