"""
A completed season's ratings must be stable regardless of the
confidence-blending settings (--confidence-games, --prior-regression).
This guards against a real risk we hit while building the early-season
fix: an intervention meant only to help sparse, in-progress data
accidentally changing historical, already-verified seasons.

The property being tested: once a team has played at least
--confidence-games games, its confidence is 1.0 and the prior term is
fully zeroed out (multiplied by 1 - confidence = 0) -- so varying
--confidence-games among any values at or below a normal season's game
count, or varying --prior-regression at all, should produce byte-for-byte
identical results for a season where every team has played enough games.
"""
import sys

from build_coefficients import per_season_iterative_ratings, ITERATIONS


def _make_games(pairs):
    class Row(dict):
        def __getitem__(self, k):
            return dict.get(self, k)

    return [
        Row(home_team=h, away_team=a, home_score=hs, away_score=as_, game_phase="regular")
        for h, a, hs, as_ in pairs
    ]


def test_full_season_unaffected_by_confidence_games_value():
    """
    A small but fully round-robin-ish season (every team plays several
    games) should give the identical result whether confidence_games is
    4, 6, 8, or 10, as long as every team has played at least that many
    games -- because confidence saturates to 1.0 in every case.
    """
    # Each of 4 teams plays every other team twice (6 games total, so with
    # confidence_games<=6 everyone is at full confidence)
    pairs = [
        ("A", "B", 30, 10), ("B", "A", 20, 17),
        ("A", "C", 24, 21), ("C", "A", 28, 14),
        ("A", "D", 35, 7),  ("D", "A", 10, 9),
        ("B", "C", 17, 16), ("C", "B", 20, 13),
        ("B", "D", 24, 20), ("D", "B", 21, 21 - 1),
        ("C", "D", 27, 24), ("D", "C", 14, 13),
    ]
    games = _make_games(pairs)
    games_played = {"A": 6, "B": 6, "C": 6, "D": 6}
    prior = {"A": 1.5, "B": 0.8, "C": 1.1, "D": 0.6}  # deliberately non-trivial priors

    results = []
    for confidence_games in (4, 5, 6):
        confidence = {t: min(1.0, n / confidence_games) for t, n in games_played.items()}
        results.append(
            per_season_iterative_ratings(games, iterations=ITERATIONS, prior=prior, confidence=confidence)
        )

    baseline = results[0]
    for other in results[1:]:
        for team in baseline:
            assert abs(baseline[team] - other[team]) < 1e-9, (
                f"{team}: {baseline[team]} != {other[team]} across different confidence_games "
                "values, despite every team having enough games for full confidence"
            )


def test_full_confidence_ignores_prior_entirely():
    """confidence=1.0 for every team must give the exact same result no matter what the prior is."""
    pairs = [("A", "B", 30, 10), ("B", "C", 20, 17), ("C", "A", 24, 21), ("A", "C", 28, 14)]
    games = _make_games(pairs)
    teams = {"A", "B", "C"}
    confidence = {t: 1.0 for t in teams}

    result_no_prior = per_season_iterative_ratings(games, iterations=ITERATIONS, prior={}, confidence=confidence)
    result_wild_prior = per_season_iterative_ratings(
        games, iterations=ITERATIONS, prior={"A": 99.0, "B": -5.0, "C": 42.0}, confidence=confidence
    )

    for team in teams:
        assert abs(result_no_prior[team] - result_wild_prior[team]) < 1e-9
