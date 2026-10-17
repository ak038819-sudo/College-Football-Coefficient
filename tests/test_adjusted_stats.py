"""Opponent-adjusted team statistics.

The whole point of this module is that it DISAGREES with the raw table, so a
test that only checks it produces numbers would pass however wrong those
numbers were. The things worth pinning down:

  * the fit recovers an effect that was planted in the data -- a defence whose
    opponents were weak must come out worse than its raw average says, and by
    roughly the amount the fixture built in;
  * the two sides are never crossed, so a team is not credited on defence for
    what its offence did;
  * the home-field term is fitted, not assumed, and the league mean stays the
    league mean;
  * the ridge term shrinks a one-game team toward the mean, and more of it
    shrinks more;
  * a game against an opponent this project does not rate is excluded from the
    raw column too, so the two columns on a row cover the same games;
  * a season with too little of a metric has no fit at all rather than one
    built on scraps.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import build_adjusted_stats as adj  # noqa: E402

SCHEMA = """
    CREATE TABLE teams (team_id INTEGER PRIMARY KEY, team_name TEXT);
    CREATE TABLE games (game_id INTEGER PRIMARY KEY, season_year INTEGER,
                        home_team_id INTEGER, away_team_id INTEGER,
                        home_score INTEGER, away_score INTEGER,
                        neutral_site INTEGER DEFAULT 0);
    CREATE TABLE non_fbs_games (game_id INTEGER PRIMARY KEY, season_year INTEGER,
                                team_id INTEGER, team_score INTEGER, opponent_score INTEGER);
    CREATE TABLE team_membership_by_season (team_id INTEGER, season_year INTEGER,
                                            conference_real TEXT);
    CREATE TABLE game_team_advanced (game_id INTEGER, team_id INTEGER, season_year INTEGER,
                                     off_plays INTEGER, off_ppa REAL, off_success_rate REAL);
"""

SEASON = 2026
# Four strong teams against twelve weak ones. Lopsided on purpose: with an even
# split a strong team's raw defensive average is wrong by only a point, and a
# test could not tell a working adjustment from a broken one. Sixteen teams
# playing a double round robin is also 480 observations, which clears
# MIN_OBSERVATIONS, so build_season has something to publish.
STRONG, WEAK = [1, 2, 3, 4], list(range(5, 17))


def _db(tmp_path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(tmp_path / "league.db"))
    conn.executescript(SCHEMA)
    return conn


def _round_robin(conn, teams: list[int], scores: dict, neutral: bool = True) -> None:
    """Every team plays every other twice, once at each venue.

    `scores` maps (offense, defense) -> points. Double round robin so no team's
    record of venues biases the home term, and every pair appears on both sides.
    """
    conn.execute("DELETE FROM games")
    conn.execute("DELETE FROM teams")
    conn.executemany("INSERT INTO teams VALUES (?, ?)",
                     [(t, f"Team {t}") for t in teams])
    gid = 1
    for home in teams:
        for away in teams:
            if home == away:
                continue
            conn.execute(
                "INSERT INTO games (game_id, season_year, home_team_id, away_team_id,"
                " home_score, away_score, neutral_site) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (gid, SEASON, home, away, scores[(home, away)], scores[(away, home)],
                 1 if neutral else 0))
            gid += 1
    conn.commit()


def _planted(strong: list[int], weak: list[int], base: int = 24,
             offense_edge: int = 7, defense_edge: int = 7) -> dict:
    """Scores built from a known offence and defence value per team.

    points(i vs j) = base + offence(i) + defence(j), with no noise, so the fit
    has one exact answer and any crossing of the two sides shows up at once.
    """
    off = {t: offense_edge for t in strong} | {t: -offense_edge for t in weak}
    dfn = {t: -defense_edge for t in strong} | {t: defense_edge for t in weak}
    return {(i, j): base + off[i] + dfn[j]
            for i in strong + weak for j in strong + weak if i != j}


def _divided_league(tmp_path) -> sqlite3.Connection:
    """Two divisions of eight, unequal offences, IDENTICAL defences.

    Each team plays its own division twice and its one cross-division partner
    twice. The cross games are what connect the two halves: without them the
    fit would have two disconnected leagues and no way to compare them at all.
    """
    conn = _db(tmp_path)
    teams = list(range(1, 17))
    conn.executemany("INSERT INTO teams VALUES (?, ?)", [(t, f"Team {t}") for t in teams])
    off = {t: (7 if t <= 8 else -7) for t in teams}
    points = lambda i: 24 + off[i]                                  # noqa: E731
    gid = 1
    pairs = [(i, j) for i in teams for j in teams
             if i != j and ((i <= 8) == (j <= 8) or abs(i - j) == 8)]
    for home, away in pairs:
        conn.execute(
            "INSERT INTO games (game_id, season_year, home_team_id, away_team_id,"
            " home_score, away_score, neutral_site) VALUES (?, ?, ?, ?, ?, ?, 1)",
            (gid, SEASON, home, away, points(home), points(away)))
        gid += 1
    conn.commit()
    return conn


class TestTheFitRecoversWhatWasPlanted:
    def test_a_defence_that_faced_weak_offences_is_marked_down(self, tmp_path):
        # Eight teams: four good, four bad, no noise at all. Every good team's
        # raw defensive average is dragged down by the four bad offences it
        # faced, and the adjustment must put it back.
        conn = _db(tmp_path)
        _round_robin(conn, STRONG + WEAK, _planted(STRONG, WEAK))
        obs = adj.point_observations(conn, SEASON)
        raw_off, raw_def, _ = adj.raw_means(obs)
        fit = adj.solve(obs, 0.0)

        # Raw: a strong team's defence faced three strong and twelve weak
        # offences, so it looks four points better than it is.
        assert raw_def[1] == pytest.approx(24 + (3 * 7 + 12 * -7) / 15 - 7, abs=1e-9)
        assert raw_def[1] == pytest.approx(12.8, abs=1e-9)
        # Adjusted: what an AVERAGE opponent would do, and the average team in
        # this lopsided league has a below-average offence (-3.5) and an
        # above-average defence (+3.5). So the fit centres both sides on the
        # league and the published values are 24 + 7 + 3.5 on offence and
        # 24 - 7 - 3.5 on defence. The sum of the two sides is what the model
        # identifies; where their shared constant sits is set by the ridge term
        # centring each side on zero, which is what makes 'average' mean the
        # average of THIS season.
        mu = fit["mu"]
        assert mu == pytest.approx(24.0, abs=1e-9)
        assert mu + fit["off"][1] == pytest.approx(34.5, abs=1e-6)
        assert mu + fit["off"][5] == pytest.approx(20.5, abs=1e-6)
        assert mu + fit["def"][1] == pytest.approx(13.5, abs=1e-6)
        assert mu + fit["def"][5] == pytest.approx(27.5, abs=1e-6)
        # The planted gap between a strong and a weak team is 14 on each side,
        # and it survives the centring exactly.
        assert (mu + fit["off"][1]) - (mu + fit["off"][5]) == pytest.approx(14, abs=1e-6)
        assert (mu + fit["def"][5]) - (mu + fit["def"][1]) == pytest.approx(14, abs=1e-6)

    def test_identical_defences_on_different_schedules_come_out_level(self, tmp_path):
        # The sharpest form of the claim. Two divisions of eight play mostly
        # within themselves: one division's offences are all good, the other's
        # all bad. Every defence in the league is EXACTLY as good as every
        # other. So the raw defensive table is pure schedule -- all eight bad
        # divisions' defences on top -- and a working adjustment must flatten it.
        conn = _divided_league(tmp_path)
        obs = adj.point_observations(conn, SEASON)
        raw_off, raw_def, _ = adj.raw_means(obs)
        # Raw: the weak division's defences look eleven points better.
        assert raw_def[9] < raw_def[1] - 10
        assert min(raw_def[t] for t in range(9, 17)) > 0
        fit = adj.solve(obs, 0.0)
        adjusted = [fit["mu"] + fit["def"][t] for t in range(1, 17)]
        assert max(adjusted) - min(adjusted) < 1e-6
        # Offence, meanwhile, really does differ, and the fit still says so.
        assert (fit["off"][1] - fit["off"][9]) == pytest.approx(14, abs=1e-6)

    def test_the_two_sides_are_never_crossed(self, tmp_path):
        # A league where one team has a great offence and a terrible defence.
        # Crossing the sides anywhere would make it look average on both.
        conn = _db(tmp_path)
        teams = [1, 2, 3, 4, 5, 6]
        scores = {}
        off = {1: 14, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0}
        dfn = {1: 14, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0}
        for i in teams:
            for j in teams:
                if i != j:
                    scores[(i, j)] = 24 + off[i] + dfn[j]
        _round_robin(conn, teams, scores)
        fit = adj.solve(adj.point_observations(conn, SEASON), 0.0)
        assert fit["off"][1] - fit["off"][2] == pytest.approx(14, abs=1e-6)
        assert fit["def"][1] - fit["def"][2] == pytest.approx(14, abs=1e-6)

    def test_the_league_mean_is_the_weighted_mean_of_the_games_in_the_fit(self, tmp_path):
        conn = _db(tmp_path)
        _round_robin(conn, STRONG + WEAK, _planted(STRONG, WEAK))
        obs = adj.point_observations(conn, SEASON)
        fit = adj.solve(obs, 2.0)
        assert fit["mu"] == pytest.approx(sum(o[2] for o in obs) / len(obs), abs=1e-9)


class TestHomeField:
    def test_the_home_effect_is_fitted_from_the_data(self, tmp_path):
        # The same league twice over, with six points added to whoever is home.
        # The fit must find six, not the 3-ish the real league happens to show.
        conn = _db(tmp_path)
        planted = _planted(STRONG, WEAK)
        conn.executemany("INSERT INTO teams VALUES (?, ?)",
                         [(t, f"Team {t}") for t in STRONG + WEAK])
        gid = 1
        for home in STRONG + WEAK:
            for away in STRONG + WEAK:
                if home == away:
                    continue
                conn.execute(
                    "INSERT INTO games (game_id, season_year, home_team_id, away_team_id,"
                    " home_score, away_score, neutral_site) VALUES (?, ?, ?, ?, ?, ?, 0)",
                    (gid, SEASON, home, away, planted[(home, away)] + 3,
                     planted[(away, home)] - 3))
                gid += 1
        conn.commit()
        fit = adj.solve(adj.point_observations(conn, SEASON), 0.0)
        assert fit["home"] == pytest.approx(6.0, abs=1e-6)
        # And `mu` is still the league mean, so the published values are what a
        # team would do at a NEUTRAL site -- the same 34.5 the no-home-field
        # fixture gives. A 0/1 home flag in place of the split would fit the
        # same six-point gap and quietly make every published number a road
        # number, with mu three points low.
        assert fit["mu"] == pytest.approx(24.0, abs=1e-9)
        assert fit["mu"] + fit["off"][1] == pytest.approx(34.5, abs=1e-6)

    def test_a_neutral_site_game_pulls_the_home_term_neither_way(self, tmp_path):
        conn = _db(tmp_path)
        _round_robin(conn, STRONG + WEAK, _planted(STRONG, WEAK), neutral=True)
        fit = adj.solve(adj.point_observations(conn, SEASON), 0.0)
        assert fit["home"] == pytest.approx(0.0, abs=1e-6)

    def test_side_of_splits_the_gap_so_the_mean_stays_the_mean(self):
        assert adj.side_of(1, 0) == 0.5
        assert adj.side_of(0, 0) == -0.5
        assert adj.side_of(1, 1) == 0.0
        assert adj.side_of(0, 1) == 0.0


class TestTheRidgeTerm:
    def _one_game_league(self, tmp_path):
        # Six teams play a full round robin; a seventh plays one game and wins
        # it 50-0. An unpenalised fit would hand it the best offence in the
        # league on that one result.
        conn = _db(tmp_path)
        teams = [1, 2, 3, 4, 5, 6]
        scores = {(i, j): 24 for i in teams for j in teams if i != j}
        _round_robin(conn, teams, scores)
        conn.execute("INSERT INTO teams VALUES (7, 'Team 7')")
        conn.execute(
            "INSERT INTO games (game_id, season_year, home_team_id, away_team_id,"
            " home_score, away_score, neutral_site) VALUES (999, ?, 7, 1, 50, 0, 1)",
            (SEASON,))
        conn.commit()
        return conn

    def test_more_shrinkage_pulls_a_one_game_team_further_toward_the_mean(self, tmp_path):
        conn = self._one_game_league(tmp_path)
        obs = adj.point_observations(conn, SEASON)
        light = adj.solve(obs, 1.0)
        heavy = adj.solve(obs, 10.0)
        assert light["off"][7] > heavy["off"][7] > 0
        # And the team with a full season is barely touched by the same change.
        assert abs(light["off"][1] - heavy["off"][1]) < abs(light["off"][7] - heavy["off"][7])

    def test_shrinkage_is_what_keeps_one_game_off_the_top_of_the_table(self, tmp_path):
        conn = self._one_game_league(tmp_path)
        obs = adj.point_observations(conn, SEASON)
        unpenalised = adj.solve(obs, 0.0)
        shipped = adj.solve(obs, adj.LAMBDA["points"])
        assert unpenalised["off"][7] > shipped["off"][7]

    def test_a_team_with_no_games_on_one_side_is_not_a_division_by_zero(self, tmp_path):
        # Reached with no ridge at all, which the calibration harness does on
        # every fold: a team held out of one side has no weight there.
        obs = [(1, 2, 24.0, 1.0, 0.0), (1, 3, 28.0, 1.0, 0.0)]
        fit = adj.solve(obs, 0.0)
        assert fit["off"][2] == 0.0 and fit["off"][3] == 0.0
        assert fit["def"][1] == 0.0


class TestGamesAgainstOpponentsThisProjectDoesNotRate:
    def test_an_unrated_opponent_is_in_neither_column(self, tmp_path):
        # The raw column must cover exactly the games the fit used, or the two
        # numbers on a row would not be comparable at all.
        conn = _db(tmp_path)
        _round_robin(conn, STRONG + WEAK, _planted(STRONG, WEAK))
        conn.execute("INSERT INTO non_fbs_games (game_id, season_year, team_id,"
                     " team_score, opponent_score) VALUES (500, ?, 1, 70, 0)", (SEASON,))
        conn.commit()
        table = adj.build_season(conn, SEASON)
        row = next(r for r in table["teams"] if r["id"] == 1)
        # Thirty rated games, not thirty-one: the 70-point blowout is absent from
        # the average, and absent from the count the row publishes beside it.
        assert row["points"]["games"] == 30
        assert row["points"]["off_raw"] == pytest.approx(24 + 7 + (3 * -7 + 12 * 7) / 15,
                                                         abs=1e-9)
        assert row["unrated"] == 1

    def test_the_count_of_them_is_carried_on_the_row(self, tmp_path):
        conn = _db(tmp_path)
        _round_robin(conn, STRONG + WEAK, _planted(STRONG, WEAK))
        conn.executemany("INSERT INTO non_fbs_games (game_id, season_year, team_id,"
                         " team_score, opponent_score) VALUES (?, ?, ?, 50, 7)",
                         [(600, SEASON, 2), (601, SEASON, 2)])
        conn.commit()
        table = adj.build_season(conn, SEASON)
        by_id = {r["id"]: r for r in table["teams"]}
        assert by_id[2]["unrated"] == 2
        assert by_id[3]["unrated"] == 0


class TestWhenASeasonHasTooLittleToFit:
    def test_a_handful_of_games_produces_no_metric_at_all(self, tmp_path):
        conn = _db(tmp_path)
        teams = [1, 2, 3, 4]
        _round_robin(conn, teams, {(i, j): 24 for i in teams for j in teams if i != j})
        table = adj.build_season(conn, SEASON)
        assert table["metrics"] == []
        assert table["teams"] == []

    def test_a_metric_the_season_has_none_of_is_absent_not_zero(self, tmp_path):
        # Points only, as every season before 2001 is: the play-by-play table is
        # empty, so those two metrics must not appear.
        conn = _db(tmp_path)
        teams = list(range(1, 17))
        _round_robin(conn, teams, {(i, j): 24 for i in teams for j in teams if i != j})
        table = adj.build_season(conn, SEASON)
        assert [m["key"] for m in table["metrics"]] == ["points"]
        assert all("ppa" not in r for r in table["teams"])

    def test_a_team_with_one_game_is_left_out_of_the_table(self, tmp_path):
        conn = _db(tmp_path)
        teams = list(range(1, 17))
        _round_robin(conn, teams, {(i, j): 24 for i in teams for j in teams if i != j})
        conn.execute("INSERT INTO teams VALUES (99, 'One Gamer')")
        conn.execute(
            "INSERT INTO games (game_id, season_year, home_team_id, away_team_id,"
            " home_score, away_score, neutral_site) VALUES (900, ?, 99, 1, 3, 60, 1)",
            (SEASON,))
        conn.commit()
        table = adj.build_season(conn, SEASON)
        assert 99 not in {r["id"] for r in table["teams"]}
        assert 1 in {r["id"] for r in table["teams"]}


class TestRanks:
    def test_one_is_best_in_each_direction(self):
        # Offence: more is better. Defence: less is better.
        assert adj.ranked({1: 30.0, 2: 20.0, 3: 25.0}, True) == {1: 1, 3: 2, 2: 3}
        assert adj.ranked({1: 30.0, 2: 20.0, 3: 25.0}, False) == {2: 1, 3: 2, 1: 3}

    def test_ties_share_a_rank_and_skip_the_next_one(self):
        assert adj.ranked({1: 30.0, 2: 30.0, 3: 25.0}, True) == {1: 1, 2: 1, 3: 3}

    def test_the_published_ranks_order_the_published_values(self, tmp_path):
        conn = _db(tmp_path)
        # Sixteen teams so the season clears MIN_OBSERVATIONS.
        teams = list(range(1, 17))
        scores = {(i, j): 24 + (7 if i <= 8 else -7) + (7 if j > 8 else -7)
                  for i in teams for j in teams if i != j}
        _round_robin(conn, teams, scores)
        table = adj.build_season(conn, SEASON)
        rows = [r for r in table["teams"] if "points" in r]
        for side, best_high in (("off", True), ("def", False)):
            ordered = sorted(rows, key=lambda r: r["points"][f"{side}_adj_rank"])
            values = [r["points"][f"{side}_adj"] for r in ordered]
            assert values == sorted(values, reverse=best_high)


class TestTheAdvancedMetrics:
    def _advanced(self, conn, teams, per_play):
        """One advanced row per team per game, weighted by plays."""
        for (gid, home, away) in conn.execute(
                "SELECT game_id, home_team_id, away_team_id FROM games").fetchall():
            for team in (home, away):
                conn.execute(
                    "INSERT INTO game_team_advanced (game_id, team_id, season_year,"
                    " off_plays, off_ppa, off_success_rate) VALUES (?, ?, ?, 70, ?, ?)",
                    (gid, team, SEASON, per_play[team], 0.4))
        conn.commit()

    def test_a_row_whose_game_is_not_rated_is_excluded_by_the_join(self, tmp_path):
        conn = _db(tmp_path)
        teams = list(range(1, 17))
        _round_robin(conn, teams, {(i, j): 24 for i in teams for j in teams if i != j})
        self._advanced(conn, teams, {t: 0.1 for t in teams})
        before = len(adj.advanced_observations(conn, SEASON, "ppa"))
        # A row for a game that is not in `games` at all -- an FBS-vs-FCS game.
        conn.execute("INSERT INTO game_team_advanced (game_id, team_id, season_year,"
                     " off_plays, off_ppa, off_success_rate) VALUES (7777, 1, ?, 70, 0.9, 0.9)",
                     (SEASON,))
        conn.commit()
        assert len(adj.advanced_observations(conn, SEASON, "ppa")) == before

    def test_observations_are_weighted_by_plays(self, tmp_path):
        conn = _db(tmp_path)
        teams = list(range(1, 17))
        _round_robin(conn, teams, {(i, j): 24 for i in teams for j in teams if i != j})
        self._advanced(conn, teams, {t: 0.1 for t in teams})
        # One team's games are made twice as long and twice as productive. The
        # weighted mean must move more than an unweighted one would.
        conn.execute("UPDATE game_team_advanced SET off_plays = 140, off_ppa = 0.5"
                     " WHERE team_id = 1")
        conn.commit()
        obs = adj.advanced_observations(conn, SEASON, "ppa")
        assert {w for _, _, _, w, _ in obs} == {70.0, 140.0}
        fit = adj.solve(obs, 0.0)
        plain = sum(v for _, _, v, _, _ in obs) / len(obs)
        assert fit["mu"] > plain

    def test_a_null_metric_is_skipped_rather_than_read_as_zero(self, tmp_path):
        conn = _db(tmp_path)
        teams = list(range(1, 17))
        _round_robin(conn, teams, {(i, j): 24 for i in teams for j in teams if i != j})
        self._advanced(conn, teams, {t: 0.1 for t in teams})
        full = len(adj.advanced_observations(conn, SEASON, "ppa"))
        conn.execute("UPDATE game_team_advanced SET off_ppa = NULL WHERE team_id = 1")
        conn.commit()
        thinned = adj.advanced_observations(conn, SEASON, "ppa")
        assert len(thinned) < full
        assert all(v != 0.0 for _, _, v, _, _ in thinned)

    def test_a_team_with_no_rows_of_its_own_still_has_a_defence(self, tmp_path):
        # The per-game metrics are nullable by design, and a team whose own rows
        # are all null for one metric still appears in every opponent's row as
        # the defence. It must be published with the side it HAS and no number
        # on the side it has not -- and it must not abort the season's export by
        # being asked for one.
        conn = _db(tmp_path)
        teams = list(range(1, 17))
        _round_robin(conn, teams, {(i, j): 24 for i in teams for j in teams if i != j})
        self._advanced(conn, teams, {t: 0.1 for t in teams})
        conn.execute("UPDATE game_team_advanced SET off_ppa = NULL WHERE team_id = 1")
        conn.commit()
        table = adj.build_season(conn, SEASON)
        assert "ppa" in {m["key"] for m in table["metrics"]}
        row = next(r for r in table["teams"] if r["id"] == 1)
        cells = row["ppa"]
        assert cells["def_raw"] is not None and cells["def_adj"] is not None
        assert cells["def_raw_rank"] is not None
        assert cells["off_raw"] is None and cells["off_adj"] is None
        assert cells["off_raw_rank"] is None and cells["off_adj_rank"] is None
        # Its season is its real one, not halved because only one side counted.
        assert cells["games"] == 30
        assert row["points"]["off_raw"] is not None   # points are unaffected
        # And nobody else lost their offence.
        others = [r for r in table["teams"] if r["id"] != 1 and "ppa" in r]
        assert others and all(r["ppa"]["off_raw"] is not None for r in others)

    def test_a_rank_is_only_given_to_a_team_that_has_the_number(self, tmp_path):
        conn = _db(tmp_path)
        teams = list(range(1, 17))
        _round_robin(conn, teams, {(i, j): 24 for i in teams for j in teams if i != j})
        self._advanced(conn, teams, {t: 0.1 + t / 100 for t in teams})
        conn.execute("UPDATE game_team_advanced SET off_ppa = NULL WHERE team_id IN (1, 2)")
        conn.commit()
        table = adj.build_season(conn, SEASON)
        rows = [r for r in table["teams"] if "ppa" in r]
        ranked_off = [r for r in rows if r["ppa"]["off_adj_rank"] is not None]
        # Fourteen teams have an offence, so the ranks run 1..14 with no gap.
        assert sorted(r["ppa"]["off_adj_rank"] for r in ranked_off) == list(range(1, 15))
        # Every team still has a defence rank.
        assert all(r["ppa"]["def_adj_rank"] is not None for r in rows)

    def test_a_metric_table_the_database_has_never_had_is_not_an_error(self, tmp_path):
        conn = sqlite3.connect(str(tmp_path / "bare.db"))
        conn.executescript("""
            CREATE TABLE teams (team_id INTEGER PRIMARY KEY, team_name TEXT);
            CREATE TABLE games (game_id INTEGER PRIMARY KEY, season_year INTEGER,
                                home_team_id INTEGER, away_team_id INTEGER,
                                home_score INTEGER, away_score INTEGER,
                                neutral_site INTEGER DEFAULT 0);
        """)
        assert adj.advanced_observations(conn, SEASON, "ppa") == []
        assert adj.unrated_games(conn, SEASON) == {}
        assert adj.season_conferences(conn, SEASON) == {}


class TestConvergence:
    def test_the_solver_stops_well_short_of_its_iteration_cap(self, tmp_path):
        conn = _db(tmp_path)
        _round_robin(conn, STRONG + WEAK, _planted(STRONG, WEAK))
        fit = adj.solve(adj.point_observations(conn, SEASON), adj.LAMBDA["points"])
        assert 1 < fit["iterations"] < adj.MAX_ITERATIONS

    def test_an_empty_season_is_an_empty_fit_not_a_crash(self):
        fit = adj.solve([], 2.0)
        assert fit["mu"] is None and fit["off"] == {} and fit["observations"] == 0
