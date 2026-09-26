"""
Milestone E (conference pages): export_conference_pages.build_conference_pages()
computes every number a conference page displays. These tests pin the rules the
page's claims rest on -- the conference/external split, the contribution
breakdown adding up, postseason eras, and standings/titles never being guessed.
"""
import csv
import sqlite3

import pytest

from export_conference_pages import INDEPENDENTS, build_conference_pages

ALPHA, BETA = "Alpha", "Beta"


def _write_csv(path, fieldnames, rows):
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def _db(tmp_path, games, membership, teams, standings=(), coe2=(), elo=()):
    conn = sqlite3.connect(str(tmp_path / "cp.db"))
    conn.execute("""CREATE TABLE games (game_id INTEGER PRIMARY KEY, season_year INTEGER, game_date TEXT,
        home_team_id INTEGER, away_team_id INTEGER, home_score INTEGER, away_score INTEGER, game_phase TEXT)""")
    conn.execute("""CREATE TABLE team_membership_by_season (team_id INTEGER, season_year INTEGER,
        conference_real TEXT, is_fbs INTEGER)""")
    conn.execute("CREATE TABLE teams (team_id INTEGER PRIMARY KEY, team_name TEXT)")
    conn.execute("""CREATE TABLE conference_standings_by_year (season_year INTEGER, conference TEXT,
        team_id INTEGER, conf_rank INTEGER)""")
    conn.execute("""CREATE TABLE conference_coe2_by_season (conference TEXT, season_year INTEGER,
        conference_coe2 REAL, external_games_counted INTEGER)""")
    conn.execute("""CREATE TABLE elo_game_history (game_id INTEGER, team_id INTEGER, postgame_elo REAL)""")
    conn.executemany("INSERT INTO games VALUES (?,?,?,?,?,?,?,?)", games)
    conn.executemany("INSERT INTO team_membership_by_season VALUES (?,?,?,1)", membership)
    conn.executemany("INSERT INTO teams VALUES (?,?)", teams)
    conn.executemany("INSERT INTO conference_standings_by_year VALUES (?,?,?,?)", standings)
    conn.executemany("INSERT INTO conference_coe2_by_season VALUES (?,?,?,?)", coe2)
    conn.executemany("INSERT INTO elo_game_history VALUES (?,?,?)", elo)
    conn.commit()
    return conn


@pytest.fixture
def synthetic(tmp_path):
    """
    Two conferences, four teams, three seasons. 2013 has a pre-CFP-era title bowl
    and 2015 a real CFP game, so the era split is exercised; 2015 also has an
    Alpha-vs-Alpha bowl, the one case where "postseason" and "external" disagree.
    """
    teams = [(1, "A One"), (2, "A Two"), (3, "B One"), (4, "B Two")]
    membership = [(t, y, c) for y in (2013, 2014, 2015)
                  for t, c in ((1, ALPHA), (2, ALPHA), (3, BETA), (4, BETA))]
    games = [
        # id, season, date, home, away, hs, as, phase
        (1, 2013, "2013-09-01", 1, 2, 30, 20, "regular"),   # Alpha internal
        (2, 2013, "2013-09-08", 1, 3, 10, 24, "regular"),   # Alpha vs Beta: external for both
        (3, 2013, "2013-09-15", 4, 2, 14, 14, "regular"),   # external tie
        (4, 2013, "2014-01-06", 1, 3, 31, 28, "cfp"),       # BCS-era title bowl -> bowl, not CFP
        (5, 2015, "2015-09-05", 3, 1, 17, 7, "regular"),    # external
        (6, 2015, "2016-01-01", 1, 2, 21, 20, "bowl"),      # SAME-conference bowl
        (7, 2015, "2016-01-11", 1, 3, 35, 14, "cfp"),       # real CFP
        (8, 2015, "2015-10-01", 2, 4, None, None, "regular"),   # not played: must be ignored
    ]
    standings = [(2015, ALPHA, 1, 1), (2015, ALPHA, 2, 2), (2015, BETA, 3, 1), (2015, BETA, 4, 2)]
    coe2 = [(ALPHA, 2015, 12.5, 4), (BETA, 2015, 9.0, 3)]
    elo = [(7, 1, 1650.0), (7, 3, 1500.0)]
    conn = _db(tmp_path, games, membership, teams, standings, coe2, elo)

    processed = tmp_path / "processed"
    processed.mkdir()
    ratings = {(1, 2013): 3.0, (2, 2013): 1.0, (3, 2013): 2.0, (4, 2013): 2.0,
               (1, 2015): 6.0, (2, 2015): 2.0, (3, 2015): 4.0, (4, 2015): 0.0}
    _write_csv(processed / "team_ratings_by_season.csv", ["season_year", "team_name", "rating"],
               [{"season_year": y, "team_name": dict(teams)[t], "rating": v} for (t, y), v in ratings.items()])
    _write_csv(processed / "conference_ratings_by_season.csv", ["season_year", "conference_name", "rating"],
               [{"season_year": 2013, "conference_name": ALPHA, "rating": 4.0},
                {"season_year": 2013, "conference_name": BETA, "rating": 4.0},
                {"season_year": 2015, "conference_name": ALPHA, "rating": 8.0},
                {"season_year": 2015, "conference_name": BETA, "rating": 4.0}])
    _write_csv(processed / "conference_coeff_5yr.csv",
               ["end_year", "window_start", "window_end", "conference_name", "coeff_5yr"],
               [{"end_year": 2015, "window_start": 2011, "window_end": 2015, "conference_name": ALPHA, "coeff_5yr": 20.0},
                {"end_year": 2015, "window_start": 2011, "window_end": 2015, "conference_name": BETA, "coeff_5yr": 30.0}])
    # load_national_champions returns one row per SYSTEM, so 2013 here is a unanimous
    # title (two awarded rows, one championship) and 2014 a split one (two teams in the
    # same conference). Also a vacated title and one in a season with no membership row.
    champions = [{"season": 2013, "team_id": 1, "system": "ap", "status": "awarded", "notes": ""},
                 {"season": 2013, "team_id": 1, "system": "coaches", "status": "awarded", "notes": ""},
                 {"season": 2014, "team_id": 1, "system": "ap", "status": "awarded", "notes": ""},
                 {"season": 2014, "team_id": 2, "system": "coaches", "status": "awarded", "notes": ""},
                 {"season": 2015, "team_id": 3, "system": "cfp", "status": "awarded", "notes": ""},
                 {"season": 2015, "team_id": 1, "system": "ap", "status": "vacated", "notes": ""},
                 {"season": 1900, "team_id": 1, "system": "ap", "status": "awarded", "notes": ""}]
    yield build_conference_pages(conn, champions, processed)
    conn.close()


def _fields(data):
    return ({f: i for i, f in enumerate(data["season_fields"])},
            {f: i for i, f in enumerate(data["member_fields"])})


def _row(data, slug, season):
    F, _ = _fields(data)
    return next(r for r in data["seasons"][slug] if r[F["season"]] == season)


def test_conference_and_external_games_are_split_by_that_seasons_membership(synthetic):
    F, _ = _fields(synthetic)
    a13 = _row(synthetic, "alpha", 2013)
    # Alpha 2013: internal 1-vs-2; external = loss to Beta, tie with Beta, title-bowl win over Beta.
    assert (a13[F["ext_w"]], a13[F["ext_l"]], a13[F["ext_t"]]) == (1, 1, 1)
    assert a13[F["conf_games"]] == 1              # de-duplicated, not once per member
    b13 = _row(synthetic, "beta", 2013)
    assert (b13[F["ext_w"]], b13[F["ext_l"]], b13[F["ext_t"]]) == (1, 1, 1)   # the mirror image
    assert b13[F["conf_games"]] == 0


def test_a_same_conference_bowl_is_postseason_but_not_an_external_game(synthetic):
    F, _ = _fields(synthetic)
    a15 = _row(synthetic, "alpha", 2015)
    # 2015 Alpha: loss to Beta (regular), win over Beta (CFP) -> 1-1 external. The
    # Alpha-vs-Alpha bowl counts as a bowl appearance for two teams and as neither
    # a win nor a loss for the conference, because a conference has no record vs itself.
    assert (a15[F["ext_w"]], a15[F["ext_l"]], a15[F["ext_t"]]) == (1, 1, 0)
    assert a15[F["bowl_teams"]] == 2 and a15[F["bowl_w"]] == 1 and a15[F["bowl_l"]] == 1
    assert synthetic["vs"]["alpha"]["2015"] == {"beta": [1, 1, 0]}


def test_pre_2014_title_bowls_count_as_bowls_and_never_as_cfp(synthetic):
    F, _ = _fields(synthetic)
    a13, a15 = _row(synthetic, "alpha", 2013), _row(synthetic, "alpha", 2015)
    assert (a13[F["bowl_teams"]], a13[F["bowl_w"]], a13[F["bowl_l"]]) == (1, 1, 0)
    assert a13[F["cfp_teams"]] == 0
    assert (a15[F["cfp_teams"]], a15[F["cfp_w"]], a15[F["cfp_l"]]) == (1, 1, 0)


def test_scheduled_games_are_ignored_everywhere(synthetic):
    F, M = _fields(synthetic)
    # Game 8 is Alpha team 2 vs Beta team 4 with no score; neither side may record it.
    a15 = _row(synthetic, "alpha", 2015)
    assert a15[F["ext_w"]] + a15[F["ext_l"]] + a15[F["ext_t"]] == 2
    team2 = next(m for m in synthetic["members"]["alpha"]["2015"] if m[M["team_id"]] == 2)
    assert team2[M["games"]] == 1                 # only the same-conference bowl


def test_contribution_shares_are_exhaustive_and_ordered(synthetic):
    _, M = _fields(synthetic)
    members = synthetic["members"]["alpha"]["2015"]
    assert [m[M["team_id"]] for m in members] == [1, 2]                 # strongest CoE first
    assert sum(m[M["share"]] for m in members) == pytest.approx(100.0, abs=0.01)
    assert members[0][M["share"]] == pytest.approx(75.0)               # 6.0 of 8.0
    # A conference whose members all rate 0.0 has no total to divide by, so no share.
    beta = {m[M["team_id"]]: m for m in synthetic["members"]["beta"]["2015"]}
    assert beta[4][M["coe"]] == 0.0 and beta[4][M["share"]] == pytest.approx(0.0)


def test_ranks_are_taken_only_among_conferences_with_members_that_season(synthetic):
    F, _ = _fields(synthetic)
    a15, b15 = _row(synthetic, "alpha", 2015), _row(synthetic, "beta", 2015)
    assert (a15[F["coe"]], a15[F["coe_rank"]], a15[F["coe_of"]]) == (8.0, 1, 2)
    assert (b15[F["coe"]], b15[F["coe_rank"]], b15[F["coe_of"]]) == (4.0, 2, 2)
    # The 5-yr window ranks independently of the single season, and Beta leads it.
    assert (a15[F["coe5_rank"]], b15[F["coe5_rank"]]) == (2, 1)
    # 2014 has members but no ratings row, so there is a season row with no rating or rank.
    a14 = _row(synthetic, "alpha", 2014)
    assert a14[F["coe"]] is None and a14[F["coe_rank"]] is None and a14[F["members"]] == 2


def test_champion_and_titles_come_from_their_own_sources_never_from_results(synthetic):
    F, _ = _fields(synthetic)
    assert _row(synthetic, "alpha", 2015)[F["champion_id"]] == 1        # standings table, conf_rank 1
    assert _row(synthetic, "alpha", 2013)[F["champion_id"]] is None     # no standings data for 2013
    assert _row(synthetic, "alpha", 2013)[F["title_ids"]] == [1]        # awarded, credited to Alpha
    assert _row(synthetic, "beta", 2015)[F["title_ids"]] == [3]
    # A vacated title is never counted, and a season with no membership row credits nobody.
    assert _row(synthetic, "alpha", 2015)[F["title_ids"]] == []
    assert synthetic["totals"]["alpha"]["titles"] == 3                  # 2013 once + 2014's two teams


def test_one_title_is_counted_once_however_many_polls_awarded_it(synthetic):
    """A unanimous champion has an awarded row per system (AP and Coaches), which is
    two rows for one championship -- it must not be listed or counted twice."""
    F, _ = _fields(synthetic)
    assert _row(synthetic, "alpha", 2013)[F["title_ids"]] == [1]        # not [1, 1]
    # A genuinely split season still credits both members, once each.
    assert _row(synthetic, "alpha", 2014)[F["title_ids"]] == [1, 2]
    for slug, rows in synthetic["seasons"].items():
        for r in rows:
            ids = r[F["title_ids"]]
            assert len(ids) == len(set(ids)), f"{slug} {r[F['season']]}: {ids}"


def test_the_title_total_matches_the_seasons_it_sums(synthetic):
    F, _ = _fields(synthetic)
    for slug, rows in synthetic["seasons"].items():
        assert synthetic["totals"][slug]["titles"] == sum(len(r[F["title_ids"]]) for r in rows)


def test_totals_roll_up_every_season(synthetic):
    T = synthetic["totals"]
    assert T["alpha"]["ext"] == [2, 2, 1]              # 2013's 1-1-1 plus 2015's 1-1-0
    assert T["alpha"]["bowl"] == [3, 2, 1, 0]          # 3 bowl bids across the two seasons
    assert T["alpha"]["cfp"] == [1, 1, 0]
    assert T["alpha"]["members_ever"] == 2
    assert T["alpha"]["peak_coe"] == [2015, 8.0]
    assert T["alpha"]["best_rank"] == [2015, 1, 2]
    assert synthetic["vs_all"]["alpha"] == {"beta": [2, 2, 1]}


def test_every_conference_reports_only_its_own_seasons(synthetic):
    F, _ = _fields(synthetic)
    for c in synthetic["conferences"]:
        seasons = [r[F["season"]] for r in synthetic["seasons"][c["slug"]]]
        assert seasons == sorted(seasons)                        # chronological, oldest first
        assert (min(seasons), max(seasons)) == (c["first_season"], c["last_season"])
        assert set(synthetic["members"][c["slug"]]) == {str(s) for s in seasons}


def test_slug_collisions_stop_the_build(tmp_path):
    teams = [(1, "A One"), (2, "B One")]
    membership = [(1, 2015, "Big 12"), (2, 2015, "Big-12")]      # both slugify to big-12
    conn = _db(tmp_path, [], membership, teams)
    processed = tmp_path / "p"
    processed.mkdir()
    for name, fields in (("team_ratings_by_season.csv", ["season_year", "team_name", "rating"]),
                         ("conference_ratings_by_season.csv", ["season_year", "conference_name", "rating"]),
                         ("conference_coeff_5yr.csv", ["end_year", "conference_name", "coeff_5yr"])):
        _write_csv(processed / name, fields, [])
    with pytest.raises(ValueError, match="slug collision"):
        build_conference_pages(conn, [], processed)
    conn.close()


def test_a_team_rating_for_an_unknown_team_stops_the_build(tmp_path):
    conn = _db(tmp_path, [], [(1, 2015, "Alpha")], [(1, "A One")])
    processed = tmp_path / "p"
    processed.mkdir()
    _write_csv(processed / "team_ratings_by_season.csv", ["season_year", "team_name", "rating"],
               [{"season_year": 2015, "team_name": "Nobody", "rating": 1.0}])
    with pytest.raises(ValueError, match="unknown team"):
        build_conference_pages(conn, [], processed)
    conn.close()


# ---------------- invariants on the real database ----------------

@pytest.fixture(scope="module")
def real(db_path, repo_root):
    conn = sqlite3.connect(str(db_path))
    if not conn.execute("SELECT COUNT(*) FROM team_membership_by_season").fetchone()[0]:
        pytest.skip("team_membership_by_season is empty -- run run_pipeline.py first")
    from export_team_pages import load_national_champions
    data = build_conference_pages(conn, load_national_champions(conn))
    yield conn, data
    conn.close()


def test_members_are_exactly_the_membership_table(real):
    conn, data = real
    expected = {}
    for tid, season, conf in conn.execute(
            "SELECT team_id, season_year, conference_real FROM team_membership_by_season "
            "WHERE conference_real IS NOT NULL"):
        expected.setdefault((conf, season), set()).add(tid)
    _, M = _fields(data)
    by_name = {c["slug"]: c["name"] for c in data["conferences"]}
    seen = {}
    for slug, seasons in data["members"].items():
        for season, rows in seasons.items():
            seen[(by_name[slug], int(season))] = {r[M["team_id"]] for r in rows}
            assert len(rows) == len(seen[(by_name[slug], int(season))]), "a team listed twice"
    assert seen == expected


def test_a_conference_season_coe_is_exactly_its_members_ratings(real):
    """The contribution breakdown must add up to the rating it breaks down, or the
    page would be attributing a conference's CoE to the wrong teams."""
    _, data = real
    F, M = _fields(data)
    checked = 0
    for slug, rows in data["seasons"].items():
        for r in rows:
            if r[F["coe"]] is None:
                continue
            members = data["members"][slug][str(r[F["season"]])]
            rated = [m for m in members if m[M["coe"]] is not None]
            assert sum(m[M["coe"]] for m in rated) == pytest.approx(r[F["coe"]], abs=0.02), \
                f"{slug} {r[F['season']]}"
            if any(m[M["coe"]] for m in rated):
                assert sum(m[M["share"]] for m in rated) == pytest.approx(100.0, abs=0.05)
            checked += 1
    assert checked > 400


def test_conference_records_agree_with_the_authoritative_records_table(real):
    """conference_team_records_by_year uses the same definitions (any-phase
    intra-conference games, completed games only) for the seasons it covers."""
    conn, data = real
    _, M = _fields(data)
    stored = {(conf, season, tid): (cw, cl, ct, ow, ol, ot) for conf, season, tid, cw, cl, ct, ow, ol, ot
              in conn.execute("""SELECT conference, season_year, team_id, conf_wins, conf_losses, conf_ties,
                                        overall_wins, overall_losses, overall_ties
                                 FROM conference_team_records_by_year""")}
    if not stored:
        pytest.skip("conference_team_records_by_year is empty")
    by_name = {c["slug"]: c["name"] for c in data["conferences"]}
    checked = 0
    for slug, seasons in data["members"].items():
        for season, rows in seasons.items():
            for m in rows:
                key = (by_name[slug], int(season), m[M["team_id"]])
                if key not in stored:
                    continue
                cw, cl, ct, ow, ol, ot = stored[key]
                assert (m[M["conf_w"]], m[M["conf_l"]], m[M["conf_t"]]) == (cw, cl, ct), key
                assert (m[M["w"]], m[M["l"]], m[M["t"]]) == (ow, ol, ot), key
                checked += 1
    assert checked > 1000


def test_external_records_are_mirrored_between_conferences(real):
    """A win for one conference is a loss for the other, in the same season."""
    _, data = real
    for slug, seasons in data["vs"].items():
        for season, opponents in seasons.items():
            for opp, (w, l, t) in opponents.items():
                if not opp:
                    continue                                  # opponent had no conference that season
                back = data["vs"].get(opp, {}).get(season, {}).get(slug)
                assert back == [l, w, t], f"{slug} vs {opp} in {season}: {[w, l, t]} vs {back}"


def test_a_conferences_external_record_is_the_sum_of_its_members(real):
    _, data = real
    F, M = _fields(data)
    for slug, rows in data["seasons"].items():
        for r in rows:
            members = data["members"][slug][str(r[F["season"]])]
            for i, key in enumerate(("ext_w", "ext_l", "ext_t")):
                col = ("ext_w", "ext_l", "ext_t")[i]
                assert r[F[col]] == sum(m[M[col]] for m in members), f"{slug} {r[F['season']]} {col}"


def test_no_conference_reports_a_season_outside_the_dataset(real):
    conn, data = real
    years = {y for (y,) in conn.execute("SELECT DISTINCT season_year FROM team_membership_by_season")}
    F, _ = _fields(data)
    for slug, rows in data["seasons"].items():
        assert {r[F["season"]] for r in rows} <= years
        assert [r[F["season"]] for r in rows] == sorted(r[F["season"]] for r in rows)


def test_no_real_conference_season_lists_a_team_twice_for_one_title(real):
    """data/reference/national_champions.csv carries a row per system, so most
    pre-BCS champions appear twice; the conference payload must still list and
    count each championship once."""
    _, data = real
    F, _ = _fields(data)
    total_titles = 0
    for slug, rows in data["seasons"].items():
        for r in rows:
            ids = r[F["title_ids"]]
            assert len(ids) == len(set(ids)), f"{slug} {r[F['season']]}: {ids}"
            total_titles += len(ids)
        assert data["totals"][slug]["titles"] == sum(len(x[F["title_ids"]]) for x in rows)
    assert total_titles > 0, "no titles were credited to any conference at all"


def test_the_independents_bucket_is_present_and_has_no_champion(real):
    """It is a bucket of unaffiliated programs, so it can never have a standings winner."""
    _, data = real
    F, _ = _fields(data)
    slug = next(c["slug"] for c in data["conferences"] if c["name"] == INDEPENDENTS)
    rows = data["seasons"][slug]
    assert rows, "the independents bucket should still get a page"
    assert all(r[F["champion_id"]] is None for r in rows)
