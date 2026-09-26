"""
Data foundation step: logos as static files, per-season game files, search index.
Pins that nothing is lost or invented on the way from the database/committed
assets to the browser-facing files.
"""
import base64
import json
import sqlite3
from collections import Counter

import pytest

import export_logo_files as lf
from export_static_data import (CONFERENCE_ALIASES, FIELDS, build_conference_search_rows,
                                build_search_index, build_season_payloads)
from predict_upcoming import build_upcoming, elo_config

PNG = b"\x89PNG\r\n\x1a\n" + b"fake-image-bytes"
URI = "data:image/png;base64," + base64.b64encode(PNG).decode()


def _sources(tmp_path, teams):
    src = tmp_path / "teams.json"
    src.write_text(json.dumps(teams), encoding="utf-8")
    return {"teams": src}


def test_logo_files_are_the_exact_embedded_bytes(tmp_path):
    out, man = tmp_path / "logos", tmp_path / "manifest.json"
    lf.export(_sources(tmp_path, {"Miami (FL)": [{"start": 1990, "end": None, "data": URI, "bg": "#123456"}]}), out, man)
    entry = json.loads(man.read_text())["teams"]["Miami (FL)"][0]
    assert (entry["start"], entry["end"], entry["bg"]) == (1990, None, "#123456")
    assert entry["src"].startswith("assets/logos/teams/miami-fl/1990-now.png?v=")
    assert (out / "teams" / "miami-fl" / "1990-now.png").read_bytes() == PNG


def test_exact_duplicate_eras_are_dropped_but_conflicts_stop_the_build(tmp_path):
    e = {"start": 2026, "end": None, "data": URI, "bg": "#6e6e6e"}
    stats = lf.export(_sources(tmp_path, {"Pac-12": [e, dict(e)]}), tmp_path / "o", tmp_path / "m.json")
    assert stats["teams"]["files"] == 1 and stats["duplicates_dropped"] == ["teams: Pac-12 2026-now"]
    with pytest.raises(ValueError):
        lf.export(_sources(tmp_path, {"Pac-12": [e, dict(e, bg="#000000")]}), tmp_path / "o2", tmp_path / "m2.json")


def test_non_png_logo_is_rejected(tmp_path):
    bad = "data:image/png;base64," + base64.b64encode(b"GIF89a...").decode()
    with pytest.raises(ValueError):
        lf.export(_sources(tmp_path, {"X": [{"start": 1, "end": None, "data": bad, "bg": "#000"}]}),
                  tmp_path / "o", tmp_path / "m.json")


@pytest.fixture
def payloads(db_path):
    conn = sqlite3.connect(str(db_path))
    p = build_season_payloads(conn, build_upcoming(conn, elo_config()))
    yield conn, p
    conn.close()


def test_every_game_appears_once_in_its_own_season(payloads):
    conn, p = payloads
    ids = Counter(r[0] for s in p.values() for r in s["games"])
    assert all(n == 1 for n in ids.values())
    completed = conn.execute("SELECT COUNT(*) FROM games WHERE home_score IS NOT NULL").fetchone()[0]
    assert sum(r[4] for s in p.values() for r in s["games"]) == completed
    season_of = dict(conn.execute("SELECT game_id, season_year FROM games"))
    assert all(season_of.get(r[0], season) == season for season, s in p.items() for r in s["games"])


def test_completed_games_carry_the_stored_pregame_values(payloads):
    conn, p = payloads
    stored = {(g, t): (pre, exp) for g, t, pre, exp in conn.execute(
        "SELECT game_id, team_id, pregame_elo, elo_expectation FROM elo_game_history")}
    i = {f: n for n, f in enumerate(FIELDS)}
    checked = 0
    for s in p.values():
        for r in s["games"]:
            if r[i["completed"]] and (r[0], r[i["home_id"]]) in stored:
                pre, exp = stored[(r[0], r[i["home_id"]])]
                assert r[i["home_pre_elo"]] == round(pre, 1) and r[i["p_home"]] == round(exp, 4)
                checked += 1
    assert checked > 0


def test_scheduled_games_have_no_results_and_lists_are_chronological(payloads):
    _, p = payloads
    i = {f: n for n, f in enumerate(FIELDS)}
    for s in p.values():
        rows = s["games"]
        assert all((r[i["date"]], r[i["kickoff_utc"]] or "") <= (q[i["date"]], q[i["kickoff_utc"]] or "")
                   for r, q in zip(rows, rows[1:]))
        for r in rows:
            if not r[i["completed"]]:
                assert all(r[i[f]] is None for f in ("home_score", "away_score", "home_post_elo",
                                                     "home_elo_change", "home_game_coe", "away_game_coe"))


def test_search_index_covers_teams_aliases_and_games(payloads):
    conn, p = payloads
    idx = build_search_index(conn, p)
    assert len(idx["teams"]) == conn.execute("SELECT COUNT(*) FROM teams").fetchone()[0]
    names = {t[1]: t for t in idx["teams"]}
    for alias, canonical in conn.execute("SELECT alias, team_name FROM team_aliases"):
        assert alias in names[canonical][3]
    assert len(idx["games"]) == sum(len(s["games"]) for s in p.values())


# ---------------- conference search rows (Milestone E pages) ----------------


def test_search_index_covers_every_conference_that_ever_existed(payloads):
    conn, p = payloads
    rows = build_search_index(conn, p)["conferences"]
    expected = {r[0] for r in conn.execute(
        "SELECT DISTINCT conference_real FROM team_membership_by_season WHERE conference_real IS NOT NULL")}
    assert {r[1] for r in rows} == expected
    assert [r[1] for r in rows] == sorted(r[1] for r in rows)
    assert len({r[0] for r in rows}) == len(rows), "two conferences share a slug"


def test_conference_slugs_match_the_conference_page_route(db_conn):
    """The header search links to #conference=<slug>, so these slugs must be the ones
    src/export_conference_pages.py generates -- otherwise a search hit 404s."""
    from export_conference_pages import build_conference_pages
    from export_team_pages import load_national_champions
    pages = build_conference_pages(db_conn, load_national_champions(db_conn))
    assert {c["slug"] for c in pages["conferences"]} == {r[0] for r in build_conference_search_rows(db_conn)}


def test_every_conference_alias_names_a_real_conference(db_conn):
    """A renamed or dropped conference must not leave a shorthand pointing at nothing."""
    real = {r[0] for r in db_conn.execute(
        "SELECT DISTINCT conference_real FROM team_membership_by_season WHERE conference_real IS NOT NULL")}
    unknown = sorted(set(CONFERENCE_ALIASES) - real)
    assert unknown == [], f"aliases for conferences that do not exist: {unknown}"
    for name, aliases in CONFERENCE_ALIASES.items():
        assert len(set(aliases)) == len(aliases), f"{name}: duplicate alias"
        assert name not in aliases, f"{name}: lists its own name as an alias"
