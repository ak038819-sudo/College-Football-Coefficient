"""A player's career, game by game, turned out of the box-score archive.

The archive is stored by GAME: to show one player's career it would have to be
read whole, which is 83 MB. These exports turn it the other way up, by player
within a season. Both halves of that layout are load-bearing, and the tests
below are about the ways the turning can go wrong rather than about the shape
of the output: a line attached to the wrong team, an opponent taken from the
wrong side of the game, a season that quietly loses its players.
"""
from __future__ import annotations

import json

from export_static_data import (GAME_LOG_SHARDS, build_player_game_logs,
                                game_log_shard, write_game_log_shards)

FIELDS = ["game_id", "week", "date", "kickoff_utc", "completed"]
TEAMS = {"Clemson": 1, "Oregon": 2}


def payload(season, games):
    return {season: {"season": season, "games": games}}


def game(game_id, date, home=1, away=2, home_score=31, away_score=24, completed=1):
    """A season-payload row, with the fields this builder reads in place."""
    row = [game_id, 1, date, None, completed, 0, 0, 0, home, away, home_score, away_score]
    return row + [None] * 13


def side(name, home_away, category, type_, lines):
    return {"name": name, "home_away": home_away,
            "categories": [{"name": category, "type": type_, "lines": lines}]}


def line(player_id, stat, name="A Player"):
    return {"name": name, "stat": stat, "id": player_id}


# ---- the shard rule ----------------------------------------------------------

def test_the_shard_rule_is_a_floored_modulo():
    """29,162 athlete ids in this archive are negative. JavaScript's % keeps
    the sign of the dividend, so a truncating rule here would have the page
    asking for shard -5 and finding nothing."""
    assert game_log_shard(128) == 128 % GAME_LOG_SHARDS
    assert game_log_shard("4685740") == 4685740 % GAME_LOG_SHARDS
    assert 0 <= game_log_shard(-4685740) < GAME_LOG_SHARDS
    assert game_log_shard(-1) == GAME_LOG_SHARDS - 1


def test_a_player_lands_in_the_shard_the_rule_names(tmp_path):
    entry = write_game_log_shards(tmp_path, {2025: {"65": [[1, "2025-09-06", 2, 1, 31, 24, {}]]}})
    src = entry["seasons"]["2025"][str(65 % GAME_LOG_SHARDS)]
    assert "65" in (tmp_path / "game_logs" / "2025" /
                    src.split("/")[-1].split("?")[0]).read_text(encoding="utf-8")


def test_every_shard_of_a_season_is_written_even_when_empty(tmp_path):
    """The page computes its shard and asks for it before it can know whether
    that shard holds anyone. A 404 in the console is a bug report."""
    entry = write_game_log_shards(tmp_path, {2025: {"65": []}})
    assert len(entry["seasons"]["2025"]) == GAME_LOG_SHARDS
    for src in entry["seasons"]["2025"].values():
        assert (tmp_path / "game_logs" / "2025" / src.split("/")[-1].split("?")[0]).exists()


def test_a_season_with_no_archive_is_absent_rather_than_empty(tmp_path):
    """Absent is how the page knows not to ask: an empty season would have it
    fetching 64 files to find nothing."""
    entry = write_game_log_shards(tmp_path, {2025: {"65": []}})
    assert "2024" not in entry["seasons"]


def test_a_shard_merges_into_its_season(tmp_path):
    """A player page loads one shard per season. Two seasons of one player, or
    a later shard of the same season, must not replace the earlier."""
    entry = write_game_log_shards(tmp_path, {
        2025: {"1": [[1, "2025-09-06", 2, 1, 31, 24, {}]],
               "2": [[2, "2025-09-13", 2, 0, 10, 20, {}]]},
        2024: {"1": [[3, "2024-09-06", 2, 1, 7, 3, {}]]}})
    import subprocess
    one, two = game_log_shard(1), game_log_shard(2)
    files = [tmp_path / "game_logs" / season / f"{shard}.js"
             for season, shard in (("2024", one), ("2025", one), ("2025", two))]
    script = ("global.window={};" +
              "".join("eval(require('fs').readFileSync(%s,'utf8'));" % json.dumps(str(f))
                      for f in files) +
              "const g=window.__CFB_GAME_LOGS__;"
              "console.log(JSON.stringify([Object.keys(g).sort(),"
              f"g[2025][{one}]['1'].length,g[2025][{two}]['2'].length,"
              f"g[2024][{one}]['1'].length]));")
    out = subprocess.run(["node", "-e", script], capture_output=True, text=True, check=True)
    assert json.loads(out.stdout) == [["2024", "2025"], 1, 1, 1]


# ---- turning the archive the other way up ------------------------------------

def logs_for(archive, games=None, season=2025):
    return build_player_game_logs(
        payload(season, games if games is not None else [game(100, "2025-09-06")]),
        {season: archive}, TEAMS)


def test_a_line_carries_the_game_its_opponent_and_the_score():
    out = logs_for({"100": [side("Clemson", "home", "passing", "YDS", [line("7", "233")])]})
    assert out[2025]["7"] == [[100, "2025-09-06", 2, 1, 31, 24, {"passing": {"YDS": "233"}}]]


def test_the_opponent_is_the_other_side_of_the_game():
    """Taken from the player's own team, every line would name its own school."""
    out = logs_for({"100": [side("Oregon", "away", "rushing", "CAR", [line("9", "12")])]})
    assert out[2025]["9"][0][2] == 1               # Oregon's opponent is Clemson
    assert out[2025]["9"][0][3] == 0               # and Oregon was away


def test_the_score_is_read_from_the_players_own_side():
    """31-24 is a win for the home team and a loss for the away one, and the
    page prints W or L from these two numbers."""
    archive = {"100": [side("Clemson", "home", "passing", "YDS", [line("7", "1")]),
                       side("Oregon", "away", "passing", "YDS", [line("8", "1")])]}
    out = logs_for(archive)
    assert out[2025]["7"][0][4:6] == [31, 24]
    assert out[2025]["8"][0][4:6] == [24, 31]


def test_every_category_of_one_game_is_one_row():
    """A quarterback who also ran is one game, not two."""
    archive = {"100": [{"name": "Clemson", "home_away": "home", "categories": [
        {"name": "passing", "type": "YDS", "lines": [line("7", "233")]},
        {"name": "passing", "type": "TD", "lines": [line("7", "3")]},
        {"name": "rushing", "type": "CAR", "lines": [line("7", "2")]}]}]}
    rows = logs_for(archive)[2025]["7"]
    assert len(rows) == 1
    assert rows[0][6] == {"passing": {"YDS": "233", "TD": "3"}, "rushing": {"CAR": "2"}}


def test_the_newest_game_comes_first():
    games = [game(100, "2025-09-06"), game(101, "2025-11-29"), game(102, "2025-10-04")]
    archive = {str(g[0]): [side("Clemson", "home", "passing", "YDS", [line("7", "1")])]
               for g in games}
    assert [r[1] for r in logs_for(archive, games)[2025]["7"]] == \
        ["2025-11-29", "2025-10-04", "2025-09-06"]


def test_a_game_the_database_does_not_hold_is_skipped():
    """The archive covers games this project never rated. Exported anyway, the
    row would carry no opponent and no score."""
    archive = {"999": [side("Clemson", "home", "passing", "YDS", [line("7", "1")])]}
    assert logs_for(archive) == {}


def test_a_game_that_was_not_played_is_skipped():
    archive = {"100": [side("Clemson", "home", "passing", "YDS", [line("7", "1")])]}
    assert logs_for(archive, [game(100, "2025-09-06", completed=0)]) == {}


def test_a_team_name_this_database_cannot_resolve_is_skipped():
    """Rather than attached to the wrong side: the opponent and the score both
    come from knowing which side the player was on."""
    archive = {"100": [side("Clemsonn", "home", "passing", "YDS", [line("7", "1")])]}
    assert logs_for(archive) == {}


def test_a_team_that_did_not_play_in_this_game_is_skipped():
    """A resolvable name is not the same as a name belonging to this game."""
    archive = {"100": [side("Oregon", "home", "passing", "YDS", [line("7", "1")])]}
    out = build_player_game_logs(payload(2025, [game(100, "2025-09-06", home=1, away=3)]),
                                 {2025: archive}, TEAMS)
    assert out == {}


def test_a_line_with_no_athlete_id_is_skipped():
    """Box scores before 2005 carry names only. A log keyed on nothing would
    put every unidentified line under one player."""
    archive = {"100": [side("Clemson", "home", "passing", "YDS",
                            [{"name": "A Player", "stat": "1"}])]}
    assert logs_for(archive) == {}


def test_a_malformed_side_does_not_take_the_season_down():
    archive = {"100": ["Clemson",
                       side("Clemson", "home", "passing", "YDS", [line("7", "1")])]}
    assert "7" in logs_for(archive)[2025]


def test_an_archive_entry_that_is_not_a_list_is_skipped():
    assert logs_for({"100": {"name": "Clemson"}}) == {}


def test_seasons_are_kept_apart():
    """The export is per season so that only the season being played is
    rewritten on a deploy; a builder that merged them would defeat that."""
    payloads = {2025: {"games": [game(100, "2025-09-06")]},
                2024: {"games": [game(200, "2024-09-07")]}}
    archives = {2025: {"100": [side("Clemson", "home", "passing", "YDS", [line("7", "1")])]},
                2024: {"200": [side("Clemson", "home", "passing", "YDS", [line("7", "2")])]}}
    out = build_player_game_logs(payloads, archives, TEAMS)
    assert sorted(out) == [2024, 2025]
    assert out[2024]["7"][0][0] == 200 and out[2025]["7"][0][0] == 100
