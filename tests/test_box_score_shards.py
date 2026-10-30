"""A game page should download one game's box scores, not a season's.

Exported whole, 2025 was 17 MB -- 1.6 MB gzipped -- fetched to show one game,
and it grew with every season played. Sharded on the game id, a game page takes
about 46 KB gzipped and the stored bytes are unchanged, because nothing is
duplicated: the Stats view, which really does aggregate a season, asks for every
shard.

The shard rule is the whole contract between the exporter and the page. If they
disagree about which file holds a game, the page asks for a file that does not
exist and the box score silently disappears -- so these tests check the rule
itself, and then check the shipped files against it.
"""
from __future__ import annotations

import json

import pytest

from export_static_data import (BOX_SCORE_FORMAT, BOX_SCORE_SHARDS,
                                box_score_shard, decode_box_score_shard,
                                drop_zero_lines, encode_box_score_shard,
                                is_zero_stat, write_box_score_shards)


def side(team, home_away, *lines):
    """One team's half of a box score, in the shape the archive really holds.

    The encoder reads these keys, so a fixture of bare strings would pass a
    test that the shipped exporter could not run.
    """
    return {"name": team, "home_away": home_away,
            "categories": [{"name": category, "type": stat_type,
                            "lines": [{"name": name, "stat": stat, "id": athlete}
                                      for name, stat, athlete in rows]}
                           for category, stat_type, rows in lines]}


def game(team="Georgia State", *, stat="23", athlete="5220652", name="Marlin Dean"):
    return [side(team, "away", ("passing", "YDS", [(name, stat, athlete)]))]


def payload_of(path):
    """The JSON a shard file carries, whichever shape it is written in.

    A v2 shard pushes its payload onto a queue; a v1 shard -- which is what the
    repository still holds between a source commit and the next deploy --
    merges an object into the season.
    """
    text = path.read_text(encoding="utf-8")
    if ".push(" in text:
        return json.loads(text[text.index(".push(") + len(".push("):text.rindex(");")])
    return json.loads(text[text.index("||{},") + len("||{},"):text.rindex(");")])


def games_in(path):
    """The games one shard file holds, decoded if it needs decoding."""
    payload = payload_of(path)
    return decode_box_score_shard(payload) if payload.get("v") else payload


def sharded_manifest(repo_root):
    """The shipped manifest's box-score entries, or a skip.

    CI checks out the exports main last committed and does not run the
    exporter, so until the next deploy they are in the pre-sharding layout. The
    unit tests below cover the rule itself; these check the shipped files
    whenever they are sharded.
    """
    manifest_path = repo_root / "ui" / "data" / "static_manifest.json"
    if not manifest_path.exists():
        pytest.skip("static exports have not been built")
    players = json.loads(manifest_path.read_text(encoding="utf-8")).get("players") or {}
    if not players:
        pytest.skip("no box-score exports in this build")
    if not all(isinstance(entry, dict) for entry in players.values()):
        pytest.skip("these exports predate the sharding; the next deploy rewrites them")
    return players


def test_a_game_lands_in_the_shard_the_rule_names(tmp_path):
    entry = write_box_score_shards(tmp_path, 2025, {
        "401838053": game(),            # 401838053 % 32 == 5
        "401838085": game("Troy"),      # also 5
        "401838054": game("Marshall"),
    })
    assert entry["shards"] == BOX_SCORE_SHARDS
    assert set(entry["files"]) == {"5", "6"}
    assert entry["files"]["5"].startswith("data/players/2025/5.js?v=")
    body = (tmp_path / "2025" / "5.js").read_text(encoding="utf-8")
    assert "401838053" in body and "401838085" in body
    assert "401838054" not in body


def test_a_shard_queues_itself_so_two_shards_both_survive(tmp_path):
    """Two shards of one season must both survive being loaded.

    A shard no longer merges into window.__CFB_PLAYERS__, because what it
    carries is encoded and what the page renders is not; it pushes onto a queue
    the page drains. Pushing is what keeps the second shard from replacing the
    first, so the test is that each file APPENDS and names its own season.
    """
    write_box_score_shards(tmp_path, 2025,
                           {"401838053": game(), "401838054": game("Marshall")})
    for shard in ("5", "6"):
        body = (tmp_path / "2025" / f"{shard}.js").read_text(encoding="utf-8")
        assert "(window.__CFB_BOX_QUEUE__=window.__CFB_BOX_QUEUE__||[]).push(" in body, body[:90]
        assert "Object.assign" not in body, "a shard that assigns loses the other shard"
        assert payload_of(tmp_path / "2025" / f"{shard}.js")["s"] == 2025


def test_an_empty_shard_is_never_written_or_named(tmp_path):
    entry = write_box_score_shards(tmp_path, 2025, {"401838053": game()})
    assert list(entry["files"]) == ["5"]
    assert [p.name for p in (tmp_path / "2025").glob("*.js")] == ["5.js"]


def test_a_negative_game_id_lands_where_the_page_will_look(tmp_path):
    """The exporter and the page must agree, and they agree on a FLOORED
    modulo: -7 belongs in 25, where a bare JavaScript % would say -7."""
    entry = write_box_score_shards(tmp_path, 1999, {"-7": game()})
    assert list(entry["files"]) == ["25"]
    assert (tmp_path / "1999" / "25.js").exists()


def test_the_version_changes_when_a_shard_changes(tmp_path):
    first = write_box_score_shards(tmp_path, 2025, {"401838053": game()})
    again = write_box_score_shards(tmp_path, 2025, {"401838053": game()})
    changed = write_box_score_shards(tmp_path, 2025, {"401838053": game(stat="24")})
    assert first == again, "the same data must produce the same file and version"
    assert changed["files"]["5"] != first["files"]["5"]


def test_the_shard_rule_is_a_floored_modulo():
    """JavaScript's % keeps the sign of the dividend and Python's does not.
    A negative id would send the page to a file the exporter never wrote -- the
    same trap the player pages hit on CFBD's negative athlete ids."""
    assert box_score_shard(401838053) == 401838053 % BOX_SCORE_SHARDS
    assert box_score_shard("401838053") == box_score_shard(401838053)
    for negative in (-1, -7, -32, -33, -1044360):
        shard = box_score_shard(negative)
        assert 0 <= shard < BOX_SCORE_SHARDS, negative
        # -7 belongs in 25 here, where JavaScript's bare % would say -7.
        assert shard == negative % BOX_SCORE_SHARDS


def test_every_shard_holds_only_the_games_the_rule_assigns_it(repo_root):
    """The shipped files, checked against the rule rather than against a fixture."""
    players = sharded_manifest(repo_root)
    checked = 0
    for season, entry in players.items():
        assert isinstance(entry, dict), f"{season} is not sharded"
        assert entry["shards"] == BOX_SCORE_SHARDS
        for shard, src in entry["files"].items():
            path = repo_root / "ui" / src.split("?")[0]
            assert path.exists(), src
            games = games_in(path)
            assert games, f"{src} is empty and should not have been written"
            for game_id in games:
                assert box_score_shard(game_id) == int(shard), (season, shard, game_id)
            checked += len(games)
    # 16,352 games carry a box score today: the archive starts in 2004 while
    # the games table reaches back to 1980. The floor is a tripwire for an
    # export that silently stopped writing, not a target.
    assert checked > 15000, f"only {checked} games checked"


def test_the_manifest_names_a_file_for_every_shard_that_has_games(repo_root):
    players = sharded_manifest(repo_root)
    for season, entry in players.items():
        on_disk = {p.stem for p in (repo_root / "ui" / "data" / "players" / season).glob("*.js")}
        assert on_disk == set(entry["files"]), season
        # A version query is what pairs a new page with new data; without it a
        # browser can serve a shard from before the last publish.
        for src in entry["files"].values():
            assert "?v=" in src, src


def test_the_shipped_exports_leave_no_unsharded_season_behind(repo_root):
    """An export rewrites the directory, so a leftover season file would be
    served to nobody and committed forever."""
    sharded_manifest(repo_root)        # skips while the exports predate this
    players_dir = repo_root / "ui" / "data" / "players"
    assert [d for d in players_dir.iterdir() if d.is_dir()], "no sharded seasons"
    assert not list(players_dir.glob("*.js")), "an unsharded season file survived"


# The encoding itself. A shard carries string tables and positional rows
# because the published site is capped at 1 GB and these box scores were 245 MB
# of it, nearly all of it the same names and the same key spellings over again.
# Nothing is dropped, so the test that matters is a round trip.
def test_the_encoding_round_trips_through_the_decoder():
    games = {"401838053": [
        side("Georgia State", "away",
             ("defensive", "TOT", [("Marlin Dean", "7", "5220652"),
                                   ("Brent Washington", "4", "5195994")]),
             ("defensive", "TD", [("Marlin Dean", "0", "5220652"),
                                  ("Brent Washington", "0", "5195994")])),
        side("Troy", "home",
             ("passing", "C/ATT", [("Goode Smith", "17/24", "4685794")]))]}
    payload = encode_box_score_shard(2025, games)
    assert payload["v"] == BOX_SCORE_FORMAT
    assert decode_box_score_shard(payload) == games


def test_a_name_is_stored_once_however_many_lines_it_appears_on():
    """The saving IS the deduplication, so it is worth asserting outright: a
    player recorded a line in every statistic his position has, and his name
    and athlete id used to be spelled out on each one."""
    rows = [("Marlin Dean", str(n), "5220652") for n in range(5)]
    payload = encode_box_score_shard(
        2025, {"1": [side("Georgia State", "away", ("defensive", "TOT", rows))]})
    assert payload["n"] == ["Marlin Dean"]
    assert payload["i"] == ["5220652"]
    assert [line[0] for line in payload["g"]["1"][0][2][0][2]] == [0, 0, 0, 0, 0]
    # and the stats, which are the only thing that differs, all survive
    assert [line[1] for line in payload["g"]["1"][0][2][0][2]] == ["0", "1", "2", "3", "4"]


def test_the_encoded_shard_is_much_smaller_than_the_objects():
    """A tripwire on the whole point of the change, on a fixture shaped like the
    real thing: eleven players, each with the statistics his position records."""
    games = {str(401838053 + g): [
        side(team, "home" if g % 2 else "away",
             ("defensive", "TOT", [(f"Player {n}", str(n % 9), str(5000000 + n))
                                   for n in range(11)]),
             ("defensive", "SOLO", [(f"Player {n}", str(n % 5), str(5000000 + n))
                                    for n in range(11)]),
             ("defensive", "TD", [(f"Player {n}", "0", str(5000000 + n))
                                  for n in range(11)]))
        for team in ("Georgia State", "Troy")] for g in range(4)}
    objects = len(json.dumps(games, separators=(",", ":")))
    encoded = len(json.dumps(encode_box_score_shard(2025, games), separators=(",", ":")))
    assert encoded < objects * 0.6, f"{encoded} vs {objects}"


def test_an_index_the_tables_do_not_reach_decodes_to_an_empty_string():
    """A shard written by a different exporter than the reader was built from
    should cost that one value, not the whole box score. The page's
    decodeBoxScores makes the same promise."""
    payload = encode_box_score_shard(
        2025, {"1": [side("Georgia State", "away",
                          ("defensive", "TOT", [("Marlin Dean", "7", "5220652")]))]})
    payload["n"] = []
    decoded = decode_box_score_shard(payload)
    assert decoded["1"][0]["categories"][0]["lines"][0]["name"] == ""
    assert decoded["1"][0]["categories"][0]["lines"][0]["stat"] == "7"


def test_the_manifest_entry_states_the_format_it_wrote(tmp_path):
    """So a human reading the manifest can tell which shape the files are in.
    The page does not need it -- a v2 shard announces itself by queueing --
    but a stale pair of manifest and files is exactly what this records."""
    entry = write_box_score_shards(tmp_path, 2025, {"401838053": game()})
    assert entry["format"] == BOX_SCORE_FORMAT


# A line the source reports as zero is not exported. CFBD publishes a row per
# rostered player per statistic, so 38.9% of the archive's lines read 0 and the
# game page drew every one of them.
def test_a_zero_line_is_not_exported(tmp_path):
    games = {"401838053": [
        side("Georgia State", "away",
             ("defensive", "TOT", [("Marlin Dean", "7", "5220652"),
                                   ("Brent Washington", "0", "5195994")]),
             ("defensive", "TD", [("Marlin Dean", "0", "5220652"),
                                  ("Brent Washington", "0", "5195994")]))]}
    entry = write_box_score_shards(tmp_path, 2025, games)
    kept = games_in(tmp_path / "2025" / "5.js")["401838053"][0]["categories"]
    # The whole TD category went: every line in it was zero.
    assert [(c["name"], c["type"]) for c in kept] == [("defensive", "TOT")]
    assert [l["name"] for l in kept[0]["lines"]] == ["Marlin Dean"]
    assert entry["files"]["5"].startswith("data/players/2025/5.js?v=")


def test_a_team_with_nothing_but_zeros_is_left_out():
    """An empty container renders as a heading with nothing under it."""
    stripped = drop_zero_lines({"1": [
        side("Georgia State", "away", ("defensive", "TD", [("A", "0", "1")])),
        side("Troy", "home", ("passing", "YDS", [("B", "23", "2")]))]})
    assert [s["name"] for s in stripped["1"]] == ["Troy"]


def test_a_game_whose_every_line_is_zero_is_still_a_game():
    """The manifest naming a shard that does not hold a game is the one failure
    the page cannot recover from, so an emptied game stays, empty.

    BOTH sides have to be all zeros here: a fixture where one side survives
    cannot tell this rule from dropping emptied games, which is exactly how the
    first version of this test let that mutant through.
    """
    stripped = drop_zero_lines({"1": [
        side("Georgia State", "away", ("defensive", "TD", [("A", "0", "1")])),
        side("Troy", "home", ("defensive", "TD", [("B", "0", "2")]))]})
    assert list(stripped) == ["1"], "the game itself survives"
    assert stripped["1"] == [], "with no team left in it"


def test_what_counts_as_zero_is_every_digit_being_zero():
    """Zero is spelled several ways across 23 seasons, and the forms that are
    NOT zero matter more: a five-yard loss, a missed kick that was a real
    attempt, and half a sack."""
    for zero in ("0", "0.0", "0/0", "0-0", "0:00", "00", "--", "-", "", "  ", None):
        assert is_zero_stat(zero), repr(zero)
    for kept in ("-5", "0/1", "0-7", "10", "0.5", "1", "DNP", "1/0"):
        assert not is_zero_stat(kept), repr(kept)


def test_dropping_zeros_leaves_every_game_with_a_box_score(repo_root):
    """No game in the shipped archive is reduced to nothing by the rule.

    The rule is applied HERE rather than read off the files, because between a
    source commit and the next deploy the shipped shards still hold their
    zeros -- a test that only looked would pass on that archive while proving
    nothing. Applying it is what makes keeping an emptied game a guard rather
    than a live code path.
    """
    players = sharded_manifest(repo_root)
    checked = emptied = 0
    for season, entry in players.items():
        for src in entry["files"].values():
            games = games_in(repo_root / "ui" / src.split("?")[0])
            for game_id, sides in drop_zero_lines(games).items():
                checked += 1
                if not any(c["lines"] for s in sides for c in s["categories"]):
                    emptied += 1
    assert checked > 15000, f"only {checked} games checked"
    assert emptied == 0, f"{emptied} of {checked} games lose every line"
