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

from export_static_data import (BOX_SCORE_SHARDS, box_score_shard,
                                write_box_score_shards)


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
        "401838053": ["five"],      # 401838053 % 32 == 5
        "401838085": ["five too"],  # also 5
        "401838054": ["six"],
    })
    assert entry["shards"] == BOX_SCORE_SHARDS
    assert set(entry["files"]) == {"5", "6"}
    assert entry["files"]["5"].startswith("data/players/2025/5.js?v=")
    body = (tmp_path / "2025" / "5.js").read_text(encoding="utf-8")
    assert "401838053" in body and "401838085" in body
    assert "401838054" not in body


def test_a_shard_merges_into_its_season_rather_than_assigning_it(tmp_path):
    """Two shards of one season must both survive being loaded."""
    write_box_score_shards(tmp_path, 2025, {"401838053": ["a"], "401838054": ["b"]})
    for shard in ("5", "6"):
        body = (tmp_path / "2025" / f"{shard}.js").read_text(encoding="utf-8")
        assert "Object.assign" in body, body[:80]
        assert "window.__CFB_PLAYERS__[2025]=window.__CFB_PLAYERS__[2025]||{}" in body


def test_an_empty_shard_is_never_written_or_named(tmp_path):
    entry = write_box_score_shards(tmp_path, 2025, {"401838053": ["five"]})
    assert list(entry["files"]) == ["5"]
    assert [p.name for p in (tmp_path / "2025").glob("*.js")] == ["5.js"]


def test_a_negative_game_id_lands_where_the_page_will_look(tmp_path):
    """The exporter and the page must agree, and they agree on a FLOORED
    modulo: -7 belongs in 25, where a bare JavaScript % would say -7."""
    entry = write_box_score_shards(tmp_path, 1999, {"-7": ["odd"]})
    assert list(entry["files"]) == ["25"]
    assert (tmp_path / "1999" / "25.js").exists()


def test_the_version_changes_when_a_shard_changes(tmp_path):
    first = write_box_score_shards(tmp_path, 2025, {"401838053": ["a"]})
    again = write_box_score_shards(tmp_path, 2025, {"401838053": ["a"]})
    changed = write_box_score_shards(tmp_path, 2025, {"401838053": ["b"]})
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
            text = path.read_text(encoding="utf-8")
            # The file merges its games into the season: the payload is the
            # second argument of the Object.assign, after the `||{},` guard.
            games = json.loads(text[text.index("||{},") + len("||{},"):text.rindex(");")])
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
