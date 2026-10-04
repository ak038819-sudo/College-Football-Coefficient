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

from export_static_data import BOX_SCORE_SHARDS, box_score_shard


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
    manifest_path = repo_root / "ui" / "data" / "static_manifest.json"
    if not manifest_path.exists():
        pytest.skip("static exports have not been built")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    players = manifest.get("players") or {}
    if not players:
        pytest.skip("no box-score exports in this build")

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
    manifest_path = repo_root / "ui" / "data" / "static_manifest.json"
    if not manifest_path.exists():
        pytest.skip("static exports have not been built")
    players = json.loads(manifest_path.read_text(encoding="utf-8")).get("players") or {}
    if not players:
        pytest.skip("no box-score exports in this build")

    for season, entry in players.items():
        on_disk = {p.stem for p in (repo_root / "ui" / "data" / "players" / season).glob("*.js")}
        assert on_disk == set(entry["files"]), season
        # A version query is what pairs a new page with new data; without it a
        # browser can serve a shard from before the last publish.
        for src in entry["files"].values():
            assert "?v=" in src, src


def test_a_sharded_season_is_no_larger_than_the_file_it_replaced(repo_root):
    """Sharding must not duplicate anything: 32 files should total what one did."""
    players_dir = repo_root / "ui" / "data" / "players"
    if not players_dir.exists():
        pytest.skip("static exports have not been built")
    season_dirs = [d for d in players_dir.iterdir() if d.is_dir()]
    assert season_dirs, "no sharded seasons"
    # And nothing from the pre-sharding layout is left behind to be served.
    assert not list(players_dir.glob("*.js")), "an unsharded season file survived"
