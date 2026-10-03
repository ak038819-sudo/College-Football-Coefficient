from datetime import datetime, timezone
import gzip
import json
import struct

import player_archive
from fetch_cfbd_players import merge_archive, fetch_season
from model_refresh_needed import needs_refresh


def test_archived_box_score_survives_empty_retry():
    saved = {'42': [{'name': 'BYU', 'categories': []}]}
    assert merge_archive(saved, {}) == saved
    assert merge_archive(saved, {'42': []}) == saved


def test_recent_final_retries_missing_efficiency_and_players():
    snapshot = {'games': [{'id': 42, 'status': 'completed', 'start_date': '2026-09-26T22:00:00Z',
                           'home': {'classification': 'fbs', 'points': 30},
                           'away': {'classification': 'fbs', 'points': 20}}]}
    rows = [{'game_id': '42', 'home_score': '30', 'away_score': '20'}]
    now = datetime(2026, 9, 27, tzinfo=timezone.utc)
    assert needs_refresh(snapshot, rows, 2026, set(), {'42'}, now)
    assert needs_refresh(snapshot, rows, 2026, {'42'}, set(), now)
    assert not needs_refresh(snapshot, rows, 2026, {'42'}, {'42'}, now)
    assert not needs_refresh(snapshot, rows, 2026, set(), set(),
                             datetime(2026, 10, 1, tzinfo=timezone.utc))


def test_january_final_belongs_to_previous_football_season():
    snapshot = {'games': [{'id': 77, 'status': 'completed', 'start_date': '2027-01-15T01:00:00Z',
                           'home': {'classification': 'fbs', 'points': 24},
                           'away': {'classification': 'fbs', 'points': 21}}]}
    assert needs_refresh(snapshot, [], 2026)


def test_player_backfill_queries_only_started_weeks(monkeypatch):
    calls = []
    def get(path, params, key):
        calls.append((path, params))
        if path == '/calendar':
            return [{'week': 1, 'seasonType': 'regular', 'startDate': '2026-08-27T00:00:00Z'},
                    {'week': 2, 'seasonType': 'regular', 'startDate': '2026-10-08T00:00:00Z'}]
        return []
    monkeypatch.setattr('fetch_cfbd_players.get_json', get)
    archive = fetch_season(2026, 'test', {'42': [{'name': 'BYU'}]},
                           datetime(2026, 10, 1, tzinfo=timezone.utc))
    assert archive['42']
    assert len([path for path, _ in calls if path == '/games/players']) == 1


# --- the archive's encoding, decided in one place ---------------------------
#
# Four modules read this archive. When it went from plain JSON to gzip, a reader
# left behind would not have failed -- it would have seen an empty season and
# reported the box scores as missing, which is why the encoding lives in one
# module and these tests drive that module rather than each caller's copy.

def test_a_plain_season_archived_before_the_change_still_reads(tmp_path):
    """Otherwise re-fetching 237 MB would be the only way to read a season
    already committed to the repository."""
    (tmp_path / "2015.json").write_text(json.dumps({"1": [{"name": "Oregon"}]}),
                                        encoding="utf-8")
    assert player_archive.read_season(tmp_path, 2015) == {"1": [{"name": "Oregon"}]}


def test_a_season_with_no_file_is_empty_rather_than_an_error(tmp_path):
    """A missing box score has always meant the source has not published one,
    never that a player recorded zero. Every caller already treats an absent
    season that way, and raising here would turn a normal gap into a failed
    deploy."""
    assert player_archive.read_season(tmp_path, 1999) == {}
    assert player_archive.season_path(tmp_path, 1999) is None


def test_writing_a_season_packs_it_and_it_reads_back_whole(tmp_path):
    archive = {"401760359": [{"name": "Air Force", "home_away": "home", "categories": [
        {"name": "passing", "type": "YDS",
         "lines": [{"name": "A Passer", "stat": "288", "id": "4361182"}]}]}]}
    path = player_archive.write_season(tmp_path, 2025, archive)

    assert path.name == "2025.json.gz"
    assert gzip.decompress(path.read_bytes()).decode("utf-8").startswith("{")
    assert player_archive.read_season(tmp_path, 2025) == archive


def test_re_archiving_an_unchanged_season_writes_an_identical_file(tmp_path):
    """The backfill commits per season, so a timestamp in the gzip header would
    make every run of it look like 23 changed seasons and commit them all."""
    archive = {"1": [{"name": "Oregon", "categories": []}]}
    first = player_archive.write_season(tmp_path, 2025, archive).read_bytes()
    assert player_archive.write_season(tmp_path, 2025, archive).read_bytes() == first
    # Asserted on the header rather than by writing twice and hoping: two writes
    # in the same second match whether or not the timestamp is zeroed. Bytes 4-8
    # of a gzip header are its mtime.
    assert struct.unpack("<I", first[4:8])[0] == 0


def test_packing_a_season_removes_the_plain_copy(tmp_path):
    """Both on disk and `season_path` prefers the packed one, which would leave
    a stale 10 MB file committed and read by nothing."""
    (tmp_path / "2025.json").write_text("{}", encoding="utf-8")
    player_archive.write_season(tmp_path, 2025, {"1": [{"name": "Oregon"}]})
    assert not (tmp_path / "2025.json").exists()
    assert player_archive.read_season(tmp_path, 2025) == {"1": [{"name": "Oregon"}]}


def test_the_packed_season_wins_when_both_are_on_disk(tmp_path):
    """A plain file a fetch could not delete must not shadow the current one."""
    (tmp_path / "2025.json").write_text(json.dumps({"old": []}), encoding="utf-8")
    with gzip.GzipFile(tmp_path / "2025.json.gz", "wb", mtime=0) as fh:
        fh.write(json.dumps({"new": []}).encode("utf-8"))
    assert player_archive.read_season(tmp_path, 2025) == {"new": []}


def test_a_partial_write_cannot_replace_a_good_season(tmp_path, monkeypatch):
    """The fetcher walks a season week by week over a few minutes. A crash
    mid-write must leave the committed archive readable, not truncated."""
    good = {"1": [{"name": "Oregon", "categories": []}]}
    player_archive.write_season(tmp_path, 2025, good)

    class Boom(Exception):
        pass

    real = gzip.GzipFile

    class Exploding(real):
        def write(self, data):  # noqa: D401
            raise Boom("disk full")

    monkeypatch.setattr(player_archive.gzip, "GzipFile", Exploding)
    try:
        player_archive.write_season(tmp_path, 2025, {"2": []})
    except Boom:
        pass
    assert player_archive.read_season(tmp_path, 2025) == good
