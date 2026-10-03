#!/usr/bin/env python3
"""Where the player box-score archive lives, and how to read it.

Four places read `data/raw/player_boxscores/`: the exporter, the refresh check,
the coverage audit and the fetcher itself. They share this module so that the
file's encoding is decided in ONE place -- when the archive went from plain JSON
to gzip, a reader left behind would have silently seen an empty season and
reported the box scores as missing rather than failing.

Gzipped because the archive is the largest thing in the repository and every CI
run and container checks it out. A season is about 10 MB of JSON and roughly a
tenth of that packed. A plain `<year>.json` is still read, so a season archived
before the change needs no re-fetch to be usable.
"""
from __future__ import annotations

import gzip
import json
from pathlib import Path

DEFAULT_DIR = Path("data/raw/player_boxscores")


def season_path(directory: Path, year: int) -> Path | None:
    """The file holding this season, in whichever encoding is on disk."""
    packed = Path(directory) / f"{year}.json.gz"
    if packed.exists():
        return packed
    plain = Path(directory) / f"{year}.json"
    return plain if plain.exists() else None


def read_season(directory: Path, year: int) -> dict:
    """A season's archive, or {} when none is committed.

    An absent season is empty rather than an error: a missing box score has
    always meant the source has not published one, never that a player recorded
    zero, and every caller already treats it that way.
    """
    path = season_path(directory, year)
    if path is None:
        return {}
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as fh:
        return json.load(fh)


def write_season(directory: Path, year: int, archive: dict) -> Path:
    """Replace a season, and remove a plain copy left by an older fetch.

    Both would otherwise be on disk and `season_path` would prefer the packed
    one, leaving a stale `.json` to be committed and read by nothing.
    """
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{year}.json.gz"
    temp = path.with_suffix(".tmp")
    body = json.dumps(archive, ensure_ascii=False, separators=(",", ":")) + "\n"
    # mtime=0 so re-archiving an unchanged season writes an identical file: gzip
    # stamps the current time into its header by default, which would make every
    # run of the backfill look like a change and commit 23 seasons for nothing.
    with gzip.GzipFile(temp, "wb", compresslevel=9, mtime=0) as fh:
        fh.write(body.encode("utf-8"))
    temp.replace(path)
    legacy = directory / f"{year}.json"
    if legacy.exists():
        legacy.unlink()
    return path
