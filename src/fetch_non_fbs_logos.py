#!/usr/bin/env python3
"""
Marks for the opponents this project rates nothing: FCS schools and below.

An FBS team's own logo comes from the dated set (src/build_dated_logo_assets.py):
one image per era, so a 1985 page shows the 1985 mark. The schools in
`non_fbs_games` have never had one. They are not teams this project holds --
they get no page, no rating and no row in the registry -- so the schedule named
them in plain text and the live scoreboard drew a monogram from their initials.
3,813 completed games across 207 schools, with nothing to look at.

This fills that in from the sportsdataverse logo archive
(github.com/sportsdataverse/sdv-assets), which captures what the official
providers serve and keeps every version it has seen. Two things it is NOT:

  * It is not dated. Every college-football row in its manifest has an empty
    valid_from/valid_to, because ESPN's college CDN serves only today's mark.
    So each school here gets ONE undated entry, good for every season, and the
    dated set stays the only source of era-accurate logos. An FBS team's eras
    always win over anything written here (see export_logo_files.py).
  * It is not a licence. The archive grants nothing and claims nothing: the
    marks belong to their owners and are used here nominatively, to identify
    the school that played the game, exactly as the dated set already is. See
    docs/non-fbs-logos.md.

The join is by ESPN team id, not by name. data/reference/espn_team_directory.json
is already in this repo (it is the provider evidence behind the team identity
audit) and gives each school's ESPN id under `location`, which is the school
name without its mascot -- so our opponent name matches it exactly for 185 of
the 207 schools, with no fuzzy matching anywhere. RENAMES below covers a school
the two sources call by different names; the rest are programs ESPN does not
list at all, almost all of them defunct or long out of football, and they keep
the monogram.

Usage:
    python src/fetch_non_fbs_logos.py                 # resolve, fetch, write ui/non_fbs_logo_assets.json
    python src/fetch_non_fbs_logos.py --dry-run       # resolve and report only, no network
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import sys
import tempfile
import urllib.request
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_dated_logo_assets import TARGET_SIZE, encode_resized  # noqa: E402
from cfbd_http import with_retries  # noqa: E402
from team_identity import canonical_name, normalize  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
RAW = REPO / "data/raw"
DIRECTORY = REPO / "data/reference/espn_team_directory.json"
DATED_TEAMS = REPO / "ui/dated_logo_assets.json"
OUT = REPO / "ui/non_fbs_logo_assets.json"
SOURCES = REPO / "data/reference/non_fbs_logo_sources.csv"

MANIFEST_URL = "https://raw.githubusercontent.com/sportsdataverse/sdv-assets/main/manifest/marks.csv"
TIMEOUT = 60.0
# What tells a stale manifest row (one school, skipped) from the archive being
# down (nothing written): more than a tenth of the attempts failing, and more
# than a handful of them, or nothing downloading at all.
FETCH_FAILURE_LIMIT = 0.1
FETCH_FAILURE_TOLERATED = 5

# A mark that is not dated covers every season. pickDatedLogoEntry() in the
# dashboard takes an entry whose start is at or below the season being viewed,
# so year 0 is "always", and nothing else in the pipeline reads it as a date.
UNDATED_START = 0

# Our opponent name -> the name ESPN's directory files the same school under.
# One entry, and it is a rename rather than a spelling: Mankato State became
# Minnesota State University, Mankato in 1998, and the games we hold under the
# old name were played before that. Everything else either matches exactly or
# is a program ESPN does not carry (see the report this prints).
RENAMES = {
    "Mankato State": "Minnesota State",
}


def opponent_names(raw_dir: Path = RAW) -> list[str]:
    """Every non-FBS opponent name in the committed CFBD game and schedule files.

    Read from the raw CSVs rather than the database so this runs in a workflow
    that has not built one, and by the same rule load_fcs_games.py loads them
    by: the row hangs off the side this project holds, and the opponent is the
    other side, spelled the way the source spells it. The files carry all-FCS
    games too (the schedule file is the whole division), so a row with neither
    side held is not ours. When BOTH sides resolve -- a school we hold for its
    FBS seasons playing from the FCS, or one of our teams under the name it had
    then -- the source's own per-game classification says which side was FBS,
    and a row where it does not say is left alone.
    """
    names: set[str] = set()
    for path in sorted(raw_dir.glob("fcs_games_*.csv")) + sorted(raw_dir.glob("fcs_schedule_*.csv")):
        for row in csv.DictReader(path.open(encoding="utf-8")):
            held = {side: bool(canonical_name((row.get(f"{side}_team") or "").strip()))
                    for side in ("home", "away")}
            if not any(held.values()):
                continue
            if all(held.values()):
                labels = {side: (row.get(f"{side}_classification") or "").strip().lower()
                          for side in ("home", "away")}
                fbs = [side for side, label in labels.items() if label == "fbs"]
                if len(fbs) != 1 or not labels["away" if fbs[0] == "home" else "home"]:
                    continue
                other = "away" if fbs[0] == "home" else "home"
            else:
                other = "home" if not held["home"] else "away"
            names.add((row[f"{other}_team"] or "").strip())
    return sorted(n for n in names if n)


def espn_directory(path: Path = DIRECTORY) -> dict[str, list[dict]]:
    """ESPN's school list grouped by its `location` -- the school without the mascot.

    A location is not quite unique: ESPN lists two Charlottes, two Troys and two
    Roosevelts, each pair a different school that happens to share a town name.
    So a location maps to a list, and a location with more than one school in it
    identifies nobody (see resolve()).
    """
    teams = json.loads(path.read_text(encoding="utf-8"))["teams"]
    by_location: dict[str, list[dict]] = {}
    for team in teams:
        by_location.setdefault(normalize(team["location"]), []).append(team)
    return by_location


def resolve(names: list[str], directory: dict[str, list[dict]]) -> tuple[dict[str, dict], list[str], list[str]]:
    """Opponent name -> its ESPN directory row.

    Also the names ESPN does not list, and the names that hit a location ESPN
    files more than one school under. An ambiguous name is reported and left
    without a mark rather than resolved to whichever row came first: the whole
    point of joining on an id is that nothing is guessed.
    """
    matched, unmatched, ambiguous = {}, [], []
    for name in names:
        rows = directory.get(normalize(RENAMES.get(name, name)), [])
        if len(rows) == 1:
            matched[name] = rows[0]
        elif rows:
            ambiguous.append(name)
        else:
            unmatched.append(name)
    return matched, unmatched, ambiguous


def sdv_default_marks(manifest_csv: str) -> dict[str, dict]:
    """ESPN team id -> the archive's current default college-football logo for it.

    One row per mark and image, so a mark whose bytes have changed since the
    archive started looking appears more than once. The newest capture
    (`last_seen`) is the one the provider is serving now.
    """
    marks: dict[str, dict] = {}
    for row in csv.DictReader(io.StringIO(manifest_csv)):
        if (row["league"], row["level"], row["mark_type"], row["variant"]) != \
                ("cfb", "team", "logo", "default"):
            continue
        if row["ext"] != "png":
            continue  # the pipeline here writes PNG; the handful of SVGs are not usable as-is
        current = marks.get(row["entity_id"])
        if current is None or row["last_seen"] > current["last_seen"]:
            marks[row["entity_id"]] = row
    return marks


def fetch(url: str, *, timeout: float = TIMEOUT, opener=None) -> bytes:
    """A retried GET of raw bytes. urllib, not requests: see cfbd_http.get_json."""
    get = opener or (lambda: urllib.request.urlopen(url, timeout=timeout).read())
    return with_retries(get, describe=f"GET {url}")


def dated_entries(name: str, dated: dict) -> list[dict] | None:
    """The dated eras of an FBS team this opponent name is an older name for.

    Four of these names are FBS programs as this project holds them, spelled the
    way the source spelled them in the seasons they were still FCS ('App State',
    'Florida Atlantic', 'Florida International', 'UL Monroe'). They have a dated
    set already, so they take it -- era-accurate, and nothing is fetched for them.
    """
    canonical = canonical_name(name)
    entries = dated.get(canonical) if canonical else None
    return [dict(e) for e in entries] if entries else None


def build(*, dry_run: bool = False, size: int = TARGET_SIZE, manifest_url: str = MANIFEST_URL,
          out: Path = OUT, sources_out: Path = SOURCES, raw_dir: Path = RAW,
          directory_path: Path = DIRECTORY, dated_path: Path = DATED_TEAMS,
          fetcher=fetch) -> dict:
    dated = json.loads(dated_path.read_text(encoding="utf-8")) if dated_path.exists() else {}
    # A name the dated set already holds under that exact spelling is an FBS
    # team of ours met in a season it was not: it has its own eras in the teams
    # section of the manifest, which win over anything written here anyway.
    names = [name for name in opponent_names(raw_dir) if name not in dated]
    matched, unmatched, ambiguous = resolve(names, espn_directory(directory_path))
    aliases = {name: entries for name in names if (entries := dated_entries(name, dated))}

    report = {"opponents": len(names), "matched": len(matched), "unmatched": unmatched,
              "ambiguous": ambiguous, "aliased_to_dated_set": sorted(aliases),
              "fetched": 0, "missing_in_archive": [], "fetch_failed": []}
    if dry_run:
        return report

    marks = sdv_default_marks(fetcher(manifest_url).decode("utf-8"))
    result: dict[str, list[dict]] = {}
    rows = []
    for name in names:
        if name in aliases:
            # Its eras, under this spelling, carrying the id for the same reason.
            espn = matched.get(name, {}).get("id")
            result[name] = [dict(entry, **({"espn": espn} if espn else {}))
                            for entry in aliases[name]]
            rows.append({"opponent_name": name, "espn_id": "", "espn_display_name": "",
                         "source": "dated set", "sha256": "", "archive_url": ""})
            continue
        team = matched.get(name)
        mark = marks.get(team["id"]) if team else None
        if mark is None:
            if team is not None:
                report["missing_in_archive"].append(name)
            rows.append({"opponent_name": name, "espn_id": team["id"] if team else "",
                         "espn_display_name": team["displayName"] if team else "",
                         "source": "", "sha256": "", "archive_url": ""})
            continue
        # One image the archive no longer serves is one school's mark, not a
        # reason to abandon the other 180: it is recorded, reported and left
        # without a mark, exactly as a school ESPN does not list is. A failure
        # rate above FETCH_FAILURE_LIMIT is an outage rather than a stale row,
        # and that does stop the run -- publishing most of the set as though it
        # were the set is how a gap gets committed and looks complete.
        try:
            raw = fetcher(mark["archive_url"])
        except Exception as error:  # noqa: BLE001 -- reported, and counted below
            print(f"{name}: {mark['archive_url']} failed ({type(error).__name__}: {error})",
                  flush=True)
            report["fetch_failed"].append(name)
            rows.append({"opponent_name": name, "espn_id": team["id"],
                         "espn_display_name": team["displayName"], "source": "",
                         "sha256": "", "archive_url": mark["archive_url"]})
            continue
        # The archive addresses an image by the sha256 of its bytes, so a
        # mismatch means the bytes are not the ones the manifest describes.
        digest = hashlib.sha256(raw).hexdigest()
        if digest != mark["sha256"]:
            raise ValueError(f"{name}: archive served {digest} where the manifest says {mark['sha256']}")
        with tempfile.NamedTemporaryFile(suffix=".png") as handle:
            handle.write(raw)
            handle.flush()
            data, bg = encode_resized(Path(handle.name), size)
        # The id travels with the mark. The live scoreboard names a school the
        # way ESPN does, mascot and all, and a school this project does not hold
        # has no canonical name to fall back on -- so the page finds its mark by
        # the feed's team id, which is this id (CFBD's scoreboard ids are ESPN's;
        # all 124 teams in the feed on 2026-10-18 matched the directory by id).
        result[name] = [{"start": UNDATED_START, "end": None, "data": data, "bg": bg,
                         "espn": team["id"]}]
        report["fetched"] += 1
        rows.append({"opponent_name": name, "espn_id": team["id"],
                     "espn_display_name": team["displayName"], "source": mark["source"],
                     "sha256": mark["sha256"], "archive_url": mark["archive_url"]})

    failed = len(report["fetch_failed"])
    attempted = report["fetched"] + failed
    outage = failed and (not report["fetched"] or
                         (failed > FETCH_FAILURE_TOLERATED and failed > attempted * FETCH_FAILURE_LIMIT))
    if outage:
        raise RuntimeError(
            f"{failed} of {attempted} images failed to download, which reads as an outage rather "
            f"than stale manifest rows. Nothing was written; try again later. "
            f"Failed: {', '.join(report['fetch_failed'])}")

    Path(out).write_text(json.dumps(result), encoding="utf-8")
    with Path(sources_out).open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["opponent_name", "espn_id", "espn_display_name",
                                                    "source", "sha256", "archive_url"])
        writer.writeheader()
        writer.writerows(rows)
    report["written"] = len(result)
    report["fetched_at"] = date.today().isoformat()
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="resolve the names and report, without touching the network")
    parser.add_argument("--size", type=int, default=TARGET_SIZE)
    parser.add_argument("--manifest-url", default=MANIFEST_URL)
    parser.add_argument("--out", default=str(OUT))
    args = parser.parse_args()

    report = build(dry_run=args.dry_run, size=args.size, manifest_url=args.manifest_url,
                   out=Path(args.out))
    print(f"{report['opponents']} non-FBS opponents: {report['matched']} resolved to an ESPN id, "
          f"{len(report['unmatched'])} not listed by ESPN")
    if report["aliased_to_dated_set"]:
        print("  taken from the dated set (FBS programs under an older name): "
              + ", ".join(report["aliased_to_dated_set"]))
    if report["unmatched"]:
        print("  no mark (these keep the monogram): " + ", ".join(report["unmatched"]))
    if report["ambiguous"]:
        print("  more than one ESPN school under that name, so no mark: "
              + ", ".join(report["ambiguous"]))
    if report.get("missing_in_archive"):
        print("  resolved but absent from the archive: " + ", ".join(report["missing_in_archive"]))
    if report.get("fetch_failed"):
        print("  in the archive but would not download: " + ", ".join(report["fetch_failed"]))
    if not args.dry_run:
        print(f"Wrote {report['written']} sets ({report['fetched']} images fetched) to {args.out}, "
              f"provenance in {SOURCES.relative_to(REPO)}")


if __name__ == "__main__":
    main()
