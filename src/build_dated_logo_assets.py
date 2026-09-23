#!/usr/bin/env python3
"""
Builds date-accurate team logo assets: for each team, a list of
(start_year, end_year_or_None, base64_png) entries, so the dashboard can
show the logo a team ACTUALLY used in a given historical season instead
of always showing their current logo (increasingly important now that
game history goes back to 1980 -- a team's 1985 logo can look nothing
like their 2026 one).

Source: a flat folder of "TeamName(startYear-endYear).png" files (an
open-ended range like "TeamName(2020-).png" means that logo is still
current). Filename team names don't always match our canonical
team_name exactly (short names like "Cal" for California, or full
formal names like "Mississippi" for our "Ole Miss") -- NAME_MAP below
covers every mismatch found by checking all 137 teams in the roster
against the actual file set; everything else matches via a direct
transform (spaces -> underscores, parens stripped, apostrophes
stripped). UCF has no entry under any name variant in the source set at
all (a genuine gap, not a naming mismatch) -- falls back to the
existing static ui/logo_assets.json entry for that one team.

Usage:
    python src/build_dated_logo_assets.py --source "path/to/CFB Logos API" [--db db/league.db] [--out ui/dated_logo_assets.json]
"""
from __future__ import annotations

import argparse
import base64
import io
import json
import re
import sqlite3
from pathlib import Path

from PIL import Image, ImageDraw

TARGET_SIZE = 96  # px, per user decision -- these display as small inline icons, not full artwork


def is_whiteish(px, thresh=235) -> bool:
    return px[0] >= thresh and px[1] >= thresh and px[2] >= thresh


def remove_white_background(img: Image.Image, thresh: int = 30, step: int = 2) -> Image.Image:
    """
    Flood-fills any white-ish background to transparent. Seeds from a
    DENSE set of points along the entire image border (every `step`
    pixels), not just the 4 corners -- a naive corner-only version was
    tried and found to fail on irregular/spiky logo shapes (e.g.
    Clemson's paw print splits its white background into several
    disconnected pockets, most of which don't touch any of the 4
    corners at all) -- confirmed via a direct alpha-channel pixel check
    after the fix, not just a visual preview (transparency renders as
    plain white in some simple image viewers, which can look like
    nothing changed even when the fix worked correctly).

    Only affects background CONNECTED to the border -- an enclosed
    white area fully inside the logo (unreachable from any border
    point) is left alone, though in practice an enclosed white "hole"
    (e.g. the loop of a letter B) is usually intended to read as
    background too and does get caught if it connects to the outside
    via even a single thin gap, which is typically the case.

    Found 49 of 532 source files (about 9%) with a fully opaque white
    background and zero real transparency -- this fixes those, and is
    a safe no-op for files that already have a transparent background
    (flood-filling from an already-transparent border pixel does
    nothing further).
    """
    img = img.convert("RGBA")
    w, h = img.size
    seeds = set()
    for x in range(0, w, step):
        seeds.add((x, 0))
        seeds.add((x, h - 1))
    for y in range(0, h, step):
        seeds.add((0, y))
        seeds.add((w - 1, y))

    for x, y in seeds:
        px = img.getpixel((x, y))
        if is_whiteish(px):
            ImageDraw.floodfill(img, (x, y), (255, 255, 255, 0), thresh=thresh)

    return img


def encode_resized(path: Path, target_size: int) -> str:
    img = Image.open(path)
    img = remove_white_background(img)
    img.thumbnail((target_size, target_size), Image.LANCZOS)
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    data = base64.b64encode(buf.getvalue()).decode("ascii")
    return f"data:image/png;base64,{data}"


FILENAME_PATTERN = re.compile(r"^(.+?)\((\d{4})-?(\d{4})?\)+\.png$", re.IGNORECASE)

# Canonical team_name -> exact logo-API filename-team-name, for every
# mismatch found checking all 137 current roster teams against the
# actual file set (see chat history for the discovery process --
# short/formal-name differences, not typos).
NAME_MAP = {
    "California": "Cal",
    "Louisiana Tech": "Louisiana_tech",  # sic -- lowercase "t" in the source filenames
    "Massachusetts": "UMass",
    "Middle Tennessee": "MTSU",
    "NC State": "North_Carolina_State",
    "Ole Miss": "Mississippi",
    "Sam Houston": "Sam_Houston_State",
}


def candidate_filenames_team(team_name: str) -> list:
    if team_name in NAME_MAP:
        return [NAME_MAP[team_name]]
    v1 = team_name.replace(" (", "_").replace(")", "").replace(" ", "_")
    return [v1, v1.replace("'", "")]


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--source", required=True, help="Path to the folder of TeamName(startYear-endYear).png files")
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--out", default="ui/dated_logo_assets.json")
    p.add_argument("--size", type=int, default=TARGET_SIZE, help="Resize target in pixels (square) -- these display as small inline icons")
    args = p.parse_args()

    source_dir = Path(args.source)
    files = list(source_dir.glob("*.png"))

    # Parse every file into (filename_team, start_year, end_year_or_None, path)
    parsed_by_filename_team = {}
    unparseable = []
    for f in files:
        m = FILENAME_PATTERN.match(f.name)
        if not m:
            unparseable.append(f.name)
            continue
        filename_team, start, end = m.group(1), int(m.group(2)), m.group(3)
        end = int(end) if end else None
        parsed_by_filename_team.setdefault(filename_team, []).append((start, end, f))

    if unparseable:
        print(f"WARNING: {len(unparseable)} file(s) didn't match the expected naming pattern, skipped:")
        for u in unparseable:
            print(f"  {u}")

    conn = sqlite3.connect(args.db)
    teams = [r[0] for r in conn.execute("SELECT team_name FROM teams ORDER BY team_name")]
    conn.close()

    result = {}
    unmatched_teams = []

    for team in teams:
        hit = None
        for candidate in candidate_filenames_team(team):
            if candidate in parsed_by_filename_team:
                hit = candidate
                break
        if hit is None:
            unmatched_teams.append(team)
            continue

        ranges = sorted(parsed_by_filename_team[hit], key=lambda r: r[0])
        entries = []
        for start, end, path in ranges:
            entries.append({"start": start, "end": end, "data": encode_resized(path, args.size)})
        result[team] = entries

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w") as f:
        json.dump(result, f)

    total_size = Path(args.out).stat().st_size
    print(f"Wrote {len(result)}/{len(teams)} teams' dated logo sets to {args.out} ({total_size:,} bytes)")
    total_variants = sum(len(v) for v in result.values())
    print(f"Total logo variants across all teams: {total_variants}")

    if unmatched_teams:
        print(f"\n{len(unmatched_teams)} team(s) with NO entry in the source set under any name variant "
              "(will fall back to the existing static logo_assets.json entry, if any):")
        for u in unmatched_teams:
            print(f"  {u}")


if __name__ == "__main__":
    main()
