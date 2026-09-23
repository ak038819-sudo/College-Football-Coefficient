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
from collections import Counter
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


def extract_secondary_color(img: Image.Image) -> str:
    """
    Finds a genuine brand/accent color from the logo's own pixels, for
    use as a background chip behind the logo -- some logos (especially
    dark or thin-lined ones) are nearly invisible against the
    dashboard's dark theme once their background is transparent, so a
    colored chip drawn from the logo's own palette gives it real
    contrast without introducing an arbitrary/unrelated color.

    Quantizes opaque pixels to reduce anti-aliasing noise, excludes
    near-white/near-black/near-gray (poor, generic chip colors, and
    common as outline/shading rather than genuine brand color), then
    returns the SECOND most common qualifying color -- the most common
    one is often a huge single-color fill (e.g. a solid background
    shape or a helmet color covering half the image), while the second
    tends to be a genuine accent/secondary brand color, which is what
    was asked for specifically.
    """
    img = img.convert("RGBA")
    counts = Counter()
    # Sample every 3rd pixel in each direction -- plenty for a stable
    # color estimate and much faster than every pixel on a 900px source.
    for y in range(0, img.height, 3):
        for x in range(0, img.width, 3):
            r, g, b, a = img.getpixel((x, y))
            if a < 200:
                continue
            mx, mn = max(r, g, b), min(r, g, b)
            if mx > 235 and mn > 200:  # near-white
                continue
            if mx < 40:  # near-black
                continue
            if mx - mn < 20:  # near-gray/desaturated
                continue
            # Quantize to reduce anti-aliasing noise inflating the distinct-color count
            key = (r // 16 * 16, g // 16 * 16, b // 16 * 16)
            counts[key] += 1

    ranked = counts.most_common(2)
    if len(ranked) < 2:
        # No qualifying secondary color (e.g. a strictly black/white/gray
        # logo) -- fall back to whatever qualifying color exists, or a
        # neutral default if there's truly none at all.
        chosen = ranked[0][0] if ranked else (110, 110, 110)
    else:
        chosen = ranked[1][0]
    return "#{:02x}{:02x}{:02x}".format(*chosen)


def encode_resized(path: Path, target_size: int) -> tuple:
    img = Image.open(path)
    img = remove_white_background(img)
    bg_color = extract_secondary_color(img)
    img.thumbnail((target_size, target_size), Image.LANCZOS)
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    data = base64.b64encode(buf.getvalue()).decode("ascii")
    return f"data:image/png;base64,{data}", bg_color


FILENAME_PATTERN = re.compile(r"^(.+?)\((\d{4})-?(\d{4})?\)+\.png$", re.IGNORECASE)

# Canonical team_name -> exact logo-API filename-team-name, for every
# mismatch found checking all 137 current roster teams against the
# actual file set (see chat history for the discovery process --
# short/formal-name differences, not typos).
TEAM_NAME_MAP = {
    "California": "Cal",
    "Louisiana Tech": "Louisiana_tech",  # sic -- lowercase "t" in the source filenames
    "Massachusetts": "UMass",
    "Middle Tennessee": "MTSU",
    "NC State": "North_Carolina_State",
    "Ole Miss": "Mississippi",
    "Sam Houston": "Sam_Houston_State",
}


def candidate_filenames_team(team_name: str) -> list:
    if team_name in TEAM_NAME_MAP:
        return [TEAM_NAME_MAP[team_name]]
    v1 = team_name.replace(" (", "_").replace(")", "").replace(" ", "_")
    return [v1, v1.replace("'", "")]


# Conference filenames turned out to be inconsistent in ways team
# filenames weren't: case varies (BIG_12 vs Big_12), separators vary
# (Mid-American vs Mid_American), and one is a confirmed typo
# (Min_American, meant to be Mid_American). So conference matching
# normalizes BOTH sides (parsed filenames and our canonical names) into
# the same lowercase/underscore space before comparing, rather than
# doing an exact-string lookup like teams do.
CONF_NAME_MAP = {
    "SEC": "southeastern_conference",
    "Pac-12": "pac_12",
    "Mid-American": "mid_american",
    "Big 12": "big_12",
    "Big Ten": "big_ten",
    "Conference USA": "conference_usa",
    "American Athletic": "american_athletic",
    "Mountain West": "mountain_west",
    "Sun Belt": "sun_belt",
}


def normalize_conf_key(name: str) -> str:
    n = name.lower().replace("-", "_").replace(" ", "_")
    if n == "min_american":  # confirmed typo for mid_american
        n = "mid_american"
    return n


def candidate_filenames_conf(conf_name: str) -> list:
    if conf_name in CONF_NAME_MAP:
        return [CONF_NAME_MAP[conf_name]]
    return [normalize_conf_key(conf_name)]


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--source", required=True, help="Path to the folder of Name(startYear-endYear).png files")
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--kind", choices=["teams", "conferences"], default="teams")
    p.add_argument("--out", default=None, help="Defaults to ui/dated_logo_assets.json (teams) or ui/dated_conference_logo_assets.json (conferences)")
    p.add_argument("--size", type=int, default=TARGET_SIZE, help="Resize target in pixels (square) -- these display as small inline icons")
    args = p.parse_args()
    out_path = args.out or (
        "ui/dated_logo_assets.json" if args.kind == "teams" else "ui/dated_conference_logo_assets.json"
    )


    source_dir = Path(args.source)
    files = list(source_dir.glob("*.png"))

    # Parse every file into (parse_key, start_year, end_year_or_None, path).
    # Teams key on the EXACT filename-team-text (proven to have no
    # case/separator inconsistency); conferences key on a NORMALIZED
    # form instead, since that source turned out to be inconsistent in
    # ways teams weren't (BIG_12 vs Big_12, Mid-American vs Mid_American
    # vs the confirmed typo Min_American) -- normalizing both sides
    # (parsed filenames here, and candidate names below) is what lets
    # those variants correctly merge into one group per real conference.
    parsed_by_key = {}
    unparseable = []
    for f in files:
        m = FILENAME_PATTERN.match(f.name)
        if not m:
            unparseable.append(f.name)
            continue
        filename_name, start, end = m.group(1), int(m.group(2)), m.group(3)
        end = int(end) if end else None
        key = filename_name if args.kind == "teams" else normalize_conf_key(filename_name)
        parsed_by_key.setdefault(key, []).append((start, end, f))

    if unparseable:
        print(f"WARNING: {len(unparseable)} file(s) didn't match the expected naming pattern, skipped:")
        for u in unparseable:
            print(f"  {u}")

    conn = sqlite3.connect(args.db)
    if args.kind == "teams":
        names = [r[0] for r in conn.execute("SELECT team_name FROM teams ORDER BY team_name")]
        candidate_fn = candidate_filenames_team
    else:
        names = sorted(set(
            r[0] for r in conn.execute(
                "SELECT DISTINCT conference_real FROM team_membership_by_season WHERE conference_real != 'FBS Independents'"
            )
        ))
        candidate_fn = candidate_filenames_conf
    conn.close()

    result = {}
    unmatched = []

    for name in names:
        hit = None
        for candidate in candidate_fn(name):
            if candidate in parsed_by_key:
                hit = candidate
                break
        if hit is None:
            unmatched.append(name)
            continue

        ranges = sorted(parsed_by_key[hit], key=lambda r: r[0])
        entries = []
        for start, end, path in ranges:
            data, bg_color = encode_resized(path, args.size)
            entries.append({"start": start, "end": end, "data": data, "bg": bg_color})
        result[name] = entries

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(result, f)

    total_size = Path(out_path).stat().st_size
    noun = "teams" if args.kind == "teams" else "conferences"
    print(f"Wrote {len(result)}/{len(names)} {noun}' dated logo sets to {out_path} ({total_size:,} bytes)")
    total_variants = sum(len(v) for v in result.values())
    print(f"Total logo variants across all {noun}: {total_variants}")

    if unmatched:
        print(f"\n{len(unmatched)} {noun[:-1]}(s) with NO entry in the source set under any name variant:")
        for u in unmatched:
            print(f"  {u}")


if __name__ == "__main__":
    main()
