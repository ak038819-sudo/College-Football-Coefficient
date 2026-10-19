"""
Marks for the schools this project rates nothing (src/fetch_non_fbs_logos.py).

Two things are worth pinning here. The opponent list has to be the same set of
names the database actually holds, because a name this misses is a school that
silently keeps its monogram. And the join has to be by id with nothing guessed:
a school ESPN does not list, or lists twice, gets no mark rather than the wrong
one.
"""
from __future__ import annotations

import base64
import csv
import hashlib
import io
import json

import pytest
from PIL import Image

import export_logo_files as lf
import fetch_non_fbs_logos as fn

REPO = fn.REPO


def png_bytes(color=(32, 64, 192), size=(500, 500)) -> bytes:
    """A real PNG, since the pipeline opens it with Pillow and reads its colors."""
    image = Image.new("RGBA", size, (255, 255, 255, 255))
    image.paste(Image.new("RGBA", (size[0] // 2, size[1] // 2), (*color, 255)), (10, 10))
    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()


def write_csv(path, rows, fields):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


GAME_FIELDS = ["game_id", "home_team", "away_team", "home_classification", "away_classification"]


def game(home, away, home_cls="fbs", away_cls="fcs", game_id=1):
    return {"game_id": game_id, "home_team": home, "away_team": away,
            "home_classification": home_cls, "away_classification": away_cls}


# ---------------- which names the schools are ----------------

def test_the_opponent_is_the_side_this_project_does_not_hold(tmp_path):
    write_csv(tmp_path / "fcs_games_2024.csv", [game("Alabama", "Mercer")], GAME_FIELDS)
    assert fn.opponent_names(tmp_path) == ["Mercer"]


def test_a_game_between_two_schools_we_hold_neither_of_is_not_ours(tmp_path):
    # The schedule file is the whole division, most of it games this project has
    # no side of at all.
    write_csv(tmp_path / "fcs_games_2024.csv",
              [game("Mercer", "Furman", "fcs", "fcs")], GAME_FIELDS)
    assert fn.opponent_names(tmp_path) == []


def test_both_sides_held_is_read_from_the_sources_own_classification(tmp_path):
    # Utah 66, Idaho 14 in 2026: both names resolve, and the row is Idaho's
    # because the source says Utah was the FBS side.
    write_csv(tmp_path / "fcs_games_2026.csv",
              [game("Utah", "Idaho", "fbs", "fcs")], GAME_FIELDS)
    assert fn.opponent_names(tmp_path) == ["Idaho"]
    # Two FBS sides, or a classification the source left blank, is not a
    # division gap, so there is no opponent to name.
    write_csv(tmp_path / "fcs_games_2026.csv",
              [game("Utah", "Idaho", "fbs", "fbs")], GAME_FIELDS)
    assert fn.opponent_names(tmp_path) == []
    write_csv(tmp_path / "fcs_games_2026.csv",
              [game("Utah", "Idaho", "fbs", "")], GAME_FIELDS)
    assert fn.opponent_names(tmp_path) == []


def test_scheduled_games_count_too(tmp_path):
    # A school on next season's schedule has played nobody yet and still needs
    # its mark, or the page gets it only after the game.
    write_csv(tmp_path / "fcs_schedule_2026.csv", [game("Ohio State", "Grambling")], GAME_FIELDS)
    assert fn.opponent_names(tmp_path) == ["Grambling"]


def test_the_opponent_list_is_the_one_the_shipped_pages_hold():
    """Measured against the exported team pages, not against a number here.

    The rule this file reimplements lives in load_fcs_games.py and reads the
    database; getting it subtly wrong would drop schools quietly. So the list is
    compared with the opponents the shipped team-page export actually names.
    """
    export = (REPO / "ui/data/team_pages.js").read_text(encoding="utf-8")
    payload = json.loads(export[export.index("=") + 1:].rstrip().rstrip(";"))
    shipped = {row[5] for row in payload["non_fbs"]}
    dated = json.loads((REPO / "ui/dated_logo_assets.json").read_text(encoding="utf-8"))
    assert shipped, "the export carries no non-FBS games at all"
    # Every school the pages name, except the ones whose own dated set already
    # covers them, is a school this script has to resolve a mark for.
    assert {n for n in fn.opponent_names() if n not in dated} == {n for n in shipped if n not in dated}


# ---------------- the join ----------------

DIRECTORY = {"teams": [
    {"id": "2000", "location": "Mercer", "displayName": "Mercer Bears"},
    {"id": "2429", "location": "Charlotte", "displayName": "Charlotte 49ers"},
    {"id": "3253", "location": "Charlotte", "displayName": "Charlotte Saints"},
    {"id": "135", "location": "Minnesota State", "displayName": "Minnesota State Mavericks"},
]}


def directory(tmp_path):
    path = tmp_path / "espn.json"
    path.write_text(json.dumps(DIRECTORY), encoding="utf-8")
    return fn.espn_directory(path)


def test_a_school_resolves_to_its_espn_id(tmp_path):
    matched, unmatched, ambiguous = fn.resolve(["Mercer"], directory(tmp_path))
    assert matched["Mercer"]["id"] == "2000"
    assert (unmatched, ambiguous) == ([], [])


def test_a_school_espn_does_not_list_gets_no_mark(tmp_path):
    matched, unmatched, ambiguous = fn.resolve(["Lambuth"], directory(tmp_path))
    assert (matched, unmatched, ambiguous) == ({}, ["Lambuth"], [])


def test_two_schools_under_one_name_resolve_to_neither(tmp_path):
    # ESPN lists a Charlotte 49ers and a Charlotte Saints. Picking either would
    # be a guess, and the point of joining on an id is that nothing is guessed.
    matched, unmatched, ambiguous = fn.resolve(["Charlotte"], directory(tmp_path))
    assert (matched, unmatched, ambiguous) == ({}, [], ["Charlotte"])


def test_a_renamed_school_resolves_through_the_rename(tmp_path):
    matched, _, _ = fn.resolve(["Mankato State"], directory(tmp_path))
    assert matched["Mankato State"]["displayName"] == "Minnesota State Mavericks"


def test_the_real_reference_files_resolve_nearly_every_school():
    """The actual join, against the actual committed reference data."""
    dated = json.loads((REPO / "ui/dated_logo_assets.json").read_text(encoding="utf-8"))
    names = [n for n in fn.opponent_names() if n not in dated]
    matched, unmatched, ambiguous = fn.resolve(names, fn.espn_directory())
    assert not ambiguous, f"ambiguous: {ambiguous}"
    # Measured at 186 of 207 on 2026-10-18. Asserted as a share rather than a
    # count so a season of new opponents does not fail it, and as a floor
    # because the only way it drops is a join that stopped working.
    assert len(matched) / len(names) > 0.85, f"only {len(matched)} of {len(names)} resolved"
    # Named schools rather than row zero: a monogram for Mercer or Eastern
    # Washington would be this feature not working.
    for school in ("Mercer", "Eastern Washington", "Montana State", "Grambling", "Furman"):
        assert school in matched, f"{school} no longer resolves"
    # And the schools that genuinely have no ESPN entry are still unresolved,
    # rather than quietly matched to something else.
    assert "Lambuth" in unmatched


# ---------------- the archive manifest ----------------

MANIFEST_FIELDS = ["level", "league", "entity_id", "entity_name", "mark_type", "variant",
                   "source", "sha256", "ext", "bytes", "archive_url", "first_seen", "last_seen"]


def manifest_row(**over):
    row = {"level": "team", "league": "cfb", "entity_id": "2000", "entity_name": "Mercer Bears",
           "mark_type": "logo", "variant": "default", "source": "espn", "sha256": "x",
           "ext": "png", "bytes": "1", "archive_url": "https://cdn/x.png",
           "first_seen": "2026-09-26", "last_seen": "2026-10-01"}
    row.update(over)
    return row


def manifest_csv(rows) -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=MANIFEST_FIELDS)
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def test_only_college_football_team_logos_are_taken_and_only_the_two_variants():
    rows = [manifest_row(),
            manifest_row(variant="dark", sha256="d", archive_url="https://cdn/d.png"),
            manifest_row(entity_id="1", league="nfl"),
            manifest_row(entity_id="2", level="conference"),
            manifest_row(entity_id="3", mark_type="wordmark"),
            manifest_row(entity_id="4", variant="scoreboard"),
            manifest_row(entity_id="5", ext="svg")]
    marks = fn.sdv_marks(manifest_csv(rows))
    assert sorted(marks) == ["2000"]
    # The two artworks of one school are two rows under one id, not two schools.
    assert sorted(marks["2000"]) == [fn.DARK_VARIANT, fn.DEFAULT_VARIANT]


def test_a_school_the_archive_has_no_dark_artwork_for_simply_has_none():
    marks = fn.sdv_marks(manifest_csv([manifest_row()]))
    assert sorted(marks["2000"]) == [fn.DEFAULT_VARIANT]


def test_the_newest_capture_of_a_changed_mark_wins():
    # A school that rebrands gets a second row; the old one keeps its last_seen.
    rows = [manifest_row(sha256="old", archive_url="https://cdn/old.png", last_seen="2026-10-01"),
            manifest_row(sha256="new", archive_url="https://cdn/new.png", last_seen="2026-11-01")]
    assert fn.sdv_marks(manifest_csv(rows))["2000"][fn.DEFAULT_VARIANT]["sha256"] == "new"
    assert fn.sdv_marks(manifest_csv(list(reversed(rows))))["2000"][fn.DEFAULT_VARIANT]["sha256"] == "new"
    # And the newest of each variant is picked independently of the other.
    rows.append(manifest_row(variant="dark", sha256="dark-old", last_seen="2026-09-28",
                             archive_url="https://cdn/dark-old.png"))
    rows.append(manifest_row(variant="dark", sha256="dark-new", last_seen="2026-11-02",
                             archive_url="https://cdn/dark-new.png"))
    marks = fn.sdv_marks(manifest_csv(rows))["2000"]
    assert (marks[fn.DEFAULT_VARIANT]["sha256"], marks[fn.DARK_VARIANT]["sha256"]) == ("new", "dark-new")


# ---------------- end to end ----------------

@pytest.fixture
def world(tmp_path):
    """A whole small world: two games, a directory, a dated set and an archive."""
    raw = tmp_path / "raw"
    raw.mkdir()
    write_csv(raw / "fcs_games_2024.csv",
              [game("Alabama", "Mercer", game_id=1),
               game("Georgia", "App State", game_id=2)], GAME_FIELDS)
    directory_path = tmp_path / "espn.json"
    directory_path.write_text(json.dumps({"teams": [
        {"id": "2000", "location": "Mercer", "displayName": "Mercer Bears"},
        {"id": "2026", "location": "App State", "displayName": "App State Mountaineers"},
    ]}), encoding="utf-8")
    dated_path = tmp_path / "dated.json"
    era = {"start": 1999, "end": None,
           "data": "data:image/png;base64," + base64.b64encode(png_bytes()).decode(), "bg": "#abcdef"}
    dated_path.write_text(json.dumps({"Appalachian State": [era]}), encoding="utf-8")

    image = png_bytes()
    marks = manifest_csv([manifest_row(sha256=hashlib.sha256(image).hexdigest(),
                                       archive_url="https://cdn/mercer.png")])
    served = {"https://manifest": marks.encode("utf-8"), "https://cdn/mercer.png": image}
    calls = []

    def fetcher(url):
        calls.append(url)
        return served[url]

    return {"raw": raw, "directory": directory_path, "dated": dated_path,
            "out": tmp_path / "non_fbs.json", "sources": tmp_path / "sources.csv",
            "fetcher": fetcher, "calls": calls, "image": image}


def run(world, **over):
    kwargs = dict(manifest_url="https://manifest", out=world["out"], sources_out=world["sources"],
                  raw_dir=world["raw"], directory_path=world["directory"],
                  dated_path=world["dated"], fetcher=world["fetcher"])
    kwargs.update(over)
    return fn.build(**kwargs)


def test_a_fetched_mark_is_one_undated_entry_with_a_chip_color(world):
    report = run(world)
    written = json.loads(world["out"].read_text(encoding="utf-8"))
    assert report["fetched"] == 1
    entries = written["Mercer"]
    assert len(entries) == 1
    assert (entries[0]["start"], entries[0]["end"]) == (fn.UNDATED_START, None)
    assert entries[0]["bg"].startswith("#") and len(entries[0]["bg"]) == 7
    # Resized to the icon it is displayed as, not the 500px source.
    raw = base64.b64decode(entries[0]["data"].partition(",")[2])
    assert max(Image.open(io.BytesIO(raw)).size) == fn.MARK_SIZE


def dark_png(size=(500, 500)) -> bytes:
    """ESPN's dark-background artwork: light ink, nothing behind it."""
    image = Image.new("RGBA", size, (255, 255, 255, 0))
    image.paste(Image.new("RGBA", (size[0] // 2, size[1] // 2), (255, 255, 255, 255)), (10, 10))
    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()


def with_dark(world, image=None, url="https://cdn/mercer-dark.png"):
    """Give Mercer a dark artwork in the archive as well as a colour one."""
    dark = image if image is not None else dark_png()
    rows = [manifest_row(sha256=hashlib.sha256(world["image"]).hexdigest(),
                         archive_url="https://cdn/mercer.png"),
            manifest_row(variant="dark", sha256=hashlib.sha256(dark).hexdigest(),
                         archive_url=url)]
    served = {"https://manifest": manifest_csv(rows).encode("utf-8"),
              "https://cdn/mercer.png": world["image"], url: dark}

    def fetcher(url_asked):
        world["calls"].append(url_asked)
        if url_asked not in served:
            raise OSError("404")
        return served[url_asked]

    world["fetcher"] = fetcher
    world["dark"] = dark
    return world


def test_a_school_with_a_dark_artwork_carries_both(world):
    report = run(with_dark(world))
    entry = json.loads(world["out"].read_text(encoding="utf-8"))["Mercer"][0]
    assert (report["fetched"], report["dark_fetched"]) == (1, 1)
    assert report["missing_dark"] == []
    # Same era, same school, two images: the second is not a second entry.
    assert entry["dark"].startswith("data:image/png;base64,")
    for key in ("data", "dark"):
        raw = base64.b64decode(entry[key].partition(",")[2])
        assert max(Image.open(io.BytesIO(raw)).size) == fn.MARK_SIZE
    # White is the dark artwork's ink, so it is still there.
    dark = Image.open(io.BytesIO(base64.b64decode(entry["dark"].partition(",")[2]))).convert("RGBA")
    assert any(count and pixel[3] > 200 and min(pixel[:3]) > 240
               for count, pixel in dark.getcolors(maxcolors=1 << 20)), "the light ink was stripped"


def test_a_school_the_archive_has_no_dark_artwork_for_keeps_its_colour_mark(world):
    report = run(world)
    entry = json.loads(world["out"].read_text(encoding="utf-8"))["Mercer"][0]
    assert report["missing_dark"] == ["Mercer"] and report["dark_fetched"] == 0
    assert "dark" not in entry and entry["data"]


def test_a_dark_artwork_that_will_not_download_costs_the_variant_not_the_school(world):
    with_dark(world)
    inner = world["fetcher"]

    def fetcher(url):
        if url.endswith("mercer-dark.png"):
            raise OSError("404")
        return inner(url)

    report = run(world, fetcher=fetcher)
    entry = json.loads(world["out"].read_text(encoding="utf-8"))["Mercer"][0]
    assert report["dark_failed"] == ["Mercer"] and report["fetched"] == 1
    assert "dark" not in entry and entry["data"]


def test_dark_bytes_that_do_not_match_the_manifest_stop_the_run(world):
    with_dark(world)
    served_dark = world["dark"]
    inner = world["fetcher"]

    def fetcher(url):
        if url.endswith("mercer-dark.png"):
            inner(url)
            return dark_png(size=(400, 400))  # a different image, so a different digest
        return inner(url)

    assert served_dark  # the manifest describes the one above, not this
    with pytest.raises(ValueError, match="archive served"):
        run(world, fetcher=fetcher)


def test_the_dark_provenance_is_written_down_too(world):
    run(with_dark(world))
    rows = {r["opponent_name"]: r for r in csv.DictReader(world["sources"].open(encoding="utf-8"))}
    assert rows["Mercer"]["dark_sha256"] == hashlib.sha256(world["dark"]).hexdigest()
    assert rows["Mercer"]["dark_archive_url"] == "https://cdn/mercer-dark.png"


def test_an_fbs_program_under_an_older_name_takes_its_own_dated_eras(world):
    report = run(world)
    written = json.loads(world["out"].read_text(encoding="utf-8"))
    assert report["aliased_to_dated_set"] == ["App State"]
    eras = json.loads(world["dated"].read_text())["Appalachian State"]
    assert [{k: v for k, v in e.items() if k != "espn"} for e in written["App State"]] == eras
    # The id rides along even here, so the live scoreboard can find the mark
    # under the name the feed uses.
    assert {e["espn"] for e in written["App State"]} == {"2026"}
    # Nothing was fetched for it: the manifest and one image, and that is all.
    assert world["calls"] == ["https://manifest", "https://cdn/mercer.png"]


def test_bytes_that_do_not_match_the_manifest_stop_the_run(world):
    other = png_bytes(color=(200, 10, 10))
    world["fetcher"] = lambda url: (other if url.endswith("mercer.png")
                                    else manifest_csv([manifest_row(
                                        sha256=hashlib.sha256(world["image"]).hexdigest(),
                                        archive_url="https://cdn/mercer.png")]).encode("utf-8"))
    with pytest.raises(ValueError, match="archive served"):
        run(world)


def test_the_provenance_of_every_school_is_written_down(world):
    run(world)
    rows = {r["opponent_name"]: r for r in csv.DictReader(world["sources"].open(encoding="utf-8"))}
    assert rows["Mercer"]["espn_id"] == "2000"
    assert rows["Mercer"]["sha256"] == hashlib.sha256(world["image"]).hexdigest()
    assert rows["App State"]["source"] == "dated set"


def test_a_dry_run_resolves_without_touching_the_network(world):
    report = run(world, dry_run=True)
    assert world["calls"] == []
    assert not world["out"].exists()
    assert report["matched"] == 2 and report["fetched"] == 0


# ---------------- what the page gets ----------------

def test_the_non_fbs_marks_are_their_own_section_of_the_manifest(tmp_path):
    uri = "data:image/png;base64," + base64.b64encode(png_bytes()).decode()
    teams = tmp_path / "teams.json"
    teams.write_text(json.dumps({"Idaho": [{"start": 1980, "end": None, "data": uri, "bg": "#111111"}]}),
                     encoding="utf-8")
    non_fbs = tmp_path / "non_fbs.json"
    non_fbs.write_text(json.dumps({"Mercer": [{"start": 0, "end": None, "data": uri, "bg": "#222222"}]}),
                       encoding="utf-8")
    out, manifest_path = tmp_path / "logos", tmp_path / "manifest.json"
    stats = lf.export({"teams": teams, "non_fbs": non_fbs}, out, manifest_path)
    manifest = json.loads(manifest_path.read_text())
    assert set(manifest) == {"teams", "non_fbs"}
    assert manifest["non_fbs"]["Mercer"][0]["src"].startswith("assets/logos/non_fbs/mercer/0-now.png?v=")
    assert (out / "non_fbs" / "mercer" / "0-now.png").exists()
    assert stats["non_fbs"] == {"names": 1, "files": 1}


def test_the_dark_artwork_is_exported_as_its_own_file_beside_the_colour_one(tmp_path):
    uri = "data:image/png;base64," + base64.b64encode(png_bytes()).decode()
    dark_uri = "data:image/png;base64," + base64.b64encode(dark_png()).decode()
    non_fbs = tmp_path / "non_fbs.json"
    non_fbs.write_text(json.dumps({"Mercer": [{"start": 0, "end": None, "data": uri,
                                               "bg": "#222222", "dark": dark_uri}]}),
                       encoding="utf-8")
    out, manifest_path = tmp_path / "logos", tmp_path / "manifest.json"
    stats = lf.export({"non_fbs": non_fbs}, out, manifest_path)
    entry = json.loads(manifest_path.read_text())["non_fbs"]["Mercer"][0]
    assert entry["darkSrc"].startswith("assets/logos/non_fbs/mercer/0-now-dark.png?v=")
    assert entry["src"].startswith("assets/logos/non_fbs/mercer/0-now.png?v=")
    assert (out / "non_fbs" / "mercer" / "0-now-dark.png").read_bytes() == dark_png()
    # Both files are counted, and a school without a dark artwork gets no key.
    assert stats["non_fbs"] == {"names": 1, "files": 2}


def test_an_entry_with_no_dark_artwork_gets_no_dark_key(tmp_path):
    uri = "data:image/png;base64," + base64.b64encode(png_bytes()).decode()
    non_fbs = tmp_path / "non_fbs.json"
    non_fbs.write_text(json.dumps({"Mercer": [{"start": 0, "end": None, "data": uri, "bg": "#222"}]}),
                       encoding="utf-8")
    manifest_path = tmp_path / "manifest.json"
    lf.export({"non_fbs": non_fbs}, tmp_path / "logos", manifest_path)
    assert "darkSrc" not in json.loads(manifest_path.read_text())["non_fbs"]["Mercer"][0]


def test_two_eras_that_differ_only_in_their_dark_artwork_are_a_conflict(tmp_path):
    """The duplicate check has to see the whole entry, or one silently wins."""
    uri = "data:image/png;base64," + base64.b64encode(png_bytes()).decode()
    dark_uri = "data:image/png;base64," + base64.b64encode(dark_png()).decode()
    non_fbs = tmp_path / "non_fbs.json"
    non_fbs.write_text(json.dumps({"Mercer": [
        {"start": 0, "end": None, "data": uri, "bg": "#222", "dark": dark_uri},
        {"start": 0, "end": None, "data": uri, "bg": "#222"}]}), encoding="utf-8")
    with pytest.raises(ValueError, match="two different logos"):
        lf.export({"non_fbs": non_fbs}, tmp_path / "logos", tmp_path / "manifest.json")


def test_white_is_only_flood_filled_out_where_it_is_the_background(tmp_path):
    """encode_resized's remove_white flag, which the dark artwork turns off."""
    import build_dated_logo_assets as bd
    path = tmp_path / "mark.png"
    path.write_bytes(png_bytes())  # a white field with a coloured square on it

    def corner_alpha(data_uri):
        raw = base64.b64decode(data_uri.partition(",")[2])
        return Image.open(io.BytesIO(raw)).convert("RGBA").getpixel((0, 0))[3]

    stripped, _ = bd.encode_resized(path, 64)
    kept, _ = bd.encode_resized(path, 64, remove_white=False)
    assert corner_alpha(stripped) == 0, "the white background survived"
    assert corner_alpha(kept) == 255, "white was removed from a mark that is made of it"


def test_a_missing_source_file_leaves_its_section_out(tmp_path):
    """The marks are fetched by a workflow of their own, so the file can be absent."""
    uri = "data:image/png;base64," + base64.b64encode(png_bytes()).decode()
    teams = tmp_path / "teams.json"
    teams.write_text(json.dumps({"Idaho": [{"start": 1980, "end": None, "data": uri, "bg": "#111111"}]}),
                     encoding="utf-8")
    manifest_path = tmp_path / "manifest.json"
    lf.export({"teams": teams, "non_fbs": tmp_path / "missing.json"}, tmp_path / "logos", manifest_path)
    assert set(json.loads(manifest_path.read_text())) == {"teams"}


# ---------------- the workflow that fetches them ----------------

def test_the_fetch_workflow_keeps_its_own_lock_and_commits_the_whole_set():
    """The two ways this job could quietly do nothing useful.

    A job queued in the deploy's concurrency group is cancelled outright by the
    next run to enter it, which is how a dispatched backfill was lost on
    2026-10-03. And a commit of the source set without the manifest would leave
    the page's manifest pointing at marks nothing had written yet.
    """
    import yaml
    workflow = yaml.safe_load(
        (REPO / ".github/workflows/fetch-non-fbs-logos.yml").read_text(encoding="utf-8"))
    job = workflow["jobs"]["fetch"]
    assert job["concurrency"]["group"] != "rebuild-and-deploy-main"

    steps = job["steps"]
    publish = next(s for s in steps if s.get("name") == "Publish")
    for path in ("ui/non_fbs_logo_assets.json", "ui/logo_manifest.json", "ui/assets/logos/",
                 "data/reference/non_fbs_logo_sources.csv"):
        assert path in publish["run"], f"{path} is not published"
    # Nothing rebuilds on a [skip ci] commit, so the message has to carry it and
    # the run has to say what is still needed.
    assert "[skip ci]" in publish["run"]
    assert any("ci-and-deploy" in str(s.get("run", "")) for s in steps)

    fetch = next(s for s in steps if s.get("name", "").startswith("Fetch the marks"))
    assert "src/fetch_non_fbs_logos.py" in fetch["run"]
    assert fetch["run"].index("fetch_non_fbs_logos.py") < fetch["run"].index("export_logo_files.py")
    # A dry run resolves and reports without writing anything, so the fetching
    # steps are the only gated ones.
    gated = {s.get("name") for s in steps if s.get("if")}
    assert gated == {"Fetch the marks and rebuild the logo manifest", "Publish",
                     "Say what has to happen next"}
    assert all("dry_run" in s["if"] for s in steps if s.get("if"))


# ---------------- when the archive is unreliable ----------------

def test_one_image_the_archive_will_not_serve_costs_one_school(world):
    """A stale manifest row is one school's mark, not the whole set's."""
    manifest = manifest_csv([
        manifest_row(sha256=hashlib.sha256(world["image"]).hexdigest(),
                     archive_url="https://cdn/mercer.png"),
        manifest_row(entity_id="2026", entity_name="App State Mountaineers",
                     sha256="whatever", archive_url="https://cdn/gone.png")])

    def fetcher(url):
        if url == "https://manifest":
            return manifest.encode("utf-8")
        if url == "https://cdn/gone.png":
            raise OSError("404")
        return world["image"]

    # App State takes its dated set, so give the failing row to a school that
    # has to be fetched: another game, another opponent.
    write_csv(world["raw"] / "fcs_games_2025.csv",
              [game("Alabama", "Furman", game_id=3)], GAME_FIELDS)
    (world["directory"]).write_text(json.dumps({"teams": [
        {"id": "2000", "location": "Mercer", "displayName": "Mercer Bears"},
        {"id": "2026", "location": "Furman", "displayName": "Furman Paladins"},
    ]}), encoding="utf-8")

    report = run(world, fetcher=fetcher)
    written = json.loads(world["out"].read_text(encoding="utf-8"))
    assert report["fetch_failed"] == ["Furman"]
    assert report["fetched"] == 1
    assert "Mercer" in written and "Furman" not in written
    # And the provenance file says a mark was looked for and not found.
    rows = {r["opponent_name"]: r for r in csv.DictReader(world["sources"].open(encoding="utf-8"))}
    assert rows["Furman"]["sha256"] == "" and rows["Furman"]["archive_url"] == "https://cdn/gone.png"


def test_an_archive_that_is_simply_down_writes_nothing(world):
    manifest = manifest_csv([manifest_row(sha256="x", archive_url="https://cdn/mercer.png")])

    def fetcher(url):
        if url == "https://manifest":
            return manifest.encode("utf-8")
        raise OSError("connection refused")

    with pytest.raises(RuntimeError, match="reads as an outage"):
        run(world, fetcher=fetcher)
    assert not world["out"].exists(), "a partial set was written"
