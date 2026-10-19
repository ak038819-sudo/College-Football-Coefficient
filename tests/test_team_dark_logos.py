"""
The light-ink artwork for our own teams (src/fetch_team_dark_logos.py).

Two things matter here. It has to land on the era a page shows TODAY and no
other, because ESPN serves today's mark and a 1985 page must keep its 1985 one.
And the join has to be the recorded ESPN id rather than a name, because our
canonical names ('Ole Miss', 'Massachusetts', 'App State') are not the names
any provider files those schools under.
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
import fetch_team_dark_logos as fd
from test_non_fbs_logos import dark_png, manifest_csv, manifest_row, png_bytes

REPO = fd.REPO


def uri(raw: bytes) -> str:
    return "data:image/png;base64," + base64.b64encode(raw).decode()


# ---------------- which era the artwork belongs to ----------------

def test_the_current_era_is_the_latest_one_not_the_open_one():
    """Most eras in the shipped set are closed and end at the current season."""
    eras = [{"start": 1956, "end": 1999}, {"start": 2020, "end": 2026}, {"start": 1999, "end": 2005}]
    assert fd.current_era(eras) == {"start": 2020, "end": 2026}
    # An open end is still the current one, whatever years sit beside it.
    assert fd.current_era(eras + [{"start": 2026, "end": None}]) == {"start": 2026, "end": None}
    assert fd.current_era([]) is None


def test_every_team_we_rate_has_a_recorded_espn_id():
    """The join this script depends on, measured against the shipped files."""
    ids = fd.team_espn_ids()
    dated = json.loads((REPO / "ui/dated_logo_assets.json").read_text(encoding="utf-8"))
    assert len(dated) > 100
    assert [name for name in dated if name not in ids] == []
    # Named teams rather than a count: these are the spellings no name match
    # would resolve, and they are exactly why the id is the join.
    for team in ("Ole Miss", "Massachusetts", "Miami (FL)", "FIU"):
        assert ids[team].isdigit(), f"{team} has no ESPN id"


# ---------------- the fetch ----------------

@pytest.fixture
def world(tmp_path):
    dated = tmp_path / "dated.json"
    dated.write_text(json.dumps({
        "Alabama": [{"start": 2004, "end": 2018, "data": uri(png_bytes()), "bg": "#111111"},
                    {"start": 2018, "end": 2026, "data": uri(png_bytes()), "bg": "#222222"}],
        "Akron": [{"start": 2022, "end": 2026, "data": uri(png_bytes()), "bg": "#333333"}],
    }), encoding="utf-8")
    identities = tmp_path / "identities.json"
    identities.write_text(json.dumps({"teams": [
        {"canonical_name": "Alabama", "provider_ids": {"espn": 333}},
        {"canonical_name": "Akron", "provider_ids": {"espn": 2006}},
    ]}), encoding="utf-8")

    dark = dark_png()
    manifest = manifest_csv([
        manifest_row(entity_id="333", variant="dark", sha256=hashlib.sha256(dark).hexdigest(),
                     archive_url="https://cdn/alabama-dark.png"),
        manifest_row(entity_id="333", variant="default", sha256="colour",
                     archive_url="https://cdn/alabama.png")])
    served = {"https://manifest": manifest.encode("utf-8"), "https://cdn/alabama-dark.png": dark}
    calls = []

    def fetcher(url):
        calls.append(url)
        if url not in served:
            raise OSError("404")
        return served[url]

    return {"dated": dated, "identities": identities, "out": tmp_path / "dark.json",
            "sources": tmp_path / "sources.csv", "fetcher": fetcher, "calls": calls, "dark": dark}


def run(world, **over):
    kwargs = dict(manifest_url="https://manifest", out=world["out"], sources_out=world["sources"],
                  dated_path=world["dated"], identities_path=world["identities"],
                  fetcher=world["fetcher"])
    kwargs.update(over)
    return fd.build(**kwargs)


def test_the_artwork_lands_on_the_current_era_alone(world):
    report = run(world)
    written = json.loads(world["out"].read_text(encoding="utf-8"))
    assert report["fetched"] == 1
    assert (written["Alabama"]["start"], written["Alabama"]["end"]) == (2018, 2026)
    assert written["Alabama"]["espn"] == "333"
    assert max(Image.open(io.BytesIO(
        base64.b64decode(written["Alabama"]["data"].partition(",")[2]))).size) == fd.MARK_SIZE
    # The colour mark of the same team is never downloaded: this script only
    # adds what the dated set does not already have.
    assert "https://cdn/alabama.png" not in world["calls"]


def test_white_survives_because_it_is_the_ink(world):
    run(world)
    written = json.loads(world["out"].read_text(encoding="utf-8"))
    image = Image.open(io.BytesIO(
        base64.b64decode(written["Alabama"]["data"].partition(",")[2]))).convert("RGBA")
    assert any(count and pixel[3] > 200 and min(pixel[:3]) > 240
               for count, pixel in image.getcolors(maxcolors=1 << 20))


def test_a_team_the_archive_has_no_dark_artwork_for_is_reported_not_fatal(world):
    report = run(world)
    assert report["missing_dark"] == ["Akron"]
    assert "Akron" not in json.loads(world["out"].read_text(encoding="utf-8"))


def test_bytes_that_do_not_match_the_manifest_stop_the_run(world):
    inner = world["fetcher"]

    def fetcher(url):
        inner(url)
        return dark_png(size=(400, 400)) if url.endswith("alabama-dark.png") else inner(url)

    with pytest.raises(ValueError, match="archive served"):
        run(world, fetcher=fetcher)


def test_an_archive_that_is_down_writes_nothing(world):
    manifest = manifest_csv([
        manifest_row(entity_id="333", variant="dark", sha256="x", archive_url="https://cdn/a.png"),
        manifest_row(entity_id="2006", variant="dark", sha256="y", archive_url="https://cdn/b.png")])

    def fetcher(url):
        if url == "https://manifest":
            return manifest.encode("utf-8")
        raise OSError("connection refused")

    with pytest.raises(RuntimeError, match="reads as an outage"):
        run(world, fetcher=fetcher)
    assert not world["out"].exists()


def test_the_provenance_of_every_team_is_written_down(world):
    run(world)
    rows = {r["team_name"]: r for r in csv.DictReader(world["sources"].open(encoding="utf-8"))}
    assert rows["Alabama"]["sha256"] == hashlib.sha256(world["dark"]).hexdigest()
    assert (rows["Alabama"]["era_start"], rows["Alabama"]["era_end"]) == ("2018", "2026")
    # A team with no artwork is still a row, with its id and era and no source.
    assert rows["Akron"]["espn_id"] == "2006" and rows["Akron"]["sha256"] == ""


# ---------------- what the page gets ----------------

def test_the_overlay_reaches_the_manifest_as_the_current_eras_dark_source(tmp_path):
    teams = tmp_path / "teams.json"
    teams.write_text(json.dumps({"Alabama": [
        {"start": 2004, "end": 2018, "data": uri(png_bytes()), "bg": "#111111"},
        {"start": 2018, "end": 2026, "data": uri(png_bytes()), "bg": "#222222"}]}), encoding="utf-8")
    overlay = tmp_path / "dark.json"
    overlay.write_text(json.dumps({"Alabama": {"espn": "333", "start": 2018, "end": 2026,
                                               "data": uri(dark_png())}}), encoding="utf-8")
    manifest_path = tmp_path / "manifest.json"
    stats = lf.export({"teams": teams}, tmp_path / "logos", manifest_path,
                      dark_overlays={"teams": overlay})
    eras = json.loads(manifest_path.read_text())["teams"]["Alabama"]
    current = next(e for e in eras if e["start"] == 2018)
    older = next(e for e in eras if e["start"] == 2004)
    assert current["darkSrc"].startswith("assets/logos/teams/alabama/2018-2026-dark.png?v=")
    assert "darkSrc" not in older, "the 1985 page would show today's artwork"
    assert stats["stale_dark_overlays"] == []


def test_an_overlay_for_an_era_that_no_longer_exists_is_dropped_and_reported(tmp_path):
    """A redrawn logo gives the team a new era; the overlay is then stale."""
    teams = tmp_path / "teams.json"
    teams.write_text(json.dumps({"Alabama": [
        {"start": 2026, "end": None, "data": uri(png_bytes()), "bg": "#222222"}]}), encoding="utf-8")
    overlay = tmp_path / "dark.json"
    overlay.write_text(json.dumps({"Alabama": {"espn": "333", "start": 2018, "end": 2026,
                                               "data": uri(dark_png())}}), encoding="utf-8")
    manifest_path = tmp_path / "manifest.json"
    stats = lf.export({"teams": teams}, tmp_path / "logos", manifest_path,
                      dark_overlays={"teams": overlay})
    entry = json.loads(manifest_path.read_text())["teams"]["Alabama"][0]
    assert "darkSrc" not in entry
    assert stats["stale_dark_overlays"] == ["teams: Alabama 2018-2026"]


def test_the_workflow_fetches_and_publishes_both_sets():
    """One dispatch has to leave the manifest and every file it names in step."""
    import yaml
    workflow = yaml.safe_load(
        (REPO / ".github/workflows/fetch-non-fbs-logos.yml").read_text(encoding="utf-8"))
    steps = workflow["jobs"]["fetch"]["steps"]
    fetch = next(s for s in steps if s.get("name", "").startswith("Fetch the marks"))
    run = fetch["run"]
    for script in ("src/fetch_non_fbs_logos.py", "src/fetch_team_dark_logos.py"):
        assert script in run
        assert run.index(script) < run.index("src/export_logo_files.py"), \
            f"{script} runs after the export that reads what it writes"
    publish = next(s for s in steps if s.get("name") == "Publish")
    for path in ("ui/non_fbs_logo_assets.json", "ui/dated_logo_dark_assets.json",
                 "ui/logo_manifest.json", "ui/assets/logos/",
                 "data/reference/team_dark_logo_sources.csv"):
        assert path in publish["run"], f"{path} is not published"
