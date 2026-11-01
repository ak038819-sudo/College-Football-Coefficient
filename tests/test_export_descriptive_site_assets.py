"""Protect truthful source-refresh fallback and nonmutating scouting exports."""
import csv
from pathlib import Path
from src.export_descriptive_site_assets import has_split_observations, SPLIT_COLUMNS


def test_missing_or_incomplete_refresh_is_not_used(tmp_path):
    missing = tmp_path / "missing.csv"
    assert not has_split_observations(missing)
    incomplete = tmp_path / "advanced.csv"
    incomplete.write_text("game_id,team,off_plays\n1,BYU,75\n")
    assert not has_split_observations(incomplete)


def test_complete_source_requires_all_six_actual_split_values(tmp_path):
    source = tmp_path / "game_advanced.csv"
    fields = ["game_id", "team"] + sorted(SPLIT_COLUMNS)
    with source.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        row = {field: "0.5" for field in SPLIT_COLUMNS}
        writer.writerow({"game_id":"1","team":"BYU",**row})
    assert has_split_observations(source)
    with source.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerow({"game_id":"1","team":"BYU",**{field:"" for field in SPLIT_COLUMNS}})
    assert not has_split_observations(source)
