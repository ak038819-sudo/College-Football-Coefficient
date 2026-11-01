"""Safe backfill staging never requires a real API token in regression tests."""
import csv
import json
from pathlib import Path

import pytest
from src import stage_matchup_backfill as staging


def fake_fetch(year, headers):
    return [
        {"season_year":year,"game_id":99,"week":1,"team":"Alpha","opponent":"Bravo",
         "garbage_time_excluded":1,"off_rush_success_rate":0.0,
         "off_pass_ppa":0.25},
        {"season_year":year,"game_id":99,"week":1,"team":"Bravo","opponent":"Alpha",
         "garbage_time_excluded":1,"off_rush_success_rate":0.45,
         "off_pass_ppa":None}
    ]


def test_staged_fields_and_zero_preserved(monkeypatch, tmp_path):
    monkeypatch.setattr(staging, "fetch_game_advanced", fake_fetch)
    reports = staging.stage_seasons([2025], tmp_path, "test-token")
    assert reports[0]["status"] == "staged_for_review"
    assert reports[0]["split_metrics"]["off_rush_success_rate"]["games_with_at_least_two_sides"] == 1
    rows = list(csv.DictReader((tmp_path / "game_advanced_2025.csv").open()))
    assert rows[0]["off_rush_success_rate"] == "0.0"
    assert rows[1]["off_pass_ppa"] == ""
    assert json.loads((tmp_path / "split_coverage_report.json").read_text())[0]["season"] == 2025


def test_reject_committed_directory_and_missing_token(tmp_path):
    with pytest.raises(ValueError):
        staging.stage_seasons([2025], Path("data/raw"), "token")
    with pytest.raises(ValueError):
        staging.stage_seasons([2025], tmp_path, "")
    with pytest.raises(ValueError):
        staging.stage_seasons([2000], tmp_path, "token")
