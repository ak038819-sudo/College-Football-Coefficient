"""
Milestone 8: scheduled refresh. Pins the season-picking rule and the workflow's
safety properties, so a future edit can't silently break the schedule, expose
the CFBD secret to other steps, or publish untested fetched data.
"""
import datetime as dt
import os
import re
from pathlib import Path

import yaml

import fetch_cfbd_games as f
from current_season import season_for

REPO = Path(__file__).resolve().parent.parent
WORKFLOW = REPO / ".github" / "workflows" / "ci-and-deploy.yml"


def test_january_title_game_belongs_to_previous_season():
    assert season_for(dt.date(2027, 1, 20)) == 2026
    assert season_for(dt.date(2027, 2, 28)) == 2026
    assert season_for(dt.date(2027, 3, 1)) == 2027
    assert season_for(dt.date(2026, 9, 24)) == 2026
    assert season_for(dt.date(2026, 12, 31)) == 2026


def _workflow():
    d = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    return d, d.get("on", d.get(True))   # PyYAML parses the bare key `on` as True


def test_workflow_runs_on_schedule_and_on_demand():
    _, on = _workflow()
    assert "workflow_dispatch" in on
    crons = [c["cron"] for c in on["schedule"]]
    assert len(crons) >= 1 and all(len(c.split()) == 5 for c in crons)


def test_secret_reaches_only_the_fetch_step():
    d, _ = _workflow()
    for name, job in d["jobs"].items():
        for step in job["steps"]:
            uses_secret = "secrets.CFBD_API_KEY" in str(step)
            assert uses_secret == step.get("name", "").startswith("Fetch fresh data"), (name, step.get("name"))


def test_fetched_data_is_tested_before_it_is_committed():
    steps = _workflow()[0]["jobs"]["rebuild-and-deploy"]["steps"]
    names = [s.get("name", "") for s in steps]
    fetch = next(i for i, n in enumerate(names) if n.startswith("Fetch fresh data"))
    gate = next(i for i, n in enumerate(names) if n.startswith("Test the freshly fetched data"))
    commit = next(i for i, n in enumerate(names) if n.startswith("Commit and push"))
    assert fetch < gate < commit
    assert steps[fetch]["if"] == steps[gate]["if"] == "github.event_name != 'push'"
    assert "data/raw/" in steps[commit]["run"], "fetched data must be committed or the next push rebuild undoes it"


def test_refresh_never_runs_two_deploys_at_once():
    job = _workflow()[0]["jobs"]["rebuild-and-deploy"]
    assert job["concurrency"]["cancel-in-progress"] is False
    assert "workflow_dispatch" in job["if"] and "schedule" in job["if"]


def test_advanced_fetch_can_be_switched_off(tmp_path):
    called = []

    class R:
        def raise_for_status(self): pass
        def json(self): return []

    original_get, original_out = f.requests.get, f.OUT_DIR
    f.requests.get = lambda url, **k: (called.append(url), R())[1]
    f.OUT_DIR = tmp_path
    os.environ["CFBD_API_KEY"], os.environ["CFBD_FETCH_ADVANCED"] = "test-key", "0"
    try:
        assert f.main(2026) == 0
    finally:
        f.requests.get, f.OUT_DIR = original_get, original_out
        del os.environ["CFBD_API_KEY"], os.environ["CFBD_FETCH_ADVANCED"]
    assert not any("advanced" in u for u in called)
    assert not (tmp_path / "advanced_2026.csv").exists()


def test_no_fetch_script_prints_the_key_or_auth_headers():
    for path in ["src/fetch_cfbd_games.py", "src/fetch_cfbd_advanced.py"]:
        text = (REPO / path).read_text(encoding="utf-8")
        for call in re.findall(r"print\((.*)\)", text):
            assert not re.search(r"api_key|headers|Authorization", call), (path, call)
