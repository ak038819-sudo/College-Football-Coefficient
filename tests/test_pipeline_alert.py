"""Drive the real alert logic against a fake GitHub.

The defect this guards: six consecutive scheduled failures published
nothing for four days and told nobody. These tests exercise the decisions
-- open once, comment after, close on recovery -- rather than asserting on
the text of a workflow file.
"""
import importlib.util
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "alert_pipeline_state.py"
_spec = importlib.util.spec_from_file_location("alert_pipeline_state", SCRIPT)
alert_pipeline_state = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(alert_pipeline_state)

alert = alert_pipeline_state.alert
LABEL = alert_pipeline_state.LABEL
MARKER = alert_pipeline_state.MARKER

RUN = "https://github.com/o/r/actions/runs/1"
LATER = "https://github.com/o/r/actions/runs/2"


class FakeGitHub:
    """Just enough of the issues API, recording every call."""

    def __init__(self, issues=None):
        self.issues = list(issues or [])
        self.calls = []
        self.next_number = 100

    def request(self, method, path, body=None):
        self.calls.append((method, path, body))
        if method == "GET" and path.startswith("/issues?"):
            assert f"labels={LABEL}" in path, "must filter by the alert label"
            assert "state=open" in path, "a closed alert must not be reused"
            return [i for i in self.issues if i["state"] == "open"]
        if method == "POST" and path == "/issues":
            issue = {"number": self.next_number, "state": "open", **body}
            self.next_number += 1
            self.issues.append(issue)
            return issue
        if method == "POST" and path.endswith("/comments"):
            return {"id": 1}
        if method == "PATCH" and path.startswith("/issues/"):
            number = int(path.rsplit("/", 1)[1])
            for issue in self.issues:
                if issue["number"] == number:
                    issue.update(body)
            return {"number": number}
        raise AssertionError(f"unexpected call: {method} {path}")

    def comments(self):
        return [b["body"] for m, p, b in self.calls
                if m == "POST" and p.endswith("/comments")]


def open_alert(number=7):
    return {"number": number, "state": "open",
            "body": f"{MARKER}\nsomething failed"}


def test_the_first_failure_opens_one_labelled_issue():
    gh = FakeGitHub()
    assert alert(gh.request, "failed", "Deploy", "main", RUN) == "opened #100"
    created = gh.issues[0]
    assert created["labels"] == [LABEL]
    assert MARKER in created["body"]
    assert RUN in created["body"]
    assert "main" in created["title"]


def test_a_second_failure_comments_instead_of_opening_another_issue():
    gh = FakeGitHub([open_alert()])
    assert alert(gh.request, "failed", "Deploy", "main", LATER) == "commented on #7"
    assert gh.comments() == ["Still failing on `main`: " + LATER]
    assert not [c for c in gh.calls if c[0] == "POST" and c[1] == "/issues"]


def test_recovery_comments_and_closes_the_open_alert():
    gh = FakeGitHub([open_alert()])
    assert alert(gh.request, "recovered", "Deploy", "main", LATER) == "closed #7"
    assert gh.comments() == ["Recovered on `main`: " + LATER]
    assert gh.issues[0]["state"] == "closed"


def test_recovery_with_nothing_flagged_does_not_touch_anything():
    gh = FakeGitHub()
    result = alert(gh.request, "recovered", "Deploy", "main", LATER)
    assert result == "nothing to close; the pipeline was not flagged as failing"
    # Every green run calls this. It must cost one read and write nothing.
    assert [m for m, _, _ in gh.calls] == ["GET"]


def test_an_unrelated_open_issue_is_not_mistaken_for_the_alert():
    gh = FakeGitHub([{"number": 3, "state": "open",
                      "body": "a human wrote this about the pipeline"}])
    assert alert(gh.request, "failed", "Deploy", "main", RUN) == "opened #100"
    assert gh.comments() == []


def test_an_alert_a_human_retitled_is_still_found():
    issue = open_alert(42)
    issue["title"] = "pipeline red -- looking into it"
    gh = FakeGitHub([issue])
    assert alert(gh.request, "failed", "Deploy", "main", LATER) == "commented on #42"


def test_a_closed_alert_is_not_reopened_but_replaced():
    gh = FakeGitHub([{"number": 9, "state": "closed",
                      "body": f"{MARKER}\nan earlier outage"}])
    assert alert(gh.request, "failed", "Deploy", "main", RUN) == "opened #100"


def test_a_missing_token_warns_rather_than_failing_the_run(monkeypatch, capsys):
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.setenv("GITHUB_REPOSITORY", "o/r")
    assert alert_pipeline_state.main(["--state", "failed"]) == 0
    assert "no pipeline alert was sent" in capsys.readouterr().out


def test_a_github_outage_warns_rather_than_failing_the_run(monkeypatch, capsys):
    monkeypatch.setenv("GITHUB_TOKEN", "t")
    monkeypatch.setenv("GITHUB_REPOSITORY", "o/r")

    def exploding(_repo, _token):
        def request(*_args, **_kwargs):
            raise alert_pipeline_state.urllib.error.URLError("down")
        return request

    monkeypatch.setattr(alert_pipeline_state, "http_request", exploding)
    assert alert_pipeline_state.main(["--state", "failed"]) == 0
    assert "Could not update the pipeline alert issue" in capsys.readouterr().out


@pytest.mark.parametrize("state", ["failed", "recovered"])
def test_the_alert_never_reports_failure_upward(monkeypatch, state):
    """Alerting is never allowed to turn a green run red, or a red one redder."""
    monkeypatch.setenv("GITHUB_TOKEN", "t")
    monkeypatch.setenv("GITHUB_REPOSITORY", "o/r")
    gh = FakeGitHub()
    monkeypatch.setattr(alert_pipeline_state, "http_request",
                        lambda _r, _t: gh.request)
    assert alert_pipeline_state.main(["--state", state]) == 0


# --- the workflow wiring ---------------------------------------------------
#
# Parsed structure, not matched text: these assert which jobs exist, what
# they depend on and which events they refuse, which is the configuration
# the alert's usefulness rests on.

import yaml

WORKFLOW = Path(__file__).resolve().parents[1] / ".github/workflows/ci-and-deploy.yml"


def _jobs():
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))["jobs"]


def test_the_workflow_may_write_issues():
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    assert workflow["permissions"]["issues"] == "write"


@pytest.mark.parametrize("job", ["alert-failure", "alert-recovery"])
def test_each_alert_job_waits_for_both_build_jobs(job):
    spec = _jobs()[job]
    assert set(spec["needs"]) == {"test", "rebuild-and-deploy"}
    # always(), or a failed test job would skip the very job meant to report it.
    assert "always()" in spec["if"]
    # A red pull request already shows its own check to whoever pushed it.
    assert "github.event_name != 'pull_request'" in spec["if"]
    assert "github.ref == 'refs/heads/main'" in spec["if"]


def test_the_failure_job_fires_when_either_build_job_fails():
    condition = _jobs()["alert-failure"]["if"]
    assert "needs.test.result == 'failure'" in condition
    assert "needs.rebuild-and-deploy.result == 'failure'" in condition


def test_the_recovery_job_only_fires_when_nothing_failed():
    condition = _jobs()["alert-recovery"]["if"]
    assert "needs.test.result == 'success'" in condition
    assert "needs.rebuild-and-deploy.result != 'failure'" in condition


@pytest.mark.parametrize("job,state", [("alert-failure", "failed"),
                                       ("alert-recovery", "recovered")])
def test_each_alert_job_runs_the_real_script_with_its_state(job, state):
    steps = _jobs()[job]["steps"]
    run = next(s["run"] for s in steps if "run" in s)
    assert "scripts/alert_pipeline_state.py" in run
    assert f"--state {state}" in run
    assert SCRIPT.exists()
