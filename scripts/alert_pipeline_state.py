#!/usr/bin/env python3
"""Make a dark publishing pipeline visible, in the repository itself.

Every scheduled run of ci-and-deploy failed from 2026-10-09 06:31 UTC to
2026-10-10 00:13 UTC -- six runs, four days with no ratings refresh
published -- and nothing announced it. A failing scheduled workflow is
invisible unless somebody opens the Actions tab, which is exactly when
nobody does.

So this keeps ONE open issue as the pipeline's health light:

  failed     an open alert exists -> comment on it with the new run
             no open alert        -> open one
  recovered  an open alert exists -> comment and close it
             no open alert        -> do nothing, print why

One issue rather than one per run, because the failure that matters here
repeats on a schedule: six issues would be six notifications saying the
same thing, and the thing worth seeing is "still broken, since when".
Closing it on recovery is what makes an open issue mean "broken now"
rather than "broke once".

The GitHub calls go through an injected `request`, so the tests drive
this real decision logic instead of asserting on workflow text.

Usage:
    scripts/alert_pipeline_state.py --state failed \
        --repo owner/name --workflow "Test, Rebuild..." --branch main \
        --run-url https://github.com/... --run-id 123
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request

API = "https://api.github.com"
LABEL = "pipeline-alert"
# One health light per pipeline, not one for the repository. Football's
# deploy and the basketball refresh fail for unrelated reasons, and a
# basketball failure that closed football's alert -- or the reverse --
# would be worse than no alert at all. The scope goes in the marker, so
# both lights share a label and a query while staying independent.
DEFAULT_SCOPE = "ci-and-deploy"
# The issue is found by this marker, not by its title, so the title stays
# free to say something useful without breaking the lookup.
MARKER = "<!-- pipeline-alert:ci-and-deploy -->"


def marker_for(scope: str) -> str:
    return f"<!-- pipeline-alert:{scope} -->"


def http_request(repo: str, token: str):
    """Real transport. Returns a `request(method, path, body)` callable."""

    def request(method: str, path: str, body: dict | None = None):
        url = f"{API}/repos/{repo}{path}"
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Authorization", f"Bearer {token}")
        req.add_header("Accept", "application/vnd.github+json")
        req.add_header("X-GitHub-Api-Version", "2022-11-28")
        if data is not None:
            req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, timeout=30) as response:
            payload = response.read()
        return json.loads(payload) if payload else None

    return request


def find_open_alerts(request, marker: str = MARKER) -> list:
    """Every open alert issue for this scope, newest last.

    Matches on the marker in the body. A human may edit or retitle the
    issue; they are unlikely to delete an HTML comment they cannot see.

    A list rather than one issue, because two runs failing seconds apart
    have each opened one before (#95 and #96 on 2026-10-18) and the next
    recovery closed only one, leaving a red light over a working
    pipeline. Recovery now closes all of them.
    """
    issues = request("GET", f"/issues?state=open&labels={LABEL}&per_page=100") or []
    return [issue for issue in issues if marker in (issue.get("body") or "")]


def find_open_alert(request, marker: str = MARKER) -> dict | None:
    """The oldest open alert for this scope, or None."""
    found = find_open_alerts(request, marker)
    return found[0] if found else None


def failure_body(workflow: str, branch: str, run_url: str,
                 marker: str = MARKER, consequence: str = "") -> str:
    return (
        f"{marker}\n"
        f"`{workflow}` is failing on `{branch}`.\n\n"
        f"{consequence or DEFAULT_CONSEQUENCE}\n\n"
        f"First failing run: {run_url}\n\n"
        f"This issue closes itself when a run on `{branch}` succeeds. "
        f"Later failures are added as comments rather than new issues."
    )


DEFAULT_CONSEQUENCE = (
    "Nothing publishes while this is red: the deploy job runs behind the "
    "test job, so a failing test means the site keeps serving its last "
    "good build and fresh ratings never reach it."
)


def alert(request, state: str, workflow: str, branch: str, run_url: str,
          scope: str = DEFAULT_SCOPE, consequence: str = "") -> str:
    marker = marker_for(scope)
    open_alerts = find_open_alerts(request, marker)
    existing = open_alerts[0] if open_alerts else None

    if state == "failed":
        if existing:
            request(
                "POST",
                f"/issues/{existing['number']}/comments",
                {"body": f"Still failing on `{branch}`: {run_url}"},
            )
            return f"commented on #{existing['number']}"
        created = request(
            "POST",
            "/issues",
            {
                "title": f"{workflow} is failing on {branch}",
                "body": failure_body(workflow, branch, run_url, marker,
                                     consequence),
                "labels": [LABEL],
            },
        )
        return f"opened #{created['number']}"

    if not open_alerts:
        return "nothing to close; the pipeline was not flagged as failing"
    for issue in open_alerts:
        request(
            "POST",
            f"/issues/{issue['number']}/comments",
            {"body": f"Recovered on `{branch}`: {run_url}"},
        )
        request("PATCH", f"/issues/{issue['number']}",
                {"state": "closed", "state_reason": "completed"})
    return "closed " + ", ".join(f"#{issue['number']}" for issue in open_alerts)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--state", choices=["failed", "recovered"], required=True)
    p.add_argument("--repo", default=os.getenv("GITHUB_REPOSITORY", ""))
    p.add_argument("--workflow", default=os.getenv("GITHUB_WORKFLOW", "workflow"))
    p.add_argument("--branch", default="main")
    p.add_argument("--run-url", default="")
    p.add_argument("--scope", default=DEFAULT_SCOPE,
                   help="which pipeline this light is for; football's deploy "
                        "and the basketball refresh keep separate lights")
    p.add_argument("--consequence", default="",
                   help="one line on what stops while this is red")
    args = p.parse_args(argv)

    token = os.getenv("GITHUB_TOKEN", "")
    if not args.repo or not token:
        # Never fail a run over its own alerting: a missing token means the
        # alert is lost, not that the build is broken.
        print("::warning::GITHUB_REPOSITORY or GITHUB_TOKEN missing; "
              "no pipeline alert was sent.")
        return 0

    try:
        print(alert(http_request(args.repo, token), args.state,
                    args.workflow, args.branch, args.run_url,
                    args.scope, args.consequence))
    except (urllib.error.URLError, urllib.error.HTTPError, KeyError, ValueError) as exc:
        print(f"::warning::Could not update the pipeline alert issue: {exc}")
        return 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
