"""Drive scripts/publish_to_main.sh against real git repositories.

The defect this guards is a publish that dies when main moves while a job
builds: the deploy rewrites generated files whole, so `git pull --rebase`
replays our rewrite on top of theirs and conflicts in every one of them.
These tests build an origin and two clones and make that race happen, rather
than asserting anything about the text of the workflow files.
"""

import os
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "publish_to_main.sh"

ENV = {
    **os.environ,
    "GIT_AUTHOR_NAME": "Test",
    "GIT_AUTHOR_EMAIL": "test@example.invalid",
    "GIT_COMMITTER_NAME": "Test",
    "GIT_COMMITTER_EMAIL": "test@example.invalid",
    "GIT_CONFIG_GLOBAL": "/dev/null",
    "GIT_CONFIG_SYSTEM": "/dev/null",
}


def git(repo, *args, check=True):
    return subprocess.run(
        ["git", *args], cwd=repo, env=ENV, check=check,
        capture_output=True, text=True,
    )


def clone(origin, path):
    subprocess.run(
        ["git", "clone", "--quiet", str(origin), str(path)],
        env=ENV, check=True, capture_output=True, text=True,
    )
    git(path, "config", "user.name", "Test")
    git(path, "config", "user.email", "test@example.invalid")
    return path


def publish(repo, message, *paths):
    return subprocess.run(
        [str(SCRIPT), message, *paths], cwd=repo, env=ENV,
        capture_output=True, text=True,
    )


@pytest.fixture
def origin(tmp_path):
    """A bare origin whose main already carries a generated file."""
    bare = tmp_path / "origin.git"
    subprocess.run(
        ["git", "init", "--quiet", "--bare", "-b", "main", str(bare)],
        env=ENV, check=True, capture_output=True, text=True,
    )
    seed = clone(origin=bare, path=tmp_path / "seed")
    (seed / "generated.html").write_text("base\n")
    (seed / "source.py").write_text("base\n")
    git(seed, "add", ".")
    git(seed, "commit", "-m", "seed")
    git(seed, "push", "origin", "main")
    return bare


def head_tree(bare, path):
    return git(bare, "show", f"main:{path}").stdout


def test_publishes_a_changed_file(origin, tmp_path):
    work = clone(origin, tmp_path / "work")
    (work / "generated.html").write_text("ours\n")

    result = publish(work, "rebuild [skip ci]", "generated.html")

    assert result.returncode == 0, result.stderr
    assert head_tree(origin, "generated.html") == "ours\n"
    assert "rebuild [skip ci]" in git(origin, "log", "-1", "--format=%s").stdout


def test_makes_no_commit_when_nothing_changed(origin, tmp_path):
    work = clone(origin, tmp_path / "work")
    before = git(work, "rev-parse", "HEAD").stdout

    result = publish(work, "rebuild [skip ci]", "generated.html")

    assert result.returncode == 0, result.stderr
    assert git(work, "rev-parse", "HEAD").stdout == before
    assert "Nothing changed" in result.stdout


def test_keeps_an_unrelated_push_that_landed_mid_build(origin, tmp_path):
    work = clone(origin, tmp_path / "work")
    other = clone(origin, tmp_path / "other")

    # Somebody else pushes a file this job never touched.
    (other / "source.py").write_text("theirs\n")
    git(other, "add", "source.py")
    git(other, "commit", "-m", "source change")
    git(other, "push", "origin", "main")

    (work / "generated.html").write_text("ours\n")
    result = publish(work, "rebuild [skip ci]", "generated.html")

    assert result.returncode == 0, result.stderr
    assert head_tree(origin, "generated.html") == "ours\n"
    assert head_tree(origin, "source.py") == "theirs\n"


def test_keeps_our_rebuild_when_the_same_generated_file_was_rewritten(origin, tmp_path):
    """The failure that killed run 37172187197, as a test.

    Both sides rewrote the whole generated file. A rebase conflicts here; the
    publish must land, keeping this run's output and their unrelated work.
    """
    work = clone(origin, tmp_path / "work")
    other = clone(origin, tmp_path / "other")

    (other / "generated.html").write_text("theirs\n")
    (other / "source.py").write_text("theirs\n")
    git(other, "add", ".")
    git(other, "commit", "-m", "Scheduled data refresh [skip ci]")
    git(other, "push", "origin", "main")

    (work / "generated.html").write_text("ours\n")
    result = publish(work, "rebuild [skip ci]", "generated.html")

    assert result.returncode == 0, result.stderr
    published = head_tree(origin, "generated.html")
    assert published == "ours\n"
    assert "<<<<<<<" not in published
    # Their unrelated change survives the reconciliation.
    assert head_tree(origin, "source.py") == "theirs\n"


def test_fails_loudly_when_the_push_is_rejected_without_a_race(origin, tmp_path):
    """A rejected push with an unmoved main is not a race, so do not retry it.

    The remote here is reachable and fetches fine -- it just refuses the push,
    the way a protected branch or a token without write access does. Retrying
    that would hide the reason behind five identical rejections.
    """
    work = clone(origin, tmp_path / "work")
    hook = origin / "hooks" / "pre-receive"
    hook.write_text("#!/bin/sh\necho 'refusing' >&2\nexit 1\n")
    hook.chmod(0o755)
    (work / "generated.html").write_text("ours\n")

    result = publish(work, "rebuild [skip ci]", "generated.html")

    assert result.returncode == 1
    assert "has not moved" in result.stderr
    # One attempt, not a retry loop against an error that will not change.
    assert result.stdout.count("attempt 1 of") == 1
    assert "attempt 2 of" not in result.stdout


def test_fails_loudly_when_the_remote_is_unreachable(origin, tmp_path):
    work = clone(origin, tmp_path / "work")
    (work / "generated.html").write_text("ours\n")
    git(work, "remote", "set-url", "origin", str(tmp_path / "missing.git"))

    result = publish(work, "rebuild [skip ci]", "generated.html")

    assert result.returncode == 1
    assert "Nothing was published" in result.stderr


def test_rejects_a_call_with_no_paths(origin, tmp_path):
    work = clone(origin, tmp_path / "work")
    result = subprocess.run(
        [str(SCRIPT), "rebuild"], cwd=work, env=ENV,
        capture_output=True, text=True,
    )
    assert result.returncode == 2


def test_reconciles_from_a_shallow_clone(origin, tmp_path):
    """CI clones with depth 1, which has no ancestor to merge against."""
    work = tmp_path / "shallow"
    subprocess.run(
        ["git", "clone", "--quiet", "--depth", "1", f"file://{origin}", str(work)],
        env=ENV, check=True, capture_output=True, text=True,
    )
    git(work, "config", "user.name", "Test")
    git(work, "config", "user.email", "test@example.invalid")
    assert git(work, "rev-parse", "--is-shallow-repository").stdout.strip() == "true"

    other = clone(origin, tmp_path / "other")
    (other / "generated.html").write_text("theirs\n")
    git(other, "add", "generated.html")
    git(other, "commit", "-m", "Scheduled data refresh [skip ci]")
    git(other, "push", "origin", "main")

    (work / "generated.html").write_text("ours\n")
    result = publish(work, "rebuild [skip ci]", "generated.html")

    assert result.returncode == 0, result.stderr + result.stdout
    assert head_tree(origin, "generated.html") == "ours\n"

