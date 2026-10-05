#!/usr/bin/env bash
# Publish files to main from a workflow, surviving a push that lands while the
# job runs.
#
# Usage: scripts/publish_to_main.sh "<commit message>" <path>...
#
# Why this exists. Every workflow here used to end with
#
#     git commit -m "..." && git pull --rebase origin main && git push origin HEAD:main
#
# and a rebase is the wrong tool for what these jobs commit. The deploy rewrites
# generated files WHOLE -- ui/dashboard.html, ui/data/*.js, static_manifest.json,
# data/processed/* -- so replaying our commit on top of somebody else's rewrite
# of the same files conflicts in every one of them, with no hunk a merge driver
# could reconcile. Run 37172187197 died exactly that way: 20-odd CONFLICT lines
# and nothing published.
#
# A merge that keeps OUR side of any overlap cannot conflict, and it is the
# right answer for this repository: our side is output we just regenerated, and
# their side of a file we did not touch is taken unchanged, so unrelated work
# pushed mid-run is kept rather than clobbered.
#
# What a merge does NOT do is rebuild. If the commit we merge in carries new
# INPUT (a roster snapshot from sync-people.yml, say), our generated files were
# built before it arrived and will not show it until the next deploy runs. That
# is automatic: completion of either source workflow queues ci-and-deploy
# through workflow_run, which rebuilds from the latest main after this run.
#
# Every git command is checked on its own line. A `&&` chain would run the push
# even after a failed merge and publish a conflicted tree.
set -uo pipefail

if [ "$#" -lt 2 ]; then
  echo "::error::usage: publish_to_main.sh \"<commit message>\" <path>..." >&2
  exit 2
fi

message=$1
shift

branch=${PUBLISH_BRANCH:-main}
attempts=${PUBLISH_ATTEMPTS:-5}

if [ -z "$(git config user.name || true)" ]; then
  git config user.name "github-actions[bot]"
  git config user.email "github-actions[bot]@users.noreply.github.com"
fi

if ! git add -- "$@"; then
  echo "::error::Could not stage $*" >&2
  exit 1
fi

if git diff --staged --quiet; then
  echo "Nothing changed; no commit."
  exit 0
fi

if ! git commit -m "$message"; then
  echo "::error::Could not commit $*" >&2
  exit 1
fi

attempt=1
while [ "$attempt" -le "$attempts" ]; do
  if git push origin "HEAD:$branch"; then
    echo "Published to $branch on attempt $attempt."
    exit 0
  fi

  echo "Push rejected. Checking whether $branch moved while this job ran (attempt $attempt of $attempts)."
  if ! git fetch origin "$branch"; then
    echo "::error::Could not fetch origin/$branch to reconcile. Nothing was published." >&2
    exit 1
  fi

  # A rejected push with an unmoved branch is not a race: permissions, a
  # protected branch, a network fault. Retrying would only hide the reason.
  if git merge-base --is-ancestor FETCH_HEAD HEAD; then
    echo "::error::Push to $branch was rejected although $branch has not moved. Nothing was published." >&2
    exit 1
  fi

  if ! git merge -X ours --no-edit FETCH_HEAD; then
    git merge --abort || true
    echo "::error::Could not reconcile with origin/$branch. Nothing was published." >&2
    exit 1
  fi
  echo "Merged the newer $branch; our regenerated files kept where both sides changed one."

  attempt=$((attempt + 1))
done

echo "::error::$branch moved under $attempts consecutive push attempts. Nothing was published." >&2
exit 1
