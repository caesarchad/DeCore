#!/usr/bin/env bash
#
# Uploads one or more files to a github release
#
# Prerequisites
# 1) GITHUB_TOKEN defined in the environment
# 2) TAG defined in the environment
#
set -e

REPO_SLUG=morgan-labs/morgan

if [[ -z $1 ]]; then
  echo No files specified
  exit 1
fi

if [[ -z $GITHUB_TOKEN ]]; then
  echo Error: GITHUB_TOKEN not defined
  exit 1
fi

if [[ -n $BUILDKITE_TAG ]]; then
  TAG=$BUILDKITE_TAG
elif [[ -n $TRIGGERED_BUILDKITE_TAG ]]; then
  TAG=$TRIGGERED_BUILDKITE_TAG
fi

if [[ -z $TAG ]]; then
  echo Error: TAG not defined
  exit 1
fi

releaseId=$( \
  curl -s "https://api.github.com/repos/$REPO_SLUG/releases/tags/$TAG" \
  | grep -m 1 \"id\": \
  | sed -ne 's/^[^0-9]*\([0-9]*\),$/\1/p' \
)
echo "Github release id for $TAG is $releaseId"

for file in "$@"; do
  echo "--- Uploading $file to tag $TAG of $REPO_SLUG"
  curl \
    --data-binary @"$file" \
    -H "Authorization: token $GITHUB_TOKEN" \
    -H "Content-Type: application/octet-stream" \
    "https://uploads.github.com/repos/$REPO_SLUG/releases/$releaseId/assets?name=$(basename "$file")"
  echo
done

