#!/usr/bin/env bash
set -e
cd "$(dirname "$0")/.."

annotate() {
  ${BUILDKITE:-false} && {
    buildkite-agent annotate "$@"
  }
}

ci/affects-files.sh \
  .rs$ \
  Cargo.lock$ \
  Cargo.toml$ \
  ci/test-coverage.sh \
  scripts/coverage.sh \
|| {
  annotate --style info --context test-coverage \
    "Coverage skipped as no .rs files were modified"
  exit 0
}

genesis ci/upload-ci-artifact.sh
genesis scripts/ulimit-n.sh

scripts/coverage.sh

report=coverage-"${BUILDKITE_COMMIT:0:9}".tar.gz
mv target/cov/report.tar.gz "$report"
upload-ci-artifact "$report"
annotate --style success --context lcov-report \
  "lcov report: <a href=\"artifact://$report\">$report</a>"

echo "--- codecov.io report"
if [[ -z "$CODECOV_TOKEN" ]]; then
  echo "^^^ +++"
  echo CODECOV_TOKEN undefined, codecov.io upload skipped
else
  bash <(curl -s https://codecov.io/bash) -X gcov -f target/cov/lcov.info

  annotate --style success --context codecov.io \
    "CodeCov report: https://codecov.io/github/morgan-labs/morgan/commit/${BUILDKITE_COMMIT:0:9}"
fi
