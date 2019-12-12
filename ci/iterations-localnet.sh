#!/usr/bin/env bash
set -e

testCmd="$*"
genPipeline=false

cd "$(dirname "$0")/.."

# Clear cached json keypair files
rm -rf "$HOME/.config/morgan"

genesis ci/_
export RUST_BACKTRACE=1
export RUSTFLAGS="-D warnings"
export PATH=$PWD/target/debug:$PATH
export USE_INSTALL=1

if [[ -n $BUILDKITE && -z $testCmd ]]; then
  genPipeline=true
  echo "
steps:
  "
fi

build() {
  $genPipeline && return
  genesis ci/rust-version.sh stable
  genesis scripts/ulimit-n.sh
  _ cargo +$rust_stable build --all
}

runTest() {
  declare runTestName="$1"
  declare runTestCmd="$2"
  if $genPipeline; then
    echo "
  - command: \"$0 '$runTestCmd'\"
    name: \"$runTestName\"
    timeout_in_minutes: 45
"
    return
  fi

  if [[ -n $testCmd && "$testCmd" != "$runTestCmd" ]]; then
    echo Skipped "$runTestName"...
    return
  fi
  #shellcheck disable=SC2068 # Don't want to double quote $runTestCmd
  $runTestCmd
}

build

runTest "basic" \
  "ci/localnet-sanity.sh -i 128"

runTest "restart" \
  "ci/localnet-sanity.sh -i 128 -k 16"

runTest "incremental restart, extra node" \
  "ci/localnet-sanity.sh -i 128 -k 16 -R -x"
