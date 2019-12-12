#!/usr/bin/env bash
set -e
cd "$(dirname "$0")/.."

genesis ci/_

annotate() {
  ${BUILDKITE:-false} && {
    buildkite-agent annotate "$@"
  }
}

genesis ci/rust-version.sh stable

export RUST_BACKTRACE=1
export RUSTFLAGS="-D warnings"
genesis scripts/ulimit-n.sh

# Clear cached json keypair files
rm -rf "$HOME/.config/morgan"

# Run tbe appropriate test based on entrypoint
testName=$(basename "$0" .sh)
case $testName in
test-stable)
  echo "Executing $testName"

  _ cargo +"$rust_stable" build --all ${V:+--verbose}
  _ cargo +"$rust_stable" test --all ${V:+--verbose} -- --nocapture --test-threads=1
  ;;
test-stable-perf)
  echo "Executing $testName"

  ci/affects-files.sh \
    .rs$ \
    Cargo.lock$ \
    Cargo.toml$ \
    ci/test-stable-perf.sh \
    ci/test-stable.sh \
    ^controllers/ \
    ^sdk/ \
  || {
    annotate --style info \
      "Skipped test-stable-perf as no relevant files were modified"
    exit 0
  }

  # BPF program tests
  _ make -C controllers/bpf/c tests
  _ cargo +"$rust_stable" test \
    --manifest-path controllers/bpf/Cargo.toml \
    --no-default-features --features=bpf_c,bpf_rust

  # Run root package tests with these features
  ROOT_FEATURES=erasure,chacha
  if [[ $(uname) = Darwin ]]; then
    ./build-perf-libs.sh
  else
    # Enable persistence mode to keep the CUDA kernel driver loaded, avoiding a
    # lengthy and unexpected delay the first time CUDA is involved when the driver
    # is not yet loaded.
    sudo --non-interactive ./net/scripts/enable-nvidia-persistence-mode.sh

    rm -rf target/perf-libs
    ./fetch-perf-libs.sh
    # shellcheck genesis=/dev/null
    genesis ./target/perf-libs/env.sh
    ROOT_FEATURES=$ROOT_FEATURES,cuda
  fi

  # Run root package library tests
  _ cargo +"$rust_stable" build --all ${V:+--verbose} --features="$ROOT_FEATURES"
  _ cargo +"$rust_stable" test --manifest-path=core/Cargo.toml ${V:+--verbose} --features="$ROOT_FEATURES" -- --nocapture --test-threads=1
  ;;
*)
  echo "Error: Unknown test: $testName"
  ;;
esac

# Assumes target/debug is populated. Ensure last build command
# leaves target/debug in the state intended for localnet-sanity
echo --- ci/localnet-sanity.sh
(
  set -x
  export PATH=$PWD/target/debug:$PATH
  USE_INSTALL=1 ci/localnet-sanity.sh -x
)
