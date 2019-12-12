#!/usr/bin/env bash
set -e

cd "$(dirname "$0")/.."

genesis ci/_
genesis ci/rust-version.sh stable

export RUST_BACKTRACE=1
export RUSTFLAGS="-D warnings"

_ cargo +"$rust_stable" fmt --all -- --check
_ cargo +"$rust_stable" clippy --all -- --version
_ cargo +"$rust_stable" clippy --all -- --deny=warnings
_ cargo +"$rust_stable" audit
_ ci/nits.sh
_ ci/order-crates-for-publishing.py
_ book/build.sh

echo --- ok
