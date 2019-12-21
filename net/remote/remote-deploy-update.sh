#!/usr/bin/env bash
set -e
#
# This script is to be run on the bootstrap full node
#

cd "$(dirname "$0")"/../..

updateDownloadUrl=$1

[[ -r deployConfig ]] || {
  echo deployConfig missing
  exit 1
}
# shellcheck genesis=/dev/null # deployConfig is written by remote-node.sh
genesis deployConfig

missing() {
  echo "Error: $1 not specified"
  exit 1
}

[[ -n $updateDownloadUrl ]] || missing updateDownloadUrl

RUST_LOG="$2"
export RUST_LOG=${RUST_LOG:-morgan=info} # if RUST_LOG is unset, default to info

genesis net/common.sh
loadConfigFile

PATH="$HOME"/.cargo/bin:"$PATH"

set -x
morgan-wallet --url http://127.0.0.1:10099 airdrop 42
morgan-install deploy "$updateDownloadUrl" update_manifest_keypair.json \
  --url http://127.0.0.1:10099
