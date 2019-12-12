#!/usr/bin/env bash
#
# Convenience script to easily deploy a software update to one of the testnets
#
# Prerequisites:
# 1) The default keypair should have some difs (eg, `morgan-wallet airdrop 123`)
# 2) The file update_manifest_keypair.json should exist if this script is not
#    run from the CI environment
#
set -e

CHANNEL=$1
TAG=$2

if [[ -z $CHANNEL || -z $TAG ]]; then
  echo "Usage: $0 [channel] [release tag]"
  exit 0
fi

# Prefer possible `cargo build --all` binaries over PATH binaries
PATH=$(cd "$(dirname "$0")/.."; echo "$PWD")/target/debug:$PATH

# shellcheck disable=2154 # is referenced but not assigned
if [[ -n $SOLANA_INSTALL_UPDATE_MANIFEST_KEYPAIR_x86_64_unknown_linux_gnu ]]; then
  echo "$SOLANA_INSTALL_UPDATE_MANIFEST_KEYPAIR_x86_64_unknown_linux_gnu" > update_manifest_keypair.json
fi

case $CHANNEL in
edge|beta)
  URL=https://api.$CHANNEL.testnet.morgan.com
  ;;
stable)
  URL=https://api.testnet.morgan.com
  ;;
localhost)
  URL=http://localhost:10099
  ;;
*)
  echo "Error: unknown channel: $CHANNEL"
  exit 1
esac

set -x
morgan-install deploy --url "$URL" \
  https://github.com/morgan-labs/morgan/releases/download/"$TAG"/morgan-release-x86_64-unknown-linux-gnu.tar.bz2 \
  update_manifest_keypair.json
