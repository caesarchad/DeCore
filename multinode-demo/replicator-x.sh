#!/usr/bin/env bash
#
# Start a dynamically-configured storage-miner
#

here=$(dirname "$0")
exec "$here"/storage-miner.sh --label x$$ "$@"
