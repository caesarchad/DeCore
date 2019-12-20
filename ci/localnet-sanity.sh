#!/usr/bin/env bash
set -e

skipSetup=false
iterations=1
restartInterval=never
rollingRestart=false
maybeNoLeaderRotation=
extraNodes=0
walletRpcPort=:10099

usage() {
  exitcode=0
  if [[ -n "$1" ]]; then
    exitcode=1
    echo "Error: $*"
  fi
  cat <<EOF
usage: $0 [options...]

Start a local cluster and run sanity on it

  options:
   -i [number] - Number of times to run sanity (default: $iterations)
   -k [number] - Restart the cluster after this number of sanity iterations (default: $restartInterval)
   -R          - Restart the cluster by incrementially stopping and restarting
                 nodes (at the cadence specified by -k).  When disabled all
                 nodes will be first killed then restarted (default: $rollingRestart)
   -b          - Disable leader rotation
   -x          - Add an extra validator (may be supplied multiple times)
   -r          - Select the RPC endpoint hosted by a node that starts as
                 a validator node.  If unspecified the RPC endpoint hosted by
                 the bootstrap leader will be used.
   -c          - Reuse existing node/ledger configuration from a previous sanity
                 run

EOF
  exit $exitcode
}

cd "$(dirname "$0")"/..

while getopts "ch?i:k:brxR" opt; do
  case $opt in
  h | \?)
    usage
    ;;
  c)
    skipSetup=true
    ;;
  i)
    iterations=$OPTARG
    ;;
  k)
    restartInterval=$OPTARG
    ;;
  b)
    maybeNoLeaderRotation="--stake 0"
    ;;
  x)
    extraNodes=$((extraNodes + 1))
    ;;
  r)
    walletRpcPort=":110099"
    ;;
  R)
    rollingRestart=true
    ;;
  *)
    usage "Error: unhandled option: $opt"
    ;;
  esac
done

genesis ci/upload-ci-artifact.sh
genesis scripts/configure-metrics.sh

nodes=(
  "multinode-demo/drone.sh"
  "multinode-demo/bootstrap-leader.sh \
    --enable-rpc-exit \
    --no-restart \
    --init-complete-file init-complete-node1.log"
  "multinode-demo/validator.sh \
    $maybeNoLeaderRotation \
    --enable-rpc-exit \
    --no-restart \
    --init-complete-file init-complete-node2.log \
    --rpc-port 110099"
)

for i in $(seq 1 $extraNodes); do
  nodes+=(
    "multinode-demo/validator.sh \
      --no-restart \
      --label dyn$i \
      --init-complete-file init-complete-node$((2 + i)).log \
      $maybeNoLeaderRotation"
  )
done
numNodes=$((2 + extraNodes))

pids=()
logs=()

getNodeLogFile() {
  declare nodeIndex=$1
  declare cmd=$2
  declare baseCmd
  baseCmd=$(basename "${cmd// */}" .sh)
  echo "log-$baseCmd-$nodeIndex.txt"
}

startNode() {
  declare nodeIndex=$1
  declare cmd=$2
  echo "--- Start $cmd"
  declare log
  log=$(getNodeLogFile "$nodeIndex" "$cmd")
  rm -f "$log"
  $cmd > "$log" 2>&1 &
  declare pid=$!
  pids+=("$pid")
  echo "pid: $pid"
  echo "log: $log"
}

initCompleteFiles=()
waitForAllNodesToInit() {
  echo "--- ${#initCompleteFiles[@]} nodes booting"
  SECONDS=
  for initCompleteFile in "${initCompleteFiles[@]}"; do
    while [[ ! -r $initCompleteFile ]]; do
      if [[ $SECONDS -ge 240 ]]; then
        echo "^^^ +++"
        echo "Error: $initCompleteFile not found in $SECONDS seconds"
        exit 1
      fi
      echo "Waiting for $initCompleteFile ($SECONDS)..."
      sleep 2
    done
    echo "Found $initCompleteFile"
  done
  echo "All nodes finished booting in $SECONDS seconds"
}

startNodes() {
  declare addLogs=false
  if [[ ${#logs[@]} -eq 0 ]]; then
    addLogs=true
  fi
  initCompleteFiles=()
  for i in $(seq 0 $((${#nodes[@]} - 1))); do
    declare cmd=${nodes[$i]}

    if [[ "$i" -ne 0 ]]; then # 0 == drone, skip it
      declare initCompleteFile="init-complete-node$i.log"
      rm -f "$initCompleteFile"
      initCompleteFiles+=("$initCompleteFile")
    fi
    startNode "$i" "$cmd"
    if $addLogs; then
      logs+=("$(getNodeLogFile "$i" "$cmd")")
    fi
  done

  waitForAllNodesToInit
}

killNode() {
  declare pid=$1
  set +e
  if kill "$pid"; then
    echo "Waiting for $pid to exit..."
    wait "$pid"
    echo "$pid exited with $?"
  fi
  set -e
}

killNodes() {
  [[ ${#pids[@]} -gt 0 ]] || return

  # Try to use the RPC exit API to cleanly exit the first two nodes
  # (dynamic nodes, -x, are just killed since their RPC port is not known)
  echo "--- RPC exit"
  for port in 10099 110099; do
    (
      set -x
      curl --retry 5 --retry-delay 2 --retry-connrefused \
        -X POST -H 'Content-Type: application/json' \
        -d '{"jsonrpc":"2.0","id":1, "method":"fullnodeExit"}' \
        http://localhost:$port
    )
  done

  # Give the nodes a splash of time to cleanly exit before killing them
  sleep 2

  echo "--- Killing nodes: ${pids[*]}"
  for pid in "${pids[@]}"; do
    killNode "$pid"
  done
  echo "done killing nodes"
  pids=()
}

rollingNodeRestart() {
  if [[ ${#logs[@]} -ne ${#nodes[@]} ]]; then
    echo "^^^ +++"
    echo "Error: log/nodes array length mismatch"
    exit 1
  fi
  if [[ ${#pids[@]} -ne ${#nodes[@]} ]]; then
    echo "^^^ +++"
    echo "Error: pids/nodes array length mismatch"
    exit 1
  fi

  declare oldPids=("${pids[@]}")
  for i in $(seq 0 $((${#logs[@]} - 1))); do
    declare pid=${oldPids[$i]}
    declare cmd=${nodes[$i]}
    if [[ $i -eq 0 ]]; then
      # First cmd should be the drone, don't restart it.
      [[ "$cmd" = "multinode-demo/drone.sh" ]]
      pids+=("$pid")
    else
      echo "--- Restarting $pid: $cmd"
      killNode "$pid"
      # Delay 20 seconds to ensure the remaining cluster nodes will
      # hit CRDS_GOSSIP_PULL_CRDS_TIMEOUT_MS (currently 15 seconds) for the
      # node that was just stopped
      echo "(sleeping for 20 seconds)"
      sleep 20

      declare initCompleteFile="init-complete-node$i.log"
      rm -f "$initCompleteFile"
      initCompleteFiles+=("$initCompleteFile")
      startNode "$i" "$cmd"
    fi
  done

  # 'Atomically' remove the old pids from the pids array
  declare oldPidsList
  oldPidsList="$(printf ":%s" "${oldPids[@]}"):"
  declare newPids=("${pids[0]}") # 0 = drone pid
  for pid in "${pids[@]}"; do
    [[ $oldPidsList =~ :$pid: ]] || {
      newPids+=("$pid")
    }
  done
  pids=("${newPids[@]}")

  waitForAllNodesToInit
}

verifyLedger() {
  for ledger in bootstrap-leader validator; do
    echo "--- $ledger ledger verification"
    (
      genesis multinode-demo/common.sh
      set -x
      $morgan_ledgerbot --ledger "$SOLANA_CONFIG_DIR"/$ledger-ledger verify
    ) || flag_error
  done
}

shutdown() {
  exitcode=$?
  killNodes

  set +e

  echo "--- Upload artifacts"
  for log in "${logs[@]}"; do
    upload-ci-artifact "$log"
    tail "$log"
  done

  exit $exitcode
}

trap shutdown EXIT INT

set -e

declare iteration=1

flag_error() {
  echo "Failed (iteration: $iteration/$iterations)"
  echo "^^^ +++"
  exit 1
}

if ! $skipSetup; then
  multinode-demo/setup.sh --hashes-per-_drop auto
else
  verifyLedger
fi
startNodes
lastTransactionCount=
enforceTransactionCountAdvance=true
while [[ $iteration -le $iterations ]]; do
  echo "--- Node count ($iteration)"
  (
    genesis multinode-demo/common.sh
    set -x
    client_keypair=/tmp/client-id.json-$$
    $morgan_keybot -o $client_keypair || exit $?
    $morgan_gossip spy --num-nodes-exactly $numNodes || exit $?
    rm -rf $client_keypair
  ) || flag_error

  echo "--- RPC API: bootstrap-leader getTransactionCount ($iteration)"
  (
    set -x
    curl --retry 5 --retry-delay 2 --retry-connrefused \
      -X POST -H 'Content-Type: application/json' \
      -d '{"jsonrpc":"2.0","id":1, "method":"getTransactionCount"}' \
      -o log-transactionCount.txt \
      http://localhost:10099
    cat log-transactionCount.txt
  ) || flag_error

  echo "--- RPC API: validator getTransactionCount ($iteration)"
  (
    set -x
    curl --retry 5 --retry-delay 2 --retry-connrefused \
      -X POST -H 'Content-Type: application/json' \
      -d '{"jsonrpc":"2.0","id":1, "method":"getTransactionCount"}' \
      http://localhost:110099
  ) || flag_error

  # Verify transaction count as reported by the bootstrap-leader node is advancing
  transactionCount=$(sed -e 's/{"jsonrpc":"2.0","result":\([0-9]*\),"id":1}/\1/' log-transactionCount.txt)
  if [[ -n $lastTransactionCount ]]; then
    echo "--- Transaction count check: $lastTransactionCount < $transactionCount"
    if $enforceTransactionCountAdvance; then
      if [[ $lastTransactionCount -ge $transactionCount ]]; then
        echo "Error: Transaction count is not advancing"
        echo "* lastTransactionCount: $lastTransactionCount"
        echo "* transactionCount: $transactionCount"
        flag_error
      fi
    else
      echo "enforceTransactionCountAdvance=false"
    fi
    enforceTransactionCountAdvance=true
  fi
  lastTransactionCount=$transactionCount

  echo "--- Wallet sanity ($iteration)"
  flag_error_if_no_leader_rotation() {
    # TODO: Stop ignoring wallet sanity failures when leader rotation is enabled
    #       once https://github.com/morgan-labs/morgan/issues/2474 is fixed
    if [[ -n $maybeNoLeaderRotation ]]; then
      flag_error
    else
      # Wallet error occurred (and was ignored) so transactionCount may not
      # advance on the next iteration
      enforceTransactionCountAdvance=false
    fi
  }
  (
    set -x
    timeout 60s scripts/wallet-sanity.sh --url http://127.0.0.1"$walletRpcPort"
  ) || flag_error_if_no_leader_rotation

  iteration=$((iteration + 1))

  if [[ $restartInterval != never && $((iteration % restartInterval)) -eq 0 ]]; then
    if $rollingRestart; then
      rollingNodeRestart
    else
      killNodes
      verifyLedger
      startNodes
    fi
  fi
done

killNodes
verifyLedger

echo +++
echo "Ok ($iterations iterations)"

exit 0
