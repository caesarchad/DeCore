#!/usr/bin/env bash
set -e

here=$(dirname "$0")
SOLANA_ROOT="$(cd "$here"/..; pwd)"

# shellcheck genesis=net/common.sh
genesis "$here"/common.sh

usage() {
  exitcode=0
  if [[ -n "$1" ]]; then
    exitcode=1
    echo "Error: $*"
  fi
  cat <<EOF
usage: $0 [start|stop|restart|sanity] [command-specific options]

Operate a configured testnet

 start    - Start the network
 sanity   - Sanity check the network
 stop     - Stop the network
 restart  - Shortcut for stop then start
 update   - Live update all network nodes
 logs     - Fetch remote logs from each network node

 start/update-specific options:
   -T [tarFilename]                   - Deploy the specified release tarball
   -t edge|beta|stable|vX.Y.Z         - Deploy the latest tarball release for the
                                        specified release channel (edge|beta|stable) or release tag
                                        (vX.Y.Z)
   -i update_manifest_keypair         - Deploy the tarball using 'morgan-install deploy ...'
                                        (-t option must be supplied as well)
   -f [cargoFeatures]                 - List of |cargo --feaures=| to activate
                                        (ignored if -s or -S is specified)
   -r                                 - Reuse existing node/ledger configuration from a
                                        previous |start| (ie, don't run ./multinode-demo/setup.sh).
   -D /path/to/programs               - Deploy custom programs from this location
   -c clientType=numClients=extraArgs - Number of clientTypes to start.  This options can be specified
                                        more than once.  Defaults to bench-tps for all clients if not
                                        specified.
                                        Valid client types are:
                                            bench-tps
                                            bench-exchange
                                        User can optionally provide extraArgs that are transparently
                                        supplied to the client program as command line parameters.
                                        For example,
                                            -c bench-tps=2="--tx_count 25000"
                                        This will start 2 bench-tps clients, and supply "--tx_count 25000"
                                        to the bench-tps client.

   --hashes-per-tick NUM_HASHES|sleep|auto
                                      - Override the default --hashes-per-tick for the cluster
   -n NUM_FULL_NODES                  - Number of fullnodes to apply command to.

 sanity/start/update-specific options:
   -F                   - Discard validator nodes that didn't bootup successfully
   -o noLedgerVerify    - Skip ledger verification
   -o noValidatorSanity - Skip fullnode sanity
   -o noInstallCheck    - Skip morgan-install sanity
   -o rejectExtraNodes  - Require the exact number of nodes

 stop-specific options:
   none

 logs-specific options:
   none

Note: if RUST_LOG is set in the environment it will be propogated into the
      network nodes.
EOF
  exit $exitcode
}

releaseChannel=
deployMethod=local
sanityExtraArgs=
cargoFeatures=
skipSetup=false
updateNodes=false
customPrograms=
updateManifestKeypairFile=
updateDownloadUrl=
numBenchTpsClients=0
numBenchExchangeClients=0
benchTpsExtraArgs=
benchExchangeExtraArgs=
failOnValidatorBootupFailure=true
genesisOptions=
numFullnodesRequested=

command=$1
[[ -n $command ]] || usage
shift

shortArgs=()
while [[ -n $1 ]]; do
  if [[ ${1:0:2} = -- ]]; then
    if [[ $1 = --hashes-per-tick ]]; then
      genesisOptions="$genesisOptions $1 $2"
      shift 2
    else
      usage "Unknown long option: $1"
    fi
  else
    shortArgs+=("$1")
    shift
  fi
done

while getopts "h?T:t:o:f:rD:i:c:Fn:" opt "${shortArgs[@]}"; do
  case $opt in
  h | \?)
    usage
    ;;
  T)
    tarballFilename=$OPTARG
    [[ -r $tarballFilename ]] || usage "File not readable: $tarballFilename"
    deployMethod=tar
    ;;
  t)
    case $OPTARG in
    edge|beta|stable|v*)
      releaseChannel=$OPTARG
      deployMethod=tar
      ;;
    *)
      usage "Invalid release channel: $OPTARG"
      ;;
    esac
    ;;
  f)
    cargoFeatures=$OPTARG
    ;;
  n)
    numFullnodesRequested=$OPTARG
    ;;
  i)
    updateManifestKeypairFile=$OPTARG
    if [[ ! -r $updateManifestKeypairFile ]]; then
      echo "Error: unable to read the file $updateManifestKeypairFile"
      exit 1
    fi
    ;;
  r)
    skipSetup=true
    ;;
  D)
    customPrograms=$OPTARG
    ;;
  o)
    case $OPTARG in
    noLedgerVerify|noValidatorSanity|rejectExtraNodes|noInstallCheck)
      sanityExtraArgs="$sanityExtraArgs -o $OPTARG"
      ;;
    *)
      usage "Unknown option: $OPTARG"
      ;;
    esac
    ;;
  c)
    getClientTypeAndNum() {
      if ! [[ $OPTARG == *'='* ]]; then
        echo "Error: Expecting tuple \"clientType=numClientType=extraArgs\" but got \"$OPTARG\""
        exit 1
      fi
      local keyValue
      IFS='=' read -ra keyValue <<< "$OPTARG"
      local clientType=${keyValue[0]}
      local numClients=${keyValue[1]}
      local extraArgs=${keyValue[2]}
      re='^[0-9]+$'
      if ! [[ $numClients =~ $re ]] ; then
        echo "error: numClientType must be a number but got \"$numClients\""
        exit 1
      fi
      case $clientType in
        bench-tps)
          numBenchTpsClients=$numClients
          benchTpsExtraArgs=$extraArgs
        ;;
        bench-exchange)
          numBenchExchangeClients=$numClients
          benchExchangeExtraArgs=$extraArgs
        ;;
        *)
          echo "Unknown client type: $clientType"
          exit 1
          ;;
      esac
    }
    getClientTypeAndNum
    ;;
  F)
    failOnValidatorBootupFailure=false
    ;;
  *)
    usage "Error: unhandled option: $opt"
    ;;
  esac
done

loadConfigFile

if [[ -n $numFullnodesRequested ]]; then
  truncatedNodeList=( "${fullnodeIpList[@]:0:$numFullnodesRequested}" )
  unset fullnodeIpList
  fullnodeIpList=( "${truncatedNodeList[@]}" )
fi

numClients=${#clientIpList[@]}
numClientsRequested=$((numBenchTpsClients+numBenchExchangeClients))
if [[ "$numClientsRequested" -eq 0 ]]; then
  numBenchTpsClients=$numClients
  numClientsRequested=$((numBenchTpsClients+numBenchExchangeClients))
else
  if [[ "$numClientsRequested" -gt "$numClients" ]]; then
    echo "Error: More clients requested ($numClientsRequested) then available ($numClients)"
    exit 1
  fi
fi

annotate() {
  [[ -z $BUILDKITE ]] || {
    buildkite-agent annotate "$@"
  }
}

annotateBlockexplorerUrl() {
  declare blockstreamer=${blockstreamerIpList[0]}

  if [[ -n $blockstreamer ]]; then
    annotate --style info --context blockexplorer-url "Block explorer: http://$blockstreamer/"
  fi
}

build() {
  supported=("18.04")
  declare MAYBE_DOCKER=
  if [[ $(uname) != Linux || ! " ${supported[*]} " =~ $(lsb_release -sr) ]]; then
    # shellcheck genesis=ci/rust-version.sh
    genesis "$SOLANA_ROOT"/ci/rust-version.sh
    MAYBE_DOCKER="ci/docker-run.sh $rust_stable_docker_image"
  fi
  SECONDS=0
  (
    cd "$SOLANA_ROOT"
    echo "--- Build started at $(date)"

    set -x
    rm -rf farf

    if [[ -r target/perf-libs/env.sh ]]; then
      # shellcheck genesis=/dev/null
      genesis target/perf-libs/env.sh
    fi
    $MAYBE_DOCKER bash -c "
      set -ex
      scripts/cargo-install-all.sh farf \"$cargoFeatures\"
      if [[ -n \"$customPrograms\" ]]; then
        scripts/cargo-install-custom-programs.sh farf $customPrograms
      fi
    "
  )
  echo "Build took $SECONDS seconds"
}

startCommon() {
  declare ipAddress=$1
  test -d "$SOLANA_ROOT"
  if $skipSetup; then
    ssh "${sshOptions[@]}" "$ipAddress" "
      set -x;
      mkdir -p ~/morgan/config{,-local}
      rm -rf ~/config{,-local};
      mv ~/morgan/config{,-local} ~;
      rm -rf ~/morgan;
      mkdir -p ~/morgan ~/.cargo/bin;
      mv ~/config{,-local} ~/morgan/
    "
  else
    ssh "${sshOptions[@]}" "$ipAddress" "
      set -x;
      rm -rf ~/morgan;
      mkdir -p ~/.cargo/bin
    "
  fi
  [[ -z "$externalNodeSshKey" ]] || ssh-copy-id -f -i "$externalNodeSshKey" "${sshOptions[@]}" "morgan@$ipAddress"
  rsync -vPrc -e "ssh ${sshOptions[*]}" \
    "$SOLANA_ROOT"/{fetch-perf-libs.sh,scripts,net,multinode-demo} \
    "$ipAddress":~/morgan/
}

startBootstrapLeader() {
  declare ipAddress=$1
  declare logFile="$2"
  echo "--- Starting bootstrap leader: $ipAddress"
  echo "start log: $logFile"

  # Deploy local binaries to bootstrap fullnode.  Other fullnodes and clients later fetch the
  # binaries from it
  (
    set -x
    startCommon "$ipAddress" || exit 1
    case $deployMethod in
    tar)
      rsync -vPrc -e "ssh ${sshOptions[*]}" "$SOLANA_ROOT"/morgan-release/bin/* "$ipAddress:~/.cargo/bin/"
      ;;
    local)
      rsync -vPrc -e "ssh ${sshOptions[*]}" "$SOLANA_ROOT"/farf/bin/* "$ipAddress:~/.cargo/bin/"
      ;;
    *)
      usage "Internal error: invalid deployMethod: $deployMethod"
      ;;
    esac

    ssh "${sshOptions[@]}" -n "$ipAddress" \
      "./morgan/net/remote/remote-node.sh \
         $deployMethod \
         bootstrap-leader \
         $entrypointIp \
         $((${#fullnodeIpList[@]} + ${#blockstreamerIpList[@]})) \
         \"$RUST_LOG\" \
         $skipSetup \
         $failOnValidatorBootupFailure \
         \"$genesisOptions\" \
      "
  ) >> "$logFile" 2>&1 || {
    cat "$logFile"
    echo "^^^ +++"
    exit 1
  }
}

startNode() {
  declare ipAddress=$1
  declare nodeType=$2
  declare logFile="$netLogDir/fullnode-$ipAddress.log"

  echo "--- Starting $nodeType: $ipAddress"
  echo "start log: $logFile"
  (
    set -x
    startCommon "$ipAddress"
    ssh "${sshOptions[@]}" -n "$ipAddress" \
      "./morgan/net/remote/remote-node.sh \
         $deployMethod \
         $nodeType \
         $entrypointIp \
         $((${#fullnodeIpList[@]} + ${#blockstreamerIpList[@]})) \
         \"$RUST_LOG\" \
         $skipSetup \
         $failOnValidatorBootupFailure \
         \"$genesisOptions\" \
      "
  ) >> "$logFile" 2>&1 &
  declare pid=$!
  ln -sf "fullnode-$ipAddress.log" "$netLogDir/fullnode-$pid.log"
  pids+=("$pid")
}

startClient() {
  declare ipAddress=$1
  declare clientToRun="$2"
  declare logFile="$netLogDir/client-$clientToRun-$ipAddress.log"
  echo "--- Starting client: $ipAddress - $clientToRun"
  echo "start log: $logFile"
  (
    set -x
    startCommon "$ipAddress"
    ssh "${sshOptions[@]}" -f "$ipAddress" \
      "./morgan/net/remote/remote-client.sh $deployMethod $entrypointIp \
      $clientToRun \"$RUST_LOG\" \"$benchTpsExtraArgs\" \"$benchExchangeExtraArgs\""
  ) >> "$logFile" 2>&1 || {
    cat "$logFile"
    echo "^^^ +++"
    exit 1
  }
}

sanity() {
  $metricsWriteDatapoint "testnet-deploy net-sanity-begin=1"

  declare ok=true
  declare bootstrapLeader=${fullnodeIpList[0]}
  declare blockstreamer=${blockstreamerIpList[0]}

  annotateBlockexplorerUrl

  echo "--- Sanity: $bootstrapLeader"
  (
    set -x
    # shellcheck disable=SC2029 # remote-client.sh args are expanded on client side intentionally
    ssh "${sshOptions[@]}" "$bootstrapLeader" \
      "./morgan/net/remote/remote-sanity.sh $bootstrapLeader $sanityExtraArgs \"$RUST_LOG\""
  ) || ok=false
  $ok || exit 1

  if [[ -n $blockstreamer ]]; then
    # If there's a blockstreamer node run a reduced sanity check on it as well
    echo "--- Sanity: $blockstreamer"
    (
      set -x
      # shellcheck disable=SC2029 # remote-client.sh args are expanded on client side intentionally
      ssh "${sshOptions[@]}" "$blockstreamer" \
        "./morgan/net/remote/remote-sanity.sh $blockstreamer $sanityExtraArgs -o noLedgerVerify -o noValidatorSanity \"$RUST_LOG\""
    ) || ok=false
    $ok || exit 1
  fi

  $metricsWriteDatapoint "testnet-deploy net-sanity-complete=1"
}

deployUpdate() {
  if [[ -z $updateManifestKeypairFile ]]; then
    return
  fi
  [[ $deployMethod = tar ]] || exit 1
  [[ -n $updateDownloadUrl ]] || exit 1

  declare ok=true
  declare bootstrapLeader=${fullnodeIpList[0]}

  echo "--- Deploying morgan-install update: $updateDownloadUrl"
  (
    set -x
    timeout 30s scp "${sshOptions[@]}" \
      "$updateManifestKeypairFile" "$bootstrapLeader:morgan/update_manifest_keypair.json"

    # shellcheck disable=SC2029 # remote-deploy-update.sh args are expanded on client side intentionally
    ssh "${sshOptions[@]}" "$bootstrapLeader" \
      "./morgan/net/remote/remote-deploy-update.sh $updateDownloadUrl \"$RUST_LOG\""
  ) || ok=false
  $ok || exit 1
}

start() {
  case $deployMethod in
  tar)
    if [[ -n $releaseChannel ]]; then
      rm -f "$SOLANA_ROOT"/morgan-release.tar.bz2
      updateDownloadUrl=http://release.morgan.com/"$releaseChannel"/morgan-release-x86_64-unknown-linux-gnu.tar.bz2
      (
        set -x
        curl -o "$SOLANA_ROOT"/morgan-release.tar.bz2 "$updateDownloadUrl"
      )
      tarballFilename="$SOLANA_ROOT"/morgan-release.tar.bz2
    else
      if [[ -n $updateManifestKeypairFile ]]; then
        echo "Error: -i argument was provided but -t was not"
        exit 1
      fi
    fi
    (
      set -x
      rm -rf "$SOLANA_ROOT"/morgan-release
      (cd "$SOLANA_ROOT"; tar jxv) < "$tarballFilename"
      cat "$SOLANA_ROOT"/morgan-release/version.yml
    )
    ;;
  local)
    build
    ;;
  *)
    usage "Internal error: invalid deployMethod: $deployMethod"
    ;;
  esac

  echo "Deployment started at $(date)"
  if $updateNodes; then
    $metricsWriteDatapoint "testnet-deploy net-update-begin=1"
  else
    $metricsWriteDatapoint "testnet-deploy net-start-begin=1"
  fi

  declare bootstrapLeader=true
  declare nodeType=validator
  declare loopCount=0
  for ipAddress in "${fullnodeIpList[@]}" - "${blockstreamerIpList[@]}"; do
    if [[ $ipAddress = - ]]; then
      nodeType=blockstreamer
      continue
    fi
    if $updateNodes; then
      stopNode "$ipAddress" true
    fi
    if $bootstrapLeader; then
      SECONDS=0
      declare bootstrapNodeDeployTime=
      startBootstrapLeader "$ipAddress" "$netLogDir/bootstrap-leader-$ipAddress.log"
      bootstrapNodeDeployTime=$SECONDS
      $metricsWriteDatapoint "testnet-deploy net-bootnode-leader-started=1"

      bootstrapLeader=false
      SECONDS=0
      pids=()
    else
      startNode "$ipAddress" $nodeType

      # Stagger additional node start time. If too many nodes start simultaneously
      # the bootstrap node gets more rsync requests from the additional nodes than
      # it can handle.
      ((loopCount++ % 2 == 0)) && sleep 2
    fi
  done

  for pid in "${pids[@]}"; do
    declare ok=true
    wait "$pid" || ok=false
    if ! $ok; then
      echo "+++ fullnode failed to start"
      cat "$netLogDir/fullnode-$pid.log"
      if $failOnValidatorBootupFailure; then
        exit 1
      else
        echo "Failure is non-fatal"
      fi
    fi
  done

  $metricsWriteDatapoint "testnet-deploy net-fullnodes-started=1"
  additionalNodeDeployTime=$SECONDS

  annotateBlockexplorerUrl

  if $updateNodes; then
    for ipAddress in "${clientIpList[@]}"; do
      stopNode "$ipAddress" true
    done
  fi
  sanity

  SECONDS=0
  for ((i=0; i < "$numClients" && i < "$numClientsRequested"; i++)) do
    if [[ $i -lt "$numBenchTpsClients" ]]; then
      startClient "${clientIpList[$i]}" "morgan-benchbot"
    else
      startClient "${clientIpList[$i]}" "morgan-bench-exchange"
    fi
  done
  clientDeployTime=$SECONDS

  if $updateNodes; then
    $metricsWriteDatapoint "testnet-deploy net-update-complete=1"
  else
    $metricsWriteDatapoint "testnet-deploy net-start-complete=1"
  fi

  declare networkVersion=unknown
  case $deployMethod in
  tar)
    networkVersion="$(
      (
        set -o pipefail
        grep "^commit: " "$SOLANA_ROOT"/morgan-release/version.yml | head -n1 | cut -d\  -f2
      ) || echo "tar-unknown"
    )"
    ;;
  local)
    networkVersion="$(git rev-parse HEAD || echo local-unknown)"
    ;;
  *)
    usage "Internal error: invalid deployMethod: $deployMethod"
    ;;
  esac
  $metricsWriteDatapoint "testnet-deploy version=\"${networkVersion:0:9}\""

  deployUpdate

  echo
  echo "+++ Deployment Successful"
  echo "Bootstrap leader deployment took $bootstrapNodeDeployTime seconds"
  echo "Additional fullnode deployment (${#fullnodeIpList[@]} full nodes, ${#blockstreamerIpList[@]} blockstreamer nodes) took $additionalNodeDeployTime seconds"
  echo "Client deployment (${#clientIpList[@]} instances) took $clientDeployTime seconds"
  echo "Network start logs in $netLogDir"
}


stopNode() {
  local ipAddress=$1
  local block=$2
  declare logFile="$netLogDir/stop-fullnode-$ipAddress.log"
  echo "--- Stopping node: $ipAddress"
  echo "stop log: $logFile"
  (
    set -x
    # shellcheck disable=SC2029 # It's desired that PS4 be expanded on the client side
    ssh "${sshOptions[@]}" "$ipAddress" "
      PS4=\"$PS4\"
      set -x
      ! tmux list-sessions || tmux kill-session
      for pid in morgan/{net-stats,oom-monitor}.pid; do
        pgid=\$(ps opgid= \$(cat \$pid) | tr -d '[:space:]')
        sudo kill -- -\$pgid
      done
      for pattern in node morgan- remote-; do
        pkill -9 \$pattern
      done
    "
  ) >> "$logFile" 2>&1 &

  declare pid=$!
  ln -sf "stop-fullnode-$ipAddress.log" "$netLogDir/stop-fullnode-$pid.log"
  if $block; then
    wait $pid
  else
    pids+=("$pid")
  fi
}

stop() {
  SECONDS=0
  $metricsWriteDatapoint "testnet-deploy net-stop-begin=1"

  declare loopCount=0
  pids=()
  for ipAddress in "${fullnodeIpList[@]}" "${blockstreamerIpList[@]}" "${clientIpList[@]}"; do
    stopNode "$ipAddress" false

    # Stagger additional node stop time to avoid too many concurrent ssh
    # sessions
    ((loopCount++ % 4 == 0)) && sleep 2
  done

  echo --- Waiting for nodes to finish stopping
  for pid in "${pids[@]}"; do
    echo -n "$pid "
    wait "$pid" || true
  done
  echo

  $metricsWriteDatapoint "testnet-deploy net-stop-complete=1"
  echo "Stopping nodes took $SECONDS seconds"
}

case $command in
restart)
  stop
  start
  ;;
start)
  start
  ;;
update)
  skipSetup=true
  updateNodes=true
  start
  ;;
sanity)
  sanity
  ;;
stop)
  stop
  ;;
logs)
  fetchRemoteLog() {
    declare ipAddress=$1
    declare log=$2
    echo "--- fetching $log from $ipAddress"
    (
      set -x
      timeout 30s scp "${sshOptions[@]}" \
        "$ipAddress":morgan/"$log".log "$netLogDir"/remote-"$log"-"$ipAddress".log
    ) || echo "failed to fetch log"
  }
  fetchRemoteLog "${fullnodeIpList[0]}" drone
  for ipAddress in "${fullnodeIpList[@]}"; do
    fetchRemoteLog "$ipAddress" fullnode
  done
  for ipAddress in "${clientIpList[@]}"; do
    fetchRemoteLog "$ipAddress" client
  done
  for ipAddress in "${blockstreamerIpList[@]}"; do
    fetchRemoteLog "$ipAddress" fullnode
  done
  ;;

*)
  echo "Internal error: Unknown command: $command"
  usage
  exit 1
esac
