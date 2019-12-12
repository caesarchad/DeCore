#!/usr/bin/env bash
set -e

cd "$(dirname "$0")"/..

if [[ -z $BUILDKITE ]]; then
  echo BUILDKITE not defined
  exit 1
fi

if [[ -z $SOLANA_METRICS_PARTIAL_CONFIG ]]; then
  echo SOLANA_METRICS_PARTIAL_CONFIG not defined
  exit 1
fi

if [[ -z $TESTNET ]]; then
  TESTNET=$(buildkite-agent meta-data get "testnet" --default "")
fi

if [[ -z $TESTNET_OP ]]; then
  TESTNET_OP=$(buildkite-agent meta-data get "testnet-operation" --default "")
fi

if [[ -z $TESTNET || -z $TESTNET_OP ]]; then
  (
    cat <<EOF
steps:
  - block: "Manage Testnet"
    fields:
      - select: "Network"
        key: "testnet"
        options:
          - label: "testnet"
            value: "testnet"
          - label: "testnet-perf"
            value: "testnet-perf"
          - label: "testnet-edge"
            value: "testnet-edge"
          - label: "testnet-edge-perf"
            value: "testnet-edge-perf"
          - label: "testnet-beta"
            value: "testnet-beta"
          - label: "testnet-beta-perf"
            value: "testnet-beta-perf"
          - label: "testnet-demo"
            value: "testnet-demo"
      - select: "Operation"
        key: "testnet-operation"
        default: "sanity-or-restart"
        options:
          - label: "Create testnet and then start software.  If the testnet already exists it will be deleted and re-created"
            value: "create-and-start"
          - label: "Create testnet, but do not start software.  If the testnet already exists it will be deleted and re-created"
            value: "create"
          - label: "Start network software on an existing testnet.  If software is already running it will be restarted"
            value: "start"
          - label: "Stop network software without deleting testnet nodes"
            value: "stop"
          - label: "Update the network software.  Restart network software on failure"
            value: "update-or-restart"
          - label: "Sanity check.  Restart network software on failure"
            value: "sanity-or-restart"
          - label: "Sanity check only"
            value: "sanity"
          - label: "Delete the testnet"
            value: "delete"
          - label: "Enable/unlock the testnet"
            value: "enable"
          - label: "Delete and then lock the testnet from further operation until it is re-enabled"
            value: "disable"
  - command: "ci/$(basename "$0")"
    agents:
      - "queue=$BUILDKITE_AGENT_META_DATA_QUEUE"
EOF
  ) | buildkite-agent pipeline upload
  exit 0
fi

ci/channel-info.sh
eval "$(ci/channel-info.sh)"


EC2_ZONES=(
  us-west-1a
  us-west-2a
  us-east-1a
  us-east-2a
  sa-east-1a
  eu-west-1a
  eu-west-2a
  eu-central-1a
  ap-northeast-2a
  ap-southeast-2a
  ap-south-1a
  ca-central-1a
)

# GCE zones with _lots_ of quota
GCE_ZONES=(
  us-west1-a
  us-central1-a
  us-east1-b
  europe-west4-a

  us-west1-b
  us-central1-b
  us-east1-c
  europe-west4-b

  us-west1-c
  us-east1-d
  europe-west4-c
)

# GCE zones with enough quota for one CPU-only fullnode
GCE_LOW_QUOTA_ZONES=(
  asia-east2-a
  asia-northeast1-b
  asia-northeast2-b
  asia-south1-c
  asia-southeast1-b
  australia-southeast1-b
  europe-north1-a
  europe-west2-b
  europe-west3-c
  europe-west6-a
  northamerica-northeast1-a
  southamerica-east1-b
)

case $TESTNET in
testnet-edge|testnet-edge-perf)
  CHANNEL_OR_TAG=edge
  CHANNEL_BRANCH=$EDGE_CHANNEL
  ;;
testnet-beta|testnet-beta-perf)
  CHANNEL_OR_TAG=beta
  CHANNEL_BRANCH=$BETA_CHANNEL
  ;;
testnet)
  CHANNEL_OR_TAG=$STABLE_CHANNEL_LATEST_TAG
  CHANNEL_BRANCH=$STABLE_CHANNEL
  : "${EC2_NODE_COUNT:=10}"
  : "${GCE_NODE_COUNT:=}"
  ;;
testnet-perf)
  CHANNEL_OR_TAG=$STABLE_CHANNEL_LATEST_TAG
  CHANNEL_BRANCH=$STABLE_CHANNEL
  ;;
testnet-demo)
  CHANNEL_OR_TAG=beta
  CHANNEL_BRANCH=$BETA_CHANNEL
  : "${GCE_NODE_COUNT:=150}"
  : "${GCE_LOW_QUOTA_NODE_COUNT:=70}"
  ;;
*)
  echo "Error: Invalid TESTNET=$TESTNET"
  exit 1
  ;;
esac

EC2_ZONE_ARGS=()
for val in "${EC2_ZONES[@]}"; do
  EC2_ZONE_ARGS+=("-z $val")
done
GCE_ZONE_ARGS=()
for val in "${GCE_ZONES[@]}"; do
  GCE_ZONE_ARGS+=("-z $val")
done
GCE_LOW_QUOTA_ZONE_ARGS=()
for val in "${GCE_LOW_QUOTA_ZONES[@]}"; do
  GCE_LOW_QUOTA_ZONE_ARGS+=("-z $val")
done

if [[ -n $TESTNET_DB_HOST ]]; then
  SOLANA_METRICS_PARTIAL_CONFIG="host=$TESTNET_DB_HOST,$SOLANA_METRICS_PARTIAL_CONFIG"
fi

export SOLANA_METRICS_CONFIG="db=$TESTNET,$SOLANA_METRICS_PARTIAL_CONFIG"
echo "SOLANA_METRICS_CONFIG: $SOLANA_METRICS_CONFIG"
genesis scripts/configure-metrics.sh

if [[ -n $TESTNET_TAG ]]; then
  CHANNEL_OR_TAG=$TESTNET_TAG
else

  if [[ $BUILDKITE_BRANCH != "$CHANNEL_BRANCH" ]]; then
    (
      cat <<EOF
steps:
  - trigger: "$BUILDKITE_PIPELINE_SLUG"
    async: true
    build:
      message: "$BUILDKITE_MESSAGE"
      branch: "$CHANNEL_BRANCH"
      env:
        TESTNET: "$TESTNET"
        TESTNET_OP: "$TESTNET_OP"
        TESTNET_DB_HOST: "$TESTNET_DB_HOST"
        EC2_NODE_COUNT: "$EC2_NODE_COUNT"
        GCE_NODE_COUNT: "$GCE_NODE_COUNT"
        GCE_LOW_QUOTA_NODE_COUNT: "$GCE_LOW_QUOTA_NODE_COUNT"
EOF
    ) | buildkite-agent pipeline upload
    exit 0
  fi
fi

sanity() {
  echo "--- sanity $TESTNET"
  case $TESTNET in
  testnet-edge)
    (
      set -x
      NO_INSTALL_CHECK=1 \
      NO_LEDGER_VERIFY=1 \
        ci/testnet-sanity.sh edge-testnet-morgan-com ec2 us-west-1a
    )
    ;;
  testnet-edge-perf)
    (
      set -x
      REJECT_EXTRA_NODES=1 \
      NO_LEDGER_VERIFY=1 \
      NO_VALIDATOR_SANITY=1 \
        ci/testnet-sanity.sh edge-perf-testnet-morgan-com ec2 us-west-2b
    )
    ;;
  testnet-beta)
    (
      set -x
      NO_INSTALL_CHECK=1 \
      NO_LEDGER_VERIFY=1 \
        ci/testnet-sanity.sh beta-testnet-morgan-com ec2 us-west-1a
    )
    ;;
  testnet-beta-perf)
    (
      set -x
      REJECT_EXTRA_NODES=1 \
      NO_LEDGER_VERIFY=1 \
      NO_VALIDATOR_SANITY=1 \
        ci/testnet-sanity.sh beta-perf-testnet-morgan-com ec2 us-west-2b
    )
    ;;
  testnet)
    (
      set -x

      ok=true
      if [[ -n $EC2_NODE_COUNT ]]; then
        NO_LEDGER_VERIFY=1 \
          ci/testnet-sanity.sh testnet-morgan-com ec2 "${EC2_ZONES[0]}" || ok=false
      elif [[ -n $GCE_NODE_COUNT ]]; then
        NO_LEDGER_VERIFY=1 \
          ci/testnet-sanity.sh testnet-morgan-com gce "${GCE_ZONES[0]}" || ok=false
      else
        echo "Error: no EC2 or GCE nodes"
        ok=false
      fi
      $ok
    )
    ;;
  testnet-perf)
    (
      set -x
      REJECT_EXTRA_NODES=1 \
      NO_LEDGER_VERIFY=1 \
      NO_VALIDATOR_SANITY=1 \
        ci/testnet-sanity.sh perf-testnet-morgan-com gce us-west1-b
      #ci/testnet-sanity.sh perf-testnet-morgan-com ec2 us-east-1a
    )
    ;;
  testnet-demo)
    (
      set -x

      ok=true
      if [[ -n $GCE_NODE_COUNT ]]; then
        NO_LEDGER_VERIFY=1 \
        NO_VALIDATOR_SANITY=1 \
          ci/testnet-sanity.sh demo-testnet-morgan-com gce "${GCE_ZONES[0]}" -f || ok=false
      else
        echo "Error: no GCE nodes"
        ok=false
      fi
      $ok
    )
    ;;
  *)
    echo "Error: Invalid TESTNET=$TESTNET"
    exit 1
    ;;
  esac
}

deploy() {
  declare maybeCreate=$1
  declare maybeStart=$2
  declare maybeStop=$3
  declare maybeDelete=$4

  echo "--- deploy \"$maybeCreate\" \"$maybeStart\" \"$maybeStop\" \"$maybeDelete\""

  # Create or recreate the nodes
  if [[ -z $maybeCreate ]]; then
    skipCreate=skip
  else
    skipCreate=""
  fi

  # Start or restart the network software on the nodes
  if [[ -z $maybeStart ]]; then
    skipStart=skip
  else
    skipStart=""
  fi

  case $TESTNET in
  testnet-edge)
    (
      set -x
      ci/testnet-deploy.sh -p edge-testnet-morgan-com -C ec2 -z us-west-1a \
        -t "$CHANNEL_OR_TAG" -n 3 -c 0 -u -P -a eipalloc-0ccd4f2239886fa94 \
        ${skipCreate:+-e} \
        ${skipStart:+-s} \
        ${maybeStop:+-S} \
        ${maybeDelete:+-D} \
        --hashes-per-tick auto
    )
    ;;
  testnet-edge-perf)
    (
      set -x
      NO_LEDGER_VERIFY=1 \
      NO_VALIDATOR_SANITY=1 \
      RUST_LOG=morgan=warn \
        ci/testnet-deploy.sh -p edge-perf-testnet-morgan-com -C ec2 -z us-west-2b \
          -g -t "$CHANNEL_OR_TAG" -c 2 \
          ${skipCreate:+-e} \
          ${skipStart:+-s} \
          ${maybeStop:+-S} \
          ${maybeDelete:+-D} \
          --hashes-per-tick auto
    )
    ;;
  testnet-beta)
    (
      set -x
      NO_VALIDATOR_SANITY=1 \
        ci/testnet-deploy.sh -p beta-testnet-morgan-com -C ec2 -z us-west-1a \
          -t "$CHANNEL_OR_TAG" -n 3 -c 0 -u -P -a eipalloc-0f286cf8a0771ce35 \
          ${skipCreate:+-e} \
          ${skipStart:+-s} \
          ${maybeStop:+-S} \
          ${maybeDelete:+-D} \
          --hashes-per-tick auto
    )
    ;;
  testnet-beta-perf)
    (
      set -x
      NO_LEDGER_VERIFY=1 \
      NO_VALIDATOR_SANITY=1 \
      RUST_LOG=morgan=warn \
        ci/testnet-deploy.sh -p beta-perf-testnet-morgan-com -C ec2 -z us-west-2b \
          -g -t "$CHANNEL_OR_TAG" -c 2 \
          ${skipCreate:+-e} \
          ${skipStart:+-s} \
          ${maybeStop:+-S} \
          ${maybeDelete:+-D} \
          --hashes-per-tick auto
    )
    ;;
  testnet)
    (
      set -x

      if [[ -n $GCE_NODE_COUNT ]] || [[ -n $skipStart ]]; then
        maybeSkipStart="skip"
      fi

      # shellcheck disable=SC2068
      ci/testnet-deploy.sh -p testnet-morgan-com -C ec2 ${EC2_ZONE_ARGS[@]} \
        -t "$CHANNEL_OR_TAG" -n "$EC2_NODE_COUNT" -c 0 -u -P -f -a eipalloc-0fa502bf95f6f18b2 \
        ${skipCreate:+-e} \
        ${maybeSkipStart:+-s} \
        ${maybeStop:+-S} \
        ${maybeDelete:+-D}

      if [[ -n $GCE_NODE_COUNT ]]; then
        # shellcheck disable=SC2068
        ci/testnet-deploy.sh -p testnet-morgan-com -C gce ${GCE_ZONE_ARGS[@]} \
          -t "$CHANNEL_OR_TAG" -n "$GCE_NODE_COUNT" -c 0 -P -f \
          ${skipCreate:+-e} \
          ${skipStart:+-s} \
          ${maybeStop:+-S} \
          ${maybeDelete:+-D} \
          -x
      fi
    )
    ;;
  testnet-perf)
    (
      set -x
      NO_LEDGER_VERIFY=1 \
      NO_VALIDATOR_SANITY=1 \
      RUST_LOG=morgan=warn \
        ci/testnet-deploy.sh -p perf-testnet-morgan-com -C gce -z us-west1-b \
          -G "--machine-type n1-standard-16 --accelerator count=2,type=nvidia-tesla-v100" \
          -t "$CHANNEL_OR_TAG" -c 2 \
          -d pd-ssd \
          ${skipCreate:+-e} \
          ${skipStart:+-s} \
          ${maybeStop:+-S} \
          ${maybeDelete:+-D} \
          --hashes-per-tick auto
    )
    ;;
  testnet-demo)
    (
      set -x

      if [[ -n $GCE_LOW_QUOTA_NODE_COUNT ]] || [[ -n $skipStart ]]; then
        maybeSkipStart="skip"
      fi

      # shellcheck disable=SC2068
      NO_LEDGER_VERIFY=1 \
      NO_VALIDATOR_SANITY=1 \
        ci/testnet-deploy.sh -p demo-testnet-morgan-com -C gce ${GCE_ZONE_ARGS[@]} \
          -t "$CHANNEL_OR_TAG" -n "$GCE_NODE_COUNT" -c 0 -P -u -f -w \
          -a demo-testnet-morgan-com \
          ${skipCreate:+-e} \
          ${maybeSkipStart:+-s} \
          ${maybeStop:+-S} \
          ${maybeDelete:+-D} \
          --hashes-per-tick auto

      if [[ -n $GCE_LOW_QUOTA_NODE_COUNT ]]; then
        # shellcheck disable=SC2068
        NO_LEDGER_VERIFY=1 \
        NO_VALIDATOR_SANITY=1 \
          ci/testnet-deploy.sh -p demo-testnet-morgan-com2 -C gce ${GCE_LOW_QUOTA_ZONE_ARGS[@]} \
            -t "$CHANNEL_OR_TAG" -n "$GCE_LOW_QUOTA_NODE_COUNT" -c 0 -P -f -x -w \
            ${skipCreate:+-e} \
            ${skipStart:+-s} \
            ${maybeStop:+-S} \
            ${maybeDelete:+-D} \
            --hashes-per-tick auto
      fi
    )
    ;;
  *)
    echo "Error: Invalid TESTNET=$TESTNET"
    exit 1
    ;;
  esac
}

ENABLED_LOCKFILE="${HOME}/${TESTNET}.is_enabled"

create-and-start() {
  deploy create start
}
create() {
  deploy create
}
start() {
  deploy "" start
}
stop() {
  deploy "" ""
}
delete() {
  deploy "" "" "" delete
}
enable_testnet() {
  touch "${ENABLED_LOCKFILE}"
  echo "+++ $TESTNET now enabled"
}
disable_testnet() {
  rm -f "${ENABLED_LOCKFILE}"
  echo "+++ $TESTNET now disabled"
}
is_testnet_enabled() {
  if [[ ! -f ${ENABLED_LOCKFILE} ]]; then
    echo "+++ ${TESTNET} is currently disabled.  Enable ${TESTNET} by running ci/testnet-manager.sh with \$TESTNET_OP=enable, then re-run with current settings."
    exit 0
  fi
}

case $TESTNET_OP in
enable)
  enable_testnet
  ;;
disable)
  disable_testnet
  delete
  ;;
create-and-start)
  is_testnet_enabled
  create-and-start
  ;;
create)
  is_testnet_enabled
  create
  ;;
start)
  is_testnet_enabled
  start
  ;;
stop)
  is_testnet_enabled
  stop
  ;;
sanity)
  is_testnet_enabled
  sanity
  ;;
delete)
  is_testnet_enabled
  delete
  ;;
update-or-restart)
  is_testnet_enabled
  if start; then
    echo Update successful
  else
    echo "+++ Update failed, restarting the network"
    $metricsWriteDatapoint "testnet-manager update-failure=1"
    create-and-start
  fi
  ;;
sanity-or-restart)
  is_testnet_enabled
  if sanity; then
    echo Pass
  else
    echo "+++ Sanity failed, updating the network"
    $metricsWriteDatapoint "testnet-manager sanity-failure=1"

    # TODO: Restore attempt to restart the cluster before recreating it
    #       See https://github.com/morgan-labs/morgan/issues/3774
    if false; then
      if start; then
        echo Update successful
      else
        echo "+++ Update failed, restarting the network"
        $metricsWriteDatapoint "testnet-manager update-failure=1"
        create-and-start
      fi
    else
      create-and-start
    fi
  fi
  ;;
*)
  echo "Error: Invalid TESTNET_OP=$TESTNET_OP"
  exit 1
  ;;
esac

echo --- fin
exit 0
