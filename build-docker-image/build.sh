#!/usr/bin/env bash
set -ex

CHANNEL=beta
rust_stable_docker_image=solanalabs/rust:1.35.0

cd "$(dirname "$0")"
rm -rf usr/

cd ..
docker pull $rust_stable_docker_image

ARGS=(
  --workdir /morgan
  --volume "$PWD:/morgan"
  --rm
)

if [[ -n $CI ]]; then
  # Share the real ~/.cargo between docker containers in CI for speed
  ARGS+=(--volume "$HOME:/home")
else
  # Avoid sharing ~/.cargo when building locally to avoid a mixed macOS/Linux
  # ~/.cargo
  ARGS+=(--volume "$PWD:/home")
fi
ARGS+=(--env "CARGO_HOME=/home/.cargo")

# kcov tries to set the personality of the binary which docker
# doesn't allow by default.
ARGS+=(--security-opt "seccomp=unconfined")

# Ensure files are created with the current host uid/gid
if [[ -z "$SOLANA_DOCKER_RUN_NOSETUID" ]]; then
  ARGS+=(--user "$(id -u):$(id -g)")
fi

# Environment variables to propagate into the container
ARGS+=(
  --env BUILDKITE
  --env BUILDKITE_AGENT_ACCESS_TOKEN
  --env BUILDKITE_BRANCH
  --env BUILDKITE_COMMIT
  --env BUILDKITE_JOB_ID
  --env BUILDKITE_TAG
  --env CI
  --env CODECOV_TOKEN
  --env CRATES_IO_TOKEN
)

set -x
docker run "${ARGS[@]}" "$rust_stable_docker_image" build-docker-image/cargo-install-all.sh build-docker-image/usr

cd "$(dirname "$0")"

cp -f ../run.sh usr/bin/morgan-run.sh

docker build -t aya015757881/testdocker:"$CHANNEL" .
rm -rf usr/

maybeEcho=
if [[ -z $CI ]]; then
  echo "Not CI, skipping |docker push|"
  maybeEcho="echo"
else
  (
    set +x
    if [[ -n $DOCKER_PASSWORD && -n $DOCKER_USERNAME ]]; then
      echo "$DOCKER_PASSWORD" | docker login --username "$DOCKER_USERNAME" --password-stdin
    fi
  )
fi
$maybeEcho docker push aya015757881/testdocker:"$CHANNEL"
