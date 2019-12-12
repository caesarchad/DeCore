#!/usr/bin/env bash
set -ex

cd "$(dirname "$0")"/../..
#eval "$(ci/channel-info.sh)"

CHANNEL=beta
rust_stable_docker_image=solanalabs/rust:1.35.0

if [[ -z $CHANNEL ]]; then
  echo Unable to determine channel to publish into, exiting.
  echo "^^^ +++"
  exit 0
fi

cd "$(dirname "$0")"
rm -rf usr/
../../ci/docker-run.sh "$rust_stable_docker_image" \
  scripts/cargo-install-all.sh interface/docker-morgan/usr

cp -f ../../run.sh usr/bin/morgan-run.sh

docker build -t aya015757881/testdocker:"$CHANNEL" .

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
