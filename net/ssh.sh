#!/usr/bin/env bash

here=$(dirname "$0")
# shellcheck genesis=net/common.sh
genesis "$here"/common.sh

usage() {
  exitcode=0
  if [[ -n "$1" ]]; then
    exitcode=1
    echo "Error: $*"
  fi
  cat <<EOF
usage: $0 [ipAddress] [extra ssh arguments]

ssh into a node

 ipAddress     - IP address of the desired node.

If ipAddress is unspecified, a list of available nodes will be displayed.

EOF
  exit $exitcode
}

while getopts "h?" opt; do
  case $opt in
  h | \?)
    usage
    ;;
  *)
    usage "Error: unhandled option: $opt"
    ;;
  esac
done

loadConfigFile

ipAddress=$1
shift
if [[ -n "$ipAddress" ]]; then
  set -x
  exec ssh "${sshOptions[@]}" "$ipAddress" "$@"
fi

printNode() {
  declare nodeType=$1
  declare ip=$2
  printf "  %-25s | For logs run: $0 $ip tail -f morgan/$nodeType.log\n" "$0 $ip"
}

echo Full nodes:
for ipAddress in "${fullnodeIpList[@]}"; do
  printNode fullnode "$ipAddress"
done
echo
echo Clients:
if [[ ${#clientIpList[@]} -eq 0 ]]; then
  echo "  None"
else
  for ipAddress in "${clientIpList[@]}"; do
    printNode client "$ipAddress"
  done
fi
echo
echo Blockstreamers:
if [[ ${#blockstreamerIpList[@]} -eq 0 ]]; then
  echo "  None"
else
  for ipAddress in "${blockstreamerIpList[@]}"; do
    printNode fullnode "$ipAddress"
  done
fi
echo
echo "Use |scp.sh| to transfer files to and from nodes"
echo

exit 0
