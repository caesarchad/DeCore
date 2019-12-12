#!/usr/bin/env bash
#
# Send a metrics datapoint
#

point=$1
if [[ -z $point ]]; then
  echo "Data point not specified"
  exit 1
fi

echo "Influx data point: $point"
if [[ -z $INFLUX_DATABASE || -z $INFLUX_USERNAME || -z $INFLUX_PASSWORD ]]; then
  echo Influx user credentials not found
  exit 0
fi

host="https://metrics.morgan.com:8086"

if [[ -n $INFLUX_HOST ]]; then
  host="$INFLUX_HOST"
fi

echo "${host}/write?db=${INFLUX_DATABASE}&u=${INFLUX_USERNAME}&p=${INFLUX_PASSWORD}" \
  | xargs curl --max-time 5 -XPOST --data-binary "$point"
exit 0
