#!/usr/bin/env python3

import json
import sys

phases_data = {}

if len(sys.argv) != 2:
    print("USAGE: {} <input file>".format(sys.argv[0]))
    sys.exit(1)

with open(sys.argv[1]) as fh:
    for line in fh.readlines():
        if "COUNTER" in line:
            json_part = line[line.find("{"):]
            x = json.loads(json_part)
            counter = x['name']
            if not (counter in phases_data):
                phases_data[counter] = {'first_ts': x['now'], 'last_ts': x['now'], 'last_count': 0,
                                        'data': [], 'max_speed': 0, 'min_speed': 9999999999.0,
                                        'count': 0,
                                        'max_speed_ts': 0, 'min_speed_ts': 0}
            phases_data[counter]['count'] += 1
            count_since_last = x['counts'] - phases_data[counter]['last_count']
            time_since_last = float(x['now'] - phases_data[counter]['last_ts'])
            if time_since_last > 1:
                speed = 1000.0 * (count_since_last / time_since_last)
                phases_data[counter]['data'].append(speed)
                if speed > phases_data[counter]['max_speed']:
                    phases_data[counter]['max_speed'] = speed
                    phases_data[counter]['max_speed_ts'] = x['now']
                if speed < phases_data[counter]['min_speed']:
                    phases_data[counter]['min_speed'] = speed
                    phases_data[counter]['min_speed_ts'] = x['now']
            phases_data[counter]['last_ts'] = x['now']
            phases_data[counter]['last_count'] = x['counts']

for phase in phases_data.keys():
    phases_data[phase]['data'].sort()
    #mean_index = phases_data[phase]['count'] / 2
    mean = 0
    average = 0
    eightieth = 0
    data_len = len(phases_data[phase]['data'])
    mean_index = int(data_len / 2)
    eightieth_index = int(data_len * 0.8)
    #print("mean idx: {} data.len: {}".format(mean_index, data_len))
    if data_len > 0:
        mean = phases_data[phase]['data'][mean_index]
        average = float(sum(phases_data[phase]['data'])) / data_len
        eightieth = phases_data[phase]['data'][eightieth_index]
    print("phase: {} max: {:,.2f} min: {:.2f} count: {} total: {} mean: {:,.2f} average: {:,.2f} 80%: {:,.2f}".format(phase,
                                                       phases_data[phase]['max_speed'],
                                                       phases_data[phase]['min_speed'],
                                                       phases_data[phase]['count'],
                                                       phases_data[phase]['last_count'],
                                                       mean, average, eightieth))
    num = 5
    idx = -1
    if data_len >= num:
        print("    top {}: ".format(num), end='')
        for x in range(0, num):
            print("{:,.2f}  ".format(phases_data[phase]['data'][idx]), end='')
            idx -= 1
            if phases_data[phase]['data'][idx] < average:
                break
        print("")
    print("    max_ts: {} min_ts: {}".format(phases_data[phase]['max_speed_ts'], phases_data[phase]['min_speed_ts']))
    print("\n")

