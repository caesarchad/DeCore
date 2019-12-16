#!/usr/bin/env python

import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt
import json
import sys

phases_to_counters = {}
phases_to_time = {}

if len(sys.argv) != 2:
    print("USAGE: {} <input file>".format(sys.argv[0]))
    sys.exit(1)

with open(sys.argv[1]) as fh:
    for line in fh.readlines():
        if "COUNTER" in line:
            json_part = line[line.find("{"):]
            x = json.loads(json_part)
            counter = x['name']
            if not (counter in phases_to_counters):
                phases_to_counters[counter] = []
                phases_to_time[counter] = []
            phases_to_counters[counter].append(x['counts'])
            phases_to_time[counter].append(x['now'])

fig, ax = plt.subplots()

for phase in phases_to_counters.keys():
    plt.plot(phases_to_time[phase], phases_to_counters[phase], label=phase)

plt.xlabel('ms')
plt.ylabel('count')

plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=3,
           ncol=2, mode="expand", borderaxespad=0.)

plt.locator_params(axis='x', nbins=10)
plt.grid(True)

plt.savefig("perf.pdf")
