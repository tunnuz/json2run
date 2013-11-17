#!/usr/bin/python
import time
import math
import random
import json

# params
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
parser.add_argument("--variant", "-v", required = True, type=float, default = 10.0, help="parameter to optimize")
parser.add_argument("--instance", "-i", required = True, type=float, default = 1.0, help="instance parameter")

args = parser.parse_args()

# interval in seconds
min_t = float(min(args.instance, args.variant))
max_t = float(max(args.variant, args.instance))

# sleep random time
t = min_t + random.random() * (max_t-min_t)
time.sleep(t)

# JSON output
try:
    print json.dumps({"time": t, "cost": 10.0*math.exp(-t)})
except:
    pass