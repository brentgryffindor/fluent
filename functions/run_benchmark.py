#!/usr/bin/env python3

import cloudpickle as cp
import logging
import sys
import client as flclient
import time
import zmq
import uuid
import numpy as np
import random

from functions_pb2 import *
from include.serializer import *
from include.shared import *

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

def get_base(N, skew):
    base = 0.0
    for k in range(1, N+1):
        base += np.power(k, -1*skew)
    return 1 / float(base)



def sample(n, base, sum_probs):
    zipf_value = None
    low = 1
    high = n

    z = random.random()
    while z == 0 or z == 1:
        z = random.random()

    while True:
        mid = int(np.floor((low + high) / 2))
        if sum_probs[mid] >= z and sum_probs[mid - 1] < z:
            zipf_value = mid
            break
        elif sum_probs[mid] >= z:
            high = mid - 1
        else:
            low = mid + 1
        if low > high:
            break
    return zipf_value


def generate_arg_map(functions, connections, num_keys, base, sum_probs):
    arg_map = {}
    keys_read = []

    for func in functions:
        num_parents = 0 
        for conn in connections:
            if conn[1] == func:
                num_parents += 1

        to_generate = 2 - num_parents
        refs = ()
        keys_chosen = []
        while not to_generate == 0:
            # sample key from zipf
            key = sample(num_keys, base, sum_probs)
            key = str(key).zfill(len(str(num_keys)))

            if key not in keys_chosen:
                keys_chosen.append(key)
                refs += (FluentReference(key, False, CROSSCAUSAL),)
                to_generate -= 1
                keys_read.append(key)

        arg_map[func] = refs
        
    return arg_map, list(set(keys_read))

if len(sys.argv) < 4:
    print('Usage: ./run_benchmark.py benchmark_name function_elb mode'
          + '{ip}')
    sys.exit(1)

f_elb = sys.argv[2]
mode = sys.argv[3]

print("before flconn")

if len(sys.argv) == 5:
    ip = sys.argv[4]
    flconn = flclient.FluentConnection(f_elb, ip)
else:
    flconn = flclient.FluentConnection(f_elb)

print("after flconn")

kvs = flconn.kvs_client
dag_name = 'causal_test'
functions = ['strmnp1', 'strmnp2', 'strmnp3']
connections = [('strmnp1', 'strmnp2'), ('strmnp2', 'strmnp3')]
total_num_keys = 10000

if mode == 'create':
    print("Creating functions and DAG")
    ### DEFINE AND REGISTER FUNCTIONS ###
    def strmnp(a,b):
        return '0'.zfill(8)

    cloud_strmnp1 = flconn.register(strmnp, 'strmnp1')
    cloud_strmnp2 = flconn.register(strmnp, 'strmnp2')
    cloud_strmnp3 = flconn.register(strmnp, 'strmnp3')

    if cloud_strmnp1 and cloud_strmnp2 and cloud_strmnp3:
        print('Successfully registered the string manipulation function.')
    else:
        print('Error registering functions.')
        sys.exit(1)
    ### CREATE DAG ###

    success, error = flconn.register_dag(dag_name, functions, connections)

    if not success:
        print('Failed to register DAG: %s' % (ErrorType.Name(error)))
        sys.exit(1)

    print("Successfully created the DAG")
elif mode == 'run':
    zipf = 1.0
    base = get_base(total_num_keys, zipf)
    sum_probs = {}
    sum_probs[0] = 0.0
    for i in range(1, total_num_keys+1):
        sum_probs[i] = sum_probs[i - 1] + (base / np.power(float(i), zipf))
    for i in range(100):
        print(i)
        arg_map, read_set = generate_arg_map(functions, connections, total_num_keys, base, sum_probs)
        output = random.choice(read_set)
        start = time.time()
        res = flconn.call_dag(dag_name, arg_map, True, NORMAL, output, '0')
        end = time.time()
        print('time is: %s' % (end - start))
        print('Result is: %s' % res)