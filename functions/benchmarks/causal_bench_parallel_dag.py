import cloudpickle as cp
import logging
import numpy as np
import random
import sys
import time
import uuid

from anna.functions_pb2 import *
from anna.kvs_pb2 import *
from include.serializer import *
from include.shared import *
from . import utils

total_num_keys = 100000

functions = ['strmnp1', 'strmnp2', 'strmnp3']
connections = [('strmnp1', 'strmnp3'), ('strmnp2', 'strmnp3')]

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


def generate_arg_map(functions, connections, num_keys, base, sum_probs, outer):
    arg_map = {}
    keys_read = []

    for func in functions:
        num_parents = 0 
        for conn in connections:
            if conn[1] == func:
                num_parents += 1

        if num_parents == 2:
            to_generate = 1
        else:
            to_generate = 2
            
        refs = ()
        keys_chosen = []
        while not to_generate == 0:
            # sample key from zipf
            key = sample(num_keys, base, sum_probs)
            key = str(key + outer * num_keys).zfill(len(str(num_keys)) + 1)

            if key not in keys_chosen:
                keys_chosen.append(key)
                refs += (FluentReference(key, False, CROSSCAUSAL),)
                to_generate -= 1
                keys_read.append(key)

        arg_map[func] = refs
        
    return arg_map, list(set(keys_read))

def run(flconn, kvs, mode, sckt):
    dag_name = 'causal_test'

    if mode == 'create':
        print("Creating functions and DAG")
        logging.info("Creating functions and DAG")
        ### DEFINE AND REGISTER FUNCTIONS ###
        def strmnp1(a,b):
            return '0'.zfill(8)

        def strmnp2(a,b,c):
            return '0'.zfill(8)

        cloud_strmnp1 = flconn.register(strmnp1, 'strmnp1')
        cloud_strmnp2 = flconn.register(strmnp1, 'strmnp2')
        cloud_strmnp3 = flconn.register(strmnp2, 'strmnp3')

        if cloud_strmnp1 and cloud_strmnp2 and cloud_strmnp3:
            logging.info('Successfully registered the string manipulation function.')
        else:
            logging.info('Error registering functions.')
            sys.exit(1)

        ### TEST REGISTERED FUNCTIONS ###
        refs = ()
        for _ in range(2):
            val = '0'.zfill(8)
            ccv = CrossCausalValue()
            ccv.vector_clock['base'] = 1
            ccv.values.extend([serialize_val(val)])
            k = str(uuid.uuid4())
            print("key name is ", k)
            kvs.put(k, ccv)

            refs += (FluentReference(k, True, CROSSCAUSAL),)

        strmnp_test1 = cloud_strmnp1(*refs).get()
        strmnp_test2 = cloud_strmnp2(*refs).get()

        refs = ()
        for _ in range(3):
            val = '0'.zfill(8)
            ccv = CrossCausalValue()
            ccv.vector_clock['base'] = 1
            ccv.values.extend([serialize_val(val)])
            k = str(uuid.uuid4())
            print("key name is ", k)
            kvs.put(k, ccv)

            refs += (FluentReference(k, True, CROSSCAUSAL),)
        strmnp_test3 = cloud_strmnp3(*refs).get()
        if strmnp_test1 != '0'.zfill(8) or strmnp_test2 != '0'.zfill(8) or strmnp_test3 != '0'.zfill(8):
            logging.error('Unexpected result from strmnp(v1, v2, v3): %s %s %s' % (str(strmnp_test1), str(strmnp_test2), str(strmnp_test3)))
            sys.exit(1)

        print('Successfully tested functions!')
        logging.info('Successfully tested functions!')

        ### CREATE DAG ###

        success, error = flconn.register_dag(dag_name, functions, connections)

        if not success:
            logging.info('Failed to register DAG: %s' % (ErrorType.Name(error)))
            sys.exit(1)

        print("Successfully created the DAG")
        logging.info("Successfully created the DAG")

    elif mode == 'warmup':
        print('Warming up keys')
        logging.info('Warming up keys')
        ### CREATE DATA###
        warm_begin = time.time()
        for k in range(1,total_num_keys+1):
            if k % 1000 == 0:
                print('warmup for key %s done' % k)
            k = str(k).zfill(len(str(total_num_keys)))
            ccv = CrossCausalValue()
            ccv.vector_clock['base'] = 1
            ccv.values.extend([serialize_val('0'.zfill(8))])
            kvs.put(k, ccv)
        warm_end = time.time()
        print('warmup took %s' % (warm_end - warm_begin))

        print('Data populated')
        logging.info('Data populated')

    elif mode == 'run':
        ### CREATE ZIPF TABLE###
        zipf = 1.2
        base = get_base(total_num_keys, zipf)
        sum_probs = {}
        sum_probs[0] = 0.0
        for i in range(1, total_num_keys+1):
            sum_probs[i] = sum_probs[i - 1] + (base / np.power(float(i), zipf))

        logging.info("Created Probability Table with zipf %f" % zipf)
        print("Created Probability Table with zipf %f" % zipf)

        ### RUN DAG ###
        print('Running DAG')
        logging.info('Running DAG')

        client_num = 100

        total_time = []

        all_times = []

        for outer in range(10):
            total_time_per_loop = 0
            for i in range(0, client_num):
                cid = str(i).zfill(3)

                logging.info("running client %s loop %s" % (cid, outer))

                arg_map, read_set = generate_arg_map(functions, connections, total_num_keys, base, sum_probs, outer)

                for func in arg_map:
                    logging.info("function is %s" % func)
                    for ref in arg_map[func]:
                        print("key of reference is %s" % ref.key)

                for key in read_set:
                    print("read set contains %s" % key)

                output = random.choice(read_set)
                print("Output key is %s" % output)

                start = time.time()
                res = flconn.call_dag(dag_name, arg_map, True, CROSS, output, cid)
                end = time.time()
                total_time_per_loop += (end - start)
                all_times.append((end - start))
                print('Result is: %s' % res)
            total_time.append(total_time_per_loop)
        print('total time is %s' % total_time)
        print('average time per loop is %s' % (sum(total_time)/len(total_time)))
        print('zipf %f' % zipf)
        utils.print_latency_stats(all_times, 'latency')