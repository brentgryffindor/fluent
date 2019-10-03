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

zipf = 0
base = 0
sum_probs = {}
sum_probs[0] = 0.0

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

        if num_parents == 0:
            to_generate = 2
        else:
            to_generate = 1

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

def run(flconn, kvs, mode, segment, params):
    dag_name = 'causal_test'

    fanout = 9

    functions = []
    connections = []
    for i in range(fanout):
        functions.append('strmnp_leaf' + str(i+1))
        connections.append(('strmnp_leaf' + str(i+1), 'strmnp_root'))
    functions.append('strmnp_root')

    logging.info(functions)
    logging.info(connections)

    #functions = ['strmnp1', 'strmnp2', 'strmnp3']
    #connections = [('strmnp1', 'strmnp3'), ('strmnp2', 'strmnp3')]
    total_num_keys = 1000000

    if mode == 'create':
        #print("Creating functions and DAG")
        logging.info("Creating functions and DAG")
        ### DEFINE AND REGISTER FUNCTIONS ###
        def strmnp_leaf(a,b):
            import time
            time.sleep(0.05)
            return '0'.zfill(8)

        def strmnp_root(a,b,c,d,e,f,g,h,i,j):
            return '0'.zfill(8)

        cloud_funcs = []

        for i in range(fanout):
            cloud_strmnp = flconn.register(strmnp_leaf, ('strmnp_leaf' + str(i+1)))
            if cloud_strmnp:
                logging.info('Successfully registered %s.' % ('strmnp_leaf' + str(i+1)))
            else:
                logging.info('Error registering %s.' % ('strmnp_leaf' + str(i+1)))
                sys.exit(1)
            cloud_funcs.append(cloud_strmnp)

        root_funcs = flconn.register(strmnp_root, ('strmnp_root'))

        '''cloud_strmnp1 = flconn.register(strmnp1, 'strmnp1')
        cloud_strmnp2 = flconn.register(strmnp1, 'strmnp2')
        cloud_strmnp3 = flconn.register(strmnp2, 'strmnp3')

        if cloud_strmnp1 and cloud_strmnp2 and cloud_strmnp3:
            logging.info('Successfully registered the string manipulation function.')
        else:
            logging.info('Error registering functions.')
            sys.exit(1)'''

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

        for i in range(fanout):
            strmnp_test = cloud_funcs[i](*refs).get()
            if strmnp_test != '0'.zfill(8):
                logging.error('Unexpected result from strmnp(): %s' % (str(strmnp_test)))
                sys.exit(1)

        '''strmnp_test1 = cloud_strmnp1(*refs).get()
        strmnp_test2 = cloud_strmnp2(*refs).get()'''


        refs = ()
        for _ in range(10):
            val = '0'.zfill(8)
            ccv = CrossCausalValue()
            ccv.vector_clock['base'] = 1
            ccv.values.extend([serialize_val(val)])
            k = str(uuid.uuid4())
            print("key name is ", k)
            kvs.put(k, ccv)

            refs += (FluentReference(k, True, CROSSCAUSAL),)
        root_test = root_funcs(*refs).get()

        if root_test != '0'.zfill(8):
            logging.error('Unexpected result from strmnp(): %s' % (str(root_test)))
            sys.exit(1)

        '''if strmnp_test1 != '0'.zfill(8) or strmnp_test2 != '0'.zfill(8) or strmnp_test3 != '0'.zfill(8):
            logging.error('Unexpected result from strmnp(v1, v2, v3): %s %s %s' % (str(strmnp_test1), str(strmnp_test2), str(strmnp_test3)))
            sys.exit(1)'''

        #print('Successfully tested functions!')
        logging.info('Successfully tested functions!')

        ### CREATE DAG ###

        success, error = flconn.register_dag(dag_name, functions, connections)

        if not success:
            logging.info('Failed to register DAG: %s' % (ErrorType.Name(error)))
            sys.exit(1)

        #print("Successfully created the DAG")
        logging.info("Successfully created the DAG")
        return []

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

    elif mode == 'zipf':
        logging.info("Creating Probability Table")
        ### CREATE ZIPF TABLE###
        params[0] = 1.0
        params[1] = get_base(total_num_keys, params[0])
        for i in range(1, total_num_keys+1):
            params[2][i] = params[2][i - 1] + (params[1] / np.power(float(i), params[0]))

        logging.info("Created Probability Table with zipf %f" % params[0])
        return []

    elif mode == 'run':
        ### RUN DAG ###
        #print('Running DAG')
        logging.info('Running DAG')
        zipf = params[0]
        base = params[1]
        sum_probs = params[2]

        #request_num = 500

        total_time = []

        all_times = []

        read_map = {}
        write_map = {}

        for i in range(15*segment, 15*segment + 15):
            cid = str(i).zfill(3)

            logging.info("running client %s" % cid)

            arg_map, read_set = generate_arg_map(functions, connections, total_num_keys, base, sum_probs)

            for func in arg_map:
                logging.info("function is %s" % func)
                for ref in arg_map[func]:
                    if ref.key not in read_map:
                        read_map[ref.key] = 0
                    read_map[ref.key] += 1
                    #logging.info("key of reference is %s" % ref.key)

            '''for key in read_set:
                logging.info("read set contains %s" % key)'''

            output = random.choice(read_set)
            if output not in write_map:
                write_map[output] = 0
            write_map[output] += 1
            #print("Output key is %s" % output)

            start = time.time()
            scheduler_time = flconn.call_dag(dag_name, arg_map, True, CROSS, output, cid)
            end = time.time()
            all_times.append((end - start))
            #all_times.append(scheduler_time)
            #print('Result is: %s' % res)
        return all_times
        #print('zipf %f' % zipf)
        #utils.print_latency_stats(all_times, 'latency')
        #print('read map size is %d' % len(read_map))
        #print(sorted(read_map.items(), key=lambda x: x[1], reverse=True))
        #print(sum(read_map.values()))
        #print('write map size is %d' % len(write_map))
        #print(sorted(write_map.items(), key=lambda x: x[1], reverse=True))
        #print(sum(write_map.values()))