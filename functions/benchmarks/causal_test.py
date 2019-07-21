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

def run(flconn, kvs, mode, sckt):
    dag_name = 'causal_test'

    if mode == 'create':
        print("Creating functions and DAG")
        logging.info("Creating functions and DAG")
        ### DEFINE AND REGISTER FUNCTIONS ###
        def strmnp(a,b,c):
            result = ''
            for i, char in enumerate(a):
                if i % 3 == 0:
                    result += a[i]
                elif i % 3 == 1:
                    result += b[i]
                else:
                    result += c[i]
            return result

        cloud_strmnp1 = flconn.register(strmnp, 'strmnp1')
        cloud_strmnp2 = flconn.register(strmnp, 'strmnp2')
        cloud_strmnp3 = flconn.register(strmnp, 'strmnp3')

        if cloud_strmnp1 and cloud_strmnp2 and cloud_strmnp3:
            logging.info('Successfully registered the string manipulation function.')
        else:
            logging.info('Error registering functions.')
            sys.exit(1)

        ### TEST REGISTERED FUNCTIONS ###
        refs = ()
        for _ in range(3):
            val = '00000'
            ccv = CrossCausalValue()
            ccv.vector_clock['base'] = 1
            ccv.values.extend([serialize_val(val)])
            k = str(uuid.uuid4())
            print("key name is ", k)
            kvs.put(k, ccv)

            refs += (FluentReference(k, True, CROSSCAUSAL),)

        strmnp_test1 = cloud_strmnp1(*refs).get()
        strmnp_test2 = cloud_strmnp2(*refs).get()
        strmnp_test3 = cloud_strmnp3(*refs).get()
        if strmnp_test1 != '00000' or strmnp_test2 != '00000' or strmnp_test3 != '00000':
            logging.error('Unexpected result from strmnp(v1, v2, v3): %s %s %s' % (str(strmnp_test1), str(strmnp_test2), str(strmnp_test3)))
            sys.exit(1)

        print('Successfully tested functions!')
        logging.info('Successfully tested functions!')

        ### CREATE DAG ###

        functions = ['strmnp1', 'strmnp2', 'strmnp3']
        connections = [('strmnp1', 'strmnp3'), ('strmnp2', 'strmnp3')]
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
        val = '00000'
        # key 'a'
        k = 'a'
        ccv = CrossCausalValue()
        ccv.vector_clock['base'] = 1
        dep = ccv.deps.add()
        dep.key = 'c'
        dep.vector_clock['base'] = 1
        ccv.values.extend([serialize_val(val)])
        kvs.put(k, ccv)

        # key 'b'
        k = 'b'
        ccv = CrossCausalValue()
        ccv.vector_clock['base'] = 1
        dep = ccv.deps.add()
        dep.key = 'd'
        dep.vector_clock['base'] = 1
        ccv.values.extend([serialize_val(val)])
        kvs.put(k, ccv)

        # key 'c'
        k = 'c'
        ccv = CrossCausalValue()
        ccv.vector_clock['base'] = 2
        ccv.values.extend([serialize_val(val)])
        kvs.put(k, ccv)

        # key 'd'
        k = 'd'
        ccv = CrossCausalValue()
        ccv.vector_clock['base'] = 1
        ccv.values.extend([serialize_val(val)])
        kvs.put(k, ccv)

        # key 'e'
        k = 'e'
        ccv = CrossCausalValue()
        ccv.vector_clock['base'] = 1
        ccv.values.extend([serialize_val(val)])
        kvs.put(k, ccv)

        # key 'f'
        k = 'f'
        ccv = CrossCausalValue()
        ccv.vector_clock['base'] = 1
        ccv.values.extend([serialize_val(val)])
        kvs.put(k, ccv)

        # key 'g'
        k = 'g'
        ccv = CrossCausalValue()
        ccv.vector_clock['base'] = 1
        ccv.values.extend([serialize_val(val)])
        kvs.put(k, ccv)

        print('Data populated')
        logging.info('Data populated')

    elif mode == 'run':
        print('Running DAG')
        logging.info('Running DAG')
        ### RUN DAG ###
        refs1 = (FluentReference('a', False, CROSSCAUSAL), FluentReference('b', False, CROSSCAUSAL), FluentReference('c', False, CROSSCAUSAL),)
        refs2 = (FluentReference('d', False, CROSSCAUSAL), FluentReference('e', False, CROSSCAUSAL), FluentReference('f', False, CROSSCAUSAL),)
        refs3 = (FluentReference('g', False, CROSSCAUSAL),)

        arg_map = { 'strmnp1' : refs1 ,
                    'strmnp2' : refs2 ,
                    'strmnp3' : refs3 }

        rid = flconn.call_dag(dag_name, arg_map, False, CROSS, 'result', 'test_cid')
        print("output key is %s" % rid)

        res = kvs.get(rid)
        while not res:
            res = kvs.get(rid)
        res = deserialize_val(res.values[0])

        print('Result is: %s' % res)
        logging.info('Result is: %s' % res)