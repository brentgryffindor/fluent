#  Copyright 2018 U.C. Berkeley RISE Lab
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import logging
import sys
import time
import zmq

from anna.lattices import *
from include.functions_pb2 import *
from include.shared import *
from include.serializer import *
from include import server_utils as sutils
from . import user_library
from . import utils


def _process_args(arg_list):
    return [get_serializer(arg.type).load(arg.body) for arg in arg_list]


def exec_function(exec_socket, kvs, status, ip, tid, consistency=CROSS):
    user_lib = user_library.FluentUserLibrary(ip, tid, kvs)
    call = FunctionCall()
    call.ParseFromString(exec_socket.recv())

    fargs = _process_args(call.args)

    f = utils._retrieve_function(call.name, kvs, consistency)
    if not f:
        logging.info('Function %s not found! Putting an error.' %
                     (call.name))
        sutils.error.error = FUNC_NOT_FOUND
        result = serialize_val(('ERROR', sutils.error.SerializeToString()))
    else:
        try:
            if consistency == NORMAL:
                logging.info('entering single func normal')
                result = _exec_func_normal(kvs, f, fargs, user_lib)
                logging.info('function executed')
            else:
                logging.info('entering single func causal')
                result = _exec_single_func_causal(kvs, call.name, f, fargs)
                logging.info('function executed')
            result = serialize_val(result)
        except Exception as e:
            logging.exception('Unexpected error %s while executing function.' %
                              (str(e)))
            sutils.error.error = EXEC_ERROR
            result = serialize_val(('ERROR: ' + str(e),
                                   sutils.error.SerializeToString()))

    user_lib.close()
    if consistency == NORMAL:
        logging.info('Normal PUT')
        succeed = kvs.put(call.resp_id, LWWPairLattice(generate_timestamp(0), result))
    else:
        logging.info('Causal PUT')
        succeed = kvs.causal_put(call.resp_id, {'base' : 1}, {}, result, '0')

    logging.info('PUT done')

    if not succeed:
        logging.info('Put key %s unsuccessful' % call.resp_id)

def _exec_single_func_causal(kvs, fname, func, args):
    func_args = []
    to_resolve = []
    deserialize = {}

    # resolve any references to KVS objects
    key_index_map = {}
    for i, arg in enumerate(args):
        if isinstance(arg, FluentReference):
            to_resolve.append(arg)
            key_index_map[arg.key] = i
            deserialize[arg.key] = arg.deserialize
        func_args += (arg,)

    if len(to_resolve) > 0:
        keys = [ref.key for ref in to_resolve]
        logging.info('enter causal get')
        result = kvs.causal_get(keys, CROSS, '0', {}, False)

        while not result:
            result = kvs.causal_get(keys, CROSS, '0', {}, False)

        logging.info('causal get done')
        kv_pairs = result

        for key in kv_pairs:
            if deserialize[key]:
                func_args[key_index_map[key]] = \
                                deserialize_val(kv_pairs[key][1])
            else:
                func_args[key_index_map[key]] = kv_pairs[key][1]

    # execute the function
    logging.info('executing function')
    return  func(*tuple(func_args))


def exec_dag_function(pusher_cache, kvs, triggers, function, schedule, ip, tid):
    logging.info('conservative flag is %s' % conservative)
    user_lib = user_library.FluentUserLibrary(ip, tid, kvs)
    if schedule.consistency == NORMAL:
        _exec_dag_function_normal(pusher_cache, kvs,
                                  triggers, function, schedule, user_lib)
    else:
        # XXX TODO do we need separate user lib for causal functions?
        _exec_dag_function_causal(pusher_cache, kvs,
                                  triggers, function, schedule)

    user_lib.close()


def _exec_dag_function_normal(pusher_cache, kvs, triggers, function, schedule,
                              user_lib):
    logging.info('exec dag normal')
    fname = schedule.target_function
    fargs = list(schedule.arguments[fname].args)

    for trname in schedule.triggers:
        trigger = triggers[trname]
        fargs += list(trigger.arguments.args)

    fargs = _process_args(fargs)
    result = _exec_func_normal(kvs, function, fargs, user_lib)

    is_sink = True
    for conn in schedule.dag.connections:
        if conn.source == fname:
            is_sink = False
            new_trigger = DagTrigger()
            new_trigger.id = schedule.id
            new_trigger.target_function = conn.sink
            new_trigger.source = fname

            if type(result) != tuple:
                result = (result,)

            al = new_trigger.arguments
            al.args.extend(list(map(lambda v: serialize_val(v, None, False),
                                    result)))

            dest_ip = schedule.locations[conn.sink]
            sckt = pusher_cache.get(sutils._get_dag_trigger_address(dest_ip))
            sckt.send(new_trigger.SerializeToString())

    if is_sink:
        if schedule.HasField('output_key'):
            logging.info('DAG %s (ID %s) completed; result at %s.' %
                         (schedule.dag.name, trigger.id, schedule.output_key))
        else:
            logging.info('DAG %s (ID %s) completed; result at %s.' %
                         (schedule.dag.name, trigger.id, schedule.id))

        result = serialize_val(result)
        if schedule.HasField('response_address'):
            sckt = pusher_cache.get(schedule.response_address)
            sckt.send(result)
        else:
            lattice = LWWPairLattice(generate_timestamp(0), result)
            if schedule.HasField('output_key'):
                kvs.put(schedule.output_key, lattice)
            else:
                kvs.put(schedule.id, lattice)


def _exec_func_normal(kvs, func, args, user_lib):
    refs = list(filter(lambda a: isinstance(a, FluentReference), args))

    if refs:
        refs = _resolve_ref_normal(refs, kvs)
    end = time.time()

    func_args = (user_lib,)
    for arg in args:
        if isinstance(arg, FluentReference):
            func_args += (refs[arg.key],)
        else:
            func_args += (arg,)

    # execute the function
    res = func(*func_args)
    return res


def _resolve_ref_normal(refs, kvs):
    start = time.time()
    keys = [ref.key for ref in refs]
    keys = list(set(keys))
    kv_pairs = kvs.get(keys)

    # when chaining function executions, we must wait
    num_nulls = len(list(filter(lambda a: not a, kv_pairs.values())))
    while num_nulls > 0:
        kv_pairs = kvs.get(keys)

    for ref in refs:
        if ref.deserialize and isinstance(kv_pairs[ref.key], LWWPairLattice):
            kv_pairs[ref.key] = deserialize_val(kv_pairs[ref.key].reveal()[1])

    end = time.time()
    return kv_pairs


def _exec_dag_function_causal(pusher_cache, kvs, triggers, function, schedule):
    logging.info('exec dag causal')
    fname = schedule.target_function

    fargs = list(schedule.arguments[fname].args)

    dependencies = {}

    for trname in schedule.triggers:
        trigger = triggers[trname]
        fargs += list(trigger.arguments.args)

        # combine dependencies from previous func
        for dep in trigger.dependencies:
            if dep.key in dependencies:
                dependencies[dep.key] = sutils._merge_vector_clock(
                      dependencies[dep.key], dep.vector_clock)
            else:
                dependencies[dep.key] = dep.vector_clock

    fargs = _process_args(fargs)

    kv_pairs = {}
    result = _exec_func_causal(kvs, function, fargs, kv_pairs,
                               schedule, dependencies, _is_sink(fname, schedule.dag.connections))
    logging.info('finish executing function')

    for key in kv_pairs:
        if key in dependencies:
            dependencies[key] = sutils._merge_vector_clock(dependencies[key],
                                                    kv_pairs[key][0])
        else:
            dependencies[key] = kv_pairs[key][0]

    is_sink = True
    for conn in schedule.dag.connections:
        if conn.source == fname:
            is_sink = False
            new_trigger = DagTrigger()
            new_trigger.id = schedule.id
            new_trigger.target_function = conn.sink
            new_trigger.source = fname

            if type(result) != tuple:
                result = (result,)

            al = new_trigger.arguments
            al.args.extend(list(map(lambda v: serialize_val(v, None, False),
                                    result)))

            for key in dependencies:
                dep = new_trigger.dependencies.add()
                dep.key = key
                dep.vector_clock.update(dependencies[key])

            dest_ip = schedule.locations[conn.sink]
            sckt = pusher_cache.get(sutils._get_dag_trigger_address(dest_ip))
            sckt.send(new_trigger.SerializeToString())

    if is_sink:
        logging.info('DAG %s (ID %s) completed in causal mode; result at %s.' %
                (schedule.dag.name, schedule.id, schedule.output_key))

        vector_clock = {}
        if schedule.output_key in dependencies:
            if schedule.client_id in dependencies[schedule.output_key]:
                dependencies[schedule.output_key][schedule.client_id] += 1
            else:
                dependencies[schedule.output_key][schedule.client_id] = 1
            vector_clock.update(dependencies[schedule.output_key])
            del dependencies[schedule.output_key]
        else:
            vector_clock = {schedule.client_id : 1}

        succeed = kvs.causal_put(schedule.output_key,
                                 vector_clock, dependencies,
                                 serialize_val(result), schedule.client_id)
        while not succeed:
            kvs.causal_put(schedule.output_key, vector_clock,
                           dependencies, serialize_val(result), schedule.client_id)

def _exec_func_causal(kvs, func, args, kv_pairs,
                      schedule, dependencies, sink):
    logging.info('exec func causal')
    func_args = []
    to_resolve = []
    deserialize = {}

    # resolve any references to KVS objects
    key_index_map = {}
    for i, arg in enumerate(args):
        if isinstance(arg, FluentReference):
            to_resolve.append(arg)
            key_index_map[arg.key] = i
            deserialize[arg.key] = arg.deserialize
        func_args += (arg,)

    if len(to_resolve) > 0:
        error = _resolve_ref_causal(to_resolve, kvs, kv_pairs,
                            schedule, dependencies, sink)
        logging.info('Done resolving reference')

        if error == KEY_DNE:
            return None

        logging.info('swapping args and deserializing')
        for key in kv_pairs:
            if deserialize[key]:
                logging.info('deserializing key %s' % key)
                func_args[key_index_map[key]] = \
                                deserialize_val(kv_pairs[key][1])
                logging.info('value is %s' % deserialize_val(kv_pairs[key][1]))
            else:
                logging.info('no deserialization of key %s' % key)
                func_args[key_index_map[key]] = kv_pairs[key][1].decode('ascii')
                logging.info('value is %s' % kv_pairs[key][1].decode('ascii'))

    # execute the function
    for f_arg in func_args:
        logging.info('argument is %s' % f_arg)
    logging.info('executing function')
    return func(*tuple(func_args))

def _resolve_ref_causal(refs, kvs, kv_pairs, schedule, dependencies, sink):
    logging.info('resolve ref causal')
    keys = [ref.key for ref in refs]
    result = kvs.causal_get(keys, schedule.consistency, schedule.client_id, dependencies, sink)
    while not result:
        result = kvs.causal_get(keys, schedule.consistency, schedule.client_id, dependencies, sink)
    logging.info('causal GET done')

    kv_pairs.update(result)
    return NO_ERROR

def _is_sink(fname, connections):
    is_sink = True
    for conn in connections:
        if conn.source == fname:
            is_sink = False
    return is_sink

