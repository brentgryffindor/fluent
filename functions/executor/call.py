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
import random

from anna.lattices import *
from include.functions_pb2 import *
from include.shared import *
from include.serializer import *
from include import server_utils as sutils
#from . import user_library
from . import utils


def _process_args(arg_list):
    return [get_serializer(arg.type).load(arg.body) for arg in arg_list]


def exec_function(exec_socket, kvs, status, ip, tid, consistency=CROSS):
    #user_lib = user_library.FluentUserLibrary(ip, tid, kvs)
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
                #logging.info('entering single func normal')
                result = _exec_func_normal(kvs, f, fargs)
                #logging.info('function executed')
            else:
                #logging.info('entering single func causal')
                result = _exec_single_func_causal(kvs, call.name, f, fargs)
                #logging.info('function executed')
            result = serialize_val(result)
        except Exception as e:
            logging.exception('Unexpected error %s while executing function.' %
                              (str(e)))
            sutils.error.error = EXEC_ERROR
            result = serialize_val(('ERROR: ' + str(e),
                                   sutils.error.SerializeToString()))

    #user_lib.close()
    if consistency == NORMAL:
        #logging.info('Normal PUT')
        succeed = kvs.put(call.resp_id, LWWPairLattice(generate_timestamp(0), result))
    else:
        #logging.info('Causal PUT')
        succeed = kvs.causal_put(call.resp_id, {'base' : 1}, {}, [result], '0')

    #logging.info('PUT done')

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
        #logging.info('enter causal get')
        kv_pairs = {}
        result = kvs.causal_get(keys, keys,
                                [], [],
                                CROSS, '0', fname, {}, False, kv_pairs)

        while not result:
            result = kvs.causal_get(keys, keys,
                                [], [],
                                CROSS, '0', fname, {}, False, kv_pairs)

        #logging.info('causal get done')

        for key in kv_pairs:
            if deserialize[key]:
                func_args[key_index_map[key]] = \
                                deserialize_val(kv_pairs[key][1])
            else:
                func_args[key_index_map[key]] = kv_pairs[key][1]

    # execute the function
    #logging.info('executing function')
    return  func(*tuple(func_args))


def exec_dag_function(pusher_cache, kvs, triggers, function, schedule, ip,
                      tid, cache, function_result_cache, executor_id, logical_clock, write_cache, conservative=False):
    #logging.info('conservative flag is %s' % conservative)
    #user_lib = user_library.FluentUserLibrary(ip, tid, kvs)
    if schedule.consistency == NORMAL:
        _exec_dag_function_normal(pusher_cache, kvs,
                                  triggers, function, schedule)
    else:
        # XXX TODO do we need separate user lib for causal functions?
        _exec_dag_function_causal(pusher_cache, kvs,
                                  triggers, function, schedule, conservative, cache, function_result_cache, executor_id, logical_clock, write_cache)

    #user_lib.close()


def _exec_dag_function_normal(pusher_cache, kvs, triggers, function, schedule):
    #logging.info('exec dag normal')
    fname = schedule.target_function
    fargs = list(schedule.arguments[fname].args)

    for trname in schedule.triggers:
        trigger = triggers[trname]
        fargs += list(trigger.arguments.args)

    fargs = _process_args(fargs)
    result = _exec_func_normal(kvs, function, fargs)

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


def _exec_func_normal(kvs, func, args):
    refs = list(filter(lambda a: isinstance(a, FluentReference), args))

    if refs:
        refs = _resolve_ref_normal(refs, kvs)
    end = time.time()

    func_args = ()
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


def _exec_dag_function_causal(pusher_cache, kvs, triggers, function, schedule, conservative, cache, function_result_cache, executor_id, logical_clock, write_cache):
    #logging.info('exec dag causal')
    fname = schedule.target_function
    #logging.info('start processing client id %s function %s' % (schedule.client_id, fname))
    # first check if we need to abort
    for trname in schedule.triggers:
        trigger = triggers[trname]
        if trigger.HasField('abort') and trigger.abort:
            #logging.info('abort due to upstream')
            _abort_dag(fname, schedule, pusher_cache)
            return


    fargs = list(schedule.arguments[fname].args)

    prior_version_tuples = []
    prior_read_map = []

    dependencies = {}

    cached = [True]

    for trname in schedule.triggers:
        trigger = triggers[trname]
        fargs += list(trigger.arguments.args)
        if not conservative:
            # combine prior_version_tuples
            prior_version_tuples += list(trigger.prior_version_tuples)
            # combine prior_read_map
            prior_read_map += list(trigger.prior_read_map)

        # combine dependencies from previous func
        for dep in trigger.dependencies:
            if dep.key in dependencies:
                dependencies[dep.key] = sutils._merge_vector_clock(
                      dependencies[dep.key], dep.vector_clock)
            else:
                dependencies[dep.key] = dep.vector_clock

        if conservative and trigger.HasField('invalidate') and trigger.invalidate:
            # if any upstream function cache is invalidated, we have to invalidate this function cache as well
            #logging.info('invalidate function result cache due to upstream invalidation')
            cached[0] = False

    if not conservative and len(schedule.triggers) > 1 and _executor_check_parallel_flow(prior_version_tuples, prior_read_map):
        #logging.info('abort due to parallel flow check failure')
        _abort_dag(fname, schedule, pusher_cache)
        return

    fargs = _process_args(fargs)

    kv_pairs = {}
    abort = [False]
    result = _exec_func_causal(kvs, function, fargs, kv_pairs,
                               schedule, prior_version_tuples, prior_read_map, dependencies, conservative, abort, cache, function_result_cache, cached)
    #logging.info('finish executing function')

    if abort[0]:
        #logging.info('abort due to resolve ref')
        _abort_dag(fname, schedule, pusher_cache)
        return

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

            new_trigger.prior_version_tuples.extend(prior_version_tuples)
            new_trigger.prior_read_map.extend(prior_read_map)

            for key in dependencies:
                dep = new_trigger.dependencies.add()
                dep.key = key
                dep.vector_clock.update(dependencies[key])

            # if function cache is invalidated at this stage, we must pass this info to invalidate downstream as well
            if conservative and not cached[0]:
                new_trigger.invalidate = True

            dest_ip = schedule.locations[conn.sink]
            if not conservative:
                sckt = pusher_cache.get(sutils._get_dag_trigger_address(dest_ip))
            else:
                sckt = pusher_cache.get(sutils._get_dag_conservative_trigger_address(dest_ip))
            sckt.send(new_trigger.SerializeToString())

    if is_sink:
        result = [serialize_val(result)]
        if schedule.HasField('response_address'):
            #logging.info('direct response')
            sckt = pusher_cache.get(schedule.response_address)
            sckt.send(result[0])

        logical_clock[0] += 1
        vector_clock = {}
        concurrent = False
        if schedule.output_key in dependencies:
            if schedule.output_key in write_cache and (not executor_id in dependencies[schedule.output_key] or dependencies[schedule.output_key][executor_id] < write_cache[schedule.output_key][0][executor_id]):
                concurrent = True
                dependencies[schedule.output_key] = sutils._merge_vector_clock(dependencies[schedule.output_key], write_cache[schedule.output_key][0])
            dependencies[schedule.output_key][executor_id] = logical_clock[0]
            vector_clock.update(dependencies[schedule.output_key])
            del dependencies[schedule.output_key]
        else:
            if schedule.output_key in write_cache:
                concurrent = True
                vector_clock.update(write_cache[schedule.output_key][0])
            vector_clock[executor_id] = logical_clock[0]
            vector_clock['base'] = 1
            #logging.error('key write not in read set!')

        if concurrent:
            # merge dependency
            for dep_key in write_cache[schedule.output_key][1]:
                if dep_key in dependencies:
                    dependencies[dep_key] = sutils._merge_vector_clock(dependencies[dep_key], write_cache[schedule.output_key][1][dep_key])
                else:
                    dependencies[dep_key] = write_cache[schedule.output_key][1][dep_key]
            # merge payload
            result.extend(write_cache[schedule.output_key][2])

        # force concurrent...
        #if 'base' in vector_clock:
        #    del vector_clock['base']

        #logging.info('issuing causal put of key %s' % schedule.output_key)
        result = [serialize_val('0'.zfill(2097152))]
        succeed = kvs.causal_put(schedule.output_key,
                                 vector_clock, dependencies,
                                 result, schedule.client_id)
        #logging.info('finish causal put of key %s' % schedule.output_key)

        while not succeed:
            #logging.info('retrying causal put')
            kvs.causal_put(schedule.output_key, vector_clock,
                           dependencies, result, schedule.client_id)

        # update write cache
        write_cache[schedule.output_key] = (vector_clock, dependencies, result)

        # if optimistic protocol, issue requests to GC the version store and schedule
        if not conservative:
            #logging.info('GCing version store and schedule')
            #logging.info('GCing schedule only for benchmark')
            observed_cache_ip = set()
            # IMPORTANT: we disable GC of version store for benchmark purpose
            for fname in schedule.locations:
                cache_ip = schedule.locations[fname].split(':')[0]
                if cache_ip not in observed_cache_ip:
                    observed_cache_ip.add(cache_ip)
                    gc_addr = utils._get_cache_gc_address(cache_ip)
                    #logging.info('cache GC address is %s' % gc_addr)
                    sckt = pusher_cache.get(gc_addr)
                    sckt.send_string(schedule.client_id)
                #logging.info('sending gc request for function %s cid %s' % (fname, schedule.client_id))
                gc_req = ExecutorGCRequest()
                gc_req.function_name = fname
                gc_req.schedule_id = schedule.id
                gc_req.client_id = schedule.client_id
                gc_addr = utils._get_schedule_gc_address(schedule.locations[fname])
                #logging.info('schedule GC address is %s' % gc_addr)
                sckt = pusher_cache.get(gc_addr)
                sckt.send(gc_req.SerializeToString())
    # GC function cache
    if conservative and schedule.target_function in function_result_cache and schedule.client_id in function_result_cache[schedule.target_function]:
        del function_result_cache[schedule.target_function][schedule.client_id]
        if len(function_result_cache[schedule.target_function]) == 0:
            del function_result_cache[schedule.target_function]

def _exec_func_causal(kvs, func, args, kv_pairs,
                      schedule, prior_version_tuples, prior_read_map, dependencies, conservative, abort, cache, function_result_cache, cached):
    #logging.info('exec func causal')
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

    keys = set(ref.key for ref in to_resolve)
    key_vc_map = {}

    if len(to_resolve) > 0:
        error = _resolve_ref_causal(keys, kvs, kv_pairs,
                            schedule, prior_version_tuples, prior_read_map, dependencies, conservative, cache, function_result_cache, cached)
        #logging.info('Done resolving reference')
        # check if it is conservative protocol and cached
        if conservative and cached[0]:
            #logging.info('function result cache hit')
            # update dependency before returning
            for key in keys:
                if key in dependencies:
                    dependencies[key] = sutils._merge_vector_clock(dependencies[key],
                                                            function_result_cache[schedule.target_function][schedule.client_id][0][key])
                else:
                    dependencies[key] = function_result_cache[schedule.target_function][schedule.client_id][0][key]
            return function_result_cache[schedule.target_function][schedule.client_id][1]

        #logging.info('function result cache miss')

        if error == KEY_DNE or error == ABORT:
            abort[0] = True
            return None

        #logging.info('swapping args and deserializing')
        for key in kv_pairs:
            logging.info('cache miss for key %s' % key)
            logging.info('key size is %s' % len(kv_pairs[key][1]))
            if deserialize[key]:
                #logging.info('deserializing key %s' % key)
                func_args[key_index_map[key]] = \
                                deserialize_val(kv_pairs[key][1])
                #logging.info('value is %s' % deserialize_val(kv_pairs[key][1]))
            else:
                #logging.info('no deserialization of key %s' % key)
                func_args[key_index_map[key]] = kv_pairs[key][1]
                #logging.info('value is %s' % kv_pairs[key][1].decode('ascii'))
            keys.remove(key)
            # update dependency
            if key in dependencies:
                dependencies[key] = sutils._merge_vector_clock(dependencies[key],
                                                        kv_pairs[key][0])
            else:
                dependencies[key] = kv_pairs[key][0]
            key_vc_map[key] = kv_pairs[key][0]
        for key in keys:
            logging.info('cache hit for key %s' % key)
            # these are keys that are cached
            # we first update the prior_read_map since cached keys are not returned by the cache
            vk = VersionedKey()
            vk.key = key
            vk.vector_clock.update(cache[key][0])
            prior_read_map.extend([vk])
            
            func_args[key_index_map[key]] = cache[key][1]
            logging.info('key size is %s' % len(cache[key][1]))
            # update dependency
            if key in dependencies:
                dependencies[key] = sutils._merge_vector_clock(dependencies[key],
                                                        cache[key][0])
            else:
                dependencies[key] = cache[key][0]
            key_vc_map[key] = cache[key][0]

    # execute the function
    #for f_arg in func_args:
    #    logging.info('argument is %s' % f_arg)
    #logging.info('executing function')
    result = func(*tuple(func_args))
    # if optimistic protocol, cache result
    if not conservative:
        if schedule.target_function not in function_result_cache:
            function_result_cache[schedule.target_function] = {}
        # perform result caching
        #logging.info('caching function result for function %s cid %s' % (schedule.target_function, schedule.client_id))
        function_result_cache[schedule.target_function][schedule.client_id] = (key_vc_map, result)

    return result

def _resolve_ref_causal(keys, kvs, kv_pairs, schedule, prior_version_tuples, prior_read_map, dependencies, conservative, cache, function_result_cache, cached):
    #logging.info('resolve ref causal')
    full_read_set = schedule.full_read_set
    result = kvs.causal_get(keys, full_read_set,
                            prior_version_tuples, prior_read_map,
                            schedule.consistency, schedule.client_id, schedule.target_function, dependencies, conservative, kv_pairs, cache, function_result_cache, cached)
    while result is None:
        #logging.info('result is None!')
        result = kvs.causal_get(keys, full_read_set,
                                prior_version_tuples, prior_read_map,
                                schedule.consistency, schedule.client_id, schedule.target_function, dependencies, conservative, kv_pairs, cache, function_result_cache, cached)
    #logging.info('causal GET done')

    if result == KEY_DNE or result == ABORT:
        #logging.info('dne or abort')
        return result

    if not conservative:
        prior_version_tuples.extend(result[0])
        prior_read_map.extend(result[1])
        # debug print
        #for prior_version_tuple in prior_version_tuples:
        #    logging.info('function name is %s' % prior_version_tuple.function_name)
        #    logging.info('key is %s' % prior_version_tuple.versioned_key.key)

    return NO_ERROR

def _executor_check_parallel_flow(prior_version_tuples, prior_read_map):
    for versioned_key_read in prior_read_map:
        for prior_version_tuple in prior_version_tuples:
            if versioned_key_read.key == prior_version_tuple.versioned_key.key and sutils._compare_vector_clock(versioned_key_read.vector_clock, prior_version_tuple.versioned_key.vector_clock) != sutils.CausalComp.GreaterOrEqual:
                return True
    return False

def _abort_dag(fname, schedule, pusher_cache):
    # send abort info to downstream
    for conn in schedule.dag.connections:
        if conn.source == fname:
            new_trigger = DagTrigger()
            new_trigger.id = schedule.id
            new_trigger.target_function = conn.sink
            new_trigger.source = fname
            new_trigger.abort = True

            dest_ip = schedule.locations[conn.sink]
            sckt = pusher_cache.get(sutils._get_dag_trigger_address(dest_ip))
            sckt.send(new_trigger.SerializeToString())