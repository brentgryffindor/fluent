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
import random
import uuid
import time
import zmq

from include.functions_pb2 import *
from include.serializer import *
from include import server_utils as sutils
from include.shared import *
from . import utils

sys_random = random.SystemRandom()


def call_function(func_call_socket, pusher_cache, executors, key_ip_map,
                  running_counts, backoff):

    call = FunctionCall()
    call.ParseFromString(func_call_socket.recv())

    if not call.HasField('resp_id'):
        call.resp_id = str(uuid.uuid4())

    refs = list(filter(lambda arg: type(arg) == FluentReference,
                       map(lambda arg: get_serializer(arg.type).load(arg.body),
                           call.args)))

    ip, tid = _pick_node(executors, key_ip_map, refs, running_counts, backoff)

    #logging.info('executor thread is %s', utils._get_exec_address(ip, tid))

    sckt = pusher_cache.get(utils._get_exec_address(ip, tid))
    sckt.send(call.SerializeToString())

    executors.discard((ip, tid))

    r = GenericResponse()
    r.success = True
    r.response_id = call.resp_id

    func_call_socket.send(r.SerializeToString())


def call_dag(call, pusher_cache, dags, func_locations, key_ip_map,
             running_counts, backoff, scheduler_ip, pending_versioned_key_collection_response, versioned_key_map):
    dag, sources = dags[call.name]

    schedule = DagSchedule()
    schedule.id = str(uuid.uuid4())
    schedule.dag.CopyFrom(dag)
    schedule.consistency = call.consistency

    if call.HasField('response_address'):
        schedule.response_address = call.response_address

    # debug
    #logging.info('check if call has output key')
    if call.HasField('output_key'):
        #logging.info('has output key %s' % call.output_key)
        schedule.output_key = call.output_key
    #else:
    #    logging.info('no output_key!')

    # debug
    #logging.info('check if call has client id')
    if call.HasField('client_id'):
        #logging.info('has client id')
        schedule.client_id = call.client_id
    #else:
    #    logging.info('no client id!')

    if schedule.consistency == CROSS:
        # define read set and full read set
        # and set dag name
        # also initialize the versioned key map
        read_set = {}
        full_read_set = set()
        versioned_key_map[schedule.client_id] = sutils.DagConsistencyMetadata(call.name)

    chosen_node = set()

    for fname in dag.functions:
        locations = func_locations[fname].copy()
        args = call.function_args[fname].args

        refs = list(filter(lambda arg: type(arg) == FluentReference,
                    map(lambda arg: get_serializer(arg.type).load(arg.body),
                        args)))
        # remove previously selected nodes
        locations = set(filter(lambda loc: loc[0] not in chosen_node, locations))
        loc = _pick_node(locations, key_ip_map, refs, running_counts, backoff)
        chosen_node.add(loc[0])
        schedule.locations[fname] = loc[0] + ':' + str(loc[1])

        logging.info('function %s scheduled on node %s tid %d' % (fname, loc[0], loc[1]))

        # copy over arguments into the dag schedule
        arg_list = schedule.arguments[fname]
        arg_list.args.extend(args)

        # populate read set and full read set
        if schedule.consistency == CROSS:
            if len(refs) != 0:
                read_set[fname] = set(ref.key for ref in refs)
                full_read_set = full_read_set.union(read_set[fname])
                versioned_key_map[schedule.client_id].per_func_read_set[fname] = read_set[fname]
                versioned_key_map[schedule.client_id].func_location[fname] = (loc[0], loc[1])

    if schedule.consistency == CROSS:
        schedule.full_read_set.extend(full_read_set)
        versioned_key_map[schedule.client_id].schedule = schedule

    for func in schedule.locations:
        loc = schedule.locations[func].split(':')
        ip = utils._get_queue_address(loc[0], loc[1])
        schedule.target_function = func

        triggers = sutils._get_dag_predecessors(dag, func)
        if len(triggers) == 0:
            triggers.append('BEGIN')

        schedule.ClearField('triggers')
        schedule.triggers.extend(triggers)

        sckt = pusher_cache.get(ip)
        sckt.send(schedule.SerializeToString())

    for source in sources:
        trigger = DagTrigger()
        trigger.id = schedule.id
        trigger.source = 'BEGIN'
        trigger.target_function = source

        ip = sutils._get_dag_trigger_address(schedule.locations[source])
        sckt = pusher_cache.get(ip)
        sckt.send(trigger.SerializeToString())

    # if we are in causal mode, start the conservative protocol by querying the caches for key versions
    if schedule.consistency == CROSS:
        #logging.info('send scheduler version query')
        # debug
        #logging.info('client id is %s' % schedule.client_id)
        for func in schedule.locations:
            if func in read_set:
                #logging.info('function name is %s' % func)
                loc = schedule.locations[func].split(':')
                ip = utils._get_cache_version_query_address(loc[0])
                version_query_request = CausalSchedulerRequest()
                version_query_request.client_id = schedule.client_id
                version_query_request.function_name = func
                version_query_request.scheduler_address = utils._get_scheduler_versioned_key_collection_address(scheduler_ip)
                # find out which arguments are kvs references
                version_query_request.keys.extend(read_set[func])
                # populate full read set
                version_query_request.full_read_set.extend(full_read_set)

                sckt = pusher_cache.get(ip)
                sckt.send(version_query_request.SerializeToString())

                if schedule.client_id not in pending_versioned_key_collection_response:
                    pending_versioned_key_collection_response[schedule.client_id] = set((func,))
                else:
                    pending_versioned_key_collection_response[schedule.client_id].add(func)
        #logging.info('done scheduler version query')

    if schedule.HasField('output_key'):
        return schedule.output_key
    else:
        return schedule.id


def _pick_node(valid_executors, key_ip_map, refs, running_counts, backoff):
    # for benchmark, randomly pick a thread
    executors = set(valid_executors)
    return sys_random.choice(executors)

'''def _pick_node(valid_executors, key_ip_map, refs, running_counts, backoff):
    # Construct a map which maps from IP addresses to the number of
    # relevant arguments they have cached. For the time begin, we will
    # just pick the machine that has the most number of keys cached.
    arg_map = {}
    reason = ''

    executors = set(valid_executors)
    for executor in backoff:
        if len(executors) > 1:
            executors.discard(executor)

    keys = list(running_counts.keys())
    sys_random.shuffle(keys)
    for key in keys:
        if len(running_counts[key]) > 1000 and len(executors) > 1:
            executors.discard(key)

    executor_ips = [e[0] for e in executors]

    for ref in refs:
        if ref.key in key_ip_map:
            ips = key_ip_map[ref.key]

            for ip in ips:
                # only choose this cached node if its a valid executor for our
                # purposes
                if ip in executor_ips:
                    if ip not in arg_map:
                        arg_map[ip] = 0

                    arg_map[ip] += 1

    max_ip = None
    max_count = 0
    for ip in arg_map.keys():
        if arg_map[ip] > max_count:
            max_count = arg_map[ip]
            max_ip = ip

    # pick a random thead from our potential executors that is on that IP
    # address; we also route some requests to a random valid node
    if max_ip:
        candidates = list(filter(lambda e: e[0] == max_ip, executors))
        max_ip = sys_random.choice(candidates)

    # This only happens if max_ip is never set, and that means that
    # there were no machines with any of the keys cached. In this case,
    # we pick a random IP that was in the set of IPs that was running
    # most recently.
    if not max_ip or sys_random.random() < 0.20:
        max_ip = sys_random.sample(executors, 1)[0]

    if max_ip not in running_counts:
        running_counts[max_ip] = set()

    running_counts[max_ip].add(time.time())

    return max_ip'''
