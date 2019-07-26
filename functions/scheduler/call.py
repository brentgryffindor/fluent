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

    logging.info('executor thread is %s', utils._get_exec_address(ip, tid))

    sckt = pusher_cache.get(utils._get_exec_address(ip, tid))
    sckt.send(call.SerializeToString())

    executors.discard((ip, tid))

    r = GenericResponse()
    r.success = True
    r.response_id = call.resp_id

    func_call_socket.send(r.SerializeToString())


def call_dag(call, pusher_cache, dags, func_locations, key_ip_map,
             running_counts, backoff, scheduler_ip, pending_versioned_key_collection_response):
    dag, sources = dags[call.name]

    schedule = DagSchedule()
    schedule.id = str(uuid.uuid4())
    schedule.dag.CopyFrom(dag)
    schedule.consistency = call.consistency

    if call.HasField('response_address'):
        schedule.response_address = call.response_address

    # debug
    logging.info('check if call has output key')
    if call.HasField('output_key'):
        logging.info('has output key %s' % call.output_key)
        schedule.output_key = call.output_key
    else:
        logging.info('no output_key!')

    # debug
    logging.info('check if call has client id')
    if call.HasField('client_id'):
        logging.info('has client id')
        schedule.client_id = call.client_id
    else:
        logging.info('no client id!')

    full_refs = []
    locations = None

    for fname in dag.functions:
        locations = func_locations[fname]
        args = call.function_args[fname].args

        refs = list(filter(lambda arg: type(arg) == FluentReference,
                    map(lambda arg: get_serializer(arg.type).load(arg.body),
                        args)))
        full_refs.append(refs)
        #loc = _pick_node(locations, key_ip_map, refs, running_counts, backoff)
        #schedule.locations[fname] = loc[0] + ':' + str(loc[1])

        # copy over arguments into the dag schedule
        arg_list = schedule.arguments[fname]
        arg_list.args.extend(args)

    full_key_set = set(ref.key for ref in full_refs)

    executors = set(locations)
    cache_ips = [e[0] for e in executors]
    cache_ip = sys_random.choice(cache_ips)

    # issue request to cache
    logging.info('sending request to cache with ip %s' % cache_ip)
    ip = utils._get_cache_version_query_address(cache_ip)
    version_query_request = CausalSchedulerRequest()
    version_query_request.client_id = schedule.client_id
    version_query_request.scheduler_address = utils._get_scheduler_versioned_key_collection_address(scheduler_ip)
    version_query_request.keys.extend(list(full_key_set))

    sckt = pusher_cache.get(ip)
    sckt.send(version_query_request.SerializeToString())

    # a map from function name to a set of function names that trigger it
    function_trigger_map = {}
    for fname in dag.functions:
        function_trigger_map[fname] = _find_upstream(fname, dag)
    finished_functions = set()
    tid = 0
    while len(finished_functions) < len(dag.functions):
        for fname in function_trigger_map:
            if not fname in finished_functions:
                finished = True
                for trigger_function in function_trigger_map[fname]:
                    if trigger_function not in finished_functions:
                        finished = False
                if finished:
                    schedule.locations[fname] = cache_ip + ':' + str(tid%3)
                    tid += 1
                    finished_functions.add(fname)

    pending_versioned_key_collection_response[schedule.client_id] = schedule

    # send schedule
    for func in schedule.locations:
        loc = schedule.locations[func].split(':')
        ip = utils._get_queue_address(loc[0], loc[1])
        logging.info('sending schedule to ip %s for func %s' % (ip, func))
        schedule.target_function = func

        triggers = sutils._get_dag_predecessors(dag, func)
        if len(triggers) == 0:
            triggers.append('BEGIN')

        schedule.ClearField('triggers')
        schedule.triggers.extend(triggers)

        sckt = pusher_cache.get(ip)
        sckt.send(schedule.SerializeToString())

    '''for source in sources:
        trigger = DagTrigger()
        trigger.id = schedule.id
        trigger.source = 'BEGIN'
        trigger.target_function = source

        ip = sutils._get_dag_trigger_address(schedule.locations[source])
        sckt = pusher_cache.get(ip)
        sckt.send(trigger.SerializeToString())'''

    if schedule.HasField('output_key'):
        return schedule.output_key
    else:
        return schedule.id


def _pick_node(valid_executors, key_ip_map, refs):
    # Construct a map which maps from IP addresses to the number of
    # relevant arguments they have cached. For the time begin, we will
    # just pick the machine that has the most number of keys cached.
    arg_map = {}

    executors = set(valid_executors)

    cache_ips = [e[0] for e in executors]

    for ref in refs:
        if ref.key in key_ip_map:
            ips = key_ip_map[ref.key]

            for ip in ips:
                # only choose this cached node if its a valid executor for our
                # purposes
                if ip in cache_ips:
                    if ip not in arg_map:
                        arg_map[ip] = 0

                    arg_map[ip] += 1

    max_ip = None
    max_count = 0
    for ip in arg_map.keys():
        if arg_map[ip] > max_count:
            max_count = arg_map[ip]
            max_ip = ip

    return max_ip

def _find_upstream(fname, dag):
    upstream = set()
    for conn in dag.connections:
        if conn.sink == fname:
            upstream.add(conn.source)
    return upstream
