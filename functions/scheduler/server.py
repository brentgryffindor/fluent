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
import uuid
import zmq

from anna.client import AnnaClient
from anna.zmq_util import SocketCache
from include.kvs_pb2 import *
from include.functions_pb2 import *
from include import server_utils as sutils
from include.shared import *
from include.serializer import *
from .create import *
from .call import *
from . import utils

THRESHOLD = 5  # how often metadata is updated

def scheduler(ip, mgmt_ip, route_addr):
    logging.basicConfig(filename='log_scheduler.txt', level=logging.INFO,
                        format='%(asctime)s %(message)s')

    kvs = AnnaClient(route_addr, ip)

    key_ip_map = {}
    ctx = zmq.Context(1)

    # Each dag consists of a set of functions and connections. Each one of
    # the functions is pinned to one or more nodes, which is tracked here.
    dags = {}
    thread_statuses = {}
    func_locations = {}
    running_counts = {}
    backoff = {}

    # this is a map from client id to a list of function names
    # used in conservative protocol for causal consistency
    pending_versioned_key_collection_response = {}

    # this is a map from client id to DagConsistencyMetadata
    # map<client_id, DagConsistencyMetadata>
    # used in conservative protocol for causal consistency
    versioned_key_map = {}

    connect_socket = ctx.socket(zmq.REP)
    connect_socket.bind(sutils.BIND_ADDR_TEMPLATE % (CONNECT_PORT))

    func_create_socket = ctx.socket(zmq.REP)
    func_create_socket.bind(sutils.BIND_ADDR_TEMPLATE % (FUNC_CREATE_PORT))

    func_call_socket = ctx.socket(zmq.REP)
    func_call_socket.bind(sutils.BIND_ADDR_TEMPLATE % (FUNC_CALL_PORT))

    dag_create_socket = ctx.socket(zmq.REP)
    dag_create_socket.bind(sutils.BIND_ADDR_TEMPLATE % (DAG_CREATE_PORT))

    dag_call_socket = ctx.socket(zmq.REP)
    dag_call_socket.bind(sutils.BIND_ADDR_TEMPLATE % (DAG_CALL_PORT))

    dag_delete_socket = ctx.socket(zmq.REP)
    dag_delete_socket.bind(sutils.BIND_ADDR_TEMPLATE % (DAG_DELETE_PORT))

    list_socket = ctx.socket(zmq.REP)
    list_socket.bind(sutils.BIND_ADDR_TEMPLATE % (LIST_PORT))

    exec_status_socket = ctx.socket(zmq.PULL)
    exec_status_socket.bind(sutils.BIND_ADDR_TEMPLATE % (sutils.STATUS_PORT))

    sched_update_socket = ctx.socket(zmq.PULL)
    sched_update_socket.bind(sutils.BIND_ADDR_TEMPLATE %
                             (sutils.SCHED_UPDATE_PORT))

    backoff_socket = ctx.socket(zmq.PULL)
    backoff_socket.bind(sutils.BIND_ADDR_TEMPLATE % (sutils.BACKOFF_PORT))

    pin_accept_socket = ctx.socket(zmq.PULL)
    pin_accept_socket.setsockopt(zmq.RCVTIMEO, 500)
    pin_accept_socket.bind(sutils.BIND_ADDR_TEMPLATE %
                           (sutils.PIN_ACCEPT_PORT))

    versioned_key_collection_socket = ctx.socket(zmq.PULL)
    versioned_key_collection_socket.bind(sutils.BIND_ADDR_TEMPLATE % (utils.SCHEDULERS_VERSIONED_KEY_COLLECTION_PORT))

    requestor_cache = SocketCache(ctx, zmq.REQ)
    pusher_cache = SocketCache(ctx, zmq.PUSH)

    poller = zmq.Poller()
    poller.register(connect_socket, zmq.POLLIN)
    poller.register(func_create_socket, zmq.POLLIN)
    poller.register(func_call_socket, zmq.POLLIN)
    poller.register(dag_create_socket, zmq.POLLIN)
    poller.register(dag_call_socket, zmq.POLLIN)
    poller.register(dag_delete_socket, zmq.POLLIN)
    poller.register(list_socket, zmq.POLLIN)
    poller.register(exec_status_socket, zmq.POLLIN)
    poller.register(sched_update_socket, zmq.POLLIN)
    poller.register(backoff_socket, zmq.POLLIN)
    poller.register(versioned_key_collection_socket, zmq.POLLIN)

    executors = set()
    schedulers = _update_cluster_state(requestor_cache, mgmt_ip, executors,
                                       key_ip_map, kvs)

    # track how often each DAG function is called
    call_frequency = {}

    start = time.time()

    while True:
        socks = dict(poller.poll(timeout=1000))

        if connect_socket in socks and socks[connect_socket] == zmq.POLLIN:
            msg = connect_socket.recv_string()
            connect_socket.send_string(route_addr)

        if (func_create_socket in socks and
                socks[func_create_socket] == zmq.POLLIN):
            create_func(func_create_socket, kvs)

        if func_call_socket in socks and socks[func_call_socket] == zmq.POLLIN:
            call_function(func_call_socket, pusher_cache, executors,
                          key_ip_map, running_counts, backoff)

        if (dag_create_socket in socks and socks[dag_create_socket]
                == zmq.POLLIN):
            create_dag(dag_create_socket, pusher_cache, kvs, executors, dags,
                       ip, pin_accept_socket, func_locations, call_frequency)

        if dag_call_socket in socks and socks[dag_call_socket] == zmq.POLLIN:
            call = DagCall()
            call.ParseFromString(dag_call_socket.recv())

            if call.name not in dags:
                resp = GenericResponse()
                resp.success = False
                resp.error = NO_SUCH_DAG

                dag_call_socket.send(resp.SerializeToString())
                continue

            exec_id = generate_timestamp(0)

            dag = dags[call.name]
            for fname in dag[0].functions:
                call_frequency[fname] += 1

            rid = call_dag(call, pusher_cache, dags, func_locations,
                           key_ip_map, running_counts, backoff, ip, pending_versioned_key_collection_response, versioned_key_map)

            resp = GenericResponse()
            resp.success = True
            resp.response_id = rid
            dag_call_socket.send(resp.SerializeToString())

        if (dag_delete_socket in socks and socks[dag_delete_socket] ==
                zmq.POLLIN):
            delete_dag(dag_delete_socket, pusher_cache, dags, func_locations,
                       call_frequency, executors)

        if list_socket in socks and socks[list_socket] == zmq.POLLIN:
            msg = list_socket.recv_string()
            prefix = msg if msg else ''

            resp = FunctionList()
            resp.names.extend(utils._get_func_list(kvs, prefix))

            list_socket.send(resp.SerializeToString())

        if exec_status_socket in socks and socks[exec_status_socket] == \
                zmq.POLLIN:
            status = ThreadStatus()
            status.ParseFromString(exec_status_socket.recv())

            key = (status.ip, status.tid)
            logging.info('Received status update from executor %s:%d.' %
                         (key[0], int(key[1])))

            # this means that this node is currently departing, so we remove it
            # from all of our metadata tracking
            if not status.running:
                if key in thread_statuses:
                    old_status = thread_statuses[key]
                    del thread_statuses[key]

                    for fname in old_status.functions:
                        func_locations[fname].discard((old_status.ip,
                                                       old_status.tid))

                executors.discard(key)
                continue

            if key not in executors:
                executors.add(key)

            if key in thread_statuses and thread_statuses[key] != status:
                # remove all the old function locations, and all the new ones
                # -- there will probably be a large overlap, but this shouldn't
                # be much different than calculating two different set
                # differences anyway
                for func in thread_statuses[key].functions:
                    if func in func_locations:
                        func_locations[func].discard(key)

            thread_statuses[key] = status
            for func in status.functions:
                if func not in func_locations:
                    func_locations[func] = set()

                func_locations[func].add(key)

        if sched_update_socket in socks and socks[sched_update_socket] == \
                zmq.POLLIN:
            status = SchedulerStatus()
            status.ParseFromString(sched_update_socket.recv())

            # retrieve any DAG that some other scheduler knows about that we do
            # not yet know about
            for dname in status.dags:
                if dname not in dags:
                    payload = kvs.get(dname)
                    while not payload:
                        payload = kvs.get(dname)
                    dag = Dag()
                    dag.ParseFromString(payload.reveal()[1])

                    dags[dag.name] = (dag, utils._find_dag_source(dag))

                    for fname in dag.functions:
                        if fname not in call_frequency:
                            call_frequency[fname] = 0

                        if fname not in func_locations:
                            func_locations[fname] = set()

            for floc in status.func_locations:
                key = (floc.ip, floc.tid)
                fname = floc.name

                if fname not in func_locations:
                    func_locations[fname] = set()

                func_locations[fname].add(key)

        if backoff_socket in socks and socks[backoff_socket] == zmq.POLLIN:
            msg = backoff_socket.recv_string()
            splits = msg.split(':')
            node, tid = splits[0], int(splits[1])

            backoff[(node, tid)] = time.time()

        if versioned_key_collection_socket in socks and socks[versioned_key_collection_socket] == zmq.POLLIN:
            #TODO: update pending map and versioned key map. if collected all, run conservative protocol
            response = CausalSchedulerResponse()
            response.ParseFromString(versioned_key_collection_socket.recv())
            if response.succeed == False:
                # TODO: handle fail
                xxx
            else:
                if response.client_id in pending_versioned_key_collection_response:
                    pending_versioned_key_collection_response[response.client_id].remove(response.function_name)
                    # update per_func_versioned_key_chain
                    versioned_key_map[response.client_id].per_func_versioned_key_chain[response.function_name] = {}
                    for head_key in response.version_chain:
                        versioned_key_map[response.client_id].per_func_versioned_key_chain[response.function_name][head_key] = {}
                        for vk in response.version_chain[head_key].versioned_keys:
                            versioned_key_map[response.client_id].per_func_versioned_key_chain[response.function_name][head_key][vk.key] = vk.vector_clock
                            # also, update global_causal_cut
                            if vk.key not in versioned_key_map[response.client_id].global_causal_cut:
                                versioned_key_map[response.client_id].global_causal_cut[vk.key] = vk.vector_clock
                            else:
                                versioned_key_map[response.client_id].global_causal_cut[vk.key] = sutils._merge_vector_clock(versioned_key_map[response.client_id].global_causal_cut[vk.key], vk.vector_clock)
                            # also, update global causal frontier
                            if vk.key not in versioned_key_map[response.client_id].global_causal_frontier:
                                versioned_key_map[response.client_id].global_causal_frontier[vk.key] = [(vk.vector_clock, TODO: CACHE_ADDR, response.function_name)]
                            else:
                                to_remove = []
                                dominated = False
                                for tp in versioned_key_map[response.client_id].global_causal_frontier[vk.key]:
                                    if sutils._compare_vector_clock(tp[0], vk.vector_clock) == CausalComp.Less:
                                        to_remove.append(tp)
                                    elif sutils._compare_vector_clock(tp[0], vk.vector_clock) == CausalComp.GreaterOrEqual:
                                        dominated = True
                                        break
                                if dominated:
                                    continue
                                for tp in to_remove:
                                    versioned_key_map[response.client_id].global_causal_frontier[vk.key].remove(tp)
                                versioned_key_map[response.client_id].global_causal_frontier[vk.key].append((vk.vector_clock, TODO: CACHE_ADDR, response.function_name))

                    if len(pending_versioned_key_collection_response[response.client_id]) == 0:
                        # trigger conservative protocol
                        # TODO: refactor to a function
                        dag_name = versioned_key_map[response.client_id].dag_name
                        # a map from function name to accumulated version lowerbound
                        prior_per_func_causal_lowerbound_map = {}
                        # a map from function name to prior key versions read
                        prior_per_func_read_map = {}
                        # a map from function name to a set of function names that trigger it
                        function_trigger_map = {}
                        for fname in dags[dag_name][0].functions:
                            function_trigger_map[fname] = _find_upstream(fname, dags[dag_name][0])

                        finished_functions = set()
                        if _simulate_optimistic_protocol(versioned_key_map, response.client_id, finished_functions, dags, function_trigger_map, prior_per_func_causal_lowerbound_map, prior_per_func_read_map):
                            # the protocol aborted, so we need to do conservative protocol
                            




        # periodically clean up the running counts map
        for executor in running_counts:
            call_times = running_counts[executor]
            new_set = set()
            for ts in call_times:
                if time.time() - ts < 2.5:
                    new_set.add(ts)

            running_counts[executor] = new_set

        remove_set = set()
        for executor in backoff:
            if time.time() - backoff[executor] > 5:
                remove_set.add(executor)

        for executor in remove_set:
            del backoff[executor]

        end = time.time()
        if end - start > THRESHOLD:
            schedulers = _update_cluster_state(requestor_cache, mgmt_ip,
                                               executors, key_ip_map, kvs)

            status = SchedulerStatus()
            for name in dags.keys():
                status.dags.append(name)

            for fname in func_locations:
                for loc in func_locations[fname]:
                    floc = status.func_locations.add()
                    floc.name = fname
                    floc.ip = loc[0]
                    floc.tid = loc[1]

            msg = status.SerializeToString()

            for sched_ip in schedulers:
                if sched_ip != ip:
                    sckt = pusher_cache.get(utils._get_scheduler_update_address
                                            (sched_ip))
                    sckt.send(msg)

            stats = ExecutorStatistics()
            for fname in call_frequency:
                fstats = stats.statistics.add()
                fstats.fname = fname
                fstats.call_count = call_frequency[fname]
                logging.info('Reporting %d calls for function %s.' %
                             (call_frequency[fname], fname))

                call_frequency[fname] = 0

            sckt = pusher_cache.get(sutils._get_statistics_report_address
                                    (mgmt_ip))
            sckt.send(stats.SerializeToString())

            start = time.time()


def _update_cluster_state(requestor_cache, mgmt_ip, executors, key_ip_map,
                          kvs):
    # update our local key-cache mapping information
    utils._update_key_maps(key_ip_map, executors, kvs)

    schedulers = utils._get_ip_set(utils._get_scheduler_list_address(mgmt_ip),
                                   requestor_cache, False)

    return schedulers


def _find_upstream(fname, dag):
    upstream = set()
    for conn in dag.connections:
        if conn.sink == fname:
            upstream.add(conn.source)
    return upstream

'''def _check_parallel_flow(function_trigger_map, prior_read_map, prior_causal_lowerbound_map):
    for func_read_set in function_trigger_map[fname]:
        for versioned_key in prior_read_map[func_read_set]:
            for func_causal_lowerbound in function_trigger_map[fname]:
                if func_causal_lowerbound != func_read_set:
                    for causal_lowerbound in prior_causal_lowerbound_map[func_causal_lowerbound]:
                        if versioned_key.key == causal_lowerbound.key and sutils._compare_vector_clock(versioned_key.vector_clock, causal_lowerbound.vector_clock) != CausalComp.GreaterOrEqual:
                            return True
    return False'''


def _check_parallel_flow(prior_causal_lowerbound_list, prior_read_list):
    for versioned_key_read in prior_read_list:
        for causal_lowerbound in prior_causal_lowerbound_list:
            if versioned_key_read.key == causal_lowerbound.key and sutils._compare_vector_clock(versioned_key_read.vector_clock, causal_lowerbound.vector_clock) != CausalComp.GreaterOrEqual:
                return True
    return False


def _construct_causal_frontier(prior_causal_lowerbound_list):
    causal_frontier = {}
    for versioned_key in prior_causal_lowerbound_list:
        if versioned_key.key not in causal_frontier:
            causal_frontier[versioned_key.key] = [[versioned_key.vector_clock, False]]
        else:
            to_remove = []
            dominated = False
            for tp in causal_frontier[versioned_key.key]:
                if sutils._compare_vector_clock(tp[0], versioned_key.vector_clock) == CausalComp.Less:
                    to_remove.append(tp)
                elif sutils._compare_vector_clock(tp[0], versioned_key.vector_clock) == CausalComp.GreaterOrEqual:
                    dominated = True
                    break
            if dominated:
                continue
            for tp in to_remove:
                causal_frontier.remove(tp)
            causal_frontier[versioned_key.key].append([versioned_key.vector_clock, False])
    return causal_frontier


def _remove_from_local_readset(key, causal_frontier, read_set, remove_candidate, version_store):
    if key not in causal_frontier:
        return False
    vc = {}
    for tp in causal_frontier[key]:
        if tp[1]:
            vc = sutils._merge_vector_clock(vc, tp[0])
    for tp in causal_frontier[key]:
        if sutils._compare_vector_clock(vc, tp[0]) != CausalComp.GreaterOrEqual:
            tp[1] = True
            vc = sutils._merge_vector_clock(vc, tp[0])
    for other_key in read_set:
        if not other_key in remove_candidate:
            if other_key in version_store:
                if key in version_store[other_key] and sutils._compare_vector_clock(vc, version_store[other_key][key]) != CausalComp.GreaterOrEqual:
                    remove_candidate.add(other_key)
                    if not _remove_from_local_readset(other_key, causal_frontier, read_set, remove_candidate, version_store):
                        return False
    return True


def _optimistic_protocol(versioned_key_map, cid, fname, causal_frontier, prior_read_map, causal_lowerbound_list, prior_read_list):
    for key in versioned_key_map[cid].per_func_read_set[fname]:
        if key in causal_frontier:
            vc = {}
            if key in versioned_key_map[cid].per_func_versioned_key_chain[fname]:
                vc = versioned_key_map[cid].per_func_versioned_key_chain[fname][key][key]
            for tp in causal_frontier[key]:
                if sutils._compare_vector_clock(vc, tp[0]) != CausalComp.GreaterOrEqual:
                    tp[1] = True
                    vc = sutils._merge_vector_clock(vc, tp[0])

    remove_candidate = set()
    for key in versioned_key_map[cid].per_func_read_set[fname]:
        if key not in remove_candidate:
            if key in versioned_key_map[cid].per_func_versioned_key_chain[fname]:
                for dep_key in versioned_key_map[cid].per_func_versioned_key_chain[fname][key]:
                    if dep_key in prior_read_map and sutils._compare_vector_clock(prior_read_map[dep_key], versioned_key_map[cid].per_func_versioned_key_chain[fname][key][dep_key]) != CausalComp.GreaterOrEqual:
                        remove_candidate.add(key)
                        if not _remove_from_local_readset(key, causal_frontier, read_set, remove_candidate, versioned_key_map[cid].per_func_versioned_key_chain[fname]):
                            # abort
                            return True
                        break
    # gather readset versions and prior causal lowerbound
    for key in versioned_key_map[cid].per_func_read_set[fname]:
        vk = VersionedKey()
        vk.key = key
        vc = {}
        if key not in remove_candidate and key in versioned_key_map[cid].per_func_versioned_key_chain[fname]:
            vc = versioned_key_map[cid].per_func_versioned_key_chain[fname][key][key]
            for dep_key in versioned_key_map[cid].per_func_versioned_key_chain[fname][key]:
                dep_vk = VersionedKey()
                dep_vk.key = dep_key
                dep_vk.vector_clock.update(versioned_key_map[cid].per_func_versioned_key_chain[fname][key][dep_key])
                causal_lowerbound_list.append(dep_vk)
        if key in causal_frontier:
            for tp in causal_frontier[key]:
                if tp[1]:
                    vc = sutils._merge_vector_clock(vc, tp[0])
        vk.vector_clock.update(vc)
        prior_read_list.append(vk)
    # no abort
    return False


def _simulate_optimistic_protocol(versioned_key_map, cid, finished_functions, dags, function_trigger_map, prior_per_func_causal_lowerbound_map, prior_per_func_read_map):
    while (len(finished_functions) < len(dags[dag_name][0].functions)):
        for fname in function_trigger_map:
            if not fname in finished_functions:
                finished = True
                for trigger_function in function_trigger_map[fname]:
                    if trigger_function not in finished_functions:
                        finished = False
                if finished:
                    # aggregate the prior_causal_lowerbound_list and prior_read_list respectively
                    prior_causal_lowerbound_list = []
                    prior_read_list = []
                    for source_func in function_trigger_map[fname]:
                        prior_causal_lowerbound_list.extend(prior_per_func_causal_lowerbound_map[source_func])
                        prior_read_list.extend(prior_per_func_read_map[source_func])

                    if len(function_trigger_map[fname]) > 1 and _check_parallel_flow(prior_causal_lowerbound_list, prior_read_list):
                        # abort
                        return True
                    # turn prior read list into a map
                    prior_read_map = {}
                    for versioned_key in prior_read_list:
                        prior_read_map[versioned_key.key] = versioned_key.vector_clock
                    causal_frontier = _construct_causal_frontier(prior_causal_lowerbound_list)
                    prior_per_func_causal_lowerbound_map[fname] = prior_causal_lowerbound_list
                    prior_per_func_read_map[fname] = prior_read_list
                    if _optimistic_protocol(versioned_key_map, cid, fname, causal_frontier, prior_read_map, prior_per_func_causal_lowerbound_map[fname], prior_per_func_read_map[fname]):
                        # abort
                        return True
                    finished_functions.add(fname)
    return False








