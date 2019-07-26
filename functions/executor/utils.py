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

from include.functions_pb2 import *
from include.kvs_pb2 import *
from include import server_utils, serializer

import zmq

UTILIZATION_REPORT_PORT = 7003
EXECUTOR_DEPART_PORT = 7005


def _retrieve_function(name, kvs, consistency=CROSS):
    kvs_name = server_utils._get_func_kvs_name(name)
    logging.info('function name is %s', kvs_name)

    if consistency == NORMAL:
        logging.info('Normal mode')
        result = kvs.get(kvs_name)
        if result:
            latt = result[kvs_name]
            return serializer.function_ser.load(latt.reveal()[1])
        else:
            return None
    else:
        logging.info('Causal mode')
        result = kvs.causal_get([kvs_name], SINGLE, '0', {}, False)
        if not result == KEY_DNE and not result == ABORT:
            return serializer.function_ser.load(result[kvs_name][1])
        else:
            return None


def _push_status(schedulers, pusher_cache, status):
    msg = status.SerializeToString()

    # tell all the schedulers your new status
    for sched in schedulers:
        sckt = pusher_cache.get(_get_status_address(sched))
        sckt.send(msg)


def _get_status_address(ip):
    return 'tcp://' + ip + ':' + str(server_utils.STATUS_PORT)


def _get_util_report_address(mgmt_ip):
    return 'tcp://' + mgmt_ip + ':' + str(UTILIZATION_REPORT_PORT)


def _get_depart_done_addr(mgmt_ip):
    return 'tcp://' + mgmt_ip + ':' + str(EXECUTOR_DEPART_PORT)


def _get_schedule_gc_address(ip_tid):
    ip, tid = ip_tid.split(':')
    return 'tcp://' + ip + ':' + str(int(tid) + server_utils.DAG_SCHEDULE_GC_PORT)
