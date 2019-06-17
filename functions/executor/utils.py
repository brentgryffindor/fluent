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


def _retrieve_function(name, kvs, consistency=NORMAL):
    kvs_name = server_utils._get_func_kvs_name(name)
<<<<<<< HEAD

    if consistency == NORMAL:
        result = kvs.get(kvs_name)
        if result:
            latt = result[kvs_name]
            return serializer.function_ser.load(latt.reveal()[1])
        else:
            return None
    else:
        result = kvs.causal_get([kvs_name], set(), {}, SINGLE, 0)
        if result:
            return serializer.function_ser.load(result[1][kvs_name][1])
        else:
            return None
=======
    result = kvs.causal_get([kvs_name], set(), {}, SINGLE, 0)

    while not result:
        logging.info("retrying get for function %s" % kvs_name)
        result = kvs.causal_get([kvs_name], set(), {}, SINGLE, 0)

    return serializer.function_ser.load(result[1][kvs_name][1])
>>>>>>> b7f4cf1c3dd1f700272799a787793bc1cc4ffc47


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
