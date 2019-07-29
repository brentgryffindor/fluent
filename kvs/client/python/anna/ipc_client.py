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

GET_REQUEST_ADDR = "ipc:///requests/get"
PUT_REQUEST_ADDR = "ipc:///requests/put"

GET_RESPONSE_ADDR_TEMPLATE = "ipc:///requests/get_%d"
PUT_RESPONSE_ADDR_TEMPLATE = "ipc:///requests/put_%d"

import logging
from .functions_pb2 import *
from .kvs_pb2 import *
from .lattices import *
import zmq
import time

class IpcAnnaClient:
    def __init__(self, thread_id = 0, ip = None):
        self.context = zmq.Context(1)

        #self.get_response_address = GET_RESPONSE_ADDR_TEMPLATE % thread_id
        self.get_response_address = 'tcp://' + ip + ':' + str(thread_id + 7650)
        #self.put_response_address = PUT_RESPONSE_ADDR_TEMPLATE % thread_id
        self.put_response_address = 'tcp://' + ip + ':' + str(thread_id + 7700)

        self.get_request_socket = self.context.socket(zmq.PUSH)
        #self.get_request_socket.connect(GET_REQUEST_ADDR)
        self.get_request_socket.connect('tcp://' + ip + ':' + str(7550))

        self.put_request_socket = self.context.socket(zmq.PUSH)
        #self.put_request_socket.connect(PUT_REQUEST_ADDR)
        self.put_request_socket.connect('tcp://' + ip + ':' + str(7600))

        self.get_response_socket = self.context.socket(zmq.PULL)
        #self.get_response_socket.setsockopt(zmq.RCVTIMEO, 5000)
        #self.get_response_socket.bind(self.get_response_address)
        self.get_response_socket.bind('tcp://*:' + str(thread_id + 7650))

        self.put_response_socket = self.context.socket(zmq.PULL)
        #self.put_response_socket.setsockopt(zmq.RCVTIMEO, 5000)
        #self.put_response_socket.bind(self.put_response_address)
        self.put_response_socket.bind('tcp://*:' + str(thread_id + 7700))

    def get(self, keys):
        if type(keys) != list:
            keys = [keys]

        request = KeyRequest()
        request.type = GET

        for key in keys:
            tp = request.tuples.add()
            tp.key = key

        request.response_address = self.get_response_address
        self.get_request_socket.send(request.SerializeToString())

        try:
            msg = self.get_response_socket.recv()
        except zmq.ZMQError as e:
            if e.errno == zmq.EAGAIN:
                logging.error("Request for %s timed out!" % (str(keys)))
            else:
                logging.error("Unexpected ZMQ error: %s." % (str(e)))

            resp = {}
            for key in keys:
                resp[key] = None

            return resp
        else:
            kv_pairs = {}
            resp = KeyResponse()
            resp.ParseFromString(msg)

            for tp in resp.tuples:
                if tp.error == 1 or tp.lattice_type == NO:
                    kv_pairs[tp.key] = None

                elif tp.lattice_type == LWW:
                    val = LWWValue()
                    val.ParseFromString(tp.payload)

                    kv_pairs[tp.key] = LWWPairLattice(val.timestamp, val.value)
                elif tp.lattice_type == SET:
                    res = set()

                    val = SetValue()
                    val.ParseFromString(tp.payload)

                    for v in val.values:
                        res.add(v)

                    kv_pairs[tp.key] = SetLattice(res)


                elif tp.lattice_type == ORDERED_SET:
                    res = ListBasedOrderedSet()
                    val = SetValue()
                    val.ParseFromString(tp.payload)
                    for v in val.values:
                        res.insert(v)
                    kv_pairs[tp.key] = OrderedSetLattice(res)

                else:
                    raise ValueError('Invalid Lattice type: ' +
                                     str(tp.lattice_type))
            return kv_pairs

    def causal_get(self, keys, consistency, client_id, dependencies, sink):
        #logging.info('Entering causal GET')
        if type(keys) != list:
            keys = list(keys)

        request = CausalGetRequest()

        if consistency == SINGLE:
            request.consistency = SINGLE
        elif consistency == CROSS:
            request.consistency = CROSS
        else:
            logging.error("Error: non causal consistency in causal mode!")
            return None

        request.client_id = client_id
        request.keys.extend(keys)
        request.gc = sink

        #for k in request.keys:
        #    logging.info('key to GET is %s' % k)

        request.response_address = self.get_response_address

        #logging.info('sending GET')
        #send_start = time.time()
        self.get_request_socket.send(request.SerializeToString())
        #send_end = time.time()
        #receive_start = time.time()
        msg = self.get_response_socket.recv()
        #send_end = time.time()
        #receive_end = time.time()
        #logging.info('took %s to send to cache' % (send_end - send_start))
        #logging.info('took %s to receive from cache' % (receive_end - receive_start))
        resp = CausalGetResponse()
        resp.ParseFromString(msg)

        kv_pairs = {}
        for tp in resp.tuples:
            val = CrossCausalValue()
            val.ParseFromString(tp.payload)

            # for now, we just take the first value in the setlattice
            kv_pairs[tp.key] = (val.vector_clock, val.values[0])
        return kv_pairs

        '''try:
            msg = self.get_response_socket.recv()
            send_end = time.time()
            logging.info('took %s to receive response from cache' % (send_end - send_start))
            #logging.info('received response')
        except zmq.ZMQError as e:
            if e.errno == zmq.EAGAIN:
                logging.error("Request for %s timed out!" % (str(keys)))
            else:
                logging.error("Unexpected ZMQ error: %s." % (str(e)))
            return None
        else:
            #logging.info('parsing response')
            #parse_start = time.time()
            resp = CausalGetResponse()
            resp.ParseFromString(msg)
            #logging.info('parsed')

            #logging.info('GET successful')
            kv_pairs = {}
            for tp in resp.tuples:
                #logging.info('key is %s', tp.key)
                val = CrossCausalValue()
                val.ParseFromString(tp.payload)

                # for now, we just take the first value in the setlattice
                kv_pairs[tp.key] = (val.vector_clock, val.values[0])
            #logging.info('returning from causal GET')
            #parse_end = time.time()
            #logging.info('parsing took %s' % (parse_end - parse_start))
            return kv_pairs'''

    def put(self, key, value):
        request = KeyRequest()
        request.type = PUT

        tp = request.tuples.add()
        tp.key = key

        if type(value) == LWWPairLattice:
            tp.lattice_type = LWW

            ser = LWWValue()
            ser.timestamp = value.reveal()[0]
            ser.value = value.reveal()[1]

            tp.payload = ser.SerializeToString()
        elif type(value) == SetLattice:
            tp.lattice_type = SET

            ser = SetValue()
            ser.values.extend(list(value.reveal()))

            tp.payload = ser.SerializeToString()

        elif type(value) == OrderedSetLattice:
            tp.lattice_type == ORDERED_SET
            ser = SetValue()
            ser.values.extend(value.reveal().lst)
            tp.payload = ser.SerializeToString()

        else:
            raise ValueError('Invalid PUT type: ' + str(type(value)))

        request.response_address = self.put_response_address

        self.put_request_socket.send(request.SerializeToString())

        try:
            msg = self.put_response_socket.recv()
        except zmq.ZMQError as e:
            if e.errno == zmq.EAGAIN:
                logging.error("Request for %s timed out!" % (str(key)))
            else:
                logging.error("Unexpected ZMQ error: %s." % (str(e)))

            return False
        else:
            resp = KeyResponse()
            resp.ParseFromString(msg)

            return resp.tuples[0].error == 0

    def causal_put(self, key, vector_clock, dependency, value, client_id):
        request = CausalPutRequest()

        tp = request.tuples.add()
        tp.key = key

        cross_causal_value = CrossCausalValue()
        cross_causal_value.vector_clock.update(vector_clock)

        for key in dependency:
            dep = cross_causal_value.deps.add()
            dep.key = key
            dep.vector_clock.update(dependency[key])

        cross_causal_value.values.extend([value])

        tp.payload = cross_causal_value.SerializeToString()

        request.response_address = self.put_response_address

        self.put_request_socket.send(request.SerializeToString())

        try:
            msg = self.put_response_socket.recv()
        except zmq.ZMQError as e:
            if e.errno == zmq.EAGAIN:
                logging.error("Request for %s timed out!" % (str(key)))
            else:
                logging.error("Unexpected ZMQ error: %s." % (str(e)))

            return False
        else:
            return True

    def _vc_merge(self, lhs, rhs):
        result = lhs
        for cid in rhs:
            if cid not in result:
                result[cid] = rhs[cid]
            else:
                result[cid] = max(result[cid], rhs[cid])
        return result
