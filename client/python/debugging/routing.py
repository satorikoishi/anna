#  Copyright 2019 U.C. Berkeley RISE Lab
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

import random
import socket
import sys

import zmq

from anna.anna_pb2 import (
    GET, PUT,  # Anna's request types
    NO_ERROR,  # Anna's error modes
    KeyAddressRequest,
    KeyAddressResponse,
    KeyResponse
)
from anna.base_client import BaseAnnaClient
from anna.common import UserThread
from anna.zmq_util import (
    recv_response,
    send_request,
    SocketCache
)

if len(sys.argv) < 3:
    print("Usage: python3 ./routing.py elb_addr my_ip")
    exit()

class RouteClient():
    def __init__(self, elb_addr, ip):
        '''
        elb_addr: Either 127.0.0.1 (local mode) or the address of an AWS ELB
        for the routing tier
        ip: The IP address of the machine being used -- if None is provided,
        one is inferred by using socket.gethostbyname(); WARNING: this does not
        always work
        elb_ports: The ports on which the routing tier will listen; use 6450 if
        running in local mode, otherwise do not change
        offset: A port numbering offset, which is only needed if multiple
        clients are running on the same machine
        '''

        self.elb_addr = elb_addr
        self.elb_ports = list(range(6450, 6454))
        self.ut = UserThread(ip, 0)

        self.context = zmq.Context(1)

        self.address_cache = {}
        self.pusher_cache = SocketCache(self.context, zmq.PUSH)

        self.response_puller = self.context.socket(zmq.PULL)
        self.response_puller.bind(self.ut.get_request_pull_bind_addr())

        self.key_address_puller = self.context.socket(zmq.PULL)
        self.key_address_puller.bind(self.ut.get_key_address_bind_addr())

        self.rid = 0

    def put(self, keys, values):
        request_ids = []

        if type(keys) != list:
            keys = [keys]
        if type(values) != list:
            values = [values]

        for key, value in zip(keys, values):
            worker_address = self._get_worker_address(key)

            if not worker_address:
                print("No worker address, return false")
                return False

        print("Worker address found, TODO: put data")
        return True
        #     send_sock = self.pusher_cache.get(worker_address)

        #     # We pass in a list because the data request preparation can prepare
        #     # multiple tuples
        #     req, tup = self._prepare_data_request([key])
        #     req.type = PUT
        #     request_ids.append(req.request_id)

        #     # PUT only supports one key operations, we only ever have to look at
        #     # the first KeyTuple returned.
        #     tup = tup[0]
        #     tup.payload, tup.lattice_type = self._serialize(value)

        #     send_request(req, send_sock)

        # responses = recv_response(request_ids, self.response_puller,
        #                          KeyResponse)

        # results = {}
        # for response in responses:
        #     tup = response.tuples[0]

        #     if tup.invalidate:
        #         self._invalidate_cache(tup.key)

        #     results[tup.key] = (tup.error == NO_ERROR)

        # return results

    # Returns the worker address for a particular key. If worker addresses for
    # that key are not cached locally, a query is synchronously issued to the
    # routing tier, and the address cache is updated.
    def _get_worker_address(self, key, pick=True):
        if key not in self.address_cache or len(self.address_cache[key]) == 0:
            port = random.choice(self.elb_ports)
            addresses = self._query_routing(key, port)
            self.address_cache[key] = addresses

        if len(self.address_cache[key]) == 0:
            print("No queried address, return None")
            return None

        if pick:
            return random.choice(self.address_cache[key])
        else:
            return self.address_cache[key]

    # Invalidates the address cache for a particular key when the server tells
    # the client that its cache is out of date.
    def _invalidate_cache(self, key):
        del self.address_cache[key]

    # Issues a synchronous query to the routing tier. Takes in a key and a
    # (randomly chosen) routing port to issue the request to. Returns a list of
    # addresses that the routing tier returned that correspond to the input
    # key.
    def _query_routing(self, key, port):
        key_request = KeyAddressRequest()

        key_request.response_address = self.ut.get_key_address_connect_addr()
        key_request.keys.append(key)
        key_request.request_id = self._get_request_id()

        dst_addr = 'tcp://' + self.elb_addr + ':' + str(port)
        send_sock = self.pusher_cache.get(dst_addr)

        print(f'Send query routing request to: {dst_addr}')
        send_request(key_request, send_sock)
        response = recv_response([key_request.request_id],
                                 self.key_address_puller,
                                 KeyAddressResponse)[0]

        if response.error != 0:
            print(f'Query routing response got error: {response.error}')
            return []

        result = []
        for t in response.addresses:
            if t.key == key:
                for a in t.ips:
                    result.append(a)

        return result
    
    def _get_request_id(self):
        response = self.ut.get_ip() + ':' + str(self.rid)
        self.rid = (self.rid + 1) % 10000
        return response

    @property
    def response_address(self):
        return self.ut.get_request_pull_connect_addr()

client = RouteClient(sys.argv[1], sys.argv[2])
res = client.put('1', '2')
print(f'Route client returns {res}')