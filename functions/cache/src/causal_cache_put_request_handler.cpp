//  Copyright 2018 U.C. Berkeley RISE Lab
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

#include "causal_cache_utils.hpp"

void put_request_handler(const string& serialized, StoreType& unmerged_store,
                         StoreType& causal_cut_store,
                         VersionStoreType& version_store,
                         map<string, Address>& request_id_to_address_map,
                         KvsAsyncClientInterface* client, logger log, SocketCache& pushers) {
  /*auto parse_begin = std::chrono::system_clock::now();

  vector<string> v;
  split(serialized, ':', v);
  auto parse_end = std::chrono::system_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                      parse_end - parse_begin)
                      .count();
  log->info("parse time is {}", duration);

  CausalPutResponse resp;
  resp.add_keys(v[0]);
  string resp_string;
  resp.SerializeToString(&resp_string);
  kZmqUtil->send_string(
      resp_string,
      &pushers[v[1]]);

  // write to KVS
  string req_id = client->put_async(v[0], std::move(v[2]),
                                    LatticeType::CROSSCAUSAL);

  request_id_to_address_map[req_id] = v[1];*/

  auto parse_begin = std::chrono::system_clock::now();
  CausalPutRequest request;
  request.ParseFromString(serialized);
  auto parse_end = std::chrono::system_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                      parse_end - parse_begin)
                      .count();
  log->info("parse time is {}", duration);

  for (CausalTuple tuple : request.tuples()) {
    Key key = tuple.key();

    CausalPutResponse resp;
    resp.add_keys(key);
    string resp_string;
    resp.SerializeToString(&resp_string);
    kZmqUtil->send_string(
        resp_string,
        &pushers[request.response_address()]);

    // write to KVS
    string req_id = client->put_async(key, std::move(tuple.payload()),
                                      LatticeType::CROSSCAUSAL);

    request_id_to_address_map[req_id] = request.response_address();
  }

  /*for (CausalTuple tuple : request.tuples()) {
    Key key = tuple.key();
    //log->info("key to put is {}", key);
    auto lattice = std::make_shared<CrossCausalLattice<SetLattice<string>>>(
        to_cross_causal_payload(deserialize_cross_causal(tuple.payload())));
    // first, update unmerged store
    if (unmerged_store.find(key) == unmerged_store.end()) {
      unmerged_store[key] = lattice;
    } else {
      unsigned comp_result = causal_comparison(unmerged_store[key], lattice);
      if (comp_result == kCausalLess) {
        unmerged_store[key] = lattice;
      } else if (comp_result == kCausalConcurrent) {
        unmerged_store[key] = causal_merge(unmerged_store[key], lattice);
      }
    }
    // write to KVS
    string req_id = client->put_async(key, serialize(*unmerged_store[key]),
                                      LatticeType::CROSSCAUSAL);
    // TODO: remove this when running in non-test mode!
    //unmerged_store.erase(key);
    request_id_to_address_map[req_id] = request.response_address();
  }*/
}