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

void get_request_handler(
    const string& serialized, set<Key>& key_set, StoreType& unmerged_store,
    InPreparationType& in_preparation, StoreType& causal_cut_store,
    VersionStoreType& version_store,
    map<Key, set<Address>>& single_callback_map,
    map<Address, PendingClientMetadata>& pending_single_metadata,
    SocketCache& pushers, KvsAsyncClientInterface* client, logger log) {
  CausalGetRequest request;
  request.ParseFromString(serialized);

  if (request.consistency() == ConsistencyType::SINGLE) {
    log->info("Receive GET in single mode");
    bool covered_locally = true;
    set<Key> read_set;
    set<Key> to_cover;
    // check if the keys are covered locally
    for (const Key& key : request.keys()) {
      log->info("Key is {}", key);
      read_set.insert(key);
      key_set.insert(key);

      if (unmerged_store.find(key) == unmerged_store.end()) {
        covered_locally = false;
        to_cover.insert(key);
        single_callback_map[key].insert(request.response_address());
        log->info("firing get request for key {} in single routine", key);
        client->get_async(key);
      }
    }
    if (!covered_locally) {
      pending_single_metadata[request.response_address()] =
          PendingClientMetadata(read_set, to_cover);
    } else {
      CausalGetResponse response;

      response.set_error(ErrorType::NO_ERROR);

      for (const Key& key : read_set) {
        CausalTuple* tp = response.add_tuples();
        tp->set_key(key);
        tp->set_payload(serialize(*(unmerged_store[key])));
      }

      // send response
      string resp_string;
      response.SerializeToString(&resp_string);
      kZmqUtil->send_string(resp_string, &pushers[request.response_address()]);
    }
  } else if (request.consistency() == ConsistencyType::CROSS) {
    log->info("Receive GET in cross mode");
    std::cout << "Receive GET in cross mode\n";
    if (version_store.find(request.client_id()) != version_store.end()) {
      CausalGetResponse response;
      for (const Key& key : request.keys()) {
        if (version_store.at(request.client_id()).find(key) !=
            version_store.at(request.client_id()).end()) {
          CausalTuple* tp = response.add_tuples();
          tp->set_key(key);
          tp->set_payload(
              serialize(*(version_store.at(request.client_id()).at(key))));
        } else {
          log->error("key {} not found in version store.", key);
        }
      }
      // send response
      string resp_string;
      response.SerializeToString(&resp_string);
      kZmqUtil->send_string(resp_string,
                            &pushers[request.response_address()]);
      if (request.has_gc() && request.gc()) {
        version_store.erase(request.client_id());
      }
    } else {
      log->error("Error: version store for client id {} doesn't exist", request.client_id());
    }
  } else {
    log->error("Found non-causal consistency level.");
  }
}