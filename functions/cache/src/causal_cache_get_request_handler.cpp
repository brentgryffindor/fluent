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
   // log->info("Receive GET in single mode");
    bool covered_locally = true;
    set<Key> read_set;
    set<Key> to_cover;
    // check if the keys are covered locally
    for (const Key& key : request.keys()) {
      //log->info("Key is {}", key);
      read_set.insert(key);
      key_set.insert(key);

      if (unmerged_store.find(key) == unmerged_store.end()) {
        covered_locally = false;
        to_cover.insert(key);
        single_callback_map[key].insert(request.response_address());
        //log->info("firing get request for key {} in single routine", key);
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
    // convert the cached keys into a map
    map<Key, VectorClock> cached_versions;
    for (const auto& vk : request.cached_keys()) {
      VectorClock vc;
      for (const auto& key_version_pair : vk.vector_clock()) {
        vc.insert(key_version_pair.first, key_version_pair.second);
      }
      cached_versions[vk.key()] = vc;
    }
    //log->info("Receive GET in cross mode");
    //std::cout << "Receive GET in cross mode\n";
    if (version_store.find(request.client_id()) != version_store.end()) {
      CausalGetResponse response;
      //auto serialize_start = std::chrono::system_clock::now();
      for (const Key& key : request.keys()) {
        //log->info("ket to get is {}", key);
        if (version_store.at(request.client_id()).find(key) !=
            version_store.at(request.client_id()).end()) {
          // first check if the cached version is the same as what we want to return
          if (cached_versions.find(key) == cached_versions.end() || cached_versions.at(key).reveal() != version_store.at(request.client_id()).at(key)->reveal().vector_clock.reveal()) {
            //log->info("key {} not cached by executor, sending...", key);
            CausalTuple* tp = response.add_tuples();
            tp->set_key(key);
            tp->set_payload(
                serialize(*(version_store.at(request.client_id()).at(key))));
          }
        } else {
          log->error("key {} not found in version store.", key);
        }
      }
      //auto serialize_end = std::chrono::system_clock::now();
      //auto serialize_time = std::chrono::duration_cast<std::chrono::microseconds>(serialize_end - serialize_start).count();
      //log->info("serialization took {} micro seconds", serialize_time);

      // send response
      string resp_string;
      response.SerializeToString(&resp_string);
      kZmqUtil->send_string(resp_string,
                            &pushers[request.response_address()]);
      if (request.has_gc() && request.gc()) {
        //log->info("gc version store entry {}", request.client_id());
        //std::cout << "gc version store entry " + request.client_id() + "\n";
        version_store.erase(request.client_id());
      }
    } else {
      log->error("Error: version store for client id {} doesn't exist", request.client_id());
    }
  } else {
    log->error("Found non-causal consistency level.");
  }
}