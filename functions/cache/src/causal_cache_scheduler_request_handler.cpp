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

void scheduler_request_handler(
    const string& serialized, set<Key>& key_set, StoreType& unmerged_store,
    InPreparationType& in_preparation, StoreType& causal_cut_store,
    VersionStoreType& version_store,
    map<string, PendingClientMetadata>& pending_cross_metadata,
    map<Key, set<Key>>& to_fetch_map,
    map<Key, std::unordered_map<VectorClock, set<Key>, VectorClockHash>>&
        cover_map,
    SocketCache& pushers, KvsAsyncClientInterface* client, logger log,
    const CausalCacheThread& cct) {
  CausalSchedulerRequest request;
  request.ParseFromString(serialized);

  // first, check the version store and see if all data is already there
  // this happens when the executor request reached first and already finished
  // populating the version store

  // debug
  //log->info("client id is {}", request.client_id());
  //std::cout << "client id is " + request.client_id() + "\n";
  // we first check if all requested keys are covered by the cache
  set<Key> read_set;
  for (const string& key : request.keys()) {
    read_set.insert(key);
  }
  set<Key> to_cover;

  if (!covered_locally(request.client_id(), read_set, to_cover, key_set,
                       unmerged_store, in_preparation, causal_cut_store,
                       version_store, pending_cross_metadata, to_fetch_map,
                       cover_map, pushers, client, cct, log)) {
    log->info("not covered");
    //std::cout << "not covered locally\n";
    pending_cross_metadata[request.client_id()].read_set_ = read_set;
    pending_cross_metadata[request.client_id()].to_cover_set_ = to_cover;
    pending_cross_metadata[request.client_id()].scheduler_response_address_ =
        request.scheduler_address();
  } else {
    log->info("covered");
    //std::cout << "covered locally\n";
    // all keys covered, first populate version store entry
    // in this case, it's not possible that keys DNE
    //log->info("creating version store entry for client id {}", request.client_id());
    for (const string& key : request.keys()) {
      version_store[request.client_id()][key] = causal_cut_store.at(key);
    }
    // then respond to scheduler
    CausalSchedulerResponse response;
    response.set_client_id(request.client_id());
    response.set_succeed(true);
    // send response
    string resp_string;
    response.SerializeToString(&resp_string);
    kZmqUtil->send_string(resp_string, &pushers[request.scheduler_address()]);
  }
}