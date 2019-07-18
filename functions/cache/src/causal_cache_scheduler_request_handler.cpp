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
    std::unordered_map<ClientIdFunctionPair, PendingClientMetadata, PairHash>& pending_cross_metadata,
    map<Key, set<Key>>& to_fetch_map,
    map<Key, std::unordered_map<VectorClock, set<Key>, VectorClockHash>>&
        cover_map,
    SocketCache& pushers, KvsAsyncClientInterface* client, logger log,
    const CausalCacheThread& cct) {

  CausalSchedulerRequest request;
  request.ParseFromString(serialized);

  // first, check the version store and see if all data is already there
  // this happens when the executor request reached first and already finished populating the version store
  auto cid_function_pair = std::make_pair(request.client_id(), request.function_name());
  if (version_store.find(cid_function_pair) != version_store.end()) {
    // the entry already exists in version store
    CausalSchedulerResponse response;
    response.set_client_id(request.client_id());
    response.set_function_name(request.function_name());
    if (version_store[cid_function_pair].first) {
      // some keys DNE
      response.set_succeed(false);
      // send response
      string resp_string;
      response.SerializeToString(&resp_string);
      kZmqUtil->send_string(resp_string, &pushers[request.scheduler_address()]);
    } else {
      send_scheduler_response(response, cid_function_pair, version_store, pushers, request.scheduler_address());
    }
  } else if (pending_cross_metadata.find(cid_function_pair) != pending_cross_metadata.end()) {
    // no entry in version store
    // but has entry in pending cross metadata
    // this means that executor already issued the GET request but not all required data
    // have been fetched from the KVS, so we just add scheduler address to the pending map
    pending_cross_metadata[cid_function_pair].scheduler_response_address_ = request.scheduler_address();
  } else {
    // no entry at all
    // we first check if all requested keys are covered by the cache
    set<Key> read_set;
    for (const string& key : request.keys()) {
      read_set.insert(key);
    }
    set<Key> to_cover;
    CausalFrontierType causal_frontier;

    if (!covered_locally(cid_function_pair, read_set, to_cover, key_set, unmerged_store, in_preparation, causal_cut_store, 
                        version_store, pending_cross_metadata, to_fetch_map, cover_map, pushers, client, cct, causal_frontier, log)) {
      pending_cross_metadata[cid_function_pair].read_set_ = read_set;
      pending_cross_metadata[cid_function_pair].to_cover_set_ =
          to_cover;
      pending_cross_metadata[cid_function_pair].scheduler_response_address_ = request.scheduler_address();
    } else {
      // all keys covered, first populate version store entry
      // in this case, it's not possible that keys DNE
      version_store[cid_function_pair].first = false;
      // retrieve full read set
      set<Key> full_read_set;
      for (const string& key : request.full_read_set()) {
        full_read_set.insert(key);
      }
      for (const string& key : request.keys()) {
        set<Key> observed_keys;
        if (causal_cut_store.find(key) != causal_cut_store.end()) {
          // for scheduler, this if statement should always pass because causal frontier is set to empty
          save_versions(cid_function_pair, key, key, version_store, causal_cut_store,
                        full_read_set, observed_keys);
        }
      }
      // then respond to scheduler
      CausalSchedulerResponse response;
      response.set_client_id(request.client_id());
      response.set_function_name(request.function_name());
      send_scheduler_response(response, cid_function_pair, version_store, pushers, request.scheduler_address());
    }
  }
}