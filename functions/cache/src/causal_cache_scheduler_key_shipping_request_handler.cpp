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

void scheduler_key_shipping_request_handler(
    const string& serialized,
    map<string, pair<set<Address>, Address>>& pending_key_shipping_map,
    std::unordered_map<ClientIdFunctionPair, StoreType, PairHash>&
        conservative_store,
    const VersionStoreType& version_store, const CausalCacheThread& cct,
    SocketCache& pushers, std::unordered_map<ClientIdFunctionPair, ProtocolMetadata, PairHash>& protocol_matadata_map) {
  SchedulerKeyShippingRequest request;
  request.ParseFromString(serialized);
  pending_key_shipping_map[request.client_id()].second =
      request.response_address();
  // first populate conservative store from local data in versioned store
  for (const auto& per_function_readset : request.per_function_readsets()) {
    auto cid_function_pair = std::make_pair(
        request.client_id(), per_function_readset.function_name());
    for (const Key& key : per_function_readset.keys()) {
      if (version_store.at(cid_function_pair).second.find(key) !=
          version_store.at(cid_function_pair).second.end()) {
        conservative_store[cid_function_pair][key] =
            version_store.at(cid_function_pair).second.at(key).at(key);
      }
    }
    // also, receiving this request means that the optimistic protocol will abort, so we update the protocol metadata
    if (protocol_matadata_map.find(cid_function_pair) == protocol_matadata_map.end() || protocol_matadata_map[cid_function_pair].progress_ == kRemoteRead) {
      // optimistic protocol for this cid_fname pair hasn't reached this cache yet or remote read
      protocol_matadata_map[cid_function_pair].msg_ = kAbort;
    } else {
      // progress is finish
      // gc happen here
      protocol_matadata_map.erase(cid_function_pair);
    }
  }
  // then send msgs to fetch from remote
  for (const auto& per_cache_function_key_pair :
       request.per_cache_function_key_pairs()) {
    pending_key_shipping_map[request.client_id()].first.insert(
        per_cache_function_key_pair.cache_address());
    KeyShippingRequest key_shipping_request;
    key_shipping_request.set_client_id(request.client_id());
    key_shipping_request.set_response_address(
        cct.causal_cache_key_shipping_response_connect_address());
    for (const auto& function_key_pair :
         per_cache_function_key_pair.function_key_pairs()) {
      auto new_function_key_pair =
          key_shipping_request.add_function_key_pairs();
      new_function_key_pair->set_source_function_name(
          function_key_pair.source_function_name());
      new_function_key_pair->set_target_function_name(
          function_key_pair.target_function_name());
      new_function_key_pair->set_key(function_key_pair.key());
    }
    // send request
    string req_string;
    key_shipping_request.SerializeToString(&req_string);
    kZmqUtil->send_string(
        req_string, &pushers[per_cache_function_key_pair.cache_address()]);
    std::cout << "cid" + request.client_id() + "sent key shipping request to cache " + per_cache_function_key_pair.cache_address() + "\n";
  }
  // check if no remote read
  if (pending_key_shipping_map[request.client_id()].first.size() == 0) {
    SchedulerKeyShippingResponse scheduler_response;
    scheduler_response.set_client_id(request.client_id());
    scheduler_response.set_cache_address(
        cct.causal_cache_scheduler_key_shipping_request_connect_address());
    // send response
    string resp_string;
    scheduler_response.SerializeToString(&resp_string);
    kZmqUtil->send_string(
        resp_string,
        &pushers[pending_key_shipping_map[request.client_id()].second]);
    // GC
    pending_key_shipping_map.erase(request.client_id());
  }
}