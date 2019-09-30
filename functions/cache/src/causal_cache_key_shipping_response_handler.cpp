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

void key_shipping_response_handler(
    const string& serialized,
    map<string, pair<set<Address>, Address>>& pending_key_shipping_map,
    std::unordered_map<ClientIdFunctionPair, StoreType, PairHash>&
        conservative_store,
    const CausalCacheThread& cct, SocketCache& pushers) {
  KeyShippingResponse response;
  response.ParseFromString(serialized);
  std::cout << "cid" + response.client_id() + "received key shipping response from cache " + response.cache_address() + "\n";
  for (const auto& function_causal_tuple_pair :
       response.function_causal_tuple_pairs()) {
    // merge into conservative store
    auto cid_function_pair = std::make_pair(
        response.client_id(), function_causal_tuple_pair.function_name());
    const Key& key = function_causal_tuple_pair.tuple().key();
    auto lattice = std::make_shared<CrossCausalLattice<SetLattice<string>>>(
        to_cross_causal_payload(deserialize_cross_causal(
            function_causal_tuple_pair.tuple().payload())));
    if (conservative_store[cid_function_pair].find(key) ==
        conservative_store[cid_function_pair].end()) {
      conservative_store[cid_function_pair][key] = lattice;
    } else {
      conservative_store[cid_function_pair][key] =
          causal_merge(conservative_store[cid_function_pair][key], lattice);
    }
  }
  pending_key_shipping_map[response.client_id()].first.erase(
      response.cache_address());
  // check if gathered all responses
  if (pending_key_shipping_map[response.client_id()].first.size() == 0) {
    std::cout << "client id " + response.client_id() + " gathered all responses\n";
    SchedulerKeyShippingResponse scheduler_response;
    scheduler_response.set_client_id(response.client_id());
    scheduler_response.set_cache_address(
        cct.causal_cache_scheduler_key_shipping_request_connect_address());
    // send response
    string resp_string;
    scheduler_response.SerializeToString(&resp_string);
    std::cout << "response address is " + pending_key_shipping_map[response.client_id()].second + "\n";
    kZmqUtil->send_string(
        resp_string,
        &pushers[pending_key_shipping_map[response.client_id()].second]);
    std::cout << "sent\n";
    // GC
    pending_key_shipping_map.erase(response.client_id());
    //std::cout << "GC client id " + response.client_id() + "\n";
  }
}