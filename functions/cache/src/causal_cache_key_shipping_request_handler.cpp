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

void key_shipping_request_handler(const string& serialized,
                                  const VersionStoreType& version_store,
                                  const CausalCacheThread& cct,
                                  SocketCache& pushers, logger log) {
  KeyShippingRequest request;
  request.ParseFromString(serialized);

  KeyShippingResponse response;
  response.set_client_id(request.client_id());
  response.set_cache_address(
      cct.causal_cache_key_shipping_request_connect_address());

  for (const auto& function_key_pair : request.function_key_pairs()) {
    auto function_causal_tuple_pair =
        response.add_function_causal_tuple_pairs();
    function_causal_tuple_pair->set_function_name(
        function_key_pair.source_function_name());
    auto cid_function_pair = std::make_pair(
        request.client_id(), function_key_pair.target_function_name());
    bool found = false;
    for (const auto& head_key_chain_pair :
         version_store.at(cid_function_pair).second) {
      for (const auto& key_ptr_pair : head_key_chain_pair.second) {
        if (key_ptr_pair.first == function_key_pair.key()) {
          found = true;
          auto tp = function_causal_tuple_pair->mutable_tuple();
          tp->set_key(key_ptr_pair.first);
          tp->set_payload(serialize(*(key_ptr_pair.second)));
          break;
        }
      }
      if (found) {
        break;
      }
    }
    if (!found) {
      log->error(
          "Requested key {} for client ID function pair {},{} not available "
          "in versioned "
          "store.",
          function_key_pair.key(), cid_function_pair.first,
          cid_function_pair.second);
    }
  }

  // send response
  string resp_string;
  response.SerializeToString(&resp_string);
  kZmqUtil->send_string(resp_string, &pushers[request.response_address()]);
}