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

void scheduler_remote_read_handler(
    const string& serialized, VersionStoreType& version_store,
    std::unordered_map<ClientIdFunctionPair, PendingClientMetadata, PairHash>&
        pending_cross_metadata,
    SocketCache& pushers, logger log,
    const CausalCacheThread& cct, std::unordered_map<ClientIdFunctionPair, ProtocolMetadata, PairHash>& protocol_matadata_map) {
  SchedulerRemoteReadRequest request;
  request.ParseFromString(serialized);
  auto cid_function_pair =
      std::make_pair(request.client_id(), request.function_name());
  if (protocol_matadata_map.find(cid_function_pair) == protocol_matadata_map.end()) {
    // optimistic protocol for this cid_fname pair hasn't reached this cache yet
    if (request.tuples_size() == 0) {
      // no remote read necessary
      protocol_matadata_map[cid_function_pair].msg_ = kNoAction;
    } else {
      // need to perform remote read
      // assemble messages
      map<Address, VersionedKeyRequest> addr_request_map;
      for (const auto& tp : request.tuples()) {
        if (addr_request_map.find(tp.cache_address()) == addr_request_map.end()) {
          addr_request_map[tp.cache_address()].set_response_address(
              cct.causal_cache_versioned_key_response_connect_address());
          addr_request_map[tp.cache_address()].set_client_id(
              cid_function_pair.first);
          addr_request_map[tp.cache_address()].set_function_name(cid_function_pair.second);
        }
        auto versioned_key_request_tuple_ptr = addr_request_map[tp.cache_address()].add_versioned_key_request_tuples();
        versioned_key_request_tuple_ptr->set_function_name(tp.function_name());
        versioned_key_request_tuple_ptr->set_key(tp.versioned_key().key());
        // populate pending metadata
        // first, convert protobuf type to VectorClock
        VectorClock vc;
        for (const auto& key_version_pair :
             tp.versioned_key().vector_clock()) {
          vc.insert(key_version_pair.first, key_version_pair.second);
        }
        pending_cross_metadata[cid_function_pair].remote_read_tracker_[tp.versioned_key().key()].insert(vc);
      }
      for (const auto& pair : addr_request_map) {
        // send request
        string req_string;
        pair.second.SerializeToString(&req_string);
        kZmqUtil->send_string(req_string, &pushers[pair.first]);
      }
      for (const auto& pair : request.read_map()) {
        // populate local read if not in remove set
        if (!pair.second) {
          pending_cross_metadata[cid_function_pair].result_[pair.first] = version_store.at(cid_function_pair).second.at(pair.first).at(pair.first);
        } else {
          pending_cross_metadata[cid_function_pair].remove_set_.insert(pair.first);
        }
      }
      protocol_matadata_map[cid_function_pair].msg_ = kRemoteRead;
    }
  } else if (protocol_matadata_map[cid_function_pair].progress_ == kRemoteRead) {
    // progress is pending remote read
    // after remote read, gc happens in versioned key response handler
    protocol_matadata_map[cid_function_pair].msg_ = kNoAction;
  } else if (protocol_matadata_map[cid_function_pair].progress_ == kFinish) {
    // progress is finish
    // gc happen here
    protocol_matadata_map.erase(cid_function_pair);
  } else {
    log->error("Invalid executor progress type.");
  }
}