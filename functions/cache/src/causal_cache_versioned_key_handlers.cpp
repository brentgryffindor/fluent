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

void versioned_key_request_handler(const string& serialized,
                                   const VersionStoreType& version_store,
                                   SocketCache& pushers, logger log,
                                   ZmqUtilInterface* kZmqUtil) {
  VersionedKeyRequest request;
  request.ParseFromString(serialized);

  VersionedKeyResponse response;
  response.set_client_id(request.client_id());
  response.set_function_name(request.function_name());
  for (const auto& versioned_key_request_tuple :
       request.versioned_key_request_tuples()) {
    ClientIdFunctionPair cid_function_pair = std::make_pair(
        request.client_id(), versioned_key_request_tuple.function_name());
    if (version_store.find(cid_function_pair) != version_store.end()) {
      bool found = false;
      for (const auto& head_key_chain_pair :
           version_store.at(cid_function_pair).second) {
        for (const auto& key_ptr_pair : head_key_chain_pair.second) {
          if (key_ptr_pair.first == versioned_key_request_tuple.key()) {
            found = true;
            CausalTuple* tp = response.add_tuples();
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
            versioned_key_request_tuple.key(), cid_function_pair.first,
            cid_function_pair.second);
      }
    } else {
      log->error(
          "Client ID function pair {},{} not available in versioned store.",
          cid_function_pair.first, cid_function_pair.second);
    }
  }
  // send response
  string resp_string;
  response.SerializeToString(&resp_string);
  kZmqUtil->send_string(resp_string, &pushers[request.response_address()]);
}

void versioned_key_response_handler(
    const string& serialized, StoreType& causal_cut_store,
    const VersionStoreType& version_store,
    std::unordered_map<ClientIdFunctionPair, PendingClientMetadata, PairHash>&
        pending_cross_metadata,
    const CausalCacheThread& cct, SocketCache& pushers,
    ZmqUtilInterface* kZmqUtil, logger log, std::unordered_map<ClientIdFunctionPair, ProtocolMetadata, PairHash>& protocol_matadata_map,
    StoreType& unmerged_store) {
  VersionedKeyResponse response;
  response.ParseFromString(serialized);

  ClientIdFunctionPair cid_function_pair =
      std::make_pair(response.client_id(), response.function_name());

  if (pending_cross_metadata.find(cid_function_pair) !=
      pending_cross_metadata.end()) {
    for (const auto& tuple : response.tuples()) {
      auto lattice = std::make_shared<CrossCausalLattice<SetLattice<string>>>(
          to_cross_causal_payload(deserialize_cross_causal(tuple.payload())));
      if (pending_cross_metadata[cid_function_pair].remote_read_tracker_.find(
              tuple.key()) != pending_cross_metadata[cid_function_pair]
                                  .remote_read_tracker_.end() &&
          pending_cross_metadata[cid_function_pair]
                  .remote_read_tracker_[tuple.key()]
                  .find(lattice->reveal().vector_clock) !=
              pending_cross_metadata[cid_function_pair]
                  .remote_read_tracker_[tuple.key()]
                  .end()) {
        // remove from tracker and merge to result_
        pending_cross_metadata[cid_function_pair]
            .remote_read_tracker_[tuple.key()]
            .erase(lattice->reveal().vector_clock);
        if (pending_cross_metadata[cid_function_pair]
                .remote_read_tracker_[tuple.key()]
                .size() == 0) {
          pending_cross_metadata[cid_function_pair].remote_read_tracker_.erase(
              tuple.key());
        }
        if (pending_cross_metadata[cid_function_pair].result_.find(
                tuple.key()) ==
            pending_cross_metadata[cid_function_pair].result_.end()) {
          pending_cross_metadata[cid_function_pair].result_[tuple.key()] =
              lattice;
        } else {
          pending_cross_metadata[cid_function_pair]
              .result_[tuple.key()] = causal_merge(
              pending_cross_metadata[cid_function_pair].result_[tuple.key()],
              lattice);
        }
      }
    }
    if (pending_cross_metadata[cid_function_pair].remote_read_tracker_.size() ==
        0) {
      // EXPERIMANTAL: merge result_ to unmerged_store to sync between caches
      for (const auto& pair : pending_cross_metadata[cid_function_pair].result_) {
        Key key = pair.first;
        if (unmerged_store.find(key) == unmerged_store.end()) {
          unmerged_store[key] = pair.second;
        } else {
          unsigned comp_result = causal_comparison(unmerged_store[key], pair.second);
          if (comp_result == kCausalLess) {
            unmerged_store[key] = pair.second;
          } else if (comp_result == kCausalConcurrent) {
            unmerged_store[key] = causal_merge(unmerged_store[key], pair.second);
          }
        }
      }
      send_executor_response(cid_function_pair, pending_cross_metadata, version_store, pushers, cct, log);
    }
  }
}