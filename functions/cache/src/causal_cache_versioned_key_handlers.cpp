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
                                   VersionStoreType& version_store,
                                   SocketCache& pushers, logger log,
                                   ZmqUtilInterface* kZmqUtil) {
  VersionedKeyRequest request;
  request.ParseFromString(serialized);

  VersionedKeyResponse response;
  response.set_client_id(request.client_id());
  response.set_function_name(request.function_name());
  for (const auto& versioned_key_request_tuple : request.versioned_key_request_tuples()) {
    ClientIdFunctionPair cid_function_pair = std::make_pair(request.client_id(), versioned_key_request_tuple.function_name());
    if (version_store.find(cid_function_pair) != version_store.end()) {
      bool found = false;
      for (const auto& head_key_chain_pair : version_store[cid_function_pair].second) {
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
            "Requested key {} for client ID function pair {},{} not available in versioned "
            "store.",
            versioned_key_request_tuple.key(), cid_function_pair.first, cid_function_pair.second);
      }
    } else {
      log->error("Client ID function pair {},{} not available in versioned store.", cid_function_pair.first, cid_function_pair.second);
    }
  }
  // send response
  string resp_string;
  response.SerializeToString(&resp_string);
  kZmqUtil->send_string(resp_string, &pushers[request.response_address()]);
}

void versioned_key_response_handler(
    const string& serialized, StoreType& causal_cut_store,
    VersionStoreType& version_store,
    std::unordered_map<ClientIdFunctionPair, PendingClientMetadata, PairHash>& pending_cross_metadata,
    map<string, set<Address>>& client_id_to_address_map,
    const CausalCacheThread& cct, SocketCache& pushers,
    ZmqUtilInterface* kZmqUtil, logger log) {
  VersionedKeyResponse response;
  response.ParseFromString(serialized);

  ClientIdFunctionPair cid_function_pair = std::make_pair(response.client_id(), response.function_name());

  if (pending_cross_metadata.find(cid_function_pair) != pending_cross_metadata.end()) {
    for (const auto& tuple : response.tuples()) {
      auto lattice = std::make_shared<CrossCausalLattice<SetLattice<string>>>(
              to_cross_causal_payload(deserialize_cross_causal(tuple.payload())));
      if (pending_cross_metadata[cid_function_pair].remote_read_tracker_.find(tuple.key()) != pending_cross_metadata[cid_function_pair].remote_read_tracker_.end()
          && pending_cross_metadata[cid_function_pair].remote_read_tracker_[tuple.key()].find(lattice->reveal().vector_clock) != pending_cross_metadata[cid_function_pair].remote_read_tracker_[tuple.key()].end()) {
        // remove from tracker and merge to result_
        pending_cross_metadata[cid_function_pair].remote_read_tracker_[tuple.key()].erase(lattice->reveal().vector_clock);
        if (pending_cross_metadata[cid_function_pair].remote_read_tracker_[tuple.key()].size() == 0) {
          pending_cross_metadata[cid_function_pair].remote_read_tracker_.erase(tuple.key());
        }
        if (pending_cross_metadata[cid_function_pair].result_.find(tuple.key()) == pending_cross_metadata[cid_function_pair].result_.end()) {
          pending_cross_metadata[cid_function_pair].result_[tuple.key()] = lattice;
        } else {
          pending_cross_metadata[cid_function_pair].result_[tuple.key()] = causal_merge(pending_cross_metadata[cid_function_pair].result_[tuple.key()], lattice);
        }
      }
    }
    // if no more remote read, return to executor and GC
    if (pending_cross_metadata[cid_function_pair].remote_read_tracker_.size() == 0) {
      // respond to executor
      CausalResponse response;

      for (const auto& pair : pending_cross_metadata[cid_function_pair].result_) {
        // first populate the requested tuple
        CausalTuple* tp = response.add_tuples();
        tp->set_key(pair.first);
        tp->set_payload(serialize(*(pair.second)));
      }
      for (const auto& pair : version_store.at(cid_function_pair).second) {
        // then populate prior_version_tuples
        for (const auto& key_ptr_pair : pair.second) {
          PriorVersionTuple* tp = response.add_prior_version_tuples();
          tp->set_cache_address(cct.causal_cache_versioned_key_request_connect_address());
          tp->set_function_name(cid_function_pair.second);
          VersionedKey vk;
          vk.set_key(key_ptr_pair.first);
          auto ptr = vk.mutable_vector_clock();
          for (const auto& client_version_pair :
               key_ptr_pair.second->reveal().vector_clock.reveal()) {
            (*ptr)[client_version_pair.first] = client_version_pair.second.reveal();
          }
          tp->set_versioned_key(std::move(vk));
        }
      }

      // send response
      string resp_string;
      response.SerializeToString(&resp_string);
      kZmqUtil->send_string(resp_string, &pushers[pending_cross_metadata[cid_function_pair].executor_response_address_]);
      // GC
      pending_cross_metadata.erase(cid_function_pair);
    }
  }
}