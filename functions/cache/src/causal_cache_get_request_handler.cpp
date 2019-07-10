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
    std::unordered_map<AddressClientIdPair, PendingClientMetadata, PairHash>& pending_cross_metadata,
    map<Key, set<Key>>& to_fetch_map,
    map<Key, std::unordered_map<VectorClock, set<Key>, VectorClockHash>>&
        cover_map,
    SocketCache& pushers, KvsAsyncClientInterface* client, logger log,
    const CausalCacheThread& cct) {
  CausalRequest request;
  request.ParseFromString(serialized);

  if (request.consistency() == ConsistencyType::SINGLE) {
    bool covered_locally = true;
    set<Key> read_set;
    set<Key> to_cover;
    // check if the keys are covered locally
    for (const CausalTuple& tuple : request.tuples()) {
      Key key = tuple.key();
      read_set.insert(key);
      key_set.insert(key);

      if (unmerged_store.find(key) == unmerged_store.end()) {
        covered_locally = false;
        to_cover.insert(key);
        single_callback_map[key].insert(request.response_address());
        client->get_async(key);
      }
    }
    if (!covered_locally) {
      pending_single_metadata[request.response_address()] =
          PendingClientMetadata(request.id(), read_set, to_cover);
    } else {
      CausalResponse response;

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
    // we first check if the version store is already populated by the scheduler
    // if so, means that all data should already be fetched or DNE
    auto addr_cid_pair = std::make_pair(request.response_address(), request.id());
    if (version_store.find(addr_cid_pair) != version_store.end()) {
      if (version_store[addr_cid_pair].first) {
        // some keys DNE
        CausalResponse response;
        response.set_error(ErrorType::KEY_DNE);
        // send response
        string resp_string;
        response.SerializeToString(&resp_string);
        kZmqUtil->send_string(resp_string, &pushers[request.response_address()]);
      } else {
        CausalFrontierType causal_frontier = construct_causal_frontier(request);
        // construct a read set
        set<Key> read_set;
        for (const auto& tuple : request.tuples()) {
          read_set.insert(tuple.key());
        }
        // it's not possible to read different versions of the same key in prior execution
        // because otherwise it'll be aborted, so a simple map is fine
        map<Key, VectorClock> prior_read_map;
        // store prior read to a map
        for (const auto& versioned_key : request.prior_read_map()) {
          // convert protobuf type to VectorClock
          for (const auto& key_version_pair : versioned_key.vector_clock()) {
            prior_read_map[versioned_key.key()].insert(key_version_pair.first, key_version_pair.second);
          }
        }
        optimistic_protocol(read_set, version_store, prior_read_map, pending_cross_metadata, pushers, cct, causal_frontier, addr_cid_pair.first, addr_cid_pair.second);
      }
    } else if (pending_cross_metadata.find(addr_cid_pair) != pending_cross_metadata.end()) {
      // this means that the scheduler request arrives first and is still fetching required data from Anna
      // so we set the executor flag to true and populate necessary metadata and wait for these data to arrive
      pending_cross_metadata[addr_cid_pair].respond_to_executor_ = true;
      // construct causal frontier
      pending_cross_metadata[addr_cid_pair].causal_frontier_ = construct_causal_frontier(request);
      // store prior read to a map
      for (const auto& versioned_key : request.prior_read_map()) {
        // convert protobuf type to VectorClock
        for (const auto& key_version_pair : versioned_key.vector_clock()) {
          pending_cross_metadata[addr_cid_pair].prior_read_map_[versioned_key.key()].insert(key_version_pair.first, key_version_pair.second);
        }
      }
      // store full read set for constructing version store later
      for (const Key& key : request.full_read_set()) {
        pending_cross_metadata[addr_cid_pair].full_read_set_.emplace(std::move(key));
      }
    } else {
      // scheduler request hasn't arrived yet
      set<Key> read_set;
      set<Key> to_cover;
      CausalFrontierType causal_frontier = construct_causal_frontier(request);
      if (!covered_locally(read_set, to_cover, key_set, unmerged_store, in_preparation, causal_cut_store, 
                          version_store, pending_cross_metadata, to_fetch_map, cover_map, pushers, client, cct, causal_frontier)) {
        pending_cross_metadata[addr_cid_pair].read_set_ = read_set;
        pending_cross_metadata[addr_cid_pair].to_cover_set_ =
            to_cover;
        pending_cross_metadata[addr_cid_pair].respond_to_executor_ = true;
        // store causal frontier
        pending_cross_metadata[addr_cid_pair].causal_frontier_ = causal_frontier;
        // store prior read to a map
        for (const auto& versioned_key : request.prior_read_map()) {
          // convert protobuf type to VectorClock
          for (const auto& key_version_pair : versioned_key.vector_clock()) {
            pending_cross_metadata[addr_cid_pair].prior_read_map_[versioned_key.key()].insert(key_version_pair.first, key_version_pair.second);
          }
        }
        // store full read set for constructing version store later
        for (const Key& key : request.full_read_set()) {
          pending_cross_metadata[addr_cid_pair].full_read_set_.emplace(std::move(key));
        }
      } else {
        // all keys covered, first populate version store entry
        // in this case, it's not possible that keys DNE
        version_store[addr_cid_pair].first = false;
        // retrieve full read set
        set<Key> full_read_set;
        for (string& key : request.full_read_set()) {
          full_read_set.emplace(std::move(key));
        }
        for (const string& key : read_set) {
          set<Key> observed_keys;
          if (causal_cut_store.find(key) != causal_cut_store.end()) {
            // save version only when the local data exists
            save_versions(addr_cid_pair, key, key, version_store, causal_cut_store,
                          full_read_set, observed_keys);
          }
        }
        // follow same logic as before...
        // it's not possible to read different versions of the same key in prior execution
        // because otherwise it'll be aborted, so a simple map is fine
        map<Key, VectorClock> prior_read_map;
        // store prior read to a map
        for (const auto& versioned_key : request.prior_read_map()) {
          // convert protobuf type to VectorClock
          for (const auto& key_version_pair : versioned_key.vector_clock()) {
            prior_read_map[versioned_key.key()].insert(key_version_pair.first, key_version_pair.second);
          }
        }
        optimistic_protocol(read_set, version_store, prior_read_map, pending_cross_metadata, pushers, cct, causal_frontier, addr_cid_pair.first, addr_cid_pair.second);
      }
    }
  } else {
    log->error("Found non-causal consistency level.");
  }
}