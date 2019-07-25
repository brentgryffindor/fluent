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

#ifndef FUNCTIONS_CACHE_INCLUDE_CAUSAL_CACHE_UTILS_HPP_
#define FUNCTIONS_CACHE_INCLUDE_CAUSAL_CACHE_UTILS_HPP_

#include "functions.pb.h"
#include "kvs_async_client.hpp"

// period to report to the KVS about its key set
const unsigned kCausalCacheReportThreshold = 5;

// period to migrate keys from unmerged store to causal cut store
const unsigned kMigrateThreshold = 10;

// macros used for vector clock comparison
const unsigned kCausalGreaterOrEqual = 0;
const unsigned kCausalLess = 1;
const unsigned kCausalConcurrent = 2;

using StoreType =
    map<Key, std::shared_ptr<CrossCausalLattice<SetLattice<string>>>>;

using InPreparationType = map<
    Key,
    pair<set<string>,
         map<Key, std::shared_ptr<CrossCausalLattice<SetLattice<string>>>>>>;

using VersionStoreType = map<string, StoreType>;

struct VectorClockHash {
  std::size_t operator()(const VectorClock& vc) const {
    std::size_t result = std::hash<int>()(-1);
    for (const auto& pair : vc.reveal()) {
      result = result ^ std::hash<string>()(pair.first) ^
               std::hash<unsigned>()(pair.second.reveal());
    }
    return result;
  }
};

struct PendingClientMetadata {
  PendingClientMetadata() = default;

  PendingClientMetadata(set<Key> read_set, set<Key> to_cover_set) :
      read_set_(std::move(read_set)),
      to_cover_set_(std::move(to_cover_set)) {}

  set<Key> read_set_;
  set<Key> to_cover_set_;
  Address scheduler_response_address_;

  bool operator==(const PendingClientMetadata& input) const {
    if (read_set_ == input.read_set_ && to_cover_set_ == input.to_cover_set_) {
      return true;
    } else {
      return false;
    }
  }
};

// given two cross causal lattices (of the same key), compare which one is
// bigger
unsigned causal_comparison(
    const std::shared_ptr<CrossCausalLattice<SetLattice<string>>>& lhs,
    const std::shared_ptr<CrossCausalLattice<SetLattice<string>>>& rhs);

// given two vector clocks (of the same key), compare which one is bigger
unsigned vector_clock_comparison(const VectorClock& lhs,
                                 const VectorClock& rhs);

// merge two causal lattices and if concurrent, return a pointer to a new merged
// lattice note that the original input lattices are not modified
std::shared_ptr<CrossCausalLattice<SetLattice<string>>> causal_merge(
    const std::shared_ptr<CrossCausalLattice<SetLattice<string>>>& lhs,
    const std::shared_ptr<CrossCausalLattice<SetLattice<string>>>& rhs);

// find a lattice from the in_preparation store that dominates the input lattice
// return a nullptr if not found
// TODO: may be more efficient to find the smallest lattice that
// satisfies the condition...
// TODO: maybe prioritize head key search?
std::shared_ptr<CrossCausalLattice<SetLattice<string>>>
find_lattice_from_in_preparation(const InPreparationType& in_preparation,
                                 const Key& key,
                                 const VectorClock& vc = VectorClock());

// return true if this lattice is not dominated by what's already in the
// in_preparation map this helper function is used to eliminate potential
// infinite loop
bool populate_in_preparation(
    const Key& head_key, const Key& dep_key,
    const std::shared_ptr<CrossCausalLattice<SetLattice<string>>>& lattice,
    InPreparationType& in_preparation);

// recursively check if the dependency of a key is met
void recursive_dependency_check(
    const Key& head_key,
    const std::shared_ptr<CrossCausalLattice<SetLattice<string>>>& lattice,
    InPreparationType& in_preparation, const StoreType& causal_cut_store,
    const StoreType& unmerged_store, map<Key, set<Key>>& to_fetch_map,
    map<Key, std::unordered_map<VectorClock, set<Key>, VectorClockHash>>&
        cover_map,
    KvsAsyncClientInterface* client, logger log);

// process a GET response received from the KVS
void process_response(
    const Key& key,
    const std::shared_ptr<CrossCausalLattice<SetLattice<string>>>& lattice,
    StoreType& unmerged_store, InPreparationType& in_preparation,
    StoreType& causal_cut_store, VersionStoreType& version_store,
    map<Key, set<Address>>& single_callback_map,
    map<Address, PendingClientMetadata>& pending_single_metadata,
    map<string, PendingClientMetadata>& pending_cross_metadata,
    map<Key, set<Key>>& to_fetch_map,
    map<Key, std::unordered_map<VectorClock, set<Key>, VectorClockHash>>&
        cover_map,
    SocketCache& pushers, KvsAsyncClientInterface* client, logger log,
    const CausalCacheThread& cct);

// merge a causal chain from in_preparation to causal cut store
// also notify clients that are waiting for the head key of the chain
void merge_into_causal_cut(
    const Key& key, StoreType& causal_cut_store,
    InPreparationType& in_preparation, VersionStoreType& version_store,
    map<string, PendingClientMetadata>& pending_cross_metadata,
    SocketCache& pushers, const CausalCacheThread& cct, logger log,
    const StoreType& unmerged_store);

bool covered_locally(
    const string& client_id, const set<Key>& read_set,
    set<Key>& to_cover, set<Key>& key_set, StoreType& unmerged_store,
    InPreparationType& in_preparation, StoreType& causal_cut_store,
    VersionStoreType& version_store,
    map<string, PendingClientMetadata>& pending_cross_metadata,
    map<Key, set<Key>>& to_fetch_map,
    map<Key, std::unordered_map<VectorClock, set<Key>, VectorClockHash>>&
        cover_map,
    SocketCache& pushers, KvsAsyncClientInterface* client,
    const CausalCacheThread& cct, logger log);

#endif  // FUNCTIONS_CACHE_INCLUDE_CAUSAL_CACHE_UTILS_HPP_
