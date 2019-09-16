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
const unsigned kCausalCacheReportThreshold = 1;

// period to migrate keys from unmerged store to causal cut store
const unsigned kMigrateThreshold = 10;

// macros used for vector clock comparison
const unsigned kCausalGreaterOrEqual = 0;
const unsigned kCausalLess = 1;
const unsigned kCausalConcurrent = 2;

using ClientIdFunctionPair = pair<string, string>;

using StoreType =
    map<Key, std::shared_ptr<CrossCausalLattice<SetLattice<string>>>>;

const unsigned kNotArrived = 0;
const unsigned kRemoteRead = 1;
const unsigned kFinish = 2;
const unsigned kAbort = 3;
const unsigned kNoAction = 4;

struct ProtocolMetadata {
  unsigned progress_ = kNotArrived;
  unsigned msg_ = kNotArrived;
};

struct VersionedKeyAddressMetadata {
  VersionedKeyAddressMetadata(Address cache_address, string function_name) :
      cache_address_(std::move(cache_address)),
      function_name_(std::move(function_name)) {}
  Address cache_address_;
  string function_name_;

  bool operator==(const VersionedKeyAddressMetadata& input) const {
    if (cache_address_ == input.cache_address_ &&
        function_name_ == input.function_name_) {
      return true;
    } else {
      return false;
    }
  }
};

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

struct PairHash {
  template <class T1, class T2>
  std::size_t operator()(const std::pair<T1, T2>& pair) const {
    return std::hash<T1>()(pair.first) ^ std::hash<T2>()(pair.second);
  }
};

using CausalFrontierType =
    map<Key,
        std::unordered_map<VectorClock, pair<bool, VersionedKeyAddressMetadata>,
                           VectorClockHash>>;

using InPreparationType = map<
    Key,
    pair<std::unordered_set<ClientIdFunctionPair, PairHash>,
         map<Key, std::shared_ptr<CrossCausalLattice<SetLattice<string>>>>>>;

struct PendingClientMetadata {
  PendingClientMetadata() = default;

  PendingClientMetadata(set<Key> read_set, set<Key> to_cover_set) :
      read_set_(std::move(read_set)),
      to_cover_set_(std::move(to_cover_set)) {}

  PendingClientMetadata(
      set<Key> read_set, set<Key> to_cover_set,
      CausalFrontierType causal_frontier, map<Key, VectorClock> prior_read_map,
      set<Key> full_read_set,
      map<Key, std::unordered_set<VectorClock, VectorClockHash>>
          remote_read_tracker) :
      read_set_(std::move(read_set)),
      to_cover_set_(std::move(to_cover_set)),
      causal_frontier_(std::move(causal_frontier)),
      full_read_set_(std::move(full_read_set)),
      remote_read_tracker_(std::move(remote_read_tracker)) {}

  set<Key> read_set_;
  set<Key> to_cover_set_;
  CausalFrontierType causal_frontier_;
  set<Key> full_read_set_;
  map<Key, std::unordered_set<VectorClock, VectorClockHash>>
      remote_read_tracker_;
  StoreType result_;
  Address executor_response_address_;
  Address scheduler_response_address_;
  map<Key, VectorClock> cached_versions_;
  // in case scheduler remote read done before executor request arrives
  // we use executor thread id to pre-send values to executor thread for caching
  unsigned executor_thread_id_;

  bool operator==(const PendingClientMetadata& input) const {
    if (read_set_ == input.read_set_ && to_cover_set_ == input.to_cover_set_ &&
        causal_frontier_ == input.causal_frontier_ &&
        full_read_set_ == input.full_read_set_ &&
        remote_read_tracker_ == input.remote_read_tracker_) {
      return true;
    } else {
      return false;
    }
  }
};

using VersionStoreType = std::unordered_map<
    ClientIdFunctionPair,
    pair<bool, map<Key, map<Key, std::shared_ptr<
                                     CrossCausalLattice<SetLattice<string>>>>>>,
    PairHash>;

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

// check if the given vector clock is dominated by any vector clock in the
// causal chain if so, return the address of the remote cache, else return empty
// string
Address find_address(
    const Key& key, const VectorClock& vc,
    const map<Address, map<Key, VectorClock>>& prior_causal_chains);

// save the relevant versions in case future caches may need them
// observed_key is initially passed in as an empty set
// to prevent infinite loop
void save_versions(const ClientIdFunctionPair& cid_function_pair,
                   const Key& head_key, const Key& key,
                   VersionStoreType& version_store,
                   const StoreType& causal_cut_store,
                   const set<Key>& full_read_set, set<Key>& observed_keys);

// figure out which key need to be retrieved remotely
// and fire the requests if needed
/*bool fire_remote_read_requests(PendingClientMetadata& metadata,
                               VersionStoreType& version_store,
                               const StoreType& causal_cut_store,
                               SocketCache& pushers,
                               const CausalCacheThread& cct, logger log);*/

// respond to client with keys all from the local causal cache
/*void respond_to_client(
    map<Address, PendingClientMetadata>& pending_cross_metadata,
    const Address& addr, const StoreType& causal_cut_store,
    const VersionStoreType& version_store, SocketCache& pushers,
    const CausalCacheThread& cct, const StoreType& unmerged_store);*/

// process a GET response received from the KVS
void process_response(
    const Key& key,
    const std::shared_ptr<CrossCausalLattice<SetLattice<string>>>& lattice,
    StoreType& unmerged_store, InPreparationType& in_preparation,
    StoreType& causal_cut_store, VersionStoreType& version_store,
    map<Key, set<Address>>& single_callback_map,
    map<Address, PendingClientMetadata>& pending_single_metadata,
    std::unordered_map<ClientIdFunctionPair, PendingClientMetadata, PairHash>&
        pending_cross_metadata,
    map<Key, set<Key>>& to_fetch_map,
    map<Key, std::unordered_map<VectorClock, set<Key>, VectorClockHash>>&
        cover_map,
    SocketCache& pushers, KvsAsyncClientInterface* client, logger log,
    const CausalCacheThread& cct,
    std::unordered_map<ClientIdFunctionPair, ProtocolMetadata, PairHash>& protocol_matadata_map);

// construct the causal frontier from previous causal caches
// this is later used to decide what keys should be read remotely
void populate_causal_frontier(const Key& key, const VectorClock& vc,
                              const Address& cache_addr,
                              const Address& executor_addr,
                              CausalFrontierType& causal_frontier);

CausalFrontierType construct_causal_frontier(const CausalGetRequest& request);

void optimistic_protocol(
    const ClientIdFunctionPair& cid_function_pair, const set<Key>& read_set,
    const VersionStoreType& version_store,
    std::unordered_map<ClientIdFunctionPair, PendingClientMetadata, PairHash>&
        pending_cross_metadata,
    SocketCache& pushers, const CausalCacheThread& cct,
    CausalFrontierType& causal_frontier, const Address& response_address, logger log, const map<Key, VectorClock>& cached_versions,
    std::unordered_map<ClientIdFunctionPair, ProtocolMetadata, PairHash>& protocol_matadata_map);

// merge a causal chain from in_preparation to causal cut store
// also notify clients that are waiting for the head key of the chain
void merge_into_causal_cut(
    const Key& key, StoreType& causal_cut_store,
    InPreparationType& in_preparation, VersionStoreType& version_store,
    std::unordered_map<ClientIdFunctionPair, PendingClientMetadata, PairHash>&
        pending_cross_metadata,
    SocketCache& pushers, const CausalCacheThread& cct, logger log,
    const StoreType& unmerged_store,
    std::unordered_map<ClientIdFunctionPair, ProtocolMetadata, PairHash>& protocol_matadata_map);

bool covered_locally(
    const ClientIdFunctionPair& cid_function_pair, const set<Key>& read_set,
    set<Key>& to_cover, set<Key>& key_set, StoreType& unmerged_store,
    InPreparationType& in_preparation, StoreType& causal_cut_store,
    VersionStoreType& version_store,
    std::unordered_map<ClientIdFunctionPair, PendingClientMetadata, PairHash>&
        pending_cross_metadata,
    map<Key, set<Key>>& to_fetch_map,
    map<Key, std::unordered_map<VectorClock, set<Key>, VectorClockHash>>&
        cover_map,
    SocketCache& pushers, KvsAsyncClientInterface* client,
    const CausalCacheThread& cct, CausalFrontierType& causal_frontier,
    logger log, std::unordered_map<ClientIdFunctionPair, ProtocolMetadata, PairHash>& protocol_matadata_map);

void send_scheduler_response(CausalSchedulerResponse& response,
                             const set<Key>& read_set,
                             const ClientIdFunctionPair& cid_function_pair,
                             const VersionStoreType& version_store,
                             SocketCache& pushers,
                             const Address& scheduler_address);

// to be used for responding to executor in versioned key response handler
void send_executor_response(const ClientIdFunctionPair& cid_function_pair, 
                            std::unordered_map<ClientIdFunctionPair, PendingClientMetadata, PairHash>& pending_cross_metadata,
                            const VersionStoreType& version_store, SocketCache& pushers, const CausalCacheThread& cct, logger log);

#endif  // FUNCTIONS_CACHE_INCLUDE_CAUSAL_CACHE_UTILS_HPP_
