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

#ifndef FUNCTIONS_CACHE_INCLUDE_CAUSAL_CACHE_HANDLERS_HPP_
#define FUNCTIONS_CACHE_INCLUDE_CAUSAL_CACHE_HANDLERS_HPP_

#include "causal_cache_utils.hpp"

void get_request_handler(
    const string& serialized, set<Key>& key_set, StoreType& unmerged_store,
    InPreparationType& in_preparation, StoreType& causal_cut_store,
    VersionStoreType& version_store,
    map<Key, set<Address>>& single_callback_map,
    map<Address, PendingClientMetadata>& pending_single_metadata,
    std::unordered_map<ClientIdFunctionPair, PendingClientMetadata, PairHash>&
        pending_cross_metadata,
    map<Key, set<Key>>& to_fetch_map,
    map<Key, std::unordered_map<VectorClock, set<Key>, VectorClockHash>>&
        cover_map,
    SocketCache& pushers, KvsAsyncClientInterface* client, logger log,
    const CausalCacheThread& cct,
    std::unordered_map<ClientIdFunctionPair, StoreType, PairHash>&
        conservative_store,
    std::unordered_map<ClientIdFunctionPair, ProtocolMetadata, PairHash>& protocol_matadata_map);

void put_request_handler(const string& serialized, StoreType& unmerged_store,
                         StoreType& causal_cut_store,
                         VersionStoreType& version_store,
                         map<string, Address>& request_id_to_address_map,
                         KvsAsyncClientInterface* client, logger log);

void versioned_key_request_handler(const string& serialized,
                                   const VersionStoreType& version_store,
                                   SocketCache& pushers, logger log,
                                   ZmqUtilInterface* kZmqUtil);

void versioned_key_response_handler(
    const string& serialized, StoreType& causal_cut_store,
    VersionStoreType& version_store,
    std::unordered_map<ClientIdFunctionPair, PendingClientMetadata, PairHash>&
        pending_cross_metadata,
    const CausalCacheThread& cct, SocketCache& pushers,
    ZmqUtilInterface* kZmqUtil, logger log, std::unordered_map<ClientIdFunctionPair, ProtocolMetadata, PairHash>& protocol_matadata_map,
    StoreType& unmerged_store, InPreparationType& in_preparation,
    map<Key, set<Key>>& to_fetch_map,
    map<Key, std::unordered_map<VectorClock, set<Key>, VectorClockHash>>&
        cover_map, KvsAsyncClientInterface* client);

void kvs_response_handler(
    const KeyResponse& response, StoreType& unmerged_store,
    InPreparationType& in_preparation, StoreType& causal_cut_store,
    VersionStoreType& version_store,
    map<Key, set<Address>>& single_callback_map,
    map<Address, PendingClientMetadata>& pending_single_metadata,
    std::unordered_map<ClientIdFunctionPair, PendingClientMetadata, PairHash>&
        pending_cross_metadata,
    map<Key, set<Key>>& to_fetch_map,
    map<Key, std::unordered_map<VectorClock, set<Key>, VectorClockHash>>&
        cover_map,
    SocketCache& pushers, KvsAsyncClientInterface* client, logger log,
    const CausalCacheThread& cct,
    map<string, Address>& request_id_to_address_map,
    std::unordered_map<ClientIdFunctionPair, ProtocolMetadata, PairHash>& protocol_matadata_map);

void periodic_migration_handler(
    const StoreType& unmerged_store, InPreparationType& in_preparation,
    StoreType& causal_cut_store, VersionStoreType& version_store,
    std::unordered_map<ClientIdFunctionPair, PendingClientMetadata, PairHash>&
        pending_cross_metadata,
    map<Key, set<Key>>& to_fetch_map,
    map<Key, std::unordered_map<VectorClock, set<Key>, VectorClockHash>>&
        cover_map,
    SocketCache& pushers, KvsAsyncClientInterface* client,
    const CausalCacheThread& cct, logger log,
    std::unordered_map<ClientIdFunctionPair, ProtocolMetadata, PairHash>& protocol_matadata_map);

void scheduler_request_handler(
    const string& serialized, set<Key>& key_set, StoreType& unmerged_store,
    InPreparationType& in_preparation, StoreType& causal_cut_store,
    VersionStoreType& version_store,
    std::unordered_map<ClientIdFunctionPair, PendingClientMetadata, PairHash>&
        pending_cross_metadata,
    map<Key, set<Key>>& to_fetch_map,
    map<Key, std::unordered_map<VectorClock, set<Key>, VectorClockHash>>&
        cover_map,
    SocketCache& pushers, KvsAsyncClientInterface* client, logger log,
    const CausalCacheThread& cct,
    std::unordered_map<ClientIdFunctionPair, ProtocolMetadata, PairHash>& protocol_matadata_map);

void scheduler_key_shipping_request_handler(
    const string& serialized,
    map<string, pair<set<Address>, Address>>& pending_key_shipping_map,
    std::unordered_map<ClientIdFunctionPair, StoreType, PairHash>&
        conservative_store,
    const VersionStoreType& version_store, const CausalCacheThread& cct,
    SocketCache& pushers, std::unordered_map<ClientIdFunctionPair, ProtocolMetadata, PairHash>& protocol_matadata_map);

void key_shipping_request_handler(const string& serialized,
                                  const VersionStoreType& version_store,
                                  const CausalCacheThread& cct,
                                  SocketCache& pushers, logger log);

void key_shipping_response_handler(
    const string& serialized,
    map<string, pair<set<Address>, Address>>& pending_key_shipping_map,
    std::unordered_map<ClientIdFunctionPair, StoreType, PairHash>&
        conservative_store,
    const CausalCacheThread& cct, SocketCache& pushers);

void scheduler_remote_read_handler(
    const string& serialized, VersionStoreType& version_store,
    std::unordered_map<ClientIdFunctionPair, PendingClientMetadata, PairHash>&
        pending_cross_metadata,
    SocketCache& pushers, logger log,
    const CausalCacheThread& cct, std::unordered_map<ClientIdFunctionPair, ProtocolMetadata, PairHash>& protocol_matadata_map);

#endif  // FUNCTIONS_CACHE_INCLUDE_CAUSAL_CACHE_HANDLERS_HPP_
