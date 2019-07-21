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

unsigned causal_comparison(
    const std::shared_ptr<CrossCausalLattice<SetLattice<string>>>& lhs,
    const std::shared_ptr<CrossCausalLattice<SetLattice<string>>>& rhs) {
  VectorClock lhs_prev_vc = lhs->reveal().vector_clock;
  VectorClock lhs_vc = lhs->reveal().vector_clock;
  VectorClock rhs_vc = rhs->reveal().vector_clock;
  lhs_vc.merge(rhs_vc);
  if (lhs_prev_vc == lhs_vc) {
    return kCausalGreaterOrEqual;
  } else if (lhs_vc == rhs_vc) {
    return kCausalLess;
  } else {
    return kCausalConcurrent;
  }
}

unsigned vector_clock_comparison(const VectorClock& lhs,
                                 const VectorClock& rhs) {
  VectorClock lhs_prev_vc = lhs;
  VectorClock lhs_vc = lhs;
  VectorClock rhs_vc = rhs;
  lhs_vc.merge(rhs_vc);
  if (lhs_prev_vc == lhs_vc) {
    return kCausalGreaterOrEqual;
  } else if (lhs_vc == rhs_vc) {
    return kCausalLess;
  } else {
    return kCausalConcurrent;
  }
}

std::shared_ptr<CrossCausalLattice<SetLattice<string>>> causal_merge(
    const std::shared_ptr<CrossCausalLattice<SetLattice<string>>>& lhs,
    const std::shared_ptr<CrossCausalLattice<SetLattice<string>>>& rhs) {
  unsigned comp_result = causal_comparison(lhs, rhs);
  if (comp_result == kCausalGreaterOrEqual) {
    return lhs;
  } else if (comp_result == kCausalLess) {
    return rhs;
  } else {
    auto result = std::make_shared<CrossCausalLattice<SetLattice<string>>>();
    result->merge(*lhs);
    result->merge(*rhs);
    return result;
  }
}

std::shared_ptr<CrossCausalLattice<SetLattice<string>>>
find_lattice_from_in_preparation(const InPreparationType& in_preparation,
                                 const Key& key, const VectorClock& vc) {
  for (const auto& pair : in_preparation) {
    for (const auto& inner_pair : pair.second.second) {
      if (inner_pair.first == key &&
          vector_clock_comparison(inner_pair.second->reveal().vector_clock,
                                  vc) == kCausalGreaterOrEqual) {
        return inner_pair.second;
      }
    }
  }
  return nullptr;
}

bool populate_in_preparation(
    const Key& head_key, const Key& dep_key,
    const std::shared_ptr<CrossCausalLattice<SetLattice<string>>>& lattice,
    InPreparationType& in_preparation) {
  if (in_preparation[head_key].second.find(dep_key) ==
      in_preparation[head_key].second.end()) {
    in_preparation[head_key].second[dep_key] = lattice;
    return true;
  } else {
    unsigned comp_result =
        causal_comparison(in_preparation[head_key].second[dep_key], lattice);
    if (comp_result == kCausalLess) {
      in_preparation[head_key].second[dep_key] = lattice;
      return true;
    } else if (comp_result == kCausalConcurrent) {
      in_preparation[head_key].second[dep_key] =
          causal_merge(in_preparation[head_key].second[dep_key], lattice);
      return true;
    } else {
      return false;
    }
  }
}

void recursive_dependency_check(
    const Key& head_key,
    const std::shared_ptr<CrossCausalLattice<SetLattice<string>>>& lattice,
    InPreparationType& in_preparation, const StoreType& causal_cut_store,
    const StoreType& unmerged_store, map<Key, set<Key>>& to_fetch_map,
    map<Key, std::unordered_map<VectorClock, set<Key>, VectorClockHash>>&
        cover_map,
    KvsAsyncClientInterface* client, logger log) {
  log->info("enter recursive dependency check, head key {}", head_key);
  for (const auto& pair : lattice->reveal().dependency.reveal()) {
    Key dep_key = pair.first;
    // first, check if the dependency is already satisfied in the causal cut
    if (causal_cut_store.find(dep_key) != causal_cut_store.end() &&
        vector_clock_comparison(
            causal_cut_store.at(dep_key)->reveal().vector_clock,
            lattice->reveal().dependency.reveal().at(dep_key)) ==
            kCausalGreaterOrEqual) {
      continue;
    }
    // then, check if the dependency is already satisfied in the in_preparation
    auto target_lattice = find_lattice_from_in_preparation(
        in_preparation, dep_key,
        lattice->reveal().dependency.reveal().at(dep_key));
    if (target_lattice != nullptr) {
      if (populate_in_preparation(head_key, dep_key, target_lattice,
                                  in_preparation)) {
        recursive_dependency_check(head_key, target_lattice, in_preparation,
                                   causal_cut_store, unmerged_store,
                                   to_fetch_map, cover_map, client, log);
      }
      // in_preparation[head_key].second[dep_key] = target_lattice;
    } else {
      // check if the dependency is satisfied in unmerged_store (unmerged store
      // should always dominate the in_preparation)
      if (unmerged_store.find(dep_key) != unmerged_store.end() &&
          vector_clock_comparison(
              unmerged_store.at(dep_key)->reveal().vector_clock,
              lattice->reveal().dependency.reveal().at(dep_key)) ==
              kCausalGreaterOrEqual) {
        if (populate_in_preparation(head_key, dep_key,
                                    unmerged_store.at(dep_key),
                                    in_preparation)) {
          recursive_dependency_check(head_key, unmerged_store.at(dep_key),
                                     in_preparation, causal_cut_store,
                                     unmerged_store, to_fetch_map, cover_map,
                                     client, log);
        }
      } else {
        // we issue GET to KVS
        to_fetch_map[head_key].insert(dep_key);
        cover_map[dep_key][lattice->reveal().dependency.reveal().at(dep_key)]
            .insert(head_key);
        log->info("firing get request for key {} in recursive dependency check", dep_key);
        client->get_async(dep_key);
      }
    }
  }
}

Address find_address(
    const Key& key, const VectorClock& vc,
    const map<Address, map<Key, VectorClock>>& prior_causal_chains) {
  for (const auto& address_map_pair : prior_causal_chains) {
    for (const auto& key_vc_pair : address_map_pair.second) {
      if (key_vc_pair.first == key &&
          vector_clock_comparison(vc, key_vc_pair.second) == kCausalLess) {
        // find a remote vector clock that dominates the local one, so read from
        // the remote node
        return address_map_pair.first;
      }
    }
  }
  // we are good to read from the local causal cache
  return "";
}

void save_versions(const ClientIdFunctionPair& cid_function_pair,
                   const Key& head_key, const Key& key,
                   VersionStoreType& version_store,
                   const StoreType& causal_cut_store,
                   const set<Key>& full_read_set, set<Key>& observed_keys) {
  if (observed_keys.find(key) == observed_keys.end()) {
    observed_keys.insert(key);
    if (full_read_set.find(key) != full_read_set.end()) {
      version_store[cid_function_pair].second[head_key][key] =
          causal_cut_store.at(key);
    }
    for (const auto& pair :
         causal_cut_store.at(key)->reveal().dependency.reveal()) {
      save_versions(cid_function_pair, head_key, pair.first, version_store,
                    causal_cut_store, full_read_set, observed_keys);
    }
  }
}

/*bool fire_remote_read_requests(PendingClientMetadata& metadata,
                               VersionStoreType& version_store,
                               const StoreType& causal_cut_store,
                               SocketCache& pushers,
                               const CausalCacheThread& cct, logger log) {
  // first we determine which key should be read from remote
  bool remote_request = false;

  map<Address, VersionedKeyRequest> addr_request_map;

  for (const Key& key : metadata.read_set_) {
    if (metadata.dne_set_.find(key) != metadata.dne_set_.end()) {
      // the key dne
      metadata.serialized_local_payload_[key] = "";
      continue;
    }
    if (causal_cut_store.find(key) == causal_cut_store.end()) {
      // no key in local causal cache, find a remote and fire request
      remote_request = true;
      Address remote_addr =
          find_address(key, VectorClock(), metadata.prior_causal_chains_);
      if (addr_request_map.find(remote_addr) == addr_request_map.end()) {
        addr_request_map[remote_addr].set_id(metadata.client_id_);
        addr_request_map[remote_addr].set_response_address(
            cct.causal_cache_versioned_key_response_connect_address());
      }
      addr_request_map[remote_addr].add_keys(key);
      metadata.remote_read_set_.insert(key);
    } else {
      Address remote_addr =
          find_address(key, causal_cut_store.at(key)->reveal().vector_clock,
                       metadata.prior_causal_chains_);
      if (remote_addr != "") {
        // we need to read from remote
        remote_request = true;

        if (addr_request_map.find(remote_addr) == addr_request_map.end()) {
          addr_request_map[remote_addr].set_id(metadata.client_id_);
          addr_request_map[remote_addr].set_response_address(
              cct.causal_cache_versioned_key_response_connect_address());
        }
        addr_request_map[remote_addr].add_keys(key);
        metadata.remote_read_set_.insert(key);
      } else {
        // we can read from local
        metadata.serialized_local_payload_[key] =
            serialize(*(causal_cut_store.at(key)));
        // copy pointer to keep the version (only for those in the future read
        // set)
        set<Key> observed_keys;
        save_versions(metadata.client_id_, key, version_store, causal_cut_store,
                      metadata.future_read_set_, observed_keys);
      }
    }
  }

  for (const auto& pair : addr_request_map) {
    // send request
    string req_string;
    pair.second.SerializeToString(&req_string);
    kZmqUtil->send_string(req_string, &pushers[pair.first]);
  }
  return remote_request;
}

void respond_to_client(
    map<Address, PendingClientMetadata>& pending_cross_metadata,
    const Address& addr, const StoreType& causal_cut_store,
    const VersionStoreType& version_store, SocketCache& pushers,
    const CausalCacheThread& cct, const StoreType& unmerged_store) {
  CausalResponse response;

  for (const Key& key : pending_cross_metadata[addr].read_set_) {
    CausalTuple* tp = response.add_tuples();
    tp->set_key(key);

    if (pending_cross_metadata[addr].dne_set_.find(key) !=
        pending_cross_metadata[addr].dne_set_.end()) {
      // key dne
      tp->set_error(1);
    } else {
      tp->set_error(0);
      tp->set_payload(serialize(*(causal_cut_store.at(key))));
    }
  }

  response.set_versioned_key_query_addr(
      cct.causal_cache_versioned_key_request_connect_address());

  if (version_store.find(pending_cross_metadata[addr].client_id_) !=
      version_store.end()) {
    for (const auto& pair :
         version_store.at(pending_cross_metadata[addr].client_id_)) {
      VersionedKey* vk = response.add_versioned_keys();
      vk->set_key(pair.first);
      auto ptr = vk->mutable_vector_clock();
      for (const auto& client_version_pair :
           pair.second->reveal().vector_clock.reveal()) {
        (*ptr)[client_version_pair.first] = client_version_pair.second.reveal();
      }
    }
  }

  // send response
  string resp_string;
  response.SerializeToString(&resp_string);
  kZmqUtil->send_string(resp_string, &pushers[addr]);
  // GC
  pending_cross_metadata.erase(addr);
}*/

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
    const CausalCacheThread& cct) {
  // first, update unmerged store
  if (unmerged_store.find(key) == unmerged_store.end()) {
    // key doesn't exist in unmerged map
    unmerged_store[key] = lattice;
    // check call back addresses for single obj causal consistency
    if (single_callback_map.find(key) != single_callback_map.end()) {
      // notify clients
      for (const auto& addr : single_callback_map[key]) {
        // pending_single_metadata[addr].to_cover_set should have this
        // key, and we remove it

        if (pending_single_metadata.find(addr) !=
            pending_single_metadata.end()) {
          // first check if this key DNE, if so return
          if (vector_clock_comparison(
                  VectorClock(), unmerged_store[key]->reveal().vector_clock) ==
              kCausalGreaterOrEqual) {
            log->info("Key DNE error!");
            CausalGetResponse response;
            response.set_error(ErrorType::KEY_DNE);
            // send response
            string resp_string;
            response.SerializeToString(&resp_string);
            kZmqUtil->send_string(resp_string, &pushers[addr]);
            // GC
            pending_single_metadata.erase(addr);
          } else {
            pending_single_metadata[addr].to_cover_set_.erase(key);
            if (pending_single_metadata[addr].to_cover_set_.size() == 0) {
              log->info("Responding to {}", addr);
              CausalGetResponse response;
              response.set_error(ErrorType::NO_ERROR);
              for (const Key& key : pending_single_metadata[addr].read_set_) {
                CausalTuple* tp = response.add_tuples();
                tp->set_key(key);
                tp->set_payload(serialize(*(unmerged_store[key])));
              }
              // send response
              string resp_string;
              response.SerializeToString(&resp_string);
              kZmqUtil->send_string(resp_string, &pushers[addr]);
              // GC
              pending_single_metadata.erase(addr);
            }
          }
        }
      }
      single_callback_map.erase(key);
    }
  } else {
    unsigned comp_result = causal_comparison(unmerged_store.at(key), lattice);
    if (comp_result == kCausalLess) {
      unmerged_store[key] = lattice;
    } else if (comp_result == kCausalConcurrent) {
      unmerged_store[key] = causal_merge(unmerged_store.at(key), lattice);
    }
  }
  // then, inspect the to_fetch_map
  if (to_fetch_map.find(key) != to_fetch_map.end() &&
      to_fetch_map[key].size() == 0) {
    // here, we know that this key is queried by the client directly
    in_preparation[key].second[key] = unmerged_store[key];
    recursive_dependency_check(key, unmerged_store[key], in_preparation,
                               causal_cut_store, unmerged_store, to_fetch_map,
                               cover_map, client, log);
    if (to_fetch_map[key].size() == 0) {
      // this key has no dependency
      merge_into_causal_cut(key, causal_cut_store, in_preparation,
                            version_store, pending_cross_metadata, pushers, cct,
                            log, unmerged_store);
      to_fetch_map.erase(key);
    }
  }
  // then, check cover_map to see if this key covers any dependency
  if (cover_map.find(key) != cover_map.end()) {
    std::unordered_map<VectorClock, set<Key>, VectorClockHash> to_remove;
    // track the head keys whose dependency is NOT satisfied
    set<Key> dependency_not_satisfied;
    // track the head keys whose dependency might be satisfied
    set<Key> dependency_may_be_satisfied;
    // loop through the set to see if anything is covered
    for (const auto& pair : cover_map[key]) {
      if (vector_clock_comparison(unmerged_store.at(key)->reveal().vector_clock,
                                  pair.first) == kCausalGreaterOrEqual) {
        for (const auto& head_key : pair.second) {
          // found a dependency that is covered
          in_preparation[head_key].second[key] = unmerged_store[key];
          if (to_fetch_map[head_key].find(key) ==
              to_fetch_map[head_key].end()) {
            log->error("Missing dependency {} in the to_fetch_map of key {}.",
                       key, head_key);
          }
          dependency_may_be_satisfied.insert(head_key);
          to_remove[pair.first].insert(head_key);
        }
      } else {
        for (const auto& head_key : pair.second) {
          dependency_not_satisfied.insert(head_key);
        }
      }
    }
    // only remove from to_fetch_map if the dependency is truly satisfied
    for (const Key& head_key : dependency_may_be_satisfied) {
      if (dependency_not_satisfied.find(head_key) ==
          dependency_not_satisfied.end()) {
        to_fetch_map[head_key].erase(key);
      }
    }

    for (const auto& pair : to_remove) {
      cover_map[key].erase(pair.first);
      for (const auto& head_key : pair.second) {
        recursive_dependency_check(
            head_key, unmerged_store[key], in_preparation, causal_cut_store,
            unmerged_store, to_fetch_map, cover_map, client, log);
        if (to_fetch_map[head_key].size() == 0) {
          // all dependency is met
          merge_into_causal_cut(head_key, causal_cut_store, in_preparation,
                                version_store, pending_cross_metadata, pushers,
                                cct, log, unmerged_store);
          to_fetch_map.erase(head_key);
        }
      }
    }
    if (cover_map[key].size() == 0) {
      cover_map.erase(key);
    } else {
      // not fully covered, so we re-issue the read request
      log->info("firing get request for key {} in process_response", key);
      client->get_async(key);
    }
  }

  // if the original response from KVS actually says key dne, remove it from
  // unmerged map
  if (vector_clock_comparison(VectorClock(),
                              unmerged_store[key]->reveal().vector_clock) ==
      kCausalGreaterOrEqual) {
    unmerged_store.erase(key);
  }
}

void populate_causal_frontier(const Key& key, const VectorClock& vc,
                              const Address& cache_addr,
                              const string& function_name,
                              CausalFrontierType& causal_frontier) {
  std::unordered_set<VectorClock, VectorClockHash> to_remove;

  for (const auto& frontier_vc_payload_pair : causal_frontier[key]) {
    if (vector_clock_comparison(frontier_vc_payload_pair.first, vc) ==
        kCausalLess) {
      to_remove.insert(frontier_vc_payload_pair.first);
    } else if (vector_clock_comparison(frontier_vc_payload_pair.first, vc) ==
               kCausalGreaterOrEqual) {
      return;
    }
  }

  for (const VectorClock& to_remove_vc : to_remove) {
    causal_frontier[key].erase(to_remove_vc);
  }

  causal_frontier[key].insert(std::make_pair(
      vc, std::make_pair(
              false, VersionedKeyAddressMetadata(cache_addr, function_name))));
}

bool remove_from_local_readset(const Key& key,
                               CausalFrontierType& causal_frontier,
                               const set<Key>& read_set,
                               set<Key>& remove_candidate,
                               const VersionStoreType& version_store,
                               const ClientIdFunctionPair& cid_function_pair, logger log) {
  // first, check if causal_frontier have at least a version to read
  // first check if not reading from local is OK
  // if we don't read from local, we need to ensure two things:
  // 1: the consistency requirement from prior causal chain need to be satisfied
  // (via remote read) 2: the consistency of other local reads need to be
  // satisfied, otherwise try to not read them from local as well
  // ---
  if (causal_frontier.find(key) == causal_frontier.end()) {
    // abort
    return false;
  }
  VectorClock vc;
  for (const auto& vc_payload_pair : causal_frontier[key]) {
    if (vc_payload_pair.second.first) {
      vc.merge(vc_payload_pair.first);
    }
  }
  // now we check if all causal frontier is satisfied
  for (auto& vc_payload_pair : causal_frontier[key]) {
    if (vector_clock_comparison(vc, vc_payload_pair.first) !=
        kCausalGreaterOrEqual) {
      vc_payload_pair.second.first = true;
      vc.merge(vc_payload_pair.first);
    }
  }
  // at this point, condition 1 is satisfied
  for (const Key& other_key : read_set) {
    if (remove_candidate.find(other_key) == remove_candidate.end()) {
      if (version_store.at(cid_function_pair).second.find(other_key) !=
          version_store.at(cid_function_pair).second.end()) {
        // we may not reach this branch if the executor request reaches before
        // the scheduler request
        if (version_store.at(cid_function_pair)
                    .second.at(other_key)
                    .find(key) != version_store.at(cid_function_pair)
                                      .second.at(other_key)
                                      .end() &&
            vector_clock_comparison(vc, version_store.at(cid_function_pair)
                                            .second.at(other_key)
                                            .at(key)
                                            ->reveal()
                                            .vector_clock) !=
                kCausalGreaterOrEqual) {
          // consider removing the "other_key" from read set
          log->info("considering key {} for removal", other_key);
          remove_candidate.insert(other_key);
          if (!remove_from_local_readset(other_key, causal_frontier, read_set,
                                         remove_candidate, version_store,
                                         cid_function_pair, log)) {
            return false;
          }
        }
      }
    }
  }
  return true;
}

CausalFrontierType construct_causal_frontier(const CausalGetRequest& request) {
  CausalFrontierType causal_frontier;
  for (const auto& prior_version_tuple : request.prior_version_tuples()) {
    // first, convert protobuf type to VectorClock
    VectorClock vc;
    for (const auto& key_version_pair :
         prior_version_tuple.versioned_key().vector_clock()) {
      vc.insert(key_version_pair.first, key_version_pair.second);
    }
    populate_causal_frontier(prior_version_tuple.versioned_key().key(), vc,
                             prior_version_tuple.cache_address(),
                             prior_version_tuple.function_name(),
                             causal_frontier);
  }
  return causal_frontier;
}

void optimistic_protocol(
    const ClientIdFunctionPair& cid_function_pair, const set<Key>& read_set,
    const VersionStoreType& version_store,
    const map<Key, VectorClock>& prior_read_map,
    std::unordered_map<ClientIdFunctionPair, PendingClientMetadata, PairHash>&
        pending_cross_metadata,
    SocketCache& pushers, const CausalCacheThread& cct,
    CausalFrontierType& causal_frontier, const Address& response_address, logger log) {
  // all keys present, need to check causal consistency
  // and figure out if we need to read anything from remote
  // ---
  // first, check from upstream to downstream to get minimum versions we must
  // read
  log->info("Entering optimistic protocol");
  for (const Key& key : read_set) {
    log->info("up -> down, checking key {}", key);
    if (causal_frontier.find(key) != causal_frontier.end()) {
      // figure out what key/addr need remote read assuming the local key is
      // read
      log->info("found {} in causal frontier", key);
      VectorClock vc;
      if (version_store.at(cid_function_pair).second.find(key) !=
          version_store.at(cid_function_pair).second.end()) {
        // we may not reach this branch if the executor request reaches before
        // the scheduler request
        vc = version_store.at(cid_function_pair)
                 .second.at(key)
                 .at(key)
                 ->reveal()
                 .vector_clock;
        // debug
        for (const auto& vec_pair : vc.reveal()) {
          log->info("vector clock of {} has cid {} v_num {}", key, vec_pair.first, vec_pair.second.reveal());
        }
      }
      for (auto& vc_payload_pair : causal_frontier[key]) {
        if (vector_clock_comparison(vc, vc_payload_pair.first) !=
            kCausalGreaterOrEqual) {
          log->info("setting remote read flag of key {} to be true", key);
          // set remote read flag to true
          vc_payload_pair.second.first = true;
          vc.merge(vc_payload_pair.first);
        }
      }
    }
  }
  // then for each local read, check version store to see if its chain
  // is inconsistent with a previous read. If so we try to find a version from
  // prior chain to read (recursively). If not possible then we need to abort
  log->info("checking from down->up");
  set<Key> remove_candidate;
  for (const Key& key : read_set) {
    if (remove_candidate.find(key) == remove_candidate.end()) {
      if (version_store.at(cid_function_pair).second.find(key) !=
          version_store.at(cid_function_pair).second.end()) {
        log->info("version store has key {}", key);
        // we may not reach this branch if the executor request reaches before
        // the scheduler request
        for (const auto& pair :
             version_store.at(cid_function_pair).second.at(key)) {
          if (prior_read_map.find(pair.first) != prior_read_map.end() &&
              vector_clock_comparison(prior_read_map.at(pair.first),
                                      pair.second->reveal().vector_clock) !=
                  kCausalGreaterOrEqual) {
            // consider removing this key from local readset
            log->info("considering key {} for removal", key);
            remove_candidate.insert(key);
            if (!remove_from_local_readset(key, causal_frontier, read_set,
                                           remove_candidate, version_store,
                                           cid_function_pair, log)) {
              // abort
              CausalGetResponse response;
              response.set_error(ErrorType::ABORT);
              // send response
              string resp_string;
              response.SerializeToString(&resp_string);
              kZmqUtil->send_string(resp_string, &pushers[response_address]);
              return;
            }
            break;
          }
        }
      }
    }
  }
  // reaching here means that we don't abort
  // issue remote read if any
  map<Address, VersionedKeyRequest> addr_request_map;
  for (const auto& pair : causal_frontier) {
    for (const auto& vc_payload_pair : pair.second) {
      if (vc_payload_pair.second.first) {
        // remote read
        log->info("remote read key {}", pair.first);
        const VersionedKeyAddressMetadata& metadata =
            vc_payload_pair.second.second;
        if (addr_request_map.find(metadata.cache_address_) ==
            addr_request_map.end()) {
          addr_request_map[metadata.cache_address_].set_response_address(
              cct.causal_cache_versioned_key_response_connect_address());
          addr_request_map[metadata.cache_address_].set_client_id(
              cid_function_pair.first);
          addr_request_map[metadata.cache_address_].set_function_name(
              cid_function_pair.second);
        }
        auto versioned_key_request_tuple_ptr =
            addr_request_map[metadata.cache_address_]
                .add_versioned_key_request_tuples();
        versioned_key_request_tuple_ptr->set_function_name(
            metadata.function_name_);
        versioned_key_request_tuple_ptr->set_key(pair.first);
        // populate pending metadata
        pending_cross_metadata[cid_function_pair]
            .remote_read_tracker_[pair.first]
            .insert(vc_payload_pair.first);
      }
    }
  }
  for (const auto& pair : addr_request_map) {
    // send request
    string req_string;
    pair.second.SerializeToString(&req_string);
    kZmqUtil->send_string(req_string, &pushers[pair.first]);
  }
  if (addr_request_map.size() != 0) {
    log->info("has remote read");
    // remote read sent, merge local read to pending map
    // debug
    log->info("printing version store keys");
    for (const auto& pair : version_store.at(cid_function_pair).second) {
      log->info("key is {}", pair.first);
      if (remove_candidate.find(pair.first) == remove_candidate.end()) {
        pending_cross_metadata[cid_function_pair].result_[pair.first] =
            pair.second.at(pair.first);
      }
    }
    pending_cross_metadata[cid_function_pair].executor_response_address_ =
        response_address;
    pending_cross_metadata[cid_function_pair].remove_set_ = remove_candidate;
  } else {
    log->info("all local read");
    // all local read
    // respond to executor
    CausalGetResponse response;

    response.set_error(ErrorType::NO_ERROR);

    for (const auto& pair : version_store.at(cid_function_pair).second) {
      log->info("local read key {}", pair.first);
      // first populate the requested tuple
      CausalTuple* tp = response.add_tuples();
      tp->set_key(pair.first);
      tp->set_payload(serialize(*(pair.second.at(pair.first))));
      // then populate prior_version_tuples
      if (remove_candidate.find(pair.first) == remove_candidate.end()) {
        for (const auto& key_ptr_pair : pair.second) {
          PriorVersionTuple* tp = response.add_prior_version_tuples();
          tp->set_cache_address(
              cct.causal_cache_versioned_key_request_connect_address());
          tp->set_function_name(cid_function_pair.second);
          auto vk = tp->mutable_versioned_key();
          vk->set_key(key_ptr_pair.first);
          auto ptr = vk->mutable_vector_clock();
          for (const auto& client_version_pair :
               key_ptr_pair.second->reveal().vector_clock.reveal()) {
            (*ptr)[client_version_pair.first] =
                client_version_pair.second.reveal();
          }
        }
      }
    }

    // send response
    string resp_string;
    response.SerializeToString(&resp_string);
    kZmqUtil->send_string(resp_string, &pushers[response_address]);
  }
}

void merge_into_causal_cut(
    const Key& key, StoreType& causal_cut_store,
    InPreparationType& in_preparation, VersionStoreType& version_store,
    std::unordered_map<ClientIdFunctionPair, PendingClientMetadata, PairHash>&
        pending_cross_metadata,
    SocketCache& pushers, const CausalCacheThread& cct, logger log,
    const StoreType& unmerged_store) {
  bool key_dne = false;
  // merge from in_preparation to causal_cut_store
  for (const auto& pair : in_preparation[key].second) {
    if (vector_clock_comparison(
            VectorClock(), pair.second->reveal().vector_clock) == kCausalLess) {
      // only merge when the key exists
      if (causal_cut_store.find(pair.first) == causal_cut_store.end()) {
        // key doesn't exist in causal cut store
        causal_cut_store[pair.first] = pair.second;
      } else {
        // we compare two lattices
        unsigned comp_result =
            causal_comparison(causal_cut_store[pair.first], pair.second);
        if (comp_result == kCausalLess) {
          causal_cut_store[pair.first] = pair.second;
        } else if (comp_result == kCausalConcurrent) {
          causal_cut_store[pair.first] =
              causal_merge(causal_cut_store[pair.first], pair.second);
        }
      }
    } else {
      key_dne = true;
    }
  }
  // notify executor and scheduler
  for (const auto& cid_function_pair : in_preparation[key].first) {
    if (pending_cross_metadata.find(cid_function_pair) !=
        pending_cross_metadata.end()) {
      if (key_dne) {
        log->info("key dne for cid function pair {} {}", cid_function_pair.first, cid_function_pair.second);
        version_store[cid_function_pair].first = true;
        if (pending_cross_metadata[cid_function_pair]
                .executor_response_address_ != "") {
          CausalGetResponse response;
          response.set_error(ErrorType::KEY_DNE);
          // send response
          string resp_string;
          response.SerializeToString(&resp_string);
          kZmqUtil->send_string(
              resp_string, &pushers[pending_cross_metadata[cid_function_pair]
                                        .executor_response_address_]);
        }
        if (pending_cross_metadata[cid_function_pair]
                .scheduler_response_address_ != "") {
          CausalSchedulerResponse response;
          response.set_client_id(cid_function_pair.first);
          response.set_function_name(cid_function_pair.second);
          response.set_succeed(false);
          // send response
          string resp_string;
          response.SerializeToString(&resp_string);
          kZmqUtil->send_string(
              resp_string, &pushers[pending_cross_metadata[cid_function_pair]
                                        .scheduler_response_address_]);
        }
        // GC
        pending_cross_metadata.erase(cid_function_pair);
      } else {
        pending_cross_metadata[cid_function_pair].to_cover_set_.erase(key);
        if (pending_cross_metadata[cid_function_pair].to_cover_set_.size() ==
            0) {
          log->info("all key covered for cid function pair {} {}", cid_function_pair.first, cid_function_pair.second);
          // all keys covered, first populate version store entry
          // set DNE to false
          version_store[cid_function_pair].first = false;
          for (const string& key :
               pending_cross_metadata[cid_function_pair].read_set_) {
            set<Key> observed_keys;
            if (causal_cut_store.find(key) != causal_cut_store.end()) {
              // for scheduler, this if statement should always pass because
              // causal frontier is set to empty
              save_versions(
                  cid_function_pair, key, key, version_store, causal_cut_store,
                  pending_cross_metadata[cid_function_pair].full_read_set_,
                  observed_keys);
            }
          }
          if (pending_cross_metadata[cid_function_pair]
                  .executor_response_address_ != "") {
            // follow same logic as before...
            optimistic_protocol(
                cid_function_pair,
                pending_cross_metadata[cid_function_pair].read_set_,
                version_store,
                pending_cross_metadata[cid_function_pair].prior_read_map_,
                pending_cross_metadata, pushers, cct,
                pending_cross_metadata[cid_function_pair].causal_frontier_,
                pending_cross_metadata[cid_function_pair]
                    .executor_response_address_, log);
          }
          if (pending_cross_metadata[cid_function_pair]
                  .scheduler_response_address_ != "") {
            CausalSchedulerResponse response;
            response.set_client_id(cid_function_pair.first);
            response.set_function_name(cid_function_pair.second);
            send_scheduler_response(response, cid_function_pair, version_store,
                                    pushers,
                                    pending_cross_metadata[cid_function_pair]
                                        .scheduler_response_address_);
          }
          if (pending_cross_metadata[cid_function_pair]
                  .remote_read_tracker_.size() == 0) {
            // GC only when no remote read determined by optimistic protocol
            pending_cross_metadata.erase(cid_function_pair);
          }
        }
      }
    }
  }
  // erase the chain in in_preparation
  in_preparation.erase(key);
}

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
    logger log) {
  log->info("covered locally called");
  bool covered = true;

  for (const string& key : read_set) {
    key_set.insert(key);

    if (causal_cut_store.find(key) == causal_cut_store.end() &&
        causal_frontier.find(key) == causal_frontier.end()) {
      // check if the key is in in_preparation
      if (in_preparation.find(key) != in_preparation.end()) {
        covered = false;
        to_cover.insert(key);
        in_preparation[key].first.insert(cid_function_pair);
      } else {
        to_fetch_map[key] = set<Key>();
        // check if key is in one of the sub-key of in_preparation or in
        // unmerged_store
        auto lattice = find_lattice_from_in_preparation(in_preparation, key);
        if (lattice != nullptr) {
          in_preparation[key].second[key] = lattice;
          recursive_dependency_check(key, lattice, in_preparation,
                                     causal_cut_store, unmerged_store,
                                     to_fetch_map, cover_map, client, log);
          if (to_fetch_map[key].size() == 0) {
            // all dependency met
            merge_into_causal_cut(key, causal_cut_store, in_preparation,
                                  version_store, pending_cross_metadata,
                                  pushers, cct, log, unmerged_store);
            to_fetch_map.erase(key);
          } else {
            in_preparation[key].first.insert(cid_function_pair);
            covered = false;
            to_cover.insert(key);
          }
        } else if (unmerged_store.find(key) != unmerged_store.end()) {
          in_preparation[key].second[key] = unmerged_store[key];
          recursive_dependency_check(key, unmerged_store[key], in_preparation,
                                     causal_cut_store, unmerged_store,
                                     to_fetch_map, cover_map, client, log);
          if (to_fetch_map[key].size() == 0) {
            // all dependency met
            merge_into_causal_cut(key, causal_cut_store, in_preparation,
                                  version_store, pending_cross_metadata,
                                  pushers, cct, log, unmerged_store);
            to_fetch_map.erase(key);
          } else {
            in_preparation[key].first.insert(cid_function_pair);
            covered = false;
            to_cover.insert(key);
          }
        } else {
          in_preparation[key].first.insert(cid_function_pair);
          covered = false;
          to_cover.insert(key);
          log->info("firing get request for key {} in covered locally", key);
          client->get_async(key);
        }
      }
    }
  }
  return covered;
}

// this assumes that the client id and executor address are already set
// and the request succeeded
void send_scheduler_response(CausalSchedulerResponse& response,
                             const ClientIdFunctionPair& cid_function_pair,
                             const VersionStoreType& version_store,
                             SocketCache& pushers,
                             const Address& scheduler_address) {
  response.set_succeed(true);
  // populate versioned keys
  for (const auto& head_key_chain_pair :
       version_store.at(cid_function_pair).second) {
    auto map_ptr = response.mutable_version_chain();
    for (const auto& pair : head_key_chain_pair.second) {
      auto vk_ptr = (*map_ptr)[head_key_chain_pair.first].add_versioned_keys();
      vk_ptr->set_key(pair.first);
      // assemble vector clock
      auto vc_ptr = vk_ptr->mutable_vector_clock();
      for (const auto& client_version_pair :
           pair.second->reveal().vector_clock.reveal()) {
        (*vc_ptr)[client_version_pair.first] =
            client_version_pair.second.reveal();
      }
    }
  }

  // send response
  string resp_string;
  response.SerializeToString(&resp_string);
  kZmqUtil->send_string(resp_string, &pushers[scheduler_address]);
}
