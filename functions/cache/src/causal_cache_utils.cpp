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

void merge_into_causal_cut(
    const Key& key, StoreType& causal_cut_store,
    InPreparationType& in_preparation, VersionStoreType& version_store,
    map<string, PendingClientMetadata>& pending_cross_metadata,
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
  // notify scheduler
  for (const string& cid : in_preparation[key].first) {
    if (pending_cross_metadata.find(cid) !=
        pending_cross_metadata.end()) {
      if (key_dne) {
        log->info("key dne for cid {}", cid);
        CausalSchedulerResponse response;
        response.set_client_id(cid);
        response.set_succeed(false);
        // send response
        string resp_string;
        response.SerializeToString(&resp_string);
        kZmqUtil->send_string(
            resp_string, &pushers[pending_cross_metadata[cid]
                                      .scheduler_response_address_]);
        // GC
        pending_cross_metadata.erase(cid);
      } else {
        pending_cross_metadata[cid].to_cover_set_.erase(key);
        if (pending_cross_metadata[cid].to_cover_set_.size() == 0) {
          log->info("all key covered for cid {}", cid);
          // all keys covered, first populate version store entry
          // set DNE to false
          for (const string& key :
               pending_cross_metadata[cid].read_set_) {
            version_store[cid][key] = causal_cut_store.at(key);
          }
          CausalSchedulerResponse response;
          response.set_client_id(cid);
          response.set_succeed(true);
          // send response
          string resp_string;
          response.SerializeToString(&resp_string);
          kZmqUtil->send_string(resp_string, &pushers[pending_cross_metadata[cid]
                                      .scheduler_response_address_]);
          pending_cross_metadata.erase(cid);
        }
      }
    }
  }
  // erase the chain in in_preparation
  in_preparation.erase(key);
}

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
    const CausalCacheThread& cct, logger log) {
  log->info("covered locally called");
  bool covered = true;

  for (const string& key : read_set) {
    key_set.insert(key);

    if (causal_cut_store.find(key) == causal_cut_store.end()) {
      // check if the key is in in_preparation
      if (in_preparation.find(key) != in_preparation.end()) {
        covered = false;
        to_cover.insert(key);
        in_preparation[key].first.insert(client_id);
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
            in_preparation[key].first.insert(client_id);
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
            in_preparation[key].first.insert(client_id);
            covered = false;
            to_cover.insert(key);
          }
        } else {
          in_preparation[key].first.insert(client_id);
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
