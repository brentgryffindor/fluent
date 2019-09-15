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
    std::unordered_map<ClientIdFunctionPair, PendingClientMetadata, PairHash>&
        pending_cross_metadata,
    map<Key, set<Key>>& to_fetch_map,
    map<Key, std::unordered_map<VectorClock, set<Key>, VectorClockHash>>&
        cover_map,
    SocketCache& pushers, KvsAsyncClientInterface* client, logger log,
    const CausalCacheThread& cct,
    std::unordered_map<ClientIdFunctionPair, StoreType, PairHash>&
        conservative_store,
    std::unordered_map<ClientIdFunctionPair, ProtocolMetadata, PairHash>& protocol_matadata_map) {
  CausalGetRequest request;
  request.ParseFromString(serialized);

  //std::cout << "response address is " + request.response_address() + "\n";

  if (request.consistency() == ConsistencyType::SINGLE) {
    //log->info("Receive GET in single mode");
    bool covered_locally = true;
    set<Key> read_set;
    set<Key> to_cover;
    // check if the keys are covered locally
    for (const Key& key : request.keys()) {
      //log->info("Key is {}", key);
      read_set.insert(key);
      key_set.insert(key);

      if (unmerged_store.find(key) == unmerged_store.end()) {
        covered_locally = false;
        to_cover.insert(key);
        single_callback_map[key].insert(request.response_address());
        //log->info("firing get request for key {} in single routine", key);
        client->get_async(key);
      }
    }
    if (!covered_locally) {
      pending_single_metadata[request.response_address()] =
          PendingClientMetadata(read_set, to_cover);
    } else {
      CausalGetResponse response;

      response.set_error(ErrorType::NO_ERROR);

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
    //log->info("Receive GET in cross mode");
    //std::cout << "Receive GET in cross mode\n";
    // convert the cached keys into a map
    map<Key, VectorClock> cached_versions;
    for (const auto& vk : request.cached_keys()) {
      VectorClock vc;
      for (const auto& key_version_pair : vk.vector_clock()) {
        vc.insert(key_version_pair.first, key_version_pair.second);
      }
      cached_versions[vk.key()] = vc;
    }

    auto cid_function_pair =
        std::make_pair(request.client_id(), request.function_name());
    //log->info("Executor: cid {} function name is {}", request.client_id(), request.function_name());
    if (request.conservative()) {
      // first check if this request is in conservative mode
      //log->info("GET for conservative protocol");
      //std::cout << "GET for conservative protocol\n";
      // fetch from conservative store
      if (conservative_store.find(cid_function_pair) !=
          conservative_store.end()) {
        CausalGetResponse response;
        response.set_error(ErrorType::NO_ERROR);
        // first check if the function result has been cached
        if (request.function_cached_keys_size() != 0) {
          // convert function cached keys into a map
          map<Key, VectorClock> function_cached_versions;
          for (const auto& vk : request.function_cached_keys()) {
            VectorClock vc;
            for (const auto& key_version_pair : vk.vector_clock()) {
              vc.insert(key_version_pair.first, key_version_pair.second);
            }
            function_cached_versions[vk.key()] = vc;
          }
          // function result cached, validate if the the key versions match
          bool cached = true;
          for (const Key& key : request.keys()) {
            if (conservative_store[cid_function_pair].find(key) != conservative_store[cid_function_pair].end()) {
              if (function_cached_versions.find(key) == function_cached_versions.end() || function_cached_versions.at(key).reveal() != conservative_store.at(cid_function_pair).at(key)->reveal().vector_clock.reveal()) {
                cached = false;
                break;
              }
            } else {
              log->error("key {} not found in conservative store.", key);
            }
          }
          if (cached) {
            response.set_cached(true);
            // send response
            string resp_string;
            response.SerializeToString(&resp_string);
            kZmqUtil->send_string(resp_string,
                                  &pushers[request.response_address()]);
            // GC the version store and conservative store
            // IMPORTANT: we disable GC of version store for benchmark purpose
            version_store.erase(cid_function_pair);
            conservative_store.erase(cid_function_pair);
            return;
          }
        }
        // function result not cached or versions don't match
        for (const Key& key : request.keys()) {
          if (conservative_store[cid_function_pair].find(key) !=
              conservative_store[cid_function_pair].end()) {
            // first check if the cached version is the same as what we want to return
            if (cached_versions.find(key) == cached_versions.end() || cached_versions.at(key).reveal() != conservative_store.at(cid_function_pair).at(key)->reveal().vector_clock.reveal()) {
              //log->info("key {} not cached by executor, sending...", key);
              CausalTuple* tp = response.add_tuples();
              tp->set_key(key);
              tp->set_payload(serialize(*(conservative_store[cid_function_pair][key])));
            }
          } else {
            log->error("key {} not found in conservative store.", key);
          }
        }
        // send response
        response.set_cached(false);
        string resp_string;
        response.SerializeToString(&resp_string);
        kZmqUtil->send_string(resp_string,
                              &pushers[request.response_address()]);
        // GC the version store and conservative store
        // IMPORTANT: we disable GC of version store for benchmark purpose
        version_store.erase(cid_function_pair);
        conservative_store.erase(cid_function_pair);
        // GC protocol metadata in case optimistic protocol skipped sending request to cache
        // due to detecting abort at the executor level
        if (protocol_matadata_map.find(cid_function_pair) != protocol_matadata_map.end()) {
          protocol_matadata_map.erase(cid_function_pair);
        }
      } else {
        log->error(
            "no matching cid function pair found in conservative store.");
      }
    } else {
      // first check protocol metadata
      if (protocol_matadata_map.find(cid_function_pair) == protocol_matadata_map.end()) {
        // scheduler msg hasn't arrived yet (no hint), so we proceed as usual
        //log->info("Executor: protocol metadata not present");
        // we first check if the version store is already populated by the scheduler
        // if so, means that all data should already be fetched or DNE
        //log->info("GET for optimistic protocol");
        //std::cout << "GET for optimistic protocol\n";
        if (version_store.find(cid_function_pair) != version_store.end()) {
          //log->info("version store already present");
          //std::cout << "version store already present\n";
          if (version_store[cid_function_pair].first) {
            //log->info("DNE");
            // some keys DNE
            CausalGetResponse response;
            response.set_error(ErrorType::KEY_DNE);
            // send response
            string resp_string;
            response.SerializeToString(&resp_string);
            kZmqUtil->send_string(resp_string,
                                  &pushers[request.response_address()]);
            //protocol_matadata_map[cid_function_pair].progress_ = kFinish;
          } else {
            //log->info("printing prior version tuples");
            // debug: print what's in the prior_version_tuples
            /*for (const auto& prior_version_tuple : request.prior_version_tuples()) {
              log->info("cache addr is {}", prior_version_tuple.cache_address());
              log->info("function name is {}", prior_version_tuple.function_name());
              log->info("key is {}", prior_version_tuple.versioned_key().key());
              for (const auto& vec_pair : prior_version_tuple.versioned_key().vector_clock()) {
                log->info("cid {} version num {}", vec_pair.first, vec_pair.second);
              }
            }*/
            CausalFrontierType causal_frontier =
                construct_causal_frontier(request);
            // construct a read set
            set<Key> read_set;
            for (const Key& key : request.keys()) {
              //std::cout << "requested key is " + key + "\n";
              read_set.insert(key);
            }
            // it's not possible to read different versions of the same key in
            // prior execution because otherwise it'll be aborted, so a simple map
            // is fine
            map<Key, VectorClock> prior_read_map;
            // store prior read to a map
            for (const auto& versioned_key : request.prior_read_map()) {
              // convert protobuf type to VectorClock
              for (const auto& key_version_pair : versioned_key.vector_clock()) {
                prior_read_map[versioned_key.key()].insert(
                    key_version_pair.first, key_version_pair.second);
              }
            }
            optimistic_protocol(cid_function_pair, read_set, version_store,
                                prior_read_map, pending_cross_metadata, pushers,
                                cct, causal_frontier, request.response_address(), log, cached_versions, protocol_matadata_map);
          }
        } else if (pending_cross_metadata.find(cid_function_pair) !=
                   pending_cross_metadata.end()) {
          // this means that the scheduler request arrives first and is still
          // fetching required data from Anna so we set the executor flag to true
          // and populate necessary metadata and wait for these data to arrive
          //log->info("scheduler arrives first, still fetching from Anna");
          pending_cross_metadata[cid_function_pair].executor_response_address_ =
              request.response_address();
          // construct causal frontier
          pending_cross_metadata[cid_function_pair].causal_frontier_ =
              construct_causal_frontier(request);
          // store prior read to a map
          for (const auto& versioned_key : request.prior_read_map()) {
            // convert protobuf type to VectorClock
            for (const auto& key_version_pair : versioned_key.vector_clock()) {
              pending_cross_metadata[cid_function_pair]
                  .prior_read_map_[versioned_key.key()]
                  .insert(key_version_pair.first, key_version_pair.second);
            }
          }
          // store full read set for constructing version store later
          for (const Key& key : request.full_read_set()) {
            pending_cross_metadata[cid_function_pair].full_read_set_.insert(key);
          }
          // store cached versions
          pending_cross_metadata[cid_function_pair].cached_versions_ = cached_versions;
        } else {
          // scheduler request hasn't arrived yet
          set<Key> read_set;
          for (const string& key : request.keys()) {
            //log->info("inserting {} to read set", key);
            read_set.insert(key);
          }
          set<Key> to_cover;
          CausalFrontierType causal_frontier = construct_causal_frontier(request);
          if (!covered_locally(cid_function_pair, read_set, to_cover, key_set,
                               unmerged_store, in_preparation, causal_cut_store,
                               version_store, pending_cross_metadata,
                               to_fetch_map, cover_map, pushers, client, cct,
                               causal_frontier, log, protocol_matadata_map)) {
            //log->info("not covered");
            pending_cross_metadata[cid_function_pair].read_set_ = read_set;
            pending_cross_metadata[cid_function_pair].to_cover_set_ = to_cover;
            pending_cross_metadata[cid_function_pair].executor_response_address_ =
                request.response_address();
            // store causal frontier
            pending_cross_metadata[cid_function_pair].causal_frontier_ =
                causal_frontier;
            // store prior read to a map
            for (const auto& versioned_key : request.prior_read_map()) {
              // convert protobuf type to VectorClock
              for (const auto& key_version_pair : versioned_key.vector_clock()) {
                pending_cross_metadata[cid_function_pair]
                    .prior_read_map_[versioned_key.key()]
                    .insert(key_version_pair.first, key_version_pair.second);
              }
            }
            // store full read set for constructing version store later
            for (const Key& key : request.full_read_set()) {
              pending_cross_metadata[cid_function_pair].full_read_set_.emplace(
                  std::move(key));
            }
            // store cached versions
            pending_cross_metadata[cid_function_pair].cached_versions_ = cached_versions;
          } else {
            //log->info("covered");
            // all keys covered, first populate version store entry
            // in this case, it's not possible that keys DNE
            version_store[cid_function_pair].first = false;
            // retrieve full read set
            set<Key> full_read_set;
            for (const string& key : request.full_read_set()) {
              full_read_set.insert(key);
            }
            for (const string& key : read_set) {
              set<Key> observed_keys;
              if (causal_cut_store.find(key) != causal_cut_store.end()) {
                // save version only when the local data exists
                save_versions(cid_function_pair, key, key, version_store,
                              causal_cut_store, full_read_set, observed_keys);
              }
            }
            // follow same logic as before...
            // it's not possible to read different versions of the same key in
            // prior execution because otherwise it'll be aborted, so a simple map
            // is fine
            map<Key, VectorClock> prior_read_map;
            // store prior read to a map
            for (const auto& versioned_key : request.prior_read_map()) {
              // convert protobuf type to VectorClock
              for (const auto& key_version_pair : versioned_key.vector_clock()) {
                prior_read_map[versioned_key.key()].insert(
                    key_version_pair.first, key_version_pair.second);
              }
            }
            optimistic_protocol(cid_function_pair, read_set, version_store,
                                prior_read_map, pending_cross_metadata, pushers,
                                cct, causal_frontier, request.response_address(), log, cached_versions, protocol_matadata_map);
          }
        }
      } else if (protocol_matadata_map[cid_function_pair].msg_ == kNoAction) {
        // skip optimistic protocol and read from version store and return, also GC protocol metadata
        //log->info("Executor: protocol metadata says NoAction");
        CausalGetResponse response;

        response.set_error(ErrorType::NO_ERROR);

        for (const Key& key : request.keys()) {
          //log->info("local read key {}", key);
          // first check if the cached version is the same as what we want to return
          if (cached_versions.find(key) == cached_versions.end() || cached_versions.at(key).reveal() != version_store.at(cid_function_pair).second.at(key).at(key)->reveal().vector_clock.reveal()) {
            //log->info("key {} not cached by executor, sending...", key);
            CausalTuple* tp = response.add_tuples();
            tp->set_key(key);
            tp->set_payload(
                serialize(*(version_store.at(cid_function_pair).second.at(key).at(key))));
          }
          // then populate prior_version_tuples
          for (const auto& key_ptr_pair : version_store.at(cid_function_pair).second.at(key)) {
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

        // send response
        string resp_string;
        response.SerializeToString(&resp_string);
        kZmqUtil->send_string(resp_string, &pushers[request.response_address()]);
        // GC protocol metadata
        protocol_matadata_map.erase(cid_function_pair);
      } else if (protocol_matadata_map[cid_function_pair].msg_ == kAbort) {
        //log->info("Executor: protocol metadata says Abort");
        // abort
        CausalGetResponse response;
        response.set_error(ErrorType::ABORT);
        // send response
        string resp_string;
        response.SerializeToString(&resp_string);
        kZmqUtil->send_string(resp_string, &pushers[request.response_address()]);
        // GC protocol metadata
        protocol_matadata_map.erase(cid_function_pair);
      } else if (protocol_matadata_map[cid_function_pair].msg_ == kRemoteRead) {
        //log->info("Executor: protocol metadata says RemoteRead");
        // scheduler msg arrived and already sent remote read to other caches
        if (pending_cross_metadata[cid_function_pair].remote_read_tracker_.size() == 0) {
          //log->info("RemoteRead already done, responding...");
          // if all remote read already done
          // we can return result now
          CausalGetResponse response;

          response.set_error(ErrorType::NO_ERROR);

          for (const auto& pair :
               pending_cross_metadata[cid_function_pair].result_) {
            // first check if the cached version is the same as what we want to return
            if (cached_versions.find(pair.first) == cached_versions.end() 
                || cached_versions.at(pair.first).reveal() != pair.second->reveal().vector_clock.reveal()) {
              //log->info("key {} not cached by executor, sending...", pair.first);
              CausalTuple* tp = response.add_tuples();
              tp->set_key(pair.first);
              tp->set_payload(serialize(*(pair.second)));
            }
            // then populate prior_version_tuples
            if (pending_cross_metadata[cid_function_pair].remove_set_.find(
                    pair.first) ==
                pending_cross_metadata[cid_function_pair].remove_set_.end()) {
              for (const auto& key_ptr_pair : version_store.at(cid_function_pair).second.at(pair.first)) {
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
          kZmqUtil->send_string(resp_string,
                                &pushers[request.response_address()]);
          // GC protocol metadata
          protocol_matadata_map.erase(cid_function_pair);
          // GC pending cross metadata
          pending_cross_metadata.erase(cid_function_pair);
        } else {
          //log->info("RemoteRead not done yet");
          // remote read not done yet
          // we need to save the cached versions for later checking and executor response address
          pending_cross_metadata[cid_function_pair].cached_versions_ = cached_versions;
          pending_cross_metadata[cid_function_pair].executor_response_address_ = request.response_address();
          // update progress to Remote Read
          protocol_matadata_map[cid_function_pair].progress_ = kRemoteRead;
        }
      } else {
        log->error("Invalid scheduler message type.");
      }
    }
  } else {
    log->error("Found non-causal consistency level.");
  }
}