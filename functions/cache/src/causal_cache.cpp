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

#include "yaml-cpp/yaml.h"

#include "causal_cache_handlers.hpp"
#include "causal_cache_utils.hpp"

ZmqUtil zmq_util;
ZmqUtilInterface* kZmqUtil = &zmq_util;

void warmup(StoreType& causal_cut_store) {
  SetLattice<string> value;
  value.insert(string(8, '0'));
  CrossCausalPayload<SetLattice<string>> payload;
  payload.vector_clock.insert("base", 1);
  payload.value = value;
  for (unsigned i = 1; i < 1000001; i++) {
    Key key = string(7 - std::to_string(i).length(), '0') + std::to_string(i);
    causal_cut_store[key] = std::make_shared<CrossCausalLattice<SetLattice<string>>>(payload);
  }
}

/*void warmup(VersionStoreType& version_store) {
  SetLattice<string> value;
  value.insert("00000");
  // func 1
  ClientIdFunctionPair cid_function_pair = std::make_pair("test_cid", "strmnp1");
  version_store[cid_function_pair].first = false;
  // func 1 key a
  CrossCausalPayload<SetLattice<string>> ccp_1_a;
  ccp_1_a.vector_clock.insert("base", 1);
  ccp_1_a.value = value;
  version_store[cid_function_pair].second["a"]["a"] = std::make_shared<CrossCausalLattice<SetLattice<string>>>(ccp_1_a);
  // func 1 key b
  CrossCausalPayload<SetLattice<string>> ccp_1_b;
  ccp_1_b.vector_clock.insert("base", 1);
  ccp_1_b.value = value;
  version_store[cid_function_pair].second["b"]["b"] = std::make_shared<CrossCausalLattice<SetLattice<string>>>(ccp_1_b);
  // func 1 key c
  CrossCausalPayload<SetLattice<string>> ccp_1_c;
  ccp_1_c.vector_clock.insert("base", 1);
  ccp_1_c.value = value;
  version_store[cid_function_pair].second["c"]["c"] = std::make_shared<CrossCausalLattice<SetLattice<string>>>(ccp_1_c);

  // func 2
  cid_function_pair = std::make_pair("test_cid", "strmnp2");
  version_store[cid_function_pair].first = false;
  // func 2 key d
  CrossCausalPayload<SetLattice<string>> ccp_2_d;
  ccp_2_d.vector_clock.insert("base", 1);
  ccp_2_d.value = value;
  version_store[cid_function_pair].second["d"]["d"] = std::make_shared<CrossCausalLattice<SetLattice<string>>>(ccp_2_d);
  // func 2 key e
  CrossCausalPayload<SetLattice<string>> ccp_2_e;
  ccp_2_e.vector_clock.insert("base", 1);
  ccp_2_e.value = value;
  version_store[cid_function_pair].second["e"]["e"] = std::make_shared<CrossCausalLattice<SetLattice<string>>>(ccp_2_e);

  // func 3
  cid_function_pair = std::make_pair("test_cid", "strmnp3");
  version_store[cid_function_pair].first = false;
  // func 3 key f
  CrossCausalPayload<SetLattice<string>> ccp_3_f;
  ccp_3_f.vector_clock.insert("base", 1);
  ccp_3_f.dependency.insert("d", VectorClock({{"base", 2}}));
  ccp_3_f.value = value;
  version_store[cid_function_pair].second["f"]["f"] = std::make_shared<CrossCausalLattice<SetLattice<string>>>(ccp_3_f);
  CrossCausalPayload<SetLattice<string>> ccp_3_f_d;
  ccp_3_f_d.vector_clock.insert("base", 2);
  ccp_3_f_d.value = value;
  version_store[cid_function_pair].second["f"]["d"] = std::make_shared<CrossCausalLattice<SetLattice<string>>>(ccp_3_f_d);
  // func 3 key g
  CrossCausalPayload<SetLattice<string>> ccp_3_g;
  ccp_3_g.vector_clock.insert("base", 1);
  ccp_3_g.value = value;
  version_store[cid_function_pair].second["g"]["g"] = std::make_shared<CrossCausalLattice<SetLattice<string>>>(ccp_3_g);
}*/

/*void warmup(VersionStoreType& version_store) {
  vector<SetLattice<string>> values;
  // setup values vector
  SetLattice<string> value1;
  value1.insert(string(1048576, '0'));
  values.push_back(value1);
  SetLattice<string> value2;
  value2.insert(string(262144, '0'));
  values.push_back(value2);
  SetLattice<string> value3;
  value3.insert(string(65536, '0'));
  values.push_back(value3);
  SetLattice<string> value4;
  value4.insert(string(16384, '0'));
  values.push_back(value4);
  SetLattice<string> value5;
  value5.insert(string(4096, '0'));
  values.push_back(value5);
  SetLattice<string> value6;
  value6.insert(string(1024, '0'));
  values.push_back(value6);
  SetLattice<string> value7;
  value7.insert(string(256, '0'));
  values.push_back(value7);

  // argument a
  ClientIdFunctionPair cid_function_pair = std::make_pair("test_cid", "strmnp1");
  version_store[cid_function_pair].first = false;
  for (unsigned i = 0; i < 2200; i += 7) {
    Key key = string(5 - std::to_string(i).length(), '0') + std::to_string(i);
    CrossCausalPayload<SetLattice<string>> ccp_1_a;
    ccp_1_a.vector_clock.insert("base", 1);
    unsigned dep_index = i + 3;
    Key dep_key = string(5 - std::to_string(dep_index).length(), '0') + std::to_string(dep_index);
    ccp_1_a.dependency.insert(dep_key, VectorClock({{"base", 1}}));
    ccp_1_a.value = values[i%7];
    version_store[cid_function_pair].second[key][key] = std::make_shared<CrossCausalLattice<SetLattice<string>>>(ccp_1_a);
    CrossCausalPayload<SetLattice<string>> ccp_1_a_d;
    ccp_1_a_d.vector_clock.insert("base", 1);
    ccp_1_a_d.value = values[dep_index % 7];
    version_store[cid_function_pair].second[key][dep_key] = std::make_shared<CrossCausalLattice<SetLattice<string>>>(ccp_1_a_d);
  }
  // argument b
  for (unsigned i = 1; i < 2200; i += 7) {
    Key key = string(5 - std::to_string(i).length(), '0') + std::to_string(i);
    CrossCausalPayload<SetLattice<string>> ccp_1_b;
    ccp_1_b.vector_clock.insert("base", 1);
    ccp_1_b.value = values[i%7];
    version_store[cid_function_pair].second[key][key] = std::make_shared<CrossCausalLattice<SetLattice<string>>>(ccp_1_b);
  }
  // argument c
  for (unsigned i = 2; i < 2200; i += 7) {
    Key key = string(5 - std::to_string(i).length(), '0') + std::to_string(i);
    CrossCausalPayload<SetLattice<string>> ccp_1_c;
    ccp_1_c.vector_clock.insert("base", 1);
    ccp_1_c.value = values[i%7];
    version_store[cid_function_pair].second[key][key] = std::make_shared<CrossCausalLattice<SetLattice<string>>>(ccp_1_c);
  }
  // argument d
  cid_function_pair = std::make_pair("test_cid", "strmnp2");
  version_store[cid_function_pair].first = false;
  for (unsigned i = 3; i < 2200; i += 7) {
    Key key = string(5 - std::to_string(i).length(), '0') + std::to_string(i);
    CrossCausalPayload<SetLattice<string>> ccp_2_d;
    ccp_2_d.vector_clock.insert("base", 1);
    ccp_2_d.value = values[i%7];
    version_store[cid_function_pair].second[key][key] = std::make_shared<CrossCausalLattice<SetLattice<string>>>(ccp_2_d);
  }
  // argument e
  for (unsigned i = 4; i < 2200; i += 7) {
    Key key = string(5 - std::to_string(i).length(), '0') + std::to_string(i);
    CrossCausalPayload<SetLattice<string>> ccp_2_e;
    ccp_2_e.vector_clock.insert("base", 1);
    unsigned dep_index = i + 1;
    Key dep_key = string(5 - std::to_string(dep_index).length(), '0') + std::to_string(dep_index);
    ccp_2_e.dependency.insert(dep_key, VectorClock({{"base", 1}}));
    ccp_2_e.value = values[i%7];
    version_store[cid_function_pair].second[key][key] = std::make_shared<CrossCausalLattice<SetLattice<string>>>(ccp_2_e);
    CrossCausalPayload<SetLattice<string>> ccp_2_e_f;
    ccp_2_e_f.vector_clock.insert("base", 1);
    ccp_2_e_f.value = values[dep_index % 7];
    version_store[cid_function_pair].second[key][dep_key] = std::make_shared<CrossCausalLattice<SetLattice<string>>>(ccp_2_e_f);
  }
  // argument f
  cid_function_pair = std::make_pair("test_cid", "strmnp3");
  version_store[cid_function_pair].first = false;
  for (unsigned i = 5; i < 2200; i += 7) {
    ClientIdFunctionPair cid_function_pair = std::make_pair("test_cid", "strmnp3");
    Key key = string(5 - std::to_string(i).length(), '0') + std::to_string(i);
    CrossCausalPayload<SetLattice<string>> ccp_3_f;
    ccp_3_f.vector_clock.insert("base", 1);
    ccp_3_f.value = values[i%7];
    version_store[cid_function_pair].second[key][key] = std::make_shared<CrossCausalLattice<SetLattice<string>>>(ccp_3_f);
  }
  // argument g
  for (unsigned i = 6; i < 2200; i += 7) {
    ClientIdFunctionPair cid_function_pair = std::make_pair("test_cid", "strmnp3");
    Key key = string(5 - std::to_string(i).length(), '0') + std::to_string(i);
    CrossCausalPayload<SetLattice<string>> ccp_3_g;
    ccp_3_g.vector_clock.insert("base", 1);
    ccp_3_g.value = values[i%7];
    version_store[cid_function_pair].second[key][key] = std::make_shared<CrossCausalLattice<SetLattice<string>>>(ccp_3_g);
  }
}*/

void run(KvsAsyncClientInterface* client, Address ip, unsigned thread_id) {
  string log_file = "causal_cache_log_" + std::to_string(thread_id) + ".txt";
  string log_name = "causal_cache_log_" + std::to_string(thread_id);
  auto log = spdlog::basic_logger_mt(log_name, log_file, true);
  log->flush_on(spdlog::level::info);

  zmq::context_t* context = client->get_context();

  SocketCache pushers(context, ZMQ_PUSH);

  // keep track of keys that this causal cache is responsible for
  set<Key> key_set;

  StoreType unmerged_store;
  InPreparationType in_preparation;
  StoreType causal_cut_store;
  VersionStoreType version_store;

  // warm up for benchmark
  log->info("warmup begin");
  //warmup(version_store);
  warmup(causal_cut_store);
  log->info("warmup end");

  map<Key, set<Key>> to_fetch_map;
  map<Key, std::unordered_map<VectorClock, set<Key>, VectorClockHash>>
      cover_map;

  map<Key, set<Address>> single_callback_map;

  map<Address, PendingClientMetadata> pending_single_metadata;
  std::unordered_map<ClientIdFunctionPair, PendingClientMetadata, PairHash>
      pending_cross_metadata;

  // mapping from request id to response address of PUT request
  map<string, Address> request_id_to_address_map;

  // mapping from client id to a set of cache address yet to hear back from
  // and the scheduler response address, used in conservative protocol
  map<string, pair<set<Address>, Address>> pending_key_shipping_map;

  std::unordered_map<ClientIdFunctionPair, ProtocolMetadata, PairHash> protocol_matadata_map;

  std::unordered_map<ClientIdFunctionPair, StoreType, PairHash>
      conservative_store;

  CausalCacheThread cct = CausalCacheThread(ip, thread_id);

  // TODO: can we find a way to make the thread classes uniform across
  // languages? or unify the python and cpp implementations; actually, mostly
  // just the user thread stuff, I think.
  zmq::socket_t get_puller(*context, ZMQ_PULL);
  get_puller.bind(cct.causal_cache_get_bind_address());

  zmq::socket_t put_puller(*context, ZMQ_PULL);
  put_puller.bind(cct.causal_cache_put_bind_address());

  zmq::socket_t update_puller(*context, ZMQ_PULL);
  update_puller.bind(cct.causal_cache_update_bind_address());

  zmq::socket_t version_gc_puller(*context, ZMQ_PULL);
  version_gc_puller.bind(cct.causal_cache_version_gc_bind_address());

  zmq::socket_t versioned_key_request_puller(*context, ZMQ_PULL);
  versioned_key_request_puller.bind(
      cct.causal_cache_versioned_key_request_bind_address());

  zmq::socket_t versioned_key_response_puller(*context, ZMQ_PULL);
  versioned_key_response_puller.bind(
      cct.causal_cache_versioned_key_response_bind_address());

  zmq::socket_t scheduler_request_puller(*context, ZMQ_PULL);
  scheduler_request_puller.bind(
      cct.causal_cache_scheduler_request_bind_address());

  zmq::socket_t scheduler_key_shipping_request_puller(*context, ZMQ_PULL);
  scheduler_key_shipping_request_puller.bind(
      cct.causal_cache_scheduler_key_shipping_request_bind_address());

  zmq::socket_t key_shipping_request_puller(*context, ZMQ_PULL);
  key_shipping_request_puller.bind(
      cct.causal_cache_key_shipping_request_bind_address());

  zmq::socket_t key_shipping_response_puller(*context, ZMQ_PULL);
  key_shipping_response_puller.bind(
      cct.causal_cache_key_shipping_response_bind_address());

  zmq::socket_t scheduler_remote_read_puller(*context, ZMQ_PULL);
  scheduler_remote_read_puller.bind(
      cct.causal_cache_scheduler_remote_read_bind_address());

  vector<zmq::pollitem_t> pollitems = {
      {static_cast<void*>(get_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(put_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(update_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(version_gc_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(versioned_key_request_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(versioned_key_response_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(scheduler_request_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(scheduler_key_shipping_request_puller), 0, ZMQ_POLLIN,
       0},
      {static_cast<void*>(key_shipping_request_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(key_shipping_response_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(scheduler_remote_read_puller), 0, ZMQ_POLLIN, 0},
  };

  auto report_start = std::chrono::system_clock::now();
  auto report_end = std::chrono::system_clock::now();

  auto migrate_start = std::chrono::system_clock::now();
  auto migrate_end = std::chrono::system_clock::now();

  while (true) {
    kZmqUtil->poll(0, &pollitems);

    // handle a GET request
    if (pollitems[0].revents & ZMQ_POLLIN) {
      std::cout << "received GET\n";
      //log->info("received GET");
      auto get_start = std::chrono::system_clock::now();
      string serialized = kZmqUtil->recv_string(&get_puller);
      get_request_handler(serialized, key_set, unmerged_store, in_preparation,
                          causal_cut_store, version_store, single_callback_map,
                          pending_single_metadata, pending_cross_metadata,
                          to_fetch_map, cover_map, pushers, client, log, cct,
                          conservative_store, protocol_matadata_map);
      auto get_end = std::chrono::system_clock::now();
      auto get_time = std::chrono::duration_cast<std::chrono::microseconds>(get_end - get_start).count();
      log->info("get took {} micro seconds", get_time);
      //log->info("done GET");
      std::cout << "done GET\n";
    }

    // handle a PUT request
    if (pollitems[1].revents & ZMQ_POLLIN) {
      std::cout << "received PUT\n";
      //log->info("received PUT");
      string serialized = kZmqUtil->recv_string(&put_puller);
      put_request_handler(serialized, unmerged_store, causal_cut_store,
                          version_store, request_id_to_address_map, client,
                          log);
      //log->info("done PUT");
      std::cout << "done PUT\n";
    }

    // handle updates received from the KVS
    if (pollitems[2].revents & ZMQ_POLLIN) {
      std::cout << "received KVS update\n";
      //log->info("received KVS update");
      string serialized = kZmqUtil->recv_string(&update_puller);
      KeyRequest updates;
      updates.ParseFromString(serialized);

      for (const KeyTuple& tuple : updates.tuples()) {
        Key key = tuple.key();
        //log->info("key is {}", key);
        // if we are no longer caching this key, then we simply ignore updates
        // for it because we received the update based on outdated information
        if (key_set.find(key) == key_set.end()) {
          continue;
        }

        auto lattice = std::make_shared<CrossCausalLattice<SetLattice<string>>>(
            to_cross_causal_payload(deserialize_cross_causal(tuple.payload())));

        process_response(key, lattice, unmerged_store, in_preparation,
                         causal_cut_store, version_store, single_callback_map,
                         pending_single_metadata, pending_cross_metadata,
                         to_fetch_map, cover_map, pushers, client, log, cct, protocol_matadata_map);
      }
      //log->info("done KVS update");
      std::cout << "done KVS update\n";
    }

    // handle version GC request
    if (pollitems[3].revents & ZMQ_POLLIN) {
      std::cout << "received version GC request\n";
      //log->info("received version GC request");
      // assume this string is the client id
      string cid = kZmqUtil->recv_string(&version_gc_puller);
      std::unordered_set<ClientIdFunctionPair, PairHash> remove_set;
      for (const auto& pair : version_store) {
        if (pair.first.first == cid) {
          remove_set.insert(pair.first);
        }
      }
      for (const auto& pair : remove_set) {
        version_store.erase(pair);
      }
      //log->info("done version GC request");
      std::cout << "done version GC request\n";
    }

    // handle versioned key request
    if (pollitems[4].revents & ZMQ_POLLIN) {
      std::cout << "received version key request\n";
      //log->info("received version key request");
      string serialized = kZmqUtil->recv_string(&versioned_key_request_puller);
      versioned_key_request_handler(serialized, version_store, pushers, log,
                                    kZmqUtil);
      //log->info("done version key request");
      std::cout << "done version key request\n";
    }

    // handle versioned key response
    if (pollitems[5].revents & ZMQ_POLLIN) {
      std::cout << "received version key response\n";
      //log->info("received version key response");
      string serialized = kZmqUtil->recv_string(&versioned_key_response_puller);
      versioned_key_response_handler(serialized, causal_cut_store,
                                     version_store, pending_cross_metadata, cct,
                                     pushers, kZmqUtil, log, protocol_matadata_map, unmerged_store);
      //log->info("done version key response");
      std::cout << "done version key response\n";
    }

    // handle scheduler key version query
    if (pollitems[6].revents & ZMQ_POLLIN) {
      std::cout << "received scheduler key version query\n";
      //log->info("received scheduler key version query");
      string serialized = kZmqUtil->recv_string(&scheduler_request_puller);
      scheduler_request_handler(serialized, key_set, unmerged_store,
                                in_preparation, causal_cut_store, version_store,
                                pending_cross_metadata, to_fetch_map, cover_map,
                                pushers, client, log, cct, protocol_matadata_map);
      //log->info("done scheduler key version query");
      std::cout << "done scheduler key version query\n";
    }

    // handle scheduler key shipping request
    if (pollitems[7].revents & ZMQ_POLLIN) {
      std::cout << "received scheduler key shipping request\n";
      //log->info("received scheduler key shipping request");
      string serialized =
          kZmqUtil->recv_string(&scheduler_key_shipping_request_puller);
      scheduler_key_shipping_request_handler(
          serialized, pending_key_shipping_map, conservative_store,
          version_store, cct, pushers, protocol_matadata_map);
      //log->info("done scheduler key shipping request");
      std::cout << "done scheduler key shipping request\n";
    }

    // handle key shipping request
    if (pollitems[8].revents & ZMQ_POLLIN) {
      std::cout << "received key shipping request\n";
      //log->info("received key shipping request");
      string serialized = kZmqUtil->recv_string(&key_shipping_request_puller);
      key_shipping_request_handler(serialized, version_store, cct, pushers, log);
      //log->info("done key shipping request");
      std::cout << "done key shipping request\n";
    }

    // handle key shipping response
    if (pollitems[9].revents & ZMQ_POLLIN) {
      std::cout << "received key shipping response\n";
      //log->info("received key shipping response");
      string serialized = kZmqUtil->recv_string(&key_shipping_response_puller);
      key_shipping_response_handler(serialized, pending_key_shipping_map,
                                    conservative_store, cct, pushers);
      //log->info("done key shipping response");
      std::cout << "done key shipping response\n";
    }

    // handle scheduler remote read
    if (pollitems[10].revents & ZMQ_POLLIN) {
      std::cout << "received scheduler remote read request\n";
      log->info("received scheduler remote read request");
      string serialized = kZmqUtil->recv_string(&scheduler_remote_read_puller);
      scheduler_remote_read_handler(serialized, version_store, pending_cross_metadata, 
                                    pushers, log, cct, protocol_matadata_map);
      std::cout << "done scheduler remote read request\n";
    }

    vector<KeyResponse> responses = client->receive_async(kZmqUtil);
    for (const auto& response : responses) {
      std::cout << "entering kvs response handler\n";
      //log->info("entering kvs response handler");
      kvs_response_handler(response, unmerged_store, in_preparation,
                           causal_cut_store, version_store, single_callback_map,
                           pending_single_metadata, pending_cross_metadata,
                           to_fetch_map, cover_map, pushers, client, log, cct,
                           request_id_to_address_map, protocol_matadata_map);
      //log->info("exit kvs response handler");
      std::cout << "exit kvs response handler\n";
    }

    // collect and store internal statistics
    report_end = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(
                        report_end - report_start)
                        .count();

    // update KVS with information about which keys this node is currently
    // caching; we only do this periodically because we are okay with receiving
    // potentially stale updates
    if (duration >= kCausalCacheReportThreshold) {
      // periodically print out who is in the version store
      log->info("printing version store");
      for (const auto& pair : version_store) {
        log->info("cid {} function {}", pair.first.first, pair.first.second);
      }
      for (const auto& pair : pending_cross_metadata) {
        log->info("pending cid {} function {}", pair.first.first, pair.first.second);
      }
      for (const auto& pair : protocol_matadata_map) {
        log->info("protocol cid {} function {}", pair.first.first, pair.first.second);
      }

      KeySet set;

      for (const auto& pair : unmerged_store) {
        set.add_keys(pair.first);
      }

      string serialized;
      set.SerializeToString(&serialized);

      LWWPairLattice<string> val(TimestampValuePair<string>(
          generate_timestamp(thread_id), serialized));
      Key key = get_user_metadata_key(ip, UserMetadataType::cache_ip);
      client->put_async(key, serialize(val), LatticeType::LWW);
      report_start = std::chrono::system_clock::now();
    }

    migrate_end = std::chrono::system_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(migrate_end -
                                                                migrate_start)
                   .count();

    // check if any key in unmerged_store is newer and migrate
    if (duration >= kMigrateThreshold) {
      std::cout << "enter periodic migration\n";
      //log->info("enter periodic migration");
      periodic_migration_handler(unmerged_store, in_preparation,
                                 causal_cut_store, version_store,
                                 pending_cross_metadata, to_fetch_map,
                                 cover_map, pushers, client, cct, log, protocol_matadata_map);
      //log->info("exit periodic migration");
      std::cout << "exit periodic migration\n";
      migrate_start = std::chrono::system_clock::now();
    }

    // TODO: check if cache size is exceeding (threshold x capacity) and evict.
  }
}

int main(int argc, char* argv[]) {
  if (argc > 1) {
    std::cerr << "Usage: " << argv[0] << "" << std::endl;
    return 1;
  }

  // read the YAML conf
  YAML::Node conf = YAML::LoadFile("conf/kvs-config.yml");
  unsigned kRoutingThreadCount = conf["threads"]["routing"].as<unsigned>();

  YAML::Node user = conf["user"];
  Address ip = user["ip"].as<Address>();

  vector<Address> routing_ips;
  if (YAML::Node elb = user["routing-elb"]) {
    routing_ips.push_back(elb.as<Address>());
  } else {
    YAML::Node routing = user["routing"];
    for (const YAML::Node& node : routing) {
      routing_ips.push_back(node.as<Address>());
    }
  }

  vector<UserRoutingThread> threads;
  for (Address addr : routing_ips) {
    for (unsigned i = 0; i < kRoutingThreadCount; i++) {
      threads.push_back(UserRoutingThread(addr, i));
    }
  }

  KvsAsyncClient cl(threads, ip, 0, 10000);
  KvsAsyncClientInterface* client = &cl;

  run(client, ip, 0);
}