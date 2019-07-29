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

void run(KvsAsyncClientInterface* client, Address ip, unsigned thread_id) {
  string log_file = "causal_cache_log_" + std::to_string(thread_id) + ".txt";
  string log_name = "causal_cache_log_" + std::to_string(thread_id);
  auto log = spdlog::basic_logger_mt(log_name, log_file, true);
  log->flush_on(spdlog::level::info);

  zmq::context_t* context = client->get_context();

  SocketCache pushers(context, ZMQ_PUSH);

  string val1 = string(262144, '0');
  string val2 = string(262144, '0');
  string val3 = string(262144, '0');

  auto test_start = std::chrono::system_clock::now();
  string result;
  for(int i = 0; i < val1.size(); i++) {
    if (i % 3 == 0) {
      result.append(std::to_string(val1.at(i)));
    } else if (i % 3 == 1) {
      result.append(std::to_string(val2.at(i)));
    } else {
      result.append(std::to_string(val3.at(i)));
    }
  }
  auto test_end = std::chrono::system_clock::now();
  auto func_time = std::chrono::duration_cast<std::chrono::milliseconds>(test_end - test_start).count();
  log->info("test took {} ms", func_time);

  // keep track of keys that this causal cache is responsible for
  set<Key> key_set;

  StoreType unmerged_store;
  InPreparationType in_preparation;
  StoreType causal_cut_store;
  VersionStoreType version_store;

  map<Key, set<Key>> to_fetch_map;
  map<Key, std::unordered_map<VectorClock, set<Key>, VectorClockHash>>
      cover_map;

  map<Key, set<Address>> single_callback_map;

  map<Address, PendingClientMetadata> pending_single_metadata;
  map<string, PendingClientMetadata> pending_cross_metadata;

  // mapping from request id to response address of PUT request
  map<string, Address> request_id_to_address_map;

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

  zmq::socket_t scheduler_request_puller(*context, ZMQ_PULL);
  scheduler_request_puller.bind(
      cct.causal_cache_scheduler_request_bind_address());

  vector<zmq::pollitem_t> pollitems = {
      {static_cast<void*>(get_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(put_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(update_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(scheduler_request_puller), 0, ZMQ_POLLIN, 0},
  };

  auto report_start = std::chrono::system_clock::now();
  auto report_end = std::chrono::system_clock::now();

  auto migrate_start = std::chrono::system_clock::now();
  auto migrate_end = std::chrono::system_clock::now();

  while (true) {
    kZmqUtil->poll(0, &pollitems);

    // handle a GET request
    if (pollitems[0].revents & ZMQ_POLLIN) {
      //std::cout << "received GET\n";
      //log->info("received GET");
      auto get_start = std::chrono::time_point_cast<std::chrono::microseconds>(std::chrono::system_clock::now());
      //auto get_start = std::chrono::system_clock::now();
      string serialized = kZmqUtil->recv_string(&get_puller);
      get_request_handler(serialized, key_set, unmerged_store, in_preparation,
                          causal_cut_store, version_store, single_callback_map,
                          pending_single_metadata, pushers, client, log);
      auto get_end = std::chrono::time_point_cast<std::chrono::microseconds>(std::chrono::system_clock::now());

      log->info("get start ts {}", get_start.time_since_epoch().count());
      log->info("get end ts {}", get_end.time_since_epoch().count());
      //auto get_end = std::chrono::system_clock::now();
      //auto get_time = std::chrono::duration_cast<std::chrono::microseconds>(get_end - get_start).count();
      //log->info("get took {} micro seconds", get_time);
      //log->info("done GET");
      //std::cout << "done GET\n";
    }

    // handle a PUT request
    if (pollitems[1].revents & ZMQ_POLLIN) {
      //std::cout << "received PUT\n";
      //log->info("received PUT");
      string serialized = kZmqUtil->recv_string(&put_puller);
      put_request_handler(serialized, unmerged_store, causal_cut_store,
                          version_store, request_id_to_address_map, client,
                          log);
      //log->info("done PUT");
      //std::cout << "done PUT\n";
    }

    // handle updates received from the KVS
    if (pollitems[2].revents & ZMQ_POLLIN) {
      //std::cout << "received KVS update\n";
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
                         to_fetch_map, cover_map, pushers, client, log, cct);
      }
      //log->info("done KVS update");
      //std::cout << "done KVS update\n";
    }

    // handle scheduler key fetch request
    if (pollitems[3].revents & ZMQ_POLLIN) {
      //std::cout << "received scheduler key version query\n";
      //log->info("received scheduler key version query");
      string serialized = kZmqUtil->recv_string(&scheduler_request_puller);
      scheduler_request_handler(serialized, key_set, unmerged_store,
                                in_preparation, causal_cut_store, version_store,
                                pending_cross_metadata, to_fetch_map, cover_map,
                                pushers, client, log, cct);
      //log->info("done scheduler key version query");
      //std::cout << "done scheduler key version query\n";
    }


    vector<KeyResponse> responses = client->receive_async(kZmqUtil);
    for (const auto& response : responses) {
      //std::cout << "entering kvs response handler\n";
      //log->info("entering kvs response handler");
      kvs_response_handler(response, unmerged_store, in_preparation,
                           causal_cut_store, version_store, single_callback_map,
                           pending_single_metadata, pending_cross_metadata,
                           to_fetch_map, cover_map, pushers, client, log, cct,
                           request_id_to_address_map);
      //log->info("exit kvs response handler");
      //std::cout << "exit kvs response handler\n";
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
    duration = std::chrono::duration_cast<std::chrono::seconds>(migrate_end -
                                                                migrate_start)
                   .count();

    // check if any key in unmerged_store is newer and migrate
    if (duration >= kMigrateThreshold) {
      //std::cout << "enter periodic migration\n";
      //log->info("enter periodic migration");
      periodic_migration_handler(unmerged_store, in_preparation,
                                 causal_cut_store, version_store,
                                 pending_cross_metadata, to_fetch_map,
                                 cover_map, pushers, client, cct, log);
      //log->info("exit periodic migration");
      //std::cout << "exit periodic migration\n";
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