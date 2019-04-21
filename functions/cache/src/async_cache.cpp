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

#include "functions.pb.h"
#include "kvs_async_client.hpp"
#include "yaml-cpp/yaml.h"

ZmqUtil zmq_util;
ZmqUtilInterface* kZmqUtil = &zmq_util;

unsigned kCacheReportThreshold = 5;

using VersionStoreType =
    map<string,
        map<Key, LWWPairLattice<string>>>;

struct PendingClientMetadata {
  PendingClientMetadata() = default;

  PendingClientMetadata(string client_id, set<Key> read_set,
                        set<Key> to_retrieve_set) :
      client_id_(std::move(client_id)),
      read_set_(std::move(read_set)),
      to_retrieve_set_(std::move(to_retrieve_set)) {}

  PendingClientMetadata(string client_id, set<Key> read_set,
                        set<Key> to_retrieve_set,
                        set<Key> future_read_set, set<Key> remote_read_set,
                        set<Key> dne_set,
                        map<Key, string> serialized_local_payload,
                        map<Key, string> serialized_remote_payload) :
      client_id_(std::move(client_id)),
      read_set_(std::move(read_set)),
      to_retrieve_set_(std::move(to_retrieve_set)),
      future_read_set_(std::move(future_read_set)),
      remote_read_set_(std::move(remote_read_set)),
      dne_set_(std::move(dne_set)),
      serialized_local_payload_(std::move(serialized_local_payload)),
      serialized_remote_payload_(std::move(serialized_remote_payload)) {}

  string client_id_;
  set<Key> read_set_;
  set<Key> to_retrieve_set_;
  set<Key> future_read_set_;
  set<Key> remote_read_set_;
  set<Key> dne_set_;
  map<Key, string> serialized_local_payload_;
  map<Key, string> serialized_remote_payload_;
};

string get_serialized_value_from_cache(
    const Key& key, LatticeType type,
    const map<Key, LWWPairLattice<string>>& local_lww_cache,
    const map<Key, SetLattice<string>>& local_set_cache, logger log) {
  if (type == LatticeType::LWW) {
    if (local_lww_cache.find(key) != local_lww_cache.end()) {
      return serialize(local_lww_cache.at(key));
    } else {
      log->error("Key {} not found in LWW cache.", key);
      return "";
    }
  } else if (type == LatticeType::SET) {
    if (local_set_cache.find(key) != local_set_cache.end()) {
      return serialize(local_set_cache.at(key));
    } else {
      log->error("Key {} not found in SET cache.", key);
      return "";
    }
  } else {
    log->error("Invalid lattice type.");
    return "";
  }
}

void update_cache(const Key& key, LatticeType type, const string& payload,
                  map<Key, LWWPairLattice<string>>& local_lww_cache,
                  map<Key, SetLattice<string>>& local_set_cache, logger log) {
  if (type == LatticeType::LWW) {
    local_lww_cache[key].merge(deserialize_lww(payload));
  } else if (type == LatticeType::SET) {
    local_set_cache[key].merge(deserialize_set(payload));
  } else {
    log->error("Invalid lattice type.");
  }
}

void send_get_response(const set<Key>& read_set, const Address& response_addr,
                       const map<Key, LatticeType>& key_type_map,
                       const map<Key, LWWPairLattice<string>>& local_lww_cache,
                       const map<Key, SetLattice<string>>& local_set_cache,
                       SocketCache& pushers, logger log) {
  KeyResponse response;
  response.set_type(RequestType::GET);

  for (const Key& key : read_set) {
    KeyTuple* tp = response.add_tuples();
    tp->set_key(key);
    if (key_type_map.find(key) == key_type_map.end()) {
      // key dne in cache, it actually means that there is a
      // response from kvs that has error = 1
      tp->set_error(1);
    } else {
      tp->set_error(0);
      tp->set_lattice_type(key_type_map.at(key));
      tp->set_payload(get_serialized_value_from_cache(
          key, key_type_map.at(key), local_lww_cache, local_set_cache, log));
    }
  }

  std::string resp_string;
  response.SerializeToString(&resp_string);
  kZmqUtil->send_string(resp_string, &pushers[response_addr]);
}

void send_error_response(RequestType type, const Address& response_addr,
                         SocketCache& pushers) {
  KeyResponse response;
  response.set_type(type);
  response.set_error(ResponseErrorType::LATTICE);
  std::string resp_string;
  response.SerializeToString(&resp_string);
  kZmqUtil->send_string(resp_string, &pushers[response_addr]);
}

Address find_address(
    const Key& key, const map<Address, map<Key, unsigned long long>>& key_locations_map, unsigned long long ts = 0) {
  for (const auto& address_map_pair : key_locations_map) {
    if (address_map_pair.second.find(key) != address_map_pair.second.end() && address_map_pair.second.at(key) != ts) {
      return address_map_pair.first;
    }
  }
  // we are good to read from the local cache
  return "";
}

void respond_to_client(map<Address, PendingClientMetadata>& pending_request_metadata, const Address& addr, 
                      const map<Key, LatticeType>& key_type_map, SocketCache& pushers, const CacheThread& ct, const VersionStoreType& version_store) {
  KeyResponse response;
  response.set_type(RequestType::GET);

  for (auto& pair : pending_request_metadata[addr].serialized_local_payload_) {
    KeyTuple* tp = response.add_tuples();
    tp->set_key(pair.first);
    tp->set_error(0);
    tp->set_lattice_type(key_type_map.at(pair.first));
    tp->set_payload(std::move(pair.second));
  }

  for (auto& pair : pending_request_metadata[addr].serialized_remote_payload_) {
    KeyTuple* tp = response.add_tuples();
    tp->set_key(pair.first);
    tp->set_error(0);
    tp->set_lattice_type(key_type_map.at(pair.first));
    tp->set_payload(std::move(pair.second));
  }

  for (const Key& key : pending_request_metadata[addr].dne_set_) {
    KeyTuple* tp = response.add_tuples();
    tp->set_key(key);
    tp->set_error(1);
  }

  response.set_key_query_addr(ct.repeatable_read_request_connect_address());

  if (version_store.find(pending_request_metadata[addr].client_id_) !=
      version_store.end()) {
    for (const auto& pair :
         version_store.at(pending_request_metadata[addr].client_id_)) {
      KvsKeyTimestampPair* p = response.add_pairs();
      p->set_key(pair.first);
      p->set_timestamp(pair.second.reveal().timestamp);
    }
  }

  std::string resp_string;
  response.SerializeToString(&resp_string);
  kZmqUtil->send_string(resp_string, &pushers[addr]);
  pending_request_metadata.erase(addr);
}

void run(KvsAsyncClientInterface* client, Address ip, unsigned thread_id) {
  string log_file = "cache_log_" + std::to_string(thread_id) + ".txt";
  string log_name = "cache_log_" + std::to_string(thread_id);
  auto log = spdlog::basic_logger_mt(log_name, log_file, true);
  log->flush_on(spdlog::level::info);

  zmq::context_t* context = client->get_context();

  SocketCache pushers(context, ZMQ_PUSH);

  map<Key, LWWPairLattice<string>> local_lww_cache;
  map<Key, SetLattice<string>> local_set_cache;

  map<Address, PendingClientMetadata> pending_request_metadata;
  map<Key, set<Address>> key_requestor_map;

  map<Key, LatticeType> key_type_map;

  // mapping from client id to a set of response address of GET request
  map<string, set<Address>> client_id_to_address_map;

  VersionStoreType version_store;

  // mapping from request id to respond address of PUT request
  map<string, Address> request_address_map;

  CacheThread ct = CacheThread(ip, thread_id);

  // TODO: can we find a way to make the thread classes uniform across
  // languages? or unify the python and cpp implementations; actually, mostly
  // just the user thread stuff, I think.
  zmq::socket_t get_puller(*context, ZMQ_PULL);
  get_puller.bind(ct.cache_get_bind_address());

  zmq::socket_t put_puller(*context, ZMQ_PULL);
  put_puller.bind(ct.cache_put_bind_address());

  zmq::socket_t update_puller(*context, ZMQ_PULL);
  update_puller.bind(ct.cache_update_bind_address());

  zmq::socket_t version_gc_puller(*context, ZMQ_PULL);
  version_gc_puller.bind(ct.version_gc_bind_address());

  zmq::socket_t repeatable_read_request_puller(*context, ZMQ_PULL);
  repeatable_read_request_puller.bind(
      ct.repeatable_read_request_bind_address());

  zmq::socket_t repeatable_read_response_puller(*context, ZMQ_PULL);
  repeatable_read_response_puller.bind(
      ct.repeatable_read_response_bind_address());

  vector<zmq::pollitem_t> pollitems = {
      {static_cast<void*>(get_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(put_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(update_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(version_gc_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(repeatable_read_request_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(repeatable_read_response_puller), 0, ZMQ_POLLIN, 0},
  };

  auto report_start = std::chrono::system_clock::now();
  auto report_end = std::chrono::system_clock::now();

  while (true) {
    kZmqUtil->poll(0, &pollitems);

    // handle a GET request
    if (pollitems[0].revents & ZMQ_POLLIN) {
      string serialized = kZmqUtil->recv_string(&get_puller);
      KeyRequest request;
      request.ParseFromString(serialized);

      log->info("received GET from addr {}", request.response_address());

      // set the future read set field
      for (const Key& key : request.future_read_set()) {
        //log->info("future read set has key {}", key);
        pending_request_metadata[request.response_address()]
            .future_read_set_.insert(key);
      }

      pending_request_metadata[request.response_address()].client_id_ = request.client_id();

      map<string, map<Key, unsigned long long>> key_locations;

      for (const auto& pair : request.key_locations()) {
        const Address& addr = pair.first;
        const KvsKeyTimestampList& list = pair.second;
        for (const auto& pair : list.pairs()) {
          key_locations[addr][pair.key()] = pair.timestamp();
        }
      }

      set<Key> read_set;

      for (KeyTuple tuple : request.tuples()) {
        Key key = tuple.key();
        log->info("Key to get is {}.", key);
        read_set.insert(key);

        if (key_type_map.find(key) == key_type_map.end()) {
          // this means key dne in cache
          Address remote_addr = find_address(key, key_locations);
          if (remote_addr != "") {
            // read from remote
            pending_request_metadata[request.response_address()].remote_read_set_.insert(key);
            RepeatableReadRequest rrr;
            rrr.set_id(request.client_id());
            rrr.set_response_address(ct.repeatable_read_response_connect_address());
            rrr.add_keys(key);
            string serialized_string;
            rrr.SerializeToString(&serialized_string);
            kZmqUtil->send_string(serialized_string, &pushers[remote_addr]);
            client_id_to_address_map[request.client_id()].insert(request.response_address());
            log->info("key {} need to be read from remote addr {}", key, remote_addr);
          } else {
            pending_request_metadata[request.response_address()].to_retrieve_set_.insert(key);
            key_requestor_map[key].insert(request.response_address());
            client->get_async(key);
            log->info("key {} need to be read from KVS", key);
          }
        } else {
          // key in cache
          Address remote_addr = find_address(key, key_locations, local_lww_cache[key].reveal().timestamp);
          if (remote_addr != "") {
            // read from remote
            pending_request_metadata[request.response_address()].remote_read_set_.insert(key);
            RepeatableReadRequest rrr;
            rrr.set_id(request.client_id());
            rrr.set_response_address(ct.repeatable_read_response_connect_address());
            rrr.add_keys(key);
            string serialized_string;
            rrr.SerializeToString(&serialized_string);
            kZmqUtil->send_string(serialized_string, &pushers[remote_addr]);
            client_id_to_address_map[request.client_id()].insert(request.response_address());
            log->info("key {} need to be read from remote addr {}", key, remote_addr);
          } else {
            pending_request_metadata[request.response_address()].serialized_local_payload_[key] = serialize(local_lww_cache.at(key));
            if (pending_request_metadata[request.response_address()].future_read_set_.find(key) != pending_request_metadata[request.response_address()].future_read_set_.end()) {
              version_store[request.client_id()][key] = local_lww_cache.at(key);
            }
            log->info("key {} can be read from cache", key);
          }
        }
      }

      pending_request_metadata[request.response_address()].read_set_ = std::move(read_set);

      if (pending_request_metadata[request.response_address()].remote_read_set_.size() == 0
          && pending_request_metadata[request.response_address()].to_retrieve_set_.size() == 0) {
        // we can send response now and GC the pending map
        log->info("all keys can be read from local cache for client addr {}", request.response_address());
        respond_to_client(pending_request_metadata, request.response_address(), key_type_map, pushers, ct, version_store);
      }
    }

    // handle a PUT request
    if (pollitems[1].revents & ZMQ_POLLIN) {
      string serialized = kZmqUtil->recv_string(&put_puller);
      KeyRequest request;
      request.ParseFromString(serialized);

      bool error = false;

      for (KeyTuple tuple : request.tuples()) {
        // this loop checks if any key has invalid lattice type
        Key key = tuple.key();

        if (!tuple.has_lattice_type()) {
          log->error("The cache requires the lattice type to PUT key.");
          send_error_response(RequestType::PUT, request.response_address(),
                              pushers);
          error = true;
          break;
        } else if ((key_type_map.find(key) != key_type_map.end()) &&
                   (key_type_map[key] != tuple.lattice_type())) {
          log->error(
              "Key {}: Lattice type for PUT does not match stored lattice "
              "type.",
              key);
          send_error_response(RequestType::PUT, request.response_address(),
                              pushers);
          error = true;
          break;
        }
      }

      if (!error) {
        for (KeyTuple tuple : request.tuples()) {
          Key key = tuple.key();

          // first update key type map
          key_type_map[key] = tuple.lattice_type();
          //log->error("key {} has lattice type {} according to PUT.", key, key_type_map[key]);
          update_cache(key, tuple.lattice_type(), tuple.payload(),
                       local_lww_cache, local_set_cache, log);
          string req_id =
              client->put_async(key, tuple.payload(), tuple.lattice_type());
          request_address_map[req_id] = request.response_address();
        }
      }
    }

    // handle updates received from the KVS
    if (pollitems[2].revents & ZMQ_POLLIN) {
      //log->info("received update from kvs");
      string serialized = kZmqUtil->recv_string(&update_puller);
      KeyRequest updates;
      updates.ParseFromString(serialized);

      for (const KeyTuple& tuple : updates.tuples()) {
        Key key = tuple.key();

        // if we are no longer caching this key, then we simply ignore updates
        // for it because we received the update based on outdated information
        if (key_type_map.find(key) == key_type_map.end()) {
          continue;
        }

        if (key_type_map[key] != tuple.lattice_type()) {
          // This is bad! This means that we have a certain lattice type stored
          // locally for a key and that is incompatible with the lattice type
          // stored in the KVS. This probably means that something was put here
          // but hasn't been propagated yet, and something *different* was put
          // globally. I think we should just drop our local copy for the time
          // being, but open to other ideas.

          log->error(
              "Key {}: Stored lattice type did not match type received "
              "in KVS update.",
              key);

          switch (key_type_map[key]) {
            case LatticeType::LWW: local_lww_cache.erase(key); break;
            case LatticeType::SET: local_set_cache.erase(key); break;
            default:
              break;  // this can never happen
          }

          key_type_map[key] = tuple.lattice_type();
        }

        update_cache(key, tuple.lattice_type(), tuple.payload(),
                     local_lww_cache, local_set_cache, log);
      }
    }

    // handle version gc request
    if (pollitems[3].revents & ZMQ_POLLIN) {
      string serialized = kZmqUtil->recv_string(&version_gc_puller);
      version_store.erase(serialized);
      log->info("received version GC request for client id {}", serialized);
    }

    // handle RR key request
    if (pollitems[4].revents & ZMQ_POLLIN) {
      log->info("received a versioned key request");
      string serialized = kZmqUtil->recv_string(&repeatable_read_response_puller);
      RepeatableReadRequest request;
      request.ParseFromString(serialized);

      RepeatableReadResponse response;
      response.set_id(request.id());
      if (version_store.find(request.id()) != version_store.end()) {
        for (const auto& key : request.keys()) {
          if (version_store[request.id()].find(key) ==
              version_store[request.id()].end()) {
            log->error(
                "Requested key {} for client ID {} not available in versioned "
                "store.",
                key, request.id());
          } else {
            //log->info("assembling payload for key {}", key);
            KeyTuple* tp = response.add_tuples();
            tp->set_key(key);
            tp->set_payload(serialize((version_store[request.id()][key])));
          }
        }
      } else {
        log->error("Client ID {} not available in versioned store.", request.id());
      }
      // send response
      string resp_string;
      response.SerializeToString(&resp_string);
      kZmqUtil->send_string(resp_string, &pushers[request.response_address()]);
    }

    // handle RR key response
    if (pollitems[5].revents & ZMQ_POLLIN) {
      log->info("received a versioned key response");
      string serialized = kZmqUtil->recv_string(&repeatable_read_response_puller);
      RepeatableReadResponse response;
      response.ParseFromString(serialized);

      if (client_id_to_address_map.find(response.id()) !=
          client_id_to_address_map.end()) {

        set<Address> address_to_gc;

        for (const Address& addr : client_id_to_address_map[response.id()]) {
          if (pending_request_metadata.find(addr) != pending_request_metadata.end()) {
            for (const KeyTuple& tp : response.tuples()) {
              log->info("response key contains {}", tp.key());
              if (pending_request_metadata[addr].remote_read_set_.find(tp.key()) !=
                  pending_request_metadata[addr].remote_read_set_.end()) {
                pending_request_metadata[addr].serialized_remote_payload_[tp.key()] =
                    tp.payload();
                pending_request_metadata[addr].remote_read_set_.erase(tp.key());
              }
            }

            if (pending_request_metadata[addr].remote_read_set_.size() == 0
                && pending_request_metadata[addr].to_retrieve_set_.size() == 0) {
              log->info("all keys received");
              // we can send response now and GC the pending map
              respond_to_client(pending_request_metadata, addr, key_type_map, pushers, ct, version_store);
            }
          }
        }
        // GC
        for (const Address& addr : address_to_gc) {
          client_id_to_address_map[response.id()].erase(addr);
        }

        if (client_id_to_address_map[response.id()].size() == 0) {
          client_id_to_address_map.erase(response.id());
        }
      } else {
        log->info("no address waiting for this client {}", response.id());
      }
    }

    vector<KeyResponse> responses = client->receive_async(kZmqUtil);
    for (const auto& response : responses) {
      Key key = response.tuples(0).key();

      if (response.has_error() &&
          response.error() == ResponseErrorType::TIMEOUT) {
        log->info("timed out!");
        if (response.type() == RequestType::GET) {
          log->info("retrying key {}", key);
          client->get_async(key);
        } else {
          if (request_address_map.find(response.response_id()) !=
              request_address_map.end()) {
            // we only retry for client-issued requests, not for the periodic
            // stat report
            string new_req_id =
                client->put_async(key, response.tuples(0).payload(),
                                  response.tuples(0).lattice_type());

            request_address_map[new_req_id] =
                request_address_map[response.response_id()];
            request_address_map.erase(response.response_id());
          }
        }
      } else {
        if (response.type() == RequestType::GET) {
          log->info("Received KVS GET response for key {}", key);
          // update cache first
          if (response.tuples(0).error() != 1) {
            // we actually got a non null key
            key_type_map[key] = response.tuples(0).lattice_type();
            //log->error("key {} has lattice type {} in kvs GET response.", key, key_type_map[key]);

            update_cache(key, response.tuples(0).lattice_type(),
                         response.tuples(0).payload(), local_lww_cache,
                         local_set_cache, log);
          } else {
            log->info("key {} dne in kvs!", key);
          }

          // notify clients
          if (key_requestor_map.find(key) != key_requestor_map.end()) {
            for (const Address& addr : key_requestor_map[key]) {
              log->info("response hit address {}", addr);
              pending_request_metadata[addr].to_retrieve_set_.erase(key);

              if (response.tuples(0).error() != 1) {
                pending_request_metadata[addr].serialized_local_payload_[key] = serialize(local_lww_cache.at(key));
                if (pending_request_metadata[addr].future_read_set_.find(key) != pending_request_metadata[addr].future_read_set_.end()) {
                  version_store[pending_request_metadata[addr].client_id_][key] = local_lww_cache.at(key);
                }
              } else {
                pending_request_metadata[addr].dne_set_.insert(key);
              }

              if (pending_request_metadata[addr].remote_read_set_.size() == 0
                  && pending_request_metadata[addr].to_retrieve_set_.size() == 0) {
                // we can send response now and GC the pending map
                log->info("all keys received");
                respond_to_client(pending_request_metadata, addr, key_type_map, pushers, ct, version_store);
              }
            }

            key_requestor_map.erase(key);
          }
        } else {
          // we only send a response if we have a response address -- e.g., we
          // don't have one for updating our own cached key set
          if (request_address_map.find(response.response_id()) !=
              request_address_map.end()) {
            string resp_string;
            response.SerializeToString(&resp_string);

            kZmqUtil->send_string(
                resp_string,
                &pushers[request_address_map[response.response_id()]]);
            request_address_map.erase(response.response_id());
          }
        }
      }
    }

    // collect and store internal statistics
    report_end = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(
                        report_end - report_start)
                        .count();

    // update KVS with information about which keys this node is currently
    // caching; we only do this periodically because we are okay with receiving
    // potentially stale updates
    if (duration >= kCacheReportThreshold) {
      KeySet set;

      for (const auto& pair : key_type_map) {
        set.add_keys(pair.first);
      }

      string serialized;
      set.SerializeToString(&serialized);

      LWWPairLattice<string> val(TimestampValuePair<string>(
          generate_timestamp(thread_id), serialized));
      Key key = get_user_metadata_key(ip, UserMetadataType::cache_ip);
      client->put_async(key, serialize(val), LatticeType::LWW);
      report_start = std::chrono::system_clock::now();

      for (const auto& pair : pending_request_metadata) {
        log->info("pending addr includes {}", pair.first);
        for (const Key& key : pair.second.to_retrieve_set_) {
          log->info("key {} still need to be retrieved from kvs", key);
        }
        for (const Key& key : pair.second.remote_read_set_) {
          log->info("key {} still need to be read from remote", key);
        }
      }
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
