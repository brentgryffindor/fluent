#ifndef RUNTIME_NETWORK_STATE_HPP_
#define RUNTIME_NETWORK_STATE_HPP_

#include <string>

#include "zmq.hpp"

#include "zmq/socket_cache.hpp"

#include "glog/logging.h"

namespace relational {

// NetworkState is a simple struct holding all of the networking junk a
// FluentExecutor needs. Specifically, there are three things:
//
// 1. A zmq::context_t because all zmq networking requires a context.
// 2. A PULL zmq:socket_t on which a FluentExecutor receives from other nodes.
// 3. A SocketCache which channels use to figure out where to send messages.
struct NetworkState {
  explicit NetworkState(const std::string& address,
                        zmq::context_t* const context_) :
      context(context_),
      socket(*context, ZMQ_PULL),
      socket_cache(context, ZMQ_PUSH) {
    socket.bind(address);
    LOG(INFO) << "Fluent listening on '" << address << "'.";
  }

  zmq::context_t* const context;
  zmq::socket_t socket;
  SocketCache socket_cache;
};

}  // namespace relational

#endif  // RUNTIME_NETWORK_STATE_H_