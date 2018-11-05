#ifndef ZMQ_UTIL_SOCKET_CACHE_HPP_
#define ZMQ_UTIL_SOCKET_CACHE_HPP_

#include <map>
#include <string>

#include "zmq.hpp"

namespace relational {
namespace zmq_util {

// A SocketCache is a map from ZeroMQ addresses to PUSH ZeroMQ sockets. The
// socket corresponding to address `address` can be retrieved from a
// SocketCache `cache` with `cache[address]` or `cache.At(address)`. If a
// socket with a given address is not in the cache when it is requested, one is
// created and connected to the address. An example:
//
//   zmq::context_t context(1);
//   SocketCache cache(&context);
//   // This will create a socket and connect it to "inproc://a".
//   zmq::socket_t& a = cache["inproc://a"];
//   // This will not createa new socket. It will return the socket created in
//   // the previous line. In other words, a and the_same_a_as_before are
//   // references to the same socket.
//   zmq::socket_t& the_same_a_as_before = cache["inproc://a"];
//   // cache.At("inproc://a") is 100% equivalent to cache["inproc://a"].
//   zmq::socket_t& another_a = cache.At("inproc://a");
class SocketCache {
 public:
  explicit SocketCache(zmq::context_t* context) : context_(context) {}
  zmq::socket_t& At(const std::string& addr);
  zmq::socket_t& operator[](const std::string& addr);

 private:
  zmq::context_t* context_;
  std::map<std::string, zmq::socket_t> cache_;
};

}  // namespace zmq_util
}  // namespace relational

#endif  // ZMQ_UTIL_SOCKET_CACHE_HPP_