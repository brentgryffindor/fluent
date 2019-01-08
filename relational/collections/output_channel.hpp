#ifndef COLLETIONS_OUTPUT_CHANNEL_HPP_
#define COLLETIONS_OUTPUT_CHANNEL_HPP_

#include <cstddef>

#include <algorithm>
#include <array>
#include <type_traits>
#include <utility>
#include <vector>

#include "glog/logging.h"

#include "common/macros.hpp"
#include "common/static_assert.hpp"
#include "common/tuple_util.hpp"
#include "common/type_traits.hpp"
#include "zmq/socket_cache.hpp"
#include "zmq/zmq_util.hpp"

namespace relational {

ZmqUtil zmq_util;
ZmqUtilInterface* kZmqUtil = &zmq_util;

// A channel is a pseudo-relation. The first column of the channel is a string
// specifying the ZeroMQ to which the tuple should be sent. For example, if
// adding the tuple ("inproc://a", 1, 2, 3) will send the tuple ("inproc://a",
// 1, 2, 3) to the node at address ("inproc//a", 1, 2, 3).
template <template <typename> class Pickler, typename T, typename... Ts>
class OutputChannel {
  static_assert(StaticAssert<std::is_same<std::string, T>>::value,
                "The first column of a channel must be a string specifying a "
                "ZeroMQ address (e.g. tcp://localhost:9999).");

 public:
  using column_types = TypeList<T, Ts...>;
  using container_type = std::vector<std::tuple<T, Ts...>>;
  OutputChannel(std::size_t id, std::string name,
          std::array<std::string, 1 + sizeof...(Ts)> column_names,
          SocketCache* socket_cache) :
      id_(id),
      name_(std::move(name)),
      column_names_(std::move(column_names)),
      socket_cache_(socket_cache) {}

  DISALLOW_COPY_AND_ASSIGN(OutputChannel);
  DEFAULT_MOVE_AND_ASSIGN(OutputChannel);

  unsigned size() const { return data_.size(); }

  const std::string& get_name() const { return name_; }

  const std::array<std::string, 1 + sizeof...(Ts)>& get_column_names() const {
    return column_names_;
  }

  const container_type& get() const { return data_; }

  void tick() { 
    for (const auto& t: data_) {
      std::vector<zmq::message_t> msgs(2 + 1 + sizeof...(Ts));
      msgs[0] = kZmqUtil->string_to_message(to_string(id_));
      msgs[1] = kZmqUtil->string_to_message(to_string(name_));

      TupleIteri(t, [this, &msgs](std::size_t i, const auto& x) {
        msgs[i + 2] = kZmqUtil->string_to_message(this->to_string(x));
      });

      zmq::socket_t& socket = socket_cache_->At(std::get<0>(t));
      kZmqUtil->send_msgs(std::move(msgs), &socket);
    }
    data_.clear(); 
  }

  void buffer_insertion(std::tuple<T, Ts...> t) {
    data_.push_back(t);
  }

 private:
  template <typename U>
  std::string to_string(const U& x) {
    return Pickler<typename std::decay<U>::type>().Dump(x);
  }

  const std::size_t id_;
  const std::string name_;
  const std::array<std::string, 1 + sizeof...(Ts)> column_names_;
  container_type data_;

  // Whenever a tuple with address `a` is added to a Channel, the socket
  // associated with `a` in `socket_cache_` is used to send the tuple.
  SocketCache* socket_cache_;
};

}  // namespace relational

#endif  // COLLETIONS_OUTPUT_CHANNEL_HPP_