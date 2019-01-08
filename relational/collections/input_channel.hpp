#ifndef COLLETIONS_INPUT_CHANNEL_HPP_
#define COLLETIONS_INPUT_CHANNEL_HPP_

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
namespace detail {

// See `GetParser`.
template <template <typename> class Pickler, typename... Ts, std::size_t... Is>
std::tuple<Ts...> parse_tuple_impl(const std::vector<std::string>& columns,
                                   std::index_sequence<Is...>) {
  return {Pickler<Ts>().Load(columns[Is])...};
}

// See `GetParser`.
template <template <typename> class Pickler, typename... Ts>
std::tuple<Ts...> parse_tuple(const std::vector<std::string>& columns) {
  using Indices = std::make_index_sequence<sizeof...(Ts)>;
  return parse_tuple_impl<Pickler, Ts...>(columns, Indices());
}

}  // namespace detail

// A channel is a pseudo-relation. The first column of the channel is a string
// specifying the ZeroMQ to which the tuple should be sent. For example, if
// adding the tuple ("inproc://a", 1, 2, 3) will send the tuple ("inproc://a",
// 1, 2, 3) to the node at address ("inproc//a", 1, 2, 3).
template <template <typename> class Pickler, typename T, typename... Ts>
class InputChannel {
  static_assert(StaticAssert<std::is_same<std::string, T>>::value,
                "The first column of a channel must be a string specifying a "
                "ZeroMQ address (e.g. tcp://localhost:9999).");

 public:
  using column_types = TypeList<T, Ts...>;
  using container_type = std::vector<std::tuple<T, Ts...>>;
  InputChannel(std::size_t id, std::string name,
          std::array<std::string, 1 + sizeof...(Ts)> column_names,
          SocketCache* socket_cache) :
      id_(id),
      name_(std::move(name)),
      column_names_(std::move(column_names)) {}

  DISALLOW_COPY_AND_ASSIGN(InputChannel);
  DEFAULT_MOVE_AND_ASSIGN(InputChannel);

  unsigned size() const { return data_.size(); }

  const std::string& get_name() const { return name_; }

  const std::array<std::string, 1 + sizeof...(Ts)>& get_column_names() const {
    return column_names_;
  }

  const container_type& get() const { return data_; }

  std::tuple<T, Ts...> parse(const std::vector<std::string>& columns) const {
    return detail::parse_tuple<Pickler, T, Ts...>(columns);
  }

  // receive actually inserts the tuple
  void receive(std::tuple<T, Ts...> t) { data_.push_back(std::move(t)); }

  void tick() { data_.clear(); }

 private:
  const std::size_t id_;
  const std::string name_;
  const std::array<std::string, 1 + sizeof...(Ts)> column_names_;
  container_type data_;
};

}  // namespace relational

#endif  // COLLETIONS_INPUT_CHANNEL_HPP_