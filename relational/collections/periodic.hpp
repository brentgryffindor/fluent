#ifndef COLLECTIONS_PERIODIC_HPP_
#define COLLECTIONS_PERIODIC_HPP_

#include <cstddef>

#include <algorithm>
#include <array>
#include <chrono>
#include <iterator>
#include <set>
#include <type_traits>
#include <utility>

#include "common/macros.hpp"

namespace relational {

// A Periodic is a two-column collection where the first column is a unique id
// of type `id` and the second column is a time point of type `time`. It looks
// something like this:
//
//   +----+------------+
//   | id | time       |
//   +----+------------+
//   | 09 | 1488762221 |
//   +----+------------+
//   | 10 | 1488762226 |
//   +----+------------+
//   | 11 | 1488762236 |
//   +----+------------+
//
// You cannot write into a Periodic. Instead, Periodics are constructed with a
// period `period` (e.g. 1 second). Then, every `period` (e.g. every 1 second),
// a new tuple is inserted into the table with a unique id and the current
// time. After every tick, the Periodic is cleared.
template <typename Clock>
class Periodic {
 public:
  using id = std::size_t;
  using time = std::chrono::time_point<Clock>;
  using period = std::chrono::milliseconds;
  using column_types = TypeList<id, time>;
  using container_type = std::vector<std::tuple<id, time>>;

  Periodic(std::string name, period period)
      : name_(std::move(name)), period_(std::move(period)), id_(0) {}
  DISALLOW_COPY_AND_ASSIGN(Periodic);
  DEFAULT_MOVE_AND_ASSIGN(Periodic);

  const std::string& get_name() const { return name_; }

  const std::array<std::string, 2>& get_column_names() const {
    static std::array<std::string, 2> column_names{{"id", "time"}};
    return column_names;
  }

  const period& get_period() const { return period_; }

  const std::vector<std::tuple<id, time>>& get() const {
    return data_;
  }

  id get_and_increment_id() {
    id i = id_;
    id_++;
    return i;
  }

  void insert(std::tuple<id, time> t) {
    data_.push_back(std::move(t));
  }

  void tick() { data_.clear(); }

 private:
  const std::string name_;
  const std::chrono::milliseconds period_;
  id id_;
  container_type data_;
};

}  // namespace relational

#endif  // COLLECTIONS_PERIODIC_HPP