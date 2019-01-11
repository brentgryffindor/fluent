#ifndef COLLECTIONS_SCRATCH_HPP_
#define COLLECTIONS_SCRATCH_HPP_

#include <array>
#include <set>

#include "common/hash_util.hpp"
#include "common/macros.hpp"
#include "common/type_list.hpp"

namespace relational {

template <typename... Ts>
struct Scratch {
 public:
  using column_types = TypeList<Ts...>;
  using container_type = std::set<std::tuple<Ts...>>;
  Scratch(std::string name, std::array<std::string, sizeof...(Ts)> column_names) :
      name_(std::move(name)),
      column_names_(std::move(column_names)) {}

  DISALLOW_COPY_AND_ASSIGN(Scratch);
  DEFAULT_MOVE_AND_ASSIGN(Scratch);

  unsigned get_dependency_count() const {
    return dependency_count;
  }

  void increment_dependency_count() {
    dependency_count += 1;
  }

  void decrement_dependency_count() {
    dependency_count -= 1;
  }

  int get_stratum() {
    return stratum;
  }

  void set_stratum(int stratum_) {
    stratum = stratum_;
  }

  void buffer_insertion(std::tuple<Ts...> t) {
    insertion_buffer.emplace(std::move(t));
  }

  unsigned size() const { return data_.size(); }

  const std::string& get_name() const { return name_; }

  const std::array<std::string, sizeof...(Ts)>& get_column_names() const {
    return column_names_;
  }

  // return true if there are new tuples derived
  bool merge() {
    bool result = false;
    for (const auto& t: insertion_buffer) {
      if (data_.find(t) == data_.end()) {
        result = true;
        data_.insert(t);
        delta_.insert(t);
      }
    }
    insertion_buffer.clear();
    return result;
  }

  void tick() {
    data_.clear();
    insertion_buffer.clear();
  }

  const container_type& get() const { return data_; }

  const container_type& get_delta() const { return delta_; }

  void clear_delta() {
    delta_.clear();
  }

 private:
  const std::string name_;
  const std::array<std::string, sizeof...(Ts)> column_names_;
  container_type data_;
  container_type delta_;
  container_type insertion_buffer;
  unsigned dependency_count = 0;
  int stratum = -1;
};

}  // namespace relational

#endif  // COLLECTIONS_SCRATCH_HPP_