#ifndef COLLECTIONS_SCRATCH_HPP_
#define COLLECTIONS_SCRATCH_HPP_

#include <algorithm>
#include <array>
#include <string>
#include <tuple>
#include <vector>

#include "collections/collection.hpp"
#include "common/macros.hpp"
#include "common/type_list.hpp"
#include "common/type_traits.hpp"

namespace relational {

template <typename... Ts>
class Scratch : public Collection {
 public:
  using column_types = TypeList<Ts...>;
  using container_type = std::vector<std::tuple<Ts...>>;
  Scratch(std::string name,
          std::array<std::string, sizeof...(Ts)> column_names) :
      name_(std::move(name)),
      column_names_(std::move(column_names)) {}
  DISALLOW_COPY_AND_ASSIGN(Scratch);
  DEFAULT_MOVE_AND_ASSIGN(Scratch);

  bool insert(std::tuple<Ts...> t) {
    data_.push_back(std::move(t));
    return true;
  }

  unsigned size() const { return data_.size(); }

  const std::string& get_name() const { return name_; }

  const std::array<std::string, sizeof...(Ts)>& get_column_names() const {
    return column_names_;
  }

  const container_type& get() const { return data_; }

  void tick() { data_.clear(); }

 private:
  const std::string name_;
  const std::array<std::string, sizeof...(Ts)> column_names_;
  container_type data_;
};

}  // namespace relational

#endif  // COLLECTIONS_SCRATCH_HPP_