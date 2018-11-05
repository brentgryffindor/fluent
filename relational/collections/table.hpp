#ifndef COLLECTIONS_TABLE_HPP_
#define COLLECTIONS_TABLE_HPP_

#include <array>
#include <unordered_map>

#include "collections/collection.hpp"
#include "common/hash_util.hpp"
#include "common/keys.hpp"
#include "common/macros.hpp"
#include "common/type_list.hpp"

namespace relational {

template <typename Keys, typename... Ts>
struct Table;

template <std::size_t... Ks, typename... Ts>
struct Table<Keys<Ks...>, Ts...> : public Collection {
 public:
  using column_types = TypeList<Ts...>;
  using key_column_types = typename TypeListProject<column_types, Ks...>::type;
  using key_tuple_type = typename TypeListToTuple<key_column_types>::type;
  using container_type = std::unordered_map<key_tuple_type, std::tuple<Ts...>,
                                            Hash<key_tuple_type>>;
  Table(std::string name, std::array<std::string, sizeof...(Ts)> column_names) :
      name_(std::move(name)),
      column_names_(std::move(column_names)) {}

  DISALLOW_COPY_AND_ASSIGN(Table);
  DEFAULT_MOVE_AND_ASSIGN(Table);

  bool insert(std::tuple<Ts...> t) {
    key_tuple_type key = TupleProject<Ks...>(t);
    if (data_.find(key) != data_.end()) {
      data_[key] = t;
      return false;
    } else {
      return data_.emplace(std::move(key), std::move(t)).second;
    }
  }

  unsigned size() const { return data_.size(); }

  const std::string& get_name() const { return name_; }

  const std::array<std::string, sizeof...(Ts)>& get_column_names() const {
    return column_names_;
  }

  void tick() {}

  const container_type& get() const { return data_; }

 private:
  const std::string name_;
  const std::array<std::string, sizeof...(Ts)> column_names_;
  container_type data_;
};

}  // namespace relational

#endif  // COLLECTIONS_TABLE_HPP_