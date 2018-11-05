#ifndef RELOP_ORDER_BY_HPP_
#define RELOP_ORDER_BY_HPP_

#include <map>
#include <vector>

#include "common/type_list.hpp"
#include "relop/relop.hpp"

namespace relational {
namespace rop {

template <typename Rel, std::size_t... Is>
struct OrderBy : public RelOperator {
  using child_column_types = typename Rel::column_types;
  using column_types = child_column_types;
  using tuple_type = typename TypeListToTuple<column_types>::type;
  using key_type = typename TypeListToTuple<
      typename TypeListProject<column_types, Is...>::type>::type;
  OrderBy(Rel child_, size_t num_) : child(std::move(child_)), num(num_) {
    column_names = child_.column_names;
  }

  std::vector<tuple_type> execute() {
    std::vector<tuple_type> result(column_names);
    tuple_type* tp_ptr = next();
    while (tp_ptr != nullptr) {
      result.push_back(*tp_ptr);
      tp_ptr = next();
    }
    return result;
  }

  tuple_type* next() {
    if (!processed) {
      tuple_type* child_next = child.next();
      while (child_next != nullptr) {
        ordered_map.emplace(TupleProject<Is...>(*child_next), *child_next);
        child_next = child.next();
      }
      processed = true;
    }
    if (ordered_map.size() == 0) {
      // reset counter, map and flag
      ordered_map.clear();
      processed = false;
      count = 0;
      return nullptr;
    } else {
      if (count < num) {
        count += 1;
        auto it = ordered_map.begin();
        next_tuple = it->second;
        ordered_map.erase(it->first);
        return &next_tuple;
      } else {
        // reset counter, map and flag
        ordered_map.clear();
        processed = false;
        count = 0;
        return nullptr;
      }
    }
  }

  Rel child;
  tuple_type next_tuple;
  bool processed = false;
  std::map<key_type, tuple_type> ordered_map;
  std::array<std::string, TypeListLen<column_types>::value> column_names;
  size_t num;
  size_t count = 0;
};

template <std::size_t...>
struct order_by;

template <std::size_t... Is>
struct order_by {
  order_by(size_t num_) : num(num_) {}
  size_t num;
};

template <std::size_t... Is, typename Rel,
          typename RelDecayed = typename std::decay<Rel>::type>
OrderBy<RelDecayed, Is...> operator|(Rel&& child_, order_by<Is...> ob) {
  return OrderBy<RelDecayed, Is...>(std::forward<Rel&&>(child_), ob.num);
}

}  // namespace rop
}  // namespace relational

#endif  // RELOP_ORDER_BY_HPP_