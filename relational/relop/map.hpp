#ifndef RELOP_MAP_HPP_
#define RELOP_MAP_HPP_

#include <initializer_list>
#include <vector>

#include "common/type_list.hpp"
#include "relop/relop.hpp"

namespace relational {
namespace rop {

template <typename Rel, typename F>
struct Map : public RelOperator {
  using child_column_types = typename Rel::column_types;
  using child_tuple_type = typename TypeListToTuple<child_column_types>::type;
  using vector_type = typename std::result_of<F(child_tuple_type)>::type;
  using vector_type_decayed = typename std::decay<vector_type>::type;
  using tuple_type = typename vector_type_decayed::value_type;
  using column_types = typename TupleToTypeList<tuple_type>::type;

  Map(Rel child_, F f_, std::initializer_list<std::string>& column_names_) :
      child(std::move(child_)),
      f(std::move(f_)) {
    std::copy(column_names_.begin(), column_names_.end(), column_names.begin());
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
    if (next_tuples.size() != 0) {
      next_tuple = next_tuples.back();
      next_tuples.pop_back();
      return &next_tuple;
    } else {
      child_tuple_type* child_next = child.next();
      if (child_next != nullptr) {
        next_tuples = f(*child_next);
        return next();
      } else {
        return nullptr;
      }
    }
  }

  Rel child;
  F f;
  tuple_type next_tuple;
  std::vector<tuple_type> next_tuples;
  std::array<std::string, TypeListLen<column_types>::value> column_names;
};

template <typename Rel, typename F,
          typename RelDecayed = typename std::decay<Rel>::type,
          typename FDecayed = typename std::decay<F>::type>
Map<RelDecayed, FDecayed> make_map(
    Rel&& child, F&& f, std::initializer_list<std::string> column_names) {
  return Map<RelDecayed, FDecayed>(std::forward<Rel>(child), std::forward<F>(f),
                                   column_names);
}

template <typename F>
struct MapPipe {
  F f;
  std::initializer_list<std::string> column_names;
};

template <typename F>
MapPipe<typename std::decay<F>::type> map(
    F&& f, std::initializer_list<std::string> column_names) {
  return {std::forward<F>(f), column_names};
}

template <typename Rel, typename F,
          typename RelDecayed = typename std::decay<Rel>::type>
Map<RelDecayed, F> operator|(Rel&& child, MapPipe<F> p) {
  return make_map(std::forward<Rel>(child), std::move(p.f), p.column_names);
}

}  // namespace rop
}  // namespace relational

#endif  // RELOP_MAP_HPP_