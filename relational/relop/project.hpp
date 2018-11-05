#ifndef RELOP_PROJECT_HPP_
#define RELOP_PROJECT_HPP_

#include "common/tuple_util.hpp"
#include "common/type_list.hpp"
#include "relop/relop.hpp"

namespace relational {
namespace rop {

template <typename Rel, std::size_t... Is>
struct Project : public RelOperator {
  using child_column_types = typename Rel::column_types;
  using column_types =
      typename TypeListProject<child_column_types, Is...>::type;
  using child_tuple_type = typename TypeListToTuple<child_column_types>::type;
  using tuple_type = typename TypeListToTuple<column_types>::type;

  explicit Project(Rel child_) : child(std::move(child_)) {
    column_names = {child.column_names[Is]...};
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
    child_tuple_type* child_next = child.next();
    if (child_next != nullptr) {
      next_tuple = TupleProject<Is...>(*child_next);
      return &next_tuple;
    } else {
      return nullptr;
    }
  }

  Rel child;
  tuple_type next_tuple;
  std::array<std::string, TypeListLen<column_types>::value> column_names;
};

template <std::size_t... Is, typename Rel,
          typename RelDecayed = typename std::decay<Rel>::type>
Project<RelDecayed, Is...> make_project(Rel&& child_) {
  return Project<RelDecayed, Is...>(std::forward<Rel>(child_));
}

template <std::size_t... Is>
struct ProjectPipe {};

template <size_t... Is>
ProjectPipe<Is...> project() {
  return {};
}

template <std::size_t... Is, typename Rel,
          typename RelDecayed = typename std::decay<Rel>::type>
Project<RelDecayed, Is...> operator|(Rel&& child_, ProjectPipe<Is...>) {
  return make_project<Is...>(std::forward<Rel&&>(child_));
}

}  // namespace rop
}  // namespace relational

#endif  // RELOP_PROJECT_HPP_