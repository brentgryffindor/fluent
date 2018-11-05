#ifndef RELOP_FILTER_HPP_
#define RELOP_FILTER_HPP_

#include <vector>

#include "common/type_list.hpp"
#include "relop/relop.hpp"

namespace relational {
namespace rop {

template <typename Rel, typename F>
struct Filter : public RelOperator {
  using child_column_types = typename Rel::column_types;
  using column_types = child_column_types;
  using tuple_type = typename TypeListToTuple<column_types>::type;
  Filter(Rel child_, F f_) : child(std::move(child_)), f(std::move(f_)) {
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
    tuple_type* child_next = child.next();
    if (child_next != nullptr) {
      if (f(*child_next)) {
        next_tuple = *child_next;
        return &next_tuple;
      } else {
        return next();
      }
    } else {
      return nullptr;
    }
  }

  Rel child;
  F f;
  tuple_type next_tuple;
  std::array<std::string, TypeListLen<column_types>::value> column_names;
};

template <typename Rel, typename F,
          typename RelDecayed = typename std::decay<Rel>::type,
          typename FDecayed = typename std::decay<F>::type>
Filter<RelDecayed, FDecayed> make_filter(Rel&& child_, F&& f_) {
  return Filter<RelDecayed, FDecayed>{std::forward<Rel>(child_),
                                      std::forward<F>(f_)};
}

template <typename F>
struct FilterPipe {
  F f;
};

template <typename F>
FilterPipe<typename std::decay<F>::type> filter(F&& f_) {
  return {std::forward<F>(f_)};
}

template <typename Rel, typename F,
          typename RelDecayed = typename std::decay<Rel>::type>
Filter<RelDecayed, F> operator|(Rel&& child_, FilterPipe<F> f_) {
  return make_filter(std::forward<Rel&&>(child_), std::move(f_.f));
}

}  // namespace rop
}  // namespace relational

#endif  // RELOP_FILTER_HPP_