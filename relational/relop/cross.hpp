#ifndef RELOP_CROSS_HPP_
#define RELOP_CROSS_HPP_

#include <unordered_map>
#include <vector>

#include "common/hash_util.hpp"
#include "common/keys.hpp"
#include "common/type_list.hpp"
#include "relop/relop.hpp"

namespace relational {
namespace rop {

template <typename Left, typename Right>
struct Cross : public RelOperator {
  using left_column_types = typename Left::column_types;
  using right_column_types = typename Right::column_types;
  using left_tuple_type = typename TypeListToTuple<left_column_types>::type;
  using right_tuple_type = typename TypeListToTuple<right_column_types>::type;

  using column_types =
      typename TypeListConcat<left_column_types, right_column_types>::type;
  using tuple_type = typename TypeListToTuple<column_types>::type;

  Cross(Left left_, Right right_) :
      left(std::move(left_)),
      right(std::move(right_)) {
    std::copy(left.column_names.begin(),
              left.column_names.begin() +
                  TypeListLen<typename Left::column_types>::value,
              column_names.begin());
    std::copy(
        right.column_names.begin(),
        right.column_names.begin() +
            TypeListLen<typename Right::column_types>::value,
        column_names.begin() + TypeListLen<typename Left::column_types>::value);
  }

  std::vector<tuple_type> execute() {
    std::vector<tuple_type> result;
    tuple_type* tp_ptr = next();
    while (tp_ptr != nullptr) {
      result.push_back(*tp_ptr);
      tp_ptr = next();
    }
    return result;
  }

  tuple_type* next() {
    if (!built) {
      // first, we build the set using the left relation
      left_tuple_type* left_next = left.next();
      while (left_next != nullptr) {
        left_vec.push_back(*left_next);
        left_next = left.next();
      }
      built = true;
    }
    if (next_tuples.size() != 0) {
      // we haven't drained the next_tuples vector yet
      next_tuple = next_tuples.back();
      next_tuples.pop_back();
      return &next_tuple;
    } else {
      // we have drained the next_tuples vector
      right_tuple_type* right_next = right.next();
      if (right_next != nullptr) {
        for (const left_tuple_type& ltp : left_vec) {
          next_tuples.push_back(std::tuple_cat(ltp, *right_next));
        }
        return next();
      } else {
        // clear vec and reset flag
        left_vec.clear();
        built = false;
        return nullptr;
      }
    }
  }

  Left left;
  Right right;
  tuple_type next_tuple;

  std::array<std::string, TypeListLen<column_types>::value> column_names;
  std::vector<left_tuple_type> left_vec;
  bool built = false;
  std::vector<tuple_type> next_tuples;
};

template <typename Left, typename Right,
          typename LeftDecayed = typename std::decay<Left>::type,
          typename RightDecayed = typename std::decay<Right>::type>
Cross<LeftDecayed, RightDecayed> make_cross(Left&& left, Right&& right) {
  return Cross<LeftDecayed, RightDecayed>{std::forward<Left>(left),
                                          std::forward<Right>(right)};
}

}  // namespace rop
}  // namespace relational

#endif  // RELOP_CROSS_HPP_