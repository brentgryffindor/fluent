#ifndef RELOP_COLLECTION_HPP_
#define RELOP_COLLECTION_HPP_

#include <vector>

#include "collections/collection.hpp"
#include "collections/collection_util.hpp"
#include "common/static_assert.hpp"

#include "common/hash_util.hpp"
#include "common/keys.hpp"
#include "common/type_list.hpp"
#include "relop/relop.hpp"

namespace relational {
namespace rop {

template <typename C>
struct Collection : public RelOperator {
  using is_base_of = std::is_base_of<relational::Collection, C>;
  static_assert(StaticAssert<is_base_of>::value, "");

  using column_types = typename C::column_types;
  using tuple_type = typename TypeListToTuple<column_types>::type;

  explicit Collection(const C* collection_) : collection(collection_) {
    column_names = collection->get_column_names();
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

  template <typename Q = C>
  typename std::enable_if<
      std::is_same<typename Q::container_type::const_iterator,
                   typename std::vector<tuple_type>::const_iterator>::value,
      tuple_type*>::type
  next() {
    if (new_invocation) {
      it = (collection->get()).begin();
      new_invocation = false;
    }
    if (it != (collection->get()).end()) {
      next_tuple = *it;
      std::advance(it, 1);
      return &next_tuple;
    } else {
      new_invocation = true;
      return nullptr;
    }
  }

  template <typename Q = C>
  typename std::enable_if<
      !std::is_same<typename Q::container_type::const_iterator,
                    typename std::vector<tuple_type>::const_iterator>::value,
      tuple_type*>::type
  next() {
    if (new_invocation) {
      it = (collection->get()).begin();
      new_invocation = false;
    }
    if (it != (collection->get()).end()) {
      next_tuple = it->second;
      std::advance(it, 1);
      return &next_tuple;
    } else {
      new_invocation = true;
      return nullptr;
    }
  }

  const C* collection;
  typename C::container_type::const_iterator it;
  tuple_type next_tuple;
  std::array<std::string, TypeListLen<column_types>::value> column_names;
  bool new_invocation = true;
};

template <typename C>
Collection<C> make_collection(const C* collection) {
  return Collection<C>(collection);
}

}  // namespace rop
}  // namespace relational

#endif  // RELOP_COLLECTION_HPP_