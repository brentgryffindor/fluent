#ifndef RELOP_ITERABLE_HPP_
#define RELOP_ITERABLE_HPP_

#include <iostream>

#include "collections/collection_util.hpp"
#include "common/static_assert.hpp"
#include "common/hash_util.hpp"
#include "common/keys.hpp"
#include "common/type_list.hpp"
#include "relop/relop.hpp"

#define REGULAR -1
#define DELTA 0

namespace relational {
namespace rop {

template <typename C>
struct Iterable : public RelOperator {
  using column_types = typename C::element_type::column_types;
  using tuple_type = typename TypeListToTuple<column_types>::type;
  using collection_type = typename C::element_type;

  explicit Iterable(const C& collection_) :
      collection(collection_) {
    id = relop_counter;
    relop_counter += 1;
    source_iterables.insert(id);
    column_names = collection->get_column_names();
  }

  std::vector<tuple_type> execute() {
    std::vector<tuple_type> result;
    tuple_type* tp_ptr = next(REGULAR);
    while (tp_ptr != nullptr) {
      result.push_back(*tp_ptr);
      tp_ptr = next(REGULAR);
    }
    return result;
  }

  template <typename Q = C>
  typename std::enable_if<GetCollectionType<typename Q::element_type>::value == CollectionType::SCRATCH, tuple_type*>::type
  next(int delta_flag) {
    const std::set<tuple_type>& dataset = (delta_flag == REGULAR) ? collection->get() : collection->get_delta();
    if (new_invocation) {
      it = dataset.begin();
      new_invocation = false;
    }
    if (it != dataset.end()) {
      next_tuple = *it;
      std::advance(it, 1);
      return &next_tuple;
    } else {
      new_invocation = true;
      return nullptr;
    }
  }

  template <typename Q = C>
  typename std::enable_if<GetCollectionType<typename Q::element_type>::value == CollectionType::TABLE, tuple_type*>::type
  next(int delta_flag) {
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

  template <typename Q = C>
  typename std::enable_if<!(GetCollectionType<typename Q::element_type>::value == CollectionType::TABLE)
                          && !(GetCollectionType<typename Q::element_type>::value == CollectionType::SCRATCH), tuple_type*>::type
  next(int delta_flag) {
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

  void push(void* upstream_tp_ptr, int stratum, int delta_flag) {
    std::cout << "push called for iterable with id " << std::to_string(id) << "\n";
    if (stratum == -1 || strata.find(stratum) != strata.end()) {
      std::cout << "stratum " << std::to_string(stratum) << " is in my set\n";
      tuple_type* tp_ptr = next(delta_flag);
      while (tp_ptr != nullptr) {
        for (auto& op : downstreams) {
          if (stratum == -1 || op->strata.find(stratum) != op->strata.end()) {
            op->push(tp_ptr, stratum, id);
          }
        }
        tp_ptr = next(delta_flag);
      }
      for (auto& op : downstreams) {
        if (stratum == -1 || op->strata.find(stratum) != op->strata.end()) {
          op->push(tp_ptr, stratum, id);
        }
      }
    } else {
      std::cout << "stratum " << std::to_string(stratum) << " is not in my set\n";
    }
  }

  void find_scratch(std::set<std::string>& scratches) {
    for (auto& op : downstreams) {
      op->find_scratch(scratches);
    }
  }

  template <typename Q = C>
  typename std::enable_if<GetCollectionType<typename Q::element_type>::value == CollectionType::SCRATCH,
      int>::type
  get_collection_stratum() {
    return collection->get_stratum();
  }

  template <typename Q = C>
  typename std::enable_if<!(GetCollectionType<typename Q::element_type>::value == CollectionType::SCRATCH),
      int>::type
  get_collection_stratum() {
    return 0;
  }

  void assign_stratum(int current_stratum, std::set<RelOperator*> ops, std::unordered_map<int, std::set<std::set<int>>>& stratum_iterables_map, int& max_stratum) {
    ops.insert(this);
    if (downstreams.size() == 0) {
      for (auto op : ops) {
        (op->strata).insert(current_stratum);
      }
      stratum_iterables_map[current_stratum].insert(source_iterables);
    } else {
      for (auto& op : downstreams) {
        op->assign_stratum(current_stratum, ops, stratum_iterables_map, max_stratum);
      }
    }
  }

  template <typename Q = C>
  typename std::enable_if<GetCollectionType<typename Q::element_type>::value == CollectionType::SCRATCH,
      bool>::type
  delta() {
    if (collection->get_delta().size() != 0) {
      return true;
    } else {
      return false;
    }
  }

  template <typename Q = C>
  typename std::enable_if<!(GetCollectionType<typename Q::element_type>::value == CollectionType::SCRATCH),
      bool>::type
  delta() {
    return false;
  }

  int id;
  const C collection;
  typename C::element_type::container_type::const_iterator it;
  tuple_type next_tuple;
  std::array<std::string, TypeListLen<column_types>::value> column_names;
  bool new_invocation = true;
  std::vector<std::shared_ptr<RelOperator>> downstreams;
};

template <typename C>
std::shared_ptr<Iterable<C>> make_iterable(const C& collection) {
  return std::make_shared<Iterable<C>>(collection);
}

}  // namespace rop
}  // namespace relational

#endif  // RELOP_ITERABLE_HPP_