#ifndef RELOP_INSERT_HPP_
#define RELOP_INSERT_HPP_

#include <vector>
#include <unordered_map>

#include "collections/all.hpp"
#include "collections/collection_util.hpp"
#include "common/hash_util.hpp"
#include "common/type_list.hpp"
#include "relop/relop.hpp"

namespace relational {
namespace rop {

template <typename Rel, typename C>
struct Insert : public RelOperator {
  using upstream_column_types = typename Rel::column_types;
  using upstream_tuple_type = typename TypeListToTuple<upstream_column_types>::type;
  using tuple_type = std::vector<std::tuple<bool>>;

  explicit Insert(const std::shared_ptr<Rel>& upstream_, const C& collection_) :
      collection(collection_) {
    id = relop_counter;
    relop_counter += 1;
    source_iterables = upstream_->source_iterables;
    column_names = {"Inserted"};
  }

  void push(void* upstream_tp_ptr, unsigned stratum, unsigned upstream_op_id) {
    std::cout << "push called for insert and with id " << std::to_string(id) << "\n";
    if (upstream_tp_ptr == nullptr) {
      std::cout << "reach end\n";
      next_tuple = std::make_tuple(inserted);
      // first send if any tuple is inserted
      for (auto& op : downstreams) {
        if (stratum == -1 || op->strata.find(stratum) != op->strata.end()) {
          op->push(&next_tuple, stratum, id);
        }
      }
      // then send punctuation
      for (auto& op : downstreams) {
        if (stratum == -1 || op->strata.find(stratum) != op->strata.end()) {
          op->push(nullptr, stratum, id);
        }
      }
      // reset status
      inserted = false;
      // decrement dependency count for scratch
      handle_scratch_dependency();
    } else {
      upstream_tuple_type* casted_upstream_tp_ptr =
          static_cast<upstream_tuple_type*>(upstream_tp_ptr);
      collection->buffer_insertion(*casted_upstream_tp_ptr);
      inserted = true;
    }
  }

  template <typename Q = C>
  typename std::enable_if<GetCollectionType<typename Q::element_type>::value == CollectionType::SCRATCH,
      void>::type
  handle_scratch_dependency() {
    collection->decrement_dependency_count();
  }

  template <typename Q = C>
  typename std::enable_if<!(GetCollectionType<typename Q::element_type>::value == CollectionType::SCRATCH),
      void>::type
  handle_scratch_dependency() {}

  void find_scratch(std::set<std::string>& scratches) {
    if (GetCollectionType<typename C::element_type>::value == CollectionType::SCRATCH) {
      scratches.insert(collection->get_name());
    }
    for (auto& op : downstreams) {
      op->find_scratch(scratches);
    }
  }

  template <typename Q = C>
  typename std::enable_if<GetCollectionType<typename Q::element_type>::value == CollectionType::SCRATCH,
      void>::type
  handle_stratum(unsigned& current_stratum) {
    collection->set_stratum(current_stratum);
  }

  template <typename Q = C>
  typename std::enable_if<!(GetCollectionType<typename Q::element_type>::value == CollectionType::SCRATCH),
      void>::type
  handle_stratum(unsigned& current_stratum) {
    current_stratum += 1;
  }

  void assign_stratum(unsigned current_stratum, std::set<RelOperator*> ops, std::unordered_map<unsigned, std::set<std::set<unsigned>>>& stratum_iterables_map, unsigned& max_stratum) {
    ops.insert(this);
    handle_stratum(current_stratum);
    if (current_stratum > max_stratum) {
      max_stratum = current_stratum;
    }
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

  unsigned id;
  std::vector<std::shared_ptr<RelOperator>> downstreams;
  C collection;
  std::tuple<bool> next_tuple;
  std::array<std::string, 1> column_names;
  bool inserted = false;
};

template <typename Rel, typename RelDecayed = typename std::decay<Rel>::type,
          typename C>
typename std::enable_if<GetCollectionType<typename C::element_type>::value == CollectionType::SCRATCH,
      std::shared_ptr<Insert<RelDecayed, C>>>::type
make_insert(
    const std::shared_ptr<Rel>& upstream_, const C& collection_) {
  collection_->increment_dependency_count();
  auto insert_ptr = std::make_shared<Insert<RelDecayed, C>>(upstream_, collection_);
  upstream_->downstreams.push_back(insert_ptr);
  return insert_ptr;
}

template <typename Rel, typename RelDecayed = typename std::decay<Rel>::type,
          typename C>
typename std::enable_if<!(GetCollectionType<typename C::element_type>::value == CollectionType::SCRATCH),
      std::shared_ptr<Insert<RelDecayed, C>>>::type
make_insert(
    const std::shared_ptr<Rel>& upstream_, const C& collection_) {
  auto insert_ptr = std::make_shared<Insert<RelDecayed, C>>(upstream_, collection_);
  upstream_->downstreams.push_back(insert_ptr);
  return insert_ptr;
}

template <typename C>
struct InsertPipe {
  InsertPipe(const C& collection_) : collection(collection_) {}
  C collection;
};

template <typename C>
InsertPipe<C> insert(const C& collection_) {
  return {collection_};
}

template <typename Rel, typename RelDecayed = typename std::decay<Rel>::type,
          typename C>
std::shared_ptr<Insert<RelDecayed, C>> operator|(const std::shared_ptr<Rel>& upstream_,
                                InsertPipe<C> p_) {
  return make_insert(upstream_, p_.collection);
}

}  // namespace rop
}  // namespace relational

#endif  // RELOP_INSERT_HPP_