#ifndef RELOP_ERASE_HPP_
#define RELOP_ERASE_HPP_

#include <vector>

#include "collections/all.hpp"
#include "common/hash_util.hpp"
#include "common/type_list.hpp"
#include "relop/relop.hpp"

namespace relational {
namespace rop {

template <typename Rel, typename C>
struct Erase : public RelOperator {
  using upstream_column_types = typename Rel::column_types;
  using upstream_tuple_type = typename TypeListToTuple<upstream_column_types>::type;
  using tuple_type = std::vector<std::tuple<bool>>;

  explicit Erase(const C& collection_) :
      collection(collection_) {
    id = relop_counter;
    relop_counter += 1;
    column_names = {"Erased"};
  }

  void push(void* upstream_tp_ptr, unsigned upstream_op_id) {
    std::cout << "I am an erase and my id is " << std::to_string(id) << "\n";
    std::cout << "push called for op: ERASE\n";
    if (upstream_tp_ptr == nullptr) {
      std::cout << "reach end\n";
      next_tuple = std::make_tuple(erased);
      // first send if any tuple is Erased
      for (auto& op : downstreams) {
        op->push(&next_tuple, id);
      }
      // then send punctuation
      for (auto& op : downstreams) {
        op->push(nullptr, id);
      }
      // reset status
      erased = false;
    } else {
      upstream_tuple_type* casted_upstream_tp_ptr =
          static_cast<upstream_tuple_type*>(upstream_tp_ptr);
      collection->buffer_deletion(*casted_upstream_tp_ptr);
      erased = true;
    }
  }

  void find_scratch(std::set<std::string>& scratches) {
    for (auto& op : downstreams) {
      op->find_scratch(scratches);
    }
  }

  unsigned id;
  std::vector<std::shared_ptr<RelOperator>> downstreams;
  C collection;
  std::tuple<bool> next_tuple;
  std::array<std::string, 1> column_names;
  bool erased = false;
};

template <typename Rel, typename RelDecayed = typename std::decay<Rel>::type,
          typename C>
std::shared_ptr<Erase<RelDecayed, C>> make_erase(
    std::shared_ptr<Rel>&& upstream_, const C& collection_) {
  auto erase_ptr = std::make_shared<Erase<RelDecayed, C>>(upstream_, collection_);
  upstream_->downstreams.push_back(erase_ptr);
  return erase_ptr;
}

template <typename C>
struct ErasePipe {
  ErasePipe(const C& collection_) : collection(collection_) {}
  C collection;
};

template <typename C>
ErasePipe<C> erase(const C& collection_) {
  return {collection_};
}

template <typename Rel, typename RelDecayed = typename std::decay<Rel>::type,
          typename C>
std::shared_ptr<Erase<RelDecayed, C>> operator|(std::shared_ptr<Rel> upstream_,
                                ErasePipe<C> p_) {
  return make_erase(std::move(upstream_), p_.collection);
}

}  // namespace rop
}  // namespace relational

#endif  // RELOP_ERASE_HPP_