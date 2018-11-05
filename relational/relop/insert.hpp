#ifndef RELOP_INSERT_HPP_
#define RELOP_INSERT_HPP_

#include <vector>

#include "collections/all.hpp"
#include "common/hash_util.hpp"
#include "common/keys.hpp"
#include "common/type_list.hpp"
#include "relop/relop.hpp"

namespace relational {
namespace rop {

template <typename Rel, typename Keys, typename... Ts>
struct Insert;

template <typename Rel, std::size_t... Ks, typename... Ts>
struct Insert<Rel, Keys<Ks...>, Ts...> : public RelOperator {
  using child_column_types = TypeList<Ts...>;
  using child_tuple_type = typename TypeListToTuple<child_column_types>::type;
  using tuple_type = std::vector<std::tuple<bool>>;

  explicit Insert(Rel child_, FluentRelation<Keys<Ks...>, Ts...>* relation_) :
      child(std::move(child_)),
      relation(relation_) {
    column_names = {"Inserted"};
  }

  std::vector<tuple_type> execute() {
    std::vector<tuple_type> result(column_names);
    std::tuple<bool>* tp_ptr = next();
    while (tp_ptr != nullptr) {
      result.push_back(*tp_ptr);
      tp_ptr = next();
    }
    return result;
  }

  std::tuple<bool>* next() {
    if (!emitted) {
      child_tuple_type* child_next = child.next();
      while (child_next != nullptr) {
        bool result = relation->insert(*child_next);
        if (result) {
          inserted = true;
        }
        child_next = child.next();
      }
      emitted = true;
      next_tuple = std::make_tuple(inserted);
      return &next_tuple;
    } else {
      // reset
      inserted = false;
      emitted = false;
      return nullptr;
    }
  }

  Rel child;
  FluentRelation<Keys<Ks...>, Ts...>* relation;
  std::tuple<bool> next_tuple;
  std::array<std::string, 1> column_names;
  bool inserted = false;
  bool emitted = false;
};

template <typename Rel, typename RelDecayed = typename std::decay<Rel>::type,
          std::size_t... Ks, typename... Ts>
Insert<RelDecayed, Keys<Ks...>, Ts...> make_insert(
    Rel&& child_, FluentRelation<Keys<Ks...>, Ts...>* relation_) {
  return Insert<RelDecayed, Keys<Ks...>, Ts...>{std::forward<Rel>(child_),
                                                relation_};
}

template <typename Keys, typename... Ts>
struct InsertPipe;

template <std::size_t... Ks, typename... Ts>
struct InsertPipe<Keys<Ks...>, Ts...> {
  InsertPipe(FluentRelation<Keys<Ks...>, Ts...>* relation_) :
      relation(relation_) {}
  FluentRelation<Keys<Ks...>, Ts...>* relation;
};

template <std::size_t... Ks, typename... Ts>
InsertPipe<Keys<Ks...>, Ts...> insert(
    FluentRelation<Keys<Ks...>, Ts...>* relation_) {
  return {relation_};
}

template <typename Rel, typename RelDecayed = typename std::decay<Rel>::type,
          std::size_t... Ks, typename... Ts>
Insert<RelDecayed, Keys<Ks...>, Ts...> operator|(
    Rel&& child_, InsertPipe<Keys<Ks...>, Ts...> p_) {
  return make_insert(std::forward<Rel&&>(child_), p_.relation);
}

}  // namespace rop
}  // namespace relational

#endif  // RELOP_INSERT_HPP_