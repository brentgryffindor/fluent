#ifndef RELOP_GROUPBY_HPP_
#define RELOP_GROUPBY_HPP_

#include <vector>

#include "common/keys.hpp"
#include "common/tuple_util.hpp"
#include "common/type_list.hpp"
#include "relop/aggops/aggregates.hpp"
#include "relop/relop.hpp"

namespace relational {
namespace rop {

template <typename Keys, typename... Aggregates>
struct group_by;

template <std::size_t... Ks, typename... Aggregates>
struct group_by<Keys<Ks...>, Aggregates...> {
  group_by(std::array<std::string, sizeof...(Aggregates)> agg_column_names_) :
      agg_column_names(agg_column_names_) {}
  std::array<std::string, sizeof...(Aggregates)> agg_column_names;
};

namespace detail {

// ProjectBySizetList
template <typename Typelist, typename SizetList>
struct TypeListProjectBySizetList;

template <typename... Ts, std::size_t... Is>
struct TypeListProjectBySizetList<TypeList<Ts...>, SizetList<Is...>> {
  using type = typename TypeListProject<TypeList<Ts...>, Is...>::type;
};

// TypeOfGet
template <typename T>
struct TypeOfGet {
  using type = typename std::decay<decltype(std::declval<T>().Get())>::type;
};

}  // namespace detail

template <typename Rel, typename Keys, typename... Aggregates>
struct GroupBy;

template <typename Rel, std::size_t... Ks, typename... Aggregates>
struct GroupBy<Rel, Keys<Ks...>, Aggregates...> : public RelOperator {
  using child_column_types = typename Rel::column_types;
  using child_tuple_type = typename TypeListToTuple<child_column_types>::type;

  using key_types = typename TypeListProject<child_column_types, Ks...>::type;
  using aggregate_impl_types = TypeList<  //
      typename Aggregates::template type<
          typename detail::TypeListProjectBySizetList<
              child_column_types,
              typename SizetListFrom<Aggregates>::type>::type>...  //
      >;
  using aggregate_types =
      typename TypeListMap<aggregate_impl_types, detail::TypeOfGet>::type;

  using column_types =
      typename TypeListConcat<key_types, aggregate_types>::type;
  using key_tuple_type = typename TypeListToTuple<key_types>::type;
  using aggregate_impl_tuple_types =
      typename TypeListToTuple<aggregate_impl_types>::type;
  using tuple_type = typename TypeListToTuple<column_types>::type;

  explicit GroupBy(Rel child_, group_by<Keys<Ks...>, Aggregates...> gb_) :
      child(std::move(child_)) {
    std::array<std::string, sizeof...(Ks)> key_column_names = {
        child.column_names[Ks]...};
    std::copy(key_column_names.begin(), key_column_names.end(),
              column_names.begin());
    std::copy(gb_.agg_column_names.begin(), gb_.agg_column_names.end(),
              column_names.begin() + key_column_names.size());
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
    if (!aggregated) {
      // perform aggregation
      child_tuple_type* child_tp_ptr = child.next();
      while (child_tp_ptr != nullptr) {
        auto& group = groups[TupleProject<Ks...>(*child_tp_ptr)];
        TupleIter(group, [this, &child_tp_ptr](auto& agg) {
          this->UpdateAgg(&agg, *child_tp_ptr);
        });
        child_tp_ptr = child.next();
      }
      aggregated = true;
    }
    if (groups.size() != 0) {
      auto it = groups.begin();
      const auto& keys_tuple = it->first;
      auto groups_tuple =
          TupleMap(it->second, [](const auto& agg) { return agg.Get(); });
      next_tuple = std::tuple_cat(keys_tuple, std::move(groups_tuple));
      groups.erase(keys_tuple);

      return &next_tuple;
    } else {
      // clear hash table and reset flag
      groups.clear();
      aggregated = false;
      return nullptr;
    }
  }

  template <template <typename, typename> class AggregateImpl,  //
            typename Columns, typename Ts, typename... Us>
  void UpdateAgg(AggregateImpl<Columns, Ts>* agg, const std::tuple<Us...>& t) {
    agg->Update(TupleProjectBySizetList<Columns>(t));
  }

  Rel child;
  tuple_type next_tuple;
  std::array<std::string, TypeListLen<column_types>::value> column_names;
  bool aggregated = false;
  std::unordered_map<key_tuple_type, aggregate_impl_tuple_types,
                     Hash<key_tuple_type>>
      groups;
};

template <typename Rel, std::size_t... Ks, typename... Aggregates,
          typename RelDecayed = typename std::decay<Rel>::type>
GroupBy<RelDecayed, Keys<Ks...>, Aggregates...> operator|(
    Rel&& child, group_by<Keys<Ks...>, Aggregates...> gb) {
  return GroupBy<RelDecayed, Keys<Ks...>, Aggregates...>(
      std::forward<Rel&&>(child), gb);
}

}  // namespace rop
}  // namespace relational

#endif  // RELOP_GROUPBY_HPP_