#ifndef RELOP_GROUPBY_HPP_
#define RELOP_GROUPBY_HPP_

#include <iostream>

#include "common/hash_util.hpp"
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
  using upstream_column_types = typename Rel::column_types;
  using upstream_tuple_type = typename TypeListToTuple<upstream_column_types>::type;

  using key_types = typename TypeListProject<upstream_column_types, Ks...>::type;
  using aggregate_impl_types = TypeList<  //
      typename Aggregates::template type<
          typename detail::TypeListProjectBySizetList<
              upstream_column_types,
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

  explicit GroupBy(const std::shared_ptr<Rel>& upstream_, group_by<Keys<Ks...>, Aggregates...> gb_) {
    id = relop_counter;
    relop_counter += 1;
    source_iterables = upstream_->source_iterables;
    std::array<std::string, sizeof...(Ks)> key_column_names = {
        upstream_->column_names[Ks]...};
    std::copy(key_column_names.begin(), key_column_names.end(),
              column_names.begin());
    std::copy(gb_.agg_column_names.begin(), gb_.agg_column_names.end(),
              column_names.begin() + key_column_names.size());
  }

  void push(void* upstream_tp_ptr, int stratum, int upstream_op_id) {
    std::cout << "push called for groupby with id " << std::to_string(id) << "\n";
    if (upstream_tp_ptr != nullptr) {
      upstream_tuple_type* casted_upstream_tp_ptr =
          static_cast<upstream_tuple_type*>(upstream_tp_ptr);
      auto& group = groups[TupleProject<Ks...>(*casted_upstream_tp_ptr)];
      TupleIter(group, [this, &casted_upstream_tp_ptr](auto& agg) {
        this->UpdateAgg(&agg, *casted_upstream_tp_ptr);
      });
    } else {
      std::cout << "exhausted upstream tuples\n";
      for (const auto& pair : groups) {
        auto groups_tuple =
            TupleMap(pair.second, [](const auto& agg) { return agg.Get(); });
        next_tuple = std::tuple_cat(pair.first, std::move(groups_tuple));
        for (auto& op : downstreams) {
          if (stratum == -1 || op->strata.find(stratum) != op->strata.end()) {
            op->push(&next_tuple, stratum, id);
          }
        }
      }
      std::cout << "reach end\n";
      for (auto& op : downstreams) {
        if (stratum == -1 || op->strata.find(stratum) != op->strata.end()) {
          op->push(nullptr, stratum, id);
        }
      }
      groups.clear();
    }
  }

  void find_scratch(std::set<std::string>& scratches) {
    for (auto& op : downstreams) {
      op->find_scratch(scratches);
    }
  }

  void assign_stratum(int current_stratum, std::set<RelOperator*> ops, std::unordered_map<int, std::set<std::set<int>>>& stratum_iterables_map, int& max_stratum) {
    ops.insert(this);
    int new_strarum = current_stratum + 1;
    if (new_strarum > max_stratum) {
      max_stratum = new_strarum;
    }
    if (downstreams.size() == 0) {
      for (auto op : ops) {
        (op->strata).insert(new_strarum);
      }
      stratum_iterables_map[new_strarum].insert(source_iterables);
    } else {
      for (auto& op : downstreams) {
        op->assign_stratum(new_strarum, ops, stratum_iterables_map, max_stratum);
      }
    }
  }

  template <template <typename, typename> class AggregateImpl,  //
            typename Columns, typename Ts, typename... Us>
  void UpdateAgg(AggregateImpl<Columns, Ts>* agg, const std::tuple<Us...>& t) {
    agg->Update(TupleProjectBySizetList<Columns>(t));
  }

  int id;
  std::vector<std::shared_ptr<RelOperator>> downstreams;
  tuple_type next_tuple;
  std::array<std::string, TypeListLen<column_types>::value> column_names;
  std::unordered_map<key_tuple_type, aggregate_impl_tuple_types,
                     Hash<key_tuple_type>>
      groups;
};

template <typename Rel, std::size_t... Ks, typename... Aggregates,
          typename RelDecayed = typename std::decay<Rel>::type>
std::shared_ptr<GroupBy<RelDecayed, Keys<Ks...>, Aggregates...>> operator|(
    const std::shared_ptr<Rel>& upstream_, group_by<Keys<Ks...>, Aggregates...> gb) {
  auto gorupby_ptr = std::make_shared<GroupBy<RelDecayed, Keys<Ks...>, Aggregates...>>(upstream_, gb);
  upstream_->downstreams.push_back(gorupby_ptr);
  return gorupby_ptr;
}

}  // namespace rop
}  // namespace relational

#endif  // RELOP_GROUPBY_HPP_