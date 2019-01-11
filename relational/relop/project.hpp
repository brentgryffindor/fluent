#ifndef RELOP_PROJECT_HPP_
#define RELOP_PROJECT_HPP_

#include <iostream>

#include "common/tuple_util.hpp"
#include "common/type_list.hpp"
#include "relop/relop.hpp"

namespace relational {
namespace rop {

template <typename Rel, std::size_t... Is>
struct Project : public RelOperator {
  using upstream_column_types = typename Rel::column_types;
  using column_types =
      typename TypeListProject<upstream_column_types, Is...>::type;
  using upstream_tuple_type =
      typename TypeListToTuple<upstream_column_types>::type;
  using tuple_type = typename TypeListToTuple<column_types>::type;

  explicit Project(const std::shared_ptr<Rel>& upstream_) {
    id = relop_counter;
    relop_counter += 1;
    source_iterables = upstream_->source_iterables;
    column_names = {upstream_->column_names[Is]...};
  }

  void push(void* upstream_tp_ptr, int stratum, int upstream_op_id) {
    std::cout << "push called for project with id " << std::to_string(id) << "\n";
    if (upstream_tp_ptr == nullptr) {
      std::cout << "reach end\n";
      for (auto& op : downstreams) {
        if (stratum == -1 || op->strata.find(stratum) != op->strata.end()) {
          op->push(upstream_tp_ptr, stratum, id);
        }
      }
    } else {
      upstream_tuple_type* casted_upstream_tp_ptr =
          static_cast<upstream_tuple_type*>(upstream_tp_ptr);
      std::cout << "tuple's first colume is " +
                       std::to_string(std::get<0>(*casted_upstream_tp_ptr)) +
                       "\n";
      next_tuple = TupleProject<Is...>(*casted_upstream_tp_ptr);
      for (auto& op : downstreams) {
        if (stratum == -1 || op->strata.find(stratum) != op->strata.end()) {
          op->push(&next_tuple, stratum, id);
        }
      }
    }
  }

  void find_scratch(std::set<std::string>& scratches) {
    for (auto& op : downstreams) {
      op->find_scratch(scratches);
    }
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

  int id;
  std::vector<std::shared_ptr<RelOperator>> downstreams;
  tuple_type next_tuple;
  std::array<std::string, TypeListLen<column_types>::value> column_names;
};

template <std::size_t... Is, typename Rel,
          typename RelDecayed = typename std::decay<Rel>::type>
std::shared_ptr<Project<RelDecayed, Is...>> make_project(
    const std::shared_ptr<Rel>& upstream_) {
  auto project_ptr = std::make_shared<Project<RelDecayed, Is...>>(upstream_);
  upstream_->downstreams.push_back(project_ptr);
  return project_ptr;
}

template <std::size_t... Is>
struct ProjectPipe {};

template <size_t... Is>
ProjectPipe<Is...> project() {
  return {};
}

template <std::size_t... Is, typename Rel,
          typename RelDecayed = typename std::decay<Rel>::type>
std::shared_ptr<Project<RelDecayed, Is...>> operator|(
    const std::shared_ptr<Rel>& upstream_, ProjectPipe<Is...>) {
  return make_project<Is...>(upstream_);
}

}  // namespace rop
}  // namespace relational

#endif  // RELOP_PROJECT_HPP_