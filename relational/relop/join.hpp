#ifndef RELOP_JOIN_HPP_
#define RELOP_JOIN_HPP_

#include <iostream>

#include "common/hash_util.hpp"
#include "common/keys.hpp"
#include "common/type_list.hpp"
#include "relop/relop.hpp"

namespace relational {
namespace rop {

template <typename Left, typename LeftKeys, typename Right, typename RightKeys>
struct Join;

template <typename Left, std::size_t... LeftKs, typename Right,
          std::size_t... RightKs>
struct Join<Left, LeftKeys<LeftKs...>, Right, RightKeys<RightKs...>>
    : public RelOperator {
  using left_column_types = typename Left::column_types;
  using right_column_types = typename Right::column_types;
  using left_tuple_type = typename TypeListToTuple<left_column_types>::type;
  using right_tuple_type = typename TypeListToTuple<right_column_types>::type;

  using left_key_column_types =
      typename TypeListProject<left_column_types, LeftKs...>::type;
  using left_key_tuple_type =
      typename TypeListToTuple<left_key_column_types>::type;

  using right_key_column_types =
      typename TypeListProject<right_column_types, RightKs...>::type;
  using right_key_tuple_type =
      typename TypeListToTuple<right_key_column_types>::type;

  using column_types =
      typename TypeListConcat<left_column_types, right_column_types>::type;
  using tuple_type = typename TypeListToTuple<column_types>::type;

  Join(const std::shared_ptr<Left>& left_, const std::shared_ptr<Right>& right_) {
    id = relop_counter;
    relop_counter += 1;
    source_iterables = left_->source_iterables;
    for (const auto& it : right_->source_iterables) {
      source_iterables.insert(it);
    }
    if (source_iterables.size() != left_->source_iterables.size() + right_->source_iterables.size()) {
      std::cerr << "duplicate source iterable!\n";
      // TODO: throw exception
    }
    left_id = left_->id;
    right_id = right_->id;
    std::copy(left_->column_names.begin(),
              left_->column_names.begin() +
                  TypeListLen<typename Left::column_types>::value,
              column_names.begin());
    std::copy(
        right_->column_names.begin(),
        right_->column_names.begin() +
            TypeListLen<typename Right::column_types>::value,
        column_names.begin() + TypeListLen<typename Left::column_types>::value);
  }

  /*std::vector<tuple_type> execute() {
    std::vector<tuple_type> result(column_names);
    tuple_type* tp_ptr = next();
    while (tp_ptr != nullptr) {
      result.push_back(*tp_ptr);
      tp_ptr = next();
    }
    return result;
  }

  tuple_type* next() {
    if (!built) {
      // first, we build the hash table using the left relation
      left_tuple_type* left_next = left.next();
      while (left_next != nullptr) {
        left_map[TupleProject<LeftKs...>(*left_next)].push_back(*left_next);
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
        right_key_tuple_type right_key_tuple =
            TupleProject<RightKs...>(*right_next);
        if (left_map.find(right_key_tuple) == left_map.end()) {
          return next();
        } else {
          for (const left_tuple_type& ltp : left_map[right_key_tuple]) {
            next_tuples.push_back(std::tuple_cat(ltp, *right_next));
          }
          return next();
        }
      } else {
        // clear hash table and reset flag
        left_map.clear();
        built = false;
        return nullptr;
      }
    }
  }*/

  void push(void* upstream_tp_ptr, int stratum, int upstream_op_id) {
    std::cout << "push called for join and with id " << std::to_string(id) << "\n";
    if (upstream_tp_ptr == nullptr) {
      if (upstream_op_id == left_id) {
        left_done = true;
      } else if (upstream_op_id == right_id) {
        right_done = true;
      } else {
        std::cerr << "op id does not match left or right\n";
      }
      // send punctuation if both side are drained
      if (left_done && right_done) {
        for (auto& op : downstreams) {
          if (stratum == -1 || op->strata.find(stratum) != op->strata.end()) {
            op->push(upstream_tp_ptr, stratum, id);
          }
        }
        // reset state
        left_done = false;
        right_done = false;
        left_map.clear();
        right_map.clear();
      }
    } else {
      if (upstream_op_id == left_id) {
        left_tuple_type* casted_left_tp_ptr =
            static_cast<left_tuple_type*>(upstream_tp_ptr);
        left_key_tuple_type left_key_tuple =
            TupleProject<LeftKs...>(*casted_left_tp_ptr);
        left_map[left_key_tuple].push_back(*casted_left_tp_ptr);
        if (right_map.find(left_key_tuple) != right_map.end()) {
          for (const right_tuple_type& rtp : right_map[left_key_tuple]) {
            next_tuples.push_back(std::tuple_cat(*casted_left_tp_ptr, rtp));
          }
          while (next_tuples.size() != 0) {
            next_tuple = next_tuples.back();
            next_tuples.pop_back();
            for (auto& op : downstreams) {
              if (stratum == -1 || op->strata.find(stratum) != op->strata.end()) {
                op->push(&next_tuple, stratum, id);
              }
            }            
          }
        }
      } else if (upstream_op_id == right_id) {
        right_tuple_type* casted_right_tp_ptr =
            static_cast<right_tuple_type*>(upstream_tp_ptr);
        right_key_tuple_type right_key_tuple =
            TupleProject<RightKs...>(*casted_right_tp_ptr);
        right_map[right_key_tuple].push_back(*casted_right_tp_ptr);
        if (left_map.find(right_key_tuple) != left_map.end()) {
          for (const left_tuple_type& ltp : left_map[right_key_tuple]) {
            next_tuples.push_back(std::tuple_cat(ltp, *casted_right_tp_ptr));
          }
          while (next_tuples.size() != 0) {
            next_tuple = next_tuples.back();
            next_tuples.pop_back();
            for (auto& op : downstreams) {
              if (stratum == -1 || op->strata.find(stratum) != op->strata.end()) {
                op->push(&next_tuple, stratum, id);
              }
            }            
          }
        }
      } else {
        std::cerr << "op id does not match left or right\n";
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
    if (stratum_count == 0) {
      stratum = current_stratum;
      stratum_count += 1;
      buffered_ops = ops;
    } else {
      stratum = std::max(stratum, current_stratum);
      for (const auto op : ops) {
        buffered_ops.insert(op);
      }
      if (downstreams.size() == 0) {
        for (auto op : buffered_ops) {
          (op->strata).insert(stratum);
        }
        stratum_iterables_map[stratum].insert(source_iterables);
      } else {
        for (auto& op : downstreams) {
          op->assign_stratum(stratum, buffered_ops, stratum_iterables_map, max_stratum);
        }
      }
    }
  }

  int id;
  int left_id;
  int right_id;
  //Left left;
  //Right right;
  std::vector<std::shared_ptr<RelOperator>> downstreams;
  tuple_type next_tuple;

  std::array<std::string, TypeListLen<column_types>::value> column_names;
  std::unordered_map<left_key_tuple_type, std::vector<left_tuple_type>,
                     Hash<left_key_tuple_type>>
      left_map;
  std::unordered_map<right_key_tuple_type, std::vector<right_tuple_type>,
                     Hash<right_key_tuple_type>>
      right_map;
  bool left_done = false;
  bool right_done = false;
  std::vector<tuple_type> next_tuples;
  int stratum;
  int stratum_count = 0;
  std::set<RelOperator*> buffered_ops;
};

template <typename LeftKeys, typename RightKeys, typename Left, typename Right,
          typename LeftDecayed = typename std::decay<Left>::type,
          typename RightDecayed = typename std::decay<Right>::type>
std::shared_ptr<Join<LeftDecayed, LeftKeys, RightDecayed, RightKeys>> make_join(const std::shared_ptr<Left>& left_,
                                                               const std::shared_ptr<Right>& right_) {
  auto join_ptr = std::make_shared<Join<LeftDecayed, LeftKeys, RightDecayed, RightKeys>>(left_, right_);
  left_->downstreams.push_back(join_ptr);
  right_->downstreams.push_back(join_ptr);
  return join_ptr;
}

}  // namespace rop
}  // namespace relational

#endif  // RELOP_JOIN_HPP_