#ifndef RELOP_RELOP_HPP_
#define RELOP_RELOP_HPP_

#include <string>
#include <set>
#include <unordered_map>

namespace relational {
namespace rop {

static int relop_counter = 0;

struct RelOperator {
  virtual ~RelOperator() {}
  virtual void push(void* upstream_tp_ptr, int stratum, int upstream_op_id) = 0;
  virtual void find_scratch(std::set<std::string>& scratches) = 0;
  virtual void assign_stratum(int current_stratum, std::set<RelOperator*> ops, std::unordered_map<int, std::set<std::set<int>>>& stratum_iterables_map, int& max_stratum) = 0;
  std::set<int> strata;
  std::set<int> source_iterables;
};

}  // namespace rop
}  // namespace relational

#endif  // RELOP_RELOP_HPP_