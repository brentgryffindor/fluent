#ifndef RELOP_RELOP_HPP_
#define RELOP_RELOP_HPP_

#include <string>
#include <set>

namespace relational {
namespace rop {

static unsigned relop_counter = 0;

struct RelOperator {
  virtual ~RelOperator() {}
  virtual void push(void* upstream_tp_ptr, unsigned stratum, unsigned upstream_op_id) = 0;
  virtual void find_scratch(std::set<std::string>& scratches) = 0;
  virtual void assign_stratum(unsigned current_stratum, std::set<RelOperator*> ops, std::unordered_map<unsigned, std::set<std::set<unsigned>>>& stratum_iterables_map, unsigned& max_stratum) = 0;
  std::set<unsigned> strata;
  std::set<unsigned> source_iterables;
};

}  // namespace rop
}  // namespace relational

#endif  // RELOP_RELOP_HPP_