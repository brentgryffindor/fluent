#ifndef RELOP_AGGOPS_AGGREGATES_HPP_
#define RELOP_AGGOPS_AGGREGATES_HPP_

#include "common/sizet_list.hpp"
#include "common/tuple_util.hpp"
#include "common/type_list.hpp"
#include "common/type_traits.hpp"

namespace relational {
namespace rop {
namespace agg {

// The GroupBy class is paramaterized on a variadic number of aggregates. This
// allows us to express the following SQL query
//
//   SELECT R.a, R.b, SUM(R.c), AVG(R.d), COUNT(R.e)
//   FROM R
//   GROUP BY R.a, R.b
//
// in C++
//
//   make_iterable("r", r) | group_by<Keys<0, 1>, Sum<2>, Avg<3>, Count<4>>();
//
// Each of those aggregates should look something like this:
//
//   template <typename SizetList, typename TypeList>
//   class AggregateImpl;
//
//   template <std::size_t... Is, typename... Ts>
//   class AggregateImpl<SizetList<Is...>, TypeList<Ts...>> {
//    public:
//     void Update(const std::tuple<Ts...>& x) { ... }
//     U Get() const { ... }
//   };
//
//   template <std::size_t... Is>
//   struct Aggregate {
//     template <typename TypeList>
//     using type = AggregateImpl<SizetList<Is...>, TypeList>;
//
//     static std::string ToDebugString const { ... }
//   };
//
// Each aggregate (e.g. Sum, Avg, Count) is paramaterized on a variadic number
// of size_ts denoting the columns over which the aggregate will be applied.
// The struct contains a single type, called `type`, that is paramaterized on
// the columns `Is` and a list of types `Ts`. `Ts` will be instantiated with
// the types of the `Is`th columns.
//
// The aggregate implementation (e.g. SumImpl, AvgImpl) has a method Update
// which takes in values of the column, and a method `Get` which returns the
// final aggregate. The return type of Get is arbitrary.
struct Aggregate {
  virtual ~Aggregate() {}
};

struct AggregateImpl {
  virtual ~AggregateImpl() {}
};

// Sum /////////////////////////////////////////////////////////////////////////
template <typename SizetList, typename TypeList>
class SumImpl;

template <std::size_t... Is, typename T, typename... Ts>
class SumImpl<SizetList<Is...>, TypeList<T, Ts...>> : public AggregateImpl {
 public:
  SumImpl() : sum_() {}
  void Update(const std::tuple<T, Ts...>& t) {
    TupleIter(t, [this](const T& x) { sum_ += x; });
  }
  T Get() const { return sum_; }

 private:
  T sum_;
};

template <std::size_t... Is>
struct Sum : public Aggregate {
  template <typename TypeList>
  using type = SumImpl<SizetList<Is...>, TypeList>;
};

// Count ///////////////////////////////////////////////////////////////////////
template <typename SizetList, typename TypeList>
class CountImpl;

template <std::size_t... Is, typename... Ts>
class CountImpl<SizetList<Is...>, TypeList<Ts...>> : public AggregateImpl {
 public:
  void Update(const std::tuple<Ts...>&) { count_++; }
  std::size_t Get() const { return count_; }

 private:
  std::size_t count_ = 0;
};

template <std::size_t... Is>
struct Count : public Aggregate {
  template <typename TypeList>
  using type = CountImpl<SizetList<Is...>, TypeList>;
};

// Avg /////////////////////////////////////////////////////////////////////////
template <typename SizetList, typename TypeList>
class AvgImpl;

template <std::size_t... Is, typename T, typename... Ts>
class AvgImpl<SizetList<Is...>, TypeList<T, Ts...>> : public AggregateImpl {
 public:
  void Update(const std::tuple<T, Ts...>& t) {
    TupleIter(t, [this](const T& x) {
      sum_ += x;
      count_++;
    });
  }

  double Get() const { return sum_ / count_; }

 private:
  double sum_ = 0;
  std::size_t count_ = 0;
};

template <std::size_t... Is>
struct Avg : public Aggregate {
  template <typename TypeList>
  using type = AvgImpl<SizetList<Is...>, TypeList>;
};

// Batch ///////////////////////////////////////////////////////////////////////
template <typename SizetList, typename TypeList>
class BatchImpl;

template <std::size_t... Is, typename T, typename... Ts>
class BatchImpl<SizetList<Is...>, TypeList<T, Ts...>> : public AggregateImpl {
 public:
  void Update(const std::tuple<T, Ts...>& t) { vec_.push_back(t); }
  std::vector<std::tuple<T, Ts...>> Get() const { return vec_; }

 private:
  std::vector<std::tuple<T, Ts...>> vec_;
};

template <std::size_t... Is>
struct Batch : public Aggregate {
  template <typename TypeList>
  using type = BatchImpl<SizetList<Is...>, TypeList>;
};

}  // namespace agg
}  // namespace rop
}  // namespace relational

#endif  //  RELOP_AGGOPS_AGGREGATES_HPP_