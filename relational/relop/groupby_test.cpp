#include "relop/groupby.hpp"
#include "relop/iterable.hpp"
#include "relop/insert.hpp"

#include <type_traits>

#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "collections/table.hpp"
#include "collections/scratch.hpp"
#include "common/keys.hpp"
#include "common/macros.hpp"

namespace relational {

TEST(GroupBy, SimpleCompileCheck) {
  std::array<std::string, 2> column_names = {"x", "y"};
  auto t = std::make_shared<Table<Keys<0,1>, char, double>>("t", column_names);
  auto it = rop::make_iterable(t);
  auto g = it | rop::group_by<Keys<0>, rop::agg::Count<1>, rop::agg::Avg<1>>({"count", "avg"});
  using removed_ref = std::remove_reference<decltype(*g)>::type;
  using actual = removed_ref::column_types;
  using expected = TypeList<char, size_t, double>;
  static_assert(StaticAssert<std::is_same<actual, expected>>::value, "");
}

TEST(GroupBy, NonEmptyTable) {
  std::array<std::string, 2> column_names = {"x", "y"};
  auto t = std::make_shared<Table<Keys<0,1>, char, double>>("t", column_names);
  std::set<std::tuple<char, size_t, double>> expected;
  t->insert(std::make_tuple('a', 1.5));
  t->insert(std::make_tuple('b', 1.5));
  t->insert(std::make_tuple('c', 1.5));
  t->insert(std::make_tuple('c', 2.5));
  t->insert(std::make_tuple('b', 2.5));
  t->insert(std::make_tuple('c', 5.0));
  std::array<std::string, 3> scratch_column_names = {"x", "count", "avg"};
  auto s = std::make_shared<Scratch<char, size_t, double>>("s", scratch_column_names);
  expected = {std::make_tuple('a', 1, 1.5), std::make_tuple('b', 2, 2.0), std::make_tuple('c', 3, 3.0)};

  auto it = rop::make_iterable(t);
  it | rop::group_by<Keys<0>, rop::agg::Count<1>, rop::agg::Avg<1>>({"count", "avg"}) | rop::insert(s);

  it->push(nullptr, -1, REGULAR);
  s->merge();
  EXPECT_THAT(s->get(),
              testing::UnorderedElementsAreArray(expected));
}

TEST(Join, ResetGroupBy) {
  std::array<std::string, 2> column_names = {"x", "y"};
  auto t = std::make_shared<Table<Keys<0,1>, char, double>>("t", column_names);
  std::set<std::tuple<char, size_t, double>> expected;
  t->insert(std::make_tuple('a', 1.5));
  t->insert(std::make_tuple('b', 1.5));
  t->insert(std::make_tuple('c', 1.5));
  t->insert(std::make_tuple('c', 2.5));
  t->insert(std::make_tuple('b', 2.5));
  t->insert(std::make_tuple('c', 5.0));
  std::array<std::string, 3> scratch_column_names = {"x", "count", "avg"};
  auto s = std::make_shared<Scratch<char, size_t, double>>("s", scratch_column_names);
  expected = {std::make_tuple('a', 1, 1.5), std::make_tuple('b', 2, 2.0), std::make_tuple('c', 3, 3.0)};

  auto it = rop::make_iterable(t);
  it | rop::group_by<Keys<0>, rop::agg::Count<1>, rop::agg::Avg<1>>({"count", "avg"}) | rop::insert(s);

  it->push(nullptr, -1, REGULAR);
  s->merge();
  EXPECT_THAT(s->get(),
              testing::UnorderedElementsAreArray(expected));
  s->tick();
  it->push(nullptr, -1, REGULAR);
  s->merge();
  EXPECT_THAT(s->get(),
              testing::UnorderedElementsAreArray(expected));
}

}  // namespace relational

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}