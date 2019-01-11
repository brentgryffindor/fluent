#include "relop/project.hpp"
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

TEST(Project, SimpleCompileCheck) {
  std::array<std::string, 2> column_names = {"x", "y"};
  auto t = std::make_shared<Table<Keys<0>, int, int>>("t", column_names);
  auto it = rop::make_iterable(t);
  auto pj = it | rop::project<0>();
  using removed_ref = std::remove_reference<decltype(*pj)>::type;
  using actual = removed_ref::column_types;
  using expected = TypeList<int>;
  static_assert(StaticAssert<std::is_same<actual, expected>>::value, "");
}

TEST(Project, NonEmptyTable) {
  std::array<std::string, 2> table_column_names = {"x", "y"};
  auto t = std::make_shared<Table<Keys<0>, int, int>>("t", table_column_names);
  std::set<std::tuple<int>> expected;
  t->insert(std::make_tuple(1, 2));
  t->insert(std::make_tuple(2, 3));
  t->insert(std::make_tuple(3, 4));
  std::array<std::string, 1> scratch_column_names = {"x"};
  auto s = std::make_shared<Scratch<int>>("s", scratch_column_names);
  expected = {std::make_tuple(1), std::make_tuple(2), std::make_tuple(3)};

  auto it = rop::make_iterable(t);
  it | rop::project<0>() | rop::insert(s);
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