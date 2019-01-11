#include "relop/join.hpp"
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

TEST(Join, SimpleCompileCheck) {
  std::array<std::string, 2> column_names_1 = {"x", "y"};
  std::array<std::string, 2> column_names_2 = {"y", "z"};
  auto t1 = std::make_shared<Table<Keys<0>, int, char>>("t1", column_names_1);
  auto t2 = std::make_shared<Table<Keys<0>, char, int>>("t2", column_names_2);
  auto it1 = rop::make_iterable(t1);
  auto it2 = rop::make_iterable(t2);
  auto j = rop::make_join<Keys<1>, Keys<0>>(it1, it2);
  using removed_ref = std::remove_reference<decltype(*j)>::type;
  using actual = removed_ref::column_types;
  using expected = TypeList<int, char, char, int>;
  static_assert(StaticAssert<std::is_same<actual, expected>>::value, "");
}

TEST(Join, NonEmptyTable) {
  std::array<std::string, 2> column_names_1 = {"x", "y"};
  std::array<std::string, 2> column_names_2 = {"y", "z"};
  auto t1 = std::make_shared<Table<Keys<0,1>, int, char>>("t1", column_names_1);
  auto t2 = std::make_shared<Table<Keys<0,1>, char, int>>("t2", column_names_2);
  std::set<std::tuple<int, char, char, int>> expected;
  t1->insert(std::make_tuple(1, 'a'));
  t1->insert(std::make_tuple(2, 'b'));
  t2->insert(std::make_tuple('a', 3));
  t2->insert(std::make_tuple('b', 4));
  t2->insert(std::make_tuple('b', 5));
  t2->insert(std::make_tuple('c', 6));
  std::array<std::string, 4> scratch_column_names = {"x", "y", "y", "z"};
  auto s = std::make_shared<Scratch<int, char, char, int>>("s", scratch_column_names);
  expected = {std::make_tuple(1, 'a', 'a', 3), std::make_tuple(2, 'b', 'b', 4), std::make_tuple(2, 'b', 'b', 5)};

  auto it1 = rop::make_iterable(t1);
  auto it2 = rop::make_iterable(t2);
  rop::make_join<Keys<1>, Keys<0>>(it1, it2) | rop::insert(s);

  it1->push(nullptr, -1, REGULAR);
  it2->push(nullptr, -1, REGULAR);
  s->merge();
  EXPECT_THAT(s->get(),
              testing::UnorderedElementsAreArray(expected));
}

TEST(Join, ResetJoin) {
  std::array<std::string, 2> column_names_1 = {"x", "y"};
  std::array<std::string, 2> column_names_2 = {"y", "z"};
  auto t1 = std::make_shared<Table<Keys<0,1>, int, char>>("t1", column_names_1);
  auto t2 = std::make_shared<Table<Keys<0,1>, char, int>>("t2", column_names_2);
  std::set<std::tuple<int, char, char, int>> expected;
  t1->insert(std::make_tuple(1, 'a'));
  t1->insert(std::make_tuple(2, 'b'));
  t2->insert(std::make_tuple('a', 3));
  t2->insert(std::make_tuple('b', 4));
  t2->insert(std::make_tuple('b', 5));
  t2->insert(std::make_tuple('c', 6));
  std::array<std::string, 4> scratch_column_names = {"x", "y", "y", "z"};
  auto s = std::make_shared<Scratch<int, char, char, int>>("s", scratch_column_names);
  expected = {std::make_tuple(1, 'a', 'a', 3), std::make_tuple(2, 'b', 'b', 4), std::make_tuple(2, 'b', 'b', 5)};

  auto it1 = rop::make_iterable(t1);
  auto it2 = rop::make_iterable(t2);
  rop::make_join<Keys<1>, Keys<0>>(it1, it2) | rop::insert(s);

  it1->push(nullptr, -1, REGULAR);
  it2->push(nullptr, -1, REGULAR);
  s->merge();
  EXPECT_THAT(s->get(),
              testing::UnorderedElementsAreArray(expected));
  s->tick();
  it2->push(nullptr, -1, REGULAR);
  it1->push(nullptr, -1, REGULAR);
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