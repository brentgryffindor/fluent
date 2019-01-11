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

TEST(Iterable, SimpleCompileCheck) {
  std::array<std::string, 1> column_names = {"x"};
  auto t = std::make_shared<Table<Keys<0>, int>>("t", column_names);
  auto collection = rop::make_iterable(t);
  using removed_ref = std::remove_reference<decltype(*collection)>::type;
  using actual = removed_ref::column_types;
  using expected = TypeList<int>;
  static_assert(StaticAssert<std::is_same<actual, expected>>::value, "");
}

TEST(Iterable, NonEmptyTable) {
  std::array<std::string, 1> column_names = {"x"};
  auto t = std::make_shared<Table<Keys<0>, int>>("t", column_names);
  std::set<std::tuple<int>> expected;
  t->insert(std::make_tuple(1));
  t->insert(std::make_tuple(2));
  t->insert(std::make_tuple(3));
  auto s = std::make_shared<Scratch<int>>("s", column_names);
  expected = {std::make_tuple(1), std::make_tuple(2), std::make_tuple(3)};

  auto it = rop::make_iterable(t);
  it | rop::insert(s);
  it->push(nullptr, -1, REGULAR);
  s->merge();
  EXPECT_THAT(s->get(),
              testing::UnorderedElementsAreArray(expected));
}

TEST(Iterable, ResetIterator) {
  std::array<std::string, 1> column_names = {"x"};
  auto t = std::make_shared<Table<Keys<0>, int>>("t", column_names);
  std::set<std::tuple<int>> expected;
  t->insert(std::make_tuple(1));
  t->insert(std::make_tuple(2));
  t->insert(std::make_tuple(3));
  auto s = std::make_shared<Scratch<int>>("s", column_names);
  expected = {std::make_tuple(1), std::make_tuple(2), std::make_tuple(3)};

  auto it = rop::make_iterable(t);
  it | rop::insert(s);
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