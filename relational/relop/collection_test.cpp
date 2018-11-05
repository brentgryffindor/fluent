#include "relop/collection.hpp"

#include <type_traits>

#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "collections/table.hpp"
#include "common/keys.hpp"
#include "common/macros.hpp"

namespace relational {

TEST(Collection, SimpleCompileCheck) {
  Table<Keys<0>, int> t("t", {{"x"}});
  rop::Collection<Table<Keys<0>, int>> collection = rop::make_collection(&t);
  UNUSED(collection);

  using actual = decltype(collection)::column_types;
  using expected = TypeList<int>;
  static_assert(StaticAssert<std::is_same<actual, expected>>::value, "");
}

TEST(Collection, NonEmptyTable) {
  Table<Keys<0>, int> t("t", {{"x"}});
  std::vector<std::tuple<int>> expected;
  t.insert(std::make_tuple(1));
  t.insert(std::make_tuple(2));
  t.insert(std::make_tuple(3));
  expected = {std::make_tuple(1), std::make_tuple(2), std::make_tuple(3)};

  rop::Collection<Table<Keys<0>, int>> collection = rop::make_collection(&t);
  EXPECT_THAT(collection.execute(),
              testing::UnorderedElementsAreArray(expected));
}

TEST(Collection, NonEmptyScratch) {
  Scratch<int> s("s", {{"x"}});
  std::vector<std::tuple<int>> expected;
  s.insert(std::make_tuple(1));
  s.insert(std::make_tuple(2));
  s.insert(std::make_tuple(3));
  expected = {std::make_tuple(1), std::make_tuple(2), std::make_tuple(3)};

  rop::Collection<Scratch<int>> collection = rop::make_collection(&s);
  EXPECT_THAT(collection.execute(),
              testing::UnorderedElementsAreArray(expected));
}

}  // namespace relational

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}