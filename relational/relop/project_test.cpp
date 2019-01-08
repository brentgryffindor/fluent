#include "relop/project.hpp"
#include "relop/collection.hpp"

#include <type_traits>

#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "collections/table.hpp"
#include "common/keys.hpp"
#include "common/macros.hpp"

namespace relational {

TEST(Project, SimpleCompileCheck) {
  std::array<std::string, 2> column_names = {"x", "y"};
  auto t = std::make_shared<Table<Keys<0>, int, int>>("t", column_names);
  // rop::Collection<Table<Keys<0>, int>> collection = rop::make_collection(&t);
  auto collection = rop::make_collection(t);
  auto pj = collection | rop::project<0>();
  using removed_ref = std::remove_reference<decltype(*pj)>::type;
  using actual = removed_ref::column_types;
  using expected = TypeList<int>;
  static_assert(StaticAssert<std::is_same<actual, expected>>::value, "");
}

TEST(Project, NonEmptyTable) {
  std::array<std::string, 2> column_names = {"x", "y"};
  auto t = std::make_shared<Table<Keys<0>, int, int>>("t", column_names);
  std::vector<std::tuple<int>> expected;
  t->insert(std::make_tuple(1, 2));
  t->insert(std::make_tuple(2, 3));
  t->insert(std::make_tuple(3, 4));
  expected = {std::make_tuple(1), std::make_tuple(2), std::make_tuple(3)};

  auto collection = rop::make_collection(t);
  auto pj = collection | rop::project<0>();

  collection->push(nullptr, -1, -1);

  // EXPECT_THAT(collection->execute(),
  //            testing::UnorderedElementsAreArray(expected));
}

}  // namespace relational

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}