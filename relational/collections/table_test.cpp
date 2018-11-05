#include "collections/table.hpp"

#include <cstddef>

#include <tuple>
#include <utility>

#include "common/hash_util.hpp"
#include "common/keys.hpp"

#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace relational {

TEST(Table, TableStartsEmpty) {
  Table<Keys<0>, char, char> t("t", {{"x", "y"}});
  std::unordered_map<std::tuple<char>, std::tuple<char, char>,
                     Hash<std::tuple<char>>>
      expected;
  EXPECT_EQ(t.get(), expected);
}

TEST(Table, Insert) {
  Table<Keys<0>, char, char> t("t", {{"x", "y"}});
  std::unordered_map<std::tuple<char>, std::tuple<char, char>,
                     Hash<std::tuple<char>>>
      expected;

  t.insert(std::make_tuple('a', 'a'));
  expected = {{std::make_tuple('a'), std::make_tuple('a', 'a')}};
  EXPECT_EQ(t.get(), expected);

  t.insert(std::make_tuple('b', 'b'));
  expected = {{std::make_tuple('a'), std::make_tuple('a', 'a')},
              {std::make_tuple('b'), std::make_tuple('b', 'b')}};
  EXPECT_EQ(t.get(), expected);

  t.insert(std::make_tuple('a', 'c'));
  expected = {{std::make_tuple('a'), std::make_tuple('a', 'c')},
              {std::make_tuple('b'), std::make_tuple('b', 'b')}};
  EXPECT_EQ(t.get(), expected);
}

TEST(Table, TickDoesntClearTable) {
  Table<Keys<0>, char, char> t("t", {{"x", "y"}});
  std::unordered_map<std::tuple<char>, std::tuple<char, char>,
                     Hash<std::tuple<char>>>
      expected;

  t.insert(std::make_tuple('a', 'a'));
  expected = {{std::make_tuple('a'), std::make_tuple('a', 'a')}};
  EXPECT_EQ(t.get(), expected);
  t.tick();
  EXPECT_EQ(t.get(), expected);
}

}  // namespace relational

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}