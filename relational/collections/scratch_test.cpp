#include "collections/scratch.hpp"

#include <cstddef>

#include <tuple>
#include <utility>

#include "common/hash_util.hpp"

#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace relational {

TEST(Scratch, ScratchStartsEmpty) {
  Scratch<char, char> s("s", {{"x", "y"}});
  std::vector<std::tuple<char, char>> expected;
  EXPECT_EQ(s.get(), expected);
}

TEST(Scratch, Insert) {
  Scratch<char, char> s("s", {{"x", "y"}});
  std::vector<std::tuple<char, char>> expected;

  s.insert(std::make_tuple('a', 'a'));
  expected = {std::make_tuple('a', 'a')};
  EXPECT_EQ(s.get(), expected);

  s.insert(std::make_tuple('b', 'b'));
  expected = {std::make_tuple('a', 'a'), std::make_tuple('b', 'b')};
  EXPECT_EQ(s.get(), expected);

  s.insert(std::make_tuple('a', 'c'));
  expected = {std::make_tuple('a', 'a'), std::make_tuple('b', 'b'),
              std::make_tuple('a', 'c')};
  EXPECT_EQ(s.get(), expected);
}

TEST(Scratch, TickClearScratch) {
  Scratch<char, char> s("s", {{"x", "y"}});
  std::vector<std::tuple<char, char>> expected;

  s.insert(std::make_tuple('a', 'a'));
  expected = {std::make_tuple('a', 'a')};
  EXPECT_EQ(s.get(), expected);
  s.tick();
  expected.clear();
  EXPECT_EQ(s.get(), expected);
}

}  // namespace relational

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}