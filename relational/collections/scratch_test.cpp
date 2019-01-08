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
  std::set<std::tuple<char, char>> expected;
  EXPECT_EQ(s.get(), expected);
  EXPECT_EQ(s.get_delta(), expected);
}

TEST(Scratch, Merge) {
  Scratch<char, char> s("t", {{"x", "y"}});
  std::set<std::tuple<char, char>> expected;

  s.buffer_insertion(std::make_tuple('a', 'a'));
  s.merge();
  expected = {std::make_tuple('a', 'a')};
  EXPECT_EQ(s.get(), expected);
  EXPECT_EQ(s.get_delta(), expected);

  s.buffer_insertion(std::make_tuple('b', 'b'));
  s.merge();
  expected = {std::make_tuple('a', 'a'), std::make_tuple('b', 'b')};
  EXPECT_EQ(s.get(), expected);
  EXPECT_EQ(s.get_delta(), expected);
}

TEST(Scratch, TickClearScratch) {
  Scratch<char, char> s("t", {{"x", "y"}});
  std::set<std::tuple<char, char>> expected;

  s.buffer_insertion(std::make_tuple('a', 'a'));
  s.merge();
  expected = {std::make_tuple('a', 'a')};
  EXPECT_EQ(s.get(), expected);
  EXPECT_EQ(s.get_delta(), expected);

  s.tick();
  s.clear_delta();
  expected.clear();
  EXPECT_EQ(s.get(), expected);
  EXPECT_EQ(s.get_delta(), expected);
}

}  // namespace relational

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}