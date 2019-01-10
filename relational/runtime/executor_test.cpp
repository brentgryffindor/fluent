#include "runtime/builder.hpp"

#include <type_traits>

#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "zmq.hpp"

#include "collections/collection_util.hpp"
#include "common/keys.hpp"
#include "common/macros.hpp"
#include "relop/all.hpp"

namespace relational {

TEST(Executor, NoCycle) {
  using milliseconds = std::chrono::milliseconds;
  zmq::context_t context(1);
  auto fe = fluent("test", "inproc://1234", &context)
                .scratch<int, int>("s1", {{"a", "b"}})
                .scratch<int, int>("s2", {{"a", "b"}})
                .scratch<int, int>("s3", {{"a", "b"}})
                .scratch<int, int>("s4", {{"a", "b"}})
                .RegisterIterables([](auto& s1, auto& s2, auto& s3, auto& s4) {
                  auto i1 = iterable(s1);
                  auto i2 = iterable(s2);
                  auto i3 = iterable(s3);
                  auto i4 = iterable(s4);
                  return std::make_tuple(i1, i2, i3, i4);
                })
                .DefineComputeGraph([](auto& s1, auto& s2, auto& s3, auto& s4, auto& i1, auto& i2, auto& i3, auto& i4) {
                  i1 | rop::insert(s2);
                  i1 | rop::insert(s3);
                  i1 | rop::insert(s4);
                  i3 | rop::insert(s2);
                  i4 | rop::insert(s2);
                  i4 | rop::insert(s3);
                });
  EXPECT_EQ(false, fe.detect_cycle());
}

TEST(Executor, HasCycle) {
  using milliseconds = std::chrono::milliseconds;
  zmq::context_t context(1);
  auto fe = fluent("test", "inproc://1234", &context)
                .scratch<int, int>("s1", {{"a", "b"}})
                .scratch<int, int>("s2", {{"a", "b"}})
                .scratch<int, int>("s3", {{"a", "b"}})
                .scratch<int, int>("s4", {{"a", "b"}})
                .RegisterIterables([](auto& s1, auto& s2, auto& s3, auto& s4) {
                  auto i1 = iterable(s1);
                  auto i2 = iterable(s2);
                  auto i3 = iterable(s3);
                  auto i4 = iterable(s4);
                  return std::make_tuple(i1, i2, i3, i4);
                })
                .DefineComputeGraph([](auto& s1, auto& s2, auto& s3, auto& s4, auto& i1, auto& i2, auto& i3, auto& i4) {
                  i1 | rop::insert(s2);
                  i2 | rop::insert(s3);
                  i3 | rop::insert(s4);
                  i4 | rop::insert(s1);
                });
  EXPECT_EQ(true, fe.detect_cycle());
}

}  // namespace relational

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
} 