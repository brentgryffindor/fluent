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

TEST(Collection, Simple) {
  using milliseconds = std::chrono::milliseconds;
  zmq::context_t context(1);
  auto fe = fluent("test", "inproc://1234", &context)
                .table<Keys<0>, std::string, int>("t1", {{"Address", "num"}})
                .scratch<std::string, int>("s1", {{"Address", "num"}})
                .ichannel<std::string, int>("ic1", {{"Address", "num"}})
                .ochannel<std::string, int>("oc1", {{"Address", "num"}})
                .periodic("p1", milliseconds(1000))
                .BootstrapTables([](auto& t1) {
                  t1.insert(std::make_tuple("inproc://1", 1));
                  t1.insert(std::make_tuple("inproc://2", 2));
                  t1.insert(std::make_tuple("inproc://3", 3));
                })
                .RegisterIterables([](auto& t1, auto& s1, auto& ic1, auto& p1) {
                  auto i1 = iterable(t1);
                  auto i2 = iterable(s1);
                  auto i3 = iterable(ic1);
                  auto i4 = iterable(p1);
                  return std::make_tuple(i1, i2, i3, i4);
                })
                .DefineComputeGraph([](auto& t1, auto& s1, auto& oc1, auto& i1, auto& i2, auto& i3, auto& i4) {
                  i1 | rop::insert(s1);
                  i2 | rop::insert(t1);
                  i2 | rop::insert(oc1);
                  rop::make_join<Keys<0>, Keys<0>>(i1, i2);
                });
  fe.run();
}

}  // namespace relational

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}