#include "collections/collection_util.hpp"

#include <string>

#include "glog/logging.h"

namespace relational {

std::string CollectionTypeToString(CollectionType type) {
  switch (type) {
    case CollectionType::TABLE:
      return "Table";
    case CollectionType::SCRATCH: 
      return "Scratch";
    case CollectionType::INPUTCHANNEL:
      return "InputChannel";
    case CollectionType::OUTPUTCHANNEL:
      return "OutputChannel";
    /*case CollectionType::STDIN:
      return "Stdin";
    case CollectionType::STDOUT:
      return "Stdout";*/
    case CollectionType::PERIODIC:
      return "Periodic";
    default: {
      CHECK(false) << "Unreachable code.";
      return "";
    }
  }
}

}  // namespace relational