#ifndef COLLECTIONS_COLLECTION_HPP_
#define COLLECTIONS_COLLECTION_HPP_

namespace relational {

class Collection {
 public:
  // see
  // https://stackoverflow.com/questions/461203/when-to-use-virtual-destructors
  // on why we need virtual destructor
  virtual ~Collection() {}
};

}  // namespace relational

#endif  // COLLECTIONS_COLLECTION_HPP_