#include "db/datapage.h"
#include "fc/btree.h"

template <bool AllowDup, size_t PAGE_SIZE = 4000, size_t RECORD_SIZE, size_t KEY_SIZE>
class FixedDBBTree {
    using page_type = typename FixedRecordDataPage<PAGE_SIZE, RECORD_SIZE, KEY_SIZE>
    using page_iter_type = typename page_type::Iterator;
    using Record = typename page_type::Record;
    using Key = typename page_type::Key;
    using KeyOrRecord = typename page_type::KeyOrRecord;
 public:
  const std::filesystem::path path;

  FixedDBBTree(std::filesystem::path path) : path(path) {}



 private:
  std::unique_ptr<std::conditional<AllowDup, frozenca::BTreeSet, frozenca::BTreeMultiSet>> tree;
};