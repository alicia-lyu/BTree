#include "db/datapage.h"
#include "fc/btree.h"

template <bool AllowDup, size_t PAGE_SIZE = 4000, size_t RECORD_SIZE, size_t KEY_SIZE>
class FixedDBBTree {
  using btree_type = typename std::conditional<AllowDup, frozenca::BTreeMultiSet, frozenca::BTreeSet>::type;
  using page_type = FixedRecordDataPage<PAGE_SIZE, RECORD_SIZE, KEY_SIZE>;
  using page_iter_type = typename page_type::Iterator;
  using Record = typename page_type::Record;
  using Key = typename page_type::Key;
  using KeyOrRecord = typename page_type::KeyOrRecord;

 public:
  const std::filesystem::path path;  // Only support a single file for leaves
  size_t size;                       // only counting valid records in leaves

  FixedDBBTree(std::filesystem::path path) : path(std::move(path)) {}

  class Iterator : public std::iterator<std::bidirectional_iterator_tag, Record> {
    FixedDBBTree* tree_;
    page_type* current_page_;
    page_iter_type page_iter;
    // size_t index_; // Only counting valid // Too hard to maintain

    void advance_page();  // TODO
    void retreat_page();  // TODO

   public:
    Iterator(FixedDBBTree* tree, page_iter_type page_iter) : tree_(tree), page_iter(page_iter), current_page_(page_iter.get_page()) {}

    Iterator& operator++() {
      if (page_iter++ == current_page_.end()) advance_page();
      //   ++index_;
      return *this
    }
    Iterator& operator--() {
      if (page_iter-- == current_page_->begin()) retreat_page();
      //   --index_;
      return *this;
    }
    bool operator==(const Iterator& other) const { return page_iter == other.page_iter && tree_ == other.tree_; }

    bool operator!=(const Iterator& other) const { return !(*this == other); }
    Record& operator*() { return *page_iter; }
    Record* operator->() { return &(*page_iter); }
  };

  Iterator begin() {
    auto leftmost_branch = *(branches.begin());
    auto leftmost_page = *(leftmost_branch.leaves_.begin());
    return Iterator(this, leftmost_page.begin());
  }

  Iterator end() {
    auto rightmost_branch = *(branches.end()--);
    auto rightmost_page = *(rightmost_branch.leaves_.end());
    return Iterator(this, rightmost_page.end());
  }

  

 private:
  std::unique_ptr<btree_type> branches;
};