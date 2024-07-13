#ifndef DB_BTREE_H
#define DB_BTREE_H

#include "db/fixed_datapage.h"
#include "fc/btree.h"
#include "fc/mmfile_nix.h"  // WindowsOS not supported
#include "fc/disk_fixed_alloc.h"

template <bool AllowDup,
          frozenca::attr_t Fanout,
          size_t PAGE_SIZE,
          size_t RECORD_SIZE,
          size_t KEY_SIZE>  // records and keys in data pages are stored as bytes (unsigned char), no additional type needed
class FixedDBBTree {
  using Page = FixedRecordDataPage<PAGE_SIZE, RECORD_SIZE, KEY_SIZE>;
  using PageIterator = typename Page::Iterator;
  using typename Page::Key;
  using typename Page::KeyOrRecord;
  using typename Page::Record;
  using BTree = std::conditional<AllowDup, frozenca::BTreeMultiSet<Key, Fanout>, frozenca::BTreeSet<Key, Fanout>>;
  // using BTree = frozenca::BTreeMultiSet<std::array<unsigned char, KEY_SIZE>, Fanout>;
  using BTreeIter = typename BTree::iterator_type;
  using BTreeNode = typename BTree::Node;

  using Alloc = typename frozenca::AllocatorFixed<Page>;

  struct Deleter {  // from fc/btree.h
    [[no_unique_address]] Alloc alloc_;
    Deleter(const Alloc& alloc) : alloc_{alloc} {}

    template <typename T>
    void operator()(T* page) noexcept {
      alloc_.deallocate(page, 1);
    }
  };

  using MMapFile = frozenca::MemoryMappedFileImpl;
  using MemRes = frozenca::MemoryResourceFixed<Page>;
  using PagePtr = std::unique_ptr<Page, Deleter>;

 public:
  const std::filesystem::path path;  // Only support a single file for leaves
  size_t size;                       // only counting valid records in leaves
  MMapFile mmap_file;
  MemRes mem_res;
  const uint64_t new_mmap_file_size;
  Alloc alloc_;

  FixedDBBTree(std::filesystem::path path)
      : path(std::move(path)),
        mmap_file(this->path),
        new_mmap_file_size(std::max((1UL << 20UL), std::filesystem::file_size(path) / 10)),
        mem_res([this]() -> MemRes {
          auto new_mmap_file = MMapFile(std::filesystem::path("/temp/b-tree"), new_mmap_file_size, true);
          return MemRes(new_mmap_file);
        }),
        alloc_(Alloc(&mem_res)) {}

  static Key get_key(const Record& record) {
    Key key;
    std::copy(record.begin(), record.begin() + KEY_SIZE, key.begin());
    return key;
  }

  class Iterator : public std::iterator<std::bidirectional_iterator_tag, Record> {
    FixedDBBTree* tree_;
    Page* current_page_;
    PageIterator page_iter;
    // size_t index_; // Only counting valid // Too hard to maintain

    void advance_page();  // TODO
    void retreat_page();  // TODO

   public:
    Iterator(FixedDBBTree* tree, PageIterator page_iter) : tree_(tree), page_iter(page_iter), current_page_(page_iter.get_page()) {}

    Iterator& operator++() {
      if (page_iter++ == current_page_->end()) advance_page();
      //   ++index_;
      return *this;
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
  PagePtr make_page() {
    auto buffer = alloc_.allocate(1);
    Page* page = new (buffer) Page();
    return PagePtr(page, Deleter(alloc_));
  }

 public:
  Iterator insert(const Record& record) {
    Key key = get_key(record);
    BTreeIter btree_it = branches.find_upper_bound(key);
    auto node = btree_it.node_;
    if (!node->is_leaf()) {
      node = branches.rightmost_leaf(node.children_[btree_it.index_]);
    }
    Page leaf = node.leaves.at(btree_it.index_ + 1);  // SCRUTINY required
    std::optional<Iterator> ret = leaf.insert(record);
    if (ret.has_value()) return Iterator(this, ret.value());
    // Split page
    PagePtr page_ptr = make_page();
    Key mid_key = leaf.split_with(*page_ptr);
    BTreeIter inserted_it;

    if constexpr (AllowDup) {
      inserted_it = branches.insert(mid_key);
    } else {
      auto ret = branches.insert(mid_key);
      if (!ret.second) throw std::runtime_error("Inserting duplicates not allowed.");
      inserted_it = ret.first;
    }
    auto inserted_node = *inserted_it;
  }

 private:
  //   std::unique_ptr<BTree> branches;
  BTree branches;
};

#endif