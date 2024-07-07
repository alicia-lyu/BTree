#ifndef DATAPAGE_H
#define DATAPAGE_H

#include <stddef.h>
#include <array>
#include <vector>
#include <bitset>
#include <optional>
#include <cstring>
#include <iterator>
#include <cassert>

#include "fc/mmfile_nix.h"

// Only having separator keys in the branch nodes entails
// Using a BTreeSet or MultiSet as the tree's container, which should stay in memory during the lifetime of BTree.
// MultiSet is only needed for skewed key, which could result in the same key overflow one data page.
//
// 2. How to link leaf nodes to the tree?
// RULED OUT 1:
// Have leaf nodes in the tree store keys only, just as the branch nodes.
// Complication is we should not expect memory space for the leaf nodes---they are just to push balance in the branches.
// FINAL:
// Manage leaf nodes (pages separately). Add a leaves_ field to BTree::Node.
// 1. Search: Instead of searching for a key in the leaf nodes, search for its lower bound which exists in the pseudo-leaf nodes.
// The pseudo leaf nodes should have a pointer to the actual leaf node (I/O needed), where the key is searched again.
// 2. Insertion: If a leaf node has space for the insertion, no action needed on the tree.
// Otherwise, split the node and insert the lower bound value of the right leaf into the tree.
// Adjust the leaves_ data member.
// 3. Deletion: No merge attempted. Just delete it from leaf node. No action needed on the tree.
//
// Why we need a new class instead of just extending Node?
// Node is not a standalone class but part of the BTree.
// Insertions, search, deletions, and IO are all managed by the BTree.

template <size_t PAGE_SIZE>
class DataPage {
 protected:
  using MMapFile = frozenca::MemoryMappedFileImpl;
  uintmax_t next_page_offset = 0;

  DataPage(MMapFile& mmap_file, uintmax_t file_offset) : next_page_offset(std::bit_cast<uintmax_t>(get_mmap_ptr(mmap_file, file_offset))) {}

  virtual ~DataPage() = default;

  void* get_mmap_ptr(MMapFile& mmap_file, uintmax_t file_offset, std::size_t offset = 0) {
    return reinterpret_cast<void*>(static_cast<unsigned char*>(mmap_file.get_page_ptr(file_offset, PAGE_SIZE)) + offset + sizeof(uintmax_t));
  }
};

template <size_t PAGE_SIZE, size_t RECORD_SIZE, size_t KEY_SIZE>
class FixedRecordDataPage : public DataPage<PAGE_SIZE> {
 public:
  static constexpr size_t RECORD_COUNT = PAGE_SIZE / RECORD_SIZE;
  static constexpr size_t DATA_SIZE = PAGE_SIZE - RECORD_COUNT / 8 - sizeof(uintmax_t);
  using Record = std::array<unsigned char, RECORD_SIZE>;
  using Key = std::array<unsigned char, KEY_SIZE>;
  using KeyOrRecord = std::variant<Record, Key>;
  using vec_iter_type = std::vector<Record>::iterator;
  using typename DataPage<PAGE_SIZE>::MMapFile;

 protected:
  std::bitset<RECORD_COUNT> bitmap_;  // 0 for free, 1 for occupied
  std::vector<Record> records_;

 public:
  // New constructor to use memory-mapped file directly
  FixedRecordDataPage(MMapFile& mmap_file, uintmax_t file_offset)
      : bitmap_(*new(get_mmap_ptr(mmap_file, file_offset)) std::bitset<RECORD_COUNT>),
        records_(*new(get_mmap_ptr(mmap_file, file_offset, sizeof(std::bitset<RECORD_COUNT>))) std::vector<Record>(RECORD_COUNT)) {
    assert(file_offset % alignof(FixedRecordDataPage));
  }

  FixedRecordDataPage() { records_.reserve(RECORD_COUNT); }

  ~FixedRecordDataPage() {
    bitmap_.~bitset();
    records_.~vector();
    // un-schematize but deallocation is deferred to mmap
  }

  size_t size() { return bitmap_.count(); }

 protected:
  Record& get_record(size_t index) { return records_.at(index); }

  Key get_key(size_t index) {
    auto record = get_record(index);
    Key key;
    std::copy(record.begin(), record.begin() + KEY_SIZE, key.begin());
    return key;
  }

  size_t get_occupancy_in_range(size_t left, size_t right) {
    size_t occupied = 0;
    for (size_t i = left; i < right; ++i) {
      if (bitmap_[i]) {
        ++occupied;
      }
    }
    return occupied;
  }

  bool get_bit(vec_iter_type rec_it) { return bitmap_[std::distance(records_.begin(), rec_it)]; }

  void flip_bit(vec_iter_type rec_it) { bitmap_.flip(std::distance(records_.begin(), rec_it)); }

  void set_bit(vec_iter_type rec_it, bool value = true) { bitmap_.set(std::distance(records_.begin(), rec_it), value); }

 public:
  class Iterator {
    FixedRecordDataPage* page_;
    vec_iter_type vec_iter;

    // An iter exposed to external world always valid unless reaches end
    void advance_to_valid() {
      while (vec_iter < page_->records_.end() && !page_->bitmap_[index()]) {
        ++vec_iter;
      }
    }

    void retreat_to_valid() {
      while (vec_iter >= page_->records_.begin() && !page_->bitmap_[index()]) {
        --vec_iter;
      }
    }

    size_t index() { return std::distance(page_->records_.begin(), vec_iter); }

   public:
    using iterator_category = std::bidirectional_iterator_tag;
    using value_type = Record;
    using difference_type = vec_iter_type::difference_type;
    using pointer = Record*;
    using reference = Record&;

    Iterator(FixedRecordDataPage* page, size_t index) : page_(page), vec_iter(page->records_.begin() + index) { advance_to_valid(); }

    Iterator(FixedRecordDataPage* page, vec_iter_type vec_iter) : page_(page), vec_iter(vec_iter) { advance_to_valid(); }

    Iterator& operator++() {
      ++vec_iter;
      advance_to_valid();
      return *this;
    }

    Iterator& operator--() {
      --vec_iter;
      retreat_to_valid();
      return *this;
    }

    bool operator==(const Iterator& other) const { return page_ == other.page_ && vec_iter == other.vec_iter; }

    bool operator!=(const Iterator& other) const { return !(*this == other); }

    Record& operator*() { return *vec_iter; }

    Record* operator->() { return &(*vec_iter); }

    vec_iter_type base() { return vec_iter; }

    FixedRecordDataPage* get_page() { return page_; }
  };

  Iterator begin() { return Iterator(this, records_.begin()); }

  Iterator end() { return Iterator(this, records_.end()); }

 protected:
  bool get_bit(Iterator it) { return get_bit(it.base()); }

  void flip_bit(Iterator it) { flip_bit(it.base()); }

  void set_bit(Iterator it) { set_bit(it.base()); }

 public:
  bool validate(Iterator it) { return get_bit(it) == true; }

  // LATER: Migrate to SIMD (How to handle bitmap jumps?)
  Iterator search_lb(const KeyOrRecord& key_or_record) {
    if (bitmap_.count() == 0) return end();
    size_t left = find_first_occupied(0);  // inclusive
    if (std::memcmp(key_or_record.data(), records_.at(left).data(), key_or_record.size()) == 0)
      return Iterator(this, left);  // Early return helps guarantee left is smaller

    size_t right = find_first_occupied(RECORD_COUNT) + 1;  // exclusive
    // Note that left is guaranteed to be occupied, while right is not.
    while (get_occupancy_in_range(left, right) <= 1)  // lock in to one entry
    {
      size_t mid = find_first_occupied(left + (right - left) / 2, left, right);
      auto record_mid = get_record(mid);  // Get the whole record but only comparing the first KEY_SIZE bytes.
      int ret = std::memcmp(key_or_record.data(), record_mid.data(), key_or_record.size());
      if (ret < 0)  // lb is less than mid
        right = mid;
      else if (ret == 0)  // lb is less than or equal to mid
      {
        right = mid + 1;
        // Only having `right = mid + 1` may enter into infinite loop, when right keeps being reset to left + 2
        if (get_occupancy_in_range(left, right) == 2) {  // 2: left and mid
          // left is guaranteed to be smaller (comes from the last arm)
          left = mid;
        }
      } else if (ret > 0)  // lb is greater than or equal to mid (although mid is smaller, but mid+1 can be bigger)
        left = mid;
    }
    // If there is any element equal to key_or_record, left is equal.
    return Iterator(this, left);
  }

  Iterator search_ub(const KeyOrRecord& key_or_record) {
    if (bitmap_.count() == 0) return end();
    size_t left = find_first_occupied(0);                  // inclusive
    size_t right = find_first_occupied(RECORD_COUNT) + 1;  // exclusive
    while (get_occupancy_in_range(left, right) <= 1)       // lock in to one entry
    {
      size_t mid = find_first_occupied(left + (right - left) / 2, left, right);
      auto record_mid = get_record(mid);  // Get the whole record but only comparing the first KEY_SIZE bytes.
      int ret = std::memcmp(key_or_record.data(), record_mid.data(), key_or_record.size());
      if (ret < 0)  // ub is less than or equal to mid
        right = mid + 1;
      else if (ret >= 0)  // ub is greater than mid
        left = mid + 1;
    }
    // ub is guaranteed to be not equal, regardless of whether a match exists.
    return Iterator(this, left);  // Will advance to valid---the one entry between [left, right]
  }

  std::optional<Iterator> search(const KeyOrRecord& key_or_record) {
    auto lb = search_lb(key_or_record);
    if (lb == end()) return std::nullopt;
    auto record_lb = *lb;
    if (std::memcmp(key_or_record.data(), record_lb.data(), key_or_record.size()) == 0)
      return lb;
    else
      return std::nullopt;
  }

  size_t find_first_occupied(size_t index, size_t lower_bound = 0, size_t upper_bound = RECORD_COUNT)
  // scanning is memory-efficient, as bitmap is expected to fit into cache line
  {
    size_t to_left = index;
    size_t to_right = index;
    while (to_left >= lower_bound || to_right < upper_bound) {
      if (to_left >= lower_bound && bitmap_[to_left] == 1) return to_left;
      if (to_right < upper_bound && bitmap_[to_right] == 1) return to_right;
      --to_left;
      ++to_right;
    }
    return RECORD_COUNT;
  }

  //
  std::pair<Iterator, bool> insert(const Record& record, bool allow_dup = true) {
    if (bitmap_.count() == RECORD_COUNT) return {Iterator(this, RECORD_COUNT), false};
    // page is full, split to be managed by page owner
    Iterator ub = search_ub(record);
    if (!allow_dup) {
      for (Iterator it = ub - 1; it < end(); --it) {
        if (get_bit(it) == true) {
          if (std::equal(record.begin(), record.end(), (*it).begin()))
            return {it++, false};  // It should have been inserted here, but cannot because of duplicate ahead of it.
          break;
        }
      }
    }

    if (get_bit(ub) == 0) {
      (*ub).swap(record);
      flip_bit(ub);
      return {ub, true};
    }
    // Moving elements is inevitable
    records_.insert(ub.base(), record);  // temporarily grows larger than DATA_SIZE
    // Erasing from the tail is more efficient
    bool before_ub = false;
    auto records__it = records_.end();
    while (records__it >= records_.begin()) {
      if (!get_bit(records__it)) {
        records_.erase(records__it);
        if (records__it < ub.base()) before_ub = true;
        break;
      }
      --records__it;
    }
    // Bitmap knows nothing about insertion and erasion yet.
    if (before_ub) {
      // all bits between (records__it, ub-1] need to shift 1 pos forward, overwriting bitmap[records__it]
      while (records__it < (ub--).base()) {
        set_bit(records__it, get_bit(records__it++));
      }
      // leaving an empty seat at ub-1, which should be set to true
      set_bit(ub, true);
    } else {
      // all bit between [ub, records__it) need to shift 1 pos backward, overwriting bitmap[records__it]
      while (records__it > ub.base()) {
        set_bit(records__it, get_bit(records__it--));
      }
      // leaving an empty seat at ub, which should be set to true
      set_bit(ub, true);
    }
    return {ub, true};
  }

  // No element shifting, which only happens at insertions
  std::optional<Iterator> erase(size_t index) {
    if (bitmap_[index]) {
      // will be cleaned when elements shift during insertions
      bitmap_[index] = false;
      return Iterator(this, index);
    }
    return std::nullopt;
  }

  std::optional<Iterator> erase(Iterator it) { return erase(it.index()); }

  std::optional<Iterator> erase(const Record& record) {
    auto found = search(record);
    if (!found.has_value()) return std::nullopt;
    return erase(found.value());
  }

  // Returns the key to copy up and insert into the tree
  // Operate on the assumption that a datapage is (almost) full when it must split, DERERRED to caller to ensure that
  // Otherwise, unexpected behavior when the bitmap is skewed.
  Key split_with(FixedRecordDataPage<PAGE_SIZE, RECORD_SIZE, KEY_SIZE>& right_sibling) {
    assert(right_sibling.bitmap_.none());
    size_t mid = find_first_occupied(RECORD_COUNT / 2);
    size_t right_index = 0;
    for (size_t i = mid; i < RECORD_COUNT; ++i) {
      if (bitmap_[i] == 1) {
        right_sibling.records_.at(right_index).swap(records_.at(i));
        bitmap_[i] = 0;
        right_sibling.bitmap_[right_index] = 1;
        ++right_index;
      }
    }
    return get_key(mid);
  }
};

// Fixed-length key, variable-length value
// template <size_t PAGE_SIZE = 4000, size_t KEY_SIZE>
// class VariableVDataPage : public DataPage<PAGE_SIZE>

#endif