#ifndef DATAPAGE_H
#define DATAPAGE_H

#include <stddef.h>
#include <sys/types.h>
#include <array>
#include <cstddef>
#include <cstdint>
#include <bitset>
#include <optional>
#include <cstring>
#include <iterator>
#include <cassert>
#include <iostream>

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
  uintmax_t next_page_offset_;

  DataPage(MMapFile& mmap_file, uintmax_t file_offset) : next_page_offset_(std::bit_cast<uintmax_t>(get_mmap_ptr(mmap_file, file_offset))) {}

  DataPage() : next_page_offset_(0) {}

  virtual ~DataPage() {}

  void* get_mmap_ptr(MMapFile& mmap_file, uintmax_t file_offset, std::size_t offset = 0) {
    return reinterpret_cast<void*>(static_cast<unsigned char*>(mmap_file.get_page_ptr(file_offset, PAGE_SIZE)) + offset + sizeof(uintmax_t));
  }
};

template <size_t PAGE_SIZE, size_t RECORD_SIZE, size_t KEY_SIZE>
class FixedRecordDataPage : public DataPage<PAGE_SIZE> {
 public:
  static constexpr size_t RECORD_COUNT = (PAGE_SIZE - sizeof(uintmax_t)) / (RECORD_SIZE + 1 / 8);
  static constexpr size_t DATA_SIZE = RECORD_COUNT * RECORD_SIZE;
  using Record = std::array<unsigned char, RECORD_SIZE>;
  using Key = std::array<unsigned char, KEY_SIZE>;
  using RecordData = std::array<unsigned char, DATA_SIZE>;
  using KeyOrRecord = std::variant<Record, Key>;
  using typename DataPage<PAGE_SIZE>::MMapFile;

 protected:
  std::bitset<RECORD_COUNT>* bitmap_;  // 0 for free, 1 for occupied
  RecordData* record_data_;

 public:
  static std::string record_to_string(const Record& record) { return std::string(record.begin(), record.end()); }

  // New constructor to use memory-mapped file directly
  FixedRecordDataPage(MMapFile& mmap_file, uintmax_t file_offset)
      : DataPage<PAGE_SIZE>(mmap_file, file_offset),
        bitmap_(new(this->get_mmap_ptr(mmap_file, file_offset)) std::bitset<RECORD_COUNT>()),
        record_data_(new(this->get_mmap_ptr(mmap_file, file_offset, sizeof(std::bitset<RECORD_COUNT>))) RecordData()) {
    assert(file_offset % PAGE_SIZE == 0);
    std::cout << "FixedRecordDataPage constructed at " << file_offset << ". Page size: " << PAGE_SIZE << ". Record size: " << RECORD_SIZE
              << ". Key size: " << KEY_SIZE << ". Record count: " << RECORD_COUNT << "\n";
  }

  FixedRecordDataPage() : bitmap_(std::make_unique<std::bitset<RECORD_COUNT>>()), record_data_(std::make_unique<RecordData>(RECORD_COUNT)) {}

  ~FixedRecordDataPage() {}

  size_t size() { return bitmap_->count(); }

 protected:
  Record* get_record(size_t index) {
    Record* record_in_place = reinterpret_cast<Record*>(record_data_->data() + RECORD_SIZE * index);
    return record_in_place;
  }

  Record copy_record(size_t index) {
    Record record;
    std::copy(record_data_->data() + RECORD_SIZE * index, record_data_->data() + RECORD_SIZE * index + RECORD_SIZE, record.begin());
    return record;
  }

  unsigned char* get_record_ptr(size_t index) { return record_data_->data() + RECORD_SIZE * index; }

  Key* get_key(size_t index) {
    Key* key_in_place = reinterpret_cast<Key*>(record_data_->data() + RECORD_SIZE * index);
    return key_in_place;
  }

  Key copy_key(size_t index) {
    Key key;
    std::copy(record_data_->data() + RECORD_SIZE * index, record_data_->data() + RECORD_SIZE * index + KEY_SIZE, key.begin());
    return key;
  }

  size_t get_occupancy_in_range(size_t left, size_t right) {
    size_t occupied = 0;
    for (size_t i = left; i < right; ++i) {
      if ((*bitmap_)[i]) {
        ++occupied;
      }
    }
    return occupied;
  }

 public:
  class Iterator {
    FixedRecordDataPage* page_;

   public:
    size_t index_;
    using iterator_category = std::bidirectional_iterator_tag;
    using value_type = Record;
    using difference_type = size_t;
    using pointer = Record*;
    using reference = Record&;

    Iterator(FixedRecordDataPage* page, size_t index) : page_(page), index_(index) {}

      void advance_to_valid() {
      while (index_ < RECORD_COUNT && !(*page_->bitmap_)[index_]) {
        ++index_;
      }
    }

    void retreat_to_valid() {
      while (index_ >= 0 && !(*page_->bitmap_)[index_]) {
        --index_;
      }
    }

    Iterator& operator++() {
      ++index_;
      return *this;
    }

    Iterator operator++(int) {
      Iterator tmp = *this;
      ++(*this);
      return tmp;
    }

    Iterator& operator--() {
      --index_;
      return *this;
    }

    Iterator operator--(int) {
      Iterator tmp = *this;
      --(*this);
      return tmp;
    }

    bool operator==(const Iterator& other) const { return page_ == other.page_ && index_ == other.index_; }

    bool operator!=(const Iterator& other) const { return !(*this == other); }

    Record& operator*() { return *page_->get_record(index_); }

    Record* operator->() { return page_->get_record(index_); }

    bool operator<(const Iterator& other) const { return index_ < other.index_; }
    bool operator<=(const Iterator& other) const { return index_ <= other.index_; }
    bool operator>(const Iterator& other) const { return index_ > other.index_; }
    bool operator>=(const Iterator& other) const { return index_ >= other.index_; }

    difference_type operator-(const Iterator& other) const { return index_ - other.index_; }
    Iterator operator+(difference_type n) const { return Iterator(page_, index_ + n); }
    Iterator operator-(difference_type n) const { return Iterator(page_, index_ - n); }

    FixedRecordDataPage* get_page() { return page_; }
  };

  Iterator begin() { return Iterator(this, 0); }

  Iterator end() { return Iterator(this, RECORD_COUNT); }

 protected:
  bool get_bit(Iterator it) { return bitmap_->test(it.index_); }

  void flip_bit(Iterator it) { bitmap_->flip(it.index_); }

  void set_bit(Iterator it, bool value) { 
    std::cout << "Setting bit at index " << it.index_ << " to " << value << std::endl;
    bitmap_->set(it.index_, value);
  }

  const void* get_data(const KeyOrRecord key_or_record) {
    if (std::holds_alternative<Record>(key_or_record)) {
      const Record& record = std::get<Record>(key_or_record);
      return static_cast<const void*>(record.data());
    } else {  // std::holds_alternative<Key>(key_or_record)
      const Key& key = std::get<Key>(key_or_record);
      return static_cast<const void*>(key.data());
    }
  }

  size_t get_size(const KeyOrRecord key_or_record) {
    if (std::holds_alternative<Record>(key_or_record)) {
      const Record& record = std::get<Record>(key_or_record);
      return record.size();
    } else {  // std::holds_alternative<Key>(key_or_record)
      const Key& key = std::get<Key>(key_or_record);
      return key.size();
    }
  }

 public:
  bool validate(Iterator it) { return get_bit(it) == true; }

  // LATER: Migrate to SIMD (How to handle bitmap jumps?)
  std::optional<Iterator> search_lb(const KeyOrRecord& key_or_record) {
    if (bitmap_->count() == 0) return end();
    size_t left = find_first_occupied(0);  // inclusive
    auto ret = std::memcmp(get_data(key_or_record), get_record(left)->data(), get_size(key_or_record));
    if (ret == 0)
      return Iterator(this, left);  // Early return helps guarantee left < key_or_record
    else if (ret < 0)
      return std::nullopt;  // key_or_record is less than the first element

    size_t right = find_first_occupied(RECORD_COUNT) + 1;  // exclusive
    // Note that left is guaranteed to be occupied, while right is not.
    while (get_occupancy_in_range(left, right) > 1)  // lock in to one entry
    {
      size_t mid = find_first_occupied(left + (right - left) / 2, left, right);
      if (mid == RECORD_COUNT)  // no valid records between [left, right)
        return Iterator(this, left);
      auto record_mid_ptr = get_record_ptr(mid);  // Get the whole record but only comparing the first KEY_SIZE bytes.
      int ret = std::memcmp(get_data(key_or_record), record_mid_ptr, get_size(key_or_record));
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
    if (bitmap_->count() == 0) return begin();
    size_t left = 0;              // inclusive
    size_t right = RECORD_COUNT;  // exclusive
    while (right - left > 1) {    // lock in to one entry
      size_t mid = find_first_occupied(left + (right - left) / 2, left, right);
      if (mid == RECORD_COUNT)  // no valid records between [left, right)
        return Iterator(this, left);

      auto record_mid_ptr = get_record_ptr(mid);  // Get the whole record but only comparing the first KEY_SIZE bytes.
      int ret = std::memcmp(get_data(key_or_record), record_mid_ptr, get_size(key_or_record));

      std::cout << "left: " << left << ", right: " << right << ", mid: " << mid << ", ret: " << ret
                << ", record_mid: " << std::string(record_mid_ptr, record_mid_ptr + 5) << std::endl;

      if (ret < 0) {  // ub is less than or equal to mid
        right = mid + 1;
        std::cout << "Adjusting right to " << right << std::endl;
        // Only having `right = mid + 1` may enter into infinite loop, when right keeps being reset to left + 2
        if (right - left == 2) {
          if (bitmap_->test(left) == true) {  // We must find out whether ub is equal to mid
            auto record_left_ptr = get_record_ptr(left);
            if (std::memcmp(get_data(key_or_record), record_left_ptr, get_size(key_or_record)) < 0) {  // ub is less than mid
              right = mid;
              // Debug statement
              std::cout << "Adjusting right to " << right << " after comparing with left record" << std::endl;
              continue;
            }
          }
          // ub is equal to mid
          left = mid;
          // Debug statement
          std::cout << "Adjusting left to " << left << "after comparing with mid record" << std::endl;
        }
      } else if (ret >= 0) {  // ub is greater than mid
        left = mid + 1;
        // Debug statement
        std::cout << "Adjusting left to " << left << std::endl;
      }
    }
    // ub is guaranteed to be not equal, regardless of whether a match exists.
    return Iterator(this, left);
  }

  std::optional<Iterator> search(const KeyOrRecord& key_or_record) {
    auto lb = search_lb(key_or_record);
    if (!lb.has_value()) return std::nullopt;
    auto record_lb = *lb.value();
    if (std::memcmp(get_data(key_or_record), record_lb.data(), get_size(key_or_record)) == 0)
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
      if (to_left >= lower_bound && (*bitmap_)[to_left] == true) return to_left;
      if (to_right < upper_bound && (*bitmap_)[to_right] == true) return to_right;
      --to_left;
      ++to_right;
    }
    return RECORD_COUNT;
  }

  //
  std::pair<Iterator, bool> insert(Record& record, bool allow_dup = true) {
    if (bitmap_->count() == RECORD_COUNT) return {Iterator(this, RECORD_COUNT), false};
    // page is full, split to be managed by page owner
    Iterator ub = search_ub(record);
    std::cout << "ub index: " << ub.index_ << std::endl;
    if (!allow_dup) {
      for (Iterator it = ub; it < end(); --it) {
        if (get_bit(it) == true) {
          if (std::equal(record.begin(), record.end(), (*it).begin()))
            return {it++, false};  // It should have been inserted here, but cannot because of duplicate ahead of it.
          break;
        }
      }
    }

    if (get_bit(ub) == false) {
      (*ub).swap(const_cast<Record&>(record));
      set_bit(ub, true);
      return {ub, true};
    }
    // Moving elements is inevitable
    // Erasing from the tail is more efficient
    bool before_ub = false;
    auto it = end() - 1;
    while (it >= begin()) {
      if (!get_bit(it)) {
        if (it < ub) before_ub = true;
        break;
      }
      --it;
    }
    // Bitmap knows nothing about insertion and erasion yet.
    if (before_ub) {
      --ub;
      // all bits between (records_it, ub-1] need to shift 1 pos forward, overwriting bitmap[records_it]
      while (it < ub) {
        set_bit(it, get_bit(it + 1));
        std::copy((*(it + 1)).begin(), (*(it + 1)).end(), (*it).begin());
        ++it;
      }
      // leaving an empty seat at ub-1, which should be set to true

    } else {
      // all bit between [ub, records_it) need to shift 1 pos backward, overwriting bitmap[records_it]
      while (it > ub) {
        set_bit(it, get_bit(it - 1));
        std::copy((*(it - 1)).begin(), (*(it - 1)).end(), (*it).begin());
        --it;
      }
      // leaving an empty seat at ub, which should be set to true
    }
    set_bit(ub, true);
    (*ub).swap(record);
    return {ub, true};
  }

  // No element shifting, which only happens at insertions
  std::optional<Iterator> erase(size_t index) {
    if ((*bitmap_)[index]) {
      // will be cleaned when elements shift during insertions
      (*bitmap_)[index] = false;
      return Iterator(this, index);
    }
    return std::nullopt;
  }

  std::optional<Iterator> erase(Iterator it) { return erase(it.index_); }

  std::optional<Iterator> erase(const Record& record) {
    auto found = search(record);
    if (!found.has_value()) return std::nullopt;
    return erase(found.value());
  }

  // Returns the key to copy up and insert into the tree
  // Operate on the assumption that a datapage is (almost) full when it must split, DERERRED to caller to ensure that
  // Otherwise, unexpected behavior when the bitmap is skewed.
  Key split_with(FixedRecordDataPage<PAGE_SIZE, RECORD_SIZE, KEY_SIZE>& right_sibling) {
    assert(right_sibling.bitmap_->none());
    size_t mid = find_first_occupied(RECORD_COUNT / 2);
    size_t right_index = 0;
    for (size_t i = mid; i < RECORD_COUNT; ++i) {
      if ((*bitmap_)[i] == 1) {
        right_sibling.get_record(right_index)->swap(*get_record(i));
        (*bitmap_)[i] = 0;
        (*right_sibling.bitmap_)[right_index] = 1;
        ++right_index;
      }
    }
    return copy_key(mid);
  }

  bool verify_order() {
    Record last;
    size_t last_index;
    std::fill(last.begin(), last.end(), 0);
    for (Iterator it = begin(); it < end(); ++it) {
      if (get_bit(it) == 1) {
        if (std::memcmp(last.data(), (*it).data(), RECORD_SIZE) > 0) {
          std::cout << "Order violation: " << record_to_string(last).substr(0, 5) << " (#" << last_index << ") > "
                    << record_to_string(*it).substr(0, 5) << " (#" << it.index_ << ")\n";
          return false;
        } else {
          std::cout << "# " << it.index_ << " " << record_to_string(*it).substr(0, 5) << "; ";
        }
        last = *it;
        last_index = it.index_;
      }
    }
    return true;
  }
};

// Fixed-length key, variable-length value
// template <size_t PAGE_SIZE = 4000, size_t KEY_SIZE>
// class VariableVDataPage : public DataPage<PAGE_SIZE>

#endif