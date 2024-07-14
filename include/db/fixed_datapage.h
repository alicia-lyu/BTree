#ifndef FIXED_DATAPAGE_H
#define FIXED_DATAPAGE_H

#include "datapage.h"
#include <array>
#include <bitset>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <fstream>
#include <stdexcept>

template <size_t PAGE_SIZE, size_t RECORD_SIZE, size_t KEY_SIZE>
class FixedRecordDataPage : public DataPage<PAGE_SIZE, std::array<unsigned char, RECORD_SIZE>, std::array<unsigned char, KEY_SIZE>> {
 public:
  static constexpr size_t RECORD_COUNT = (PAGE_SIZE - sizeof(uintmax_t)) / (RECORD_SIZE + 1 / 8);
  using Base = DataPage<PAGE_SIZE, std::array<unsigned char, RECORD_SIZE>, std::array<unsigned char, KEY_SIZE>>;
  using Key = std::array<unsigned char, KEY_SIZE>;
  using Record = std::array<unsigned char, RECORD_SIZE>;
  using typename Base::KeyOrRecord;
  using typename Base::MMapFile;

 private:
  static constexpr size_t DATA_SIZE = RECORD_COUNT * RECORD_SIZE;
  using RecordData = std::array<unsigned char, DATA_SIZE>;
  using Self = FixedRecordDataPage<PAGE_SIZE, RECORD_SIZE, KEY_SIZE>;

 protected:
  std::bitset<RECORD_COUNT>* bitmap_;  // 0 for free, 1 for occupied
  RecordData* record_data_;
  std::filesystem::path path_;
  uintmax_t page_offset_;

 public:
  FixedRecordDataPage(std::filesystem::path p, uintmax_t file_offset) {
    std::ifstream file(p, std::ios::binary | std::ios::in);
    file.seekg(file_offset);
    if (!file.read(reinterpret_cast<char*>(&this->next_page_offset_), sizeof(uintmax_t))) {
      throw std::runtime_error("Failed to read next page offset");
    }
    if (!file.read(reinterpret_cast<char*>(bitmap_), sizeof(std::bitset<RECORD_COUNT>))) {
      throw std::runtime_error("Failed to read bitmap");
    }
    if (!file.read(reinterpret_cast<char*>(record_data_), DATA_SIZE)) {
      throw std::runtime_error("Failed to read record data");
    }
  }

  FixedRecordDataPage() : bitmap_(std::make_unique<std::bitset<RECORD_COUNT>>()), record_data_(std::make_unique<RecordData>(RECORD_COUNT)) {}

  ~FixedRecordDataPage() {
    std::ofstream file(path_, std::ios::binary | std::ios::out);
    file.seekp(page_offset_);
    if (!file.write(reinterpret_cast<char*>(&this->next_page_offset_), sizeof(uintmax_t))) {
        throw std::runtime_error("Failed to write next page offset");
    }
    if (!file.write(reinterpret_cast<char*>(bitmap_), sizeof(std::bitset<RECORD_COUNT>))) {
        throw std::runtime_error("Failed to write bitmap");
    }
    if (!file.write(reinterpret_cast<char*>(record_data_), DATA_SIZE)) {
        throw std::runtime_error("Failed to write record data");
    }
  }

  size_t size() { return bitmap_->count(); }

 protected:
  Record* get_record(size_t index) override {
    Record* record_in_place = reinterpret_cast<Record*>(record_data_->data() + RECORD_SIZE * index);
    return record_in_place;
  }

  Record copy_record(size_t index) override {
    Record record;
    std::copy(record_data_->data() + RECORD_SIZE * index, record_data_->data() + RECORD_SIZE * index + RECORD_SIZE, record.begin());
    return record;
  }

  unsigned char* get_record_ptr(size_t index) override { return record_data_->data() + RECORD_SIZE * index; }

  Key* get_key(size_t index) override {
    Key* key_in_place = reinterpret_cast<Key*>(record_data_->data() + RECORD_SIZE * index);
    return key_in_place;
  }

  Key copy_key(size_t index) override {
    Key key;
    std::copy(record_data_->data() + RECORD_SIZE * index, record_data_->data() + RECORD_SIZE * index + KEY_SIZE, key.begin());
    return key;
  }

 public:
  using typename Base::iterator_type;

  iterator_type begin() override { return iterator_type(this, 0); }

  iterator_type end() override { return iterator_type(this, RECORD_COUNT); }

 protected:
  bool get_bit(iterator_type it) { return bitmap_->test(it.index_); }

  void flip_bit(iterator_type it) { bitmap_->flip(it.index_); }

  void set_bit(iterator_type it, bool value) {
    // std::cout << "Setting bit at index " << it.index_ << " to " << value << std::endl;
    bitmap_->set(it.index_, value);
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

 public:
  bool validate(iterator_type it) override { return get_bit(it) == true; }

  // LATER: Migrate to SIMD (How to handle bitmap jumps?)
  iterator_type search_lb(const KeyOrRecord& key_or_record) override {
    if (bitmap_->count() == 0) return end();
    size_t left = find_first_occupied(0);  // inclusive
    auto ret = std::memcmp(Base::get_data(key_or_record), get_record_ptr(left), Base::get_size(key_or_record));
    if (ret == 0)
      return iterator_type(this, left);  // Early return helps guarantee left < key_or_record
    else if (ret < 0)
      return end();  // key_or_record is less than the first element

    size_t right = find_first_occupied(RECORD_COUNT) + 1;  // exclusive
    // Note that left is guaranteed to be occupied, while right is not.
    while (get_occupancy_in_range(left, right) > 1)  // lock in to one entry
    {
      size_t mid = find_first_occupied(left + (right - left) / 2, left, right);
      if (mid == RECORD_COUNT)  // no valid records between [left, right)
        return iterator_type(this, left);
      auto record_mid_ptr = get_record_ptr(mid);  // Get the whole record but only comparing the first KEY_SIZE bytes.
      int ret = std::memcmp(Base::get_data(key_or_record), record_mid_ptr, Base::get_size(key_or_record));
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
    return iterator_type(this, left);
  }

  iterator_type search_ub(const KeyOrRecord& key_or_record) override {
    if (bitmap_->count() == 0) return begin();
    size_t left = 0;              // inclusive
    size_t right = find_first_occupied(RECORD_COUNT);
    auto ret = std::memcmp(Base::get_data(key_or_record), get_record_ptr(right), Base::get_size(key_or_record));
    if (ret >= 0)
      return end();
    ++right; // exclusive
    // Now it is guaranteed that ub exists before right

    while (right - left > 1) {    // lock in to one entry
      size_t mid = find_first_occupied(left + (right - left) / 2, left, right);
      if (mid == RECORD_COUNT)  // no valid records between [left, right)
        return iterator_type(this, left);

      auto record_mid_ptr = get_record_ptr(mid);  // Get the whole record but only comparing the first KEY_SIZE bytes.
      int ret = std::memcmp(Base::get_data(key_or_record), record_mid_ptr, Base::get_size(key_or_record));

      std::cout << "left: " << left << ", right: " << right << ", mid: " << mid << ", ret: " << ret
                << ", record_mid: " << std::string(record_mid_ptr, record_mid_ptr + 5) << std::endl;

      if (ret < 0) {  // ub is less than or equal to mid
        right = mid + 1;
        std::cout << "Adjusting right to " << right << std::endl;
        // Only having `right = mid + 1` may enter into infinite loop, when right keeps being reset to left + 2
        if (right - left == 2) {
          if (bitmap_->test(left) == true) {  // We must find out whether ub is equal to mid
            auto record_left_ptr = get_record_ptr(left);
            if (std::memcmp(Base::get_data(key_or_record), record_left_ptr, Base::get_size(key_or_record)) < 0) {  // ub is less than mid
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
    return iterator_type(this, left);
  }

  iterator_type search(const KeyOrRecord& key_or_record) override {
    auto lb = search_lb(key_or_record);
    if (!lb.has_value()) return end();
    auto record_lb = *lb.value();
    if (std::memcmp(Base::get_data(key_or_record), record_lb.data(), Base::get_size(key_or_record)) == 0)
      return lb;
    else
      return end();
  }

  std::pair<iterator_type, bool> insert(Record& record, bool allow_dup = true) override {
    if (bitmap_->count() == RECORD_COUNT) return {iterator_type(this, RECORD_COUNT), false};
    // page is full, split to be managed by page owner
    iterator_type ub = search_ub(record);
    std::cout << "ub index: " << ub.index_ << std::endl;
    if (!allow_dup) {
      for (iterator_type it = ub; it < end(); --it) {
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
    // LATER: Migrate to std::shift_left and std::shift_right
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

 private:
  // No element shifting, which only happens at insertions
  std::optional<iterator_type> erase(size_t index) {
    if ((*bitmap_)[index]) {
      // will be cleaned when elements shift during insertions
      (*bitmap_)[index] = false;
      return iterator_type(this, index);
    }
    return std::nullopt;
  }

 public:
  std::optional<iterator_type> erase(iterator_type it) override { return erase(it.index_); }

  std::optional<iterator_type> erase(const Record& record) override {
    auto found = search(record);
    if (!found.has_value()) return std::nullopt;
    return erase(found.value());
  }

  bool is_full() {
    return bitmap_->count() == RECORD_COUNT;
  }

  // Returns the key to copy up and insert into the tree
  // Operate on the assumption that a datapage is (almost) full when it must split, DERERRED to caller to ensure that
  // Otherwise, unexpected behavior when the bitmap is skewed.
  Key split_with(Base* right_sibling) override {
    // Perform a dynamic_cast to check if right_sibling is of type Self
    Self* right_sibling_self = dynamic_cast<Self*>(right_sibling);
    assert(right_sibling_self && "right_sibling is not of the correct type");

    assert(right_sibling_self->bitmap_->none());

    size_t mid = find_first_occupied(RECORD_COUNT / 2);
    size_t right_index = 0;

    // Move records from the middle to the end of the current page to the right_sibling
    for (size_t i = mid; i < RECORD_COUNT; ++i) {
      if ((*bitmap_)[i] == 1) {
        right_sibling_self->get_record(right_index)->swap(*get_record(i));
        (*bitmap_)[i] = 0;
        (*right_sibling_self->bitmap_)[right_index] = 1;
        ++right_index;
      }
    }

    // Return the key of the middle record
    return copy_key(mid);
  }

  bool verify_order() override {
    Record last;
    size_t last_index;
    std::fill(last.begin(), last.end(), 0);
    for (iterator_type it = begin(); it < end(); ++it) {
      if (get_bit(it) == 1) {
        if (std::memcmp(last.data(), (*it).data(), RECORD_SIZE) > 0) {
          std::cout << "Order violation: " << Base::record_to_string(last).substr(0, 5) << " (#" << last_index << ") > "
                    << Base::record_to_string(*it).substr(0, 5) << " (#" << it.index_ << ")\n";
          return false;
        } else {
          std::cout << "# " << it.index_ << " " << Base::record_to_string(*it).substr(0, 5) << "; ";
        }
        last = *it;
        last_index = it.index_;
      }
    }
    std::cout << std::endl;
    return true;
  }

  iterator_type max() override {
    iterator_type it = end() - 1;
    while (it >= begin()) {
      if (get_bit(it) == 1) return it;
      --it;
    }
    return it;
  }

  iterator_type min() override {
    iterator_type it = begin();
    while (it < end()) {
      if (get_bit(it) == 1) return it;
      ++it;
    }
    return it;
  }
};

#endif  // FIXED_DATAPAGE_H