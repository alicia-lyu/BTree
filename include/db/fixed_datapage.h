#ifndef FIXED_DATAPAGE_H
#define FIXED_DATAPAGE_H

#include "datapage.h"
#include <array>
#include <bitset>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <fstream>
#include <optional>
#include <ostream>
#include <stdexcept>

template <size_t PAGE_SIZE, size_t RECORD_SIZE, size_t KEY_SIZE>
class FixedRecordDataPage : public DataPage<PAGE_SIZE, std::array<unsigned char, RECORD_SIZE>, std::array<unsigned char, KEY_SIZE>> {
 public:
  static constexpr size_t PAGE_SIZE_CONST = PAGE_SIZE;
  static constexpr size_t RECORD_COUNT = (PAGE_SIZE - sizeof(uintmax_t)) / (RECORD_SIZE + 1 / 8);
  using Base = DataPage<PAGE_SIZE, std::array<unsigned char, RECORD_SIZE>, std::array<unsigned char, KEY_SIZE>>;
  using Key = std::array<unsigned char, KEY_SIZE>;
  using Record = std::array<unsigned char, RECORD_SIZE>;
  using typename Base::KeyOrRecord;

  uintmax_t page_offset_;

 private:
  static constexpr size_t DATA_SIZE = RECORD_COUNT * RECORD_SIZE;
  using RecordData = std::array<unsigned char, DATA_SIZE>;
  using Self = FixedRecordDataPage<PAGE_SIZE, RECORD_SIZE, KEY_SIZE>;

 protected:
  std::unique_ptr<std::bitset<RECORD_COUNT>> bitmap_;  // 0 for free, 1 for occupied
  std::unique_ptr<RecordData> record_data_;
  std::filesystem::path path_;

 public:
  FixedRecordDataPage(std::filesystem::path p, uintmax_t file_offset, std::optional<uintmax_t> next_page_offset = std::nullopt)
      : page_offset_(file_offset), path_(p) {
    assert(file_offset != 0);
    assert(next_page_offset != 0);
    
    bitmap_ = std::make_unique<std::bitset<RECORD_COUNT>>();
    record_data_ = std::make_unique<RecordData>();

    if (next_page_offset != std::nullopt) {
      this->next_page_offset_ = next_page_offset.value();
      return;
    }

    assert(std::filesystem::exists(p) && std::filesystem::file_size(p) >= file_offset + PAGE_SIZE);
    std::ifstream file(p, std::ios::binary | std::ios::in);
    file.seekg(file_offset);
    assert(sizeof(uintmax_t) + sizeof(std::bitset<RECORD_COUNT>) + sizeof(RecordData) <= PAGE_SIZE);
    if (!file.read(reinterpret_cast<char*>(&this->next_page_offset_), sizeof(uintmax_t))) {
      throw std::runtime_error("Failed to read next page offset");
    }
    if (!file.read(reinterpret_cast<char*>(bitmap_.get()), sizeof(std::bitset<RECORD_COUNT>))) {
      throw std::runtime_error("Failed to read bitmap");
    }
    if (!file.read(reinterpret_cast<char*>(record_data_.get()), sizeof(RecordData))) {
      throw std::runtime_error("Failed to read record data");
    }
    std::cout << "Deserialized " << *this << std::endl;
    assert(this->next_page_offset_ != 0);
  }

  FixedRecordDataPage() {
    bitmap_ = std::make_unique<std::bitset<RECORD_COUNT>>();
    record_data_ = std::make_unique<RecordData>();
    this->next_page_offset_ = std::numeric_limits<uintmax_t>::max();
  }

  ~FixedRecordDataPage() {
    std::ofstream file(path_, std::ios::binary);
    std::cout << "Serializing " << *this << std::endl;
    file.seekp(page_offset_);
    if (!file.write(reinterpret_cast<char*>(&this->next_page_offset_), sizeof(uintmax_t))) {
      std::cerr << "Error: Failed to write next page offset" << std::endl;
    }
    if (!file.write(reinterpret_cast<char*>(bitmap_.get()), sizeof(std::bitset<RECORD_COUNT>))) {
      std::cerr << "Error: Failed to write bitmap" << std::endl;
    }
    if (!file.write(reinterpret_cast<char*>(record_data_.get()), sizeof(RecordData))) {
      std::cerr << "Error: Failed to write record data" << std::endl;
    }

    size_t padding = PAGE_SIZE_CONST - sizeof(uintmax_t) - sizeof(std::bitset<RECORD_COUNT>) - sizeof(RecordData);
    if (padding < 0) {
      std::cerr << "Error: Data exceeds page size." << std::endl;
    } else if (padding > 0) {
      std::cout << "FixedRecordDataPage at offset " << page_offset_ << ": Padding " << padding << " bytes to page in file" << path_ << std::endl;
      file.write(std::string(padding, '\0').c_str(), padding);
    }
  }

  size_t size() const override { return bitmap_->count(); }

  size_t max_size() const override { return RECORD_COUNT; }

 protected:
  Record* get_record(size_t index) const override {
    Record* record_in_place = reinterpret_cast<Record*>(record_data_->data() + RECORD_SIZE * index);
    return record_in_place;
  }

  Record copy_record(size_t index) const override {
    Record record;
    std::copy(record_data_->data() + RECORD_SIZE * index, record_data_->data() + RECORD_SIZE * index + RECORD_SIZE, record.begin());
    return record;
  }

  unsigned char* get_record_ptr(size_t index) const override { return record_data_->data() + RECORD_SIZE * index; }

  Key* get_key(size_t index) const override {
    Key* key_in_place = reinterpret_cast<Key*>(record_data_->data() + RECORD_SIZE * index);
    return key_in_place;
  }

  Key copy_key(size_t index) const override {
    Key key;
    std::copy(record_data_->data() + RECORD_SIZE * index, record_data_->data() + RECORD_SIZE * index + KEY_SIZE, key.begin());
    return key;
  }

 public:
  using iterator_type = Base::Iterator;

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
    // std::cout << "Finding first occupied from " << index << " in [" << lower_bound << ", " << upper_bound << ")" << std::endl;
    // std::cout << "Bitmap: " << bitmap_->to_string() << std::endl;
    assert(index >= lower_bound && index < upper_bound);
    int to_left = index;
    int to_right = index;
    int lower_bound_int = static_cast<int>(lower_bound);
    int upper_bound_int = static_cast<int>(upper_bound);
    while (to_left >= lower_bound_int || to_right < upper_bound_int) {
      if (to_left >= lower_bound_int && bitmap_->test(to_left)) {
        // std::cout << "Found at " << to_left << std::endl;
        return to_left;
      }
      if (to_right < upper_bound_int && bitmap_->test(to_right)) {
        // std::cout << "Found at " << to_right << std::endl;
        return to_right;
      }
      --to_left;
      ++to_right;
    }
    return RECORD_COUNT;
  }

 public:
  bool validate(iterator_type it) override { return get_bit(it) == true; }

  // Returns the iterator to the first element that is greater than or equal to key_or_record
  iterator_type search_lb(const KeyOrRecord& key_or_record) override {
    size_t left = find_first_occupied(0); // inclusive
    size_t right = RECORD_COUNT;  // exclusive
    // Note that left is guaranteed to be occupied, while right is not.
    while (right - left > 1)  // lock in to one entry
    {
      size_t mid = find_first_occupied(left + (right - left) / 2, left, right);
      // std::cout << "left: " << left << ", right: " << right << ", mid: " << mid << std::endl;
      if (mid == RECORD_COUNT)  // no valid records between [left, right)
        return end();
      auto record_mid_ptr = get_record_ptr(mid);
      int ret = std::memcmp(Base::get_data(key_or_record), record_mid_ptr, Base::get_size(key_or_record));
      if (ret <= 0)  // lb is less than or equal to mid
      {
        right = mid + 1;
        if (right - left == 2) {
          // we must find out if lb is equal to mid
          if (bitmap_->test(left) == true) {
            auto record_left_ptr = get_record_ptr(left);
            if (std::memcmp(Base::get_data(key_or_record), record_left_ptr, Base::get_size(key_or_record)) <= 0) {
              right = mid;
              continue;
            }
          }
          left = mid;
        }
      } else if (ret > 0)  // lb is greater than mid
        left = mid + 1;
    }
    // std::cout << "lb is " << left << std::endl;
    // If there is any element equal to key_or_record, left is equal.
    return iterator_type(this, left);
  }

  iterator_type search_ub(const KeyOrRecord& key_or_record) override {
    if (bitmap_->count() == 0) return begin();
    size_t left = 0;  // inclusive
    size_t right = find_first_occupied(RECORD_COUNT - 1);
    auto ret = std::memcmp(Base::get_data(key_or_record), get_record_ptr(right), Base::get_size(key_or_record));
    if (ret >= 0) {
      if (right == RECORD_COUNT - 1)
        return end();
      else
        return iterator_type(this, right + 1);  // The first empty spot
    }
    ++right;  // exclusive
    // Now it is guaranteed that ub exists before right

    while (right - left > 1) {  // lock in to one entry
      size_t mid = find_first_occupied(left + (right - left) / 2, left, right);
      if (mid == RECORD_COUNT)  // no valid records between [left, right)
        return iterator_type(this, left);

      auto record_mid_ptr = get_record_ptr(mid);  // Get the whole record but only comparing the first KEY_SIZE bytes.
      int ret = std::memcmp(Base::get_data(key_or_record), record_mid_ptr, Base::get_size(key_or_record));

      // std::cout << "left: " << left << ", right: " << right << ", mid: " << mid << ", ret: " << ret
      //           << ", record_mid: " << std::string(record_mid_ptr, record_mid_ptr + 5) << std::endl;

      if (ret < 0) {  // ub is less than or equal to mid
        right = mid + 1;
        // std::cout << "Adjusting right to " << right << std::endl;
        // Only having `right = mid + 1` may enter into infinite loop, when right keeps being reset to left + 2
        if (right - left == 2) {
          if (bitmap_->test(left) == true) {  // We must find out whether ub is equal to mid
            auto record_left_ptr = get_record_ptr(left);
            if (std::memcmp(Base::get_data(key_or_record), record_left_ptr, Base::get_size(key_or_record)) < 0) {  // ub is less than mid
              right = mid;
              // Debug statement
              // std::cout << "Adjusting right to " << right << " after comparing with left record" << std::endl;
              continue;
            }
          }
          // ub is equal to mid
          left = mid;
          // Debug statement
          // std::cout << "Adjusting left to " << left << "after comparing with mid record" << std::endl;
        }
      } else if (ret >= 0) {  // ub is greater than mid
        left = mid + 1;
        // Debug statement
        // std::cout << "Adjusting left to " << left << std::endl;
      }
    }
    // ub is guaranteed to be not equal, regardless of whether a match exists.
    return iterator_type(this, left);
  }

  iterator_type search(const KeyOrRecord& key_or_record) override {
    auto lb = search_lb(key_or_record);
    if (lb == end()) return end();
    auto record_lb = *lb;
    if (std::memcmp(Base::get_data(key_or_record), record_lb.data(), Base::get_size(key_or_record)) == 0)
      return lb;
    else
      return end();
  }

  std::pair<iterator_type, bool> insert(const Record& record, bool allow_dup = true) override {
    if (bitmap_->count() == RECORD_COUNT) return {end(), false};  // page is full, split to be managed by page owner

    iterator_type ub = end();
    if (!allow_dup) {
      iterator_type lb = search_lb(record);
      if (lb != end()) {
        auto ret = std::memcmp(record.data(), (*lb).data(), RECORD_SIZE);
        if (ret == 0) return {lb, false};
      }
      ub = lb++;
    } else {
      ub = search_ub(record);
    }

    if (ub == end()) {
      size_t empty_index = solidify();
      ub = iterator_type(this, empty_index);
    }

    if (get_bit(ub) == false) {
      std::copy(record.begin(), record.end(), ub->begin());
      set_bit(ub, true);
      return {ub, true};
    }
    // Moving elements is inevitable
    // Erasing from the tail is more efficient
    bool before_ub = false;
    auto it = --end();
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
      // all bits between (records_it, ub-1] need to shift 1 pos forward, overwriting bitmap[records_it]
      std::move(record_data_->begin() + (it.index_ + 1) * RECORD_SIZE,
                record_data_->begin() + ub.index_ * RECORD_SIZE,
                record_data_->begin() + (it.index_) * RECORD_SIZE);
      --ub;
      while (it < ub) {
        set_bit(it, get_bit(it));
        ++it;
      }
      // leaving an empty seat at ub-1, which should be set to true
    } else {
      // all bit between [ub, records_it) need to shift 1 pos backward, overwriting bitmap[records_it]
      std::move(record_data_->begin() + ub.index_ * RECORD_SIZE,
                record_data_->begin() + it.index_ * RECORD_SIZE,
                record_data_->begin() + (ub.index_ + 1) * RECORD_SIZE);
      while (it > ub) {
        set_bit(it, get_bit(it - 1));
        --it;
      }
      // leaving an empty seat at ub, which should be set to true
    }
    set_bit(ub, true);
    std::copy(record.begin(), record.end(), ub->data());
    return {ub, true};
  }

 private:
  // No element shifting, which only happens at insertions
  iterator_type erase(size_t index) {
    if ((*bitmap_)[index]) {
      // will be cleaned when elements shift during insertions
      (*bitmap_)[index] = false;
      return iterator_type(this, index);
    }
    return end();
  }

 public:
  iterator_type erase(iterator_type it) override { return erase(it.index_); }

  iterator_type erase(const Record& record) override { return erase(search(record)); }

  bool is_full() override { return bitmap_->count() == RECORD_COUNT; }

  // Returns the key to copy up and insert into the tree
  // Operate on the assumption that a datapage is (almost) full when it must split, DERERRED to caller to ensure that
  // Otherwise, unexpected behavior when the bitmap is skewed.
  Record split_with(Base* right_sibling) override {
    // Perform a dynamic_cast to check if right_sibling is of type Self
    Self* right_sibling_self = dynamic_cast<Self*>(right_sibling);
    assert(right_sibling_self && "right_sibling is not of the correct type");
    assert(is_full());
    assert(right_sibling_self->bitmap_->none());

    auto empty_start = solidify();

    size_t total_size = size();

    assert(empty_start * 1.1 >= RECORD_COUNT);

    size_t left_size = RECORD_COUNT / 2;
    size_t right_size = size() - left_size;

    std::copy(record_data_->begin() + left_size * RECORD_SIZE, record_data_->end(), right_sibling_self->record_data_->begin());

    for (size_t i = left_size; i < RECORD_COUNT; ++i) {
      bitmap_->reset(i);
    }

    for (size_t i = 0; i < right_size; ++i) {
      right_sibling_self->bitmap_->set(i);
    }

    right_sibling->next_page_offset_ = this->next_page_offset_;
    this->next_page_offset_ = right_sibling_self->page_offset_;

    assert(size() + right_sibling_self->size() == total_size);

    // Return the key of the middle record
    return *(right_sibling->min());
  }

  // Place all valid records at the beginning of the page
  // Returning the start of empty space
  size_t solidify() {
    // Move valid records to the beginning
    size_t dest_index = 0;
    for (size_t src_index = 0; src_index < RECORD_COUNT; ++src_index) {
      if (bitmap_->test(src_index)) {
        if (dest_index != src_index) {
          std::copy(record_data_->begin() + src_index * RECORD_SIZE,
                    record_data_->begin() + (src_index + 1) * RECORD_SIZE,
                    record_data_->begin() + dest_index * RECORD_SIZE);
          std::fill(record_data_->begin() + src_index * RECORD_SIZE,
                    record_data_->begin() + (src_index + 1) * RECORD_SIZE,
                    static_cast<unsigned char>('\0'));
        }
        dest_index++;
      }
    }

    for (size_t i = 0; i < dest_index; ++i) {
      bitmap_->set(i);
    }

    for (size_t i = dest_index; i < RECORD_COUNT; ++i) {
      bitmap_->reset(i);
    }

    return dest_index;
  }

  void merge_with(Base* right_sibling) override {
    Self* right_sibling_self = dynamic_cast<Self*>(right_sibling);
    assert(right_sibling_self && "right_sibling is not of the correct type");

    size_t target_size = size() + right_sibling_self->size();
    assert(target_size <= max_size());

    auto left_empty_start = solidify();
    right_sibling_self->solidify();

    // Move records from right sibling to current page
    std::copy(
        right_sibling_self->record_data_->begin(), right_sibling_self->record_data_->end(), record_data_->begin() + left_empty_start * RECORD_SIZE);

    // Update the bitmap
    for (size_t i = 0; i < right_sibling_self->size(); ++i) {
      bitmap_->set(left_empty_start + i);
    }

    right_sibling_self->bitmap_->reset();

    this->next_page_offset_ = right_sibling->next_page_offset_;

    assert(size() == target_size);
    assert(verify_order());
  }

  // When the current page is less than half full, redistribute records from the right sibling
  Record borrow_from(Base* right_sibling) override {
    Self* right_sibling_self = dynamic_cast<Self*>(right_sibling);
    assert(right_sibling_self && "right_sibling is not of the correct type");

    size_t left_size = size();
    size_t right_size = right_sibling_self->size();
    size_t total_size = left_size + right_size;
    size_t target_left_size = total_size / 2;

    auto left_empty_start = solidify();
    right_sibling_self->solidify();

    assert(left_size < target_left_size);

    size_t to_move = target_left_size - left_size;

    // Move records from right sibling to current page
    std::copy(right_sibling_self->record_data_->begin(),
              right_sibling_self->record_data_->begin() + to_move * RECORD_SIZE,
              record_data_->begin() + left_empty_start * RECORD_SIZE);

    // Update the bitmaps
    for (size_t i = 0; i < to_move; ++i) {
      bitmap_->set(left_empty_start + i);
      right_sibling_self->bitmap_->reset(i);
    }

    assert(size() == target_left_size);
    assert(verify_order());

    return *(right_sibling->min());
  }

  bool verify_order() override {
    Record last;
    size_t last_index;
    std::fill(last.begin(), last.end(), 0);
    for (iterator_type it = begin(); it < end(); ++it) {
      if (get_bit(it) == 1) {
        if (std::memcmp(last.data(), (*it).data(), RECORD_SIZE) > 0) {
          std::cout << "Order violation: " << Base::record_to_string(last).substr(RECORD_SIZE - 5, 5) << " (#" << last_index << ") > "
                    << Base::record_to_string(*it).substr(RECORD_SIZE - 5, 5) << " (#" << it.index_ << ")\n";
          return false;
        } else {
          std::cout << "# " << it.index_ << " " << Base::record_to_string(*it).substr(RECORD_SIZE - 5, 5) << "; ";
        }
        last = *it;
        last_index = it.index_;
      }
    }
    std::cout << std::endl;
    return true;
  }

  iterator_type retreat_to_valid(iterator_type it) {
    if (it == end()) --it;
    while (it >= begin()) {
      if (get_bit(it) == 1) return it;
      --it;
    }
    return end();
  }

  iterator_type advance_to_valid(iterator_type it) {
    while (it < end()) {
      if (get_bit(it) == 1) return it;
      ++it;
    }
    return end();
  }

  iterator_type max() override { return retreat_to_valid(end()); }

  iterator_type min() override { return advance_to_valid(begin()); }

  friend std::ostream& operator<<(std::ostream& os, const Self& page) {
    os << "Page at offset " << page.page_offset_ << ":\n";
    os << "next_page_offset_: " << page.next_page_offset_ << std::endl;
    for (size_t i = 0; i < RECORD_COUNT; ++i) {
      if (page.bitmap_->test(i)) {
        os << "#" << i << ": " << page.record_to_string(*(page.get_record(i))).substr(0, 5) << "...; ";
      } else {
        os << "#" << i << ": Empty" << "; ";
      }
    }
    os << std::endl;
    return os;
  }
};

#endif  // FIXED_DATAPAGE_H