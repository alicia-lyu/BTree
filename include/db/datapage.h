#include <stddef.h>
#include <filesystem>
#include <fstream>
#include <array>
#include <vector>
#include <bitset>
#include <optional>
#include <cstring>

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

template <size_t PAGE_SIZE = 4000, class Istream = std::ifstream, class Ostream = std::ofstream>
class DataPage {
 public:
  const std::filesystem::path path;
  const size_t file_offset;

  DataPage(std::filesystem::path path, size_t file_offset) : path(path), file_offset(file_offset) {}

  virtual ~DataPage() = default;
};

template <size_t PAGE_SIZE = 4000, size_t RECORD_SIZE, size_t KEY_SIZE>
  requires(PAGE_SIZE - PAGE_SIZE / RECORD_SIZE / 8 >= RECORD_SIZE)
class FixedRecordDataPage : public DataPage<PAGE_SIZE> {
  static constexpr RECORD_COUNT = PAGE_SIZE / RECORD_SIZE;
  static constexpr DATA_SIZE = PAGE_SIZE - RECORD_COUNT / 8;
  using Record = typename std::array<unsigned char, RECORD_SIZE>;
  using Key = typename std::array<unsigned char, KEY_SIZE>;
  using KeyOrRecord = typename std::variant<Record, Key>;

 public:
  std::bitset<RECORD_COUNT> bitmap_;  // 0 for free, 1 for occupied
  std::vector<Record> records;

  FixedRecordDataPage(std::filesystem::path path, size_t file_offset) : DataPage(path, file_offset), records(RECORD_COUNT) {
    Istream file(path, std::ios::binary | std::ios::in);
    file.seekg(file_offset);
    file.read(reinterpret_cast<char *>(&bitmap_), sizeof(bitmap_));
    file.read(reinterpret_cast<char *>(&records.data()), sizeof(records));
  }

  ~FixedRecordDataPage() { flush(); }

  void flush() {
    Ostream file(path, std::ios::binary | std::ios::out);
    file.seekp(file_offset);
    file.write(reinterpret_cast<char *>(&bitmap_), sizeof(bitmap_));
    file.write(reinterpret_cast<char *>(&records.data()), sizeof(records));
  }

  Record &get_record(size_t index) { return records.at(index); }

  Key get_key(size_t index) {
    auto record = get_record(index);
    Key key;
    std::copy(record.begin(), record.begin() + KEY_SIZE, key.begin());
    return key;
  }

  size_t get_occupancy_in_range(size_t left, size_t right) {
    size_t occupied = 0;
    for (size_t i = left; i < right; i++) {
      if (bitmap_[i] = true) occupied++;
    }
    return occupied;
  }

  class Iterator : public std::iterator<std::bidirectional_iterator_tag, Record> {
    FixedRecordDataPage *page_;
    size_t index_;

    void advance_to_valid() {
      while (index_ < RECORD_COUNT && !page_->bitmap_[index_]) {
        ++index_;
      }
    }

    void retreat_to_valid() {
      while (index_ > 0 && !page_->bitmap_[index_]) {
        --index_;
      }
    }

   public:
    Iterator(FixedRecordDataPage *page, size_t index) : page_(page), index_(index) { advance_to_valid(); }

    Iterator &operator++() {
      ++index_;
      advance_to_valid();
      return *this;
    }

    Iterator &operator--() {
      retreat_to_valid();
      return *this;
    }

    bool operator==(const Iterator &other) const { return page_ == other.page_ && index_ == other.index_; }

    bool operator!=(const Iterator &other) const { return !(*this == other); }

    Record &operator*() { return page_->get_record(index_); }

    Record *operator->() { return &page_->get_record(index_); }
  };

  Iterator begin() { return Iterator(this, 0); }

  Iterator end() { return Iterator(this, RECORD_COUNT); }

  // LATER: Migrate to SIMD (How to handle bitmap jumps?)
  Iterator search_lb(const KeyOrRecord &key_or_record) {
    if (bitmap_.count() == 0) return end();
    size_t left = find_first_occupied(0);  // inclusive
    if (std::memcmp(key_or_record.data(), records.at(left).data(), key_or_record.size()) == 0)
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

  Iterator search_ub(const KeyOrRecord &key_or_record) {
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
    return Iterator(this, left);
  }

  std::optional<Iterator> search(const KeyOrRecord &key_or_record) {
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
      to_left--;
      to_right++;
    }
    return RECORD_COUNT;
  }

  Iterator insert(const Record &record) {
    if (bitmap_.count() == RECORD_COUNT) return Iterator(this, RECORD_COUNT);
    // page is full, split to be managed by page owner
    auto ub = search_ub(record);
    if (bitmap_[ub.index_] == 0) {
      (*ub).swap(record);
      bitmap_[ub.index_] = true;
      return Iterator(this, ub);
    }
    // Moving elements is inevitable
    records.insert(ub, record);  // temporarily grows larger than DATA_SIZE
    // Erasing from the tail is more efficient
    auto records_iter = records.end();
    bool before_ub = false;
    for (size_t i = RECORD_COUNT - 1; i >= 0; i--) {
      if (bitmap_[i] == false) {
        records.erase(records_iter - i);
        if (i < ub.index_) before_ub = true;
        break;
      }
    }
    if (before_ub) return Iterator(this, ub.index_ - 1);
    else return ub;
  }

  Iterator erase(size_t index) {
    if (bitmap_[index]) {
      // will be cleaned when elements shift during insertions
      bitmap_[index] = false;
      return Iterator(this, index);
    }
    return Iterator(this, RECORD_COUNT);
  }

  Iterator erase(Iterator it)
  {
    records.erase(it);
  }

  Iterator erase(const Record &record) {
    auto found = search(record);
    if (!found.has_value()) return Iterator(this, RECORD_COUNT);

  }

  // Returns the key to copy up and insert into the tree
  Key split_with(FixedRecordDataPage<PAGE_SIZE, RECORD_SIZE, KEY_SIZE> right_sibling) {
    assert(right_sibling.bitmap_.none());
    size_t mid = find_first_occupied(RECORD_COUNT / 2);
    size_t right_index = 0;
    for (size_t i = mid; i < RECORD_COUNT; i++) {
      if (bitmap_[i] == 1) {
        right_sibling.records.at(right_index).swap(records.at(i));
        bitmap_[i] = 0;
        right_sibling.bitmap_[right_index] = 1;
        right_index++;
      }
    }
    return get_key(mid);
  }
};

// Fixed-length key, variable-length value
// template <size_t PAGE_SIZE = 4000, size_t KEY_SIZE>
// class VariableVDataPage : public DataPage<PAGE_SIZE>