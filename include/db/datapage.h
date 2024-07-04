#include <stddef.h>
#include <filesystem>
#include <fstream>
#include <array>
#include <vector>
#include <bitset>
#include <optional>

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

template <size_t PAGE_SIZE = 4000>
class DataPage {
 public:
  const std::filesystem::path path;
  const size_t file_offset;

  DataPage(std::filesystem::path path, size_t file_offset) : path(path), file_offset(file_offset) {}

  virtual ~DataPage() = default;
};

template <size_t PAGE_SIZE = 4000, size_t RECORD_SIZE, size_t KEY_SIZE, class Istream = std::ifstream, class Ostream = std::ofstream>
  requires(PAGE_SIZE - PAGE_SIZE / RECORD_SIZE / 8 >= RECORD_SIZE)
class FixedRecordDataPage : public DataPage<PAGE_SIZE> {
  constexpr RECORD_COUNT = PAGE_SIZE / RECORD_SIZE;
  constexpr DATA_SIZE = PAGE_SIZE - RECORD_COUNT / 8;
  using Record = typename std::array<unsigned char, RECORD_SIZE>;
  using Key = typename std::array<unsigned char, KEY_SIZE>;
  using KeyOrRecord = typename std::variant<Record, Key>;

 public:
  std::bitset<RECORD_COUNT> bitmap_;  // 0 for free, 1 for occupied
  std::vector<Record> records;

  FixedRecordDataPage(size_t page_size, std::filesystem::path path, size_t file_offset)
      : DataPage(page_size, path, file_offset), records(RECORD_COUNT) {
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

  Key &&get_key(size_t index) {
    auto record = get_record(index);
    Key key;
    std::copy(record.begin(), record.begin() + KEY_SIZE, key.begin())
    return std::move(key);
  }

  size_t search_lb(const KeyOrRecord &key_or_record) {
    if (bitmap_.count() == 0) return 0;
    size_t left = 0;                // inclusive
    size_t right = bitmap_.size();  // exclusive
    while (left + 1 < right)        // lock in to one entry
    {
      size_t mid = find_first_occupied(left + (right - left) / 2, left, right);
      auto record_mid = get_record(mid);  // Get the whole record but only comparing the first KEY_SIZE bytes.
      int ret = std::memcmp(key_or_record.data(), record_mid.data(), key_or_record.size());
      if (ret < 0)  // lb is less than mid
        right = mid;
      else if (ret == 0)
        right = mid + 1;  // lb is less than or equal to mid
      else if (ret > 0)
        left = mid;  // lb is greater than or equal to mid (although mid is smaller, but mid+1 can be bigger)
    }
    return left;
  }

  size_t search_ub(const KeyOrRecord &key_or_record) {
    if (bitmap_.count() == 0) return 0;
    size_t left = 0;                // inclusive
    size_t right = bitmap_.size();  // exclusive
    while (left + 1 < right)        // lock in to one entry
    {
      size_t mid = find_first_occupied(left + (right - left) / 2, left, right);
      auto record_mid = get_record(mid);  // Get the whole record but only comparing the first KEY_SIZE bytes.
      int ret = std::memcmp(key_or_record.data(), record_mid.data(), key_or_record.size());
      if (ret < 0)  // ub is less than or equal to mid
        right = mid + 1;
      else if (ret >= 0)  // ub is greater than mid
        left = mid + 1;
    }
    return left;
  }

  std::optional<size_t> search(const KeyOrRecord &key_or_record)
  // Non-expected behavior if the searched item is not unique in the data page.
  {
    size_t left = 0;                // inclusive
    size_t right = bitmap_.size();  // exclusive
    while (left < right)            // lock in to one entry
    {
      size_t mid = find_first_occupied(left + (right - left) / 2, left, right);
      if (mid == RECORD_COUNT) return std::nullopt;
      auto record_mid = get_record(mid);
      int ret = std::memcmp(key_or_record.data(), record_mid.data(), key_or_record.size());
      if (ret < 0)
        right = mid;
      else if (ret == 0)
        return mid;
      else if (ret > 0)
        left = mid + 1;
    }
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

  bool insert_record(const Record &record) {
    if (bitmap_.count() == RECORD_COUNT) return false;
    // page is full, split managed by page owner
    auto ub = search_ub(record);
    if (bitmap_[ub] == 0)
      get_record(ub).swap(record);
    else {  // elements moving is inevitable
      records.insert(records.begin() + ub, record);
      // temporarily grows larger than DATA_SIZE
      auto records_iter = records.end();
      for (size_t i = RECORD_COUNT - 1; i >= 0; i--) {
        // erasing from the tail is more efficient
        if (bitmap_[i] == false) {
          records.erase(records_iter);
          bitmap_[i] = true;
          break;
        }
        records_iter--;
      }
    }
  }

  bool delete_record(size_t index) {
    if (bitmap_[index]) {
      // will be cleaned when elements shift during insertions
      bitmap_[index] = false;
      return true;
    }
    return false;
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