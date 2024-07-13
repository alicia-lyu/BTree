#ifndef DATAPAGE_H
#define DATAPAGE_H

#include <stddef.h>
#include <sys/types.h>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <cstring>
#include <iterator>
#include <cassert>

#include "fc/mmfile_nix.h"

// C++ named requirement: Container
template <typename C>
concept Container = requires(C c) {
  typename C::value_type;
  typename C::reference;
  typename C::const_reference;
  typename C::iterator;
  typename C::const_iterator;
  typename C::difference_type;
  typename C::size_type;

  { c.begin() } -> std::same_as<typename C::iterator>;
  { c.end() } -> std::same_as<typename C::iterator>;
  { c.cbegin() } -> std::same_as<typename C::const_iterator>;
  { c.cend() } -> std::same_as<typename C::const_iterator>;
  { c.size() } -> std::same_as<typename C::size_type>;
  { c.max_size() } -> std::same_as<typename C::size_type>;
  { c.empty() } -> std::convertible_to<bool>;
} && std::copyable<C> && requires(C c, typename C::size_type n) {
  { c.data() } -> std::same_as<typename C::value_type*>;
};

template <size_t PAGE_SIZE, Container Record, Container Key>
class DataPage {
 protected:
  using KeyOrRecord = std::variant<Record, Key>;
  using MMapFile = frozenca::MemoryMappedFileImpl;
  uintmax_t next_page_offset_;

  DataPage(MMapFile& mmap_file, uintmax_t file_offset) : next_page_offset_(std::bit_cast<uintmax_t>(get_mmap_ptr(mmap_file, file_offset))) {}

  DataPage() : next_page_offset_(0) {}

  virtual ~DataPage() {}

  void* get_mmap_ptr(MMapFile& mmap_file, uintmax_t file_offset, std::size_t offset = 0) {
    return reinterpret_cast<void*>(static_cast<unsigned char*>(mmap_file.get_page_ptr(file_offset, PAGE_SIZE)) + offset + sizeof(uintmax_t));
  }

 public:
  static std::string record_to_string(const Record& record) { return std::string(record.begin(), record.end()); }

  static const void* get_data(const KeyOrRecord key_or_record) {
    return std::visit([](const auto& key_or_record) -> const void* { return key_or_record.data(); }, key_or_record);
  }

  static size_t get_size(const KeyOrRecord key_or_record) {
    return std::visit([](const auto& key_or_record) -> size_t { return key_or_record.size(); }, key_or_record);
  }

  virtual Record* get_record(size_t index) = 0;
  virtual Record copy_record(size_t index) = 0;
  virtual unsigned char* get_record_ptr(size_t index) = 0;
  virtual Key* get_key(size_t index) = 0;
  virtual Key copy_key(size_t index) = 0;

  class Iterator {
    DataPage<PAGE_SIZE, Record, Key>* page_;

   public:
    size_t index_;
    using iterator_category = std::bidirectional_iterator_tag;
    using value_type = Record;
    using difference_type = std::ptrdiff_t;
    using pointer = value_type*;
    using reference = value_type&;

    Iterator(DataPage<PAGE_SIZE, Record, Key>* page, size_t index) : page_(page), index_(index) {}

    Record* get_record() { return page_->get_record(index_); }
    Record copy_record() { return page_->copy_record(index_); }
    Key* get_key() { return page_->get_key(index_); }
    Key copy_key() { return page_->copy_key(index_); }

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

    reference operator*() { return *page_->get_record(index_); }

    pointer operator->() { return page_->get_record(index_); }

    bool operator<(const Iterator& other) const { return index_ < other.index_; }
    bool operator<=(const Iterator& other) const { return index_ <= other.index_; }
    bool operator>(const Iterator& other) const { return index_ > other.index_; }
    bool operator>=(const Iterator& other) const { return index_ >= other.index_; }

    difference_type operator-(const Iterator& other) const { return index_ - other.index_; }
    Iterator operator+(difference_type n) const { return Iterator(page_, index_ + n); }
    Iterator operator-(difference_type n) const { return Iterator(page_, index_ - n); }

    DataPage<PAGE_SIZE, Record, Key>* get_page() { return page_; }
  };

  using iterator_type = Iterator;

  virtual iterator_type begin() = 0;
  virtual iterator_type end() = 0;

  virtual bool validate(iterator_type it) = 0;

  virtual std::optional<iterator_type> search_lb(const KeyOrRecord& key_or_record) = 0;
  virtual iterator_type search_ub(const KeyOrRecord& key_or_record) = 0;
  virtual std::optional<iterator_type> search(const KeyOrRecord& key_or_record) = 0;

  virtual std::pair<iterator_type, bool> insert(Record& record, bool allow_dup = true) = 0;

  virtual std::optional<iterator_type> erase(iterator_type it) = 0;
  virtual std::optional<iterator_type> erase(const Record& record) = 0;

  virtual Key split_with(DataPage<PAGE_SIZE, Record, Key>* right_sibling) = 0;

  virtual bool verify_order() = 0;

  virtual iterator_type max() = 0;
  virtual iterator_type min() = 0;

  // LATER: STL-like interface
  // using value_type = Record;
  // using reference = value_type&;
  // using const_reference = const value_type&;
  // using iterator = Iterator;
  // using const_iterator = const Iterator;
  // using size_type = size_t;
  // using difference_type = std::ptrdiff_t;
  // virtual size_type size() const = 0;
  // virtual size_type max_size() const = 0;
  // virtual bool empty() const = 0;
  // virtual void clear() = 0;
  // virtual void swap(DataPage& other) = 0;
};

// Fixed-length key, variable-length value
// template <size_t PAGE_SIZE = 4000, size_t KEY_SIZE>
// class VariableVDataPage : public DataPage<PAGE_SIZE>

#endif