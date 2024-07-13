#ifndef DB_BTREE_H
#define DB_BTREE_H

#include <cstddef>
#include <fstream>
#include <filesystem>
#include <variant>
#include <optional>
#include "db/datapage.h"
#include "fc/btree.h"
#include "fc/details.h"

template <typename T>
concept DerivedFromDataPage = requires {
  typename std::remove_cvref_t<T>::Record;
  typename std::remove_cvref_t<T>::Key;
  std::is_base_of_v<DataPage<std::remove_cvref_t<T>::PAGE_SIZE, typename std::remove_cvref_t<T>::Record, typename std::remove_cvref_t<T>::Key>, std::remove_cvref_t<T>>;
};

template <bool AllowDup, frozenca::attr_t Fanout, DerivedFromDataPage PageType>
class DBBTree {
  using BTree = std::conditional_t<AllowDup, frozenca::BTreeMultiSet<typename PageType::Key, Fanout>, frozenca::BTreeSet<typename PageType::Key, Fanout>>;
  using BTreeIter = typename BTree::iterator_type;
  using BTreeNode = typename BTree::Node;
  using KeyOrRecord = std::variant<typename PageType::Key, typename PageType::Record>;

  std::filesystem::path pages_path_;
  BTree btree_;

public:
  DBBTree(std::filesystem::path pages_path, std::filesystem::path btree_path) : pages_path_(pages_path) {
    std::ifstream btree_file(btree_path, std::ios::binary);
    btree_file >> btree_;
    // TODO: Check any metadata at the beginning of pages_file?
  }

  class Iterator {
    using iterator_category = std::bidirectional_iterator_tag;
    using value_type = typename PageType::Record;
    using difference_type = std::ptrdiff_t;
    using pointer = value_type*;
    using reference = value_type&;

    PageType* page_;
    typename PageType::iterator_type page_iter_;

  public:
    Iterator(PageType* page, typename PageType::iterator_type page_iter)
      : page_(page), page_iter_(page_iter) {}

    reference operator*() { return *page_iter_; }
    pointer operator->() { return &(*page_iter_); }

    Iterator& operator++() {
      ++page_iter_;
      return *this;
    }

    Iterator operator++(int) {
      Iterator tmp = *this;
      ++(*this);
      return tmp;
    }

    Iterator& operator--() {
      --page_iter_;
      return *this;
    }

    Iterator operator--(int) {
      Iterator tmp = *this;
      --(*this);
      return tmp;
    }

    bool operator==(const Iterator& other) const { return page_ == other.page_ && page_iter_ == other.page_iter_; }
    bool operator!=(const Iterator& other) const { return !(*this == other); }
  };

  std::optional<Iterator> search(const typename PageType::Key& key) {
    auto [btree_it, node] = btree_.find_page(key);
    if (!node) return std::nullopt;

    auto page_key = node->get_page_key();
    PageType page;  // Assume PageType has a constructor that initializes it from a key
    // Load the page using the key or the index
    auto page_it = page.search(page_key);
    if (!page_it) return std::nullopt;

    return Iterator(&page, *page_it);
  }

  std::pair<Iterator, bool> insert(const typename PageType::Record& record) {
    // Assume PageType has a method to get the key from a record
    auto key = PageType::get_key(record);
    auto [btree_it, node] = btree_.insert_page(key, /* page index */ 0);  // page index should be managed properly
    if (!node) return {Iterator(nullptr, typename PageType::iterator_type()), false};

    PageType page;  // Assume PageType has a constructor that initializes it from a key
    auto [page_it, inserted] = page.insert(record);
    return {Iterator(&page, page_it), inserted};
  }

  // Additional methods for full STL-like interface...
};

#endif // DB_BTREE_H