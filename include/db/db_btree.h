#ifndef DB_BTREE_H
#define DB_BTREE_H

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <filesystem>
#include <limits>
#include <variant>
#include <optional>
#include "db/datapage.h"
#include "fc/btree.h"
#include "fc/details.h"

template <typename T>
concept DerivedFromDataPage = requires {
  typename std::remove_cvref_t<T>::Record;
  typename std::remove_cvref_t<T>::Key;
  std::is_base_of_v<DataPage<std::remove_cvref_t<T>::PAGE_SIZE, typename std::remove_cvref_t<T>::Record, typename std::remove_cvref_t<T>::Key>,
                    std::remove_cvref_t<T>>;
};

template <bool AllowDup, frozenca::attr_t Fanout, DerivedFromDataPage PageType>
class DBBTree {
  using BTree =
      std::conditional_t<AllowDup, frozenca::BTreeMultiSet<typename PageType::Key, Fanout>, frozenca::BTreeSet<typename PageType::Key, Fanout>>;
  using BTreeIter = typename BTree::iterator_type;
  using BTreeNode = typename BTree::Node;
  using KeyOrRecord = std::variant<typename PageType::Key, typename PageType::Record>;

  std::filesystem::path pages_path_;
  std::filesystem::path btree_path_;
  BTree btree_;

  struct DBBTreeNonConstIterTraits {
    using difference_type = std::ptrdiff_t;
    using value_type = typename PageType::Record;
    using pointer = value_type*;
    using reference = value_type&;
    using iterator_category = std::bidirectional_iterator_tag;
    using iterator_concept = iterator_category;

    static reference make_ref(value_type& val) noexcept { return val; }
  };

  struct DBBTreeConstIterTraits {
    using difference_type = std::ptrdiff_t;
    using value_type = typename PageType::Record;
    using pointer = const value_type*;
    using reference = const value_type&;
    using iterator_category = std::bidirectional_iterator_tag;
    using iterator_concept = iterator_category;

    static reference make_ref(const value_type& val) noexcept { return val; }
  };

  template <typename IterTraits>
  struct DBBTreeIterator {
    using difference_type = typename IterTraits::difference_type;
    using value_type = typename IterTraits::value_type;
    using pointer = typename IterTraits::pointer;
    using reference = typename IterTraits::reference;
    using iterator_category = typename IterTraits::iterator_category;
    using iterator_concept = typename IterTraits::iterator_concept;

    PageType* page_ = nullptr;
    typename PageType::iterator_type page_iter_;

    DBBTreeIterator() noexcept = default;

    DBBTreeIterator(PageType* page, typename PageType::iterator_type page_iter) noexcept : page_{page}, page_iter_{page_iter} {}

    template <typename IterTraitsOther>
    DBBTreeIterator(const DBBTreeIterator<IterTraitsOther>& other) noexcept : DBBTreeIterator(other.page_, other.page_iter_) {}

    reference operator*() const noexcept { return IterTraits::make_ref(*page_iter_); }

    pointer operator->() const noexcept { return &(IterTraits::make_ref(*page_iter_)); }

    void increment() noexcept {
      ++page_iter_;
      // Handle increment logic to traverse across pages if needed
    }

    void decrement() noexcept {
      --page_iter_;
      // Handle decrement logic to traverse across pages if needed
    }

    DBBTreeIterator& operator++() noexcept {
      increment();
      return *this;
    }

    DBBTreeIterator operator++(int) noexcept {
      DBBTreeIterator temp = *this;
      increment();
      return temp;
    }

    DBBTreeIterator& operator--() noexcept {
      decrement();
      return *this;
    }

    DBBTreeIterator operator--(int) noexcept {
      DBBTreeIterator temp = *this;
      decrement();
      return temp;
    }

    friend bool operator==(const DBBTreeIterator& x, const DBBTreeIterator& y) noexcept { return x.page_ == y.page_ && x.page_iter_ == y.page_iter_; }

    friend bool operator!=(const DBBTreeIterator& x, const DBBTreeIterator& y) noexcept { return !(x == y); }
  };

 public:
  using iterator = DBBTreeIterator<DBBTreeNonConstIterTraits>;
  using const_iterator = DBBTreeIterator<DBBTreeConstIterTraits>;
  using value_type = typename PageType::Record;
  using key_type = typename PageType::Key;
  using reference = value_type&;
  using const_reference = const value_type&;
  using size_type = std::size_t;
  using difference_type = std::ptrdiff_t;
  using attr_t = frozenca::attr_t;

  DBBTree(std::filesystem::path pages_path, std::filesystem::path btree_path) : pages_path_(pages_path), btree_path_(btree_path) {
    std::ifstream btree_file(btree_path, std::ios::binary);
    btree_file >> btree_;
    // TODO: metadata about empty pages?
  }

  ~DBBTree() {
    std::ofstream btree_file(btree_path_, std::ios::binary | std::ios::trunc);
    btree_file << btree_;
  }

  uintmax_t translated_offset(attr_t page_index) { // page table
    if (page_index < 0)
      throw std::runtime_error("Placeholder page index");
    else if (page_index == std::numeric_limits<attr_t>::max())
      throw std::runtime_error("Non-page index");
    return page_index * PageType::PAGE_SIZE;
  }

  attr_t translated_index(uintmax_t offset) { // page table
    if (offset % PageType::PAGE_SIZE != 0)
      throw std::runtime_error("Invalid offset");
    return offset / PageType::PAGE_SIZE;
  }

  iterator search(const key_type& key) {
    auto [btree_it, node] = btree_.find_page(key);
    if (!node) return std::nullopt;

    key_type page_key = node->get_page_key();  // lower bound of all records in the page
    auto offset = translate_offset(node->page_index_);

    assert(std::memcmp(&key, &page_key, sizeof(key)) >= 0);

    PageType page(pages_path_, offset);

    assert(std::memcmp(&key, &page.min(), sizeof(key)) >= 0);
    assert(std::memcmp(&key, &page.max(), sizeof(key)) <= 0 || page = end().page_);

    auto page_it = page.search(page_key);

    if (page_it == page.end()) return end();

    return iterator(&page, *page_it);
  }

  std::pair<iterator, bool> insert(const typename PageType::Record& record) {
    auto key = PageType::get_key(record);
    auto [btree_it1, target_node] = btree_.find_page(key);
    PageType * target_page = new PageType(pages_path_, translated_offset(target_node->page_index_));
    if (target_page->is_full()) {
      uintmax_t new_offset = std::filesystem::file_size(pages_path_); // Find empty pages: page table?
      PageType * new_page = new PageType(pages_path_, new_offset); 
      key_type mid_key = target_page->split_with(new_page);
      btree_.insert(mid_key, translated_index(new_offset));
      if (std::memcmp(&key, &mid_key, sizeof(key)) >= 0) {
        target_page = new_page;
      }
    }
    auto [page_it, inserted] = target_page->insert(record);

    return {iterator(&target_page, page_it), inserted};
  }

  iterator begin() {
    return iterator(const_cast<DBBTree<AllowDup, Fanout, PageType>>(this).begin());
  }

  iterator end() {
    return iterator(const_cast<DBBTree<AllowDup, Fanout, PageType>>(this).end());
  }

  const_iterator begin() const {
    auto btree_begin = btree_.begin();
    if (btree_begin == btree_.end()) return end();
    auto leftmost_page = btree_begin.node_.children_[0].get();
    return const_iterator(leftmost_page, leftmost_page->begin());
  }

  const_iterator end() const {
    auto btree_last = btree_.end()--;
    auto rightmost_page = btree_last.node_.children_.back().get();
    return const_iterator(rightmost_page, rightmost_page->end());
  }

  const_iterator cbegin() const { return begin(); }

  const_iterator cend() const { return end(); }

  // Additional methods for full STL-like interface...
};

#endif  // DB_BTREE_H