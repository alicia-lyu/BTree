#ifndef DB_BTREE_H
#define DB_BTREE_H

#include <sys/types.h>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <filesystem>
#include <iterator>
#include <limits>
#include <variant>
#include <optional>
#include "db/datapage.h"
#include "db/fixed_datapage.h"
#include "fc/btree.h"
#include "fc/details.h"
#include "db/buffer_pool.h"

template <bool AllowDup, frozenca::attr_t Fanout
, class PageTypePlaceholder
// , DerivedFromDataPage PageType // uncomment for production
>
class DBBTree {
  using PageType = FixedRecordDataPage<4098, 200, 20>; // for linter
  using BTree =
      std::conditional_t<AllowDup, frozenca::BTreeMultiSet<typename PageType::Key, Fanout>, frozenca::BTreeSet<typename PageType::Key, Fanout>>;
  using BTreeIter = typename BTree::iterator_type;
  using KeyOrRecord = std::variant<typename PageType::Key, typename PageType::Record>;

  std::filesystem::path btree_path_;
  BTree btree_;
  BufferPool<PageType> buffer_pool_;

  struct DBBTreeNonConstIterTraits {
    using difference_type = std::ptrdiff_t;
    using value_type = typename PageType::Record;
    using pointer = value_type*;
    using reference = value_type&;
    using iterator_category = std::forward_iterator_tag;
    using iterator_concept = iterator_category;

    static reference make_ref(value_type& val) noexcept { return val; }
  };

  struct DBBTreeConstIterTraits {
    using difference_type = std::ptrdiff_t;
    using value_type = typename PageType::Record;
    using pointer = const value_type*;
    using reference = const value_type&;
    using iterator_category = std::forward_iterator_tag;
    using iterator_concept = iterator_category;

    static reference make_ref(const value_type& val) noexcept { return val; }
  };

  template <typename IterTraits>
  class DBBTreeIterator {
   public:
    using difference_type = typename IterTraits::difference_type;
    using value_type = typename IterTraits::value_type;
    using pointer = typename IterTraits::pointer;
    using reference = typename IterTraits::reference;
    using iterator_category = typename IterTraits::iterator_category;
    using iterator_concept = typename IterTraits::iterator_concept;

   private:
    PageType* page_;
    typename PageType::iterator_type page_iter_;
    DBBTree* tree_;

    void increment() noexcept {
      ++page_iter_;
      if (page_iter_ != page_->end()) return;
      auto next_page_offset = page_->next_page_offset_;
      if (next_page_offset == std::numeric_limits<uintmax_t>::max()) return;  // end of the tree
      page_ = tree_->buffer_pool_.get_page(next_page_offset).get();
      page_iter_ = page_->begin();
    }

   public:
    DBBTreeIterator(PageType* page, typename PageType::iterator_type page_iter, DBBTree* tree) noexcept : page_{page}, page_iter_{page_iter}, tree_{tree} {}

    DBBTreeIterator() = delete;

    template <typename IterTraitsOther>
    DBBTreeIterator(const DBBTreeIterator<IterTraitsOther>& other) noexcept : DBBTreeIterator(other.page_, other.page_iter_, other.tree_) {}

    PageType * get_page() const noexcept { return page_; }
    PageType::iterator_type get_page_iter() const noexcept { return page_iter_; }

    reference operator*() const noexcept { return IterTraits::make_ref(*page_iter_); }

    pointer operator->() const noexcept { return &(IterTraits::make_ref(*page_iter_)); }

    DBBTreeIterator& operator++() noexcept {
      increment();
      return *this;
    }

    DBBTreeIterator operator++(int) noexcept {
      DBBTreeIterator temp = *this;
      increment();
      return temp;
    }

    // operator-- is limited to the same page

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

  DBBTree(std::filesystem::path pages_path, std::filesystem::path btree_path, uint max_pages) : btree_path_(btree_path), buffer_pool_(max_pages, pages_path) {
    std::ifstream btree_file(btree_path, std::ios::binary);
    btree_file >> btree_;
    // TODO: metadata about empty pages?
  }

  ~DBBTree() {
    std::ofstream btree_file(btree_path_, std::ios::binary | std::ios::trunc);
    btree_file << btree_;
  }

  uintmax_t translated_offset(attr_t page_index) {  // page table
    if (page_index < 0)
      throw std::runtime_error("Placeholder page index");
    else if (page_index == std::numeric_limits<attr_t>::max())
      throw std::runtime_error("Non-page index");
    return page_index * PageType::PAGE_SIZE_CONST;
  }

  attr_t translated_index(uintmax_t offset) {  // page table
    if (offset % PageType::PAGE_SIZE_CONST != 0) throw std::runtime_error("Invalid offset");
    return offset / PageType::PAGE_SIZE_CONST;
  }

  iterator search(const key_type& key) {
    auto [btree_it, node] = btree_.find_page(key);
    if (!node) return std::nullopt;

    key_type page_key = node->get_page_key();
    auto offset = translate_offset(node->page_index_);
    auto& page = buffer_pool_.get_page(offset);

    auto page_it = page->search(page_key);
    if (page_it == page->end()) return end();

    return iterator(page.get(), *page_it);
  }

  std::pair<iterator, bool> insert(const value_type& record) {
    auto key = PageType::extract_key(record);
    auto [btree_it, target_node] = btree_.find_page(key);
    auto& target_page = buffer_pool_.get_page(translated_offset(target_node->page_index_));
    if (target_page->is_full()) {
      auto [new_page, new_offset] = buffer_pool_.get_new_page();
      key_type mid_key = target_page->split_with(new_page);
      btree_.insert(mid_key, translated_index(new_offset));
      if (std::memcmp(&key, &mid_key, sizeof(key)) >= 0) {
        target_page = std::move(new_page);
      }
    }
    auto [page_it, inserted] = target_page->insert(record);
    return {iterator(target_page.get(), page_it), inserted};
  }

  std::optional<iterator> erase(const value_type& record) {
    auto key = PageType::extract_key(record);
    auto [btree_it, target_node] = btree_.find_page(key);
    auto& target_page = buffer_pool_.get_page(translated_offset(target_node->page_index_));
    auto page_it = target_page->erase(record);
    inspect_after_erase(page_it);
    return page_it.has_value() ? std::make_optional(iterator(target_page.get(), *page_it)) : std::nullopt;
  }

  std::optional<iterator> erase(iterator it) {
    auto page_it = it.get_page()->erase(it.get_page_iter());
    inspect_after_erase(page_it);
    return page_it.has_value() ? std::make_optional(iterator(it.get_page(), *page_it)) : std::nullopt;
  }

  void inspect_after_erase(PageType::iterator_type it) {
    PageType * page = dynamic_cast<PageType*>(it.get_page());
    uintmax_t sibling_offset = page->next_page_offset_;
    if (page->size() < page->max_size() / 2) {
      PageType * sibling = buffer_pool_.get_page(sibling_offset);
      if (sibling->size() + page->size() <= page->max_size()) {
        page->merge_with(sibling);
        btree_.erase(sibling->copy_min_key());
        buffer_pool_.discard_page(sibling->next_page_offset_);
      } else {
        key_type sibling_original_key = sibling->copy_min_key();
        key_type mid_key = page->borrow_from(sibling);
        btree_.erase_page(sibling_original_key, translated_index(sibling_offset));
        btree_.insert_page(mid_key, translated_index(sibling_offset));
      }
    }
  }

  iterator begin() { return iterator(const_cast<DBBTree<AllowDup, Fanout, PageType>>(this).begin()); }

  iterator end() { return iterator(const_cast<DBBTree<AllowDup, Fanout, PageType>>(this).end()); }

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