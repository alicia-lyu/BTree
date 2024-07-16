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
#include <memory>
#include <variant>
#include "db/fixed_datapage.h"
#include "fc/btree.h"
#include "fc/details.h"
#include "db/buffer_pool.h"

template <bool AllowDup, frozenca::attr_t Fanout, class PageTypePlaceholder
          // , DerivedFromDataPage PageType // uncomment for production
          >
class DBBTree {
  using PageType = FixedRecordDataPage<4098, 200, 20>;  // for linter
  using BTree =
      std::conditional_t<AllowDup, frozenca::BTreeMultiSet<typename PageType::Key, Fanout>, frozenca::BTreeSet<typename PageType::Key, Fanout>>;
  using BTreeIter = typename BTree::iterator_type;
  using KeyOrRecord = std::variant<typename PageType::Key, typename PageType::Record>;

  std::filesystem::path btree_path_;
  std::unique_ptr<BTree> btree_;
  std::unique_ptr<BufferPool<PageType>> buffer_pool_;

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
    std::shared_ptr<PageType> page_;
    typename PageType::iterator_type page_iter_;
    BufferPool<PageType>* buffer_pool_;

    void increment() noexcept {
      ++page_iter_;
      page_iter_ = page_->advance_to_valid(page_iter_);

      if (page_iter_ != page_->end()) return;

      auto next_page_offset = page_->next_page_offset_;
      if (next_page_offset == std::numeric_limits<uintmax_t>::max()) return;  // end of the tree
      page_ = buffer_pool_->get_page(next_page_offset);
      page_iter_ = page_->begin();
      page_iter_ = page_->advance_to_valid(page_iter_);
    }

   public:
    DBBTreeIterator(std::shared_ptr<PageType> page, typename PageType::iterator_type page_iter, BufferPool<PageType>* buffer_pool) noexcept
        : page_{page}, page_iter_{page_->advance_to_valid(page_iter)}, buffer_pool_(buffer_pool) {}

    DBBTreeIterator() = delete;

    template <typename IterTraitsOther>
    DBBTreeIterator(const DBBTreeIterator<IterTraitsOther>& other) noexcept : DBBTreeIterator(other.page_, other.page_iter_, other.tree_) {}

    std::shared_ptr<PageType> get_page() const noexcept { return page_; }
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

  DBBTree(std::filesystem::path pages_path, std::filesystem::path btree_path, uint buffer_max_pages)
      : btree_path_(btree_path), buffer_pool_(std::make_unique<BufferPool<PageType>>(buffer_max_pages, pages_path)) {
    btree_ = std::make_unique<BTree>();
    if (std::filesystem::exists(btree_path)) {
      std::ifstream btree_file(btree_path, std::ios::binary);
      btree_file >> *btree_;
    }
    if (btree_->empty()) {
      PageType::Key initial_key;
      std::fill(initial_key.begin(), initial_key.end(), 0);
      auto [initial_new_page_right, offset_right] = buffer_pool_->get_new_page();
      auto [initial_new_page_left, offset_left] = buffer_pool_->get_new_page(offset_right);
      btree_->initialize_pages(initial_key, translated_index(offset_right), translated_index(offset_left));
    }
  }

  ~DBBTree() {
    std::ofstream btree_file(btree_path_, std::ios::binary | std::ios::trunc);
    btree_file << *btree_;
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

  iterator make_it(std::shared_ptr<PageType> page, typename PageType::iterator_type page_iter) {
    return iterator(page, page_iter, buffer_pool_.get());
  }

  iterator search_lb(const key_type& key) {
    auto [btree_it, node] = btree_->find_page_lb(key);

    auto offset = translated_offset(node->page_index_);
    std::shared_ptr<PageType> page = node->is_placeholder_page() ? process_placeholder_page(btree_it, node) : buffer_pool_->get_page(offset);

    auto page_it = page->search_lb(key);
    if (page_it == page->end()) {
      return end();
    } else {
      return make_it(page, page_it);
    }
  }

  iterator search_ub(const key_type& key) {
    auto [btree_it, node] = btree_->find_page_ceil(key);

    auto offset = translated_offset(node->page_index_);
    std::shared_ptr<PageType> page = node->is_placeholder_page() ? process_placeholder_page(btree_it, node) : buffer_pool_->get_page(offset);

    auto page_it = page->search_ub(key);
    if (page_it == page->end()) {
      assert(btree_it == std::prev(btree_->end()));
      return end();
    } else {
      return make_it(page.get(), page_it);
    }
  }

  iterator search(const value_type& record) {
    iterator lb = search_lb(PageType::extract_key(record));
    while (lb != end()) {
      auto ret = std::memcmp(&record, &*lb, sizeof(record));
      if (ret == 0) {
        return lb;
      } else if (ret > 0) {
        return end();
      }
      ++lb;
    }
    return end();
  }

  std::shared_ptr<PageType> find_page_to_insert(const value_type& record) {
    auto key = PageType::extract_key(record);
    auto [btree_it, node] = btree_->find_page_lb(key);
    auto page_lb = node->is_placeholder_page() ? process_placeholder_page(btree_it, node) : buffer_pool_->get_page(translated_offset(node->page_index_));
    if constexpr (!AllowDup) {
      return page_lb;
    }
    while (std::next(btree_it) != btree_->end()) {
      key_type page_key_lb = btree_it.node_->keys_[btree_it.index_];
      key_type page_key_ub = (++btree_it).node_->keys_[btree_it.index_];
      // Key in btree limits the current record to be inserted to page_lb
      if (std::memcmp(key.data(), page_key_ub.data(), key.size()) < 0) {
        return page_lb;
      } else // Key alone cannot tell which page to insert
      if (std::memcmp(page_key_lb.data(), page_key_ub.data(), key.size()) == 0) {
        assert(btree_it.get_page());

        assert(page_lb->next_page_offset_ == translated_offset(btree_it.get_page()->page_index_));
        auto next_page = buffer_pool_->get_page(page_lb->next_page_offset_);
        assert(next_page);
        // If record is less than the min of next page, insert to current page
        if (std::memcmp(record.data(), next_page->min()->data(), record.size()) < 0) {
          return page_lb;
        }
        // Otherwise, continue to next page
        page_lb = next_page;
      }
    }
    return page_lb;
  }

  std::pair<iterator, bool> insert(const value_type& record) {
    auto page = find_page_to_insert(record);

    if (page->is_full()) {
      auto [new_page, new_offset] = buffer_pool_->get_new_page(page->next_page_offset_);
      value_type mid_val = page->split_with(new_page.get());
      btree_->insert_page(PageType::extract_key(mid_val), translated_index(new_offset));
      if (std::memcmp(&record, &mid_val, sizeof(record)) >= 0) {
        page = std::move(new_page);
      }
    }

    auto [page_it, inserted] = page->insert(record, AllowDup);

    auto result = make_it(page, page_it);

    return {result, inserted};
  }

  iterator erase(const value_type& record) {
    auto key = PageType::extract_key(record);
    auto [btree_it, target_node] = btree_->find_page(key);
    std::shared_ptr<PageType> target_page = buffer_pool_->get_page(translated_offset(target_node->page_index_));
    auto page_it = target_page->erase(record);
    inspect_after_erase(page_it);
    if (page_it == target_page->end()) {
      return end();
    } else {
      return make_it(target_page, page_it);
    }
  }

  iterator erase(iterator it) {
    std::shared_ptr<PageType> page = it.get_page();
    auto page_it = page->erase(it.get_page_iter());
    inspect_after_erase(page_it);
    if (page_it == page->end()) {
      return end();
    } else {
      return make_it(page, page_it);
    }
  }

  PageType::iterator_type inspect_after_erase(PageType::iterator_type it) {
    PageType* page = dynamic_cast<PageType*>(it.get_page());  // page pointer owned by caller of inspect_after_erase
    uintmax_t sibling_offset = page->next_page_offset_;
    if (page->size() < page->max_size() / 2) {
      std::shared_ptr<PageType> sibling = buffer_pool_->get_page(sibling_offset);
      if (sibling->size() + page->size() <= page->max_size()) {
        page->merge_with(sibling.get());
        btree_->erase_page(sibling->copy_min_key(), translated_index(sibling_offset));
        buffer_pool_->discard_page(sibling->next_page_offset_);
      } else {
        key_type sibling_original_key = sibling->copy_min_key();
        value_type mid_val = page->borrow_from(sibling.get());
        btree_->erase_page(sibling_original_key, translated_index(sibling_offset));
        btree_->insert_page(PageType::extract_key(mid_val), translated_index(sibling_offset));
      }
      // TODO: it is shifted
    }
    return it;
  }

  std::shared_ptr<PageType> process_placeholder_page(BTreeIter it, BTree::Node* node) {
    assert(node->is_placeholder_page());
    auto next_page_node = it.get_next_page();
    uintmax_t next_page_offset = std::numeric_limits<uintmax_t>::max();
    if (next_page_node) {
      assert(!next_page_node->is_placeholder_page());  // only the leftmost page can be placeholder
      next_page_offset = translated_offset(next_page_node->page_index_);
    }

    auto [page, page_offset] = buffer_pool_->get_new_page(next_page_offset);
    node->page_index_ = translated_index(page_offset);
    return page;
  }

  iterator begin() {
    auto btree_begin = btree_->begin();
    if (btree_begin == btree_->end()) return end();
    assert(!btree_begin.node_->children_.empty());
    auto page_node = btree_begin.node_->children_.front().get();

    if (page_node->is_placeholder_page()) {
      std::shared_ptr<PageType> leftmost_page = process_placeholder_page(btree_begin, btree_begin.node_->children_.front().get());
      return make_it(leftmost_page, leftmost_page->begin());
    } else {
      std::shared_ptr<PageType> leftmost_page = buffer_pool_->get_page(translated_offset(page_node->page_index_));
      return make_it(leftmost_page, leftmost_page->begin());
    }
  }

  iterator end() {
    auto btree_last = std::prev(btree_->end());
    std::shared_ptr<PageType> rightmost_page = buffer_pool_->get_page(translated_offset(btree_last.get_page()->page_index_));
    return make_it(rightmost_page, rightmost_page->end());
  }

  bool verify_order() {
    value_type prev = {0};
    std::shared_ptr<PageType> prev_page = nullptr;
    iterator end = this->end();
    for (iterator it = begin(); it != this->end(); ++it) {
      if (it.get_page() != prev_page) {
        std::cout << *(it.get_page()) << std::endl;
      }
      if (std::memcmp(&prev, &*it, sizeof(value_type)) > 0) {
        std::cout << "Order violation: " << std::string(prev.begin(), prev.begin() + 5) << " vs " << std::string(it->begin(), it->begin() + 5) << std::endl;
        assert(false);
        return false;
      } else {
        std::cout << std::string(it->begin(), it->begin() + 5) << std::endl;
      }
      prev = *it;
      prev_page = it.get_page();
    }
    return true;
  }
};

#endif  // DB_BTREE_H