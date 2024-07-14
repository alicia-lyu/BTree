#ifndef BUFFER_POOL_H
#define BUFFER_POOL_H

#include <cstdint>
#include <list>
#include <unordered_map>
#include <memory>
#include <filesystem>
#include <fstream>
#include "db/datapage.h"

template <typename T>
concept DerivedFromDataPage = requires {
  typename std::remove_cvref_t<T>::Record;
  typename std::remove_cvref_t<T>::Key;
  { std::remove_cvref_t<T>::PAGE_SIZE_CONST } -> std::convertible_to<size_t>;
  std::is_base_of_v<DataPage<std::remove_cvref_t<T>::PAGE_SIZE_CONST, typename std::remove_cvref_t<T>::Record, typename std::remove_cvref_t<T>::Key>,
                    std::remove_cvref_t<T>>;
};

template <DerivedFromDataPage PageType>
class BufferPool {
 public:
  using PagePtr = std::unique_ptr<PageType>;

  BufferPool(uint max_pages, const std::filesystem::path& pages_path) : max_pages_(max_pages), pages_path_(pages_path) {
    std::ifstream pages_file(pages_path, std::ios::binary);
    if (!pages_file.read(reinterpret_cast<char*>(&empty_pages_start), sizeof(empty_pages_start))) {
      throw std::runtime_error("Failed to read empty pages start");
    }
    size_t discarded_page_count;
    if (!pages_file.read(reinterpret_cast<char*>(&discarded_page_count), sizeof(discarded_page_count))) {
      throw std::runtime_error("Failed to read discarded page size");
    }
    // LATER: Now we assume all discarded page offsets can fit into one page
    discarded_page_offsets.reserve(discarded_page_count);
    if (!pages_file.read(reinterpret_cast<char*>(discarded_page_offsets.data()), discarded_page_count * sizeof(uint))) {
      throw std::runtime_error("Failed to read discarded page indexes");
    }
  }

  ~BufferPool() {
    std::ofstream pages_file(pages_path_, std::ios::binary | std::ios::trunc);
    size_t discarded_page_count = discarded_page_offsets.size();
    pages_file.write(reinterpret_cast<const char*>(&discarded_page_count), sizeof(discarded_page_count));
    pages_file.write(reinterpret_cast<const char*>(discarded_page_offsets.data()), discarded_page_count * sizeof(uint));
  }

  // Simplified LRU: Only getting touches a page
  PageType* get_page(uintmax_t offset) {
    auto it = page_map_.find(offset);
    if (it != page_map_.end()) {
      // Move accessed page to the front of the list
      pages_.splice(pages_.begin(), pages_, it->second);
      return it->second->second;
    }
    // Load page if not found
    if (pages_.size() == max_pages_) {
      // Evict least recently used page
      page_map_.erase(pages_.back().first);
      pages_.pop_back();
    }
    pages_.emplace_front(offset, std::make_unique<PageType>(pages_path_, offset));
    page_map_[offset] = pages_.begin();
    return pages_.front().second.get();
  }

  std::pair<PageType*, uintmax_t> get_new_page() {
    uintmax_t new_offset;
    if (empty_pages_start + PageType::PAGE_SIZE_CONST <= std::filesystem::file_size(pages_path_)) {
      new_offset = empty_pages_start;
      empty_pages_start += PageType::PAGE_SIZE_CONST;
    } else if (!discarded_page_offsets.empty()) {
      new_offset = discarded_page_offsets.back();
      discarded_page_offsets.pop_back();
    } else {
      new_offset = std::filesystem::file_size(pages_path_);
      std::filesystem::resize_file(pages_path_, new_offset + PageType::PAGE_SIZE_CONST);
    }
    return {get_page(new_offset), new_offset};
  }

  void discard_page(uintmax_t offset) {
    for (auto page: pages_) {
        if (page.first == offset) {
            page_map_.erase(offset);
            pages_.remove(page);
            break;
        }
    }
    if (offset + PageType::PAGE_SIZE_CONST == empty_pages_start) {
      empty_pages_start = offset;
    } else {
      discarded_page_offsets.push_back(offset);
    }
  }

   private:
    std::size_t max_pages_;
    std::filesystem::path pages_path_;
    std::list<std::pair<uintmax_t, PagePtr>> pages_;  // LRU list
    std::unordered_map<uintmax_t, decltype(pages_.begin())> page_map_;
    std::vector<uintmax_t> discarded_page_offsets;  // Before empty_pages_start
    uintmax_t empty_pages_start;
  };

#endif