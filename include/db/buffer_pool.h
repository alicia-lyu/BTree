#ifndef BUFFER_POOL_H
#define BUFFER_POOL_H

#include <cstdint>
#include <list>
#include <optional>
#include <unordered_map>
#include <memory>
#include <filesystem>
#include <fstream>
#include <iostream>
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
  using PagePtr = std::shared_ptr<PageType>;

  BufferPool(uint max_pages, const std::filesystem::path& pages_path) : max_pages_(max_pages), pages_path_(pages_path) {
    if (!std::filesystem::exists(pages_path)) {
      empty_pages_start = PageType::PAGE_SIZE_CONST;
      discarded_page_offsets = {};
      std::filesystem::create_directories(pages_path.parent_path());
      std::ofstream pages_file(pages_path, std::ios::binary);
      assert(std::filesystem::file_size(pages_path) == 0);
      std::filesystem::resize_file(pages_path, PageType::PAGE_SIZE_CONST);
      return;
    }
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
    pages_file.write(reinterpret_cast<const char*>(&empty_pages_start), sizeof(empty_pages_start));
    pages_file.write(reinterpret_cast<const char*>(&discarded_page_count), sizeof(discarded_page_count));
    pages_file.write(reinterpret_cast<const char*>(discarded_page_offsets.data()), discarded_page_count * sizeof(uint));
    size_t padding = PageType::PAGE_SIZE_CONST - (sizeof(empty_pages_start) + sizeof(discarded_page_count) + discarded_page_count * sizeof(uint));
    if (padding > 0) {
      pages_file.write(std::string(padding, '\0').c_str(), padding);
    } else if (padding < 0) {
      std::cerr << "Padding for buffer pool metadata is negative" << std::endl;
    }
  }

  // Simplified LRU: Only getting touches a page
  PagePtr get_page(uintmax_t offset, std::optional<uintmax_t> next_page_offset = std::nullopt) {
    auto map_it = page_map_.find(offset);
    if (map_it != page_map_.end()) {
      // Move accessed page to the front of the list
      pages_.splice(pages_.begin(), pages_, map_it->second);
      return map_it->second->second;
    }
    // Load page if not found
    if (pages_.size() == max_pages_) {
      // Evict least recently used page that is not actively in use
      auto pages_it = --pages_.end();
      while (pages_it != pages_.begin() && pages_it->second.use_count() > 1) {
        --pages_it;
      }
      if (pages_it->second.use_count() > 1) {
        throw std::runtime_error("All pages are in use");
      } else {
        page_map_.erase(pages_it->first);
        pages_.erase(pages_it);
        assert(page_map_.contains(pages_it->first) == false);
      }
    }
    pages_.emplace_front(offset, std::make_shared<PageType>(pages_path_, offset, next_page_offset));
    page_map_[offset] = pages_.begin();
    return pages_.front().second;
  }

  bool query_page(uintmax_t offset) {
    return page_map_.find(offset) != page_map_.end();
  }

  std::pair<PagePtr, uintmax_t> get_new_page(uintmax_t next_page_offset) {
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
      empty_pages_start = new_offset + PageType::PAGE_SIZE_CONST;
    }
    return {get_page(new_offset, next_page_offset), new_offset};
  }

  void discard_page(uintmax_t offset) {
    for (auto &page: pages_) {
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