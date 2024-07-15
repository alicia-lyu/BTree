#include <cstddef>
#include <iostream>
#include <array>
#include <filesystem>
#include <cassert>
#include <string>

#include "db/db_btree.h"
#include "db/buffer_pool.h"
#include "db/fixed_datapage.h"

constexpr size_t PAGE_SIZE = 4096;
constexpr size_t RECORD_SIZE = 200;
constexpr size_t KEY_SIZE = 20;
constexpr size_t MAX_PAGES = 8;

using Record = std::array<unsigned char, RECORD_SIZE>;
using Key = std::array<unsigned char, KEY_SIZE>;

// Helper function to create a sample record
Record create_sample_record(int id) {
    Record record = {};
    std::string id_str = std::to_string(id);
    std::copy(id_str.begin(), id_str.end(), record.begin());
    return record;
}

// Helper function to create a sample key
Key create_sample_key(int id) {
    Key key = {};
    std::string id_str = std::to_string(id);
    std::copy(id_str.begin(), id_str.end(), key.begin());
    return key;
}

// Test to validate DBBTree construction and initialization
void test_DBBTree_initialization() {
    std::filesystem::path pages_path = "./pages.bin";
    std::filesystem::path btree_path = "./btree.bin";

    // Remove any existing test files
    std::filesystem::remove(pages_path);
    std::filesystem::remove(btree_path);

    // Create the DBBTree instance
    DBBTree<false, 4, FixedRecordDataPage<PAGE_SIZE, RECORD_SIZE, KEY_SIZE>> btree(pages_path, btree_path, MAX_PAGES);
    std::cout << "DBBTree initialization test passed." << std::endl;
}

// Test to validate search functionality in DBBTree
void test_DBBTree_insert_search() {
    std::filesystem::path pages_path = "./pages.bin";
    std::filesystem::path btree_path = "./btree.bin";

    // Remove any existing test files
    std::filesystem::remove(pages_path);
    std::filesystem::remove(btree_path);

    // Create the DBBTree instance
    DBBTree<false, 4, FixedRecordDataPage<PAGE_SIZE, RECORD_SIZE, KEY_SIZE>> btree(pages_path, btree_path, MAX_PAGES);

    // Insert a record
    Record record = create_sample_record(1);
    auto [it1, inserted] = btree.insert(record);
    assert(inserted == true);

    // Search for the inserted record
    auto it2 = btree.search(create_sample_record(1));
    assert(it2 != btree.end());
    std::cout << "DBBTree search test passed." << std::endl;
}

// Test to validate iterator functionality in DBBTree
void test_DBBTree_iterator() {
    std::filesystem::path pages_path = "./pages.bin";
    std::filesystem::path btree_path = "./btree.bin";

    // Remove any existing test files
    std::filesystem::remove(pages_path);
    std::filesystem::remove(btree_path);

    // Create the DBBTree instance
    DBBTree<false, 4, FixedRecordDataPage<PAGE_SIZE, RECORD_SIZE, KEY_SIZE>> btree(pages_path, btree_path, MAX_PAGES);

    // Insert multiple records
    for (int i = 1; i <= 10; ++i) {
        auto [it, inserted] = btree.insert(create_sample_record(i));
        assert(inserted == true);
    }

    // Iterate through the records and validate
    int expected_id = 1;
    for (auto it = btree.begin(); it != btree.end(); ++it) {
        Record record = *it;
        std::string id_str = std::to_string(expected_id);
        assert(std::equal(id_str.begin(), id_str.end(), record.begin()));
        ++expected_id;
    }
    std::cout << "DBBTree iterator test passed." << std::endl;
}

// Test to validate BufferPool functionality
void test_BufferPool() {
    std::filesystem::path pages_path = "./pages.bin";

    // Remove any existing test files
    std::filesystem::remove(pages_path);

    // Create BufferPool instance
    BufferPool<FixedRecordDataPage<PAGE_SIZE, RECORD_SIZE, KEY_SIZE>> buffer_pool(50, pages_path);

    std::vector<uintmax_t> page_offsets;
    // Simulate getting pages
    for (size_t i = 0; i < 100; ++i) {
        auto page = buffer_pool.get_new_page(page_offsets.empty() ? PAGE_SIZE : page_offsets.back() + PAGE_SIZE);
        assert(page.first.use_count() == 2);
        page_offsets.push_back(page.second);
    }

    auto offset_it = page_offsets.begin();
    while (offset_it < page_offsets.begin() + 50) {
        assert(!buffer_pool.query_page(*offset_it));
        ++offset_it;
    }
        
    while (offset_it < page_offsets.end()) {
        assert(buffer_pool.query_page(*offset_it));
        ++offset_it;
    }

    std::cout << "BufferPool test passed." << std::endl;
}

// Test function for serialization and deserialization
void test_page_serialization() {
    std::filesystem::path page_path = "./test_page.bin";
    uintmax_t file_offset = 0;

    std::filesystem::remove(page_path);

    {
        FixedRecordDataPage<PAGE_SIZE, RECORD_SIZE, KEY_SIZE> page(page_path, file_offset);

        for (int i = 1; i <= 100; ++i) {
            page.insert(create_sample_record(i));
        }
        // page destroyed
    }

    {
        FixedRecordDataPage<PAGE_SIZE, RECORD_SIZE, KEY_SIZE> page(page_path, file_offset);
        auto page_it = page.begin();
        for (int i = 1; i <= 100; ++i, ++page_it) {
            auto record = *page_it;
            std::string id_str = std::to_string(i);
            assert(std::equal(id_str.begin(), id_str.end(), record.begin()));
        }
    }

    std::cout << "Serialization and deserialization test passed." << std::endl;
}

// Test to validate record erasure in FixedRecordDataPage
void test_DataPage_erase() {
    std::filesystem::path page_path = "./test_page.bin";
    uintmax_t file_offset = 0;

    std::filesystem::remove(page_path);

    FixedRecordDataPage<PAGE_SIZE, RECORD_SIZE, KEY_SIZE> page(page_path, file_offset);

    // Insert records to fill up the page
    for (int i = 1; i <= 50; ++i) {
        page.insert(create_sample_record(i));
    }

    // Erase half the records
    for (int i = 1; i <= 25; ++i) {
        auto it = page.search(create_sample_record(i));
        page.erase(it);
    }

    // Verify remaining records
    for (int i = 26; i <= 50; ++i) {
        auto it = page.search(create_sample_record(i));
        assert(it != page.end());
    }

    std::cout << "DataPage erase test passed." << std::endl;
}

// Test to validate record erasure in DBBTree
void test_DBBTree_erase() {
    std::filesystem::path pages_path = "./pages.bin";
    std::filesystem::path btree_path = "./btree.bin";

    std::filesystem::remove(pages_path);
    std::filesystem::remove(btree_path);

    DBBTree<false, 4, FixedRecordDataPage<PAGE_SIZE, RECORD_SIZE, KEY_SIZE>> btree(pages_path, btree_path, MAX_PAGES);

    // Insert multiple records
    for (int i = 1; i <= 100; ++i) {
        auto [it, inserted] = btree.insert(create_sample_record(i));
        assert(inserted == true);
    }

    // Erase some records to trigger page merging and borrowing
    for (int i = 1; i <= 25; ++i) {
        auto it = btree.search(create_sample_record(i));
        btree.erase(it);
    }

    // Verify remaining records
    for (int i = 26; i <= 100; ++i) {
        auto it = btree.search(create_sample_record(i));
        assert(it != btree.end());
    }

    std::cout << "DBBTree erase test passed." << std::endl;
}

int main() {
    test_BufferPool();
    test_page_serialization();
    test_DBBTree_initialization();
    test_DBBTree_insert_search();
    test_DBBTree_iterator();
    test_DataPage_erase();
    test_DBBTree_erase();
    std::cout << "All tests passed successfully." << std::endl;
    return 0;
}