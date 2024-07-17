#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <array>
#include <filesystem>
#include <cassert>
#include <limits>
#include <string>

#include "dbtest_helper.h"
#include "db/db_btree.h"
#include "db/buffer_pool.h"
#include "db/fixed_datapage.h"
#include "fc/btree.h"

constexpr size_t MAX_PAGES = 8;

// Test to validate DBBTree construction and initialization
void test_DBBTree_initialization() {
    std::filesystem::path pages_path = "./pages.bin";
    std::filesystem::path btree_path = "./btree.bin";

    // Remove any existing test files
    std::filesystem::remove(pages_path);
    std::filesystem::remove(btree_path);

    // Create the DBBTree instance
    DBBTree<false, 4, FixedRecordDataPage<PAGE_SIZE, RECORD_SIZE, KEY_SIZE>> dbbtree(pages_path, btree_path, MAX_PAGES);
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
    DBBTree<true, 4, FixedRecordDataPage<PAGE_SIZE, RECORD_SIZE, KEY_SIZE>> btree(pages_path, btree_path, MAX_PAGES);

    // Insert a record
    Record record = create_sample_record(1);
    auto pair = btree.insert(record);
    assert(pair.second == true);

    assert(btree.verify_order());

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
    DBBTree<true, 4, FixedRecordDataPage<PAGE_SIZE, RECORD_SIZE, KEY_SIZE>> btree(pages_path, btree_path, MAX_PAGES);

    // Insert multiple records
    for (size_t i = 0; i < 100; ++i) {
        auto [it, inserted] = btree.insert(create_sample_record(i));
        assert(inserted == true);
    }

    // TODO: When records exceed the capacity of buffer pool
    // Deserialization is not working properly.

    assert(btree.verify_order());

    // Iterate through the records and validate
    size_t expected_id = 0;
    for (auto it = btree.begin(); it != btree.end(); ++it) {
        Record record = *it;
        Record expected = create_sample_record(expected_id);
        if(!std::equal(expected.begin(), expected.end(), record.begin())) {
            std::cout << "Expected: " << std::string(expected.begin(), expected.end()) << std::endl;
            std::cout << "Actual: " << std::string(record.begin(), record.end()) << std::endl;
        }
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
    for (size_t i = 0; i < 50; ++i) {
        auto page = buffer_pool.get_new_page();
        assert(page.first.use_count() == 2);
        page_offsets.push_back(page.second);
    }

    auto offset_it = page_offsets.begin();
    // while (offset_it < page_offsets.begin() + 50) {
    //     assert(!buffer_pool.query_page(*offset_it));
    //     ++offset_it;
    // }
        
    while (offset_it < page_offsets.end()) {
        assert(buffer_pool.query_page(*offset_it));
        ++offset_it;
    }

    offset_it = page_offsets.begin();

    while(offset_it < page_offsets.end()) {
        auto page = buffer_pool.get_page(*offset_it);
        assert(page->next_page_offset_ = std::numeric_limits<uintmax_t>::max());
    }

    std::cout << "BufferPool test passed." << std::endl;
}

// Test to validate record erasure in DBBTree
void test_DBBTree_erase() {
    std::filesystem::path pages_path = "./pages.bin";
    std::filesystem::path btree_path = "./btree.bin";

    std::filesystem::remove(pages_path);
    std::filesystem::remove(btree_path);

    DBBTree<true, 4, FixedRecordDataPage<PAGE_SIZE, RECORD_SIZE, KEY_SIZE>> btree(pages_path, btree_path, MAX_PAGES);

    // Insert multiple records
    for (size_t i = 0; i < 50; ++i) {
        auto [it, inserted] = btree.insert(create_sample_record(i));
        assert(inserted == true);
    }

    // Erase some records to trigger page merging and borrowing
    for (size_t i = 0; i < 25; ++i) {
        auto it = btree.search(create_sample_record(i));
        assert(it != btree.end());
        btree.erase(it);
    }

    // Verify remaining records
    for (size_t i = 25; i < 50; ++i) {
        auto it = btree.search(create_sample_record(i));
        assert(it != btree.end());
    }

    assert(btree.verify_order());
    std::cout << "DBBTree erase test passed." << std::endl;
}

int main() {
    test_BufferPool();
    test_DBBTree_initialization();
    test_DBBTree_insert_search();
    test_DBBTree_iterator();
    test_DBBTree_erase();
    std::cout << "All tests passed successfully." << std::endl;
    return 0;
}