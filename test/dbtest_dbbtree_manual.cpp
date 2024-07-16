#include <cstddef>
#include <fstream>
#include <iostream>
#include <array>
#include <filesystem>
#include <cassert>
#include <string>

#include "db/db_btree.h"
#include "db/buffer_pool.h"
#include "db/fixed_datapage.h"
#include "fc/btree.h"

constexpr size_t PAGE_SIZE = 4096;
constexpr size_t RECORD_SIZE = 200;
constexpr size_t KEY_SIZE = 20;
constexpr size_t MAX_PAGES = 8;

using Record = std::array<unsigned char, RECORD_SIZE>;
using Key = std::array<unsigned char, KEY_SIZE>;

// Helper function to create a sample record
Record create_sample_record(int id) {
    Record record = {};
    // Convert the integer to a string with leading zeros, fixed length
    std::string id_str = std::to_string(id);
    id_str = std::string(4 - id_str.length(), '0') + id_str; // Adjust 10 to your required length
    for (size_t i = 0; i < RECORD_SIZE / 4; i++) {
        std::copy(id_str.begin(), id_str.end(), record.begin() + i * 4);
    }
    size_t remaining = RECORD_SIZE - RECORD_SIZE / 4 * 4;
    std::copy(id_str.begin(), id_str.begin() + remaining, record.end() - remaining);
    return record;
}

// Helper function to create a sample key
Key create_sample_key(int id) {
    Key key = {};
    // Convert the integer to a string with leading zeros, fixed length
    std::string id_str = std::to_string(id);
    id_str = std::string(4 - id_str.length(), '0') + id_str; // Adjust 10 to your required length
    for (size_t i = 0; i < KEY_SIZE / 4; i++) {
        std::copy(id_str.begin(), id_str.end(), key.begin() + i * 4);
    }
    size_t remaining = KEY_SIZE - KEY_SIZE / 4 * 4;
    std::copy(id_str.begin(), id_str.begin() + remaining, key.end() - remaining);
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

std::filesystem::path get_new_pages_file(size_t page_count) {
    std::filesystem::path page_path = "./test_page.bin";

    std::filesystem::remove(page_path);

    std::filesystem::create_directories(page_path.parent_path());
    std::ofstream file(page_path, std::ios::binary);
    file.close();
    std::filesystem::resize_file(page_path, PAGE_SIZE * (page_count + 1));

    return page_path;
}

// Test function for serialization and deserialization
void test_page_serialization() {
    uintmax_t file_offset = PAGE_SIZE;
    std::filesystem::path page_path = get_new_pages_file(2);
    {
        FixedRecordDataPage<PAGE_SIZE, RECORD_SIZE, KEY_SIZE> page(page_path, file_offset, PAGE_SIZE * 2);

        for (size_t i = 0; i < 100; ++i) {
            auto [it, ret] = page.insert(create_sample_record(i));
            if (i < page.RECORD_COUNT) {
                assert(ret == true);
            } else {
                assert(ret == false);
            }
        }

        assert(page.verify_order());
        // page destroyed
    }

    {
        FixedRecordDataPage<PAGE_SIZE, RECORD_SIZE, KEY_SIZE> page(page_path, file_offset);
        auto page_it = page.begin();
        for (size_t i = 0; i < page.RECORD_COUNT; ++i, ++page_it) {
            auto record = *page_it;
            auto expected = create_sample_record(i);
            assert(std::equal(expected.begin(), expected.end(), record.begin()));
        }
    }

    std::cout << "Serialization and deserialization test passed." << std::endl;
}

// Test to validate record erasure in FixedRecordDataPage
void test_page_erase() {
    std::filesystem::path page_path = get_new_pages_file(1);
    uintmax_t file_offset = PAGE_SIZE;

    FixedRecordDataPage<PAGE_SIZE, RECORD_SIZE, KEY_SIZE> page(page_path, file_offset, 0);

    // Insert records to fill up the page
    for (size_t i = 0; i < page.RECORD_COUNT; ++i) {
        page.insert(create_sample_record(i));
    }

    // Erase half the records
    for (size_t i = 0; i < page.RECORD_COUNT / 2; ++i) {
        auto it = page.search(create_sample_record(i));
        page.erase(it);
    }

    // Verify remaining records
    for (size_t i = page.RECORD_COUNT / 2; i < page.RECORD_COUNT; ++i) {
        auto it = page.search(create_sample_record(i));
        assert(it != page.end());
    }

    std::cout << "DataPage erase test passed." << std::endl;
}

void test_page_split() {
    std::filesystem::path page_path = get_new_pages_file(2);

    FixedRecordDataPage<PAGE_SIZE, RECORD_SIZE, KEY_SIZE> page1(page_path, PAGE_SIZE, 0);

    for (size_t i = 0; i < page1.RECORD_COUNT; ++i) {
        page1.insert(create_sample_record(i));
    }

    FixedRecordDataPage<PAGE_SIZE, RECORD_SIZE, KEY_SIZE> page2(page_path, PAGE_SIZE * 2, 0);

    page1.split_with(&page2);

    assert(page1.verify_order());

    assert(page2.verify_order());

    assert(std::memcmp(page1.max()->data(), page2.min()->data(), RECORD_SIZE) <= 0);
}

void test_page_merge() {
    std::filesystem::path page_path = get_new_pages_file(2);

    FixedRecordDataPage<PAGE_SIZE, RECORD_SIZE, KEY_SIZE> page1(page_path, PAGE_SIZE, 0);

    FixedRecordDataPage<PAGE_SIZE, RECORD_SIZE, KEY_SIZE> page2(page_path, PAGE_SIZE * 2, 0);

    for (size_t i = 0; i < page1.RECORD_COUNT / 2; ++i) {
        page1.insert(create_sample_record(i));
        page2.insert(create_sample_record(i + page1.RECORD_COUNT / 2));
    }

    page1.merge_with(&page2);
}

void test_page_borrow() {
    std::filesystem::path page_path = get_new_pages_file(2);

    FixedRecordDataPage<PAGE_SIZE, RECORD_SIZE, KEY_SIZE> page1(page_path, PAGE_SIZE, 0);

    FixedRecordDataPage<PAGE_SIZE, RECORD_SIZE, KEY_SIZE> page2(page_path, PAGE_SIZE * 2, 0);

    for (size_t i = 0; i < page1.RECORD_COUNT / 2; ++i) {
        page1.insert(create_sample_record(i));
        page2.insert(create_sample_record(i + page1.RECORD_COUNT / 2));
    }

    for (size_t i = 0; i < 2; ++i) {
        page2.insert(create_sample_record(i + page1.RECORD_COUNT / 2));
    }

    page1.borrow_from(&page2);
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
    test_page_erase();
    test_page_split();
    test_page_merge();
    test_page_borrow();

    test_DBBTree_initialization();
    test_DBBTree_insert_search();
    test_DBBTree_iterator();
    test_DBBTree_erase();
    std::cout << "All tests passed successfully." << std::endl;
    return 0;
}