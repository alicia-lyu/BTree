#include "db/fixed_datapage.h"
#include "dbtest_helper.h"
#include <cassert>
#include <algorithm>

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

int main() {}
