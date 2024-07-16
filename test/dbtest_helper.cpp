// dbtest_helper.cpp
#include "dbtest_helper.h"
#include <algorithm>
#include <fstream>

Record create_sample_record(int id) {
    Record record = {};
    std::string id_str = std::to_string(id);
    id_str = std::string(4 - id_str.length(), '0') + id_str;
    for (size_t i = 0; i < RECORD_SIZE / 4; i++) {
        std::copy(id_str.begin(), id_str.end(), record.begin() + i * 4);
    }
    size_t remaining = RECORD_SIZE - RECORD_SIZE / 4 * 4;
    std::copy(id_str.begin(), id_str.begin() + remaining, record.end() - remaining);
    return record;
}

Key create_sample_key(int id) {
    Key key = {};
    std::string id_str = std::to_string(id);
    id_str = std::string(4 - id_str.length(), '0') + id_str;
    for (size_t i = 0; i < KEY_SIZE / 4; i++) {
        std::copy(id_str.begin(), id_str.end(), key.begin() + i * 4);
    }
    size_t remaining = KEY_SIZE - KEY_SIZE / 4 * 4;
    std::copy(id_str.begin(), id_str.begin() + remaining, key.end() - remaining);
    return key;
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