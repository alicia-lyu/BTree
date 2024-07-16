// dbtest_helper.h
#ifndef DBTEST_HELPER_H
#define DBTEST_HELPER_H

#include <array>
#include <filesystem>
#include <string>

constexpr size_t PAGE_SIZE = 4096;
constexpr size_t RECORD_SIZE = 200;
constexpr size_t KEY_SIZE = 20;

using Record = std::array<unsigned char, RECORD_SIZE>;
using Key = std::array<unsigned char, KEY_SIZE>;

Record create_sample_record(int id);
Key create_sample_key(int id);
std::filesystem::path get_new_pages_file(size_t page_count);

#endif // DBTEST_HELPER_H