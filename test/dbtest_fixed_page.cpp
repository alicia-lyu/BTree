#include "db/datapage.h"
#include "fc_catch2.h"

using MMapFile = frozenca::MemoryMappedFileImpl;

constexpr size_t PAGE_SIZE = 4096;
constexpr size_t RECORD_SIZE = 128;
constexpr size_t KEY_SIZE = 16;

using TestPage = FixedRecordDataPage<PAGE_SIZE, RECORD_SIZE, KEY_SIZE>;

TEST_CASE("FixedRecordDataPage basic operations", "[DataPage]") {
  MMapFile mmap_file("./Testing/Temporary/mmap.tmp", PAGE_SIZE * 2, true);

  SECTION("Insert and retrieve records") {
    TestPage page(mmap_file, 0);
    TestPage::Record record;
    std::fill(record.begin(), record.end(), 'a');

    REQUIRE_NOTHROW(page.insert(record));
    // auto [it, ret] = page.insert(record);
    // REQUIRE(page.size() == 1);
    // REQUIRE(ret == true);
    // REQUIRE(page.validate(it) == true);
    // REQUIRE(std::equal(record.begin(), record.end(), (*it).begin()));

    // auto retrieved_record = *it;
    // REQUIRE(std::equal(record.begin(), record.end(), retrieved_record.begin()));
  }

  SECTION("Search lower and upper bounds") {
    TestPage page(mmap_file, 0);
    TestPage::Record record1;
    std::fill(record1.begin(), record1.end(), 'a');

    TestPage::Record record2;
    std::fill(record2.begin(), record2.end(), 'b');

    page.insert(record1);
    page.insert(record2);
    REQUIRE(page.size() == 2);

    auto lb = page.search_lb(record1);
    REQUIRE(lb.has_value());
    REQUIRE(std::equal(record1.begin(), record1.end(), (*lb.value()).begin()));

    auto ub = page.search_ub(record1);
    REQUIRE(ub != page.end());
    REQUIRE(std::equal(record2.begin(), record2.end(), (*ub).begin()));
  }

  SECTION("Erase records") {
    TestPage page(mmap_file, 0);
    TestPage::Record record;
    std::fill(record.begin(), record.end(), 'a');

    auto [it, ret] = page.insert(record);
    REQUIRE(page.size() == 1);
    page.erase(*it);
    REQUIRE(page.size() == 0);
  }

  SECTION("Split pages") {
    TestPage left_page(mmap_file, 0);
    TestPage right_page(mmap_file, PAGE_SIZE);

    for (size_t i = 0; i < TestPage::RECORD_COUNT; ++i) {
      TestPage::Record record;
      std::fill(record.begin(), record.end(), 'a' + i);
      left_page.insert(record);
    }

    REQUIRE(left_page.size() == TestPage::RECORD_COUNT);

    auto split_key = left_page.split_with(right_page);
    REQUIRE(left_page.size() == TestPage::RECORD_COUNT / 2);
    REQUIRE(right_page.size() == TestPage::RECORD_COUNT / 2 );
    REQUIRE(std::equal(split_key.begin(), split_key.end(), (*right_page.begin()).begin()));
  }
}
