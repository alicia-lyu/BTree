#include "db/datapage.h"
#include <cassert>
#include <algorithm>

constexpr size_t PAGE_SIZE = 4096;
constexpr size_t RECORD_SIZE = 128;
constexpr size_t KEY_SIZE = 16;

using TestPage = FixedRecordDataPage<PAGE_SIZE, RECORD_SIZE, KEY_SIZE>;
using MMapFile = TestPage::MMapFile;
using Record = TestPage::Record;

int main() {
  MMapFile mmap_file("./Testing/Temporary/mmap.tmp", PAGE_SIZE * 2, true);
  TestPage page(mmap_file, 0);
  Record record;

  std::fill(record.begin(), record.end(), 'a');

  auto [it, ret] = page.insert(record);
  assert(page.size() == 1);
  assert(ret == true);
  assert(page.validate(it) == true);
  assert(std::equal(record.begin(), record.end(), (*it).begin()));

  auto retrieved_record = *it;
  assert(std::equal(record.begin(), record.end(), retrieved_record.begin()));
}
