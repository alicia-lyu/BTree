#include "db/datapage.h"
#include <cassert>
#include <algorithm>

constexpr size_t PAGE_SIZE = 4096;
constexpr size_t RECORD_SIZE = 128;
constexpr size_t KEY_SIZE = 16;

using TestPage = FixedRecordDataPage<PAGE_SIZE, RECORD_SIZE, KEY_SIZE>;
using MMapFile = TestPage::MMapFile;
using Record = TestPage::Record;

class DummyMMapFile : public frozenca::MemoryMappedFileImpl {
 public:
  DummyMMapFile(const std::string& file_name, size_t size, bool create) : frozenca::MemoryMappedFileImpl(file_name, size, create) {
    (void)file_name;  // To suppress unused parameter warning
    if (create) {
      data_ = new unsigned char[size];
      std::fill(data_, data_ + size, 0);
    }
    size_ = size;
  }

  ~DummyMMapFile() { delete[] data_; }

  void* get_page_ptr(std::uint64_t file_offset, std::size_t page_size) {
    assert(file_offset + page_size <= size_);
    return static_cast<void*>(data_ + file_offset);
  }

 private:
  unsigned char* data_;
  size_t size_;
};

int main() {
  DummyMMapFile mmap_file("./Testing/Temporary/mmap.tmp", PAGE_SIZE * 2, true);
  TestPage page(mmap_file, 0);
  Record record;

  std::fill(record.begin(), record.end(), 'a');

  auto [it, ret] = page.insert(record);
  assert(page.size() == 1);
  assert(ret == true);
  assert(page.validate(it) == true);
  std::fill(record.begin(), record.end(), 'a');
  assert(std::equal(record.begin(), record.end(), (*it).begin()));

  auto it_opt = page.erase(*it);
  assert(it_opt.has_value());
  assert(page.size() == 0);
  assert(page.validate(it_opt.value()) == false);

  // ret = true;

  size_t inserted = 0;
  while (ret) {
    char random_char = 'a' + rand() % 26;
    std::fill(record.begin(), record.end(), random_char);
    std::cout << "Inserting record: " << record.data() << std::endl;
    std::tie(it, ret) = page.insert(record);
    if (ret) {
      inserted++;
      assert(page.size() == inserted);
      assert(page.validate(it) == true);
      std::fill(record.begin(), record.end(), random_char);
      assert(std::equal(record.begin(), record.end(), (*it).begin()));
    }
  }
  page.verify_order();
  assert(inserted == TestPage::RECORD_COUNT);
}
