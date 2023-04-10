#ifndef MINISQL_DISK_FILE_META_PAGE_H
#define MINISQL_DISK_FILE_META_PAGE_H

#include <cstdint>

#include "page/bitmap_page.h"

// page_size我们在测试中是512byte，实际上是4096byte，即4kb
static constexpr page_id_t MAX_VALID_PAGE_ID = (PAGE_SIZE - 8) / 4 * BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();

class DiskFileMetaPage {
 public:
  uint32_t GetExtentNums() { return num_extents_; }

  uint32_t GetAllocatedPages() { return num_allocated_pages_; }

  uint32_t GetExtentUsedPage(uint32_t extent_id) {
    if (extent_id >= num_extents_) {
      return 0;
    }
    return extent_used_page_[extent_id];
  }

 public:
  // 太雷人了，为啥变量也是public
  uint32_t num_allocated_pages_{0};
  // 每个拓展分区包含了一个位图页和32704个数据页
  // 32704 = (4096-4*2)*8
  uint32_t num_extents_{0};       // each extent consists with a bit map and BIT_MAP_SIZE pages
  uint32_t extent_used_page_[0];  // 零长数组，相当于一个地址标识符
};

#endif  // MINISQL_DISK_FILE_META_PAGE_H
