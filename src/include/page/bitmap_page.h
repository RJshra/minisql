#ifndef MINISQL_BITMAP_PAGE_H
#define MINISQL_BITMAP_PAGE_H

#include <bitset>

#include "common/config.h"
#include "common/macros.h"

template <size_t PageSize>
class BitmapPage {
 public:
  /**
   * @return The number of pages that the bitmap page can record, i.e. the capacity of an extent.
   */
  static constexpr size_t GetMaxSupportedSize() { return 8 * MAX_CHARS; }

  /**
   * @param page_offset Index in extent of the page allocated.
   * @return true if successfully allocate a page.
   */
  bool AllocatePage(uint32_t &page_offset);

  /**
   * @return true if successfully de-allocate a page.
   */
  bool DeAllocatePage(uint32_t page_offset);

  /**
   * @return whether a page in the extent is free
   */
  [[nodiscard]] bool IsPageFree(uint32_t page_offset) const;

 private:
  /**
   * check a bit(byte_index, bit_index) in bytes is free(value 0).
   *
   * @param byte_index value of page_offset / 8
   * @param bit_index value of page_offset % 8
   * @return true if a bit is 0, false if 1.
   */
  [[nodiscard]] bool IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const;

  /** Note: need to update if modify page structure. */
  static constexpr size_t MAX_CHARS = PageSize - 2 * sizeof(uint32_t);

 private:
  /** The space occupied by all members of the class should be equal to the PageSize */
  [[maybe_unused]] uint32_t page_allocated_;
  [[maybe_unused]] uint32_t next_free_page_;
  [[maybe_unused]] unsigned char bytes[MAX_CHARS];
};

/**
 * this func is use to set the specific index to 0
 * @param byte
 * @param index
 */
[[maybe_unused]] void unsetByteIndex(uint8_t &byte, uint32_t index);

/**
 * this func is used to set the special index to 1
 * @param byte
 * @param index
 */
[[maybe_unused]] void setByteIndex(uint8_t &byte, uint32_t index);

/**
 * convert the special index bit
 * @param byte
 * @param index
 */
[[maybe_unused]] void convertByteIndex(uint8_t &byte, uint32_t index);

[[maybe_unused]] bool isByteIndexSet(uint8_t byte, uint32_t index);

#endif  // MINISQL_BITMAP_PAGE_H
