#include "page/bitmap_page.h"

template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  for (size_t byteIndex = 0; byteIndex < MAX_CHARS; byteIndex++) {
    if (bytes[byteIndex] != 0xFF) {
      for (int i = 0; i < 8; ++i) {
        if (IsPageFreeLow(byteIndex, i)) {
          setByteIndex(bytes[byteIndex], i);
          page_offset = byteIndex * 8 + i;
          return true;
        }
      }
    }
  }
  return false;
}

template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if (IsPageFree(page_offset)) {
    return false;
  }
  size_t byteIndex = page_offset / 8;
  size_t bitIndex = page_offset % 8;
  unsetByteIndex(bytes[byteIndex], bitIndex);
  return true;
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  size_t maxSize = GetMaxSupportedSize();
  size_t byteIndex = page_offset / 8;
  size_t bitIndex = page_offset % 8;
  if (byteIndex >= maxSize) {
    return false;
  }
  return IsPageFreeLow(byteIndex, bitIndex);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  char targetByte = bytes[byte_index];
  if (!isByteIndexSet(targetByte, bit_index)) {
    return true;
  }
  return false;
}

[[maybe_unused]] void unsetByteIndex(uint8_t &byte, uint32_t index) {
  uint8_t mask = 0x80 >> index;
  mask = ~mask;
  byte &= mask;
}

[[maybe_unused]] void setByteIndex(uint8_t &byte, uint32_t index) {
  uint8_t mask = 0x80 >> index;
  byte |= mask;
}

[[maybe_unused]] void convertByteIndex(uint8_t &byte, uint32_t index) {
  if (isByteIndexSet(byte, index)) {
    unsetByteIndex(byte, index);
  } else {
    setByteIndex(byte, index);
  }
}

[[maybe_unused]] bool isByteIndexSet(uint8_t byte, uint32_t index) {
  uint8_t byte_bak = byte;
  setByteIndex(byte, index);
  if (byte == byte_bak) {
    return true;
  }
  return false;
}

// used for test
template class BitmapPage<9>;

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;