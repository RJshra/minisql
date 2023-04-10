#include "record/row.h"
#include <bitset>

typedef uint32_t uint;

uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  uint fieldCount = GetFieldCount();
  std::bitset<32> bitset;
  TypeId typeIdList[fieldCount];
  char *p = buf;

  // write info into mem
  for (uint i = 0; i < fieldCount; i++) {
    if (fields_[i]->IsNull()) {
      bitset.set(i);
    }
    typeIdList[i] = fields_[i]->type_id_;
  }

  // write info into disk
  MACH_WRITE_INT32(p, fieldCount);
  p += sizeof(uint);
  MACH_WRITE_INT32(p, bitset.to_ulong());
  p += sizeof(uint);
  for (uint i = 0; i < fieldCount; i++) {
    MACH_WRITE_TO(TypeId, p, typeIdList[i]);
    p += sizeof(TypeId);
    p += fields_[i]->SerializeTo(p);
  }

  return p - buf;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  uint fieldCount;
  uint bitsetNum;
  char *p = buf;

  fieldCount = MACH_READ_INT32(p);
  p += sizeof(uint);
  bitsetNum = MACH_READ_INT32(p);
  p += sizeof(uint);
  std::bitset<32> bitset(bitsetNum);
  TypeId typeIdList[fieldCount];
  for (uint i = 0; i < fieldCount; i++) {
    typeIdList[i] = MACH_READ_FROM(TypeId, p);
    p += sizeof(TypeId);
    auto *field = new Field(typeIdList[i]);
    p += Field::DeserializeFrom(p, typeIdList[i], &field, bitset[i], heap_);
    fields_.push_back(field);
  }

  return p - buf;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  uint size = 0;
  uint fieldCount = GetFieldCount();

  size += (sizeof(uint) * 2);
  size += (fieldCount * sizeof(TypeId));
  for (uint i = 0; i < fieldCount; i++) {
    size += fields_[i]->GetSerializedSize();
  }
  return size;
}
