#include "record/column.h"
#include "glog/logging.h"

typedef uint32_t uint;

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

uint32_t Column::SerializeTo(char *buf) const {
  char *p = buf;
  // colName
  uint nameLen = strlen(name_.c_str());
  MACH_WRITE_INT32(p, nameLen);
  p += sizeof(uint);
  MACH_WRITE_STRING(p, name_);
  p += strlen(name_.c_str());
  // type
  MACH_WRITE_TO(TypeId, p, type_);
  p += sizeof(TypeId);
  // col index
  MACH_WRITE_INT32(p, table_ind_);
  p += sizeof(uint);
  // nullable and unique
  MACH_WRITE_TO(bool, p, nullable_);
  p += sizeof(bool);
  MACH_WRITE_TO(bool, p, unique_);
  p += sizeof(bool);

  return p - buf;
}

uint32_t Column::GetSerializedSize() const {
  uint size = 0;
  size += (2 * sizeof(uint));
  size += (2 * sizeof(bool));
  size += strlen(name_.c_str());
  size += sizeof(TypeId);
  return size;
}

uint32_t Column::DeserializeFrom(char *buf, Column *&column, MemHeap *heap) {
  if (column != nullptr) {
    LOG(WARNING) << "Pointer to column is not null in column deserialize." << std::endl;
  }

  // define var in class
  char *p = buf;
  uint nameLen;
  TypeId type = kTypeInvalid;
  uint colIndex = 0;
  bool nullable = false;
  bool unique = false;

  // read from disk(buf)
  nameLen = MACH_READ_UINT32(p);
  p += sizeof(uint);
  char columnName_char[nameLen + 1];
  memcpy(columnName_char, p, nameLen);
  std::string columnName(columnName_char);
  p += nameLen;
  type = MACH_READ_FROM(TypeId, p);
  p += sizeof(TypeId);
  colIndex = MACH_READ_UINT32(p);
  p += sizeof(uint);
  nullable = MACH_READ_FROM(bool, p);
  p += sizeof(bool);
  unique = MACH_READ_FROM(bool, p);
  p += sizeof(bool);

  void *mem = heap->Allocate(sizeof(Column));
  column = new (mem) Column(columnName, type, colIndex, nullable, unique);

  return p - buf;
}
