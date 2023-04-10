#include "catalog/table.h"

uint32_t TableMetadata::SerializeTo(char *buf) const {
  // 写入魔数，用来验证
  char *p = buf;
  MACH_WRITE_UINT32(p, TABLE_METADATA_MAGIC_NUM);
  p += sizeof(uint32_t);

  // 写入这个表要存储的page id
  MACH_WRITE_TO(table_id_t, p, table_id_);
  p += sizeof(uint32_t);

  // 写入表名
  uint32_t len = table_name_.size();
  memcpy(p, &len, sizeof(uint32_t));
  MACH_WRITE_STRING(p, table_name_);
  p += (len + sizeof(uint32_t));

  // 写入要存储的root page id
  MACH_WRITE_UINT32(p, root_page_id_);
  p += sizeof(int32_t);

  // 写入整个表
  p += schema_->SerializeTo(p);

  return p - buf;
}

uint32_t TableMetadata::GetSerializedSize() const {
  uint32_t res = 0;
  uint32_t len = table_name_.size();
  res = 4 * sizeof(uint32_t) + len + schema_->GetSerializedSize();
  return res;
}

/**
 * @param heap Memory heap passed by TableInfo
 */
uint32_t TableMetadata::DeserializeFrom(char *buf, TableMetadata *&table_meta, MemHeap *heap) {
  table_id_t table_id;
  string table_name;
  page_id_t root_page_id;
  TableSchema *schema = nullptr;
  char *p = buf;

  // 验证是否是魔数
  [[maybe_unused]] uint32_t val = MACH_READ_FROM(uint32_t, p);
  p += sizeof(uint32_t);

  // 首先获取table id
  table_id = MACH_READ_FROM(uint32_t, p);
  p += sizeof(uint32_t);

  // 然后获取是表名
  uint32_t name_len = MACH_READ_FROM(uint32_t, p);
  p += sizeof(uint32_t);
  table_name.append(p, name_len);
  p += name_len;

  // 获取根页id
  root_page_id = MACH_READ_FROM(int32_t, p);
  p += 4;

  p += schema->DeserializeFrom(p, schema, heap);

  // 将我们创造出来的表放到heap中进行管理
  void *mem = heap->Allocate(sizeof(TableMetadata));
  table_meta = new (mem) TableMetadata(table_id, table_name, root_page_id, schema);

  return p - buf;
}

/**
 * Only called by create table
 *
 * @param heap Memory heap passed by TableInfo
 */
TableMetadata *TableMetadata::Create(table_id_t table_id, std::string table_name, page_id_t root_page_id,
                                     TableSchema *schema, MemHeap *heap) {
  // allocate space for table metadata
  void *buf = heap->Allocate(sizeof(TableMetadata));
  return new (buf) TableMetadata(table_id, table_name, root_page_id, schema);
}

TableMetadata::TableMetadata(table_id_t table_id, std::string table_name, page_id_t root_page_id, TableSchema *schema)
    : table_id_(table_id), table_name_(table_name), root_page_id_(root_page_id), schema_(schema) {}
