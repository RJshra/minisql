#include "catalog/indexes.h"

IndexMetadata *IndexMetadata::Create(const index_id_t index_id, const string &index_name, const table_id_t table_id,
                                     const vector<uint32_t> &key_map, MemHeap *heap) {
  void *buf = heap->Allocate(sizeof(IndexMetadata));
  return new (buf) IndexMetadata(index_id, index_name, table_id, key_map);
}

uint32_t IndexMetadata::SerializeTo(char *buf) const {
  char *p = buf;

  MACH_WRITE_UINT32(p, INDEX_METADATA_MAGIC_NUM);
  p += sizeof(uint32_t);

  MACH_WRITE_UINT32(p, GetIndexId());
  p += sizeof(uint32_t);

  uint32_t len = index_name_.size();
  memcpy(p, &len, sizeof(uint32_t));
  MACH_WRITE_STRING(p, index_name_);
  p += (len + sizeof(uint32_t));

  MACH_WRITE_TO(table_id_t, p, GetTableId());
  p += sizeof(uint32_t);

  len = key_map_.size();
  MACH_WRITE_UINT32(p, len);
  p += sizeof(uint32_t);

  for (int i = 0; i < int(len); i++) {
    MACH_WRITE_TO(uint32_t, p, key_map_[i]);
    p += sizeof(uint32_t);
  }

  return p - buf;
}

uint32_t IndexMetadata::GetSerializedSize() const {
  uint32_t res = 0;
  uint32_t len = index_name_.size();
  uint32_t keymapSize = key_map_.size();
  res = len + sizeof(uint32_t) * keymapSize + sizeof(uint32_t) * 5;
  return res;
}

uint32_t IndexMetadata::DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap) {
  index_id_t index_id;
  string index_name;
  table_id_t table_id;
  vector<uint32_t> key_map;
  char *p = buf;

  [[maybe_unused]] uint32_t val = MACH_READ_FROM(uint32_t, p);
  p += sizeof(uint32_t);

  index_id = MACH_READ_UINT32(p);
  p += sizeof(uint32_t);

  uint name_len = MACH_READ_UINT32(p);
  p += sizeof(uint32_t);
  index_name.append(p, name_len);
  p += name_len;

  table_id = MACH_READ_UINT32(p);
  p += sizeof(uint32_t);

  uint32_t map_len = MACH_READ_UINT32(p);
  p += sizeof(uint32_t);

  for (int i = 0; i < int(map_len); i++) {
    uint32_t tmp = MACH_READ_UINT32(p);
    p += sizeof(uint32_t);
    key_map.push_back(tmp);
  }

  index_meta = IndexMetadata::Create(index_id, index_name, table_id, key_map, heap);
  return p - buf;
}