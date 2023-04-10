#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  char *p = buf;

  MACH_WRITE_UINT32(p, CATALOG_METADATA_MAGIC_NUM);
  p += sizeof(uint32_t);

  uint32_t len = table_meta_pages_.size();
  for (auto it : table_meta_pages_) {
    if (it.second == INVALID_PAGE_ID) len--;
  }

  MACH_WRITE_UINT32(p, len);
  p += sizeof(uint32_t);

  // TODO: maybe false because serial
  for (auto item : table_meta_pages_) {
    uint32_t tableId = item.first;
    int32_t pageId = item.second;
    if (pageId >= 0) {
      MACH_WRITE_UINT32(p, tableId);
      p += sizeof(uint32_t);
      MACH_WRITE_UINT32(p, pageId);
      p += sizeof(int32_t);
    }
  }

  len = index_meta_pages_.size();
  for (auto item : index_meta_pages_) {
    if (item.second == INVALID_PAGE_ID) len--;
  }

  MACH_WRITE_UINT32(p, len);
  p += sizeof(uint32_t);

  for (auto item : index_meta_pages_) {
    uint32_t tableId = item.first;
    int32_t pageId = item.second;
    if (pageId >= 0) {
      MACH_WRITE_UINT32(p, tableId);
      p += sizeof(uint32_t);
      MACH_WRITE_UINT32(p, pageId);
      p += sizeof(int32_t);
    }
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf, MemHeap *heap) {
  char *p = buf;
  auto *ret = new (heap->Allocate(sizeof(CatalogMeta))) CatalogMeta;

  [[maybe_unused]] uint32_t val = MACH_READ_FROM(uint32_t, p);
  p += 4;

  uint32_t len = MACH_READ_UINT32(p);
  p += sizeof(uint32_t);
  for (int i = 0; i < int(len); i++) {
    uint32_t tableId = MACH_READ_FROM(uint32_t, p);
    p += sizeof(uint32_t);
    int32_t pageId = MACH_READ_FROM(int32_t, p);
    p += sizeof(uint32_t);
    ret->table_meta_pages_[tableId] = pageId;
  }
  ret->GetTableMetaPages()->emplace(len, INVALID_PAGE_ID);
  len = MACH_READ_UINT32(p);
  p += sizeof(uint32_t);
  for (int i = 0; i < int(len); i++) {
    uint32_t iid = MACH_READ_UINT32(p);
    p += 4;
    int32_t pid = MACH_READ_UINT32(p);
    p += 4;
    ret->index_meta_pages_[iid] = pid;
  }
  ret->GetIndexMetaPages()->emplace(len, INVALID_PAGE_ID);

  return ret;
}

uint32_t CatalogMeta::GetSerializedSize() const {
  uint32_t len = table_meta_pages_.size();
  for (auto item : table_meta_pages_) {
    if (item.second == INVALID_PAGE_ID) len--;
  }
  len += index_meta_pages_.size();
  for (auto item : index_meta_pages_) {
    if (item.second == INVALID_PAGE_ID) len--;
  }
  len = len * 8 + sizeof(uint32_t) + 8;
  return len;
}

CatalogMeta::CatalogMeta() {}

CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager),
      lock_manager_(lock_manager),
      log_manager_(log_manager),
      heap_(new SimpleMemHeap()) {
  if (init) {
    catalog_meta_ = new (heap_->Allocate(sizeof(CatalogMeta))) CatalogMeta;
  } else {
    Page *page = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);
    char *table = page->GetData();

    catalog_meta_ = CatalogMeta::DeserializeFrom(table, heap_);
    next_index_id_ = catalog_meta_->GetNextIndexId();
    next_table_id_ = catalog_meta_->GetNextTableId();

    for (auto item : catalog_meta_->table_meta_pages_) {
      // 如果逻辑页号正确的话
      if (item.second >= 0) {
        TableInfo *tableInfo;
        TableMetadata *meta = nullptr;
        meta->DeserializeFrom(table, meta, heap_);

        page = buffer_pool_manager->FetchPage(item.second);
        table = page->GetData();
        tableInfo = TableInfo::Create(heap_);
        TableHeap *table_heap =
            TableHeap::Create(buffer_pool_manager_, meta->GetSchema(), nullptr, log_manager_, lock_manager_, heap_);
        tableInfo->Init(meta, table_heap);
        table_names_[meta->GetTableName()] = meta->GetTableId();
        tables_[meta->GetTableId()] = tableInfo;
        index_names_.insert({meta->GetTableName(), unordered_map<std::string, index_id_t>()});
      }
    }

    for (auto item : catalog_meta_->index_meta_pages_) {
      if (item.second >= 0) {
        IndexMetadata *meta;

        page = buffer_pool_manager->FetchPage(item.second);
        table = page->GetData();
        IndexMetadata::DeserializeFrom(table, meta, heap_);
        TableInfo *tableInfo = tables_[meta->GetTableId()];
        IndexInfo *indexInfo = IndexInfo::Create(heap_);
        indexInfo->Init(meta, tableInfo, buffer_pool_manager_);
        index_names_[tableInfo->GetTableName()][meta->GetIndexName()] = meta->GetIndexId();
        indexes_[meta->GetIndexId()] = indexInfo;
      }
    }
  }
}

CatalogManager::~CatalogManager() { delete heap_; }

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Transaction *txn,
                                    TableInfo *&table_info) {
  if (table_names_.count(table_name) > 0) return DB_INDEX_ALREADY_EXIST;

  table_id_t tableId = next_table_id_++;
  page_id_t pageId;
  Page *new_table_page = buffer_pool_manager_->NewPage(pageId);

  catalog_meta_->table_meta_pages_[tableId] = pageId;
  catalog_meta_->table_meta_pages_[next_table_id_] = -1;

  TableMetadata *table_meta = TableMetadata::Create(tableId, table_name, pageId, schema, heap_);
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, schema, txn, log_manager_, lock_manager_, heap_);
  table_info = TableInfo::Create(heap_);
  table_info->Init(table_meta, table_heap);

  table_names_[table_name] = tableId;
  tables_[tableId] = table_info;
  index_names_.insert({table_name, std::unordered_map<std::string, index_id_t>()});

  uint32_t len = table_meta->GetSerializedSize();
  char meta[len + 1];
  table_meta->SerializeTo(meta);
  char *p = new_table_page->GetData();
  memcpy(p, meta, len);

  len = catalog_meta_->GetSerializedSize();
  char buf[len + 1];
  catalog_meta_->SerializeTo(buf);
  p = buffer_pool_manager_->FetchPage(0)->GetData();
  memset(p, 0, PAGE_SIZE);
  memcpy(p, buf, len);

  if (table_heap != nullptr) return DB_SUCCESS;
  return DB_FAILED;
}

dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  if (table_names_.count(table_name) > 0) return DB_TABLE_NOT_EXIST;

  table_id_t table_id = table_names_[table_name];
  table_info = tables_[table_id];
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  for (auto item : tables_) {
    tables.push_back(item.second);
  }
  return DB_FAILED;
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info) {
  if (index_names_.count(table_name) > 0) return DB_TABLE_NOT_EXIST;

  auto item = index_names_[table_name];
  if (item.count(index_name) > 0) return DB_INDEX_ALREADY_EXIST;

  index_id_t indexId = next_index_id_++;
  vector<uint32_t> keyMap;
  TableInfo *tableInfo;

  dberr_t error = GetTable(table_name, tableInfo);
  if (!error) return DB_FAILED;
  for (const auto &sItem : index_keys) {
    uint32_t tableKey;
    dberr_t err = tableInfo->GetSchema()->GetColumnIndex(sItem, tableKey);
    if (err) return DB_FAILED;
    keyMap.push_back(tableKey);
  }

  IndexMetadata *index_meta_data_ptr =
      IndexMetadata::Create(indexId, index_name, table_names_[table_name], keyMap, heap_);

  index_info = IndexInfo::Create(heap_);
  index_info->Init(index_meta_data_ptr, tableInfo, buffer_pool_manager_);

  index_names_[table_name][index_name] = indexId;
  indexes_[indexId] = index_info;

  page_id_t pageId;
  Page *new_index_page = buffer_pool_manager_->NewPage(pageId);
  catalog_meta_->index_meta_pages_[indexId] = pageId;
  catalog_meta_->index_meta_pages_[next_index_id_] = -1;

  auto len = index_meta_data_ptr->GetSerializedSize();
  char meta[len + 1];
  index_meta_data_ptr->SerializeTo(meta);
  char *p = new_index_page->GetData();
  memcpy(p, meta, len);

  len = catalog_meta_->GetSerializedSize();
  char cmeta[len + 1];
  catalog_meta_->SerializeTo(cmeta);
  p = buffer_pool_manager_->FetchPage(0)->GetData();
  memset(p, 0, PAGE_SIZE);
  memcpy(p, cmeta, len);

  return DB_SUCCESS;
}

dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  auto search_table = index_names_.find(table_name);
  if (search_table == index_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  auto index_table = search_table->second;
  auto search_index_id = index_table.find(index_name);
  if (search_index_id == index_table.end()) {
    return DB_INDEX_NOT_FOUND;
  }
  index_id_t index_id = search_index_id->second;
  auto it = indexes_.find(index_id);
  if (it == indexes_.end()) return DB_INDEX_NOT_FOUND;
  index_info = it->second;
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  auto table_indexes = index_names_.find(table_name);
  if (table_indexes == index_names_.end()) return DB_TABLE_NOT_EXIST;
  auto indexes_map = table_indexes->second;
  for (const auto &item : indexes_map) {
    indexes.push_back(indexes_.find(item.second)->second);
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::DropTable(const string &table_name) {
  auto tableItem = table_names_.find(table_name);
  if (tableItem == table_names_.end()) return DB_TABLE_NOT_EXIST;
  table_id_t tid = tableItem->second;

  auto indexItem = index_names_.find(table_name);
  for (const auto &item : indexItem->second) {
    DropIndex(table_name, item.first);
  }

  index_names_.erase(table_name);
  tables_.erase(tid);
  page_id_t page_id = catalog_meta_->table_meta_pages_[tid];
  catalog_meta_->table_meta_pages_.erase(tid);
  buffer_pool_manager_->DeletePage(page_id);
  table_names_.erase(table_name);
  auto len = catalog_meta_->GetSerializedSize();
  char meta[len + 1];
  catalog_meta_->SerializeTo(meta);
  char *p = buffer_pool_manager_->FetchPage(0)->GetData();
  memset(p, 0, PAGE_SIZE);
  memcpy(p, meta, len);
  return DB_SUCCESS;
}

dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // ASSERT(false, "Not Implemented yet");
  auto tableItem = index_names_.find(table_name);
  if (tableItem == index_names_.end()) return DB_TABLE_NOT_EXIST;
  auto indexItem = tableItem->second.find(index_name);
  if (indexItem == tableItem->second.end()) return DB_INDEX_NOT_FOUND;

  index_id_t index_id = indexItem->second;
  page_id_t page_id = catalog_meta_->index_meta_pages_[index_id];
  catalog_meta_->index_meta_pages_.erase(index_id);
  tableItem->second.erase(index_name);
  indexes_.erase(index_id);
  buffer_pool_manager_->DeletePage(page_id);

  uint32_t len = catalog_meta_->GetSerializedSize();
  char meta[len + 1];
  catalog_meta_->SerializeTo(meta);
  char *p = buffer_pool_manager_->FetchPage(0)->GetData();
  memset(p, 0, PAGE_SIZE);
  memcpy(p, meta, len);
  return DB_SUCCESS;
}

dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  auto item = tables_.find(table_id);
  if (item == tables_.end()) return DB_TABLE_NOT_EXIST;
  table_info = item->second;
  return DB_SUCCESS;
}