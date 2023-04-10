#include "storage/table_iterator.h"
#include "common/macros.h"
#include "storage/table_heap.h"

TableIterator::TableIterator() {}

TableIterator::TableIterator(TableHeap *tableHeap, RowId rowId, Transaction *transaction)
    : pTableHeap(tableHeap), pTransaction(transaction), pRow(new Row(rowId)) {
  if (rowId.GetPageId() != INVALID_PAGE_ID) {
    pTableHeap->GetTuple(pRow, transaction);
  }
}

TableIterator::TableIterator(const TableIterator &other)
    : pTableHeap(other.pTableHeap), pTransaction(other.pTransaction), pRow(new Row(*other.pRow)) {}

TableIterator::~TableIterator() { delete pRow; }

bool TableIterator::operator==(const TableIterator &itr) const { return pRow->GetRowId() == itr.pRow->GetRowId(); }

bool TableIterator::operator!=(const TableIterator &itr) const { return !(*this == itr); }

Row& TableIterator::operator*() { return *pRow; }

Row *TableIterator::operator->() { return pRow; }

TableIterator &TableIterator::operator++() {
  BufferPoolManager *buffer_pool_manager = pTableHeap->buffer_pool_manager_;
  auto cur_page = static_cast<TablePage *>(buffer_pool_manager->FetchPage(pRow->rid_.GetPageId()));
  cur_page->RLatch();

  RowId next_tuple_rid;
  if (!cur_page->GetNextTupleRid(pRow->rid_, &next_tuple_rid)) {
    while (cur_page->GetNextPageId() != INVALID_PAGE_ID) {
      auto next_page = static_cast<TablePage *>(buffer_pool_manager->FetchPage(cur_page->GetNextPageId()));
      cur_page->RUnlatch();
      buffer_pool_manager->UnpinPage(cur_page->GetTablePageId(), false);
      cur_page = next_page;
      cur_page->RLatch();
      if (cur_page->GetFirstTupleRid(&next_tuple_rid)) {
        break;
      }
    }
  }
  pRow->rid_ = next_tuple_rid;

  if (*this != pTableHeap->End()) {
    pTableHeap->GetTuple(pRow, pTransaction);
  }
  cur_page->RUnlatch();
  buffer_pool_manager->UnpinPage(cur_page->GetTablePageId(), false);
  return *this;
}

TableIterator TableIterator::operator++(int) {
  TableIterator old_heap(*this);
  ++(*this);
  return old_heap;
}
