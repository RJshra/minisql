#include "storage/table_heap.h"

bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  // confirm the data can place in a page
  if (row.GetSerializedSize(nullptr) + 32 > PAGE_SIZE) {
    return false;
  }

  auto curPage = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  if (curPage == nullptr) {
    LOG(WARNING) << "Warning: the first page cant find in disk" << endl;
    return false;
  }

  curPage->WLatch();
  while (!curPage->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
    auto nextPageId = curPage->GetNextPageId();
    if (nextPageId != INVALID_PAGE_ID) {
      curPage->WUnlatch();
      buffer_pool_manager_->UnpinPage(curPage->GetTablePageId(), false);
      curPage = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(nextPageId));
      curPage->WLatch();
    } else {
      auto newPage = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(nextPageId));
      if (newPage == nullptr) {
        curPage->WUnlatch();
        buffer_pool_manager_->UnpinPage(curPage->GetTablePageId(), false);
        return false;
      }
      newPage->WLatch();
      curPage->SetNextPageId(nextPageId);
      newPage->Init(nextPageId, curPage->GetTablePageId(), log_manager_, txn);
      curPage->WUnlatch();
      buffer_pool_manager_->UnpinPage(curPage->GetTablePageId(), true);
      curPage = newPage;
    }
  }
  curPage->WUnlatch();
  buffer_pool_manager_->UnpinPage(curPage->GetTablePageId(), true);

  return true;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    LOG(WARNING) << "warning: when update tuple, the page is nullptr" << endl;
    return false;
  }
  Row oldRow(row);
  page->WLatch();
  bool isUpdate = page->UpdateTuple(row, &oldRow, schema_, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), isUpdate);
  return isUpdate;
}

void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    LOG(WARNING) << "warning: ApplyDelete cant find a page containing rid" << endl;
    return;
  }
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback the delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::FreeHeap() {}

bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  RowId rowId(row->GetRowId());
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rowId.GetPageId()));
  if (page == nullptr) {
    return false;
  }
  page->RLatch();
  bool res = page->GetTuple(row, schema_, txn, lock_manager_);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(rowId.GetPageId(), false);
  return res;
}

TableIterator TableHeap::Begin(Transaction *txn) {
  RowId rowId;
  auto pageId = first_page_id_;
  while (pageId != INVALID_PAGE_ID) {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rowId.GetPageId()));
    page->RLatch();
    auto foundTuple = page->GetFirstTupleRid(&rowId);
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(pageId, false);
    if (foundTuple) {
      break;
    }
    pageId = page->GetNextPageId();
  }
  return TableIterator(this, rowId, txn);
}

TableIterator TableHeap::End() { return TableIterator(this, RowId(INVALID_PAGE_ID, 0), nullptr); }
