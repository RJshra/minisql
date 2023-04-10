#include "buffer/buffer_pool_manager.h"
#include "glog/logging.h"
#include "page/bitmap_page.h"

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

// 1.     Search the page table for the requested page (P).
// 1.1    If P exists, pin it and return it immediately.
// 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
//        Note that pages are always found from the free list first.
// 2.     If R is dirty, write it back to the disk.
// 3.     Delete R from the page table and insert P.
// 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  Page *result = nullptr;
  frame_id_t frame_id;
  if (page_table_.find(page_id) != page_table_.end()) {
    size_t i = 0;
    for (; i < pool_size_; i++)
      if (pages_[i].page_id_ == page_id) break;
    result = &pages_[i];
    ++result->pin_count_;
    auto temp = page_table_.find(page_id);
    frame_id = temp->second;
    replacer_->Pin(frame_id);
    return result;
  } else {
    if (!free_list_.empty()) {
      frame_id = free_list_.front();
      free_list_.pop_front();

      for (size_t i = 0; i < pool_size_; i++) {
        if (pages_[i].page_id_ == INVALID_PAGE_ID) {
          result = &pages_[i];
          break;
        }
      }
    } else {
      if (!replacer_->Victim(&frame_id)) return nullptr;

      page_id_t old = INVALID_PAGE_ID;
      for (auto & temp : page_table_) {
        if (temp.second == frame_id) {
          old = temp.first;
          break;
        }
      }
      for (size_t i = 0; i < pool_size_; i++) {
        if (pages_[i].page_id_ == old) {
          result = &pages_[i];
          break;
        }
      }
      for (auto it = page_table_.begin(); it != page_table_.end(); it++) {
        if (it->first == old) {
          page_table_.erase(it);
        }
      }
    }
  }

  if (result->is_dirty_) {
    disk_manager_->WritePage(result->GetPageId(), result->GetData());
  }

  page_table_.emplace(page_id, frame_id);

  result->page_id_ = page_id;
  result->pin_count_ = 1;
  result->is_dirty_ = false;
  disk_manager_->ReadPage(page_id, result->GetData());
  return result;
}

// 0.   Make sure you call AllocatePage!
// 1.   If all the pages in the buffer pool are pinned, return nullptr.
// 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
// 3.   Update P's metadata, zero out memory and add P to the page table.
// 4.   Set the page ID output parameter. Return a pointer to P.
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  frame_id_t frame_id = INVALID_FRAME_ID;
  Page *result = nullptr;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    for (size_t i = 0; i < pool_size_; i++) {
      if (pages_[i].page_id_ == INVALID_PAGE_ID) {
        result = &pages_[i];
        break;
      }
    }
  } else {
    if (!replacer_->Victim(&frame_id)) return nullptr;
    for (size_t i = 0; i < pool_size_; i++) {
      auto temp = page_table_.find(pages_[i].page_id_);
      if (temp->second == frame_id) {
        result = &pages_[i];
        break;
      }
    }
    for (auto temp = page_table_.begin(); temp != page_table_.end(); temp++) {
      if (temp->second == frame_id) {
        page_table_.erase(temp);
      }
    }
  }

  page_id = disk_manager_->AllocatePage();

  if (result->is_dirty_) {
    disk_manager_->WritePage(result->page_id_, result->GetData());
  }

  page_table_.emplace(page_id, frame_id);

  result->page_id_ = page_id;
  result->pin_count_ = 1;
  result->is_dirty_ = false;
  result->ResetMemory();
  return result;
}

// 0.   Make sure you call DeallocatePage!
// 1.   Search the page table for the requested page (P).
// 1.   If P does not exist, return true.
// 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
// 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  if (page_table_.find(page_id) == page_table_.end()) return true;
  Page *result = nullptr;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].page_id_ == page_id) {
      if (pages_[i].pin_count_ != 0) return false;
      result = &pages_[i];
      break;
    }
  }
  auto temp = page_table_.find(page_id);
  frame_id_t frame_id = temp->second;

  for (auto it = page_table_.begin(); it != page_table_.end(); it++) {
    if (it->second == frame_id) {
      page_table_.erase(it);
    }
  }

  result->page_id_ = INVALID_PAGE_ID;
  result->is_dirty_ = false;
  free_list_.push_back(frame_id);
  replacer_->Pin(frame_id);
  disk_manager_->DeAllocatePage(page_id);
  return true;
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  if (page_table_.find(page_id) == page_table_.end()) return false;
  Page *result = nullptr;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].page_id_ == page_id) {
      result = &pages_[i];
      break;
    }
  }
  if (result->pin_count_ > 0) {
    if (--result->pin_count_ == 0) {
      auto temp = page_table_.find(page_id);
      frame_id_t frame_id = temp->second;
      replacer_->Unpin(frame_id);
    }
  } else {
    return false;
  }
  if (is_dirty) result->is_dirty_ = true;
  return true;
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
  if (page_id == INVALID_PAGE_ID) return false;
  Page *result = nullptr;
  if (page_table_.find(page_id) != page_table_.end()) {
    for (size_t i = 0; i < pool_size_; i++) {
      if (pages_[i].page_id_ == page_id) {
        result = &pages_[i];
        break;
      }
    }
    disk_manager_->WritePage(page_id, result->GetData());
    return true;
  }
  return false;
}

[[maybe_unused]] page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

[[maybe_unused]] void BufferPoolManager::DeallocatePage(page_id_t page_id) { disk_manager_->DeAllocatePage(page_id); }

bool BufferPoolManager::IsPageFree(page_id_t page_id) { return disk_manager_->IsPageFree(page_id); }

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}