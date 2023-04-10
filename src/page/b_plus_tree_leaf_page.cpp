#include <algorithm>
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/b_plus_tree_leaf_page.h"
#include "page/b_plus_tree_internal_page.h"
#include <cstring>
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
    SetParentPageId(parent_id);
    SetPageId(page_id);
    SetPageType(IndexPageType::LEAF_PAGE);
    SetNextPageId(INVALID_PAGE_ID);
    SetMaxSize(max_size);
    SetSize(0);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  next_page_id_=next_page_id;
}

/**
 * Helper method to find the first index i so that array_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  int i=0;
  for(;i<GetSize();i++)
    if(comparator(array_[i].first,key)>=0)
      break;
  return i;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  KeyType key=array_[index].first;
  return key;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  return array_[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
//  int low = 0, high = GetSize(), middle = 0;
//  while(low < high) {
//    middle = (low + high)/2;
//    if(comparator(array_[middle].first,key)==0) {
//      return middle;
//    }
//    else if(comparator(array_[middle].first,key)>0) {
//      high = middle;
//    }
//    else if(comparator(array_[middle].first,key)<0) {
//      low = middle + 1;
//    }
//  }
//  if(comparator(array_[middle].first,key)>0)
//    middle--;
//  for(int i=GetSize()-1;i>middle;i--){
//    array_[i+1]=array_[i];
//  }
//  array_[middle].first=key;
//  array_[middle].second=value;
//  IncreaseSize(1);
//  return GetSize();

  if (GetSize() == 0 || comparator(key, KeyAt(GetSize() - 1)) > 0)
  {
    array_[GetSize()] = {key, value};
  }

  else if (comparator(key, array_[0].first) < 0)
  {
    //memmove(array_ + 1, array_, static_cast<size_t>(GetSize() * sizeof(MappingType)));
    for(int i=GetSize()-1;i>=0;i--)
      array_[i+1]=array_[i];
    array_[0] = {key, value};
  }
  else
  {
    int low = 0, high = GetSize() - 1, mid;
    while (low < high && low + 1 != high)
    {
      mid = low + (high - low) / 2;
      if (comparator(key, array_[mid].first) < 0)
      {
        high = mid;
      }
      else if (comparator(key, array_[mid].first) > 0)
      {
        low = mid;
      }
      else
      {
        assert(0);
      }
    }
     for(int i=GetSize()-1;i>=high;i--)
      array_[i+1]=array_[i];
    array_[high] = {key, value};
  }

  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  int half=GetSize()/2;
  recipient->CopyNFrom(array_+GetSize()-half,half);
  IncreaseSize(-1*half);

}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  for(int i=0;i<size;i++)
    array_[GetSize()+i]=*items++;
  IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value, const KeyComparator &comparator) const {
//  int low = 0, high = GetSize(), middle = 0;
//  while(low < high) {
//    middle = (low + high)/2;
//    if(comparator(array_[middle].first,key)==0) {
//      value=array_[middle].second;
//      return true;
//    }
//    else if(comparator(array_[middle].first,key)>0) {
//      high = middle;
//    }
//    else if(comparator(array_[middle].first,key)<0) {
//      low = middle + 1;
//    }
//  }
//  if(comparator(array_[middle].first,key)==0) {
//    value=array_[middle].second;
//    return true;
//  }
//  return false;
    if (GetSize() == 0 || comparator(key, KeyAt(0)) < 0 ||
        comparator(key, KeyAt(GetSize() - 1)) > 0)
    {
      return false;
    }

    int low = 0, high = GetSize() - 1, mid;
    while (low <= high)
    {
      mid = low + (high - low) / 2;
      if (comparator(key, KeyAt(mid)) > 0)
      {
        low = mid + 1;
      }
      else if (comparator(key, KeyAt(mid)) < 0)
      {
        high = mid - 1;
      }
      else
      {
        value = array_[mid].second;
        return true;
      }
    }
    return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {
  if (GetSize() == 0 || comparator(key, KeyAt(0)) < 0 ||
      comparator(key, KeyAt(GetSize() - 1)) > 0)
  {
    return GetSize();
  }

  int low = 0, high = GetSize() - 1, mid;
  while (low <= high)
  {
    mid = low + (high - low) / 2;
    if (comparator(key, KeyAt(mid)) > 0)
    {
      low = mid + 1;
    }
    else if (comparator(key, KeyAt(mid)) < 0)
    {
      high = mid - 1;
    }
    else
    {

      for(int i=mid;i<GetSize()-1;i++)
        array_[i]=array_[i+1];
      IncreaseSize(-1);
      break;
    }
  }
  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  recipient->CopyNFrom(array_,GetSize());
  recipient->SetNextPageId(GetNextPageId());
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {

  recipient->CopyLastFrom(array_[0]);
  for(int i=0;i<GetSize()-1;i++)
    array_[i]=array_[i+1];
  IncreaseSize(-1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  array_[GetSize()]=item;
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  recipient->CopyFirstFrom(array_[GetSize()-1]);
  IncreaseSize(-1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  for(int i=GetSize()-1;i>=0;i--)
    array_[i+1]=array_[i];
  array_[0]=item;
  IncreaseSize(1);
}


/*
 * My function
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient,BufferPoolManager *buffer_pool_manager){
  int half=GetSize()/2;
  recipient->CopyNFrom(array_+GetSize()-half,half);
  IncreaseSize(-1*half);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient,const KeyType &middle_key,BufferPoolManager *buffer_pool_manager){
  recipient->CopyNFrom(array_,GetSize());
  recipient->SetNextPageId(GetNextPageId());
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient,const KeyType &middle_key,BufferPoolManager *buffer_pool_manager){
  Page* page=buffer_pool_manager->FetchPage(GetParentPageId());
  auto* parent=reinterpret_cast<BPlusTreeInternalPage<KeyType, decltype(GetPageId()),KeyComparator>*>(page->GetData());
  parent->SetKeyAt(parent->ValueIndex(GetPageId()),array_[1].first);
  buffer_pool_manager->UnpinPage(GetParentPageId(),true);

  recipient->CopyLastFrom(array_[0]);
  for(int i=0;i<GetSize()-1;i++)
    array_[i]=array_[i+1];
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient,const KeyType &middle_key,BufferPoolManager *buffer_pool_manager){
  recipient->CopyFirstFrom(array_[GetSize()-1],middle_key,buffer_pool_manager);
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item,const KeyType& middle_key,BufferPoolManager* bufferPoolManager_){
  for(int i=GetSize()-1;i>=0;i--)
    array_[i+1]=array_[i];
  array_[0]=item;
  IncreaseSize(1);
  Page* page=bufferPoolManager_->FetchPage(GetParentPageId());
  auto* parent=reinterpret_cast<BPlusTreeInternalPage<KeyType,decltype(GetPageId()),KeyComparator>*>(page->GetData());
  int index=parent->ValueIndex(GetPageId());
  parent->SetKeyAt(index,item.first);

  bufferPoolManager_->UnpinPage(GetParentPageId(),true);

//  Page* page=bufferPoolManager_->FetchPage(GetParentPageId());
//  auto* parent=reinterpret_cast<BPlusTreeInternalPage<KeyType, decltype(GetPageId()),KeyComparator>*>(page->GetData());
}

template
class BPlusTreeLeafPage<int, int, BasicComparator<int>>;

template
class BPlusTreeLeafPage<GenericKey<4>, RowId, GenericComparator<4>>;

template
class BPlusTreeLeafPage<GenericKey<8>, RowId, GenericComparator<8>>;

template
class BPlusTreeLeafPage<GenericKey<16>, RowId, GenericComparator<16>>;

template
class BPlusTreeLeafPage<GenericKey<32>, RowId, GenericComparator<32>>;

template
class BPlusTreeLeafPage<GenericKey<64>, RowId, GenericComparator<64>>;